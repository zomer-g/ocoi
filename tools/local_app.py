"""Local Processor Web UI — single-file app with browser interface.

Run:  python tools/local_app.py
Open: http://localhost:5555

Requires: pip install httpx pymupdf openai python-dotenv
"""

import asyncio
import base64
import hashlib
import json
import os
import re
import sys
import threading
import time
from datetime import datetime
from http.server import HTTPServer, BaseHTTPRequestHandler
from pathlib import Path
from urllib.parse import parse_qs, urlparse
import webbrowser

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

APP_DIR = Path(__file__).parent / "local_processor"
STATE_FILE = APP_DIR / ".state.json"
ENV_FILE = APP_DIR / ".env"
CACHE_DIR = Path("./cache")
PORT = 5555

try:
    from dotenv import load_dotenv
    if ENV_FILE.exists():
        load_dotenv(ENV_FILE, override=True)
except ImportError:
    pass

SERVER_URL = os.getenv("OCOI_SERVER_URL", "https://www.ocoi.org.il").rstrip("/")
PUSH_API_KEY = os.getenv("PUSH_API_KEY", "")
DEEPSEEK_API_KEY = os.getenv("DEEPSEEK_API_KEY", "")
DEEPSEEK_BASE_URL = os.getenv("DEEPSEEK_BASE_URL", "https://api.deepseek.com")
CKAN_BASE_URL = os.getenv("CKAN_BASE_URL", "https://www.odata.org.il")
CKAN_QUERY = os.getenv("CKAN_QUERY", "ניגוד עניינים")

# ---------------------------------------------------------------------------
# State management
# ---------------------------------------------------------------------------

def load_state() -> dict:
    if STATE_FILE.exists():
        try:
            return json.loads(STATE_FILE.read_text(encoding="utf-8"))
        except Exception:
            return {}
    return {}

def save_state(state: dict):
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    STATE_FILE.write_text(json.dumps(state, ensure_ascii=False, indent=2, default=str), encoding="utf-8")

def mark(state, url, status, **extra):
    if url not in state:
        state[url] = {}
    state[url]["status"] = status
    state[url]["updated_at"] = datetime.now().isoformat()
    for k, v in extra.items():
        state[url][k] = v
    save_state(state)

def get_summary(state: dict) -> dict:
    counts = {}
    for info in state.values():
        s = info.get("status", "unknown")
        counts[s] = counts.get(s, 0) + 1
    return counts

# ---------------------------------------------------------------------------
# Live log
# ---------------------------------------------------------------------------

_log_lines: list[str] = []
_log_lock = threading.Lock()
_task_running = False

def log(msg: str):
    with _log_lock:
        _log_lines.append(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")
        if len(_log_lines) > 500:
            _log_lines.pop(0)

def get_logs(since: int = 0) -> list[str]:
    with _log_lock:
        return _log_lines[since:]

# ---------------------------------------------------------------------------
# CKAN Import
# ---------------------------------------------------------------------------

async def _search_ckan() -> list[dict]:
    import httpx
    search_url = f"{CKAN_BASE_URL}/api/3/action/package_search"
    all_docs = []
    async with httpx.AsyncClient(timeout=30) as client:
        resp = await client.get(search_url, params={"q": CKAN_QUERY, "rows": 0})
        resp.raise_for_status()
        total = resp.json()["result"]["count"]
        log(f"Found {total} CKAN datasets")
        for start in range(0, total, 100):
            resp = await client.get(search_url, params={"q": CKAN_QUERY, "rows": 100, "start": start})
            resp.raise_for_status()
            for ds in resp.json()["result"]["results"]:
                for res in ds.get("resources", []):
                    fmt = (res.get("format") or "").upper()
                    url = res.get("url", "")
                    if url and fmt in ("PDF", "DOCX", "DOC"):
                        all_docs.append({
                            "file_url": url,
                            "title": res.get("name") or ds.get("title", ""),
                            "file_format": fmt.lower(),
                            "file_size": res.get("size"),
                            "source_type": "ckan",
                            "source_id": ds.get("id", ""),
                            "source_title": ds.get("title", ""),
                            "source_url": f"{CKAN_BASE_URL}/dataset/{ds.get('id', '')}",
                        })
            log(f"Fetched {min(start+100, total)}/{total} datasets")
    return all_docs

async def _check_server_dupes(urls: list[str]) -> set[str]:
    if not urls or not PUSH_API_KEY:
        return set()
    import httpx
    try:
        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.post(
                f"{SERVER_URL}/api/v1/push/check-duplicates",
                json={"urls": urls},
                headers={"X-Push-Key": PUSH_API_KEY},
            )
            resp.raise_for_status()
            return set(resp.json().get("existing_urls", []))
    except Exception as e:
        log(f"Warning: server dedup check failed: {e}")
        return set()

async def run_import(limit: int | None = None):
    global _task_running
    _task_running = True
    try:
        log("=== Import Phase ===")
        docs = await _search_ckan()
        log(f"Total document URLs: {len(docs)}")

        state = load_state()
        new_docs = [d for d in docs if d["file_url"] not in state]
        log(f"New (not in local state): {len(new_docs)}")

        server_existing = await _check_server_dupes([d["file_url"] for d in new_docs])
        if server_existing:
            new_docs = [d for d in new_docs if d["file_url"] not in server_existing]
            log(f"After server dedup: {len(new_docs)}")

        if limit:
            new_docs = new_docs[:limit]
        if not new_docs:
            log("Nothing to download.")
            return

        CACHE_DIR.mkdir(parents=True, exist_ok=True)
        import httpx
        downloaded = 0
        async with httpx.AsyncClient(timeout=60, follow_redirects=True) as client:
            for i, doc in enumerate(new_docs, 1):
                url = doc["file_url"]
                try:
                    log(f"[{i}/{len(new_docs)}] Downloading: {doc['title'][:60]}")
                    resp = await client.get(url)
                    resp.raise_for_status()
                    pdf = resp.content
                    if not pdf[:5].startswith(b"%PDF"):
                        log(f"  Skipped — not a valid PDF")
                        mark(state, url, "failed", title=doc["title"], error="not_pdf")
                        continue
                    h = hashlib.sha256(pdf).hexdigest()
                    path = CACHE_DIR / f"{h}.pdf"
                    path.write_bytes(pdf)
                    mark(state, url, "downloaded", title=doc["title"], content_hash=h,
                         local_path=str(path), file_format=doc["file_format"],
                         file_size=len(pdf), source_type=doc["source_type"],
                         source_id=doc["source_id"], source_title=doc["source_title"],
                         source_url=doc["source_url"])
                    downloaded += 1
                    log(f"  OK ({len(pdf):,} bytes)")
                except Exception as e:
                    log(f"  Failed: {e}")
                    mark(state, url, "failed", title=doc["title"], error=str(e)[:200])
        log(f"Downloaded: {downloaded}/{len(new_docs)}")
    finally:
        _task_running = False

# ---------------------------------------------------------------------------
# Convert
# ---------------------------------------------------------------------------

def run_convert(limit: int | None = None):
    global _task_running
    _task_running = True
    try:
        import pymupdf  # noqa: F401
        log("=== Convert Phase ===")
        state = load_state()
        to_convert = [u for u, i in state.items() if i.get("status") == "downloaded"]
        if limit:
            to_convert = to_convert[:limit]
        if not to_convert:
            log("Nothing to convert.")
            return

        log(f"Documents to convert: {len(to_convert)}")
        converted = 0
        for i, url in enumerate(to_convert, 1):
            info = state[url]
            lp = Path(info.get("local_path", ""))
            title = info.get("title", "")[:60]
            if not lp.exists():
                log(f"[{i}/{len(to_convert)}] {title} — PDF not found")
                mark(state, url, "failed", error="pdf_not_found")
                continue
            try:
                log(f"[{i}/{len(to_convert)}] Converting: {title}")
                md = _convert_pdf(lp)
                if len(md.strip()) > 50:
                    mdp = lp.with_suffix(".md")
                    mdp.write_text(md, encoding="utf-8")
                    mark(state, url, "converted", markdown_path=str(mdp), markdown_chars=len(md))
                    log(f"  OK ({len(md):,} chars)")
                else:
                    mark(state, url, "converted", markdown_chars=0)
                    log(f"  No text extracted")
                converted += 1
            except Exception as e:
                log(f"  Failed: {e}")
                mark(state, url, "failed", error=str(e)[:200])
        log(f"Converted: {converted}/{len(to_convert)}")
    finally:
        _task_running = False

def _convert_pdf(pdf_path: Path) -> str:
    import pymupdf
    doc = pymupdf.open(str(pdf_path))
    pages = []
    for i, page in enumerate(doc):
        blocks = page.get_text("blocks")
        paras = []
        for b in blocks:
            if b[6] == 0:
                t = b[4].strip()
                if t:
                    t = re.sub(r"[\u200f\u200e]+", " ", t)
                    t = re.sub(r"(?<!\n)\n(?!\n)", " ", t)
                    t = re.sub(r" +", " ", t)
                    paras.append(t)
        if paras:
            pages.append(f"--- עמוד {i+1} ---\n" + "\n".join(paras))
    result = "\n\n".join(pages)
    if len(result.strip()) <= 50:
        try:
            pages = []
            for i, page in enumerate(doc):
                tp = page.get_textpage_ocr(language="heb", dpi=200, full=True)
                blocks = page.get_text("blocks", textpage=tp)
                paras = []
                for b in blocks:
                    if b[6] == 0:
                        t = b[4].strip()
                        if t:
                            t = re.sub(r"[\u200f\u200e]+", " ", t)
                            t = re.sub(r"(?<!\n)\n(?!\n)", " ", t)
                            t = re.sub(r" +", " ", t)
                            paras.append(t)
                if paras:
                    pages.append(f"--- עמוד {i+1} ---\n" + "\n".join(paras))
            result = "\n\n".join(pages)
        except Exception:
            pass
    doc.close()
    return result

# ---------------------------------------------------------------------------
# Extract
# ---------------------------------------------------------------------------

SYSTEM_PROMPT = """אתה מומחה בניתוח הסדרים למניעת ניגוד עניינים בישראל.

כללים קריטיים:
1. חלץ אך ורק מידע שכתוב במפורש במסמך. אסור להמציא או לנחש שמות, חברות או עובדות.
2. אם שם לא מופיע במפורש — כתוב null. לעולם אל תנחש.
3. שמות חברות וארגונים — העתק בדיוק כפי שמופיע במסמך.
4. החזר JSON תקין בלבד."""

USER_PROMPT = """נתח את מסמך ניגוד העניינים הבא.

שלב 1 — זהה את הנושא המרכזי (בעל התפקיד):
מסמכים אלה יכולים להיות מכמה סוגים:
- חוות דעת למניעת ניגוד עניינים — נשלחת אל בעל התפקיד. חפש את שמו בשורת "לכבוד".
- פראפרזה/סיכום הסדר — השם מופיע בשורת "הנדון".
- הצהרת ניגוד עניינים עצמית — בעל התפקיד הוא הכותב. חפש את שם החותם.
- הסדר למניעת ניגוד עניינים — השם מופיע בשורת "אל:" או "הנדון".

חשוב: חלץ את השם המלא (שם פרטי + שם משפחה).

שלב 2 — זהה חברות, בנקים וארגונים:
לכל חברה ציין סוג קשר: owns|manages|employed_by|board_member|related_to

שלב 3 — זהה מגבלות: תיאור, גוף, סוג (full|partial|cooling_off)

שלב 4 — בני משפחה עם קשרים עסקיים בלבד.

החזר JSON:
{{"office_holder":{{"name_hebrew":"...","name_english":null,"title":"...","position":"...","ministry":"..."}},"restrictions":[{{"description":"...","related_entities":["..."],"related_domains":["..."],"restriction_type":"...","end_date":null,"details":"..."}}],"companies":[{{"name_hebrew":"...","name_english":null,"company_type":"...","relationship_to_holder":"..."}}],"associations":[{{"name_hebrew":"...","relationship_to_holder":"..."}}],"family_members":[{{"name":"...","relation":"...","related_companies":["..."]}}],"domains":["..."]}}

טקסט המסמך:
{document_text}"""


async def run_extract(limit: int | None = None):
    global _task_running
    _task_running = True
    try:
        log("=== Extract Phase ===")
        if not DEEPSEEK_API_KEY:
            log("ERROR: DEEPSEEK_API_KEY not set")
            return

        state = load_state()
        to_extract = [u for u, i in state.items()
                      if i.get("status") == "converted" and i.get("markdown_chars", 0) > 50]
        if limit:
            to_extract = to_extract[:limit]
        if not to_extract:
            log("Nothing to extract.")
            return

        from openai import AsyncOpenAI
        client = AsyncOpenAI(api_key=DEEPSEEK_API_KEY, base_url=DEEPSEEK_BASE_URL)

        log(f"Documents to extract: {len(to_extract)}")
        extracted = 0
        for i, url in enumerate(to_extract, 1):
            info = state[url]
            title = info.get("title", "")[:60]
            mdp = info.get("markdown_path")
            if not mdp or not Path(mdp).exists():
                continue
            try:
                log(f"[{i}/{len(to_extract)}] Extracting: {title}")
                md = Path(mdp).read_text(encoding="utf-8")
                title_prefix = f"כותרת המסמך: {info.get('title','')}\n\n"
                truncated = title_prefix + md[:15000]

                response = await client.chat.completions.create(
                    model="deepseek-chat",
                    messages=[
                        {"role": "system", "content": SYSTEM_PROMPT},
                        {"role": "user", "content": USER_PROMPT.format(document_text=truncated)},
                    ],
                    temperature=0.1, max_tokens=4000,
                    response_format={"type": "json_object"},
                )
                data = json.loads(response.choices[0].message.content)
                jp = Path(mdp).with_suffix(".json")
                jp.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
                mark(state, url, "extracted", extraction_path=str(jp))
                extracted += 1
                holder = data.get("office_holder", {}).get("name_hebrew", "?")
                nc = len(data.get("companies", []))
                nr = len(data.get("restrictions", []))
                log(f"  OK — {holder}, {nc} companies, {nr} restrictions")
                await asyncio.sleep(0.5)
            except Exception as e:
                log(f"  Failed: {e}")
                mark(state, url, "failed", error=str(e)[:200])
        log(f"Extracted: {extracted}/{len(to_extract)}")
    finally:
        _task_running = False

# ---------------------------------------------------------------------------
# Push
# ---------------------------------------------------------------------------

async def run_push(limit: int | None = None, skip_extract: bool = False):
    global _task_running
    _task_running = True
    try:
        import httpx
        log("=== Push Phase ===")
        if not PUSH_API_KEY:
            log("ERROR: PUSH_API_KEY not set")
            return

        state = load_state()
        target = "converted" if skip_extract else "extracted"
        to_push = [u for u, i in state.items() if i.get("status") == target]
        if limit:
            to_push = to_push[:limit]
        if not to_push:
            log("Nothing to push.")
            return

        log(f"Documents to push: {len(to_push)}")
        pushed = skipped = failed = 0
        async with httpx.AsyncClient() as client:
            for i, url in enumerate(to_push, 1):
                info = state[url]
                title = info.get("title", "")[:60]
                payload = {
                    "title": info.get("title", ""),
                    "file_url": url,
                    "file_format": info.get("file_format", "pdf"),
                    "file_size": info.get("file_size"),
                    "content_hash": info.get("content_hash"),
                    "source_type": info.get("source_type", "ckan"),
                    "source_id": info.get("source_id", ""),
                    "source_title": info.get("source_title", ""),
                    "source_url": info.get("source_url", ""),
                }
                mdp = info.get("markdown_path")
                if mdp and Path(mdp).exists():
                    payload["markdown_content"] = Path(mdp).read_text(encoding="utf-8")
                if not skip_extract:
                    ep = info.get("extraction_path")
                    if ep and Path(ep).exists():
                        payload["extraction_json"] = json.loads(Path(ep).read_text(encoding="utf-8"))
                lp = info.get("local_path")
                if lp and Path(lp).exists():
                    payload["pdf_base64"] = base64.b64encode(Path(lp).read_bytes()).decode("ascii")

                for attempt in range(3):
                    try:
                        log(f"[{i}/{len(to_push)}] Pushing: {title}")
                        resp = await client.post(
                            f"{SERVER_URL}/api/v1/push/documents",
                            json=payload,
                            headers={"X-Push-Key": PUSH_API_KEY},
                            timeout=120,
                        )
                        resp.raise_for_status()
                        st = resp.json().get("status", "?")
                        if st == "created":
                            mark(state, url, "pushed")
                            pushed += 1
                            log(f"  Created")
                        elif st == "skipped":
                            mark(state, url, "pushed")
                            skipped += 1
                            log(f"  Skipped (duplicate)")
                        else:
                            failed += 1
                            log(f"  Server: {st}")
                        break
                    except Exception as e:
                        if attempt < 2:
                            log(f"  Retry in {5*(attempt+1)}s: {e}")
                            await asyncio.sleep(5 * (attempt + 1))
                        else:
                            log(f"  Failed: {e}")
                            mark(state, url, "failed", error=str(e)[:200])
                            failed += 1
                            break
        log(f"Pushed: {pushed}, Skipped: {skipped}, Failed: {failed}")
    finally:
        _task_running = False

# ---------------------------------------------------------------------------
# Run All
# ---------------------------------------------------------------------------

async def run_all(limit: int | None = None):
    await run_import(limit=limit)
    run_convert()
    if DEEPSEEK_API_KEY:
        await run_extract()
    else:
        log("Skipping extraction (no DEEPSEEK_API_KEY)")
    await run_push(skip_extract=not DEEPSEEK_API_KEY)
    log("=== Done ===")

# ---------------------------------------------------------------------------
# Web UI
# ---------------------------------------------------------------------------

HTML = """<!DOCTYPE html>
<html lang="he" dir="rtl">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Local Processor — ניגוד עניינים לעם</title>
<style>
* { box-sizing: border-box; margin: 0; padding: 0; }
body { font-family: 'Segoe UI', Tahoma, sans-serif; background: #f0f2f5; color: #1a1a2e; min-height: 100vh; }
.header { background: #044E66; color: white; padding: 16px 24px; display: flex; align-items: center; gap: 12px; }
.header h1 { font-size: 20px; font-weight: 600; }
.header .badge { background: rgba(255,255,255,0.15); padding: 4px 10px; border-radius: 12px; font-size: 12px; }
.container { max-width: 1100px; margin: 24px auto; padding: 0 16px; }
.grid { display: grid; grid-template-columns: 1fr 1fr; gap: 16px; margin-bottom: 20px; }
@media (max-width: 700px) { .grid { grid-template-columns: 1fr; } }
.card { background: white; border-radius: 12px; padding: 20px; box-shadow: 0 1px 3px rgba(0,0,0,0.08); }
.card h2 { font-size: 15px; color: #044E66; margin-bottom: 12px; display: flex; align-items: center; gap: 8px; }
.card h2 .icon { font-size: 20px; }
.stats { display: grid; grid-template-columns: repeat(auto-fit, minmax(80px, 1fr)); gap: 8px; }
.stat { text-align: center; padding: 10px 6px; background: #f8f9fa; border-radius: 8px; }
.stat .num { font-size: 22px; font-weight: 700; color: #044E66; }
.stat .label { font-size: 11px; color: #666; margin-top: 2px; }
.actions { display: flex; flex-wrap: wrap; gap: 8px; }
.btn { padding: 10px 18px; border: none; border-radius: 8px; font-size: 13px; font-weight: 600;
       cursor: pointer; transition: all 0.15s; display: inline-flex; align-items: center; gap: 6px; }
.btn:disabled { opacity: 0.5; cursor: not-allowed; }
.btn-primary { background: #044E66; color: white; }
.btn-primary:hover:not(:disabled) { background: #06607C; }
.btn-green { background: #059669; color: white; }
.btn-green:hover:not(:disabled) { background: #047857; }
.btn-amber { background: #d97706; color: white; }
.btn-amber:hover:not(:disabled) { background: #b45309; }
.btn-red { background: #dc2626; color: white; }
.btn-red:hover:not(:disabled) { background: #b91c1c; }
.btn-blue { background: #2563eb; color: white; }
.btn-blue:hover:not(:disabled) { background: #1d4ed8; }
.limit-input { width: 70px; padding: 8px; border: 1px solid #d1d5db; border-radius: 6px;
               font-size: 13px; text-align: center; }
.limit-group { display: flex; align-items: center; gap: 6px; font-size: 13px; color: #666; }
.log-card { grid-column: 1 / -1; }
.log { background: #1a1a2e; color: #e2e8f0; border-radius: 8px; padding: 12px 16px;
       font-family: 'Cascadia Code', 'Consolas', monospace; font-size: 12px; line-height: 1.6;
       height: 380px; overflow-y: auto; direction: ltr; text-align: left; }
.log .line { white-space: pre-wrap; word-break: break-all; }
.log .time { color: #64748b; }
.config-table { width: 100%; font-size: 13px; }
.config-table td { padding: 4px 8px; border-bottom: 1px solid #f0f0f0; }
.config-table td:first-child { color: #666; width: 40%; }
.config-table .val { font-family: monospace; color: #044E66; word-break: break-all; }
.config-table .missing { color: #dc2626; font-weight: 600; }
.running-indicator { display: inline-block; width: 8px; height: 8px; background: #22c55e;
                     border-radius: 50%; animation: pulse 1s infinite; margin-left: 8px; }
@keyframes pulse { 0%,100% { opacity: 1; } 50% { opacity: 0.3; } }
</style>
</head>
<body>
<div class="header">
  <svg width="28" height="28" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
    <path d="M12 22s8-4 8-10V5l-8-3-8 3v7c0 6 8 10 8 10z"/>
  </svg>
  <h1>Local Processor</h1>
  <span class="badge">ניגוד עניינים לעם</span>
  <span id="running" style="display:none" class="running-indicator"></span>
</div>

<div class="container">
  <div class="grid">
    <!-- Status -->
    <div class="card">
      <h2><span class="icon">📊</span> סטטוס</h2>
      <div class="stats" id="stats">
        <div class="stat"><div class="num">—</div><div class="label">loading</div></div>
      </div>
    </div>

    <!-- Config -->
    <div class="card">
      <h2><span class="icon">⚙️</span> הגדרות</h2>
      <table class="config-table" id="config"></table>
    </div>

    <!-- Actions -->
    <div class="card" style="grid-column: 1 / -1;">
      <h2><span class="icon">🚀</span> פעולות</h2>
      <div style="display: flex; justify-content: space-between; align-items: center; flex-wrap: wrap; gap: 12px;">
        <div class="actions">
          <button class="btn btn-primary" onclick="doAction('run_all')">▶ הרץ הכל</button>
          <button class="btn btn-blue" onclick="doAction('import')">📥 ייבוא</button>
          <button class="btn btn-amber" onclick="doAction('convert')">📄 המרה</button>
          <button class="btn btn-green" onclick="doAction('extract')">🔍 חילוץ</button>
          <button class="btn btn-primary" onclick="doAction('push')">☁️ העלאה</button>
          <button class="btn btn-red" onclick="doAction('push_skip')">☁️ העלאה (ללא חילוץ)</button>
        </div>
        <div class="limit-group">
          <label>מגבלה:</label>
          <input type="number" id="limit" class="limit-input" placeholder="הכל" min="1">
        </div>
      </div>
    </div>

    <!-- Log -->
    <div class="card log-card">
      <h2><span class="icon">📋</span> לוג</h2>
      <div class="log" id="log"></div>
    </div>
  </div>
</div>

<script>
let logOffset = 0;
let polling = null;

function doAction(action) {
  const limit = document.getElementById('limit').value || '';
  const btns = document.querySelectorAll('.btn');
  btns.forEach(b => b.disabled = true);
  fetch('/api/action?action=' + action + '&limit=' + limit)
    .then(r => r.json())
    .then(d => { if (d.error) alert(d.error); })
    .catch(e => alert('Error: ' + e));
  startPolling();
}

function startPolling() {
  if (polling) return;
  polling = setInterval(poll, 800);
}

function poll() {
  // Status
  fetch('/api/status').then(r => r.json()).then(d => {
    const el = document.getElementById('stats');
    const running = d.running;
    document.getElementById('running').style.display = running ? 'inline-block' : 'none';
    if (!running) {
      document.querySelectorAll('.btn').forEach(b => b.disabled = false);
    }
    const s = d.summary;
    const total = Object.values(s).reduce((a,b) => a+b, 0);
    let html = '<div class="stat"><div class="num">' + total + '</div><div class="label">סה"כ</div></div>';
    const labels = {downloaded:'הורדו',converted:'הומרו',extracted:'חולצו',pushed:'הועלו',failed:'נכשלו'};
    const colors = {downloaded:'#2563eb',converted:'#d97706',extracted:'#059669',pushed:'#044E66',failed:'#dc2626'};
    for (const [k,v] of Object.entries(labels)) {
      const n = s[k] || 0;
      html += '<div class="stat"><div class="num" style="color:' + (colors[k]||'#044E66') + '">' + n + '</div><div class="label">' + v + '</div></div>';
    }
    el.innerHTML = html;
  });

  // Logs
  fetch('/api/logs?since=' + logOffset).then(r => r.json()).then(d => {
    const el = document.getElementById('log');
    for (const line of d.lines) {
      const div = document.createElement('div');
      div.className = 'line';
      const m = line.match(/^\\[([^\\]]+)\\] (.*)$/);
      if (m) {
        div.innerHTML = '<span class="time">[' + m[1] + ']</span> ' + m[2].replace(/</g,'&lt;');
      } else {
        div.textContent = line;
      }
      el.appendChild(div);
    }
    logOffset += d.lines.length;
    if (d.lines.length > 0) el.scrollTop = el.scrollHeight;
  });
}

// Config
fetch('/api/config').then(r => r.json()).then(d => {
  const el = document.getElementById('config');
  let html = '';
  for (const [k,v] of Object.entries(d)) {
    const cls = v === '❌ NOT SET' ? 'val missing' : 'val';
    html += '<tr><td>' + k + '</td><td class="' + cls + '">' + v + '</td></tr>';
  }
  el.innerHTML = html;
});

// Start polling
startPolling();
</script>
</body>
</html>"""


class Handler(BaseHTTPRequestHandler):
    def log_message(self, format, *args):
        pass  # Suppress default HTTP logging

    def do_GET(self):
        parsed = urlparse(self.path)
        path = parsed.path
        qs = parse_qs(parsed.query)

        if path == "/" or path == "":
            self._html(HTML)
        elif path == "/api/status":
            state = load_state()
            self._json({"summary": get_summary(state), "running": _task_running})
        elif path == "/api/logs":
            since = int(qs.get("since", [0])[0])
            self._json({"lines": get_logs(since)})
        elif path == "/api/config":
            self._json({
                "Server": SERVER_URL,
                "Push Key": "✅ Set" if PUSH_API_KEY else "❌ NOT SET",
                "DeepSeek Key": "✅ Set" if DEEPSEEK_API_KEY else "❌ NOT SET",
                "CKAN URL": CKAN_BASE_URL,
                "Query": CKAN_QUERY,
            })
        elif path == "/api/action":
            action = qs.get("action", [""])[0]
            limit_str = qs.get("limit", [""])[0]
            limit = int(limit_str) if limit_str else None

            if _task_running:
                self._json({"error": "A task is already running"})
                return

            if action == "run_all":
                threading.Thread(target=lambda: asyncio.run(run_all(limit)), daemon=True).start()
            elif action == "import":
                threading.Thread(target=lambda: asyncio.run(run_import(limit)), daemon=True).start()
            elif action == "convert":
                threading.Thread(target=lambda: run_convert(limit), daemon=True).start()
            elif action == "extract":
                threading.Thread(target=lambda: asyncio.run(run_extract(limit)), daemon=True).start()
            elif action == "push":
                threading.Thread(target=lambda: asyncio.run(run_push(limit)), daemon=True).start()
            elif action == "push_skip":
                threading.Thread(target=lambda: asyncio.run(run_push(limit, skip_extract=True)), daemon=True).start()
            else:
                self._json({"error": f"Unknown action: {action}"})
                return
            self._json({"ok": True})
        else:
            self.send_error(404)

    def _json(self, data):
        body = json.dumps(data, ensure_ascii=False).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", len(body))
        self.end_headers()
        self.wfile.write(body)

    def _html(self, html):
        body = html.encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", len(body))
        self.end_headers()
        self.wfile.write(body)


def main():
    print(f"\n  🛡️  Local Processor — ניגוד עניינים לעם")
    print(f"  {'='*42}")
    print(f"  Server:    {SERVER_URL}")
    print(f"  Push Key:  {'✅' if PUSH_API_KEY else '❌ NOT SET'}")
    print(f"  DeepSeek:  {'✅' if DEEPSEEK_API_KEY else '❌ NOT SET'}")
    print(f"  {'='*42}")
    print(f"  Open http://localhost:{PORT} in your browser")
    print(f"  Press Ctrl+C to stop\n")

    server = HTTPServer(("127.0.0.1", PORT), Handler)
    webbrowser.open(f"http://localhost:{PORT}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nShutting down...")
        server.shutdown()


if __name__ == "__main__":
    main()
