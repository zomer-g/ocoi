// OCOI extension — content script.
//
// Strategy:
//  1. Walk visible text on the page and extract candidate Hebrew name
//     phrases (multi-word, 2-5 words, all-Hebrew letters with optional
//     internal whitespace and gershayim/maqaf).
//  2. For each unique candidate, ask the background worker to hit the OCOI
//     /search endpoint. Throttle so we don't blast the API.
//  3. For candidates that get a confident match (the search result name is
//     a substring of the candidate, or vice-versa), wrap the matched text
//     in <span class="ocoi-hit"> with the entity id+type stored in dataset.
//  4. Click on a hit → fetch /graph/neighbors → render a floating panel
//     with a simple radial graph drawn in pure SVG.
//
// Auto-scan is OFF by default; user triggers a scan from the popup or by
// right-clicking selected text. This keeps the extension cheap on every
// foreign page until the user actually wants OCOI to look at the page.

(() => {
  if (window.__ocoiInjected) return;
  window.__ocoiInjected = true;

  // Hebrew block U+0590..U+05FF covers letters, finals, geresh and gershayim.
  // Allow ASCII apostrophe / quote / hyphen as in-token punctuation.
  const HEBREW_TOKEN_SRC = "[\\u0590-\\u05FF'\"\\-]+";
  const HEBREW_RANGE = /[֐-׿]/;
  // Maximal run of Hebrew words separated by spaces. We then break each run
  // into n-grams of size 2..4 — entity names in OCOI virtually all fit in
  // that window, while a longer regex would only catch the *whole*
  // sentence and miss the actual name embedded in it.
  const HEBREW_RUN = new RegExp(
    `(?:${HEBREW_TOKEN_SRC})(?:[ \\u00A0](?:${HEBREW_TOKEN_SRC}))*`,
    "g"
  );
  const NGRAM_MIN = 2;
  const NGRAM_MAX = 4;

  // Strip zero-width and bidirectional formatting characters that
  // Webflow / many CMSs sprinkle between Hebrew words for layout. They
  // are invisible to readers but they prevent our regex from seeing two
  // words as adjacent. Replace with a regular space so the run regex
  // treats them as a normal separator.
  const INVISIBLE_RE = /[​-‏‪-‮⁠-⁯﻿]/g;
  function preprocess(text) {
    return String(text).replace(INVISIBLE_RE, " ").replace(/[\t\n\r]+/g, " ");
  }

  function extractCandidates(text, sink) {
    const cleaned = preprocess(text);
    HEBREW_RUN.lastIndex = 0;
    let m;
    while ((m = HEBREW_RUN.exec(cleaned)) !== null) {
      const words = m[0].split(/[  ]+/);
      if (words.length < NGRAM_MIN) continue;
      const nMax = Math.min(NGRAM_MAX, words.length);
      for (let n = NGRAM_MIN; n <= nMax; n++) {
        for (let i = 0; i + n <= words.length; i++) {
          const phrase = words.slice(i, i + n).join(" ");
          if (phrase.length >= 5 && phrase.length <= 80) {
            sink.add(phrase);
          }
        }
      }
    }
  }

  const SCAN_CONCURRENCY = 2;
  const MAX_CANDIDATES_PER_SCAN = 400;
  // Verbose console + warning emission. Off in production. Flip to true
  // by setting localStorage.OCOI_DEBUG = "1" in DevTools → reload page.
  const DEBUG =
    (typeof window !== "undefined" &&
      window.localStorage &&
      window.localStorage.getItem("OCOI_DEBUG") === "1") || false;

  // Cache: candidate string → search result row (or null for "no match").
  const candidateMap = new Map();
  // Track text nodes we've already wrapped so re-scans don't double-wrap.
  const seenNodes = new WeakSet();

  // ---------------------------------------------------------------------
  // Candidate extraction

  function shouldSkip(node) {
    const p = node.parentElement;
    if (!p) return true;
    const tag = p.tagName;
    if (
      tag === "SCRIPT" ||
      tag === "STYLE" ||
      tag === "NOSCRIPT" ||
      tag === "TEXTAREA" ||
      tag === "INPUT" ||
      tag === "CODE" ||
      tag === "PRE"
    ) return true;
    if (p.closest("[data-ocoi-panel], .ocoi-hit, [contenteditable=\"true\"]")) {
      return true;
    }
    return false;
  }

  // Block-level text containers we treat as a "unit" for both candidate
  // extraction and highlighting. We work at this level (not at text nodes)
  // because frameworks like Webflow split words across multiple <span>s,
  // so a phrase like "יריב לוין" can have its two words live in two
  // sibling text nodes — invisible to a per-text-node regex but visible
  // in the parent's combined textContent.
  const BLOCK_SELECTOR =
    "p, h1, h2, h3, h4, h5, h6, li, td, th, dt, dd, blockquote, " +
    "figcaption, caption, summary, article, section, header, footer, " +
    "main, aside, div";

  function blockPriority(el) {
    // Lower = scanned first. We want article-body content scanned before
    // sidebar / nav / footer "div soup", so a candidate cap can't truncate
    // the actual headline of the page out of the run.
    const tag = el.tagName;
    let base;
    if (/^H[1-6]$/.test(tag)) base = parseInt(tag[1], 10) - 1; // H1=0..H6=5
    else if (tag === "P") base = 10;
    else if (
      tag === "LI" || tag === "TD" || tag === "TH" ||
      tag === "DT" || tag === "DD" || tag === "BLOCKQUOTE" ||
      tag === "FIGCAPTION" || tag === "CAPTION"
    ) base = 12;
    else base = 20; // div, section, header, footer, ...
    // Inside <article> / <main> beats arbitrary divs in the chrome.
    if (el.closest('article, main, [role="main"], [role="article"]')) {
      base -= 5;
    }
    return base;
  }

  function collectLeafBlocks() {
    const all = document.body.querySelectorAll(BLOCK_SELECTOR);
    const out = [];
    for (const el of all) {
      // A "leaf" block is one with no nested block-level descendant —
      // i.e. all its text lives directly under it in inline-level wrappers.
      if (el.querySelector(BLOCK_SELECTOR)) continue;
      // Skip blocks that the page or our own panel hijacked.
      if (el.closest("[data-ocoi-panel], .ocoi-hit")) continue;
      const text = el.textContent || "";
      if (text.length < 5) continue;
      if (!HEBREW_RANGE.test(text)) continue;
      out.push(el);
    }
    // Stable sort by priority — Array.prototype.sort is stable in V8 since
    // 2018, so blocks at the same priority preserve document order.
    out.sort((a, b) => blockPriority(a) - blockPriority(b));
    return out;
  }

  function gatherTextNodes(block) {
    // Build a map of text nodes inside `block` and their start offsets in
    // the combined string. We re-implement textContent ourselves so the
    // offsets we hand to Range later line up exactly.
    const walker = document.createTreeWalker(block, NodeFilter.SHOW_TEXT, {
      acceptNode(node) {
        if (shouldSkip(node)) return NodeFilter.FILTER_REJECT;
        if (!node.nodeValue) return NodeFilter.FILTER_REJECT;
        return NodeFilter.FILTER_ACCEPT;
      },
    });
    const textNodes = [];
    const starts = [];
    let combined = "";
    let n;
    while ((n = walker.nextNode())) {
      textNodes.push(n);
      starts.push(combined.length);
      combined += n.nodeValue;
    }
    return { textNodes, starts, combined };
  }

  function collectCandidates() {
    const blocks = [];
    const candidates = new Set();
    let totalLeafBlocks = 0;
    for (const el of collectLeafBlocks()) {
      totalLeafBlocks++;
      const info = gatherTextNodes(el);
      if (!info.combined || info.combined.length < 5) continue;
      if (!HEBREW_RANGE.test(info.combined)) continue;
      extractCandidates(info.combined, candidates);
      blocks.push({ element: el, ...info });
    }
    return { blocks, candidates: [...candidates], totalLeafBlocks };
  }

  // ---------------------------------------------------------------------
  // Match decision: server returns ilike-style results, so we need to
  // double-check the result is actually a sensible match for the candidate
  // text the user is looking at — otherwise a candidate "משרד הבריאות"
  // could match dozens of unrelated rows.

  function normalize(s) {
    return String(s || "")
      .replace(/[׳״'"׳״\-]/g, "")
      .replace(/\s+/g, " ")
      .trim();
  }

  function pickBestMatch(candidate, results) {
    if (!Array.isArray(results) || results.length === 0) return null;
    const cn = normalize(candidate);
    if (!cn || cn.length < 5) return null;
    let best = null;
    let bestScore = 0;
    for (const r of results) {
      const rn = normalize(r.name);
      // Reject ultra-short result names — single common words like "כהן"
      // would otherwise match too aggressively against page text.
      if (!rn || rn.length < 5) continue;
      let score = 0;
      if (rn === cn) score = 100;
      else if (cn.includes(rn) || rn.includes(cn)) {
        const longer = Math.max(rn.length, cn.length);
        const shorter = Math.min(rn.length, cn.length);
        score = Math.round((shorter / longer) * 90);
      }
      if (score > bestScore) {
        bestScore = score;
        best = r;
      }
    }
    // Require ≥70 — strict enough that "משרד הבריאות" doesn't quietly match
    // "המשרד לשירותי דת" via a coincidental substring.
    if (bestScore < 70 || !best) return null;
    // Normalize the field shape: the API returns `entity_type`, but the
    // rest of the extension wants `type` (used in dataset, panel, neighbors
    // lookup). Return a small, stable object instead of the raw row.
    return {
      id: best.id,
      type: best.entity_type || best.type,
      name: best.name,
    };
  }

  // ---------------------------------------------------------------------
  // API plumbing — every fetch goes through the service worker.

  function rpc(message, timeoutMs = 25000) {
    return new Promise((resolve) => {
      let settled = false;
      const finish = (val) => {
        if (settled) return;
        settled = true;
        resolve(val);
      };
      const timer = setTimeout(() => {
        finish({ status: "error", error: "client timeout after " + timeoutMs + "ms" });
      }, timeoutMs);
      try {
        chrome.runtime.sendMessage(message, (resp) => {
          clearTimeout(timer);
          if (chrome.runtime.lastError) {
            finish({ status: "error", error: chrome.runtime.lastError.message });
            return;
          }
          finish(resp);
        });
      } catch (e) {
        clearTimeout(timer);
        finish({ status: "error", error: String(e) });
      }
    });
  }

  async function lookupCandidate(candidate) {
    if (candidateMap.has(candidate)) return candidateMap.get(candidate);
    const resp = await rpc({ type: "ocoi.search", q: candidate });
    let match = null;
    if (resp && resp.status === "ok") {
      match = pickBestMatch(candidate, resp.data || []);
    }
    candidateMap.set(candidate, match);
    return match;
  }

  async function runWithConcurrency(items, worker, concurrency) {
    let i = 0;
    const runners = Array.from({ length: concurrency }, async () => {
      while (i < items.length) {
        const idx = i++;
        try {
          await worker(items[idx]);
        } catch {
          // swallow — single candidate failure shouldn't kill the scan
        }
      }
    });
    await Promise.all(runners);
  }

  // ---------------------------------------------------------------------
  // Highlight injection — wraps a phrase that may span multiple text
  // nodes inside a leaf block, using the Range API so the wrap survives
  // cross-element boundaries (e.g. one word inside <span>, the next not).

  function makeHitSpan(match) {
    const span = document.createElement("span");
    span.className = "ocoi-hit";
    span.dataset.ocoiId = match.id;
    span.dataset.ocoiType = match.type;
    span.dataset.ocoiName = match.name;
    return span;
  }

  function locate(starts, textNodes, charOffset) {
    // Map a char offset in the combined block string back to (node, offsetInNode).
    let i = 0;
    while (i < textNodes.length - 1 && starts[i + 1] <= charOffset) i++;
    return { node: textNodes[i], offset: charOffset - starts[i] };
  }

  function wrapRange(startNode, startOffset, endNode, endOffset, match) {
    const range = document.createRange();
    try {
      range.setStart(startNode, startOffset);
      range.setEnd(endNode, endOffset);
    } catch {
      return false;
    }
    const span = makeHitSpan(match);
    try {
      // Cheap path: works only when the range stays inside one element.
      range.surroundContents(span);
      return true;
    } catch {
      // Fallback: extract the contents and re-insert wrapped. Works across
      // sibling text nodes (and shallowly nested inline elements).
      try {
        const frag = range.extractContents();
        span.appendChild(frag);
        range.insertNode(span);
        return true;
      } catch {
        return false;
      }
    }
  }

  function escapeRegex(s) {
    return s.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
  }

  // Build a regex that matches `phrase` even if invisible characters
  // (zero-width spaces, bidi marks) are sprinkled inside it in the source
  // text — Webflow does this. We allow the marks AND collapse runs of
  // ASCII / NBSP whitespace so a multi-word phrase still matches.
  function phraseRegex(phrase) {
    const flexSep = "[ \\u00A0\\u200B-\\u200F\\u202A-\\u202E\\u2060-\\u206F\\uFEFF]*";
    const parts = phrase.split(/[  ]+/).filter(Boolean).map(escapeRegex);
    const body = parts.join(`[ \\u00A0]${flexSep}`);
    return new RegExp(flexSep + body + flexSep);
  }

  function findPhraseIn(combined, phrase) {
    // Try the cheap path first — if the phrase appears literally, use it.
    const lit = combined.indexOf(phrase);
    if (lit >= 0) return { index: lit, length: phrase.length };
    // Otherwise fall back to a flexible regex that tolerates bidi marks.
    const m = phraseRegex(phrase).exec(combined);
    if (!m) return null;
    return { index: m.index, length: m[0].length };
  }

  function highlightPhraseInBlock(blockInfo, phrase, match) {
    // Re-gather text nodes — earlier highlights may have mutated the DOM,
    // so the cached starts/textNodes are stale.
    const fresh = gatherTextNodes(blockInfo.element);
    const found = findPhraseIn(fresh.combined, phrase);
    if (!found) return false;
    const start = locate(fresh.starts, fresh.textNodes, found.index);
    const stop = locate(fresh.starts, fresh.textNodes, found.index + found.length);
    return wrapRange(start.node, start.offset, stop.node, stop.offset, match);
  }

  function applyHighlights(blocks, candidateMatches) {
    // Process longest phrases first so "עמותת אור ירוק" wins over "אור ירוק"
    // when both happen to match the same span of text.
    const phrases = [...candidateMatches.entries()].sort(
      (a, b) => b[0].length - a[0].length
    );

    let highlights = 0;
    for (const blockInfo of blocks) {
      if (!blockInfo.element.isConnected) continue;
      for (const [phrase, match] of phrases) {
        // Cheap filter: skip blocks that obviously don't contain the phrase
        // before paying for a fresh DOM walk.
        const tc = blockInfo.element.textContent || "";
        if (!tc.includes(phrase)) continue;
        if (highlightPhraseInBlock(blockInfo, phrase, match)) {
          highlights++;
        }
      }
    }
    return highlights;
  }

  // ---------------------------------------------------------------------
  // Floating panel — relationship graph

  let panelEl = null;

  function ensurePanel() {
    if (panelEl) return panelEl;
    panelEl = document.createElement("div");
    panelEl.className = "ocoi-panel";
    panelEl.dir = "rtl";
    panelEl.setAttribute("data-ocoi-panel", "1");
    panelEl.innerHTML = `
      <div class="ocoi-panel-header">
        <div class="ocoi-panel-title"></div>
        <button class="ocoi-panel-close" aria-label="סגירה">✕</button>
      </div>
      <div class="ocoi-panel-body"></div>
      <div class="ocoi-panel-footer">
        <a class="ocoi-panel-link" target="_blank" rel="noopener noreferrer">פתיחה ב-OCOI ↗</a>
      </div>
    `;
    document.documentElement.appendChild(panelEl);

    panelEl.querySelector(".ocoi-panel-close").addEventListener("click", () => {
      panelEl.classList.remove("ocoi-panel--open");
    });

    makeDraggable(panelEl, panelEl.querySelector(".ocoi-panel-header"));
    return panelEl;
  }

  function makeDraggable(el, handle) {
    let startX = 0, startY = 0, baseLeft = 0, baseTop = 0, dragging = false;
    handle.addEventListener("mousedown", (e) => {
      if (e.target.closest(".ocoi-panel-close")) return;
      dragging = true;
      startX = e.clientX;
      startY = e.clientY;
      const rect = el.getBoundingClientRect();
      baseLeft = rect.left;
      baseTop = rect.top;
      el.style.left = `${baseLeft}px`;
      el.style.top = `${baseTop}px`;
      el.style.right = "auto";
      el.style.bottom = "auto";
      e.preventDefault();
    });
    window.addEventListener("mousemove", (e) => {
      if (!dragging) return;
      el.style.left = `${baseLeft + (e.clientX - startX)}px`;
      el.style.top = `${baseTop + (e.clientY - startY)}px`;
    });
    window.addEventListener("mouseup", () => { dragging = false; });
  }

  // ניגוד עניינים לעם is served from גרסאות לעם now. Its old per-type routes
  // (/persons/<id>, /companies/<id>, …) do not exist there — the whole project
  // is one page whose focus lives in the query string.
  function entityPagePath(type, id) {
    if (!type || !id) return "/projects/ocoi";
    return `/projects/ocoi?tab=graph&type=${encodeURIComponent(type)}` +
           `&id=${encodeURIComponent(id)}`;
  }

  function ocoiOrigin() {
    return "https://www.over.org.il";
  }

  function typeLabel(type) {
    return ({
      person: "אישיות ציבורית",
      company: "חברה",
      association: "עמותה",
      domain: "תחום",
    })[type] || type;
  }

  async function showPanel(match) {
    const panel = ensurePanel();
    panel.classList.add("ocoi-panel--open");
    panel.querySelector(".ocoi-panel-title").textContent =
      `${match.name} · ${typeLabel(match.type)}`;
    const link = panel.querySelector(".ocoi-panel-link");
    link.href = `${ocoiOrigin()}${entityPagePath(match.type, match.id)}`;
    const body = panel.querySelector(".ocoi-panel-body");
    body.innerHTML = "";
    const loading = document.createElement("div");
    loading.className = "ocoi-loading";
    loading.textContent = "טוען קשרים…";
    body.appendChild(loading);

    // Elapsed-time ticker — appended to the loading text once per second
    // so the user sees progress instead of suspecting a hang during long
    // server-side graph queries (depth=1 traversals can take 5-10s).
    const startedAt = Date.now();
    const ticker = setInterval(() => {
      const sec = Math.floor((Date.now() - startedAt) / 1000);
      if (sec >= 2 && body.firstChild === loading) {
        loading.textContent = `טוען קשרים… (${sec}s)`;
      }
    }, 500);

    const resp = await rpc({
      type: "ocoi.neighbors",
      entityId: match.id,
      entityType: match.type,
      depth: 1,
    });
    clearInterval(ticker);

    if (!resp || resp.status !== "ok" || !resp.data) {
      // Build the error/retry UI from DOM nodes (not innerHTML) so we can
      // safely interpolate the error message without HTML injection.
      body.innerHTML = "";
      const wrap = document.createElement("div");
      wrap.className = "ocoi-empty";
      const main = document.createElement("div");
      main.textContent = resp && resp.error
        ? `שגיאה בטעינה: ${resp.error}`
        : "אין נתונים זמינים.";
      const retry = document.createElement("button");
      retry.className = "ocoi-retry";
      retry.type = "button";
      retry.textContent = "נסה שוב";
      retry.addEventListener("click", () => showPanel(match));
      wrap.appendChild(main);
      wrap.appendChild(retry);
      body.appendChild(wrap);
      if (DEBUG) {
        console.log("[OCOI] neighbors failed for", match, "→", resp);
      }
      return;
    }
    renderGraph(body, match, resp.data);
  }

  // Pure-SVG radial graph: center node + N spokes around it. Edge labels on
  // hover so the panel stays readable even with many neighbors.
  function renderGraph(container, center, subgraph) {
    const nodes = (subgraph.nodes || []).filter(
      (n) => !(n.id === center.id && n.entity_type === center.type)
    );
    const edges = subgraph.edges || [];

    if (nodes.length === 0) {
      container.innerHTML = `<div class="ocoi-empty">לא נמצאו קשרים ישירים.</div>`;
      return;
    }

    const W = 360, H = 320, cx = W / 2, cy = H / 2, r = 120;
    const svgNS = "http://www.w3.org/2000/svg";
    const svg = document.createElementNS(svgNS, "svg");
    svg.setAttribute("viewBox", `0 0 ${W} ${H}`);
    svg.setAttribute("width", "100%");
    svg.setAttribute("height", H);
    svg.classList.add("ocoi-svg");

    const positions = new Map();
    positions.set(`${center.type}|${center.id}`, { x: cx, y: cy });

    nodes.forEach((node, i) => {
      const angle = (i / nodes.length) * Math.PI * 2 - Math.PI / 2;
      positions.set(`${node.entity_type}|${node.id}`, {
        x: cx + r * Math.cos(angle),
        y: cy + r * Math.sin(angle),
      });
    });

    // Edges first (so they sit behind the nodes)
    for (const e of edges) {
      const from = positions.get(`${e.source_type}|${e.source_id}`);
      const to = positions.get(`${e.target_type}|${e.target_id}`);
      if (!from || !to) continue;
      const line = document.createElementNS(svgNS, "line");
      line.setAttribute("x1", from.x);
      line.setAttribute("y1", from.y);
      line.setAttribute("x2", to.x);
      line.setAttribute("y2", to.y);
      line.setAttribute("class", "ocoi-edge");
      const tt = e.relationship_type || e.type || "";
      if (tt) {
        const title = document.createElementNS(svgNS, "title");
        title.textContent = tt;
        line.appendChild(title);
      }
      svg.appendChild(line);
    }

    function drawNode(name, type, id, x, y, isCenter) {
      const g = document.createElementNS(svgNS, "g");
      g.setAttribute("class", `ocoi-node ocoi-node--${type}${isCenter ? " ocoi-node--center" : ""}`);
      g.setAttribute("transform", `translate(${x}, ${y})`);

      const circle = document.createElementNS(svgNS, "circle");
      circle.setAttribute("r", isCenter ? 18 : 12);
      g.appendChild(circle);

      const text = document.createElementNS(svgNS, "text");
      text.setAttribute("y", isCenter ? 34 : 26);
      text.setAttribute("text-anchor", "middle");
      text.textContent = name && name.length > 22 ? name.slice(0, 21) + "…" : name || "";
      g.appendChild(text);

      const title = document.createElementNS(svgNS, "title");
      title.textContent = name || "";
      g.appendChild(title);

      if (!isCenter) {
        g.style.cursor = "pointer";
        g.addEventListener("click", () => {
          showPanel({ id, type, name });
        });
      }
      svg.appendChild(g);
    }

    drawNode(center.name, center.type, center.id, cx, cy, true);
    for (const node of nodes) {
      const pos = positions.get(`${node.entity_type}|${node.id}`);
      if (!pos) continue;
      drawNode(node.name, node.entity_type, node.id, pos.x, pos.y, false);
    }

    container.innerHTML = "";
    container.appendChild(svg);

    const legend = document.createElement("div");
    legend.className = "ocoi-legend";
    legend.textContent = `${nodes.length} קשרים מדרגה 1`;
    container.appendChild(legend);
  }

  // ---------------------------------------------------------------------
  // Click handler — delegated, so newly added .ocoi-hit nodes work too.

  document.addEventListener("click", (e) => {
    const hit = e.target.closest && e.target.closest(".ocoi-hit");
    if (!hit) return;
    e.preventDefault();
    e.stopPropagation();
    showPanel({
      id: hit.dataset.ocoiId,
      type: hit.dataset.ocoiType,
      name: hit.dataset.ocoiName || hit.textContent,
    });
  }, true);

  // ---------------------------------------------------------------------
  // Public scan entry point — invoked from the popup or the context menu.

  async function scanPage() {
    const settings = await rpc({ type: "ocoi.settings" });
    if (!settings || settings.enabled === false) {
      return { scanned: 0, matched: 0, highlighted: 0 };
    }

    const { blocks, candidates, totalLeafBlocks } = collectCandidates();
    const limited = candidates.slice(0, MAX_CANDIDATES_PER_SCAN);

    if (DEBUG) {
      console.group("[OCOI] scan diagnostics");
      console.log("api base:", settings.apiBase);
      console.log("leafBlocks total =", totalLeafBlocks, "  blocks with Hebrew text =", blocks.length);
      console.log("unique candidates =", candidates.length, "  truncated to =", limited.length);
      console.log("first 12 candidates:", limited.slice(0, 12));
      console.groupEnd();
    }

    rpc({ type: "ocoi.badge", text: "…", color: "#e91e63" });

    if (DEBUG) {
      console.log("[OCOI] starting API loop:", limited.length, "queries, concurrency =", SCAN_CONCURRENCY);
    }

    const scanStart = Date.now();
    const matches = new Map();
    const failureSamples = [];
    const errorCounts = Object.create(null);
    let okCount = 0;
    let processed = 0;
    let lastBadgeTick = 0;
    let lastProgressLog = 0;
    await runWithConcurrency(
      limited,
      async (cand) => {
        processed++;
        const now = Date.now();
        if (now - lastBadgeTick > 400) {
          lastBadgeTick = now;
          rpc({ type: "ocoi.badge", text: String(matches.size || "."), color: "#e91e63" });
        }
        // Periodic progress log so the user can see scanning isn't hung.
        if (DEBUG && processed - lastProgressLog >= 50) {
          lastProgressLog = processed;
          const elapsed = ((now - scanStart) / 1000).toFixed(1);
          console.log("[OCOI] progress: %d/%d processed, %d matches, %ds elapsed",
            processed, limited.length, matches.size, elapsed);
        }
        if (candidateMap.has(cand)) {
          const cached = candidateMap.get(cand);
          if (cached) matches.set(cand, cached);
          return;
        }
        const resp = await rpc({ type: "ocoi.search", q: cand });
        let m = null;
        if (resp && resp.status === "ok") {
          okCount++;
          m = pickBestMatch(cand, resp.data || []);
        } else {
          // Bucket the failure reason so we can summarise at the end.
          const reason = (resp && resp.error) || "unknown";
          errorCounts[reason] = (errorCounts[reason] || 0) + 1;
          if (failureSamples.length < 3) {
            failureSamples.push({ cand, resp });
          }
        }
        candidateMap.set(cand, m);
        if (m) matches.set(cand, m);
      },
      SCAN_CONCURRENCY
    );

    if (DEBUG) {
      const failed = limited.length - okCount;
      const elapsed = ((Date.now() - scanStart) / 1000).toFixed(1);
      console.group("[OCOI] scan results — total " + elapsed + "s");
      console.log("queries OK =", okCount, "  queries failed =", failed);
      if (failed > 0) {
        console.log("error breakdown:", errorCounts);
        console.log("first failure samples:", failureSamples);
      }
      console.log("matches =", matches.size,
        [...matches.entries()].map(([k, v]) => ({ q: k, name: v?.name, type: v?.type }))
      );
      console.groupEnd();
    }

    let highlighted = 0;
    if (matches.size > 0) {
      highlighted = applyHighlights(blocks, matches);
    }
    if (DEBUG) {
      console.log("[OCOI] highlighted spans:", highlighted);
    }

    // Final badge — turns green so the user knows scanning is done, with
    // the count of distinct matched entities. Cleared on next navigation.
    rpc({
      type: "ocoi.badge",
      text: String(matches.size),
      color: matches.size > 0 ? "#10b981" : "#9ca3af",
    });

    // Pre-warm the neighbor cache for the matched entities. Fire-and-forget
    // — the worker will fetch them serially and stash them in
    // chrome.storage.local, so when the user clicks an underlined name the
    // panel opens instantly instead of round-tripping through Cloudflare.
    if (matches.size > 0) {
      const entities = [...matches.values()].slice(0, 8).map((m) => ({
        id: m.id,
        type: m.type,
      }));
      rpc({ type: "ocoi.prewarm", entities }, 5000);
    }

    return {
      scanned: limited.length,
      matched: matches.size,
      highlighted,
      blocks: blocks.length,
    };
  }

  async function lookupSelection(text) {
    const cand = (text || "").trim();
    if (!cand) return;
    const match = await lookupCandidate(cand);
    if (match) {
      showPanel(match);
    } else {
      // Surface "not found" as a brief panel state so the user gets feedback.
      const panel = ensurePanel();
      panel.classList.add("ocoi-panel--open");
      panel.querySelector(".ocoi-panel-title").textContent = cand;
      panel.querySelector(".ocoi-panel-link").href =
        `${ocoiOrigin()}/projects/ocoi?q=${encodeURIComponent(cand)}`;
      // textContent (not innerHTML) — `cand` is user-supplied page text and
      // could otherwise carry HTML markup, so we never let it parse.
      const body = panel.querySelector(".ocoi-panel-body");
      body.innerHTML = "";
      const empty = document.createElement("div");
      empty.className = "ocoi-empty";
      empty.textContent = `לא זוהתה ישות תואמת ב-OCOI עבור "${cand}".`;
      body.appendChild(empty);
    }
  }

  chrome.runtime.onMessage.addListener((msg, _sender, sendResponse) => {
    if (!msg || typeof msg !== "object") return false;
    if (msg.type === "ocoi.scan") {
      scanPage().then((r) => sendResponse({ status: "ok", ...r }));
      return true;
    }
    if (msg.type === "ocoi.lookup-selection") {
      lookupSelection(msg.text).then(() => sendResponse({ status: "ok" }));
      return true;
    }
    return false;
  });

  // No auto-scan: the script is injected on-demand by the service worker
  // when the user clicks the toolbar button or right-clicks selected text,
  // so simply being loaded already implies the user asked for action.
})();
