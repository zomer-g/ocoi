# Notes for Chrome Web Store Reviewers

Paste the **Quick summary** + **Test instructions** sections into the
"Notes for reviewers" box on the dashboard.

---

## Quick summary

This extension highlights names of Israeli public officials, companies,
and associations on the current page that exist in OCOI (ocoi.org.il),
the public conflict-of-interest transparency dataset, and shows their
relationship map in a floating panel.

The extension is fully on-demand:
- No `content_scripts` entry in the manifest — nothing auto-injects.
- The content script is injected via `chrome.scripting.executeScript`
  only after a user gesture (toolbar click → "Scan this page", or
  right-click selected text → "Look up on OCOI").
- Combined with `activeTab`, this means the extension cannot read any
  page the user did not explicitly act on.

No build step. The repository contents under `chrome-extension/` are
exactly what is in the submitted zip.

---

## Architecture / Data flow

```
   user gesture                              OCOI public API
        │                                          ▲
        ▼                                          │
   popup.html  ──┐                                 │
                 │  chrome.runtime.sendMessage     │
                 ▼                                 │
   service-worker.js ──── chrome.scripting ───── HTTPS fetch
        │                                          │
        │  chrome.tabs.sendMessage                 │
        ▼                                          │
   content-script.js  ── extracts n-grams ─────────┘
        │
        ▼
   floating panel (SVG graph, drawn from /graph/neighbors response)
```

Every fetch to `ocoi.org.il` originates in the **service worker**,
never in the content script. So the host page never sees the network
request and there is no per-site CORS handshake.

---

## Key source files

| File | Role |
|------|------|
| `manifest.json` | MV3 declaration. No static content_scripts. |
| `background/service-worker.js` | Owns API fetches, in-memory cache (10 min TTL), context menu, on-demand `chrome.scripting.executeScript`. |
| `content/content-script.js` | Extracts Hebrew n-grams (2–4 words) from visible text, asks the worker to query the API, wraps strong matches in `<span class="ocoi-hit">`, renders the floating panel. |
| `content/content-style.css` | Styles for the underline + the floating panel. |
| `popup/popup.{html,js,css}` | Toolbar popup — "Scan this page" button, enable toggle, API base override. |
| `_locales/{he,en}/messages.json` | Localized name + description + context-menu label. |

---

## Where data leaves the browser (exhaustive list)

All outbound fetches originate in `background/service-worker.js`:

1. `apiSearch()` → `${apiBase}/search?q={candidate}&limit=5`
2. `apiNeighbors()` → `${apiBase}/graph/neighbors/{id}?type={type}&depth={depth}`

`apiBase` defaults to `https://www.ocoi.org.il/api/v1` and is the only
remote origin the extension is allowed to contact (per
`host_permissions`). All requests use `credentials: "omit"`.

There are no other `fetch`, `XMLHttpRequest`, `WebSocket`,
`navigator.sendBeacon`, or `<img src=…>` calls anywhere in the codebase.

---

## No remote code

- No `eval()`, `new Function()`, `setTimeout(string)`, or
  `setInterval(string)`.
- No `<script src="https://…">` — every script is a local file.
- No `chrome.scripting.executeScript({ code: … })` — only
  `{ files: ["content/content-script.js"] }`.
- No code obfuscation. The submitted zip mirrors the repository.

---

## Test instructions (no login required)

1. Load unpacked from the unzipped folder, or load the submitted zip.
2. Pin the extension's icon to the toolbar.
3. Open one of these public Israeli news pages (Hebrew content):
   - https://www.themarker.com/
   - https://www.globes.co.il/
   - https://www.calcalist.co.il/
   - https://www.ynet.co.il/
4. Click the OCOI extension icon → click **"סריקת הדף הנוכחי"**
   (Scan this page) in the popup.
5. Within a few seconds, names of Israeli public officials, companies,
   or associations that appear in OCOI's public dataset will be
   underlined with a dotted pink line.
6. Click any underlined name. A draggable panel opens with the entity
   in the center and its first-degree relationships drawn in SVG.
7. Alternative path: select arbitrary Hebrew text, right-click,
   choose **"חיפוש ב-OCOI"** (Look up on OCOI) from the context menu.

Sample names known to exist in OCOI for direct verification:
- "בנימין נתניהו" (person)
- "אור ירוק" (association)
- "טבע תעשיות פרמצבטיות" (company, partial match)

---

## Permissions requested (rationale recap)

| Permission | Why |
|------------|-----|
| `activeTab` | Read text of the page the user explicitly scanned. |
| `scripting` | Inject the content script in response to user gesture. |
| `storage` | Save the API base override and the on/off toggle. |
| `contextMenus` | Add a single "Look up on OCOI" entry on text selection. |
| `host_permissions: https://www.ocoi.org.il/*, https://ocoi.org.il/*` | Talk to the public OCOI API and follow its 301 redirect. |
| `host_permissions: http://localhost:8000/*` | Developer testing only — pointing at a local OCOI instance. |

There is no `<all_urls>` permission, no `tabs` permission, no
`webRequest`, no `cookies`, no `unlimitedStorage`.

---

## Known limitations the reviewer may notice

- Hebrew name detection is based on string matching (n-grams) against the
  OCOI search endpoint. Strong matches (≥70% length-normalised overlap)
  are highlighted; weaker matches are dropped. Some edge cases may still
  highlight a coincidental match — the floating panel always shows the
  entity type and a link to the full record on ocoi.org.il for verification.
- The extension does not work on pages where Chrome blocks injection
  (chrome://, chromewebstore.google.com, the new-tab page). The popup
  surfaces this state to the user.
