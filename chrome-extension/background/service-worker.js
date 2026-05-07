// OCOI extension — background service worker.
//
// Owns:
//  - The single context-menu entry that lets the user trigger a lookup on
//    arbitrary selected text.
//  - On-demand injection of the content script. We deliberately do NOT
//    auto-inject on every page (no static "content_scripts" entry in the
//    manifest). The extension only runs on a tab once the user has acted
//    on it (toolbar click → popup → "scan", or right-click selected text).
//    Combined with activeTab, this means the extension never reads a page
//    the user didn't ask it to.
//  - All fetches to the OCOI API. Doing them here means content scripts on
//    foreign pages never need to talk directly to ocoi.org.il, so we don't
//    have to negotiate per-site CORS and the user's page never sees the
//    request go out.
//  - A small in-memory cache keyed by query string and entity id, so opening
//    the same name on multiple pages in one browser session doesn't refetch.

const DEFAULTS = {
  apiBase: "https://www.ocoi.org.il/api/v1",
  enabled: true,
};

const SEARCH_TTL_MS = 10 * 60 * 1000;
const NEIGHBORS_TTL_MS = 10 * 60 * 1000;

const searchCache = new Map();
const neighborsCache = new Map();

async function getSettings() {
  const stored = await chrome.storage.local.get(DEFAULTS);
  return { ...DEFAULTS, ...stored };
}

function fromCache(map, key, ttl) {
  const entry = map.get(key);
  if (!entry) return null;
  if (Date.now() - entry.ts > ttl) {
    map.delete(key);
    return null;
  }
  return entry.data;
}

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

// One running flag for the whole worker, so when Cloudflare starts
// throwing interstitials we slow EVERY in-flight request down for a
// moment instead of each one independently retrying and amplifying the
// stampede. Reset itself after a short cooldown.
let cloudflareCooldownUntil = 0;

async function respectCooldown() {
  const wait = cloudflareCooldownUntil - Date.now();
  if (wait > 0) await sleep(wait);
}

function tripCooldown(ms) {
  cloudflareCooldownUntil = Math.max(cloudflareCooldownUntil, Date.now() + ms);
}

// Fetch wrapper that recognises Cloudflare's "challenge" (HTML body
// served at the API URL with HTTP 200 or 403/503), waits, and retries.
async function fetchJsonWithRetry(url, attempts = 4, perAttemptTimeoutMs = 25000) {
  let lastError = null;
  for (let i = 0; i < attempts; i++) {
    await respectCooldown();
    const ctrl = new AbortController();
    const timer = setTimeout(() => ctrl.abort(), perAttemptTimeoutMs);
    try {
      const res = await fetch(url, { credentials: "omit", signal: ctrl.signal });
      clearTimeout(timer);
      const ct = (res.headers.get("content-type") || "").toLowerCase();
      if (res.status === 429 || res.status === 503) {
        lastError = `HTTP ${res.status}`;
        tripCooldown(1200 + i * 800);
        continue;
      }
      if (!res.ok) {
        return { ok: false, error: `HTTP ${res.status}` };
      }
      if (!ct.includes("json")) {
        lastError = `non-json response (${ct || "no content-type"})`;
        tripCooldown(1500 + i * 700);
        continue;
      }
      const json = await res.json();
      return { ok: true, json };
    } catch (e) {
      clearTimeout(timer);
      lastError = (e && e.name === "AbortError") ? "fetch timeout" : String(e);
      await sleep(500 + i * 600);
    }
  }
  return { ok: false, error: lastError || "exhausted retries" };
}

async function apiSearch(q) {
  const { apiBase } = await getSettings();
  const trimmed = String(q || "").trim();
  if (!trimmed) return { status: "ok", data: [] };
  const key = `${apiBase}|${trimmed}`;
  const cached = fromCache(searchCache, key, SEARCH_TTL_MS);
  if (cached) return cached;

  const url = `${apiBase}/search?q=${encodeURIComponent(trimmed)}&limit=5`;
  const r = await fetchJsonWithRetry(url);
  if (!r.ok) {
    return { status: "error", error: r.error, data: [] };
  }
  searchCache.set(key, { ts: Date.now(), data: r.json });
  return r.json;
}

async function apiNeighbors(entityId, entityType, depth = 1) {
  const { apiBase } = await getSettings();
  const key = `${apiBase}|${entityType}|${entityId}|${depth}`;
  const cached = fromCache(neighborsCache, key, NEIGHBORS_TTL_MS);
  if (cached) return cached;

  const url = `${apiBase}/graph/neighbors/${entityId}?type=${encodeURIComponent(entityType)}&depth=${depth}`;
  const r = await fetchJsonWithRetry(url);
  if (!r.ok) {
    return { status: "error", error: r.error, data: null };
  }
  neighborsCache.set(key, { ts: Date.now(), data: r.json });
  return r.json;
}

async function ensureInjected(tabId) {
  // Idempotent: the content script guards itself with window.__ocoiInjected,
  // so re-running executeScript on a tab where it's already present is a
  // no-op. We rely on that rather than tracking state per tab.
  await chrome.scripting.insertCSS({
    target: { tabId },
    files: ["content/content-style.css"],
  }).catch(() => {});
  await chrome.scripting.executeScript({
    target: { tabId },
    files: ["content/content-script.js"],
  });
}

chrome.runtime.onInstalled.addListener(() => {
  chrome.contextMenus.create({
    id: "ocoi-lookup-selection",
    title: chrome.i18n.getMessage("ctxLookup") || "OCOI lookup",
    contexts: ["selection"],
  });
});

chrome.contextMenus.onClicked.addListener(async (info, tab) => {
  if (info.menuItemId !== "ocoi-lookup-selection" || !tab?.id) return;
  const text = (info.selectionText || "").trim();
  if (!text) return;
  try {
    await ensureInjected(tab.id);
    chrome.tabs.sendMessage(tab.id, { type: "ocoi.lookup-selection", text });
  } catch (e) {
    // Most likely cause: the page is a chrome:// URL or the web store, where
    // we can't inject. Nothing actionable for the user; swallow silently.
  }
});

chrome.runtime.onMessage.addListener((msg, _sender, sendResponse) => {
  if (!msg || typeof msg !== "object") return false;

  if (msg.type === "ocoi.search") {
    apiSearch(msg.q).then(sendResponse).catch((e) =>
      sendResponse({ status: "error", error: String(e), data: [] })
    );
    return true;
  }

  if (msg.type === "ocoi.neighbors") {
    apiNeighbors(msg.entityId, msg.entityType, msg.depth || 1)
      .then(sendResponse)
      .catch((e) =>
        sendResponse({ status: "error", error: String(e), data: null })
      );
    return true;
  }

  if (msg.type === "ocoi.settings") {
    getSettings().then(sendResponse);
    return true;
  }

  if (msg.type === "ocoi.inject") {
    const tabId = msg.tabId;
    if (!tabId) {
      sendResponse({ status: "error", error: "missing tabId" });
      return true;
    }
    ensureInjected(tabId)
      .then(() => sendResponse({ status: "ok" }))
      .catch((e) => sendResponse({ status: "error", error: String(e) }));
    return true;
  }

  // Toolbar badge updates from the content script — gives the user a
  // tangible signal that scanning is in progress / done, even after the
  // popup window has closed.
  if (msg.type === "ocoi.badge") {
    const tabId = _sender?.tab?.id;
    if (tabId) {
      const text = msg.text != null ? String(msg.text) : "";
      const color = msg.color || "#e91e63";
      chrome.action.setBadgeText({ tabId, text }).catch(() => {});
      chrome.action.setBadgeBackgroundColor({ tabId, color }).catch(() => {});
    }
    sendResponse({ status: "ok" });
    return true;
  }

  return false;
});
