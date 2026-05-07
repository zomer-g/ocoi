const DEFAULTS = {
  apiBase: "https://www.ocoi.org.il/api/v1",
  enabled: true,
};

const $ = (sel) => document.querySelector(sel);

async function load() {
  const s = await chrome.storage.local.get(DEFAULTS);
  $("#enabled").checked = s.enabled !== false;
  $("#apiBase").value = s.apiBase || DEFAULTS.apiBase;
}

async function save(patch) {
  await chrome.storage.local.set(patch);
}

async function activeTab() {
  const [tab] = await chrome.tabs.query({ active: true, currentWindow: true });
  return tab;
}

function setStatus(text) {
  $("#status").textContent = text || "";
}

function isInjectable(url) {
  if (!url) return false;
  if (url.startsWith("chrome://")) return false;
  if (url.startsWith("chrome-extension://")) return false;
  if (url.startsWith("edge://")) return false;
  if (url.startsWith("about:")) return false;
  if (url.startsWith("https://chrome.google.com/webstore")) return false;
  if (url.startsWith("https://chromewebstore.google.com")) return false;
  return true;
}

$("#scan").addEventListener("click", async () => {
  const tab = await activeTab();
  if (!tab?.id) return;
  if (!isInjectable(tab.url)) {
    setStatus("התוסף לא יכול לרוץ על דף זה (chrome://, חנות התוספים).");
    return;
  }
  const btn = $("#scan");
  btn.disabled = true;
  setStatus("מטעין…");

  try {
    // Inject (idempotent — content script guards on window.__ocoiInjected).
    const inj = await chrome.runtime.sendMessage({
      type: "ocoi.inject",
      tabId: tab.id,
    });
    if (!inj || inj.status !== "ok") {
      setStatus("הזרקת הסקריפט נכשלה. נסו לרענן את הדף.");
      return;
    }

    // Fire-and-forget: scanning a long Hebrew page can take 5-15 seconds,
    // and Chrome closes the message channel as soon as the popup loses
    // focus (which it usually does within a couple of seconds). So we
    // *intentionally* don't await sendMessage's response — the content
    // script will finish on its own and update the page DOM with the
    // highlights. We just notify the user to close the popup and watch.
    chrome.tabs
      .sendMessage(tab.id, { type: "ocoi.scan" })
      .then((resp) => {
        if (resp && resp.status === "ok") {
          const matched = resp.matched ?? 0;
          const highlighted = resp.highlighted ?? 0;
          setStatus(`${matched} התאמות, ${highlighted} סומנו.`);
        }
      })
      .catch(() => {
        // Popup probably closed before scan finished — that's fine, the
        // content script keeps running in the page.
      });

    setStatus("סריקה החלה. ניתן לסגור — סימונים יופיעו בדף תוך כמה שניות.");
  } catch (e) {
    setStatus(`שגיאה: ${e.message || e}`);
  } finally {
    btn.disabled = false;
  }
});

$("#enabled").addEventListener("change", (e) => {
  save({ enabled: e.target.checked });
});

$("#save").addEventListener("click", async () => {
  const v = $("#apiBase").value.trim() || DEFAULTS.apiBase;
  await save({ apiBase: v.replace(/\/+$/, "") });
  setStatus("נשמר.");
  setTimeout(() => setStatus(""), 1500);
});

load();
