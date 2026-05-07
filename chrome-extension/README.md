# OCOI Chrome Extension — מיפוי קשרים

A Chrome extension (Manifest V3) that detects names of Israeli public
officials, companies, and associations on any web page, queries the public
[OCOI](https://ocoi.org.il) API, and shows a floating relationship map for
matched entities.

---

## התקנה מקומית בכרום (ללא חנות) — מדריך מהיר

התוסף תומך באופן מלא ב־**Load unpacked** — אין צורך בבילד, ב-npm, או באריזה
ל-ZIP. כל הקבצים מוכנים לטעינה ישירה.

1. פתחו את כרום וגלשו אל `chrome://extensions`.
2. הפעילו את **מצב מפתח** (Developer mode) — מתג בפינה הימנית-עליונה.
3. לחצו על **Load unpacked** (טעינת חבילה לא ארוזה).
4. בחרו את התיקייה `chrome-extension/` (זו שמכילה את `manifest.json`).
5. נעצו את הסמל הוורוד של "ניגוד עניינים לעם" לסרגל הכלים.
6. גלשו לאתר חדשותי בעברית (ynet, גלובס, TheMarker, כלכליסט וכו'),
   לחצו על הסמל בסרגל הכלים, ולחצו **"סריקת הדף הנוכחי"**.

תוך שניות ספורות שמות של אישי ציבור, חברות ועמותות שמופיעים ב-OCOI יסומנו
בקו תחתי ורוד מקווקו. לחיצה על שם תפתח חלון צף עם גרף הקשרים שלו.

### ברירת מחדל מול שרת מקומי

ברירת המחדל היא לפנות ל-`https://ocoi.org.il/api/v1`. אם אתם מריצים את
ה-API מקומית (`uv run ocoi-api` שמאזין על `localhost:8000`), פתחו את הפופאפ
של התוסף ושנו את שדה **"כתובת API"** ל:

```
http://localhost:8000/api/v1
```

`host_permissions` כבר כולל את `localhost:8000`, אז אין צורך בעריכה
נוספת — רק לשמור ולסרוק.

### עדכון התוסף אחרי שינוי קוד

אחרי כל עריכה של קובץ ב-`chrome-extension/`:
1. חזרו ל-`chrome://extensions`.
2. לחצו על אייקון העדכון 🔄 בכרטיס של התוסף.
3. רעננו את הדף שאתם בודקים עליו (Ctrl/Cmd + R).

זה מספיק. אין צורך ב"Remove" ואז "Load unpacked" מחדש.

### אריזה ל-ZIP (אופציונלי, להעברה למחשב אחר)

אם תרצו להעביר את התוסף הארוז לחבר שיוכל גם הוא לטעון אותו ב-Load unpacked,
הריצו מתוך תיקיית הריפו:

```bash
node chrome-extension/pack.js
```

יוצר `chrome-extension/dist/ocoi-extension-v0.1.0.zip` (ללא `dist/` עצמו, ללא
`pack.js`, ללא `README.md`). הנמען פותח את ה-ZIP, גורר את התיקייה הפרושה
ל-`chrome://extensions` עם Developer mode מופעל — זה הכל.

### בעיות נפוצות

| תסמין | פתרון |
|-------|-------|
| הסמל אפור ולחיצה על "סריקה" מציגה "הסקריפט אינו פעיל" | רעננו את הדף לאחר התקנת התוסף — content script נטען רק על דפים שנפתחו אחרי ההתקנה |
| "סורק…" נתקע | פתחו DevTools (F12) → Console; אם יש שגיאת CORS, ודאו שכתובת ה-API בפופאפ זהה ל-`host_permissions` ב-`manifest.json` |
| ההיילייטים לא מופיעים | הדף לא מכיל ביטויים של 2–5 מילים בעברית טהורה, או שהשמות שבדף לא קיימים ב-OCOI. נסו אתר חדשות פוליטי |
| `__MSG_extName__` מופיע במקום השם בעברית | המבנה של `_locales/` לא נשמר. ודאו ששתי תיקיות-המשנה `he` ו-`en` קיימות עם `messages.json` בכל אחת |


## How it works

1. The user opens a Hebrew web page (news article, government page, …).
2. They click the toolbar icon → **"סריקת הדף הנוכחי"** (or enable
   auto-scan in the popup).
3. The content script extracts multi-word Hebrew name candidates from the
   visible text and queries `GET /api/v1/search?q=…` for each one through
   the background service worker.
4. Strong matches (the API result name aligns with the candidate text) get
   wrapped in `<span class="ocoi-hit">` with a subtle pink dotted underline.
5. Clicking a highlight pulls `GET /api/v1/graph/neighbors/{id}?type=…&depth=1`
   and renders a radial relationship graph in a draggable floating panel.
   Each non-center node is itself clickable, so the user can walk the graph
   without leaving the page.
6. Right-click → **"OCOI"** on selected text triggers the same lookup
   without scanning the rest of the page.

All API calls go through the background worker, so no foreign page ever
talks to ocoi.org.il directly. Results are cached in memory for 10 minutes
per browser session.

## API endpoints used

| Endpoint | Purpose |
|----------|---------|
| `GET /api/v1/search?q={text}&limit=5` | Find candidate entities for a phrase |
| `GET /api/v1/graph/neighbors/{id}?type={type}&depth=1` | First-degree neighborhood for a matched entity |

By default the extension hits `https://ocoi.org.il/api/v1`. To point it at
a local dev server, change the API base in the popup to
`http://localhost:8000/api/v1`.

## Install (unpacked, for development)

1. Open Chrome → `chrome://extensions`
2. Toggle **Developer mode** (top right)
3. Click **Load unpacked** → select the `chrome-extension/` folder
4. Pin the OCOI icon to the toolbar
5. Open any page (try a news article on `ynet.co.il`, `themarker.com`,
   `globes.co.il`) and click the icon → **סריקת הדף הנוכחי**

## File layout

```
chrome-extension/
├── manifest.json
├── _locales/{he,en}/messages.json
├── icons/icon-{16,48,128}.png
├── background/service-worker.js     # API proxy, context menu, cache
├── content/
│   ├── content-script.js            # candidate extraction, highlighting, graph
│   └── content-style.css
└── popup/
    ├── popup.html
    ├── popup.js
    └── popup.css
```

## Notes

- The shipped icons are flat brand-pink squares. Replace
  `icons/icon-{16,48,128}.png` with the project logo before publishing.
- `host_permissions` is restricted to `ocoi.org.il` (and `localhost:8000`
  for development). The extension never reads or sends data to any other
  origin.
- The candidate extractor is intentionally conservative: it only considers
  pure-Hebrew phrases of 2–5 words. This keeps API load light on long
  pages — at most 250 candidates per scan with a concurrency of 4.
