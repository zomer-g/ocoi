# Chrome Web Store Dashboard — טקסטים להעתקה

מסמך זה הוא מקור-האמת היחיד לכל הטקסטים שצריך להזין בטופס ה-CWS. סדר
הסעיפים תואם את הטופס בדאשבורד; אפשר להעתיק כל סעיף כמו שהוא.

---

## URLs for Dashboard

```
Privacy Policy URL:  https://www.z-g.co.il/ocoi-extension-privacy
Homepage URL:        https://www.z-g.co.il/ocoi-extension
Support URL:         mailto:guy@z-g.co.il
```

> שלוש הכתובות הראשונות חייבות להחזיר HTTP 200 ציבורי (ללא התחברות) לפני
> הגשה. ראו `WEBSITE_PAGES.md` לתוכן שיש להעלות לאתר z-g.co.il.

---

## Single Purpose Description

```
The extension marks names of Israeli public officials, companies, and
associations on the current web page that have a published record on
OCOI (ocoi.org.il), and shows their relationship map in a floating
panel — so the reader can verify potential conflicts of interest
without leaving the page.
```

---

## Permission Justifications

### `activeTab`
```
Required so the extension can read the visible text of the page the user
explicitly chose to scan, by clicking the toolbar icon. Without it, the
content script cannot extract Hebrew name candidates from the page.
```

### `scripting`
```
Used to inject the content script (content/content-script.js) and stylesheet
(content/content-style.css) into the active tab on demand, when the user
clicks the toolbar icon or selects "Look up on OCOI" from the right-click
menu. There is no static content_scripts entry — every injection is a
direct response to a user gesture.
```

### `storage`
```
Stores the user's two preferences locally: (1) the API base URL — so a
developer can point the extension at a local OCOI server during testing —
and (2) an "enabled" toggle. No PII or browsing history is stored.
```

### `contextMenus`
```
Adds a single right-click menu item ("Look up on OCOI") on selected text
so the user can trigger an ad-hoc lookup without scanning the whole page.
The menu only appears in the "selection" context.
```

### Host permission `https://www.ocoi.org.il/*`
```
The extension's only data source. The background service worker calls
GET /api/v1/search and GET /api/v1/graph/neighbors to look up entities
and fetch their relationship subgraph. Limiting the host permission to
the OCOI domain means the extension cannot fetch from any other origin.
```

### Host permission `https://ocoi.org.il/*`
```
Identical purpose; covers the apex domain because ocoi.org.il currently
issues a 301 redirect to www.ocoi.org.il, and Chrome requires the redirect
target to be in host_permissions for the fetch to follow.
```

### Host permission `http://localhost:8000/*`
```
Allows the extension to be pointed at a local OCOI development server
during testing only. End users do not interact with this URL.
```

---

## Data Usage Declarations

| Category | Collected? | Notes |
|----------|-----------|-------|
| Personally identifiable information | No | The extension does not collect names, emails, addresses, ages, or any other identifier of the user. |
| Health information | No | |
| Financial and payment information | No | |
| Authentication information | No | |
| Personal communications | No | |
| Location | No | |
| Web history | No | |
| User activity | No | The extension does not track clicks, scroll, page views, or behavioral data. |
| Website content | **Yes — limited** | When the user clicks the toolbar icon, the extension reads the visible Hebrew text of the active tab to extract multi-word name candidates. Each candidate phrase (typically 2–4 Hebrew words) is sent to `https://www.ocoi.org.il/api/v1/search` strictly to perform the lookup. No raw page text, URLs, titles, screenshots, or metadata are sent. Nothing is persisted. |

### Per "Website content" justification
```
Used for: implementing the extension's single purpose — matching names on
the page against OCOI's public dataset and showing their relationship map.

Transferred to: ocoi.org.il only. Not shared with any third party.

Not sold. Not used for advertising. Not used for assessing creditworthiness
or for lending purposes. Not used for any purpose unrelated to the
extension's single purpose.
```

---

## Three Certification Checkboxes

1. ☑ I do not sell or transfer user data to third parties, apart from the
   approved use cases described above.
2. ☑ I do not use or transfer user data for purposes unrelated to my
   item's single purpose.
3. ☑ I do not use or transfer user data to determine creditworthiness or
   for lending purposes.
