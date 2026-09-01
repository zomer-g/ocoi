# ocoi.org.il — the static site that replaces the app

ניגוד עניינים לעם now lives inside גרסאות לעם at
<https://www.over.org.il/projects/ocoi>. This folder is what ocoi.org.il serves
after the Render web service and its database are shut down.

It is not just a "we moved" page. OCOI's credibility model was that every claim
links back to the document it came from, and its MCP server instructed models to
cite `https://www.ocoi.org.il/document?id=…` and `/entity?type=&id=`. Those URLs
are sitting in text nobody can go back and edit. If this domain answered 404 for
them, every citation the project ever produced would break at once.

So the site carries them across:

| old URL | goes to |
|---|---|
| `/document?id=X` | `over.org.il/projects/ocoi?tab=documents&doc=X` |
| `/entity?type=T&id=X` | `over.org.il/projects/ocoi?tab=graph&type=T&id=X` |
| `/search?q=Q` | `over.org.il/projects/ocoi?q=Q` |
| `/persons/X`, `/companies/X`, `/associations/X`, `/domains/X` | the same entity, on the graph |
| `/api/v1/*` | `over.org.il/api/ocoi/*` |
| anything else | the project page |

The query-string routes are redirected by a tiny script in
`document/index.html`, `entity/index.html` and `search/index.html`, because the
destination RESHAPES the parameters (`id` becomes `doc`, and a `tab` is added)
and a plain rewrite rule cannot express that. The path-shaped routes are in
`_redirects` for hosts that read it, and in `404.html` for hosts that do not —
so both kinds of host end up doing the right thing.

## Deploying it

**Render (a Static Site, replacing the current `ocoi` web service):**

1. New → Static Site, pointed at this repo.
2. Publish directory: `static-site`. Build command: leave empty.
3. Move the `ocoi.org.il` + `www.ocoi.org.il` custom domains onto it.
4. Only then delete the old `ocoi` web service and the `ocoi-db` database.

Render ignores `_redirects`; `404.html` covers the same routes there, and the
three HTML redirectors work everywhere.

**Netlify / Cloudflare Pages:** publish this directory as-is — `_redirects` is
read natively and the path routes become real 301s.

## Checking it works

Open each of these and confirm you land on the right place:

    /document?id=<any real document id>
    /entity?type=person&id=<any real entity id>
    /search?q=כחלון
    /persons/<any real person id>
    /nonsense-path

The ids can be taken from the SQL console at <https://www.over.org.il/data>,
e.g. `SELECT id FROM ocoi.documents LIMIT 1`.
