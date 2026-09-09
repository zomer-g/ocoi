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

**Render.** `render.yaml` at the repo root no longer declares the Docker web
service or the Postgres database — it declares this folder as a Static Site,
which has no instance and no database and therefore costs nothing to keep up.

Render cannot change an existing service's runtime, so the static site is a NEW
service (`ocoi-static`) and the cut-over is done by hand, in this order:

1. Sync the blueprint (Render dashboard → the Blueprint → Sync). `ocoi-static`
   is created; the old `ocoi` service and `ocoi-db` keep running untouched —
   removing a resource from a blueprint does not delete it.
2. Check `ocoi-static.onrender.com` serves the landing page.
3. Move the `ocoi.org.il` and `www.ocoi.org.il` custom domains off `ocoi` and
   onto `ocoi-static`. (This is why the blueprint does not declare `domains:` —
   claiming a domain the old service still holds would fail the sync.)
4. Confirm the checks below against the real domain.
5. Only now, and only once the data is safely in over.org.il, delete the `ocoi`
   web service and the `ocoi-db` database in the dashboard. That deletion is
   what actually stops the billing, and it is irreversible.

The path-shaped routes (`/persons/<id>`, `/documents`, `/api/v1/*`) are real
301s in the blueprint's `routes:`. The query-string routes are not, and cannot
be: their destination reshapes the parameters, so they stay HTML redirectors —
Render skips a rule when a real file exists at that path, so those pages keep
answering for themselves. `404.html` does the same path redirects in JS and
stays as the net underneath.

**Netlify / Cloudflare Pages:** publish this directory as-is — `_redirects` is
read natively and the path routes become real 301s there too.

## Checking it works

Open each of these and confirm you land on the right place:

    /document?id=<any real document id>
    /entity?type=person&id=<any real entity id>
    /search?q=כחלון
    /persons/<any real person id>
    /nonsense-path

The ids can be taken from the SQL console at <https://www.over.org.il/data>,
e.g. `SELECT id FROM ocoi.documents LIMIT 1`.
