"""Permission catalogue + URL-section → permission map.

A single source of truth for what each role / permission unlocks in the
admin panel. Two roles exist:

* ``admin`` — implicit access to everything, including user management.
* ``content_manager`` — has a per-user subset of the keys below.

The path-prefix map is consulted by the FastAPI dependency that gates
the admin router (see ``ocoi_api.auth``). Edit this file whenever you
add a new admin section.
"""

# ── Permission keys ──────────────────────────────────────────────────────

PERMISSION_LABELS_HE: dict[str, str] = {
    "view_dashboard":       "צפייה בלוח הבקרה",
    "manage_entities":      "ניהול ישויות",
    "manage_relationships": "ניהול קשרים",
    "manage_documents":     "ניהול מסמכים",
    "manage_import":        "ייבוא נתונים",
    "manage_registry":      "ניהול מרשמים",
    "manage_suggestions":   "ניהול הצעות תיקון",
    "manage_site_content":  "תוכן האתר",
}

ALL_PERMISSIONS: tuple[str, ...] = tuple(PERMISSION_LABELS_HE.keys())

# Reasonable defaults for a newly created content_manager. The admin can
# tighten or loosen this per user via the admin UI.
DEFAULT_CONTENT_MANAGER_PERMISSIONS: tuple[str, ...] = (
    "view_dashboard",
    "manage_entities",
    "manage_relationships",
    "manage_documents",
    "manage_suggestions",
    "manage_site_content",
)


# ── URL prefix → required permission ────────────────────────────────────
#
# Values:
#   None  → admin-only (content_managers always denied)
#   "..." → permission key from ALL_PERMISSIONS

SECTION_PERMISSIONS: dict[str, str | None] = {
    # admin-only
    "/api/v1/admin/users":           None,
    "/api/v1/admin/settings":        None,
    "/api/v1/admin/memory":          None,
    "/api/v1/admin/db-storage":      None,
    "/api/v1/admin/permissions":     None,
    "/api/v1/admin/mcp":             None,
    # dashboard
    "/api/v1/admin/stats":           "view_dashboard",
    # entities
    "/api/v1/admin/persons":         "manage_entities",
    "/api/v1/admin/companies":       "manage_entities",
    "/api/v1/admin/associations":    "manage_entities",
    "/api/v1/admin/domains":         "manage_entities",
    "/api/v1/admin/lookup":          "manage_entities",
    # relationships
    "/api/v1/admin/relationships":   "manage_relationships",
    # documents
    "/api/v1/admin/documents":       "manage_documents",
    # import
    "/api/v1/admin/import":          "manage_import",
    # registry
    "/api/v1/admin/registry":        "manage_registry",
    # suggestions
    "/api/v1/admin/suggestions":     "manage_suggestions",
    # site content
    "/api/v1/admin/site-content":    "manage_site_content",
}


def required_permission_for_path(path: str) -> tuple[bool, str | None]:
    """Return ``(admin_only, permission)`` for the given URL path.

    * ``admin_only=True, permission=None`` → only admins may call it.
    * ``admin_only=False, permission="..."`` → admins, or content managers
      that have this permission key.
    * Any unmapped path under ``/api/v1/admin`` falls back to admin-only
      so a brand-new endpoint is locked down until explicitly opened.
    """
    # Pick the longest prefix that matches, so ``/admin/users/{id}`` wins
    # over the broader catch-all.
    best_match: tuple[bool, str | None] | None = None
    best_len = -1
    for prefix, perm in SECTION_PERMISSIONS.items():
        if path.startswith(prefix) and len(prefix) > best_len:
            best_match = (perm is None, perm)
            best_len = len(prefix)
    if best_match is not None:
        return best_match
    # Default-deny for anything else under /admin
    if path.startswith("/api/v1/admin"):
        return True, None
    return False, None
