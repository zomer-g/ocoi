"""Classification of where an EntityRelationship row originated.

Every relationship row in the DB is tagged with one of these constants so
that the same table can host data from heterogeneous public sources without
losing the lineage. New sources can be added here.
"""

ORIGIN_COI_DECLARATION = "coi_declaration"
ORIGIN_MK_EXPENSE = "mk_expense"

ORIGIN_KINDS: tuple[str, ...] = (
    ORIGIN_COI_DECLARATION,
    ORIGIN_MK_EXPENSE,
)

ORIGIN_LABELS_HE: dict[str, str] = {
    ORIGIN_COI_DECLARATION: "הסדר למניעת ניגוד עניינים",
    ORIGIN_MK_EXPENSE: "הוצאות קשר עם הציבור",
}


def is_valid_origin(kind: str) -> bool:
    return kind in ORIGIN_KINDS
