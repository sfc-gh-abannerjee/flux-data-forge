"""
SQL sanitization utilities for Flux Data Forge.

Prevents SQL injection by validating identifiers and parameterizing values.
All Snowflake object names (databases, schemas, tables, tasks) must pass
through validate_identifier() before being interpolated into SQL strings.
"""

import re


# Snowflake identifier pattern: letters, digits, underscores, dollars.
# Optionally dot-separated for qualified names (DB.SCHEMA.TABLE).
_IDENTIFIER_PART = re.compile(r'^[A-Za-z_][A-Za-z0-9_$]*$')

# Maximum length for a single identifier part (Snowflake limit is 255)
_MAX_IDENT_LEN = 255


def validate_identifier(name: str, allow_qualified: bool = True) -> str:
    """
    Validate a Snowflake object identifier to prevent SQL injection.

    Args:
        name: The identifier string (e.g. "MY_DB", "DB.SCHEMA.TABLE")
        allow_qualified: If True, allows dot-separated qualified names

    Returns:
        The validated identifier string (unchanged if valid)

    Raises:
        ValueError: If the identifier contains invalid characters
    """
    if not name or not isinstance(name, str):
        raise ValueError("Identifier must be a non-empty string")

    name = name.strip()
    if not name:
        raise ValueError("Identifier must be a non-empty string")

    if allow_qualified:
        parts = name.split('.')
    else:
        parts = [name]

    if len(parts) > 3:
        raise ValueError(f"Too many qualifier levels: {name!r} (max 3: DB.SCHEMA.OBJECT)")

    for part in parts:
        if len(part) > _MAX_IDENT_LEN:
            raise ValueError(f"Identifier part too long: {len(part)} chars (max {_MAX_IDENT_LEN})")
        if not _IDENTIFIER_PART.match(part):
            raise ValueError(
                f"Invalid identifier: {part!r}. "
                "Must start with a letter or underscore and contain only "
                "letters, digits, underscores, or dollar signs."
            )

    return name


def sanitize_sql_value(value: str) -> str:
    """
    Escape a string value for safe use in SQL single-quoted literals.

    Doubles any single quotes to prevent SQL injection when the value
    is interpolated into a SQL string like WHERE col = '{value}'.

    For new code, prefer parameterized queries. This function is a
    safety net for legacy f-string SQL patterns.

    Args:
        value: The raw string value

    Returns:
        The escaped string (single quotes doubled)
    """
    if not isinstance(value, str):
        value = str(value)
    return value.replace("'", "''")
