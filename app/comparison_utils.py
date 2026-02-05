"""Utility functions for comparing Darwin Core field values."""


def normalize_case(value):
    """
    Normalize string values to lowercase for case-insensitive comparison.
    
    Args:
        value: Any value
        
    Returns:
        Lowercased string if value is a string, otherwise the original value
    """
    if isinstance(value, str):
        return value.lower()
    return value


def normalize_connectors(value):
    """
    Normalize connector words/symbols to a common form.
    Treats "&" and " et " as equivalent (both become " & ").
    
    Args:
        value: Any value
        
    Returns:
        String with normalized connectors if value is a string, otherwise original value
    """
    if not isinstance(value, str):
        return value
    
    import re
    # Normalize " et " (with spaces) to " & "
    # Use word boundary to avoid matching "et" within words like "letter"
    result = re.sub(r'\s+et\s+', ' & ', value, flags=re.IGNORECASE)
    return result


def normalize_string(value):
    """
    Apply all string normalizations: lowercase and connector normalization.
    
    Args:
        value: Any value
        
    Returns:
        Normalized string if value is a string, otherwise original value
    """
    if not isinstance(value, str):
        return value
    
    result = normalize_case(value)
    result = normalize_connectors(result)
    return result


def normalize_null(value):
    """
    Normalize null/None/empty values.
    Treats missing keys (None), explicit null values, and empty strings as equivalent.
    
    Args:
        value: Any value
        
    Returns:
        None if value is None, null-like, or empty string, otherwise the original value
    """
    if value is None:
        return None
    if value == "":
        return None
    return value


def values_equal_null_aware(value1, value2) -> bool:
    """
    Compare two values treating None/null as equivalent.
    Missing key and key with value null are considered the same.
    Comparison is case-insensitive and normalizes connectors (& = et).
    
    Args:
        value1: First value to compare
        value2: Second value to compare
        
    Returns:
        True if values are equal (considering None equivalence)
    """
    v1 = normalize_null(value1)
    v2 = normalize_null(value2)
    return normalize_string(v1) == normalize_string(v2)


def normalize_semicolon_list(value: str | None) -> set[str] | None:
    """
    Normalize a semicolon-separated string into a set of normalized values.
    
    Each part is trimmed, lowercased, and has connectors normalized (& = et).
    
    Args:
        value: String with values separated by semicolons, or None
        
    Returns:
        Set of normalized strings, or None if input is None
    """
    if value is None:
        return None
    if not isinstance(value, str):
        return value
    
    # Split by semicolon, trim whitespace, normalize each part, filter empty strings
    parts = [normalize_string(part.strip()) for part in value.split(";")]
    parts = [part for part in parts if part]  # Remove empty strings
    return set(parts)


def values_equal_semicolon_list(value1, value2) -> bool:
    """
    Compare two semicolon-separated values, ignoring order, whitespace, and case.
    
    Splits each value by semicolon, trims whitespace from each part,
    lowercases, and compares the resulting sets (order-independent).
    
    Args:
        value1: First value (string with semicolon-separated items, or None)
        value2: Second value (string with semicolon-separated items, or None)
        
    Returns:
        True if the sets of values are equal
        
    Examples:
        >>> values_equal_semicolon_list("A; B; C", "c;B;a")
        True
        >>> values_equal_semicolon_list("A; B", "A;B;C")
        False
        >>> values_equal_semicolon_list(None, None)
        True
    """
    set1 = normalize_semicolon_list(value1)
    set2 = normalize_semicolon_list(value2)
    
    # Handle None cases
    if set1 is None and set2 is None:
        return True
    if set1 is None or set2 is None:
        return False
    
    return set1 == set2


def values_equal(value1, value2, semicolon_list: bool = False) -> bool:
    """
    Compare two values using the appropriate comparison method.
    
    All string comparisons are case-insensitive and normalize connectors (& = et).
    
    Args:
        value1: First value to compare
        value2: Second value to compare
        semicolon_list: If True, treat values as semicolon-separated lists
        
    Returns:
        True if values are considered equal
    """
    # First apply null normalization
    v1 = normalize_null(value1)
    v2 = normalize_null(value2)
    
    # Handle None cases
    if v1 is None and v2 is None:
        return True
    if v1 is None or v2 is None:
        return False
    
    # Apply semicolon list comparison if requested (already normalized)
    if semicolon_list:
        return values_equal_semicolon_list(v1, v2)
    
    # Normalized comparison for strings (case-insensitive, connectors normalized)
    return normalize_string(v1) == normalize_string(v2)
