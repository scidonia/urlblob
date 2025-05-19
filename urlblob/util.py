def build_range_header(
    byte_range: range | None = None,
    start: int | None = None,
    end: int | None = None,
) -> dict:
    """
    Build a headers dictionary with Range header if needed.

    Args:
        byte_range: Python range object to use for byte range.
        start: Start byte position.
        end: End byte position.

    Returns:
        Dictionary with Range header if range parameters are provided.

    Raises:
        ValueError: If both byte_range and start/end parameters are provided.
    """
    headers = {}

    # Check for conflicting parameters
    if byte_range is not None and (start is not None or end is not None):
        raise ValueError("Cannot specify both byte_range and start/end parameters")

    # Handle byte range
    if byte_range is not None:
        # Extract start and end from range object
        range_start = byte_range.start or 0
        # Range objects have exclusive end, HTTP ranges have inclusive end
        range_end = (byte_range.stop - 1) if byte_range.stop is not None else None
        headers["Range"] = (
            f"bytes={range_start}-{range_end if range_end is not None else ''}"
        )
    elif start is not None or end is not None:
        # Use explicit start/end parameters
        range_start = start or 0
        range_end = end  # HTTP Range headers use inclusive end
        headers["Range"] = (
            f"bytes={range_start}-{range_end if range_end is not None else ''}"
        )

    return headers
