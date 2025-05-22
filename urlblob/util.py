# Copyright 2025 Scidonia Limited
# Licensed under the Apache License, Version 2.0 (the "License");

import re
from .common import UrlType
from .error import parse_error


# Precompiled regexes for S3-compatible services
_S3_COMPATIBLE_PATTERNS = [
    re.compile(pattern)
    for pattern in [
        r"\.s3\.[a-z0-9-]+\.amazonaws\.com",
        r"s3\.amazonaws\.com",
        r"\.your-objectstorage.com",  # hetzner
    ]
]

# Precompiled regexes for GCP-compatible services
_GCP_COMPATIBLE_PATTERNS = [
    re.compile(pattern)
    for pattern in [
        r"\.storage\.googleapis\.com",
        r"storage\.cloud\.google\.com",
    ]
]

# Precompiled regexes for Azure-compatible services
_AZURE_COMPATIBLE_PATTERNS = [
    re.compile(pattern)
    for pattern in [
        r"\.blob\.core\.windows\.net",
    ]
]

# Combined regex for S3-compatible services
_S3_COMBINED_PATTERN = re.compile(
    "|".join(f"({pattern.pattern})" for pattern in _S3_COMPATIBLE_PATTERNS)
)

# Combined regex for GCP-compatible services
_GCP_COMBINED_PATTERN = re.compile(
    "|".join(f"({pattern.pattern})" for pattern in _GCP_COMPATIBLE_PATTERNS)
)

# Combined regex for Azure-compatible services
_AZURE_COMBINED_PATTERN = re.compile(
    "|".join(f"({pattern.pattern})" for pattern in _AZURE_COMPATIBLE_PATTERNS)
)


def is_s3_compatible(url: str) -> bool:
    """
    Check if a URL is from an S3-compatible storage provider.

    Args:
        url: The URL to analyze.

    Returns:
        bool: True if the URL is from an S3-compatible provider.
    """
    # Use the combined pattern for efficient matching
    return bool(_S3_COMBINED_PATTERN.search(url))


def is_gcp_compatible(url: str) -> bool:
    """
    Check if a URL is from a GCP-compatible storage provider.

    Args:
        url: The URL to analyze.

    Returns:
        bool: True if the URL is from a GCP-compatible provider.
    """
    # Use the combined pattern for efficient matching
    return bool(_GCP_COMBINED_PATTERN.search(url))


def is_azure_compatible(url: str) -> bool:
    """
    Check if a URL is from an Azure-compatible storage provider.

    Args:
        url: The URL to analyze.

    Returns:
        bool: True if the URL is from an Azure-compatible provider.
    """
    # Use the combined pattern for efficient matching
    return bool(_AZURE_COMBINED_PATTERN.search(url))


def parse_url_type(url_type_str: str) -> UrlType:
    """
    Parse a string into a UrlType enum value, with support for aliases.

    Args:
        url_type_str: String representation of URL type.

    Returns:
        UrlType: The corresponding enum value.

    Raises:
        ValueError: If the string doesn't match any known URL type.
    """
    # Convert to uppercase for case-insensitive matching
    url_type_upper = url_type_str.upper()

    # Direct enum name matching
    if url_type_upper in UrlType.__members__:
        return UrlType[url_type_upper]

    # Aliases
    aliases = {
        "AWS": UrlType.S3,
        "AWS_S3": UrlType.S3,
        "GOOGLE": UrlType.GCP,
        "AZ": UrlType.AZURE,
        "WINDOWS": UrlType.AZURE,
    }

    if url_type_upper in aliases:
        return aliases[url_type_upper]

    # If no match is found, raise an error with valid options
    valid_types = ", ".join([x.lower() for x in UrlType.__members__.keys()])
    raise ValueError(f"Invalid URL type: {url_type_str}. Valid types: {valid_types}")


def detect_url_type(url: str) -> UrlType:
    """
    Detect the cloud provider type from a URL.

    Args:
        url: The URL to analyze.

    Returns:
        UrlType: The detected URL type (S3, GCP, AZURE, or GENERIC).
    """
    # Check for S3-compatible providers
    if is_s3_compatible(url):
        return UrlType.S3

    # Check for GCP-compatible providers
    if is_gcp_compatible(url):
        return UrlType.GCP

    # Check for Azure-compatible providers
    if is_azure_compatible(url):
        return UrlType.AZURE

    # Default to generic if no patterns match
    return UrlType.GENERIC


def build_get_headers(
    byte_range: range | None = None,
    start: int | None = None,
    end: int | None = None,
) -> dict:
    """
    Build a headers dictionary with Range header if needed.

    Args:
        byte_range: Python range object to use for byte range (end-exclusive).
        start: Start byte position.
        end: End byte position (end-inclusive, unlike byte_range).

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


def build_put_headers(
    url_type: UrlType,
    content_type: str | None = None,
) -> dict:
    """
    Build headers dictionary for PUT requests.

    Args:
        url_type: URL type to customize headers for specific cloud providers.
        content_type: Optional content type to set.

    Returns:
        Dictionary with appropriate headers for the PUT request.
    """
    headers = {}

    if content_type:
        headers["Content-Type"] = content_type

    # Add Azure-specific headers
    if url_type == UrlType.AZURE:
        # Azure Blob Storage requires x-ms-blob-type header
        # BlockBlob is the most common type for general purpose storage
        headers["x-ms-blob-type"] = "BlockBlob"

    return headers


async def validate_response(response, url_type: UrlType):
    """
    Validate the HTTP response and raise appropriate errors based on the URL type.

    Args:
        response: The HTTP response object from httpx.
        url_type: The type of URL (S3, GCP, Azure, etc.)

    Raises:
        BlobError: If the response indicates an error, with details specific to the provider.
            May raise specific subclasses like BlobNotFoundError, AuthenticationFailedError, etc.
    """
    if not response.is_success:
        await response.aread()

        raise parse_error(response, url_type)


def sync_validate_response(response, url_type: UrlType):
    """
    Validate the HTTP response and raise appropriate errors based on the URL type.
    Synchronous version of validate_response.

    Args:
        response: The HTTP response object from httpx.
        url_type: The type of URL (S3, GCP, Azure, etc.)

    Raises:
        BlobError: If the response indicates an error, with details specific to the provider.
            May raise specific subclasses like BlobNotFoundError, AuthenticationFailedError, etc.
    """
    if not response.is_success:
        response.read()

        raise parse_error(response, url_type)
