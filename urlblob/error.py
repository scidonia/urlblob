# Copyright 2025 Scidonia Limited
# Licensed under the Apache License, Version 2.0 (the "License");

from dataclasses import dataclass, field
import xml.etree.ElementTree as ET
from io import BytesIO
from httpx import Response


from .common import UrlType


@dataclass
class BlobError(Exception):
    url_type: UrlType
    status_code: int
    reason: str | None = None
    message: str | None = None
    extra_info: str | None = None
    raw_data: str | None = field(default=None, repr=False)

    def __str__(self) -> str:
        error = f"{self.reason or self.message or self.raw_data or self.status_code}"
        if self.extra_info is not None:
            error += f" ({self.extra_info})"

        return error


@dataclass
class RetryableBlobError(BlobError):
    pass


@dataclass
class NonRetryableBlobError(BlobError):
    pass


@dataclass
class ContainerNotFoundError(NonRetryableBlobError):
    pass


@dataclass
class BlobNotFoundError(NonRetryableBlobError):
    pass


@dataclass
class AuthenticationFailedError(NonRetryableBlobError):
    pass


def parse_azure_error(response):
    """
    Parse an Azure error response into an appropriate BlobError.

    Args:
        response: The HTTP response object from httpx.

    Returns:
        An appropriate BlobError subclass instance based on the error type:
        - ContainerNotFoundError: If the container doesn't exist
        - BlobNotFoundError: If the blob doesn't exist
        - AuthenticationFailedError: If authentication failed
        - RetryableBlobError: For server errors (5xx)
        - NonRetryableBlobError: For other errors
    """
    status_code = response.status_code
    content = response.content
    reason = response.reason_phrase

    # Default values
    message = None

    # Try to parse XML response
    message = None
    code = None
    raw_data = content.decode("utf-8", errors="replace")
    root = None
    try:
        # Parse XML content
        root = ET.parse(BytesIO(content)).getroot()

        # Extract message
        message_elem = root.find(".//Message")
        if message_elem is not None:
            message = message_elem.text
        code_elem = root.find(".//Code")
        if code_elem is not None:
            code = code_elem.text
    except Exception:
        # If XML parsing fails, use raw content
        pass

    data = {
        "url_type": UrlType.AZURE,
        "status_code": status_code,
        "reason": reason,
        "message": message,
        "raw_data": raw_data,
    }
    error_type = NonRetryableBlobError  # will be overwritten if we can find something more specific

    # Create appropriate error type based on status code
    if status_code == 404:
        if code is not None and code == "ContainerNotFound":
            error_type = ContainerNotFoundError
        else:
            error_type = BlobNotFoundError
    elif status_code == 403:
        # For authentication errors, look for AuthenticationErrorDetail
        try:
            if root is not None:
                auth_detail_elem = root.find(".//AuthenticationErrorDetail")
                if auth_detail_elem is not None:
                    auth_detail = auth_detail_elem.text
                    data["extra_info"] = auth_detail
        except Exception:
            pass

        error_type = AuthenticationFailedError
    elif status_code >= 500:
        error_type = RetryableBlobError

    return error_type(**data)


def parse_s3_error(response):
    """
    Parse an S3 error response into an appropriate BlobError.

    Args:
        response: The HTTP response object from httpx.

    Returns:
        An appropriate BlobError subclass instance based on the error type:
        - ContainerNotFoundError: If the bucket doesn't exist
        - BlobNotFoundError: If the object doesn't exist
        - AuthenticationFailedError: If authentication failed
        - RetryableBlobError: For server errors (5xx)
        - NonRetryableBlobError: For other errors
    """
    status_code = response.status_code
    content = response.content
    reason = response.reason_phrase

    # Default values
    message = None
    code = None
    raw_data = content.decode("utf-8", errors="replace")

    try:
        # Parse XML content
        root = ET.parse(BytesIO(content)).getroot()

        # S3 error responses typically have Code and Message elements
        code_elem = root.find(".//Code")
        message_elem = root.find(".//Message")

        if code_elem is not None:
            code = code_elem.text

        if message_elem is not None:
            message = message_elem.text
    except Exception:
        # If XML parsing fails, use raw content
        pass

    data = {
        "url_type": UrlType.S3,
        "status_code": status_code,
        "reason": reason,
        "message": message,
        "raw_data": raw_data,
    }

    # Map common S3 error codes to appropriate error types
    error_type = NonRetryableBlobError
    if status_code == 404:
        if code == "NoSuchBucket":
            error_type = ContainerNotFoundError
        else:
            error_type = BlobNotFoundError
    elif status_code == 403:
        data["extra_info"] = code
        error_type = AuthenticationFailedError
    elif status_code >= 500:
        error_type = RetryableBlobError

    return error_type(**data)


def parse_generic_error(response: Response, url_type: UrlType) -> BlobError:
    """
    Parse a generic error response into an appropriate BlobError.
    Used for Generic and GCP URL types, or when a more specific parser isn't available.

    Args:
        response: The HTTP response object from httpx.
        url_type: The type of URL (Generic, GCP, etc.)

    Returns:
        An appropriate BlobError subclass instance based on the HTTP status code:
        - BlobNotFoundError: For 404 errors
        - AuthenticationFailedError: For 403 errors
        - RetryableBlobError: For server errors (5xx)
        - NonRetryableBlobError: For other errors
    """
    data = {
        "url_type": url_type,
        "status_code": response.status_code,
        "reason": response.reason_phrase,
        "raw_data": response.content.decode("utf-8", errors="replace"),
    }

    error_type = NonRetryableBlobError
    if response.status_code == 403:
        error_type = AuthenticationFailedError
    elif response.status_code == 404:
        error_type = BlobNotFoundError
    elif response.status_code >= 500:
        error_type = RetryableBlobError

    return error_type(**data)


def parse_error(response: Response, url_type: UrlType) -> BlobError:
    """
    Parse an error response into an appropriate BlobError based on the URL type.

    Args:
        response: The HTTP response object from httpx.
        url_type: The type of URL (S3, AZURE, GCP, GENERIC)

    Returns:
        An appropriate BlobError subclass instance.
    """
    if url_type == UrlType.AZURE:
        return parse_azure_error(response)
    elif url_type == UrlType.S3:
        return parse_s3_error(response)
    else:
        return parse_generic_error(response, url_type)
