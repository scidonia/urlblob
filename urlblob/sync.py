# Copyright 2025 Scidonia Limited
# Licensed under the Apache License, Version 2.0 (the "License");

"""
Synchronous versions of the UrlBlob and UrlBlobManager classes.
"""

from typing import Union, List, Iterator, Iterable
import httpx
from io import BytesIO

from .common import UrlType
from .stat import UrlBlobStats
from .util import (
    detect_url_type,
    build_get_headers,
    build_put_headers,
    sync_validate_response,
)


class SyncUrlBlob:
    """Synchronous version of UrlBlob for working with blob storage via URLs."""

    _client: httpx.Client

    def __init__(self, url: str, client: httpx.Client, url_type: UrlType | None = None):
        """
        Initialize a SyncUrlBlob.

        Args:
            url: The URL of the blob.
            client: The httpx Client to use for requests.
            url_type: Optional URL type override. If not provided, it will be detected.
        """
        self.url = url
        self._client = client
        self.url_type = url_type if url_type is not None else detect_url_type(url)

    def stat(self) -> UrlBlobStats:
        """
        Get statistics about the blob.

        Returns:
            UrlBlobStats: Statistics about the blob.
        """
        headers = build_get_headers(None, 0, 0)
        response = self._client.get(self.url, headers=headers)
        sync_validate_response(response, self.url_type)
        response.close()
        return UrlBlobStats(headers=response.headers)

    def get(
        self,
        byte_range: range | None = None,
        start: int | None = None,
        end: int | None = None,
    ) -> bytes:
        """
        Download the blob content.

        Args:
            byte_range: Optional range of bytes to download (end-exclusive, like Python's range).
            start: Optional start byte position (alternative to byte_range).
            end: Optional end byte position (alternative to byte_range, end-inclusive).

        Returns:
            bytes: The downloaded content.
        """
        headers = build_get_headers(byte_range, start, end)

        response = self._client.get(self.url, headers=headers)
        sync_validate_response(response, self.url_type)
        return response.content

    def get_lines(
        self,
        byte_range: range | None = None,
        start: int | None = None,
        end: int | None = None,
    ) -> List[str]:
        """
        Download the blob content and split it into lines.

        Args:
            byte_range: Optional range of bytes to download (end-exclusive, like Python's range).
            start: Optional start byte position (alternative to byte_range).
            end: Optional end byte position (alternative to byte_range, end-inclusive).

        Returns:
            List[str]: The downloaded content split into lines.
        """
        content = self.get(byte_range, start, end)
        return content.decode("utf-8").splitlines()

    def grow_to_valid_string(
        self,
        byte_range: range | None = None,
        start: int | None = None,
        end: int | None = None,
    ) -> str:
        """
        Download blob content as a valid UTF-8 string, handling partial UTF-8 sequences
        at range boundaries by extending the range as needed.

        Args:
            byte_range: Optional range of bytes to download (end-exclusive, like Python's range).
            start: Optional start byte position (alternative to byte_range).
            end: Optional end byte position (alternative to byte_range, end-inclusive).

        Returns:
            str: The downloaded content as a valid UTF-8 string.
        """
        # If no range specified, get everything and use replace for invalid sequences
        if byte_range is None and start is None and end is None:
            content = self.get()
            return content.decode("utf-8", errors="replace")

        # Convert byte_range to start/end if provided
        if byte_range is not None:
            range_start = byte_range.start if byte_range.start is not None else 0
            # Convert end-exclusive range to end-inclusive for consistency with other methods
            range_end = byte_range.stop - 1 if byte_range.stop is not None else None
        else:
            range_start = start
            range_end = end

        # Get the initial fragment
        fragment = self.get(start=range_start, end=range_end)
        original_fragment = fragment
        left_extension = 0
        right_extension = 0
        blob_stats = None

        while True:
            try:
                # At some point we need to give up. Since each valid UTF-8 sequence is at most 4 bytes long,
                # we should have a parsable string by extending at most 3 times both on the left and right.
                if left_extension >= 4 or right_extension >= 4:
                    break

                # Try to parse what we have now
                return fragment.decode("utf-8")
            except UnicodeDecodeError as e:
                if e.start == 0 and e.reason.startswith("invalid start"):
                    # Failure is at the very start, so let's extend leftward
                    left_extension += 1
                    # We cannot extend past the start of the document, and we should only extend at most 3 steps
                    if (
                        range_start is None
                        or left_extension > range_start
                        or left_extension >= 4
                    ):
                        break

                    pos = range_start - left_extension
                    # Extend one byte to the left
                    left_byte = self.get(start=pos, end=pos)
                    fragment = left_byte + fragment
                elif e.end == len(fragment) and e.reason.startswith("unexpected end"):
                    # Failure is at the very end, so let's extend rightward
                    right_extension += 1
                    # If end is None there's no chance of extending rightward. Otherwise make sure we don't go past the end.
                    blob_stats = blob_stats or self.stat()  # Lazy lookup
                    if (
                        range_end is None
                        or range_end + right_extension + 1 >= blob_stats.size()
                        or right_extension >= 4
                    ):
                        break
                    pos = range_end + right_extension
                    right_byte = self.get(start=pos, end=pos)
                    fragment += right_byte
                else:
                    # Failure appears to be in the middle. There is no recovery from this
                    break

        # If we got here, we hit a break condition in the loop above. Fall back to decode with replace
        return original_fragment.decode("utf-8", errors="replace")

    def shrink_to_valid_string(
        self,
        byte_range: range | None = None,
        start: int | None = None,
        end: int | None = None,
    ) -> str:
        """
        Download blob content as a valid UTF-8 string, handling partial UTF-8 sequences
        at range boundaries by shrinking the range as needed.

        Args:
            byte_range: Optional range of bytes to download (end-exclusive, like Python's range).
            start: Optional start byte position (alternative to byte_range).
            end: Optional end byte position (alternative to byte_range, end-inclusive).

        Returns:
            str: The downloaded content as a valid UTF-8 string.
        """
        # If no range specified, get everything and use replace for invalid sequences
        if byte_range is None and start is None and end is None:
            content = self.get()
            return content.decode("utf-8", errors="replace")

        # Convert byte_range to start/end if provided
        if byte_range is not None:
            range_start = byte_range.start if byte_range.start is not None else 0
            # Convert end-exclusive range to end-inclusive for consistency with other methods
            range_end = byte_range.stop - 1 if byte_range.stop is not None else None
        else:
            range_start = start
            range_end = end

        # Get the initial fragment
        fragment = self.get(start=range_start, end=range_end)

        while True:
            try:
                return fragment.decode("utf-8")
            except UnicodeDecodeError as e:
                # if the failing bit is at the start or end, we throw it away
                if e.start == 0:
                    fragment = fragment[e.end :]
                elif e.end == len(fragment):
                    fragment = fragment[: e.start]
                else:
                    # but if something in the middle fails, let's ust fall back to an error replace
                    return fragment.decode("utf-8", errors="replace")

    def put(
        self,
        content: Union[
            str,
            bytes,
            Iterator[bytes],
            Iterable[bytes],
        ],
        content_type: str | None = None,
    ) -> None:
        """
        Upload content to the URL using HTTP PUT.

        Args:
            content: The content to upload. Can be a string, bytes,
                    or an iterator over string or bytes.
            content_type: Optional content type header to set. If not provided,
                          the server will determine the content type.
        """
        headers = build_put_headers(self.url_type, content_type)

        # Convert string to bytes if needed
        if isinstance(content, str):
            content = content.encode("utf-8")

        # Handle iterators/iterables by collecting into bytes
        if isinstance(content, (Iterator, Iterable)) and not isinstance(
            content, (bytes, bytearray)
        ):
            buffer = BytesIO()
            for chunk in content:
                buffer.write(chunk)
            content = buffer.getvalue()

        response = self._client.put(self.url, content=content, headers=headers)
        sync_validate_response(response, self.url_type)

    def put_lines(
        self,
        lines: Union[
            List[str],
            List[bytes],
            Iterator[str],
            Iterable[str],
            Iterator[bytes],
            Iterable[bytes],
        ],
        content_type: str | None = None,
    ) -> None:
        """
        Upload content to the URL using HTTP PUT, with each item in the input
        separated by newlines.

        Args:
            lines: The lines to upload. Can be a list of strings/bytes,
                  or an iterator over strings/bytes.
            content_type: Optional content type header to set. If not provided,
                          the server will determine the content type.
        """
        headers = build_put_headers(self.url_type, content_type)

        # For lists, join with newlines
        if isinstance(lines, (list, tuple)):
            content = b""
            for i, line in enumerate(lines):
                if i > 0:
                    content += b"\n"
                if isinstance(line, str):
                    content += line.encode("utf-8")
                else:
                    content += line

            response = self._client.put(self.url, content=content, headers=headers)
            sync_validate_response(response, self.url_type)
        else:
            # For iterators/iterables, collect into a single bytes object with newlines
            buffer = BytesIO()
            is_first = True

            for line in lines:
                if not is_first:
                    buffer.write(b"\n")
                else:
                    is_first = False

                if isinstance(line, str):
                    buffer.write(line.encode("utf-8"))
                else:
                    buffer.write(line)

            response = self._client.put(
                self.url, content=buffer.getvalue(), headers=headers
            )
            sync_validate_response(response, self.url_type)


class SyncUrlBlobManager:
    """Synchronous version of UrlBlobManager for creating SyncUrlBlob instances."""

    _client: httpx.Client

    def __init__(self):
        """Initialize a SyncUrlBlobManager with a new httpx Client."""
        self._client = httpx.Client(http2=True)

    def from_url(self, url: str, url_type: UrlType | None = None) -> SyncUrlBlob:
        """
        Create a SyncUrlBlob from a URL.

        Args:
            url: The URL of the blob.
            url_type: Optional URL type override. If not provided, it will be detected.

        Returns:
            SyncUrlBlob: A new SyncUrlBlob instance.
        """
        return SyncUrlBlob(url, self._client, url_type)

    def close(self) -> None:
        """Close the underlying httpx Client."""
        self._client.close()

    def __enter__(self) -> "SyncUrlBlobManager":
        """Support for context manager protocol."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Close the client when exiting the context."""
        self.close()
