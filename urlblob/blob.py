from contextlib import nullcontext
from httpx import AsyncClient, Headers


class UrlBlobStats:
    _headers: Headers

    def __init__(self, headers: Headers):
        self._headers = headers

    def size(self) -> int:
        """
        Get the size of the blob from the Content-Length header.

        Returns:
            int: Size in bytes.

        Raises:
            ValueError: If Content-Length header is not present or invalid.
        """
        size = self.size_or_none()
        if size is None:
            raise ValueError("Content-Length header is not present or invalid")
        return size

    def size_or_none(self) -> int | None:
        """
        Get the size of the blob from the Content-Length header, or None if not present.

        Returns:
            int or None: Size in bytes, or None if Content-Length header is not present.

        Raises:
            ValueError: If Content-Length header is present but invalid.
        """
        content_length = self._headers.get("Content-Length")
        if content_length is not None:
            return int(content_length)
        return None

    def content_type(self) -> str:
        """
        Get the content type of the blob from the Content-Type header.

        Returns:
            str: Content type.

        Raises:
            ValueError: If Content-Type header is not present.
        """
        content_type = self.content_type_or_none()
        if content_type is None:
            raise ValueError("Content-Type header is not present")
        return content_type

    def content_type_or_none(self) -> str | None:
        """
        Get the content type of the blob from the Content-Type header, or None if not present.

        Returns:
            str or None: Content type, or None if Content-Type header is not present.
        """
        return self._headers.get("Content-Type")

    def last_modified(self) -> str:
        """
        Get the last modified date of the blob from the Last-Modified header.

        Returns:
            str: Last modified date.

        Raises:
            ValueError: If Last-Modified header is not present.
        """
        last_modified = self.last_modified_or_none()
        if last_modified is None:
            raise ValueError("Last-Modified header is not present")
        return last_modified

    def last_modified_or_none(self) -> str | None:
        """
        Get the last modified date of the blob from the Last-Modified header, or None if not present.

        Returns:
            str or None: Last modified date, or None if Last-Modified header is not present.
        """
        return self._headers.get("Last-Modified")

    def to_dict(self) -> dict:
        """
        Convert the stats to a dictionary.

        Returns:
            dict: Dictionary representation of the stats.
        """
        result = {}

        size = self.size_or_none()
        if size is not None:
            result["size"] = size

        content_type = self.content_type_or_none()
        if content_type is not None:
            result["content_type"] = content_type

        last_modified = self.last_modified_or_none()
        if last_modified is not None:
            result["last_modified"] = last_modified

        return result


class UrlBlob:
    _client: AsyncClient
    _url: str

    def __init__(self, url: str, client: AsyncClient):
        self._url = url
        self._client = client

    async def stat(self) -> UrlBlobStats:
        response = await self._client.head(self._url)
        return UrlBlobStats(headers=response.headers)

    async def get(
        self,
        byte_range: range | None = None,
        start: int | None = None,
        end: int | None = None,
    ) -> bytes:
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

        response = await self._client.get(self._url, headers=headers)

        return response.content
