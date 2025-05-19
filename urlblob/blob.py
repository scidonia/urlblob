from contextlib import nullcontext
from httpx import AsyncClient, Headers
from .stat import UrlBlobStats


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
