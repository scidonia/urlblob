from contextlib import nullcontext
from httpx import AsyncClient, Headers
from .stat import UrlBlobStats
from .util import build_range_header, detect_url_type, UrlType


class UrlBlob:
    _client: AsyncClient
    _url: str
    _url_type: UrlType

    def __init__(self, url: str, client: AsyncClient, url_type: UrlType | None = None):
        """
        Initialize a UrlBlob.

        Args:
            url: The URL to the blob.
            client: The HTTP client to use for requests.
            url_type: Optional explicit URL type to use instead of auto-detection.
        """
        self._url = url
        self._client = client
        self._url_type = url_type if url_type is not None else detect_url_type(url)

    async def stat(self) -> UrlBlobStats:
        response = await self._client.head(self._url)
        return UrlBlobStats(headers=response.headers)

    async def get(
        self,
        byte_range: range | None = None,
        start: int | None = None,
        end: int | None = None,
    ) -> bytes:
        headers = build_range_header(byte_range, start, end)

        response = await self._client.get(self._url, headers=headers)

        return response.content
