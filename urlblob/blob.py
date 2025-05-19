from contextlib import nullcontext
from httpx import AsyncClient, Headers
from .stat import UrlBlobStats
from .util import build_range_header


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
        headers = build_range_header(byte_range, start, end)

        response = await self._client.get(self._url, headers=headers)

        return response.content
