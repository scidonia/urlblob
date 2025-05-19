from httpx import AsyncClient
from .blob import UrlBlob


class UrlBlobManager:
    _client: AsyncClient

    def __init__(self):
        self._client = AsyncClient(http2=True)

    def from_url(self, url: str) -> UrlBlob:
        return UrlBlob(url, self._client)
