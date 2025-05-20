from httpx import AsyncClient

from urlblob.common import UrlType
from .blob import UrlBlob


class UrlBlobManager:
    _client: AsyncClient

    def __init__(self):
        self._client = AsyncClient(http2=True)

    def from_url(self, url: str, url_type: UrlType | None = None) -> UrlBlob:
        return UrlBlob(url, self._client, url_type=url_type)
