from httpx import AsyncClient, Headers
from .stat import UrlBlobStats
from .util import build_range_header, detect_url_type, UrlType

from typing import IO


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

    async def put(
        self,
        content: str | bytes | IO[str] | IO[bytes],
        content_type: str | None = None,
    ) -> None:
        """
        Upload content to the URL using HTTP PUT.

        Args:
            content: The content to upload. Can be a string, bytes, or a file-like object.
            content_type: Optional content type header to set. If not provided,
                          the server will determine the content type.

        Raises:
            ValueError: If the upload fails with details about the error.
        """
        from .util import build_put_headers

        headers = build_put_headers(url_type=self._url_type, content_type=content_type)
        response = await self._client.put(self._url, content=content, headers=headers)

        # Check if the upload was successful (2xx status codes)
        if not response.is_success:
            error_msg = f"Upload failed with status {response.status_code}"
            error_details = response.text
            error_msg += f": {error_details}"
            raise ValueError(error_msg)
