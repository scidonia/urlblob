from httpx import AsyncClient, Headers
from .stat import UrlBlobStats
from .util import build_range_header, detect_url_type, UrlType

from typing import IO, AsyncIterator, Iterator, Union, AsyncIterable, Iterable, List


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

    async def stream(
        self,
        byte_range: range | None = None,
        start: int | None = None,
        end: int | None = None,
    ) -> AsyncIterator[bytes]:
        headers = build_range_header(byte_range, start, end)

        async with self._client.stream("GET", self._url, headers=headers) as response:
            async for chunk in response.aiter_bytes():
                yield chunk

    async def stream_lines(
        self,
        byte_range: range | None = None,
        start: int | None = None,
        end: int | None = None,
    ) -> AsyncIterator[bytes]:
        headers = build_range_header(byte_range, start, end)

        async with self._client.stream("GET", self._url, headers=headers) as response:
            async for line in response.aiter_lines():
                yield line

    async def put(
        self,
        content: Union[
            str,
            bytes,
            IO[str],
            IO[bytes],
            Iterator[bytes],
            AsyncIterator[bytes],
            Iterable[bytes],
            AsyncIterable[bytes],
        ],
        content_type: str | None = None,
    ) -> None:
        """
        Upload content to the URL using HTTP PUT.

        Args:
            content: The content to upload. Can be a string, bytes, file-like object,
                    or an iterator/async iterator over bytes.
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

    async def put_lines(
        self,
        lines: Union[
            List[str],
            List[bytes],
            Iterator[str],
            AsyncIterator[str],
            Iterable[str],
            AsyncIterable[str],
            Iterator[bytes],
            AsyncIterator[bytes],
            Iterable[bytes],
            AsyncIterable[bytes],
        ],
        content_type: str | None = None,
    ) -> None:
        """
        Upload content to the URL using HTTP PUT, with each item in the input
        separated by newlines.

        Args:
            lines: The lines to upload. Can be a list of strings/bytes,
                  or an iterator/async iterator over strings/bytes.
            content_type: Optional content type header to set. If not provided,
                          the server will determine the content type.

        Raises:
            ValueError: If the upload fails with details about the error.
        """
        # Handle different types of input
        if isinstance(lines, (list, tuple)):
            # For lists/tuples, join with newlines
            if all(isinstance(line, str) for line in lines):
                content = "\n".join(lines)
            else:
                # Convert bytes to a single bytes object with newlines
                content = b"\n".join(
                    line if isinstance(line, bytes) else line.encode() for line in lines
                )

            # Use the existing put method
            await self.put(content=content, content_type=content_type)
        else:
            # For iterators/iterables, create an async generator that adds newlines
            async def line_generator():
                newline = b"\n"
                is_first = True

                # Handle both sync and async iterables
                if hasattr(lines, "__aiter__"):
                    async for line in lines:
                        if not is_first:
                            yield newline
                        else:
                            is_first = False

                        if isinstance(line, str):
                            yield line.encode()
                        else:
                            yield line
                else:
                    for line in lines:
                        if not is_first:
                            yield newline
                        else:
                            is_first = False

                        if isinstance(line, str):
                            yield line.encode()
                        else:
                            yield line

            # Use the existing put method with the generator
            await self.put(content=line_generator(), content_type=content_type)
