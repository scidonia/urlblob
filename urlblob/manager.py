# Copyright 2025 Scidonia Limited
# Licensed under the Apache License, Version 2.0 (the "License");

from httpx import AsyncClient

from urlblob.common import UrlType
from .blob import UrlBlob


class UrlBlobManager:
    _client: AsyncClient

    def __init__(self):
        self._client = AsyncClient(http2=True)

    def from_url(self, url: str, url_type: UrlType | None = None) -> UrlBlob:
        """
        Create a UrlBlob from a URL.

        Args:
            url: The URL of the blob.
            url_type: Optional URL type override. If not provided, it will be detected.

        Returns:
            UrlBlob: A new UrlBlob instance.
        """
        return UrlBlob(url, self._client, url_type=url_type)

    async def close(self) -> None:
        """Close the underlying httpx Client."""
        await self._client.aclose()

    async def __enter__(self) -> "UrlBlobManager":
        """Support for context manager protocol."""
        return self

    async def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Close the client when exiting the context."""
        await self.close()
