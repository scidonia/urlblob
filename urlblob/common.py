from enum import Enum, auto


class UrlType(Enum):
    """Enum representing different cloud provider URL types."""

    S3 = auto()
    GCP = auto()
    AZURE = auto()
    GENERIC = auto()
