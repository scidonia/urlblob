# Copyright 2025 Scidonia Limited
# Licensed under the Apache License, Version 2.0 (the "License");

from enum import Enum, auto


class UrlType(Enum):
    """Enum representing different cloud provider URL types."""

    S3 = auto()
    GCP = auto()
    AZURE = auto()
    GENERIC = auto()
