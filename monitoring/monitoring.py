"""Base abstractions for monitoring backends."""

from abc import ABC, abstractmethod
from typing import Any, ClassVar, Optional, cast

import settings


class Monitoring(ABC):
    """Abstract base class for monitoring backends."""

    DEFAULT_NODES: ClassVar[list[str]]

    def __init__(self, mconfig: dict[str, Any]) -> None:
        """Resolve monitoring nodes from configuration or subclass defaults."""
        nodes_list = mconfig.get("nodes", self.DEFAULT_NODES)
        # Remove this cast and ignore once settings.getnodes() is fully typed.
        self._nodes = cast(str, settings.getnodes(*nodes_list))  # type: ignore[no-untyped-call]

    @abstractmethod
    def start(self, directory: str) -> None:
        """Start monitoring and write output beneath the given directory."""

    @abstractmethod
    def stop(self, directory: Optional[str]) -> None:
        """Stop monitoring and finalize any output files."""
