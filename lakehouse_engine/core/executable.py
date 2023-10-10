"""Module representing an executable lakehouse engine component."""
from abc import ABC, abstractmethod
from typing import Any, Optional


class Executable(ABC):
    """Abstract class defining the behaviour of an executable component."""

    @abstractmethod
    def execute(self) -> Optional[Any]:
        """Define the executable component behaviour.

        E.g., the behaviour of an algorithm inheriting from this.
        """
        pass
