from typing import Optional


class PetexException(Exception):
    """High-level error surfaced to callers."""

    def __init__(self, message: str, code: Optional[int] = None):
        self.message = message
        self.code = code
        super().__init__(self.__str__())

    def GetError(self) -> str:  # kept for backwards-compat parity
        return self.message

    def __str__(self) -> str:
        return f"{self.message}" if self.code is None else f"[{self.code}] {self.message}"