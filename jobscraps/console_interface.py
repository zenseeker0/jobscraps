class ConsoleInterface:
    """Simple console interface abstraction."""

    def info(self, message: str) -> None:
        """Display an informational message."""
        print(message)

    def prompt(self, prompt: str) -> str:
        """Prompt the user and return their input."""
        return input(prompt)

    # Backwards compatibility with older calls
    def print(self, message: str) -> None:  # pragma: no cover - legacy support
        self.info(message)

    def input(self, prompt: str) -> str:  # pragma: no cover - legacy support
        return self.prompt(prompt)


console = ConsoleInterface()

__all__ = ["ConsoleInterface", "console"]
