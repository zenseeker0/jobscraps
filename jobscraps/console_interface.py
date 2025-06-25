class ConsoleInterface:
    """Simple console interface abstraction."""

    def print(self, message: str) -> None:
        print(message)

    def input(self, prompt: str) -> str:
        return input(prompt)


console = ConsoleInterface()

__all__ = ["ConsoleInterface", "console"]
