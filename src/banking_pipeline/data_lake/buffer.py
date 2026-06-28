class EventBuffer:
    def __init__(self, max_size: int = 100):
        self.max_size = max_size
        self.events: list[dict] = []

    def add(self, event: dict) -> None:
        self.events.append(event)

    def is_full(self) -> bool:
        return len(self.events) >= self.max_size

    def get_events(self) -> list[dict]:
        return self.events

    def clear(self) -> None:
        self.events.clear()

    def size(self) -> int:
        return len(self.events)