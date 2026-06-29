from datetime import datetime

class EventBuffer:
    def __init__(self, max_size: int = 100):
        self.max_size = max_size
        self.first_event_time: datetime | None = None
        self.events: list[dict] = []

    def add(self, event: dict) -> None:
        if not self.events:
            self.first_event_time = datetime.utcnow()
        self.events.append(event)

    def is_full(self) -> bool:
        return len(self.events) >= self.max_size

    def get_events(self) -> list[dict]:
        return self.events.copy()
    def clear(self) -> None:
        self.events.clear()
        self.first_event_time = None

    def size(self) -> int:
        return len(self.events)
    
    def should_flush(
            self,
            flush_interval_seconds: int,
            ) -> bool:
        """
        Return True if the oldest buffered event has
        been waiting longer than the configured interval.
        """

        if not self.events:
            return False

        elapsed = (
            datetime.utcnow() - self.first_event_time
        ).total_seconds()

        return elapsed >= flush_interval_seconds