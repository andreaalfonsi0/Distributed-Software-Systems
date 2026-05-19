from __future__ import annotations

import threading


class LamportClock:
    def __init__(self, initial_value: int = 0) -> None:
        self._value = initial_value
        self._lock = threading.Lock()

    @property
    def value(self) -> int:
        with self._lock:
            return self._value

    def seed(self, value: int) -> None:
        with self._lock:
            self._value = max(self._value, value)

    def tick(self) -> int:
        with self._lock:
            self._value += 1
            return self._value

    def update(self, remote_value: int) -> int:
        with self._lock:
            self._value = max(self._value, remote_value) + 1
            return self._value
