"""
Retry strategies for transient failure recovery in distributed systems.
Implements Exponential Backoff with Full Jitter to prevent Thundering Herd problem.
"""

from __future__ import annotations
import abc
import random
from typing import Optional


class RetryPolicy(abc.ABC):
    @abc.abstractmethod
    def calculate_delay(self, attempt: int) -> float:
        """Returns the delay in seconds before the next retry attempt."""
        pass


class FixedDelayRetry(RetryPolicy):
    def __init__(self, delay_seconds: float = 2.0):
        self.delay_seconds = max(0.0, delay_seconds)

    def calculate_delay(self, attempt: int) -> float:
        return self.delay_seconds


class LinearBackoffRetry(RetryPolicy):
    def __init__(self, base_delay: float = 1.0, step: float = 2.0, max_delay: float = 60.0):
        self.base_delay = base_delay
        self.step = step
        self.max_delay = max_delay

    def calculate_delay(self, attempt: int) -> float:
        delay = self.base_delay + (attempt * self.step)
        return min(delay, self.max_delay)


class ExponentialBackoffRetry(RetryPolicy):
    """
    Exponential Backoff with Full Jitter.
    Formula: Sleep = Uniform(0, Min(MaxDelay, BaseDelay * 2^(attempt - 1)))
    Reduces contention and lock collision across concurrent workers.
    """
    def __init__(
        self,
        base_delay: float = 1.0,
        factor: float = 2.0,
        max_delay: float = 300.0,
        use_jitter: bool = True,
        seed: Optional[int] = None,
    ):
        self.base_delay = base_delay
        self.factor = factor
        self.max_delay = max_delay
        self.use_jitter = use_jitter
        self._rng = random.Random(seed) if seed is not None else random.Random()

    def calculate_delay(self, attempt: int) -> float:
        if attempt <= 0:
            return 0.0

        exponential = self.base_delay * (self.factor ** (attempt - 1))
        capped = min(exponential, self.max_delay)

        if not self.use_jitter:
            return capped

        # Full Jitter: sample uniformly between 0 and capped delay
        return self._rng.uniform(0.0, capped)
