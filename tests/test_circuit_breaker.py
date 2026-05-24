"""Tests for the circuit breaker module: three-state model."""

import time

import pytest

from simtradedata.resilience.circuit_breaker import (
    CircuitBreaker,
    CircuitBreakerConfig,
    CircuitState,
)


def _wait_half_open(cb: CircuitBreaker, deadline_seconds: float = 2.0) -> None:
    """Poll for the OPEN -> HALF_OPEN transition.

    ``CircuitBreaker.state`` is a property that performs the transition on
    read, so a fixed sleep races with scheduler delays under load. Polling
    asserts the same behaviour without depending on exact wall-clock timing.
    """
    deadline = time.monotonic() + deadline_seconds
    while cb.state is not CircuitState.HALF_OPEN and time.monotonic() < deadline:
        time.sleep(0.01)


@pytest.mark.unit
class TestCircuitBreaker:
    """Tests for CircuitBreaker state transitions."""

    def _make_breaker(
        self,
        failure_threshold: int = 3,
        success_threshold: int = 2,
        timeout: float = 0.3,
    ) -> CircuitBreaker:
        """Create a circuit breaker with fast-test defaults."""
        config = CircuitBreakerConfig(
            failure_threshold=failure_threshold,
            success_threshold=success_threshold,
            timeout=timeout,
        )
        return CircuitBreaker("test", config=config)

    def test_starts_closed(self):
        cb = self._make_breaker()
        assert cb.state is CircuitState.CLOSED

    def test_opens_after_failure_threshold(self):
        cb = self._make_breaker(failure_threshold=3)
        for _ in range(3):
            cb.record_failure()
        assert cb.state is CircuitState.OPEN

    def test_stays_closed_below_threshold(self):
        cb = self._make_breaker(failure_threshold=3)
        cb.record_failure()
        cb.record_failure()
        assert cb.state is CircuitState.CLOSED

    def test_success_resets_failure_count(self):
        cb = self._make_breaker(failure_threshold=3)
        # Two failures, then a success resets the count.
        cb.record_failure()
        cb.record_failure()
        cb.record_success()
        # Two more failures should not open (total only 2 since reset).
        cb.record_failure()
        cb.record_failure()
        assert cb.state is CircuitState.CLOSED

    def test_transitions_to_half_open_after_timeout(self):
        # 300ms gives the OPEN assertion headroom under load; polling replaces
        # the old fixed 60ms sleep that raced with the state property.
        cb = self._make_breaker(failure_threshold=3, timeout=0.3)
        for _ in range(3):
            cb.record_failure()
        assert cb.state is CircuitState.OPEN
        _wait_half_open(cb)
        assert cb.state is CircuitState.HALF_OPEN

    def test_half_open_closes_on_success_threshold(self):
        cb = self._make_breaker(
            failure_threshold=3, success_threshold=2, timeout=0.3,
        )
        # Drive to OPEN.
        for _ in range(3):
            cb.record_failure()
        assert cb.state is CircuitState.OPEN
        _wait_half_open(cb)
        assert cb.state is CircuitState.HALF_OPEN
        # Meet the success threshold.
        cb.record_success()
        cb.record_success()
        assert cb.state is CircuitState.CLOSED

    def test_half_open_reopens_on_failure(self):
        cb = self._make_breaker(failure_threshold=3, timeout=0.3)
        # Drive to OPEN.
        for _ in range(3):
            cb.record_failure()
        assert cb.state is CircuitState.OPEN
        _wait_half_open(cb)
        assert cb.state is CircuitState.HALF_OPEN
        # A single failure should reopen.
        cb.record_failure()
        assert cb.state is CircuitState.OPEN
