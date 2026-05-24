"""Tests for the cooldown module: tiered error handling and source tracking."""

import time

import pytest

from simtradedata.resilience.cooldown import (
    CooldownConfig,
    SmartCooldown,
    SourceState,
)


@pytest.mark.unit
class TestSmartCooldown:
    """Tests for SmartCooldown tiered cooldown behavior."""

    def test_not_in_cooldown_initially(self):
        cd = SmartCooldown()
        assert cd.is_in_cooldown("source_a") is False

    def test_enters_cooldown_after_failure(self):
        cd = SmartCooldown()
        cd.record_failure("source_a", "timeout")
        assert cd.is_in_cooldown("source_a") is True

    def test_cooldown_expires(self):
        # A 50ms window plus a fixed 60ms sleep was flaky under load: a scheduler
        # delay between record_failure and the first assertion could already
        # expire the cooldown. Give the "still active" assertion headroom and
        # poll for expiry instead of relying on one exact sleep.
        config = CooldownConfig(timeout=0.2)
        cd = SmartCooldown(config=config)
        cd.record_failure("source_a", "timeout")
        assert cd.is_in_cooldown("source_a") is True
        deadline = time.monotonic() + 2.0
        while cd.is_in_cooldown("source_a") and time.monotonic() < deadline:
            time.sleep(0.01)
        assert cd.is_in_cooldown("source_a") is False

    def test_success_resets_consecutive_failures(self):
        cd = SmartCooldown()
        cd.record_failure("source_a", "timeout")
        status_after_failure = cd.get_status("source_a")
        assert status_after_failure["consecutive_failures"] == 1

        cd.record_success("source_a")
        status_after_success = cd.get_status("source_a")
        assert status_after_success["consecutive_failures"] == 0

    def test_consecutive_failures_increase_cooldown(self):
        config = CooldownConfig(timeout=10.0)
        cd = SmartCooldown(config=config)

        # First failure: multiplier = 1.0, duration = 10.0
        cd.record_failure("source_a", "timeout")
        status_1 = cd.get_status("source_a")
        remaining_1 = status_1["cooldown_remaining"]

        # Reset cooldown_until so the next failure triggers a fresh window.
        cd._sources["source_a"].cooldown_until = 0.0

        # Second failure: multiplier = 1.5, duration = 15.0
        cd.record_failure("source_a", "timeout")
        status_2 = cd.get_status("source_a")
        remaining_2 = status_2["cooldown_remaining"]

        # Reset again for third failure.
        cd._sources["source_a"].cooldown_until = 0.0

        # Third failure: multiplier = 2.0, duration = 20.0
        cd.record_failure("source_a", "timeout")
        status_3 = cd.get_status("source_a")
        remaining_3 = status_3["cooldown_remaining"]

        # Each successive cooldown should be longer than the previous.
        assert remaining_2 > remaining_1
        assert remaining_3 > remaining_2

    def test_rate_limit_has_longer_cooldown_than_timeout(self):
        config = CooldownConfig(timeout=30.0, rate_limit=300.0)
        cd = SmartCooldown(config=config)

        cd.record_failure("source_timeout", "timeout")
        cd.record_failure("source_rate", "rate_limit")

        timeout_remaining = cd.get_status("source_timeout")["cooldown_remaining"]
        rate_remaining = cd.get_status("source_rate")["cooldown_remaining"]

        assert rate_remaining > timeout_remaining

    def test_different_sources_independent(self):
        cd = SmartCooldown()
        cd.record_failure("source_a", "timeout")

        assert cd.is_in_cooldown("source_a") is True
        assert cd.is_in_cooldown("source_b") is False

        status_a = cd.get_status("source_a")
        status_b = cd.get_status("source_b")
        assert status_a["total_failures"] == 1
        assert status_b["total_failures"] == 0


@pytest.mark.unit
class TestSourceState:
    """Tests for SourceState dataclass defaults."""

    def test_default_values(self):
        state = SourceState()
        assert state.cooldown_until == 0.0
        assert state.consecutive_failures == 0
        assert state.total_requests == 0
        assert state.total_failures == 0
        assert state.last_failure_time == 0.0
        assert state.last_success_time == 0.0


@pytest.mark.unit
class TestCooldownConfig:
    """Tests for CooldownConfig dataclass defaults."""

    def test_default_values(self):
        config = CooldownConfig()
        assert config.timeout == 30.0
        assert config.connection_error == 60.0
        assert config.rate_limit == 300.0
        assert config.forbidden == 600.0
        assert config.default == 120.0
        assert config.max_multiplier == 5.0


@pytest.mark.unit
class TestGetStatus:
    """Tests for the get_status method output format."""

    def test_status_keys(self):
        cd = SmartCooldown()
        status = cd.get_status("new_source")
        expected_keys = {
            "is_in_cooldown",
            "cooldown_remaining",
            "consecutive_failures",
            "total_requests",
            "total_failures",
        }
        assert set(status.keys()) == expected_keys

    def test_total_requests_counts_both(self):
        cd = SmartCooldown()
        cd.record_failure("src", "timeout")
        cd.record_success("src")
        cd.record_failure("src", "timeout")
        status = cd.get_status("src")
        assert status["total_requests"] == 3
        assert status["total_failures"] == 2
