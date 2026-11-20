import unittest
from pulsequeue.core.retry import FixedDelayRetry, LinearBackoffRetry, ExponentialBackoffRetry


class TestRetryPolicies(unittest.TestCase):
    def test_fixed_delay(self):
        policy = FixedDelayRetry(delay_seconds=3.5)
        self.assertEqual(policy.calculate_delay(1), 3.5)
        self.assertEqual(policy.calculate_delay(5), 3.5)

    def test_linear_backoff(self):
        policy = LinearBackoffRetry(base_delay=1.0, step=2.0, max_delay=10.0)
        self.assertEqual(policy.calculate_delay(1), 3.0)
        self.assertEqual(policy.calculate_delay(2), 5.0)
        self.assertEqual(policy.calculate_delay(10), 10.0)  # Capped at max_delay

    def test_exponential_backoff_without_jitter(self):
        policy = ExponentialBackoffRetry(base_delay=1.0, factor=2.0, max_delay=30.0, use_jitter=False)
        self.assertEqual(policy.calculate_delay(1), 1.0)
        self.assertEqual(policy.calculate_delay(2), 2.0)
        self.assertEqual(policy.calculate_delay(3), 4.0)
        self.assertEqual(policy.calculate_delay(4), 8.0)
        self.assertEqual(policy.calculate_delay(10), 30.0)  # Capped

    def test_exponential_backoff_with_jitter_bounds(self):
        policy = ExponentialBackoffRetry(base_delay=2.0, factor=2.0, max_delay=20.0, use_jitter=True, seed=42)
        for attempt in range(1, 6):
            delay = policy.calculate_delay(attempt)
            upper_bound = min(2.0 * (2.0 ** (attempt - 1)), 20.0)
            self.assertGreaterEqual(delay, 0.0)
            self.assertLessEqual(delay, upper_bound)


if __name__ == "__main__":
    unittest.main()
