import numpy as np

from consumer.consumer import score_event


class _FakeModel:
    def predict(self, x):
        # Return -1 for anomaly, 1 for normal
        return np.array([-1])

    def decision_function(self, x):
        return np.array([0.42])


def test_score_event_shape_and_types():
    event = {"temperature": "70", "pressure": "100", "vibration": "0.03"}
    is_anomaly, score = score_event(_FakeModel(), event)
    assert is_anomaly is True
    assert isinstance(score, float)

