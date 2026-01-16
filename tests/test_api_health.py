from api.main import health


def test_health_endpoint():
    assert health() == {"status": "ok"}

