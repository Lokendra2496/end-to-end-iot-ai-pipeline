from shared.settings import get_settings


def test_settings_defaults_load():
    s = get_settings()
    assert s.KAFKA_TOPIC
    assert s.PG_DB

