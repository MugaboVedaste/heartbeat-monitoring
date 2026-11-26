"""
Unit tests for the alert rules in src/alerting.py.
No Kafka, PostgreSQL or mail server needed.
"""

from datetime import datetime, timedelta, timezone

from src.alerting import AlertDetector, build_email

START = datetime(2026, 11, 26, 10, 0, tzinfo=timezone.utc)


def feed(detector, statuses, customer="CUST-001", start_second=0):
    """Send readings with the given statuses, one second apart. Returns the alerts raised."""
    rate_for = {"high": 150, "low": 42, "normal": 72}
    alerts = []
    for i, status in enumerate(statuses):
        row = {
            "customer_id": customer,
            "recorded_at": START + timedelta(seconds=start_second + i),
            "heart_rate": rate_for[status],
            "status": status,
        }
        alert = detector.process(row)
        if alert:
            alerts.append(alert)
    return alerts


def make_detector(cooldown=300):
    return AlertDetector(consecutive=3, cooldown_seconds=cooldown, recovery=3)


def test_single_abnormal_reading_does_not_alert():
    assert feed(make_detector(), ["normal", "high", "normal", "normal"]) == []


def test_two_in_a_row_is_not_enough():
    assert feed(make_detector(), ["high", "high", "normal"]) == []


def test_three_high_in_a_row_raises_one_high_alert():
    alerts = feed(make_detector(), ["normal", "high", "high", "high"])

    assert len(alerts) == 1
    assert alerts[0].alert_type == "high"
    assert alerts[0].heart_rates == [150, 150, 150]
    assert alerts[0].started_at == START + timedelta(seconds=1)
    assert alerts[0].triggered_at == START + timedelta(seconds=3)


def test_three_low_in_a_row_raises_low_alert():
    alerts = feed(make_detector(), ["low", "low", "low"])
    assert [a.alert_type for a in alerts] == ["low"]


def test_mixed_high_and_low_is_not_a_streak():
    assert feed(make_detector(), ["high", "low", "high", "low"]) == []


def test_ongoing_problem_does_not_repeat_during_cooldown():
    alerts = feed(make_detector(cooldown=300), ["high"] * 20)
    assert len(alerts) == 1  # not one email per second


def test_ongoing_problem_alerts_again_after_cooldown():
    alerts = feed(make_detector(cooldown=10), ["high"] * 20)
    # alert at second 2, then again once 10 seconds have passed (second 12)
    assert [a.triggered_at for a in alerts] == [START + timedelta(seconds=2), START + timedelta(seconds=12)]


def test_recovered_alert_after_problem_ends():
    alerts = feed(make_detector(), ["high", "high", "high", "normal", "normal", "normal"])
    assert [a.alert_type for a in alerts] == ["high", "recovered"]


def test_no_recovered_alert_without_a_problem_first():
    assert feed(make_detector(), ["normal"] * 10) == []


def test_customers_are_tracked_separately():
    detector = make_detector()
    alerts = []
    for i in range(3):  # readings from two customers, interleaved
        alerts += feed(detector, ["high"], customer="CUST-001", start_second=i)
        alerts += feed(detector, ["normal"], customer="CUST-002", start_second=i)
    assert [(a.customer_id, a.alert_type) for a in alerts] == [("CUST-001", "high")]


def test_alert_email_contains_the_important_details():
    alert = feed(make_detector(), ["high", "high", "high"])[0]
    email = build_email(alert)

    assert "HIGH" in email["Subject"] and "CUST-001" in email["Subject"]
    body = email.get_content()
    assert "150, 150, 150" in body
    assert "3 readings in a row" in body


def test_recovered_email_says_back_to_normal():
    alerts = feed(make_detector(), ["low"] * 3 + ["normal"] * 3)
    email = build_email(alerts[-1])
    assert "RECOVERED" in email["Subject"]
    assert "back in the normal range" in email.get_content()
