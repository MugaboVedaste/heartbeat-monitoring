"""
Unit tests for the pipeline health rules in src/health.py.
No Kafka, PostgreSQL or mail server needed.
"""

from src.health import CheckResult, HealthTracker, build_health_email


def ok(name="kafka", label="Kafka broker"):
    return CheckResult(name, label, True, "reachable")


def fail(name="kafka", label="Kafka broker"):
    return CheckResult(name, label, False, "not reachable")


def kinds(notifications):
    return [(n.kind, n.result.name) for n in notifications]


def test_healthy_pipeline_sends_nothing():
    tracker = HealthTracker(failures_before_alert=2)
    for _ in range(5):
        assert tracker.update([ok()]) == []


def test_one_failed_check_is_treated_as_a_blip():
    tracker = HealthTracker(failures_before_alert=2)
    assert tracker.update([fail()]) == []
    assert tracker.update([ok()]) == []   # recovered before alerting: no email at all


def test_problem_reported_after_two_failures_in_a_row():
    tracker = HealthTracker(failures_before_alert=2)
    tracker.update([fail()])
    assert kinds(tracker.update([fail()])) == [("problem", "kafka")]


def test_ongoing_problem_is_reported_only_once():
    tracker = HealthTracker(failures_before_alert=2)
    notifications = []
    for _ in range(10):
        notifications += tracker.update([fail()])
    assert kinds(notifications) == [("problem", "kafka")]


def test_resolved_is_reported_when_check_passes_again():
    tracker = HealthTracker(failures_before_alert=2)
    tracker.update([fail()])
    tracker.update([fail()])
    assert kinds(tracker.update([ok()])) == [("resolved", "kafka")]


def test_checks_are_tracked_separately():
    tracker = HealthTracker(failures_before_alert=2)
    db = ("postgres", "PostgreSQL")
    tracker.update([fail(), ok(*db)])
    result = tracker.update([fail(), fail(*db)])  # kafka: 2nd failure, postgres: 1st failure
    assert kinds(result) == [("problem", "kafka")]
    assert kinds(tracker.update([ok(), fail(*db)])) == [("resolved", "kafka"), ("problem", "postgres")]


def test_problem_email_lists_problem_and_current_status():
    tracker = HealthTracker(failures_before_alert=1)
    results = [fail("group:heartbeat-db-writer", "Database consumer"), ok("postgres", "PostgreSQL")]
    email = build_health_email(tracker.update(results), results)

    assert email["Subject"] == "PIPELINE PROBLEM: Database consumer"
    body = email.get_content()
    assert "✗ Database consumer: not reachable" in body
    assert "✓ PostgreSQL: reachable" in body


def test_resolved_email_subject():
    tracker = HealthTracker(failures_before_alert=1)
    tracker.update([fail()])
    results = [ok()]
    email = build_health_email(tracker.update(results), results)
    assert email["Subject"] == "PIPELINE RESOLVED: Kafka broker"
