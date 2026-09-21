import json
import runpy
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parents[1] / "src"))

from calculations import area_of_circle, get_nth_fibonacci

from src.anomaly_detector import AnomalyDetector
from src.aiops_pipeline import run_pipeline
from src.event_consumer import EventConsumer
from src.event_producer import EventProducer
from src.event_topic import EventTopic


def test_normal_record_is_not_anomaly():
    detector = AnomalyDetector()

    record = {
        "timestamp": "2026-09-20T10:00:00",
        "service": "payment-service",
        "response_time_ms": 120,
        "cpu_percent": 42,
        "memory_percent": 51,
        "log_level": "INFO",
        "message": "Payment request processed successfully"
    }

    assert detector.detect(record) is None


def test_anomalous_record_is_detected():
    detector = AnomalyDetector()

    record = {
        "timestamp": "2026-09-20T10:05:00",
        "service": "payment-service",
        "response_time_ms": 610,
        "cpu_percent": 75,
        "memory_percent": 70,
        "log_level": "ERROR",
        "message": "Payment service timeout"
    }

    event = detector.detect(record)

    assert event is not None
    assert event["type"] == "ANOMALY"


def test_producer_publishes_event():
    topic = EventTopic("anomaly-events")
    producer = EventProducer(topic)

    event = {
        "type": "ANOMALY",
        "service": "payment-service"
    }

    assert producer.publish(event)
    assert len(topic.get_messages()) == 1


def test_consumer_receives_event():
    topic = EventTopic("anomaly-events")
    producer = EventProducer(topic)
    consumer = EventConsumer(topic)

    event = {
        "type": "ANOMALY",
        "service": "payment-service"
    }

    producer.publish(event)

    messages = consumer.consume()

    assert len(messages) == 1


@pytest.mark.parametrize(
    ("field", "value", "reason"),
    [
        ("response_time_ms", 501, "High response time"),
        ("cpu_percent", 81, "High CPU utilization"),
        ("memory_percent", 81, "High memory utilization"),
        ("log_level", "WARNING", "Error log detected"),
    ],
)
def test_anomaly_detector_reports_each_reason(field, value, reason):
    record = {
        "timestamp": "2026-09-20T10:05:00",
        "service": "payment-service",
        "response_time_ms": 120,
        "cpu_percent": 42,
        "memory_percent": 51,
        "log_level": "INFO",
        "message": "Service telemetry",
    }
    record[field] = value

    event = AnomalyDetector().detect(record)

    assert event["type"] == "ANOMALY"
    assert event["reasons"] == [reason]


def test_producer_rejects_empty_event():
    topic = EventTopic("anomaly-events")

    assert EventProducer(topic).publish(None) is False
    assert topic.get_messages() == []


def test_topic_can_clear_messages():
    topic = EventTopic("anomaly-events")
    topic.publish({"type": "ANOMALY"})

    topic.clear()

    assert topic.get_messages() == []


def test_run_pipeline_processes_records(tmp_path):
    data_file = tmp_path / "service_data.json"
    data_file.write_text(
        json.dumps([
            {
                "timestamp": "2026-09-20T10:00:00",
                "service": "payment-service",
                "response_time_ms": 120,
                "cpu_percent": 42,
                "memory_percent": 51,
                "log_level": "INFO",
                "message": "OK",
            },
            {
                "timestamp": "2026-09-20T10:05:00",
                "service": "payment-service",
                "response_time_ms": 610,
                "cpu_percent": 75,
                "memory_percent": 70,
                "log_level": "ERROR",
                "message": "Timeout",
            },
        ]),
        encoding="utf-8",
    )

    result = run_pipeline(data_file)

    assert result["records_processed"] == 2
    assert len(result["anomalies_detected"]) == 1
    assert result["events_consumed"] == []


def test_pipeline_script_entrypoint_runs(monkeypatch):
    class FakeConsumer:
        def __init__(self, topic):
            self.topic = topic

        def consume(self):
            return [{
                "service": "payment-service",
                "timestamp": "2026-09-20T10:05:00",
                "type": "ANOMALY",
                "reasons": ["High response time"],
            }]

    import event_consumer

    monkeypatch.setattr(event_consumer, "EventConsumer", FakeConsumer)

    runpy.run_path(
        str(Path(__file__).parents[1] / "src" / "aiops_pipeline.py"),
        run_name="__main__",
    )


def test_calculations_reject_negative_values():
    with pytest.raises(ValueError, match="Radius cannot be negative"):
        area_of_circle(-1)

    with pytest.raises(ValueError, match="n cannot be negative"):
        get_nth_fibonacci(-1)