import runpy

from src.anomaly_detector import AnomalyDetector
from src.aiops_pipeline import load_data, run_pipeline
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


def test_detector_reports_each_anomaly_reason():
    detector = AnomalyDetector()
    record = {
        "timestamp": "2026-09-20T10:06:00",
        "service": "payment-service",
        "response_time_ms": 600,
        "cpu_percent": 90,
        "memory_percent": 90,
        "log_level": "WARNING",
        "message": "Multiple service thresholds exceeded"
    }

    event = detector.detect(record)

    assert event["reasons"] == [
        "High response time",
        "High CPU utilization",
        "High memory utilization",
        "Error log detected",
    ]


def test_pipeline_loads_data_and_consumes_detected_events():
    data = load_data("data/service_data.json")

    result = run_pipeline("data/service_data.json")

    assert result["records_processed"] == len(data)
    assert len(result["anomalies_detected"]) == 2
    assert result["events_consumed"] == []


def test_pipeline_script_prints_summary(capsys):
    runpy.run_module("src.aiops_pipeline", run_name="__main__")

    output = capsys.readouterr().out

    assert "AIOps Pipeline Result" in output
    assert "Records processed: 10" in output
    assert "Anomalies detected: 2" in output


def test_producer_publishes_event():
    topic = EventTopic("anomaly-events")
    producer = EventProducer(topic)

    event = {
        "type": "ANOMALY",
        "service": "payment-service"
    }

    assert producer.publish(event)
    assert len(topic.get_messages()) == 1


def test_producer_rejects_empty_event():
    producer = EventProducer(EventTopic("anomaly-events"))

    assert producer.publish(None) is False


def test_topic_clear_removes_published_events():
    topic = EventTopic("anomaly-events")
    topic.publish({"type": "ANOMALY"})

    topic.clear()

    assert topic.get_messages() == []


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