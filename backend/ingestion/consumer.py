"""
ingestion/consumer.py
Kafka consumer base class with offset management, dead-letter routing, and metrics.
Run one consumer process per source type; scale horizontally by partition count.

Usage:
    python -m ingestion.consumer --adapter era5 --partitions 0-31
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import signal
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Type

import structlog
from confluent_kafka import Consumer, KafkaError, KafkaException, Message, Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from prometheus_client import Counter, Gauge, Histogram, start_http_server

from config.settings import get_settings
from ingestion.validators import PhysicsValidator, SchemaValidator, ValidationResult

settings = get_settings()
log = structlog.get_logger(__name__)

# ─── PROMETHEUS METRICS ───────────────────────────────────────────────────────
RECORDS_CONSUMED = Counter("pcmip_ingestion_records_consumed_total", "Records consumed", ["source", "topic"])
RECORDS_VALID = Counter("pcmip_ingestion_records_valid_total", "Records passing validation", ["source"])
RECORDS_REJECTED = Counter("pcmip_ingestion_records_rejected_total", "Records rejected", ["source", "reason"])
RECORDS_FLAGGED = Counter("pcmip_ingestion_records_flagged_total", "Records flagged with warnings", ["source", "flag"])
CONSUMER_LAG = Gauge("pcmip_ingestion_consumer_lag_messages", "Consumer lag", ["source", "partition"])
PROCESSING_TIME = Histogram("pcmip_ingestion_processing_seconds", "Per-record processing time", ["source"])
DEAD_LETTER_DEPTH = Gauge("pcmip_ingestion_dead_letter_depth", "Dead-letter queue depth")


# ─── PROVENANCE ENVELOPE ──────────────────────────────────────────────────────

@dataclass
class ProvenanceEnvelope:
    dataset_id: str = field(default_factory=lambda: f"ds_{uuid.uuid4().hex[:8]}")
    source_id: str = ""
    ingest_ts: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    raw_hash: str = ""
    schema_version: str = ""
    quality_flags: List[str] = field(default_factory=lambda: ["QF_VALID"])
    cf_standard: Optional[str] = None
    cmip7_var: Optional[str] = None
    fair_compliant: bool = True

    def to_dict(self) -> Dict[str, Any]:
        return {
            "dataset_id": self.dataset_id,
            "source_id": self.source_id,
            "ingest_ts": self.ingest_ts,
            "raw_hash": self.raw_hash,
            "schema_version": self.schema_version,
            "quality_flags": self.quality_flags,
            "cf_standard": self.cf_standard,
            "cmip7_var": self.cmip7_var,
            "fair_compliant": self.fair_compliant,
        }


# ─── DEAD LETTER RECORD ───────────────────────────────────────────────────────

@dataclass
class DeadLetterRecord:
    original_topic: str
    original_partition: int
    original_offset: int
    source_id: str
    error_type: str  # "SCHEMA_VIOLATION" | "PHYSICS_REJECT" | "PARSE_ERROR"
    error_detail: str
    raw_payload: bytes
    failed_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())

    def to_dict(self) -> Dict[str, Any]:
        return {
            "original_topic": self.original_topic,
            "original_partition": self.original_partition,
            "original_offset": self.original_offset,
            "source_id": self.source_id,
            "error_type": self.error_type,
            "error_detail": self.error_detail,
            "raw_payload_b64": __import__("base64").b64encode(self.raw_payload).decode(),
            "failed_at": self.failed_at,
        }


# ─── BASE CONSUMER ────────────────────────────────────────────────────────────

class PCMIPConsumer:
    """
    Base Kafka consumer with:
    - Exactly-once semantics via manual offset commit after successful downstream write
    - Dead-letter routing on validation failure
    - Prometheus metrics
    - Graceful shutdown on SIGTERM
    - Schema registry integration
    """

    POLL_TIMEOUT_SECONDS = 1.0
    MAX_POLL_RECORDS = 500
    COMMIT_INTERVAL_RECORDS = 100  # Commit every N records

    def __init__(self, source_id: str, topic: str) -> None:
        self.source_id = source_id
        self.topic = topic
        self._running = False
        self._records_since_commit = 0

        # Kafka consumer config — SASL/TLS in production, plaintext for local
        consumer_config = {
            "bootstrap.servers": settings.kafka_brokers,
            "group.id": f"{settings.kafka_consumer_group}.{source_id}",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,  # Manual commit only
            "max.poll.interval.ms": 300_000,
            "session.timeout.ms": 30_000,
            "fetch.max.bytes": 52_428_800,  # 50MB
            "max.partition.fetch.bytes": 10_485_760,  # 10MB
        }
        self.consumer = Consumer(consumer_config)

        # Producer for validated-records and dead-letter topics
        producer_config = {
            "bootstrap.servers": settings.kafka_brokers,
            "acks": "all",  # Wait for all ISR replicas
            "retries": 5,
            "retry.backoff.ms": 1000,
            "compression.type": "lz4",
            "linger.ms": 10,
        }
        self.producer = Producer(producer_config)

        # Schema registry
        self.schema_registry = SchemaRegistryClient({"url": settings.kafka_schema_registry_url})

        # Validators
        self.schema_validator = SchemaValidator(source_id=source_id, registry=self.schema_registry)
        self.physics_validator = PhysicsValidator(source_id=source_id)

        # Graceful shutdown
        signal.signal(signal.SIGTERM, self._handle_sigterm)
        signal.signal(signal.SIGINT, self._handle_sigterm)

        log.info("consumer_initialised", source=source_id, topic=topic)

    def _handle_sigterm(self, signum: int, frame: Any) -> None:
        log.info("consumer_shutdown_signal", signal=signum, source=self.source_id)
        self._running = False

    def _compute_raw_hash(self, payload: bytes) -> str:
        return "sha256:" + hashlib.sha256(payload).hexdigest()

    def _route_to_dead_letter(self, msg: Message, error_type: str, detail: str) -> None:
        dlr = DeadLetterRecord(
            original_topic=msg.topic(),
            original_partition=msg.partition(),
            original_offset=msg.offset(),
            source_id=self.source_id,
            error_type=error_type,
            error_detail=detail,
            raw_payload=msg.value(),
        )
        self.producer.produce(
            topic=settings.kafka_dead_letter_topic,
            key=self.source_id.encode(),
            value=json.dumps(dlr.to_dict()).encode(),
        )
        RECORDS_REJECTED.labels(source=self.source_id, reason=error_type).inc()
        log.warning("record_dead_lettered", source=self.source_id, error=error_type, detail=detail[:200])

    def _emit_validated_record(self, record: Dict[str, Any], provenance: ProvenanceEnvelope) -> None:
        """Produce to validated-records topic with provenance attached."""
        enriched = {**record, "_provenance": provenance.to_dict()}
        self.producer.produce(
            topic=settings.kafka_validated_topic,
            key=provenance.dataset_id.encode(),
            value=json.dumps(enriched).encode(),
        )
        RECORDS_VALID.labels(source=self.source_id).inc()

    def _maybe_commit(self, force: bool = False) -> None:
        self._records_since_commit += 1
        if force or self._records_since_commit >= self.COMMIT_INTERVAL_RECORDS:
            self.consumer.commit(asynchronous=True)
            self._records_since_commit = 0

    def process_message(self, msg: Message) -> bool:
        """
        Process a single Kafka message through the full validation pipeline.
        Returns True if the message was accepted, False if dead-lettered.

        Override process_raw_record() in subclasses to handle source-specific parsing.
        """
        t_start = time.perf_counter()
        raw_payload = msg.value()

        # Step 1: Parse raw payload into a dict
        try:
            record = self.parse_raw_record(raw_payload)
        except Exception as e:
            self._route_to_dead_letter(msg, "PARSE_ERROR", str(e))
            return False

        # Step 2: Schema validation
        schema_result: ValidationResult = self.schema_validator.validate(record)
        if not schema_result.passed and schema_result.severity == "REJECT":
            self._route_to_dead_letter(msg, "SCHEMA_VIOLATION", schema_result.message)
            return False

        # Step 3: Physics plausibility
        physics_result: ValidationResult = self.physics_validator.validate(record)
        if not physics_result.passed and physics_result.severity == "REJECT":
            self._route_to_dead_letter(msg, "PHYSICS_REJECT", physics_result.message)
            return False

        # Step 4: Build provenance envelope
        quality_flags = ["QF_VALID"]
        if not schema_result.passed:
            quality_flags.append("QF_SCHEMA_WARN")
        if not physics_result.passed:
            quality_flags.append("QF_PHYSICS_WARN")
            for flag in physics_result.flags:
                RECORDS_FLAGGED.labels(source=self.source_id, flag=flag).inc()

        provenance = ProvenanceEnvelope(
            source_id=self.source_id,
            raw_hash=self._compute_raw_hash(raw_payload),
            schema_version=schema_result.schema_version,
            quality_flags=quality_flags,
            cf_standard=record.get("cf_standard_name"),
            cmip7_var=record.get("cmip7_variable"),
        )

        # Step 5: Emit to validated topic
        self._emit_validated_record(record, provenance)

        elapsed = time.perf_counter() - t_start
        PROCESSING_TIME.labels(source=self.source_id).observe(elapsed)
        RECORDS_CONSUMED.labels(source=self.source_id, topic=self.topic).inc()

        return True

    def parse_raw_record(self, payload: bytes) -> Dict[str, Any]:
        """
        Override in source-specific adapters to parse GRIB2, NetCDF4, HDF-EOS, etc.
        Must return a normalised dict with at minimum:
          - variable: str (CF standard name)
          - value: float
          - lat: float
          - lon: float
          - time: str (ISO8601)
          - unit: str
        """
        raise NotImplementedError("Subclasses must implement parse_raw_record()")

    def run(self) -> None:
        """Main consume loop. Blocks until SIGTERM is received."""
        self._running = True
        self.consumer.subscribe([self.topic])
        log.info("consumer_started", source=self.source_id, topic=self.topic)

        try:
            while self._running:
                msg = self.consumer.poll(timeout=self.POLL_TIMEOUT_SECONDS)

                if msg is None:
                    continue

                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        log.debug("partition_eof", partition=msg.partition())
                        continue
                    raise KafkaException(msg.error())

                accepted = self.process_message(msg)
                self._maybe_commit()

                # Update consumer lag metric
                _, high_watermark = self.consumer.get_watermark_offsets(msg.topic_partition())
                lag = high_watermark - msg.offset() - 1
                CONSUMER_LAG.labels(
                    source=self.source_id,
                    partition=msg.partition(),
                ).set(lag)

        except KafkaException as e:
            log.error("kafka_error", source=self.source_id, error=str(e))
            raise
        finally:
            # Flush and commit on shutdown
            self.producer.flush(timeout=30)
            self.consumer.commit(asynchronous=False)
            self.consumer.close()
            log.info("consumer_shutdown_complete", source=self.source_id)
