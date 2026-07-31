import json
import logging
import os
import random
import signal
import sys
import threading
import time
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timezone
from queue import Empty, Full, Queue
from typing import Any, Deque, Dict, List, Optional, Tuple

from azure.eventhub import EventData, EventHubProducerClient
from azure.eventhub.exceptions import AuthenticationError, EventHubError
from requests.exceptions import (
    ChunkedEncodingError,
    ConnectionError as RequestsConnectionError,
    Timeout,
)
from requests_sse import EventSource


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s %(threadName)s %(message)s",
)

logger = logging.getLogger("wikimedia-eventhub-producer")


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class Config:
    eventhub_connection_string: str
    wikipedia_stream_url: str
    user_agent: str
    filter_change_type: Optional[str]
    filter_domain_suffix: Optional[str]
    queue_max_size: int
    batch_max_events: int
    batch_max_seconds: float
    max_consecutive_failures: int
    retry_initial_seconds: float
    retry_max_seconds: float
    retry_jitter_ratio: float
    log_metrics_seconds: float
    shutdown_timeout_seconds: float

    @classmethod
    def from_env(cls) -> "Config":
        connection_string = os.getenv("EVENTHUB_CONNECTION_STRING")
        if not connection_string:
            raise RuntimeError(
                "EVENTHUB_CONNECTION_STRING environment variable is required."
            )

        filter_change_type = os.getenv("FILTER_CHANGE_TYPE", "").strip() or None
        filter_domain_suffix = os.getenv("FILTER_DOMAIN_SUFFIX", "").strip() or None
        queue_max_size = int(os.getenv("QUEUE_MAX_SIZE", "5000"))
        batch_max_events = int(os.getenv("BATCH_MAX_EVENTS", "100"))
        batch_max_seconds = float(os.getenv("BATCH_MAX_SECONDS", "3"))
        max_consecutive_failures = int(
            os.getenv("MAX_CONSECUTIVE_FAILURES", "10")
        )
        retry_initial_seconds = float(
            os.getenv("RETRY_INITIAL_SECONDS", "5")
        )
        retry_max_seconds = float(os.getenv("RETRY_MAX_SECONDS", "300"))
        retry_jitter_ratio = float(os.getenv("RETRY_JITTER_RATIO", "0.20"))
        log_metrics_seconds = float(os.getenv("LOG_METRICS_SECONDS", "30"))
        shutdown_timeout_seconds = float(
            os.getenv("SHUTDOWN_TIMEOUT_SECONDS", "30")
        )

        if queue_max_size <= 0:
            raise ValueError("QUEUE_MAX_SIZE must be greater than 0.")
        if batch_max_events <= 0:
            raise ValueError("BATCH_MAX_EVENTS must be greater than 0.")
        if batch_max_seconds <= 0:
            raise ValueError("BATCH_MAX_SECONDS must be greater than 0.")
        if max_consecutive_failures <= 0:
            raise ValueError(
                "MAX_CONSECUTIVE_FAILURES must be greater than 0."
            )
        if retry_initial_seconds <= 0 or retry_max_seconds <= 0:
            raise ValueError("Retry delays must be greater than 0.")
        if retry_initial_seconds > retry_max_seconds:
            raise ValueError(
                "RETRY_INITIAL_SECONDS cannot exceed RETRY_MAX_SECONDS."
            )
        if not 0 <= retry_jitter_ratio <= 1:
            raise ValueError("RETRY_JITTER_RATIO must be between 0 and 1.")
        if log_metrics_seconds <= 0:
            raise ValueError("LOG_METRICS_SECONDS must be greater than 0.")
        if shutdown_timeout_seconds <= 0:
            raise ValueError(
                "SHUTDOWN_TIMEOUT_SECONDS must be greater than 0."
            )

        return cls(
            eventhub_connection_string=connection_string,
            wikipedia_stream_url=os.getenv(
                "WIKIPEDIA_STREAM_URL",
                "https://stream.wikimedia.org/v2/stream/recentchange",
            ),
            user_agent=os.getenv(
                "USER_AGENT",
                "ivanrazumovskyi-wikimedia-producer/1.0",
            ),
            filter_change_type=filter_change_type,
            filter_domain_suffix=filter_domain_suffix,
            queue_max_size=queue_max_size,
            batch_max_events=batch_max_events,
            batch_max_seconds=batch_max_seconds,
            max_consecutive_failures=max_consecutive_failures,
            retry_initial_seconds=retry_initial_seconds,
            retry_max_seconds=retry_max_seconds,
            retry_jitter_ratio=retry_jitter_ratio,
            log_metrics_seconds=log_metrics_seconds,
            shutdown_timeout_seconds=shutdown_timeout_seconds,
        )


# ---------------------------------------------------------------------------
# Shared runtime state
# ---------------------------------------------------------------------------

shutdown_event = threading.Event()
fatal_error_event = threading.Event()


@dataclass
class Metrics:
    read_events: int = 0
    filtered_canary_events: int = 0
    filtered_change_type_events: int = 0
    filtered_domain_events: int = 0
    malformed_events: int = 0
    queued_events: int = 0
    oversized_events: int = 0
    sent_events: int = 0
    sent_batches: int = 0
    wikipedia_reconnects: int = 0
    eventhub_reconnects: int = 0
    replayed_events: int = 0

    def snapshot(self) -> Dict[str, int]:
        return {
            "read_events": self.read_events,
            "filtered_canary_events": self.filtered_canary_events,
            "filtered_change_type_events": self.filtered_change_type_events,
            "filtered_domain_events": self.filtered_domain_events,
            "malformed_events": self.malformed_events,
            "queued_events": self.queued_events,
            "oversized_events": self.oversized_events,
            "sent_events": self.sent_events,
            "sent_batches": self.sent_batches,
            "wikipedia_reconnects": self.wikipedia_reconnects,
            "eventhub_reconnects": self.eventhub_reconnects,
            "replayed_events": self.replayed_events,
        }


metrics = Metrics()
metrics_lock = threading.Lock()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def calculate_backoff(
    base_delay: float,
    max_delay: float,
    jitter_ratio: float,
) -> float:
    capped = min(base_delay, max_delay)
    jitter = random.uniform(0, capped * jitter_ratio)
    return capped + jitter


def wait_or_shutdown(seconds: float) -> bool:
    """
    Wait for up to `seconds`.

    Returns True if shutdown was requested during the wait.
    """
    return shutdown_event.wait(timeout=seconds)


def handle_shutdown_signal(signum: int, _frame: Any) -> None:
    logger.info(
        "Signal %s received; graceful shutdown requested.",
        signum,
    )
    shutdown_event.set()


signal.signal(signal.SIGTERM, handle_shutdown_signal)
signal.signal(signal.SIGINT, handle_shutdown_signal)


def update_metric(name: str, amount: int = 1) -> None:
    with metrics_lock:
        current = getattr(metrics, name)
        setattr(metrics, name, current + amount)


def get_metrics_snapshot() -> Dict[str, int]:
    with metrics_lock:
        return metrics.snapshot()


def log_metrics() -> None:
    logger.info(
        "Metrics: %s",
        json.dumps(get_metrics_snapshot(), sort_keys=True),
    )


def mark_fatal_error(message: str) -> None:
    logger.error(message)
    fatal_error_event.set()
    shutdown_event.set()


# ---------------------------------------------------------------------------
# Wikimedia reader
# ---------------------------------------------------------------------------

def normalize_event(change: Dict[str, Any]) -> Dict[str, Any]:
    """
    Preserve the original Wikimedia event and add producer metadata.
    """
    normalized = dict(change)
    normalized["_producer"] = {
        "source": "wikimedia-recentchange",
        "producer_timestamp_utc": utc_now_iso(),
    }
    return normalized


def get_filter_reason(
    change: Dict[str, Any],
    config: Config,
) -> Optional[str]:
    """
    Return the reason an event should be filtered, or None when it should pass.

    By default, the producer keeps every real Wikimedia recent-change event.
    Optional environment variables can narrow the stream:

    - FILTER_CHANGE_TYPE=edit
    - FILTER_DOMAIN_SUFFIX=wikipedia.org

    The domain suffix filter accepts all language editions such as
    en.wikipedia.org, pl.wikipedia.org, and uk.wikipedia.org.
    """
    domain = change.get("meta", {}).get("domain", "")

    if domain == "canary":
        return "canary"

    if (
        config.filter_change_type is not None
        and change.get("type") != config.filter_change_type
    ):
        return "change_type"

    if (
        config.filter_domain_suffix is not None
        and not domain.endswith(config.filter_domain_suffix)
    ):
        return "domain"

    return None


def wikipedia_reader(
    config: Config,
    output_queue: Queue[Dict[str, Any]],
) -> None:
    consecutive_failures = 0
    retry_delay = config.retry_initial_seconds

    try:
        while not shutdown_event.is_set():
            try:
                headers = {"User-Agent": config.user_agent}

                logger.info(
                    "Connecting to Wikimedia SSE stream: %s",
                    config.wikipedia_stream_url,
                )

                with EventSource(
                    config.wikipedia_stream_url,
                    headers=headers,
                ) as stream:
                    logger.info("Connected to Wikimedia SSE stream.")
                    consecutive_failures = 0
                    retry_delay = config.retry_initial_seconds

                    for event in stream:
                        if shutdown_event.is_set():
                            break

                        if event.type != "message":
                            continue

                        update_metric("read_events")

                        try:
                            change = json.loads(event.data)
                        except (TypeError, ValueError, json.JSONDecodeError):
                            update_metric("malformed_events")
                            continue

                        filter_reason = get_filter_reason(change, config)
                        if filter_reason == "canary":
                            update_metric("filtered_canary_events")
                            continue
                        if filter_reason == "change_type":
                            update_metric("filtered_change_type_events")
                            continue
                        if filter_reason == "domain":
                            update_metric("filtered_domain_events")
                            continue

                        payload = normalize_event(change)

                        while not shutdown_event.is_set():
                            try:
                                output_queue.put(payload, timeout=1)
                                update_metric("queued_events")
                                break
                            except Full:
                                logger.warning(
                                    "Output queue is full (%s items); "
                                    "applying backpressure.",
                                    output_queue.qsize(),
                                )

            except (
                RequestsConnectionError,
                Timeout,
                ChunkedEncodingError,
            ) as exc:
                consecutive_failures += 1
                update_metric("wikipedia_reconnects")

                if consecutive_failures >= config.max_consecutive_failures:
                    mark_fatal_error(
                        "Too many consecutive Wikimedia failures "
                        f"({consecutive_failures})."
                    )
                    raise

                delay = calculate_backoff(
                    retry_delay,
                    config.retry_max_seconds,
                    config.retry_jitter_ratio,
                )

                logger.warning(
                    "Wikimedia stream failed (%s/%s): %s. "
                    "Reconnecting in %.1fs.",
                    consecutive_failures,
                    config.max_consecutive_failures,
                    exc,
                    delay,
                )

                if wait_or_shutdown(delay):
                    break

                retry_delay = min(
                    retry_delay * 2,
                    config.retry_max_seconds,
                )

            except Exception:
                mark_fatal_error("Unexpected Wikimedia reader failure.")
                raise

    except Exception:
        logger.exception("Wikimedia reader terminated with an error.")
    finally:
        logger.info("Wikimedia reader stopped.")


# ---------------------------------------------------------------------------
# Event Hub sender
# ---------------------------------------------------------------------------

def create_producer(config: Config) -> EventHubProducerClient:
    return EventHubProducerClient.from_connection_string(
        conn_str=config.eventhub_connection_string,
        retry_total=4,
        retry_backoff_factor=0.5,
        retry_backoff_max=10,
        retry_mode="exponential",
    )


def serialize_event(payload: Dict[str, Any]) -> EventData:
    return EventData(
        json.dumps(
            payload,
            ensure_ascii=False,
            separators=(",", ":"),
        )
    )


def build_batch(
    producer: EventHubProducerClient,
    payloads: List[Dict[str, Any]],
) -> Tuple[Any, List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    Build one EventDataBatch.

    Returns:
        batch:
            EventDataBatch ready to send.
        included:
            Payloads successfully included in this batch.
        remaining:
            Payloads that did not fit and must be processed later.

    Oversized single events are skipped and counted.
    """
    batch = producer.create_batch()
    included: List[Dict[str, Any]] = []
    remaining: List[Dict[str, Any]] = []

    for index, payload in enumerate(payloads):
        event_data = serialize_event(payload)

        try:
            batch.add(event_data)
            included.append(payload)
        except ValueError:
            if included:
                remaining.extend(payloads[index:])
                break

            update_metric("oversized_events")
            logger.error(
                "Dropping oversized event. user=%r title=%r",
                payload.get("user"),
                payload.get("title"),
            )

    return batch, included, remaining


def send_payloads(
    producer: EventHubProducerClient,
    payloads: List[Dict[str, Any]],
) -> int:
    """
    Send all supplied payloads, splitting them into SDK-sized batches.

    If send_batch raises, the caller still owns the original payload list and
    can replay it after reconnecting.
    """
    pending: Deque[Dict[str, Any]] = deque(payloads)
    sent_count = 0

    while pending:
        current_payloads = list(pending)
        batch, included, remaining = build_batch(
            producer,
            current_payloads,
        )

        if not included:
            pending = deque(remaining)
            continue

        producer.send_batch(batch)

        batch_size = len(included)
        sent_count += batch_size
        update_metric("sent_events", batch_size)
        update_metric("sent_batches")

        pending = deque(remaining)

    return sent_count


def eventhub_sender(
    config: Config,
    input_queue: Queue[Dict[str, Any]],
) -> None:
    consecutive_failures = 0
    retry_delay = config.retry_initial_seconds

    replay_buffer: Deque[Dict[str, Any]] = deque()
    producer: Optional[EventHubProducerClient] = None

    try:
        while (
            not shutdown_event.is_set()
            or not input_queue.empty()
            or replay_buffer
        ):
            try:
                if producer is None:
                    producer = create_producer(config)
                    producer.__enter__()
                    logger.info("Connected to Azure Event Hubs.")
                    consecutive_failures = 0
                    retry_delay = config.retry_initial_seconds

                batch_payloads: List[Dict[str, Any]] = []
                batch_started_at = time.monotonic()

                while len(batch_payloads) < config.batch_max_events:
                    if replay_buffer:
                        batch_payloads.append(replay_buffer.popleft())
                        continue

                    remaining = (
                        config.batch_max_seconds
                        - (time.monotonic() - batch_started_at)
                    )

                    if remaining <= 0:
                        break

                    if shutdown_event.is_set() and input_queue.empty():
                        break

                    try:
                        payload = input_queue.get(
                            timeout=max(0.1, remaining)
                        )
                        batch_payloads.append(payload)
                    except Empty:
                        break

                if not batch_payloads:
                    if shutdown_event.is_set() and input_queue.empty():
                        break
                    continue

                try:
                    sent = send_payloads(
                        producer,
                        batch_payloads,
                    )
                    if sent:
                        snapshot = get_metrics_snapshot()
                        if snapshot["sent_events"] % 500 < sent:
                            logger.info(
                                "%s total events sent.",
                                snapshot["sent_events"],
                            )

                except EventHubError:
                    replay_buffer.extendleft(reversed(batch_payloads))
                    update_metric(
                        "replayed_events",
                        len(batch_payloads),
                    )
                    raise

            except AuthenticationError:
                mark_fatal_error(
                    "Event Hub authentication failed. Check identity, "
                    "role assignment, or connection string."
                )
                raise

            except EventHubError as exc:
                consecutive_failures += 1
                update_metric("eventhub_reconnects")

                if producer is not None:
                    try:
                        producer.close()
                    except Exception:
                        logger.exception(
                            "Failed while closing Event Hub producer."
                        )
                    producer = None

                if consecutive_failures >= config.max_consecutive_failures:
                    mark_fatal_error(
                        "Too many consecutive Event Hub failures "
                        f"({consecutive_failures})."
                    )
                    raise

                delay = calculate_backoff(
                    retry_delay,
                    config.retry_max_seconds,
                    config.retry_jitter_ratio,
                )

                logger.warning(
                    "Event Hub failure (%s/%s): %s. "
                    "Recreating client in %.1fs.",
                    consecutive_failures,
                    config.max_consecutive_failures,
                    exc,
                    delay,
                )

                if wait_or_shutdown(delay):
                    continue

                retry_delay = min(
                    retry_delay * 2,
                    config.retry_max_seconds,
                )

            except Exception:
                mark_fatal_error("Unexpected Event Hub sender failure.")
                raise

    except Exception:
        logger.exception("Event Hub sender terminated with an error.")
    finally:
        if producer is not None:
            try:
                producer.close()
            except Exception:
                logger.exception(
                    "Failed while closing Event Hub producer."
                )

        logger.info("Event Hub sender stopped.")


# ---------------------------------------------------------------------------
# Application entry point
# ---------------------------------------------------------------------------

def main() -> int:
    try:
        config = Config.from_env()
    except Exception:
        logger.exception("Invalid application configuration.")
        return 1

    event_queue: Queue[Dict[str, Any]] = Queue(
        maxsize=config.queue_max_size
    )

    logger.info(
        "Producer starting. "
        "filter_change_type=%r filter_domain_suffix=%r "
        "batch_max_events=%s batch_max_seconds=%s "
        "queue_max_size=%s",
        config.filter_change_type,
        config.filter_domain_suffix,
        config.batch_max_events,
        config.batch_max_seconds,
        config.queue_max_size,
    )

    reader_thread = threading.Thread(
        target=wikipedia_reader,
        name="wikimedia-reader",
        args=(config, event_queue),
        daemon=False,
    )

    sender_thread = threading.Thread(
        target=eventhub_sender,
        name="eventhub-sender",
        args=(config, event_queue),
        daemon=False,
    )

    reader_thread.start()
    sender_thread.start()

    try:
        while reader_thread.is_alive() and sender_thread.is_alive():
            log_metrics()
            shutdown_event.wait(
                timeout=config.log_metrics_seconds
            )

    except KeyboardInterrupt:
        logger.info("Keyboard interrupt received.")
        shutdown_event.set()

    finally:
        shutdown_event.set()

        reader_thread.join(
            timeout=config.shutdown_timeout_seconds
        )
        sender_thread.join(
            timeout=config.shutdown_timeout_seconds
        )

        if reader_thread.is_alive():
            logger.error(
                "Wikimedia reader did not stop within timeout."
            )
            fatal_error_event.set()

        if sender_thread.is_alive():
            logger.error(
                "Event Hub sender did not stop within timeout."
            )
            fatal_error_event.set()

        log_metrics()

    if fatal_error_event.is_set():
        logger.error("Producer stopped because of a fatal error.")
        return 1

    logger.info("Producer stopped cleanly.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
