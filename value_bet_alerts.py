#!/usr/bin/env python3

import argparse
import json
import os
import re
import ssl
import sys
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime, timezone
from html import escape
from html.parser import HTMLParser
from http.cookiejar import CookieJar
from math import ceil
from pathlib import Path
from typing import Any, Optional
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode, urljoin
from urllib.request import HTTPSHandler, HTTPCookieProcessor, OpenerDirector, Request, build_opener, urlopen

try:
    from pymongo import MongoClient
    from pymongo.collection import Collection
    from pymongo.errors import DuplicateKeyError, PyMongoError
except ImportError:
    MongoClient = None
    Collection = Any

    class DuplicateKeyError(Exception):
        pass

    class PyMongoError(Exception):
        pass


API_URL = "https://api.odds-api.io/v3/value-bets"
USER_AGENT = "evfinder-value-bet-alerts/2.0"
ODDS_API_SOURCE = "odds_api"
SUREBET_SOURCE = "surebet_valuebets"
SOURCE_ALIASES = {
    "odds_api": ODDS_API_SOURCE,
    "odds-api": ODDS_API_SOURCE,
    "surebet": SUREBET_SOURCE,
    "surebet_valuebets": SUREBET_SOURCE,
    "surebet-valuebets": SUREBET_SOURCE,
}
DEFAULT_MIN_EV = 0.05
DEFAULT_MIN_BET_ODDS = 1.40
DEFAULT_POLL_INTERVAL = 60
DEFAULT_STATE_FILE = ".seen_value_bets.json"
DEFAULT_MAX_REQUESTS_PER_HOUR = 100
DEFAULT_MONGODB_DATABASE = "evfinder"
DEFAULT_MONGODB_COLLECTION = "sent_alerts"
DEFAULT_ALERT_ARCHIVE_FILE = "logs/sent_alerts.jsonl"
DEFAULT_API_MAX_RETRIES = 3
DEFAULT_RATE_LIMIT_WARN_THRESHOLD = 10
DEFAULT_SCRAPED_MIN_EV = 2.0
DEFAULT_SCRAPED_MIN_BET_ODDS = 1.50
DEFAULT_SCRAPED_MIN_PROBABILITY = 0.35
DEFAULT_ENABLE_ODDS_API_SOURCE = True
DEFAULT_ENABLE_SUREBET_SOURCE = False
DEFAULT_SUREBET_BASE_URL = "https://en.surebet.com"
DEFAULT_SUREBET_LOGIN_TIMEOUT = 20
STATE_VERSION = 3


@dataclass
class RateLimitStatus:
    limit: Optional[int] = None
    remaining: Optional[int] = None
    reset_at: Optional[str] = None
    retry_after_seconds: Optional[int] = None


@dataclass
class NormalizedAlertCandidate:
    source: str
    kind: str
    source_record_id: str
    bookmaker: str
    sport: str
    tournament: str
    event_label: str
    start_time: str
    market_label: str
    odds: Optional[float]
    odds_display: str
    probability: Optional[float]
    probability_display: str
    ev_percent: Optional[float]
    ev_display: str
    line: Optional[float]
    line_display: str
    deep_link: str
    raw_payload: dict[str, Any]


class ApiRequestError(RuntimeError):
    def __init__(
        self,
        message: str,
        *,
        status_code: Optional[int] = None,
        rate_limit: Optional[RateLimitStatus] = None,
    ) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.rate_limit = rate_limit or RateLimitStatus()


class TimestampedStream:
    def __init__(self, stream: Any) -> None:
        self.stream = stream
        self.at_line_start = True

    def write(self, text: str) -> int:
        if not text:
            return 0

        written = 0
        for chunk in text.splitlines(keepends=True):
            if self.at_line_start:
                timestamp = datetime.now().astimezone().isoformat(timespec="seconds")
                prefix = f"[{timestamp}] "
                self.stream.write(prefix)
                written += len(prefix)

            self.stream.write(chunk)
            written += len(chunk)
            self.at_line_start = chunk.endswith(("\n", "\r"))

        return written

    def flush(self) -> None:
        self.stream.flush()

    def isatty(self) -> bool:
        return bool(getattr(self.stream, "isatty", lambda: False)())


class AlertArchive:
    def __init__(self, path: Path) -> None:
        self.path = path

    def record_sent_alert(
        self,
        *,
        candidate: NormalizedAlertCandidate,
        chat_target: str,
        message: str,
    ) -> None:
        entry = build_sent_alert_record(
            candidate=candidate,
            chat_target=chat_target,
            message=message,
        )

        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(entry, ensure_ascii=True, default=json_default))
            handle.write("\n")


class AlertSource(ABC):
    key: str
    label: str

    @abstractmethod
    def fetch_candidates(
        self,
        args: argparse.Namespace,
        *,
        ssl_context: Optional[ssl.SSLContext] = None,
    ) -> list[NormalizedAlertCandidate]:
        raise NotImplementedError

    def close(self) -> None:
        return


def json_default(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")


def round_to_step(value: float, step: float) -> float:
    return round(value / step) * step


def load_env_file(path: Path) -> None:
    if not path.exists():
        return

    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError:
        return

    for raw_line in lines:
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue

        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip("'\"")

        if key and key not in os.environ:
            os.environ[key] = value


def load_env_defaults() -> None:
    load_env_file(Path(".env"))
    load_env_file(Path(".env.example"))


def configure_timestamped_output() -> None:
    if not isinstance(sys.stdout, TimestampedStream):
        sys.stdout = TimestampedStream(sys.stdout)
    if not isinstance(sys.stderr, TimestampedStream):
        sys.stderr = TimestampedStream(sys.stderr)


def env_flag(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Poll Odds-API.io and Surebet value bets, then send new +EV alerts to Telegram."
    )
    parser.add_argument(
        "--api-key",
        default=os.getenv("ODDS_API_IO_KEY") or os.getenv("ODDS_API_KEY"),
        help="Odds-API.io API key. Defaults to ODDS_API_IO_KEY or ODDS_API_KEY.",
    )
    parser.add_argument(
        "--bookmakers",
        default=os.getenv("ODDS_API_BOOKMAKERS") or os.getenv("ODDS_API_BOOKMAKER"),
        help="Comma-separated bookmaker names, e.g. 'Bet365,Pinnacle'.",
    )
    parser.add_argument(
        "--telegram-bot-token",
        default=os.getenv("TELEGRAM_BOT_TOKEN"),
        help="Telegram bot token. Defaults to TELEGRAM_BOT_TOKEN.",
    )
    parser.add_argument(
        "--telegram-chat-target",
        "--telegram-chat-id",
        dest="telegram_chat_target",
        default=(
            os.getenv("TELEGRAM_CHAT_TARGET")
            or os.getenv("TELEGRAM_CHAT_USERNAME")
            or os.getenv("TELEGRAM_CHAT_ID")
        ),
        help=(
            "Telegram destination target. Accepts a numeric chat ID or an @channel/public chat "
            "username. Defaults to TELEGRAM_CHAT_TARGET, TELEGRAM_CHAT_USERNAME, or TELEGRAM_CHAT_ID."
        ),
    )
    parser.add_argument(
        "--min-ev",
        type=float,
        default=float(os.getenv("MIN_EXPECTED_VALUE", DEFAULT_MIN_EV)),
        help="Minimum expected value for Odds-API alerts. 0.05 means 5%%.",
    )
    parser.add_argument(
        "--min-bet-odds",
        type=float,
        default=float(os.getenv("MIN_BET_ODDS", DEFAULT_MIN_BET_ODDS)),
        help="Minimum bookmaker odds required for Odds-API alerts.",
    )
    parser.add_argument(
        "--scraped-min-ev",
        type=float,
        default=float(os.getenv("SCRAPED_MIN_EXPECTED_VALUE", DEFAULT_SCRAPED_MIN_EV)),
        help="Minimum expected value for Surebet scraped alerts. 2.0 means 2%%.",
    )
    parser.add_argument(
        "--scraped-min-bet-odds",
        type=float,
        default=float(os.getenv("SCRAPED_MIN_BET_ODDS", DEFAULT_SCRAPED_MIN_BET_ODDS)),
        help="Minimum odds required for Surebet scraped alerts.",
    )
    parser.add_argument(
        "--scraped-min-probability",
        type=float,
        default=float(os.getenv("SCRAPED_MIN_PROBABILITY", DEFAULT_SCRAPED_MIN_PROBABILITY)),
        help="Minimum probability required for Surebet scraped alerts.",
    )
    parser.add_argument(
        "--poll-interval",
        type=int,
        default=int(os.getenv("POLL_INTERVAL_SECONDS", DEFAULT_POLL_INTERVAL)),
        help="Polling interval in seconds.",
    )
    parser.add_argument(
        "--state-file",
        default=os.getenv("STATE_FILE", DEFAULT_STATE_FILE),
        help="Path to the JSON file used to remember sent alerts.",
    )
    parser.add_argument(
        "--alert-archive-file",
        default=os.getenv("ALERT_ARCHIVE_FILE", DEFAULT_ALERT_ARCHIVE_FILE),
        help="Path to the JSONL file used to archive successfully sent alerts.",
    )
    parser.add_argument(
        "--mongodb-uri",
        default=os.getenv("MONGODB_URI"),
        help="MongoDB connection URI. When set, duplicate alert state is stored in MongoDB.",
    )
    parser.add_argument(
        "--mongodb-database",
        default=os.getenv("MONGODB_DATABASE", DEFAULT_MONGODB_DATABASE),
        help="MongoDB database name for alert state.",
    )
    parser.add_argument(
        "--mongodb-collection",
        default=os.getenv("MONGODB_COLLECTION", DEFAULT_MONGODB_COLLECTION),
        help="MongoDB collection name for alert state.",
    )
    parser.add_argument(
        "--max-requests-per-hour",
        type=int,
        default=int(os.getenv("MAX_REQUESTS_PER_HOUR", DEFAULT_MAX_REQUESTS_PER_HOUR)),
        help="Maximum allowed Odds-API requests per hour across all bookmakers.",
    )
    parser.add_argument(
        "--api-max-retries",
        type=int,
        default=int(os.getenv("ODDS_API_MAX_RETRIES", DEFAULT_API_MAX_RETRIES)),
        help="Maximum retries for rate-limited Odds-API requests.",
    )
    parser.add_argument(
        "--rate-limit-warn-threshold",
        type=int,
        default=int(os.getenv("RATE_LIMIT_WARN_THRESHOLD", DEFAULT_RATE_LIMIT_WARN_THRESHOLD)),
        help="Log a warning when Odds-API remaining requests reach this threshold or lower.",
    )
    parser.add_argument(
        "--bankroll",
        type=float,
        default=float(os.getenv("BANKROLL", "0") or 0),
        help="Optional bankroll amount to show a stake amount as well as percentage.",
    )
    parser.add_argument(
        "--bankroll-currency",
        default=os.getenv("BANKROLL_CURRENCY", "EUR"),
        help="Currency label for bankroll-based stake amounts.",
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="Run a single check and exit.",
    )
    parser.add_argument(
        "--ignore-state",
        action="store_true",
        help="Ignore saved alerted bet IDs for this run.",
    )
    parser.add_argument(
        "--test-telegram",
        action="store_true",
        help="Send a Telegram test message and exit.",
    )
    parser.add_argument(
        "--sources",
        default=os.getenv("SOURCES"),
        help="Comma-separated source keys. Supported: odds_api,surebet_valuebets.",
    )
    parser.add_argument(
        "--surebet-username",
        default=os.getenv("SUREBET_USERNAME"),
        help="Surebet.com username or email.",
    )
    parser.add_argument(
        "--surebet-password",
        default=os.getenv("SUREBET_PASSWORD"),
        help="Surebet.com password.",
    )
    parser.add_argument(
        "--surebet-base-url",
        default=os.getenv("SUREBET_BASE_URL", DEFAULT_SUREBET_BASE_URL),
        help="Base URL for Surebet scraping.",
    )
    parser.add_argument(
        "--surebet-browser-headless",
        action="store_true",
        default=env_flag("SUREBET_BROWSER_HEADLESS", True),
        help="Kept for config compatibility. The v1 Python scraper uses an authenticated HTTP session.",
    )
    parser.add_argument(
        "--surebet-browser-binary",
        default=os.getenv("SUREBET_BROWSER_BINARY", ""),
        help="Kept for config compatibility with the Java reference app.",
    )
    parser.add_argument(
        "--surebet-browser-login-timeout-seconds",
        type=int,
        default=int(os.getenv("SUREBET_BROWSER_LOGIN_TIMEOUT_SECONDS", DEFAULT_SUREBET_LOGIN_TIMEOUT)),
        help="Login timeout for Surebet session bootstrap.",
    )
    parser.add_argument(
        "--ca-bundle",
        default=os.getenv("CA_BUNDLE"),
        help=(
            "Optional PEM bundle to trust for outbound HTTPS. Defaults to CA_BUNDLE. "
            "Service-specific bundles can still override this via TELEGRAM_CA_BUNDLE or "
            "ODDS_API_CA_BUNDLE."
        ),
    )
    parser.add_argument(
        "--telegram-ca-bundle",
        default=os.getenv("TELEGRAM_CA_BUNDLE"),
        help="Optional PEM bundle to trust specifically for Telegram HTTPS requests.",
    )
    parser.add_argument(
        "--odds-api-ca-bundle",
        default=os.getenv("ODDS_API_CA_BUNDLE"),
        help="Optional PEM bundle to trust specifically for Odds-API.io HTTPS requests.",
    )
    parser.add_argument(
        "--allow-insecure-telegram",
        action="store_true",
        default=env_flag("TELEGRAM_ALLOW_INSECURE_TLS"),
        help=(
            "Disable TLS certificate verification for Telegram requests only. "
            "Use only as a last resort."
        ),
    )
    return parser.parse_args()


def parse_source_names(raw_sources: Optional[str]) -> list[str]:
    if not raw_sources:
        return []

    parsed: list[str] = []
    for raw_source in raw_sources.split(","):
        key = raw_source.strip().lower()
        if not key:
            continue
        if key not in SOURCE_ALIASES:
            raise SystemExit(
                f"Unknown source {raw_source!r}. Supported values: odds_api, surebet_valuebets."
            )
        normalized = SOURCE_ALIASES[key]
        if normalized not in parsed:
            parsed.append(normalized)
    return parsed


def get_selected_sources(args: argparse.Namespace) -> list[str]:
    explicit = parse_source_names(args.sources)
    if explicit:
        return explicit

    selected: list[str] = []
    if env_flag("ENABLE_ODDS_API_SOURCE", DEFAULT_ENABLE_ODDS_API_SOURCE):
        selected.append(ODDS_API_SOURCE)
    if env_flag("ENABLE_SUREBET_SOURCE", DEFAULT_ENABLE_SUREBET_SOURCE):
        selected.append(SUREBET_SOURCE)
    return selected


def require_config(args: argparse.Namespace, selected_sources: list[str]) -> None:
    missing = []
    if not args.telegram_bot_token:
        missing.append("TELEGRAM_BOT_TOKEN / --telegram-bot-token")
    if not args.telegram_chat_target:
        missing.append(
            "TELEGRAM_CHAT_TARGET / TELEGRAM_CHAT_USERNAME / TELEGRAM_CHAT_ID / "
            "--telegram-chat-target"
        )

    if not args.test_telegram:
        if not selected_sources:
            missing.append(
                "at least one enabled source (set ENABLE_ODDS_API_SOURCE=1, ENABLE_SUREBET_SOURCE=1, "
                "or pass --sources)"
            )
        if ODDS_API_SOURCE in selected_sources:
            if not args.api_key:
                missing.append("ODDS_API_IO_KEY / --api-key")
            if not args.bookmakers:
                missing.append("ODDS_API_BOOKMAKER(S) / --bookmakers")
        if SUREBET_SOURCE in selected_sources:
            if not args.surebet_username:
                missing.append("SUREBET_USERNAME / --surebet-username")
            if not args.surebet_password:
                missing.append("SUREBET_PASSWORD / --surebet-password")

    if missing:
        raise SystemExit("Missing required configuration: " + ", ".join(missing))


def get_bookmakers(raw_bookmakers: str) -> list[str]:
    return [item.strip() for item in raw_bookmakers.split(",") if item.strip()]


def validate_polling_budget(args: argparse.Namespace, selected_sources: list[str]) -> None:
    if args.once or ODDS_API_SOURCE not in selected_sources:
        return

    requests_per_cycle = len(get_bookmakers(args.bookmakers))
    if requests_per_cycle == 0:
        raise SystemExit("No valid bookmakers were provided.")

    estimated_requests_per_hour = ceil(3600 / args.poll_interval) * requests_per_cycle
    if estimated_requests_per_hour > args.max_requests_per_hour:
        min_interval = ceil((3600 * requests_per_cycle) / args.max_requests_per_hour)
        raise SystemExit(
            "Configured polling exceeds the request budget: "
            f"~{estimated_requests_per_hour} requests/hour for {requests_per_cycle} bookmaker(s). "
            f"Use --poll-interval >= {min_interval} or increase MAX_REQUESTS_PER_HOUR."
        )


def load_alert_state(path: Path) -> tuple[set[str], dict[str, dict[str, Any]]]:
    if not path.exists():
        return set(), {}

    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return set(), {}

    if isinstance(data, list):
        return {item for item in data if isinstance(item, str)}, {}

    if not isinstance(data, dict):
        return set(), {}

    seen_ids_raw = data.get("seen_ids")
    seen_ids = (
        {item for item in seen_ids_raw if isinstance(item, str)}
        if isinstance(seen_ids_raw, list)
        else set()
    )

    sent_alerts_raw = data.get("sent_alerts")
    sent_alerts = (
        {
            key: value
            for key, value in sent_alerts_raw.items()
            if isinstance(key, str) and isinstance(value, dict)
        }
        if isinstance(sent_alerts_raw, dict)
        else {}
    )

    return seen_ids, sent_alerts


def save_alert_state(
    path: Path,
    seen_ids: set[str],
    sent_alerts: dict[str, dict[str, Any]],
) -> None:
    path.write_text(
        json.dumps(
            {
                "version": STATE_VERSION,
                "seen_ids": sorted(seen_ids),
                "sent_alerts": sent_alerts,
            },
            indent=2,
            sort_keys=True,
            default=json_default,
        ),
        encoding="utf-8",
    )


def build_ssl_context(
    *,
    cafile: Optional[str] = None,
    allow_insecure: bool = False,
) -> ssl.SSLContext:
    if allow_insecure:
        return ssl._create_unverified_context()

    try:
        return ssl.create_default_context(cafile=cafile)
    except FileNotFoundError as exc:
        raise RuntimeError(f"CA bundle file was not found: {cafile}") from exc
    except ssl.SSLError as exc:
        raise RuntimeError(f"Failed to load CA bundle {cafile!r}: {exc}") from exc


def resolve_ca_bundle(*candidates: Optional[str]) -> Optional[str]:
    for candidate in candidates:
        if candidate and candidate.strip():
            return candidate.strip()
    return None


def parse_int_header(value: Optional[str]) -> Optional[int]:
    if value is None or not value.strip():
        return None
    try:
        return int(value.strip())
    except ValueError:
        return None


def extract_rate_limit_status(headers: Any) -> RateLimitStatus:
    if headers is None:
        return RateLimitStatus()
    return RateLimitStatus(
        limit=parse_int_header(headers.get("x-ratelimit-limit")),
        remaining=parse_int_header(headers.get("x-ratelimit-remaining")),
        reset_at=headers.get("x-ratelimit-reset"),
        retry_after_seconds=parse_int_header(headers.get("Retry-After")),
    )


def log_rate_limit_status(bookmaker: str, status: RateLimitStatus, warn_threshold: int) -> None:
    if (
        status.limit is None
        and status.remaining is None
        and status.reset_at is None
        and status.retry_after_seconds is None
    ):
        return

    parts = [f"[info] Odds-API usage for {bookmaker}:"]
    if status.remaining is not None:
        parts.append(f"remaining={status.remaining}")
    if status.limit is not None:
        parts.append(f"limit={status.limit}")
    if status.reset_at:
        parts.append(f"reset={status.reset_at}")
    print(" ".join(parts))

    if status.remaining is not None and status.remaining <= warn_threshold:
        warning = f"[warn] Odds-API remaining requests are low for {bookmaker}: {status.remaining}"
        if status.reset_at:
            warning += f" (reset={status.reset_at})"
        print(warning, file=sys.stderr)


def compute_retry_delay_seconds(rate_limit: RateLimitStatus, attempt: int) -> int:
    if rate_limit.retry_after_seconds is not None and rate_limit.retry_after_seconds > 0:
        return rate_limit.retry_after_seconds
    return min(60, 2 ** max(0, attempt - 1))


def http_json(
    url: str,
    *,
    method: str = "GET",
    payload: Optional[dict[str, Any]] = None,
    ssl_context: Optional[ssl.SSLContext] = None,
) -> tuple[Any, RateLimitStatus]:
    body = None
    headers = {"User-Agent": USER_AGENT}

    if payload is not None:
        body = json.dumps(payload).encode("utf-8")
        headers["Content-Type"] = "application/json"

    request = Request(url, data=body, headers=headers, method=method)

    try:
        with urlopen(request, timeout=30, context=ssl_context) as response:
            charset = response.headers.get_content_charset() or "utf-8"
            raw = response.read().decode(charset)
            return json.loads(raw), extract_rate_limit_status(response.headers)
    except HTTPError as exc:
        charset = exc.headers.get_content_charset() or "utf-8"
        raw = exc.read().decode(charset, errors="replace").strip()
        detail = raw
        rate_limit = extract_rate_limit_status(exc.headers)

        if raw:
            try:
                parsed = json.loads(raw)
                if isinstance(parsed, dict) and parsed.get("error"):
                    detail = str(parsed["error"])
            except json.JSONDecodeError:
                pass

        message = f"HTTP {exc.code}"
        if detail:
            message += f": {detail}"
        raise ApiRequestError(
            message,
            status_code=exc.code,
            rate_limit=rate_limit,
        ) from exc


def http_request(
    url: str,
    *,
    method: str = "GET",
    payload: Optional[dict[str, Any]] = None,
    ssl_context: Optional[ssl.SSLContext] = None,
) -> tuple[int, str, RateLimitStatus]:
    body = None
    headers = {"User-Agent": USER_AGENT}

    if payload is not None:
        body = json.dumps(payload).encode("utf-8")
        headers["Content-Type"] = "application/json"

    request = Request(url, data=body, headers=headers, method=method)

    try:
        with urlopen(request, timeout=30, context=ssl_context) as response:
            charset = response.headers.get_content_charset() or "utf-8"
            raw = response.read().decode(charset, errors="replace")
            return response.status, raw, extract_rate_limit_status(response.headers)
    except HTTPError as exc:
        charset = exc.headers.get_content_charset() or "utf-8"
        raw = exc.read().decode(charset, errors="replace").strip()
        detail = raw or exc.reason
        raise ApiRequestError(
            f"HTTP {exc.code}: {detail}",
            status_code=exc.code,
            rate_limit=extract_rate_limit_status(exc.headers),
        ) from exc


def fetch_value_bets(
    api_key: str,
    bookmaker: str,
    *,
    ssl_context: Optional[ssl.SSLContext] = None,
    max_retries: int = DEFAULT_API_MAX_RETRIES,
    warn_threshold: int = DEFAULT_RATE_LIMIT_WARN_THRESHOLD,
) -> list[dict[str, Any]]:
    query = urlencode(
        {
            "apiKey": api_key,
            "bookmaker": bookmaker,
            "includeEventDetails": "true",
        }
    )
    url = f"{API_URL}?{query}"
    for attempt in range(1, max_retries + 2):
        try:
            data, rate_limit = http_json(url, ssl_context=ssl_context)
            log_rate_limit_status(bookmaker, rate_limit, warn_threshold)
            if not isinstance(data, list):
                raise ValueError(f"Unexpected API response for bookmaker {bookmaker!r}: {data!r}")
            return data
        except ApiRequestError as exc:
            if exc.status_code != 429 or attempt > max_retries:
                raise

            delay_seconds = compute_retry_delay_seconds(exc.rate_limit, attempt)
            message = (
                f"[warn] Odds-API rate limit hit for {bookmaker}. "
                f"Retrying in {delay_seconds}s ({attempt}/{max_retries})."
            )
            if exc.rate_limit.reset_at:
                message += f" reset={exc.rate_limit.reset_at}"
            print(message, file=sys.stderr)
            time.sleep(delay_seconds)

    raise RuntimeError(f"Failed to fetch value bets for {bookmaker}: retry loop exited unexpectedly")


def get_bet_odds(bet: dict[str, Any]) -> str:
    bet_side = str(bet.get("betSide", ""))
    bookmaker_odds = bet.get("bookmakerOdds")
    if not isinstance(bookmaker_odds, dict):
        return "N/A"

    value = bookmaker_odds.get(bet_side)
    return str(value) if value is not None else "N/A"


def get_market_line(bet: dict[str, Any]) -> Any:
    market = bet.get("market") if isinstance(bet.get("market"), dict) else {}
    bookmaker_odds = bet.get("bookmakerOdds") if isinstance(bet.get("bookmakerOdds"), dict) else {}

    for source in (bet, market, bookmaker_odds):
        for key in ("line", "hdp", "point"):
            value = source.get(key)
            if value is not None:
                return value

    return None


def normalize_ev_percent(expected_value: Any) -> Optional[float]:
    try:
        ev = float(str(expected_value).strip().rstrip("%").replace(",", "."))
    except (TypeError, ValueError):
        return None

    if ev >= 100:
        return ev - 100
    if ev <= 1:
        return ev * 100
    return ev


def parse_float(value: Any) -> Optional[float]:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def parse_numeric_text(value: Any) -> Optional[float]:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    text = text.replace("\xa0", " ").replace(",", ".")
    text = re.sub(r"[^0-9.+-]", "", text)
    if not text or text in {".", "-", "+", "+.", "-."}:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def parse_probability(value: Any) -> Optional[float]:
    if value is None:
        return None
    raw = str(value).strip()
    if not raw:
        return None
    numeric = parse_numeric_text(raw)
    if numeric is None:
        return None
    if "%" in raw or numeric > 1:
        return numeric / 100
    return numeric


def format_ev_percent(expected_value: Any) -> str:
    percent = normalize_ev_percent(expected_value)
    if percent is None:
        return "N/A"
    return f"{percent:.2f}%"


def format_probability(probability: Optional[float], raw_value: Any = None) -> str:
    if raw_value is not None and str(raw_value).strip():
        raw = str(raw_value).strip()
        if raw.endswith("%"):
            return raw
    if probability is None:
        return "N/A"
    return f"{probability * 100:.2f}%"


def format_comparable_number(value: Any, decimals: int) -> str:
    numeric = parse_numeric_text(value)
    if numeric is None:
        return str(value or "")
    return f"{numeric:.{decimals}f}"


def normalize_threshold_percent(min_ev: float) -> float:
    return min_ev * 100 if min_ev <= 1 else min_ev


def describe_bet(bet: dict[str, Any]) -> str:
    event = bet.get("event") if isinstance(bet.get("event"), dict) else {}
    market = bet.get("market") if isinstance(bet.get("market"), dict) else {}

    market_name = str(market.get("name", bet.get("market", "Unknown")))
    bet_side = str(bet.get("betSide", "Unknown"))
    line = get_market_line(bet)

    if market_name.lower() == "ml":
        if bet_side == "home":
            return f"Back {event.get('home', 'home team')} to win"
        if bet_side == "away":
            return f"Back {event.get('away', 'away team')} to win"
        if bet_side == "draw":
            return "Back draw"
        return f"Back moneyline: {bet_side}"

    if market_name.lower() == "spread":
        if bet_side == "home":
            team = event.get("home", "home team")
            handicap = f"-{line}" if line is not None else None
        elif bet_side == "away":
            team = event.get("away", "away team")
            handicap = f"+{line}" if line is not None else None
        else:
            team = bet_side
            handicap = str(line) if line is not None else None

        return f"Back {team} {handicap}" if handicap is not None else f"Back {team} spread"

    if market_name.lower() == "totals":
        if bet_side == "home":
            side_label = "Over"
        elif bet_side == "away":
            side_label = "Under"
        else:
            side_label = bet_side

        return f"Back {side_label} {line}" if line is not None else f"Back totals: {side_label}"

    line_suffix = f" @ {line}" if line is not None else ""
    return f"Back {market_name} {bet_side}{line_suffix}"


def calculate_stake_details(bet: dict[str, Any]) -> Optional[dict[str, float]]:
    market = bet.get("market") if isinstance(bet.get("market"), dict) else {}

    bet_side = str(bet.get("betSide", ""))
    sharp_odd = parse_float(market.get(bet_side))
    bet_odd = parse_float(get_bet_odds(bet))
    if sharp_odd is None or bet_odd is None or bet_odd <= 1:
        return None

    outcomes = []
    for key, value in market.items():
        if key in {"name", "max", "hdp", "line", "point"}:
            continue
        odd = parse_float(value)
        if odd is None or odd <= 1:
            continue
        outcomes.append((key, odd))

    if len(outcomes) < 2:
        return None

    probabilities = {key: 1 / odd for key, odd in outcomes}
    hold = sum(probabilities.values())
    payout = 1 / hold
    margin = hold - 1
    outcome_count = len(outcomes)
    fair_probability = probabilities.get(bet_side)
    if fair_probability is None:
        return None

    fair_probability -= margin / outcome_count
    if fair_probability <= 0:
        return None

    fair_odd = 1 / fair_probability
    net_odds = bet_odd - 1
    if net_odds <= 0:
        return None

    kelly = ((net_odds * fair_probability) - (1 - fair_probability)) / net_odds
    quarter_kelly = max(0.0, kelly / 4)
    suggested_stake = min(1.5, round_to_step(quarter_kelly * 100, 0.25))

    return {
        "suggested_stake_percent": suggested_stake,
        "payout_percent": payout * 100,
        "fair_odd": fair_odd,
        "sharp_odd": sharp_odd,
        "outcome_count": float(outcome_count),
    }


def calculate_surebet_stake_details(candidate: NormalizedAlertCandidate) -> Optional[dict[str, float]]:
    if candidate.source != SUREBET_SOURCE:
        return None

    if candidate.odds is None or candidate.odds <= 1:
        return None
    if candidate.ev_percent is None:
        return None

    overvalue_ratio = 1 + (candidate.ev_percent / 100)
    fair_probability = overvalue_ratio / candidate.odds
    if fair_probability <= 0 or fair_probability >= 1:
        return None

    fair_odd = 1 / fair_probability
    net_odds = candidate.odds - 1
    if net_odds <= 0:
        return None

    kelly = ((net_odds * fair_probability) - (1 - fair_probability)) / net_odds
    quarter_kelly = max(0.0, kelly / 4)
    suggested_stake = min(1.5, round_to_step(quarter_kelly * 100, 0.25))

    return {
        "suggested_stake_percent": suggested_stake,
        "fair_probability_percent": fair_probability * 100,
        "fair_odd": fair_odd,
        "overvalue_ratio": overvalue_ratio,
    }


def format_stake_amount(stake_percent: float, bankroll: float, currency: str) -> str:
    amount = bankroll * (stake_percent / 100)
    return f"{amount:.2f} {currency}"


def normalize_odds_api_candidate(bet: dict[str, Any]) -> NormalizedAlertCandidate:
    event = bet.get("event") if isinstance(bet.get("event"), dict) else {}
    market = bet.get("market") if isinstance(bet.get("market"), dict) else {}
    bet_side = str(bet.get("betSide", ""))
    market_name = str(market.get("name", bet.get("market", "Unknown")))
    event_label = f"{event.get('home', 'Unknown')} vs {event.get('away', 'Unknown')}"
    odds = parse_float(get_bet_odds(bet))
    ev_percent = normalize_ev_percent(bet.get("expectedValue"))
    line = parse_numeric_text(get_market_line(bet))

    return NormalizedAlertCandidate(
        source=ODDS_API_SOURCE,
        kind="valuebet",
        source_record_id=str(bet.get("id", "")),
        bookmaker=str(bet.get("bookmaker", "")),
        sport=str(event.get("sport", "")),
        tournament=str(event.get("league", "")),
        event_label=event_label,
        start_time=str(event.get("date", "")),
        market_label=f"{market_name} {bet_side}".strip(),
        odds=odds,
        odds_display=str(get_bet_odds(bet)),
        probability=None,
        probability_display="",
        ev_percent=ev_percent,
        ev_display=format_ev_percent(bet.get("expectedValue")),
        line=line,
        line_display=str(get_market_line(bet) or ""),
        deep_link=str(
            (bet.get("bookmakerOdds") if isinstance(bet.get("bookmakerOdds"), dict) else {}).get("href", "")
        ),
        raw_payload=bet,
    )


def split_tbody_rows(html: str) -> list[str]:
    pattern = re.compile(
        r"<tbody\b[^>]*\bclass=(['\"])[^'\"]*\bvaluebet_record\b[^'\"]*\1[^>]*>.*?</tbody>",
        re.IGNORECASE | re.DOTALL,
    )
    return pattern.findall(html)


class SurebetValuebetRowParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.row_id = ""
        self.bookmaker = ""
        self.bookmaker_href = ""
        self.sport = ""
        self.event = ""
        self.event_href = ""
        self.market = ""
        self.odds = ""
        self.odds_href = ""
        self.tournament = ""
        self.time = ""
        self.text_center_values: list[str] = []
        self.current_td_classes: set[str] = set()
        self.current_td_text: list[str] = []
        self.anchor_depth = 0
        self.capture_bookmaker = False
        self.capture_event = False
        self.capture_market = False
        self.capture_odds = False
        self.capture_sport = False
        self.capture_tournament = False
        self.inside_sup = False
        self.inside_tbody = False

    def handle_starttag(self, tag: str, attrs: list[tuple[str, Optional[str]]]) -> None:
        attr_map = {key: value or "" for key, value in attrs}
        classes = set(attr_map.get("class", "").split())

        if tag == "tbody":
            if "valuebet_record" in classes:
                self.inside_tbody = True
                self.row_id = attr_map.get("id", "")
            return

        if not self.inside_tbody:
            return

        if tag == "td":
            self.current_td_classes = classes
            self.current_td_text = []
            return

        if tag == "sup":
            self.inside_sup = True
            return

        if tag == "a":
            self.anchor_depth += 1
            href = attr_map.get("href", "")
            if {"booker", "booker-first"}.issubset(self.current_td_classes) and "minor" not in classes and not self.bookmaker:
                self.capture_bookmaker = True
                self.bookmaker_href = href
            elif "event" in self.current_td_classes and not self.event:
                self.capture_event = True
                self.event_href = href
            elif "value" in self.current_td_classes and "value_link" in classes:
                self.capture_odds = True
                self.odds_href = href
            return

        if tag == "span" and "minor" in classes:
            if {"booker", "booker-first"}.issubset(self.current_td_classes) and self.anchor_depth == 0:
                self.capture_sport = True
            elif "event" in self.current_td_classes:
                self.capture_tournament = True
            return

        if tag == "abbr" and "coeff" in self.current_td_classes and not self.inside_sup:
            self.capture_market = True

    def handle_endtag(self, tag: str) -> None:
        if tag == "tbody":
            self.inside_tbody = False
            return

        if not self.inside_tbody:
            return

        if tag == "sup":
            self.inside_sup = False
            return

        if tag == "a":
            self.anchor_depth = max(0, self.anchor_depth - 1)
            self.capture_bookmaker = False
            self.capture_event = False
            self.capture_odds = False
            return

        if tag == "span":
            self.capture_sport = False
            self.capture_tournament = False
            return

        if tag == "abbr":
            self.capture_market = False
            return

        if tag == "td":
            text = self._flush_td_text()
            if "time" in self.current_td_classes and text and not self.time:
                self.time = text
            if "text-center" in self.current_td_classes and text:
                self.text_center_values.append(text)
            self.current_td_classes = set()
            self.current_td_text = []

    def handle_data(self, data: str) -> None:
        if not self.inside_tbody:
            return
        text = data.strip()
        if not text:
            return

        self.current_td_text.append(text)
        if self.capture_bookmaker:
            self.bookmaker += text
        elif self.capture_event:
            self.event += text
        elif self.capture_market:
            self.market += text
        elif self.capture_odds:
            self.odds += text
        elif self.capture_sport:
            self.sport += text
        elif self.capture_tournament:
            self.tournament += text

    def _flush_td_text(self) -> str:
        return " ".join(part.strip() for part in self.current_td_text if part.strip()).strip()

    def to_record(self) -> dict[str, str]:
        probability = self.text_center_values[0] if self.text_center_values else ""
        overvalue = self.text_center_values[-1] if len(self.text_center_values) >= 2 else ""
        return {
            "row_id": self.row_id.strip(),
            "bookmaker": self.bookmaker.strip(),
            "event": self.event.strip(),
            "market": self.market.strip(),
            "odds": self.odds.strip(),
            "probability": probability.strip(),
            "overvalue": overvalue.strip(),
            "sport": self.sport.strip(),
            "tournament": self.tournament.strip(),
            "time": self.time.strip(),
            "event_href": self.event_href.strip(),
            "odds_href": self.odds_href.strip(),
        }


def parse_surebet_valuebets_html(html: str) -> list[dict[str, str]]:
    rows = re.findall(
        r"(<tbody\b[^>]*\bclass=(['\"])[^'\"]*\bvaluebet_record\b[^'\"]*\2[^>]*>.*?</tbody>)",
        html,
        re.IGNORECASE | re.DOTALL,
    )
    parsed: list[dict[str, str]] = []
    for row_html, _quote in rows:
        parser = SurebetValuebetRowParser()
        parser.feed(row_html)
        record = parser.to_record()
        if record["row_id"]:
            parsed.append(record)
    return parsed


def normalize_surebet_candidate(record: dict[str, str], base_url: str) -> NormalizedAlertCandidate:
    deep_link = record.get("odds_href") or record.get("event_href") or ""
    return NormalizedAlertCandidate(
        source=SUREBET_SOURCE,
        kind="valuebet",
        source_record_id=record.get("row_id", ""),
        bookmaker=record.get("bookmaker", ""),
        sport=record.get("sport", ""),
        tournament=record.get("tournament", ""),
        event_label=record.get("event", ""),
        start_time=record.get("time", ""),
        market_label=record.get("market", ""),
        odds=parse_numeric_text(record.get("odds")),
        odds_display=record.get("odds", ""),
        probability=parse_probability(record.get("probability")),
        probability_display=format_probability(parse_probability(record.get("probability")), record.get("probability")),
        ev_percent=normalize_ev_percent(record.get("overvalue")),
        ev_display=format_ev_percent(record.get("overvalue")),
        line=None,
        line_display="",
        deep_link=urljoin(base_url, deep_link) if deep_link else "",
        raw_payload=record,
    )


class OddsApiValueBetSource(AlertSource):
    key = ODDS_API_SOURCE
    label = "Odds-API"

    def fetch_candidates(
        self,
        args: argparse.Namespace,
        *,
        ssl_context: Optional[ssl.SSLContext] = None,
    ) -> list[NormalizedAlertCandidate]:
        candidates: list[NormalizedAlertCandidate] = []
        for bookmaker in get_bookmakers(args.bookmakers):
            try:
                bets = fetch_value_bets(
                    args.api_key,
                    bookmaker,
                    ssl_context=ssl_context,
                    max_retries=args.api_max_retries,
                    warn_threshold=args.rate_limit_warn_threshold,
                )
            except (RuntimeError, URLError, ValueError, json.JSONDecodeError) as exc:
                print(f"[error] Failed to fetch value bets for {bookmaker}: {exc}", file=sys.stderr)
                continue

            candidates.extend(normalize_odds_api_candidate(bet) for bet in bets)
        return candidates


class SurebetSession:
    def __init__(
        self,
        *,
        base_url: str,
        username: str,
        password: str,
        ssl_context: Optional[ssl.SSLContext] = None,
        login_timeout_seconds: int = DEFAULT_SUREBET_LOGIN_TIMEOUT,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.username = username
        self.password = password
        self.login_timeout_seconds = login_timeout_seconds
        self.cookie_jar = CookieJar()
        self.opener = self._build_opener(ssl_context=ssl_context)
        self.logged_in = False

    def _build_opener(self, *, ssl_context: Optional[ssl.SSLContext]) -> OpenerDirector:
        handlers = [HTTPCookieProcessor(self.cookie_jar)]
        if ssl_context is not None:
            handlers.append(HTTPSHandler(context=ssl_context))
        return build_opener(*handlers)

    def _request_text(
        self,
        url: str,
        *,
        method: str = "GET",
        data: Optional[dict[str, Any]] = None,
        referer: Optional[str] = None,
    ) -> tuple[str, str]:
        headers = {
            "User-Agent": USER_AGENT,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        }
        if referer:
            headers["Referer"] = referer

        body = None
        if data is not None:
            headers["Content-Type"] = "application/x-www-form-urlencoded"
            body = urlencode(data).encode("utf-8")

        request = Request(url, data=body, headers=headers, method=method)
        with self.opener.open(request, timeout=max(30, self.login_timeout_seconds)) as response:
            charset = response.headers.get_content_charset() or "utf-8"
            html = response.read().decode(charset, errors="replace")
            return html, response.geturl()

    def _extract_authenticity_token(self, html: str) -> Optional[str]:
        patterns = [
            r'<meta\s+name=["\']csrf-token["\']\s+content=["\']([^"\']+)["\']',
            r'<input[^>]+name=["\']authenticity_token["\'][^>]+value=["\']([^"\']+)["\']',
        ]
        for pattern in patterns:
            match = re.search(pattern, html, re.IGNORECASE)
            if match:
                return match.group(1)
        return None

    def _looks_logged_in(self, html: str, final_url: str) -> bool:
        if "/users/sign_in" in final_url:
            return False
        if "href=\"/users/sign_out\"" in html or "href='/users/sign_out'" in html:
            return True
        if "id=\"sign-in-form-submit-button\"" in html or "id='sign-in-form-submit-button'" in html:
            return False
        return True

    def login(self) -> None:
        sign_in_url = urljoin(self.base_url + "/", "users/sign_in")
        html, final_url = self._request_text(sign_in_url)
        token = self._extract_authenticity_token(html)

        payload = {
            "user[email]": self.username,
            "user[password]": self.password,
            "commit": "Sign in",
        }
        if token:
            payload["authenticity_token"] = token

        post_html, post_url = self._request_text(
            sign_in_url,
            method="POST",
            data=payload,
            referer=final_url,
        )
        if not self._looks_logged_in(post_html, post_url):
            raise RuntimeError("Surebet login failed. Check SUREBET_USERNAME and SUREBET_PASSWORD.")
        self.logged_in = True

    def fetch_valuebets_html(self) -> str:
        if not self.logged_in:
            self.login()

        url = urljoin(self.base_url + "/", "valuebets")
        html, final_url = self._request_text(url, referer=self.base_url)
        if not self._looks_logged_in(html, final_url):
            self.logged_in = False
            self.login()
            html, final_url = self._request_text(url, referer=self.base_url)
            if not self._looks_logged_in(html, final_url):
                raise RuntimeError("Surebet session could not access /valuebets after login.")
        return html

    def resolve_final_link(self, url: str) -> str:
        if not url:
            return url
        if not self.logged_in:
            self.login()

        request = Request(
            url,
            headers={
                "User-Agent": USER_AGENT,
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Referer": self.base_url,
            },
            method="GET",
        )

        try:
            with self.opener.open(request, timeout=max(30, self.login_timeout_seconds)) as response:
                final_url = response.geturl()
                if final_url:
                    return final_url
        except HTTPError as exc:
            location = exc.headers.get("Location") if exc.headers else None
            if location:
                return urljoin(url, location)
            raise

        return url


class SurebetValuebetsSource(AlertSource):
    key = SUREBET_SOURCE
    label = "Surebet Valuebets"

    def __init__(self, args: argparse.Namespace, *, ssl_context: Optional[ssl.SSLContext] = None) -> None:
        self.base_url = args.surebet_base_url
        self.resolved_link_cache: dict[str, str] = {}
        self.session = SurebetSession(
            base_url=args.surebet_base_url,
            username=args.surebet_username,
            password=args.surebet_password,
            ssl_context=ssl_context,
            login_timeout_seconds=args.surebet_browser_login_timeout_seconds,
        )

    def _resolve_candidate_link(self, candidate: NormalizedAlertCandidate) -> NormalizedAlertCandidate:
        if not candidate.deep_link:
            return candidate

        cached = self.resolved_link_cache.get(candidate.deep_link)
        if cached is not None:
            candidate.deep_link = cached
            return candidate

        try:
            final_link = self.session.resolve_final_link(candidate.deep_link)
        except (RuntimeError, URLError, HTTPError, ValueError) as exc:
            print(
                f"[warn] Failed to resolve final bookmaker link for {candidate.source_record_id or candidate.deep_link}: {exc}",
                file=sys.stderr,
            )
            final_link = candidate.deep_link

        self.resolved_link_cache[candidate.deep_link] = final_link
        candidate.deep_link = final_link
        return candidate

    def fetch_candidates(
        self,
        args: argparse.Namespace,
        *,
        ssl_context: Optional[ssl.SSLContext] = None,
    ) -> list[NormalizedAlertCandidate]:
        del args, ssl_context
        html = self.session.fetch_valuebets_html()
        records = parse_surebet_valuebets_html(html)
        return [
            self._resolve_candidate_link(normalize_surebet_candidate(record, self.base_url))
            for record in records
        ]


class AlertStore(ABC):
    @abstractmethod
    def reserve_alert(self, candidate: NormalizedAlertCandidate) -> bool:
        raise NotImplementedError

    @abstractmethod
    def mark_sent(
        self,
        candidate: NormalizedAlertCandidate,
        *,
        chat_target: str,
        message: str,
    ) -> None:
        raise NotImplementedError

    @abstractmethod
    def mark_failed(self, candidate: NormalizedAlertCandidate, error: str) -> None:
        raise NotImplementedError

    def close(self) -> None:
        return


def build_alert_fingerprint(candidate: NormalizedAlertCandidate) -> str:
    if candidate.source == SUREBET_SOURCE:
        return "|".join(
            [
                candidate.source,
                candidate.kind,
                candidate.sport.strip(),
                candidate.tournament.strip(),
                candidate.event_label.strip(),
            ]
        )
    if candidate.source == ODDS_API_SOURCE:
        return "|".join(
            [
                candidate.source,
                candidate.kind,
                candidate.bookmaker.strip(),
                candidate.sport.strip(),
                candidate.tournament.strip(),
                candidate.event_label.strip(),
                candidate.start_time.strip(),
                candidate.market_label.strip(),
                format_comparable_number(candidate.odds, 4),
            ]
        )

    components = [
        candidate.source,
        candidate.kind,
        candidate.source_record_id,
        candidate.bookmaker,
        candidate.sport,
        candidate.tournament,
        candidate.event_label,
        candidate.start_time,
        candidate.market_label,
        format_comparable_number(candidate.odds, 4),
        format_comparable_number(candidate.ev_percent, 2),
        format_comparable_number(candidate.probability, 4),
        format_comparable_number(candidate.line, 4),
    ]
    return "|".join(components)


def build_alert_state_entry(candidate: NormalizedAlertCandidate) -> dict[str, Any]:
    return {
        "source": candidate.source,
        "kind": candidate.kind,
        "source_record_id": candidate.source_record_id,
        "bookmaker": candidate.bookmaker,
        "sport": candidate.sport,
        "tournament": candidate.tournament,
        "event_label": candidate.event_label,
        "start_time": candidate.start_time,
        "market_label": candidate.market_label,
        "odds": format_comparable_number(candidate.odds, 4),
        "ev_percent": format_comparable_number(candidate.ev_percent, 2),
        "probability": format_comparable_number(candidate.probability, 4),
        "line": format_comparable_number(candidate.line, 4),
        "sent_at": datetime.now(timezone.utc).isoformat(),
    }


def build_alert_document(candidate: NormalizedAlertCandidate, fingerprint: str) -> dict[str, Any]:
    return {
        "fingerprint": fingerprint,
        "status": "pending",
        "source": candidate.source,
        "kind": candidate.kind,
        "source_record_id": candidate.source_record_id,
        "bookmaker": candidate.bookmaker,
        "sport": candidate.sport,
        "tournament": candidate.tournament,
        "event_label": candidate.event_label,
        "start_time": candidate.start_time,
        "market_label": candidate.market_label,
        "odds": format_comparable_number(candidate.odds, 4),
        "ev_percent": candidate.ev_percent,
        "probability": candidate.probability,
        "line": format_comparable_number(candidate.line, 4),
        "created_at": datetime.now(timezone.utc),
        "sent_at": None,
        "last_error": None,
    }


def build_sent_alert_record(
    *,
    candidate: NormalizedAlertCandidate,
    chat_target: str,
    message: str,
) -> dict[str, Any]:
    return {
        "sent_at": datetime.now(timezone.utc).isoformat(),
        "source": candidate.source,
        "kind": candidate.kind,
        "source_record_id": candidate.source_record_id,
        "fingerprint": build_alert_fingerprint(candidate),
        "telegram_chat_target": chat_target,
        "telegram_message_html": message,
        "alert": build_alert_state_entry(candidate),
        "raw_payload": candidate.raw_payload,
    }


class FileAlertStore(AlertStore):
    def __init__(self, path: Path, *, ignore_state: bool = False) -> None:
        self.path = path
        if ignore_state:
            self.seen_ids: set[str] = set()
            self.sent_alerts: dict[str, dict[str, Any]] = {}
        else:
            self.seen_ids, self.sent_alerts = load_alert_state(path)
        self.pending_fingerprints: set[str] = set()

    def reserve_alert(self, candidate: NormalizedAlertCandidate) -> bool:
        alert_fingerprint = build_alert_fingerprint(candidate)

        if alert_fingerprint in self.sent_alerts or alert_fingerprint in self.pending_fingerprints:
            return False

        self.pending_fingerprints.add(alert_fingerprint)
        return True

    def mark_sent(
        self,
        candidate: NormalizedAlertCandidate,
        *,
        chat_target: str,
        message: str,
    ) -> None:
        alert_fingerprint = build_alert_fingerprint(candidate)

        self.pending_fingerprints.discard(alert_fingerprint)
        self.sent_alerts[alert_fingerprint] = build_alert_state_entry(candidate)
        self.save()

    def mark_failed(self, candidate: NormalizedAlertCandidate, error: str) -> None:
        record_id = candidate.source_record_id.strip()
        alert_fingerprint = build_alert_fingerprint(candidate)

        self.pending_fingerprints.discard(alert_fingerprint)
        print(
            f"[warn] Alert reservation released for {record_id or alert_fingerprint}: {error}",
            file=sys.stderr,
        )

    def save(self) -> None:
        save_alert_state(self.path, self.seen_ids, self.sent_alerts)


class MongoAlertStore(AlertStore):
    def __init__(self, uri: str, database: str, collection_name: str) -> None:
        if MongoClient is None:
            raise SystemExit(
                "MongoDB support requires pymongo. Install dependencies with "
                "`py -3 -m pip install -r requirements.txt`."
            )

        self.client = MongoClient(uri, serverSelectionTimeoutMS=5000)
        self.collection: Collection = self.client[database][collection_name]
        self.client.admin.command("ping")
        self._ensure_indexes()

    def _ensure_indexes(self) -> None:
        self.collection.create_index("fingerprint", unique=True, name="uniq_fingerprint")
        self.collection.create_index(
            [("source", 1), ("source_record_id", 1)],
            name="source_record_id",
        )
        self.collection.create_index("created_at", name="created_at")
        self.collection.create_index("status", name="status")

    def reserve_alert(self, candidate: NormalizedAlertCandidate) -> bool:
        fingerprint = build_alert_fingerprint(candidate)
        document = build_alert_document(candidate, fingerprint)
        try:
            self.collection.insert_one(document)
            return True
        except DuplicateKeyError:
            return False
        except PyMongoError as exc:
            raise RuntimeError(f"Failed to reserve alert in MongoDB: {exc}") from exc

    def mark_sent(
        self,
        candidate: NormalizedAlertCandidate,
        *,
        chat_target: str,
        message: str,
    ) -> None:
        fingerprint = build_alert_fingerprint(candidate)
        sent_alert = build_sent_alert_record(
            candidate=candidate,
            chat_target=chat_target,
            message=message,
        )
        try:
            self.collection.update_one(
                {"fingerprint": fingerprint},
                {
                    "$set": {
                        "status": "sent",
                        "sent_at": datetime.now(timezone.utc),
                        "last_error": None,
                        "state": build_alert_state_entry(candidate),
                        "telegram_chat_target": sent_alert["telegram_chat_target"],
                        "telegram_message_html": sent_alert["telegram_message_html"],
                        "raw_payload": sent_alert["raw_payload"],
                    }
                },
            )
        except PyMongoError as exc:
            raise RuntimeError(f"Failed to mark alert as sent in MongoDB: {exc}") from exc

    def mark_failed(self, candidate: NormalizedAlertCandidate, error: str) -> None:
        fingerprint = build_alert_fingerprint(candidate)
        try:
            self.collection.delete_one({"fingerprint": fingerprint})
        except PyMongoError as exc:
            raise RuntimeError(f"Failed to mark alert as failed in MongoDB: {exc}") from exc

    def close(self) -> None:
        self.client.close()


def filter_candidates(
    candidates: list[NormalizedAlertCandidate],
    *,
    min_ev: float,
    min_bet_odds: float,
    min_probability: Optional[float] = None,
) -> list[NormalizedAlertCandidate]:
    threshold_percent = normalize_threshold_percent(min_ev)
    filtered: list[NormalizedAlertCandidate] = []

    for candidate in candidates:
        if candidate.ev_percent is None or candidate.ev_percent < threshold_percent:
            continue
        if candidate.odds is None or candidate.odds <= min_bet_odds:
            continue
        if min_probability is not None and candidate.probability is not None:
            if candidate.probability <= min_probability:
                continue
        filtered.append(candidate)

    return sorted(filtered, key=lambda item: item.ev_percent or 0, reverse=True)


def format_odds_api_alert(bet: dict[str, Any]) -> str:
    event = bet.get("event") if isinstance(bet.get("event"), dict) else {}
    market = bet.get("market") if isinstance(bet.get("market"), dict) else {}
    bookmaker_odds = bet.get("bookmakerOdds") if isinstance(bet.get("bookmakerOdds"), dict) else {}
    line = get_market_line(bet)
    fair_odds = bet.get("fairOdds") or bet.get("trueOdds") or bet.get("sharpOdds")
    market_reference_odds = market.get(str(bet.get("betSide", "")))
    sport = str(event.get("sport", "Unknown"))
    league = str(event.get("league", "Unknown"))
    start = str(event.get("date", "Unknown"))
    bookmaker = str(bet.get("bookmaker", "Unknown"))
    bet_label = describe_bet(bet)
    odds = get_bet_odds(bet)
    ev = format_ev_percent(bet.get("expectedValue"))
    matchup = f"{event.get('home', 'Unknown')} vs {event.get('away', 'Unknown')}"
    stake = calculate_stake_details(bet)

    lines = [
        "<b>EV ALERT</b>",
        f"<b>{escape(bet_label)}</b>",
        "",
        f"<b>Edge:</b> {escape(ev)}",
        f"<b>Odds:</b> {escape(odds)}",
    ]

    if line is not None:
        lines.append(f"<b>Line:</b> {escape(str(line))}")
    if stake is not None:
        lines.append(f"<b>Stake:</b> {stake['suggested_stake_percent']:.2f}% of bankroll")
    if market_reference_odds is not None:
        lines.append(f"<b>Sharp odd:</b> {escape(str(market_reference_odds))}")
    elif stake is not None:
        lines.append(f"<b>Sharp odd:</b> {stake['sharp_odd']:.3f}")
    if fair_odds is not None:
        lines.append(f"<b>Fair odds:</b> {escape(str(fair_odds))}")
    elif stake is not None:
        lines.append(f"<b>Fair odds:</b> {stake['fair_odd']:.2f}")
    if stake is not None:
        lines.append(f"<b>Market payout:</b> {stake['payout_percent']:.2f}%")

    lines.extend(
        [
            "",
            f"<b>Match:</b> {escape(matchup)}",
            f"<b>League:</b> {escape(league)}",
            f"<b>Sport:</b> {escape(sport)}",
            f"<b>Start:</b> {escape(start)}",
            f"<b>Bookmaker:</b> {escape(bookmaker)}",
        ]
    )

    if stake is None:
        lines.extend(
            [
                "",
                "<i>Stake not available for this market shape.</i>",
            ]
        )

    href = bookmaker_odds.get("href")
    if href:
        safe_href = escape(str(href), quote=True)
        lines.extend(
            [
                "",
                f'<a href="{safe_href}">Place bet</a>',
            ]
        )

    return "\n".join(lines)


def format_surebet_valuebet_alert(
    candidate: NormalizedAlertCandidate,
    bankroll: float = 0,
    bankroll_currency: str = "EUR",
) -> str:
    stake = calculate_surebet_stake_details(candidate)
    lines = [
        "<b>SUREBET VALUEBET</b>",
        f"<b>{escape(candidate.bookmaker or 'Unknown bookmaker')}</b>",
        "",
        f"<b>Sport:</b> {escape(candidate.sport or 'Unknown')}",
        f"<b>Tournament:</b> {escape(candidate.tournament or 'Unknown')}",
        f"<b>Event:</b> {escape(candidate.event_label or 'Unknown')}",
        f"<b>Time:</b> {escape(candidate.start_time or 'Unknown')}",
        "",
        f"<b>Market:</b> {escape(candidate.market_label or 'Unknown')}",
        f"<b>Odds:</b> {escape(candidate.odds_display or 'N/A')}",
        f"<b>Value:</b> {escape(candidate.ev_display or 'N/A')}",
    ]

    if candidate.probability is not None or candidate.probability_display:
        lines.append(f"<b>Probability:</b> {escape(candidate.probability_display or 'N/A')}")
    if stake is not None:
        lines.append(
            f"<b>Estimated stake:</b> {stake['suggested_stake_percent']:.2f}% of bankroll"
        )
        if bankroll > 0:
            lines.append(
                f"<b>Stake amount:</b> {escape(format_stake_amount(stake['suggested_stake_percent'], bankroll, bankroll_currency))}"
            )
        lines.append(f"<b>Estimated fair odds:</b> {stake['fair_odd']:.2f}")
        lines.append(
            f"<b>Estimated fair probability:</b> {stake['fair_probability_percent']:.2f}%"
        )
        lines.append("<i>Stake estimated from scraped odds and overvalue.</i>")

    if candidate.deep_link:
        safe_href = escape(candidate.deep_link, quote=True)
        lines.extend(["", f'<a href="{safe_href}">Open value bet</a>'])

    return "\n".join(lines)


def format_alert(
    candidate: NormalizedAlertCandidate,
    bankroll: float = 0,
    bankroll_currency: str = "EUR",
) -> str:
    if candidate.source == ODDS_API_SOURCE:
        return format_odds_api_alert(candidate.raw_payload)
    if candidate.source == SUREBET_SOURCE:
        return format_surebet_valuebet_alert(candidate, bankroll, bankroll_currency)
    raise RuntimeError(f"Unknown alert source {candidate.source!r}")


def format_alert_with_bankroll(
    candidate: NormalizedAlertCandidate,
    bankroll: float,
    bankroll_currency: str,
) -> str:
    if candidate.source != ODDS_API_SOURCE:
        return format_alert(candidate, bankroll, bankroll_currency)

    message = format_alert(candidate, bankroll, bankroll_currency)
    stake = calculate_stake_details(candidate.raw_payload)
    if stake is None or bankroll <= 0:
        return message

    bankroll_line = (
        f"<b>Stake amount:</b> "
        f"{escape(format_stake_amount(stake['suggested_stake_percent'], bankroll, bankroll_currency))}"
    )
    return message.replace(
        f"<b>Stake:</b> {stake['suggested_stake_percent']:.2f}% of bankroll",
        (
            f"<b>Stake:</b> {stake['suggested_stake_percent']:.2f}% of bankroll\n"
            f"{bankroll_line}"
        ),
        1,
    )


def send_telegram_alert(
    bot_token: str,
    chat_target: str,
    candidate: NormalizedAlertCandidate,
    bankroll: float,
    bankroll_currency: str,
    *,
    ssl_context: Optional[ssl.SSLContext] = None,
) -> str:
    message = format_alert_with_bankroll(candidate, bankroll, bankroll_currency)
    payload = {
        "chat_id": chat_target,
        "text": message,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }
    status, raw, _rate_limit = http_request(
        f"https://api.telegram.org/bot{bot_token}/sendMessage",
        method="POST",
        payload=payload,
        ssl_context=ssl_context,
    )

    if status not in (200, 204):
        raise RuntimeError(f"Unexpected Telegram status {status}: {raw}")
    return message


def send_telegram_message(
    bot_token: str,
    chat_target: str,
    message: str,
    *,
    ssl_context: Optional[ssl.SSLContext] = None,
) -> None:
    payload = {
        "chat_id": chat_target,
        "text": message,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }
    status, raw, _rate_limit = http_request(
        f"https://api.telegram.org/bot{bot_token}/sendMessage",
        method="POST",
        payload=payload,
        ssl_context=ssl_context,
    )
    if status not in (200, 204):
        raise RuntimeError(f"Unexpected Telegram status {status}: {raw}")


def create_alert_store(args: argparse.Namespace) -> AlertStore:
    if args.mongodb_uri:
        return MongoAlertStore(
            args.mongodb_uri,
            args.mongodb_database,
            args.mongodb_collection,
        )
    return FileAlertStore(Path(args.state_file), ignore_state=args.ignore_state)


def create_sources(
    args: argparse.Namespace,
    selected_sources: list[str],
    *,
    surebet_ssl_context: Optional[ssl.SSLContext] = None,
) -> list[AlertSource]:
    sources: list[AlertSource] = []
    if ODDS_API_SOURCE in selected_sources:
        sources.append(OddsApiValueBetSource())
    if SUREBET_SOURCE in selected_sources:
        sources.append(SurebetValuebetsSource(args, ssl_context=surebet_ssl_context))
    return sources


def apply_source_filters(
    source_key: str,
    candidates: list[NormalizedAlertCandidate],
    args: argparse.Namespace,
) -> list[NormalizedAlertCandidate]:
    if source_key == ODDS_API_SOURCE:
        return filter_candidates(
            candidates,
            min_ev=args.min_ev,
            min_bet_odds=args.min_bet_odds,
        )
    if source_key == SUREBET_SOURCE:
        return filter_candidates(
            candidates,
            min_ev=args.scraped_min_ev,
            min_bet_odds=args.scraped_min_bet_odds,
            min_probability=args.scraped_min_probability,
        )
    return candidates


def run_check(
    args: argparse.Namespace,
    alert_store: AlertStore,
    sources: list[AlertSource],
    *,
    odds_api_ssl_context: Optional[ssl.SSLContext] = None,
    surebet_ssl_context: Optional[ssl.SSLContext] = None,
    telegram_ssl_context: Optional[ssl.SSLContext] = None,
    alert_archive: Optional[AlertArchive] = None,
) -> int:
    alerts_sent = 0

    for source in sources:
        try:
            source_candidates = source.fetch_candidates(
                args,
                ssl_context=odds_api_ssl_context if source.key == ODDS_API_SOURCE else surebet_ssl_context,
            )
            filtered_candidates = apply_source_filters(source.key, source_candidates, args)
        except (RuntimeError, URLError, ValueError, json.JSONDecodeError) as exc:
            print(f"[error] Failed to fetch alerts for {source.label}: {exc}", file=sys.stderr)
            continue

        reserved_candidates: list[NormalizedAlertCandidate] = []
        for candidate in filtered_candidates:
            try:
                if alert_store.reserve_alert(candidate):
                    reserved_candidates.append(candidate)
            except RuntimeError as exc:
                print(f"[error] Failed to reserve alert for {source.label}: {exc}", file=sys.stderr)

        if not reserved_candidates:
            print(f"[info] No new alerts above threshold for {source.label}")

        for candidate in reserved_candidates:
            alert_id = candidate.source_record_id or build_alert_fingerprint(candidate)
            try:
                sent_message = send_telegram_alert(
                    args.telegram_bot_token,
                    args.telegram_chat_target,
                    candidate,
                    args.bankroll,
                    args.bankroll_currency,
                    ssl_context=telegram_ssl_context,
                )
                alert_store.mark_sent(
                    candidate,
                    chat_target=args.telegram_chat_target,
                    message=sent_message,
                )
                if alert_archive is not None:
                    alert_archive.record_sent_alert(
                        candidate=candidate,
                        chat_target=args.telegram_chat_target,
                        message=sent_message,
                    )
                alerts_sent += 1
                print(
                    f"[alert] Sent {alert_id} ({candidate.ev_display or 'N/A'} EV) for {source.label}"
                )
            except (RuntimeError, URLError, ValueError, json.JSONDecodeError) as exc:
                try:
                    alert_store.mark_failed(candidate, str(exc))
                except RuntimeError as store_exc:
                    print(
                        f"[error] Failed to update alert state for {alert_id}: {store_exc}",
                        file=sys.stderr,
                    )
                print(f"[error] Failed to send Telegram alert for {alert_id}: {exc}", file=sys.stderr)

    return alerts_sent


def close_sources(sources: list[AlertSource]) -> None:
    for source in sources:
        try:
            source.close()
        except Exception as exc:
            print(f"[warn] Failed to close source {source.key}: {exc}", file=sys.stderr)


def main() -> int:
    load_env_defaults()
    configure_timestamped_output()
    args = parse_args()
    selected_sources = get_selected_sources(args)
    require_config(args, selected_sources)
    validate_polling_budget(args, selected_sources)

    shared_ca_bundle = resolve_ca_bundle(
        args.ca_bundle,
        os.getenv("SSL_CERT_FILE"),
        os.getenv("REQUESTS_CA_BUNDLE"),
        os.getenv("CURL_CA_BUNDLE"),
    )
    odds_api_ca_bundle = resolve_ca_bundle(args.odds_api_ca_bundle, shared_ca_bundle)
    telegram_ca_bundle = resolve_ca_bundle(args.telegram_ca_bundle, shared_ca_bundle)

    odds_api_ssl_context = build_ssl_context(cafile=odds_api_ca_bundle)
    surebet_ssl_context = build_ssl_context(cafile=shared_ca_bundle)
    telegram_ssl_context = build_ssl_context(
        cafile=telegram_ca_bundle,
        allow_insecure=args.allow_insecure_telegram,
    )

    if args.test_telegram:
        send_telegram_message(
            args.telegram_bot_token,
            args.telegram_chat_target,
            "<b>EVFinder test</b>\nTelegram delivery is working.",
            ssl_context=telegram_ssl_context,
        )
        print("[info] Sent Telegram test message")
        return 0

    alert_store = create_alert_store(args)
    alert_archive = AlertArchive(Path(args.alert_archive_file))
    sources = create_sources(
        args,
        selected_sources,
        surebet_ssl_context=surebet_ssl_context,
    )

    try:
        while True:
            run_check(
                args,
                alert_store,
                sources,
                odds_api_ssl_context=odds_api_ssl_context,
                surebet_ssl_context=surebet_ssl_context,
                telegram_ssl_context=telegram_ssl_context,
                alert_archive=alert_archive,
            )

            if args.once:
                return 0

            time.sleep(args.poll_interval)
    finally:
        close_sources(sources)
        alert_store.close()


if __name__ == "__main__":
    raise SystemExit(main())
