#!/usr/bin/env python3

import argparse
import json
import os
import sys
import time
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from math import ceil
from html import escape
from pathlib import Path
from typing import Any, Optional
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

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
DEFAULT_MIN_EV = 0.05
DEFAULT_POLL_INTERVAL = 60
DEFAULT_STATE_FILE = ".seen_value_bets.json"
DEFAULT_MAX_REQUESTS_PER_HOUR = 100
DEFAULT_MONGODB_DATABASE = "evfinder"
DEFAULT_MONGODB_COLLECTION = "sent_alerts"
STATE_VERSION = 2


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
    # Prefer .env, but allow .env.example for this simple local workflow.
    load_env_file(Path(".env"))
    load_env_file(Path(".env.example"))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Poll Odds-API.io value bets and send new +EV alerts to Telegram."
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
        help="Minimum expected value as a decimal. 0.05 means 5%%.",
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
        help="Maximum allowed API requests per hour across all bookmakers.",
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
    return parser.parse_args()


def require_config(args: argparse.Namespace) -> None:
    missing = []
    if not args.api_key:
        missing.append("ODDS_API_IO_KEY / --api-key")
    if not args.bookmakers:
        missing.append("ODDS_API_BOOKMAKER(S) / --bookmakers")
    if not args.telegram_bot_token:
        missing.append("TELEGRAM_BOT_TOKEN / --telegram-bot-token")
    if not args.telegram_chat_target:
        missing.append(
            "TELEGRAM_CHAT_TARGET / TELEGRAM_CHAT_USERNAME / TELEGRAM_CHAT_ID / "
            "--telegram-chat-target"
        )

    if missing:
        raise SystemExit("Missing required configuration: " + ", ".join(missing))


def get_bookmakers(raw_bookmakers: str) -> list[str]:
    return [item.strip() for item in raw_bookmakers.split(",") if item.strip()]


def validate_polling_budget(args: argparse.Namespace, bookmakers: list[str]) -> None:
    if args.once:
        return

    requests_per_cycle = len(bookmakers)
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
        ),
        encoding="utf-8",
    )


def http_json(
    url: str,
    *,
    method: str = "GET",
    payload: Optional[dict[str, Any]] = None,
) -> Any:
    body = None
    headers = {"User-Agent": "evfinder-value-bet-alerts/1.0"}

    if payload is not None:
        body = json.dumps(payload).encode("utf-8")
        headers["Content-Type"] = "application/json"

    request = Request(url, data=body, headers=headers, method=method)

    try:
        with urlopen(request, timeout=30) as response:
            charset = response.headers.get_content_charset() or "utf-8"
            raw = response.read().decode(charset)
            return json.loads(raw)
    except HTTPError as exc:
        charset = exc.headers.get_content_charset() or "utf-8"
        raw = exc.read().decode(charset, errors="replace").strip()
        detail = raw

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
        raise RuntimeError(message) from exc


def http_request(
    url: str,
    *,
    method: str = "GET",
    payload: Optional[dict[str, Any]] = None,
) -> tuple[int, str]:
    body = None
    headers = {"User-Agent": "evfinder-value-bet-alerts/1.0"}

    if payload is not None:
        body = json.dumps(payload).encode("utf-8")
        headers["Content-Type"] = "application/json"

    request = Request(url, data=body, headers=headers, method=method)

    try:
        with urlopen(request, timeout=30) as response:
            charset = response.headers.get_content_charset() or "utf-8"
            raw = response.read().decode(charset, errors="replace")
            return response.status, raw
    except HTTPError as exc:
        charset = exc.headers.get_content_charset() or "utf-8"
        raw = exc.read().decode(charset, errors="replace").strip()
        detail = raw or exc.reason
        raise RuntimeError(f"HTTP {exc.code}: {detail}") from exc


def fetch_value_bets(api_key: str, bookmaker: str) -> list[dict[str, Any]]:
    query = urlencode(
        {
            "apiKey": api_key,
            "bookmaker": bookmaker,
            "includeEventDetails": "true",
        }
    )
    url = f"{API_URL}?{query}"
    data = http_json(url)
    if not isinstance(data, list):
        raise ValueError(f"Unexpected API response for bookmaker {bookmaker!r}: {data!r}")
    return data


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
        ev = float(expected_value)
    except (TypeError, ValueError):
        return None

    # Odds-API.io value bets currently return 100 as break-even, e.g. 101.5 means 1.5% edge.
    if ev >= 100:
        return ev - 100

    # Allow decimal-style inputs too, where 0.05 means 5%.
    if ev <= 1:
        return ev * 100

    # And percent-style inputs, where 5.0 means 5%.
    return ev


def parse_float(value: Any) -> Optional[float]:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def format_ev_percent(expected_value: Any) -> str:
    percent = normalize_ev_percent(expected_value)
    if percent is None:
        return "N/A"
    return f"{percent:.2f}%"


def format_comparable_number(value: Any, decimals: int) -> str:
    numeric = parse_float(value)
    if numeric is None:
        return str(value or "")
    return f"{numeric:.{decimals}f}"


def build_alert_fingerprint(bet: dict[str, Any]) -> str:
    event = bet.get("event") if isinstance(bet.get("event"), dict) else {}
    market = bet.get("market") if isinstance(bet.get("market"), dict) else {}

    components = [
        str(bet.get("bookmaker", "")),
        str(event.get("sport", "")),
        str(event.get("league", "")),
        str(event.get("home", "")),
        str(event.get("away", "")),
        str(event.get("date", "")),
        str(market.get("name", bet.get("market", ""))),
        str(bet.get("betSide", "")),
        format_comparable_number(get_bet_odds(bet), 4),
        format_comparable_number(normalize_ev_percent(bet.get("expectedValue")), 2),
        format_comparable_number(get_market_line(bet), 4),
    ]
    return "|".join(components)


def build_alert_state_entry(bet: dict[str, Any]) -> dict[str, Any]:
    return {
        "bet_id": str(bet.get("id", "")),
        "bookmaker": str(bet.get("bookmaker", "")),
        "odds": format_comparable_number(get_bet_odds(bet), 4),
        "ev_percent": format_comparable_number(normalize_ev_percent(bet.get("expectedValue")), 2),
        "line": format_comparable_number(get_market_line(bet), 4),
        "sent_at": datetime.now(timezone.utc).isoformat(),
    }


def build_alert_document(bet: dict[str, Any], fingerprint: str) -> dict[str, Any]:
    event = bet.get("event") if isinstance(bet.get("event"), dict) else {}
    market = bet.get("market") if isinstance(bet.get("market"), dict) else {}
    bet_id = str(bet.get("id", "")).strip()
    ev_percent = normalize_ev_percent(bet.get("expectedValue"))

    document = {
        "fingerprint": fingerprint,
        "status": "pending",
        "bookmaker": str(bet.get("bookmaker", "")),
        "sport": str(event.get("sport", "")),
        "league": str(event.get("league", "")),
        "home": str(event.get("home", "")),
        "away": str(event.get("away", "")),
        "event_date": str(event.get("date", "")),
        "market_name": str(market.get("name", bet.get("market", ""))),
        "bet_side": str(bet.get("betSide", "")),
        "odds": format_comparable_number(get_bet_odds(bet), 4),
        "ev_percent": ev_percent,
        "line": format_comparable_number(get_market_line(bet), 4),
        "created_at": datetime.now(timezone.utc),
        "sent_at": None,
        "last_error": None,
    }
    if bet_id:
        document["bet_id"] = bet_id
    return document


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


def format_stake_amount(stake_percent: float, bankroll: float, currency: str) -> str:
    amount = bankroll * (stake_percent / 100)
    return f"{amount:.2f} {currency}"


def format_alert(bet: dict[str, Any]) -> str:
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


def send_telegram_alert(
    bot_token: str,
    chat_target: str,
    bet: dict[str, Any],
    bankroll: float,
    bankroll_currency: str,
) -> None:
    message = format_alert_with_bankroll(bet, bankroll, bankroll_currency)
    payload = {
        "chat_id": chat_target,
        "text": message,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }
    status, raw = http_request(
        f"https://api.telegram.org/bot{bot_token}/sendMessage",
        method="POST",
        payload=payload,
    )

    if status not in (200, 204):
        raise RuntimeError(f"Unexpected Telegram status {status}: {raw}")


def format_alert_with_bankroll(bet: dict[str, Any], bankroll: float, bankroll_currency: str) -> str:
    message = format_alert(bet)
    stake = calculate_stake_details(bet)
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


def send_telegram_message(bot_token: str, chat_target: str, message: str) -> None:
    payload = {
        "chat_id": chat_target,
        "text": message,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }
    status, raw = http_request(
        f"https://api.telegram.org/bot{bot_token}/sendMessage",
        method="POST",
        payload=payload,
    )
    if status not in (200, 204):
        raise RuntimeError(f"Unexpected Telegram status {status}: {raw}")


class AlertStore(ABC):
    @abstractmethod
    def reserve_alert(self, bet: dict[str, Any]) -> bool:
        raise NotImplementedError

    @abstractmethod
    def mark_sent(self, bet: dict[str, Any]) -> None:
        raise NotImplementedError

    @abstractmethod
    def mark_failed(self, bet: dict[str, Any], error: str) -> None:
        raise NotImplementedError

    def close(self) -> None:
        return


class FileAlertStore(AlertStore):
    def __init__(self, path: Path, *, ignore_state: bool = False) -> None:
        self.path = path
        if ignore_state:
            self.seen_ids: set[str] = set()
            self.sent_alerts: dict[str, dict[str, Any]] = {}
        else:
            self.seen_ids, self.sent_alerts = load_alert_state(path)
        self.pending_fingerprints: set[str] = set()
        self.pending_bet_ids: set[str] = set()

    def reserve_alert(self, bet: dict[str, Any]) -> bool:
        bet_id = str(bet.get("id", "")).strip()
        alert_fingerprint = build_alert_fingerprint(bet)

        if alert_fingerprint in self.sent_alerts or alert_fingerprint in self.pending_fingerprints:
            return False
        if bet_id and (bet_id in self.seen_ids or bet_id in self.pending_bet_ids):
            return False

        self.pending_fingerprints.add(alert_fingerprint)
        if bet_id:
            self.pending_bet_ids.add(bet_id)
        return True

    def mark_sent(self, bet: dict[str, Any]) -> None:
        bet_id = str(bet.get("id", "")).strip()
        alert_fingerprint = build_alert_fingerprint(bet)

        self.pending_fingerprints.discard(alert_fingerprint)
        if bet_id:
            self.pending_bet_ids.discard(bet_id)
            self.seen_ids.add(bet_id)
        self.sent_alerts[alert_fingerprint] = build_alert_state_entry(bet)
        self.save()

    def mark_failed(self, bet: dict[str, Any], error: str) -> None:
        bet_id = str(bet.get("id", "")).strip()
        alert_fingerprint = build_alert_fingerprint(bet)

        self.pending_fingerprints.discard(alert_fingerprint)
        if bet_id:
            self.pending_bet_ids.discard(bet_id)
        print(f"[warn] Alert reservation released for {bet_id or alert_fingerprint}: {error}", file=sys.stderr)

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
        self.collection.create_index("bet_id", unique=True, sparse=True, name="uniq_bet_id")
        self.collection.create_index("created_at", name="created_at")
        self.collection.create_index("status", name="status")

    def reserve_alert(self, bet: dict[str, Any]) -> bool:
        fingerprint = build_alert_fingerprint(bet)
        document = build_alert_document(bet, fingerprint)
        try:
            self.collection.insert_one(document)
            return True
        except DuplicateKeyError:
            return False
        except PyMongoError as exc:
            raise RuntimeError(f"Failed to reserve alert in MongoDB: {exc}") from exc

    def mark_sent(self, bet: dict[str, Any]) -> None:
        fingerprint = build_alert_fingerprint(bet)
        try:
            self.collection.update_one(
                {"fingerprint": fingerprint},
                {
                    "$set": {
                        "status": "sent",
                        "sent_at": datetime.now(timezone.utc),
                        "last_error": None,
                        "state": build_alert_state_entry(bet),
                    }
                },
            )
        except PyMongoError as exc:
            raise RuntimeError(f"Failed to mark alert as sent in MongoDB: {exc}") from exc

    def mark_failed(self, bet: dict[str, Any], error: str) -> None:
        fingerprint = build_alert_fingerprint(bet)
        try:
            self.collection.delete_one({"fingerprint": fingerprint})
        except PyMongoError as exc:
            raise RuntimeError(f"Failed to mark alert as failed in MongoDB: {exc}") from exc

    def close(self) -> None:
        self.client.close()


def filter_candidate_value_bets(
    bets: list[dict[str, Any]],
    min_ev: float,
) -> list[dict[str, Any]]:
    candidate_bets: list[dict[str, Any]] = []

    for bet in bets:
        ev_percent = normalize_ev_percent(bet.get("expectedValue"))
        threshold_percent = min_ev * 100 if min_ev <= 1 else min_ev

        if ev_percent is None or ev_percent < threshold_percent:
            continue

        candidate_bets.append(bet)

    return sorted(
        candidate_bets,
        key=lambda bet: normalize_ev_percent(bet.get("expectedValue")) or 0,
        reverse=True,
    )


def run_check(
    args: argparse.Namespace,
    alert_store: AlertStore,
) -> int:
    bookmakers = get_bookmakers(args.bookmakers)
    alerts_sent = 0

    for bookmaker in bookmakers:
        try:
            bets = fetch_value_bets(args.api_key, bookmaker)
            candidate_bets = filter_candidate_value_bets(bets, args.min_ev)
        except (RuntimeError, URLError, ValueError, json.JSONDecodeError) as exc:
            print(f"[error] Failed to fetch value bets for {bookmaker}: {exc}", file=sys.stderr)
            continue

        reserved_bets: list[dict[str, Any]] = []
        for bet in candidate_bets:
            try:
                if alert_store.reserve_alert(bet):
                    reserved_bets.append(bet)
            except RuntimeError as exc:
                print(f"[error] Failed to reserve alert for {bookmaker}: {exc}", file=sys.stderr)

        if not reserved_bets:
            print(f"[info] No new value bets above threshold for {bookmaker}")

        for bet in reserved_bets:
            bet_id = str(bet.get("id", "unknown"))
            try:
                send_telegram_alert(
                    args.telegram_bot_token,
                    args.telegram_chat_target,
                    bet,
                    args.bankroll,
                    args.bankroll_currency,
                )
                alert_store.mark_sent(bet)
                alerts_sent += 1
                print(
                    f"[alert] Sent {bet_id} ({format_ev_percent(bet.get('expectedValue'))} EV) for {bookmaker}"
                )
            except (RuntimeError, URLError, ValueError, json.JSONDecodeError) as exc:
                try:
                    alert_store.mark_failed(bet, str(exc))
                except RuntimeError as store_exc:
                    print(f"[error] Failed to update alert state for {bet_id}: {store_exc}", file=sys.stderr)
                print(f"[error] Failed to send Telegram alert for {bet_id}: {exc}", file=sys.stderr)

    return alerts_sent


def create_alert_store(args: argparse.Namespace) -> AlertStore:
    if args.mongodb_uri:
        return MongoAlertStore(
            args.mongodb_uri,
            args.mongodb_database,
            args.mongodb_collection,
        )
    return FileAlertStore(Path(args.state_file), ignore_state=args.ignore_state)


def main() -> int:
    load_env_defaults()
    args = parse_args()
    require_config(args)
    bookmakers = get_bookmakers(args.bookmakers)
    validate_polling_budget(args, bookmakers)

    if args.test_telegram:
        send_telegram_message(
            args.telegram_bot_token,
            args.telegram_chat_target,
            "<b>EVFinder test</b>\nTelegram delivery is working.",
        )
        print("[info] Sent Telegram test message")
        return 0

    alert_store = create_alert_store(args)

    try:
        while True:
            run_check(args, alert_store)

            if args.once:
                return 0

            time.sleep(args.poll_interval)
    finally:
        alert_store.close()


if __name__ == "__main__":
    raise SystemExit(main())
