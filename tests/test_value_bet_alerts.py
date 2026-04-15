import importlib.util
import tempfile
import unittest
from types import SimpleNamespace
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
MODULE_PATH = ROOT / "value_bet_alerts.py"
SPEC = importlib.util.spec_from_file_location("value_bet_alerts", MODULE_PATH)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(MODULE)


SUREBET_HTML = """
<table>
  <tbody class="valuebet_record" id="vb_123">
    <tr>
      <td class="booker booker-first">
        <a href="/bookmaker/betano">Betano</a>
        <span class="minor">Football</span>
      </td>
      <td class="event">
        <a href="/events/abc">Benfica vs Porto</a>
        <span class="minor">Primeira Liga</span>
      </td>
      <td class="coeff">
        <abbr>Over 2.5</abbr>
      </td>
      <td class="value">
        <a class="value_link" href="/valuebets/123">2.05</a>
      </td>
      <td class="text-center">68.00%</td>
      <td class="text-center">102.40%</td>
      <td class="time">15 Apr 20:00</td>
    </tr>
  </tbody>
  <tbody class="valuebet_record" id="vb_124">
    <tr>
      <td class="booker booker-first">
        <a href="/bookmaker/bwin">Bwin</a>
        <span class="minor">Basketball</span>
      </td>
      <td class="event">
        <a href="/events/def">Lakers vs Celtics</a>
        <span class="minor">NBA</span>
      </td>
      <td class="coeff">
        <abbr>Away ML</abbr>
      </td>
      <td class="value">
        <a class="value_link" href="/valuebets/124">1.90</a>
      </td>
      <td class="text-center">59.00%</td>
      <td class="text-center">101.10%</td>
      <td class="time">16 Apr 01:30</td>
    </tr>
  </tbody>
</table>
"""


class ValueBetAlertsTests(unittest.TestCase):
    def test_parse_surebet_valuebets_html_extracts_reference_fields(self) -> None:
        rows = MODULE.parse_surebet_valuebets_html(SUREBET_HTML)

        self.assertEqual(2, len(rows))
        self.assertEqual("vb_123", rows[0]["row_id"])
        self.assertEqual("Betano", rows[0]["bookmaker"])
        self.assertEqual("Football", rows[0]["sport"])
        self.assertEqual("Benfica vs Porto", rows[0]["event"])
        self.assertEqual("Primeira Liga", rows[0]["tournament"])
        self.assertEqual("Over 2.5", rows[0]["market"])
        self.assertEqual("2.05", rows[0]["odds"])
        self.assertEqual("68.00%", rows[0]["probability"])
        self.assertEqual("102.40%", rows[0]["overvalue"])

    def test_normalize_odds_api_candidate_preserves_existing_ev_shape(self) -> None:
        raw = {
            "id": "bet_1",
            "bookmaker": "Betano PT",
            "betSide": "home",
            "expectedValue": 105.2,
            "event": {
                "sport": "Soccer",
                "league": "Primeira Liga",
                "home": "Benfica",
                "away": "Porto",
                "date": "2026-04-15T20:00:00Z",
            },
            "market": {
                "name": "ml",
                "home": 1.9,
                "away": 2.0,
            },
            "bookmakerOdds": {
                "home": 2.1,
                "href": "https://example.com/bet",
            },
        }

        candidate = MODULE.normalize_odds_api_candidate(raw)

        self.assertEqual(MODULE.ODDS_API_SOURCE, candidate.source)
        self.assertEqual("valuebet", candidate.kind)
        self.assertEqual(2.1, candidate.odds)
        self.assertAlmostEqual(5.2, candidate.ev_percent, places=2)
        self.assertEqual("Benfica vs Porto", candidate.event_label)

    def test_filter_candidates_applies_scraped_thresholds(self) -> None:
        rows = MODULE.parse_surebet_valuebets_html(SUREBET_HTML)
        candidates = [
            MODULE.normalize_surebet_candidate(rows[0], "https://en.surebet.com"),
            MODULE.normalize_surebet_candidate(rows[1], "https://en.surebet.com"),
        ]

        filtered = MODULE.filter_candidates(
            candidates,
            min_ev=2.0,
            min_bet_odds=2.0,
            min_probability=0.60,
        )

        self.assertEqual(1, len(filtered))
        self.assertEqual("vb_123", filtered[0].source_record_id)

    def test_fingerprint_is_source_aware(self) -> None:
        surebet_record = MODULE.parse_surebet_valuebets_html(SUREBET_HTML)[0]
        surebet_candidate = MODULE.normalize_surebet_candidate(
            surebet_record,
            "https://en.surebet.com",
        )
        odds_candidate = MODULE.normalize_odds_api_candidate(
            {
                "id": "vb_123",
                "bookmaker": surebet_candidate.bookmaker,
                "betSide": "home",
                "expectedValue": 102.4,
                "event": {
                    "sport": surebet_candidate.sport,
                    "league": surebet_candidate.tournament,
                    "home": "Benfica",
                    "away": "Porto",
                    "date": surebet_candidate.start_time,
                },
                "market": {"name": "ml", "home": 1.9, "away": 2.0},
                "bookmakerOdds": {"home": 2.05},
            }
        )

        self.assertNotEqual(
            MODULE.build_alert_fingerprint(surebet_candidate),
            MODULE.build_alert_fingerprint(odds_candidate),
        )

    def test_file_alert_store_allows_same_row_id_when_fingerprint_changes(self) -> None:
        row = MODULE.parse_surebet_valuebets_html(SUREBET_HTML)[0]
        candidate_one = MODULE.normalize_surebet_candidate(row, "https://en.surebet.com")
        row_changed = dict(row)
        row_changed["overvalue"] = "103.10%"
        candidate_two = MODULE.normalize_surebet_candidate(row_changed, "https://en.surebet.com")

        with tempfile.TemporaryDirectory() as temp_dir:
            store = MODULE.FileAlertStore(Path(temp_dir) / "state.json")

            self.assertTrue(store.reserve_alert(candidate_one))
            store.mark_sent(candidate_one, chat_target="chat", message="msg")
            self.assertTrue(store.reserve_alert(candidate_two))

    def test_calculate_surebet_stake_details_uses_overvalue_and_odds(self) -> None:
        row = MODULE.parse_surebet_valuebets_html(SUREBET_HTML)[0]
        candidate = MODULE.normalize_surebet_candidate(row, "https://en.surebet.com")

        stake = MODULE.calculate_surebet_stake_details(candidate)

        self.assertIsNotNone(stake)
        assert stake is not None
        self.assertAlmostEqual(0.50, stake["suggested_stake_percent"], places=2)
        self.assertAlmostEqual(49.95, stake["fair_probability_percent"], places=2)
        self.assertAlmostEqual(2.00, stake["fair_odd"], places=2)

    def test_format_surebet_alert_contains_source_specific_fields(self) -> None:
        row = MODULE.parse_surebet_valuebets_html(SUREBET_HTML)[0]
        candidate = MODULE.normalize_surebet_candidate(row, "https://en.surebet.com")

        message = MODULE.format_alert(candidate, bankroll=1000, bankroll_currency="EUR")

        self.assertIn("SUREBET VALUEBET", message)
        self.assertIn("Betano", message)
        self.assertIn("Benfica vs Porto", message)
        self.assertIn("Over 2.5", message)
        self.assertIn("2.40%", message)
        self.assertIn("Estimated stake", message)
        self.assertIn("0.50% of bankroll", message)
        self.assertIn("5.00 EUR", message)

    def test_surebet_source_replaces_intermediary_link_with_resolved_bookmaker_link(self) -> None:
        args = SimpleNamespace(
            surebet_base_url="https://en.surebet.com",
            surebet_username="user",
            surebet_password="pass",
            surebet_browser_login_timeout_seconds=20,
        )
        source = MODULE.SurebetValuebetsSource(args)
        source.session.fetch_valuebets_html = lambda: SUREBET_HTML
        source.session.resolve_final_link = lambda url: "https://www.betano.pt/event/123" if url.endswith("/123") else url

        candidates = source.fetch_candidates(args)

        self.assertEqual("https://www.betano.pt/event/123", candidates[0].deep_link)
        self.assertEqual("https://en.surebet.com/valuebets/124", candidates[1].deep_link)


if __name__ == "__main__":
    unittest.main()
