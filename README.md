# EVFinder

Simple Python monitor for Odds-API.io value bets with Telegram alerts.

## What it does

- Fetches value bets from `https://api.odds-api.io/v3/value-bets`
- Filters for bets with expected value greater than or equal to `5%`
- Sends Telegram alerts only for bets that have not already been sent with the same odds, EV, and line
- Includes a suggested stake using the `ev-calculator` quarter-Kelly logic

## Configuration

The script will read variables from `.env` first, and then fall back to `.env.example`.

Recommended local setup:

```bash
cp .env.example .env
```

Then set:

- `ODDS_API_IO_KEY`
- `ODDS_API_BOOKMAKER` or pass `--bookmakers`
- `TELEGRAM_BOT_TOKEN`
- `TELEGRAM_CHAT_ID`

Optional:

- `MIN_EXPECTED_VALUE=0.05`
- `POLL_INTERVAL_SECONDS=60`
- `MAX_REQUESTS_PER_HOUR=100`
- `BANKROLL=1000`
- `BANKROLL_CURRENCY=EUR`
- `STATE_FILE=.seen_value_bets.json`

For a 100 requests/hour plan, `60` seconds is a safe default for one bookmaker. If you monitor multiple bookmakers, the script will stop and tell you the minimum safe interval instead of silently exceeding the budget.

## Usage

Run once:

```bash
export ODDS_API_IO_KEY=...
export ODDS_API_BOOKMAKER="Betano PT"
export TELEGRAM_BOT_TOKEN=...
export TELEGRAM_CHAT_ID=...
python3 value_bet_alerts.py --once
```

Run continuously:

```bash
python3 value_bet_alerts.py
```

Multiple bookmakers:

```bash
python3 value_bet_alerts.py --bookmakers "Bet365,Pinnacle,Unibet"
```

The script stores alert state in `.seen_value_bets.json` so it does not re-send duplicates on restart. A bet is treated as a duplicate when the same market for the same event/bookmaker has the same odds, EV, and line as a previously sent alert.

## Running continuously

The repo includes a GitHub Actions scheduler for persistent alert state across runs.

### GitHub Actions scheduler

Workflow file:

- `.github/workflows/value-bet-alerts.yml`

Set these GitHub repository secrets:

- `ODDS_API_IO_KEY`
- `ODDS_API_BOOKMAKER`
- `TELEGRAM_BOT_TOKEN`
- `TELEGRAM_CHAT_ID`

Optional GitHub repository variables:

- `MIN_EXPECTED_VALUE`
- `BANKROLL`
- `BANKROLL_CURRENCY`

The workflow runs every 10 minutes and commits `.seen_value_bets.json` back to the repo so alert state survives between runs.

Tradeoffs:

- very cheap and easy
- not truly always-on, only scheduled
- scheduled workflows can be delayed at busy times
- in public repositories, GitHub can disable scheduled workflows after 60 days of no repository activity

## Free deployment notes

Current free options can change over time. As of April 2, 2026:

- GitHub Actions still supports scheduled workflows, with a minimum interval of 5 minutes. Scheduled runs are on the default branch and may be delayed under load. In public repos, they can be disabled after 60 days of inactivity. Sources: [GitHub workflow syntax](https://docs.github.com/en/actions/reference/workflow-syntax), [GitHub events docs](https://docs.github.com/en/actions/writing-workflows/choosing-when-your-workflow-runs/events-that-trigger-workflows?ref=faun)
- Oracle Cloud still advertises Always Free compute resources, which can host a tiny always-on process if you want a cloud VM later. Sources: [Oracle Always Free resources](https://docs.oracle.com/en-us/iaas/Content/FreeTier/freetier_topic-Always_Free_Resources.htm), [Oracle Cloud Free](https://www.oracle.com/cloud/free)
- Railway currently has a limited free tier based on a small monthly credit, not a strong “always free” always-on option. Source: [Railway pricing](https://docs.railway.com/pricing)
- Fly.io no longer offers general free allowances to new customers, outside legacy plans or limited trials. Source: [Fly pricing](https://fly.io/docs/about/pricing/)
- Render still offers some free services, but this script is a better fit for a scheduler or VM than a long-running free web service. Source: [Render free docs](https://render.com/docs/free), [Render cron jobs](https://render.com/docs/cronjobs)

## Stake logic

The suggested stake mirrors the logic from `nemenxius/ev-calculator`:

- additive margin removal on a 2-way sharp market
- additive margin removal extended to 3-way markets when `home/draw/away` prices are available
- quarter Kelly sizing
- rounded to `0.25%` steps
- capped at `1.5%` of bankroll

If `BANKROLL` is set, the alert also includes the cash stake amount. Stake may still be omitted for unusual market shapes if the API does not expose enough sharp odds to build a fair probability.
