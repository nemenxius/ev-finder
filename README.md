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
- `TELEGRAM_CHAT_TARGET`

Optional:

- `MIN_EXPECTED_VALUE=0.05`
- `POLL_INTERVAL_SECONDS=60`
- `MAX_REQUESTS_PER_HOUR=100`
- `BANKROLL=1000`
- `BANKROLL_CURRENCY=EUR`
- `STATE_FILE=.seen_value_bets.json`
- `MONGODB_URI`
- `MONGODB_DATABASE=evfinder`
- `MONGODB_COLLECTION=sent_alerts`

For a 100 requests/hour plan, `60` seconds is a safe default for one bookmaker. If you monitor multiple bookmakers, the script will stop and tell you the minimum safe interval instead of silently exceeding the budget. For example, `2` bookmakers requires at least `72` seconds.

## Usage

Run once:

```bash
export ODDS_API_IO_KEY=...
export ODDS_API_BOOKMAKER="Betano PT"
export TELEGRAM_BOT_TOKEN=...
export TELEGRAM_CHAT_TARGET=...
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

`TELEGRAM_CHAT_TARGET` can be either a numeric chat ID or an `@channel_username` style target where Telegram accepts usernames. The script still supports the older `TELEGRAM_CHAT_ID` name for backward compatibility.

If `MONGODB_URI` is set, the script stores duplicate-alert state in MongoDB instead of the local JSON file. This is the recommended setup if you want durable shared state across restarts or future multi-machine runs.

## Running continuously

This script is a good fit for an always-on local machine or a small VM. The state file lives next to the script, so alerts survive restarts as long as `.seen_value_bets.json` stays in place.

### Windows PC setup

1. Install Python 3 and make sure `py` works in Command Prompt.
2. Put the project on the Windows machine.
3. Install dependencies:

```bat
py -3 -m pip install -r requirements.txt
```

4. Copy `.env.example` to `.env` and fill in your values.
5. Add MongoDB settings if you want database-backed duplicate suppression:

```env
MONGODB_URI=mongodb://localhost:27017
MONGODB_DATABASE=evfinder
MONGODB_COLLECTION=sent_alerts
```

6. Set `ODDS_API_BOOKMAKER` as a comma-separated list, for example `Betano PT,Betclic PT`.
7. Keep `POLL_INTERVAL_SECONDS` high enough for your bookmaker count.
8. Start the script with:

```bat
py -3 value_bet_alerts.py
```

Or use the included launcher:

```bat
run_value_bet_alerts.bat
```

The launcher writes output to `logs\value_bet_alerts.log`.

### MongoDB behavior

When MongoDB is enabled, the script:

- creates a unique index on the alert fingerprint
- creates a sparse unique index on `bet_id` when the API provides one
- reserves an alert in MongoDB before sending to Telegram
- marks the alert as `sent` after successful delivery
- releases the reservation if sending fails, so the alert can be retried later

This is much safer than the local JSON file if the process restarts unexpectedly or if you later run more than one worker.

### Windows Task Scheduler

If you want the script to restart automatically after reboots or user logins:

1. Open Task Scheduler.
2. Create a new task.
3. On `General`, choose `Run whether user is logged on or not`.
4. On `Triggers`, add `At startup` or `At log on`.
5. On `Actions`, choose `Start a program`.
6. Set `Program/script` to the full path of `run_value_bet_alerts.bat`.
7. Set `Start in` to the `ev-finder` folder.

Because the Python script is long-running, Task Scheduler should launch it once and leave it running. Do not schedule it every minute unless you also change the script to `--once`.

### Polling budget examples

With `MAX_REQUESTS_PER_HOUR=100`:

- `1` bookmaker needs `POLL_INTERVAL_SECONDS >= 60`
- `2` bookmakers need `POLL_INTERVAL_SECONDS >= 72`
- `3` bookmakers need `POLL_INTERVAL_SECONDS >= 108`
- `4` bookmakers need `POLL_INTERVAL_SECONDS >= 144`

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
