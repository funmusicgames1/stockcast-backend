"""
datetime_utils.py — CST-aware "today" helper.

GitHub Actions runners use UTC. This whole system is designed around
America/Chicago (the cron's intended 6 AM CST slot), so every "what day is
it" decision needs CST's calendar date, not UTC's — otherwise a run that
executes late at night UTC (a delayed scheduled run, or a manual trigger)
can land on the wrong side of the UTC midnight boundary while it's still
the same calendar day in CST. This caused a real gap on 2026-10-26/27:
a manual run at 00:56 UTC (still Oct 26 evening in CST) got labeled Oct 27
by bare date.today(), and was later overwritten by the real Oct 27 run.

Use today_cst() everywhere a "today"/"yesterday" label is computed — never
bare date.today().
"""

from datetime import date, datetime
import pytz

_CST = pytz.timezone("America/Chicago")


def today_cst() -> date:
    return datetime.now(_CST).date()
