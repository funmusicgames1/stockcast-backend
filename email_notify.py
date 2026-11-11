"""
email_notify.py — Sends the daily Levels email via Gmail SMTP (stdlib only,
no new paid service). Requires a Gmail App Password (Google Account >
Security > App Passwords, needs 2FA already on) set as GMAIL_APP_PASSWORD,
and the sending address as GMAIL_SENDER. Sends to the same address.
"""

import os
import csv
import io
import smtplib
import logging
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders

logger = logging.getLogger(__name__)

SMTP_HOST = "smtp.gmail.com"
SMTP_PORT = 465  # implicit SSL -- STARTTLS on 587 gets reset ("Connection
                 # unexpectedly closed") from GitHub Actions runner IP ranges


def _send(subject: str, html_body: str, csv_rows: list, csv_headers: list, csv_filename: str) -> bool:
    sender   = os.getenv("GMAIL_SENDER")
    app_pass = os.getenv("GMAIL_APP_PASSWORD")
    if not sender or not app_pass:
        logger.warning("GMAIL_SENDER/GMAIL_APP_PASSWORD not set — skipping email.")
        return False

    msg = MIMEMultipart()
    msg["From"]    = sender
    msg["To"]      = sender
    msg["Subject"] = subject
    msg.attach(MIMEText(html_body, "html"))

    if csv_rows:
        buf = io.StringIO()
        writer = csv.DictWriter(buf, fieldnames=csv_headers, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(csv_rows)

        part = MIMEBase("text", "csv")
        part.set_payload(buf.getvalue())
        encoders.encode_base64(part)
        part.add_header("Content-Disposition", f"attachment; filename={csv_filename}")
        msg.attach(part)

    try:
        with smtplib.SMTP_SSL(SMTP_HOST, SMTP_PORT, timeout=20) as server:
            server.login(sender, app_pass)
            server.sendmail(sender, [sender], msg.as_string())
        logger.info(f"Email sent: {subject}")
        return True
    except Exception as e:
        logger.error(f"Failed to send email: {e}")
        return False


def send_pivot_levels_email(rows: list, date_str: str) -> bool:
    """7 AM email: OHLC + Pivot/Support/Resistance."""
    if not rows:
        logger.warning("No pivot levels rows — skipping email.")
        return False

    table_rows = "".join(
        f"<tr><td>{r['ticker']}</td><td>{r['open']}</td><td>{r['high']}</td>"
        f"<td>{r['low']}</td><td>{r['close']}</td><td>{r['pivot']}</td>"
        f"<td>{r['support']}</td><td>{r['resistance']}</td></tr>"
        for r in rows
    )
    html = f"""
    <h2>StockCast Daily Levels — {date_str}</h2>
    <p>OHLC + Pivot/Support/Resistance from {rows[0]['prev_date']}'s close.</p>
    <table border="1" cellpadding="6" cellspacing="0" style="border-collapse:collapse;font-family:monospace">
      <tr><th>Ticker</th><th>Open</th><th>High</th><th>Low</th><th>Close</th>
          <th>Pivot</th><th>Support</th><th>Resistance</th></tr>
      {table_rows}
    </table>
    <p style="color:#888;font-size:0.85em">Gap levels (X1-X5) follow in a separate email after today's market open.</p>
    """
    headers = ["ticker", "prev_date", "open", "high", "low", "close", "pivot", "support", "resistance"]
    return _send(f"StockCast Levels {date_str} — Pivot/S/R", html, rows, headers, f"levels_pivot_{date_str}.csv")


def send_gap_levels_email(rows: list, date_str: str) -> bool:
    """~9:34 AM email: gap-based X1-X5 levels."""
    if not rows:
        logger.warning("No gap levels rows — skipping email.")
        return False

    table_rows = "".join(
        f"<tr><td>{r['ticker']}</td><td>{r['gap_direction']}</td><td>{r['x1']}</td>"
        f"<td>{r['x2']}</td><td>{r['x3']}</td><td>{r['x4']}</td><td>{r['x5']}</td></tr>"
        for r in rows
    )
    html = f"""
    <h2>StockCast Daily Levels — {date_str} (Gap Levels)</h2>
    <table border="1" cellpadding="6" cellspacing="0" style="border-collapse:collapse;font-family:monospace">
      <tr><th>Ticker</th><th>Gap</th><th>X1</th><th>X2</th><th>X3</th><th>X4</th><th>X5</th></tr>
      {table_rows}
    </table>
    """
    headers = ["ticker", "a1", "b1", "gap_direction", "x1", "x2", "x3", "x4", "x5"]
    return _send(f"StockCast Levels {date_str} — Gap X1-X5", html, rows, headers, f"levels_gap_{date_str}.csv")
