"""
auth.py — Authentication module for Databricks Matrimony Match Analyser
Provides signup, OTP email verification, login, and audit logging.
"""

import os
import hashlib
import secrets
import smtplib
import logging
import time
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime, timedelta
import pandas as pd

log = logging.getLogger("matrimony_app")

# ── Config ────────────────────────────────────────────────────────────────
_SMTP_EMAIL    = os.getenv("SMTP_EMAIL", "")
_SMTP_PASSWORD = os.getenv("SMTP_PASSWORD", "")
_SMTP_SERVER   = os.getenv("SMTP_SERVER", "smtp.gmail.com")
_SMTP_PORT     = int(os.getenv("SMTP_PORT", "587"))
_OTP_EXPIRY_MIN = 10

USERS_TABLE = "matrimony.default.app_users"
OTP_TABLE   = "matrimony.default.app_otp_codes"
AUDIT_TABLE = "matrimony.default.app_audit_logs"

# ── SQL helpers (injected by app.py) ─────────────────────────────────────
_run_query = None
_execute_dml = None

def init_sql_helpers(run_query_fn, execute_dml_fn):
    global _run_query, _execute_dml
    _run_query = run_query_fn
    _execute_dml = execute_dml_fn

# ── Password hashing ─────────────────────────────────────────────────────
def hash_password(password, salt=None):
    if salt is None:
        salt = secrets.token_hex(16)
    hashed = hashlib.pbkdf2_hmac("sha256", password.encode(), bytes.fromhex(salt), 100000)
    return hashed.hex(), salt

def verify_password(password, stored_hash, salt):
    hashed, _ = hash_password(password, salt)
    return secrets.compare_digest(hashed, stored_hash)

# ── Audit logging ────────────────────────────────────────────────────────
def log_audit_event(email, event_type, details=""):
    try:
        safe_details = details.replace("'", "`")
        safe_email = (email or "").replace("'", "`")
        _execute_dml(
            f"INSERT INTO {AUDIT_TABLE} (email, event_type, event_details, created_at) "
            f"VALUES ('{safe_email}', '{event_type}', '{safe_details}', current_timestamp())"
        )
        log.info(f"[AUDIT] {event_type} | email={email}")
    except Exception as e:
        log.error(f"[AUDIT] Failed to log event: {e}")

# ── Email OTP ────────────────────────────────────────────────────────────
def send_otp_email(to_email, otp_code):
    if not _SMTP_EMAIL or not _SMTP_PASSWORD:
        log.warning("[SMTP] SMTP not configured — OTP shown in logs only")
        log.info(f"[OTP] Code for {to_email}: {otp_code}")
        return True
    try:
        msg = MIMEMultipart("alternative")
        msg["Subject"] = "Databricks Matrimony — OTP Verification Code"
        msg["From"] = _SMTP_EMAIL
        msg["To"] = to_email
        body = (
            "<div style='font-family:Arial,sans-serif;max-width:400px;padding:20px;'>"
            "<h2 style='color:#c0392b;'>Databricks Matrimony Match Analyser</h2>"
            "<p>Your One-Time Password (OTP) is:</p>"
            f"<div style='font-size:28px;font-weight:bold;letter-spacing:6px;"
            f"color:#c0392b;background:#fdf2f2;padding:15px;border-radius:8px;"
            f"text-align:center;margin:15px 0;'>{otp_code}</div>"
            f"<p style='color:#666;font-size:13px;'>Expires in {_OTP_EXPIRY_MIN} minutes.</p>"
            "</div>"
        )
        msg.attach(MIMEText(body, "html"))
        with smtplib.SMTP(_SMTP_SERVER, _SMTP_PORT) as server:
            server.starttls()
            server.login(_SMTP_EMAIL, _SMTP_PASSWORD)
            server.send_message(msg)
        log.info(f"[SMTP] OTP sent to {to_email}")
        return True
    except Exception as e:
        log.error(f"[SMTP] Failed: {e}")
        log.info(f"[OTP] Code for {to_email}: {otp_code}")
        return True

# ── Signup ────────────────────────────────────────────────────────────────
def handle_signup(name, email, password):
    if not name or not email or not password:
        return "Please fill in all fields."
    email = email.strip().lower()
    if len(password) < 6:
        return "Password must be at least 6 characters."
    existing = _run_query(f"SELECT email, status FROM {USERS_TABLE} WHERE email = '{email}'")
    if len(existing) > 0:
        status = existing.iloc[0]["status"]
        if status == "ACTIVE":
            return "Already registered. Please login."
        otp = str(secrets.randbelow(900000) + 100000)
        exp = (datetime.now() + timedelta(minutes=_OTP_EXPIRY_MIN)).strftime("%Y-%m-%d %H:%M:%S")
        _execute_dml(
            f"INSERT INTO {OTP_TABLE} (email, otp_code, expires_at, used, created_at) "
            f"VALUES ('{email}', '{otp}', '{exp}', false, current_timestamp())"
        )
        send_otp_email(email, otp)
        log_audit_event(email, "OTP_RESENT", "Pending user resend")
        return f"OTP resent to {email}. Enter the code in Verify OTP tab."
    pw_hash, pw_salt = hash_password(password)
    _execute_dml(
        f"INSERT INTO {USERS_TABLE} (email, name, password_hash, password_salt, status, created_at, updated_at) "
        f"VALUES ('{email}', '{name.strip()}', '{pw_hash}', '{pw_salt}', 'PENDING', current_timestamp(), current_timestamp())"
    )
    otp = str(secrets.randbelow(900000) + 100000)
    exp = (datetime.now() + timedelta(minutes=_OTP_EXPIRY_MIN)).strftime("%Y-%m-%d %H:%M:%S")
    _execute_dml(
        f"INSERT INTO {OTP_TABLE} (email, otp_code, expires_at, used, created_at) "
        f"VALUES ('{email}', '{otp}', '{exp}', false, current_timestamp())"
    )
    send_otp_email(email, otp)
    log_audit_event(email, "SIGNUP", f"New user: {name.strip()}")
    log_audit_event(email, "OTP_SENT", "Verification OTP sent")
    return f"Signup successful! OTP sent to {email}. Enter the 6-digit code in Verify OTP tab."

# ── OTP verification ──────────────────────────────────────────────────────
def handle_otp_verify(email, otp_code):
    if not email or not otp_code:
        return "Please enter email and OTP code.", False
    email = email.strip().lower()
    otp_code = otp_code.strip()
    rows = _run_query(
        f"SELECT otp_code, expires_at, used FROM {OTP_TABLE} "
        f"WHERE email = '{email}' ORDER BY created_at DESC LIMIT 1"
    )
    if len(rows) == 0:
        return "No OTP found. Please sign up first.", False
    row = rows.iloc[0]
    if row["used"]:
        return "OTP already used. Please request a new one.", False
    if datetime.now() > pd.to_datetime(row["expires_at"]):
        return "OTP expired. Please sign up again.", False
    if row["otp_code"] != otp_code:
        log_audit_event(email, "OTP_FAILED", "Wrong code")
        return "Incorrect OTP. Please try again.", False
    _execute_dml(f"UPDATE {USERS_TABLE} SET status = 'ACTIVE', updated_at = current_timestamp() WHERE email = '{email}'")
    _execute_dml(f"UPDATE {OTP_TABLE} SET used = true WHERE email = '{email}' AND otp_code = '{otp_code}'")
    log_audit_event(email, "OTP_VERIFIED", "Account activated")
    return "Email verified! Account is now active. Please login.", True

# ── Login ─────────────────────────────────────────────────────────────────
def handle_login(email, password):
    if not email or not password:
        return "Please enter email and password.", None
    email = email.strip().lower()
    rows = _run_query(f"SELECT email, name, password_hash, password_salt, status FROM {USERS_TABLE} WHERE email = '{email}'")
    if len(rows) == 0:
        log_audit_event(email, "LOGIN_FAILED", "Email not found")
        return "Email not registered. Please sign up first.", None
    row = rows.iloc[0]
    if row["status"] != "ACTIVE":
        return "Account not verified. Please verify with OTP first.", None
    if not verify_password(password, row["password_hash"], row["password_salt"]):
        log_audit_event(email, "LOGIN_FAILED", "Wrong password")
        return "Incorrect password. Please try again.", None
    log_audit_event(email, "LOGIN_SUCCESS", f"User: {row['name']}")
    return f"Welcome, {row['name']}! Login successful.", email
