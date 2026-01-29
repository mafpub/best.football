"""Zoho Mail API integration for camp notification emails."""

import json
import logging
import os
from pathlib import Path

import requests

from .zoho_oauth import ZohoOAuth

logger = logging.getLogger(__name__)

# Template loading with caching
_template_cache: dict[str, dict] = {}
TEMPLATE_DIR = Path(__file__).parent / "emails"


def _load_template(template_id: str) -> dict | None:
    """Load email template from JSON file with caching."""
    if template_id in _template_cache:
        return _template_cache[template_id]

    template_path = TEMPLATE_DIR / f"{template_id}.json"
    if not template_path.exists():
        logger.error(f"Template not found: {template_path}")
        return None

    try:
        with open(template_path) as f:
            template = json.load(f)
            _template_cache[template_id] = template
            return template
    except Exception as e:
        logger.error(f"Failed to load template {template_id}: {e}")
        return None


def _render_template(template_id: str, **variables) -> tuple[str, str] | None:
    """Render template with variables, returning (subject, body) or None."""
    template = _load_template(template_id)
    if not template:
        return None

    try:
        subject = template["subject"].format(**variables)
        body = template["body"].format(**variables)
        return subject, body
    except KeyError as e:
        logger.error(f"Missing template variable for {template_id}: {e}")
        return None


def send_email(
    to_email: str,
    subject: str,
    html_body: str,
    from_email: str | None = None,
) -> tuple[bool, str]:
    """
    Core email sending function - all OAuth and API logic centralized here.

    Args:
        to_email: Recipient email address
        subject: Email subject line
        html_body: HTML content of the email
        from_email: Sender address (defaults to ZOHO_FROM_EMAIL env var)

    Returns:
        Tuple of (success, message)
    """
    oauth = ZohoOAuth()
    access_token = oauth.get_valid_access_token()
    if not access_token:
        return False, "Failed to get valid Zoho OAuth token"

    account_id = oauth.get_account_id()
    if not account_id:
        return False, "Missing ZOHO_ACCOUNT_ID configuration"

    from_email = from_email or os.getenv("ZOHO_FROM_EMAIL", "support@best.football")

    payload = {
        "fromAddress": from_email,
        "toAddress": to_email,
        "subject": subject,
        "content": html_body,
        "mailFormat": "html",
    }

    headers = {
        "Authorization": f"Zoho-oauthtoken {access_token}",
        "Content-Type": "application/json",
    }

    try:
        response = requests.post(
            f"https://mail.zoho.com/api/accounts/{account_id}/messages",
            json=payload,
            headers=headers,
            timeout=30,
        )
        if response.status_code == 200:
            return True, "Email sent successfully"
        else:
            error_msg = f"Zoho API error: {response.status_code} - {response.text}"
            logger.error(error_msg)
            return False, error_msg
    except Exception as e:
        error_msg = f"Failed to send email: {e}"
        logger.error(error_msg)
        return False, error_msg


def send_camp_submitted_email(
    camp_id: str,
    camp_name: str,
    city: str,
    state: str,
    organizer_type: str,
    submitted_by: str,
    submitted_email: str,
    start_date: str | None,
    end_date: str | None,
    action_token: str,
) -> tuple[bool, str]:
    """Send notification to admin when a new camp is submitted."""
    base_url = os.getenv("BASE_URL", "https://best.football")
    variables = {
        "camp_id": camp_id,
        "camp_name": camp_name,
        "city": city,
        "state": state,
        "organizer_type": organizer_type,
        "submitted_by": submitted_by,
        "submitted_email": submitted_email,
        "start_date": start_date or "Not specified",
        "end_date": end_date or "Not specified",
        "approve_url": f"{base_url}/api/camps/{camp_id}/approve?token={action_token}",
        "reject_url": f"{base_url}/api/camps/{camp_id}/reject?token={action_token}",
    }

    result = _render_template("camp_submitted", **variables)
    if not result:
        return False, "Failed to render camp_submitted template"

    subject, body = result
    to_email = os.getenv("CAMP_NOTIFICATION_EMAIL", "support@best.football")

    success, msg = send_email(to_email, subject, body)
    if success:
        logger.info(f"Camp submission notification sent for {camp_name} (ID: {camp_id})")
    return success, msg


def send_camp_approved_email(
    to_email: str,
    camp_name: str,
    camp_id: str,
    state: str,
) -> tuple[bool, str]:
    """Send approval notification to camp submitter."""
    base_url = os.getenv("BASE_URL", "https://best.football")
    variables = {
        "camp_name": camp_name,
        "camp_url": f"{base_url}/camps/{state.lower()}/{camp_id}.html",
    }

    result = _render_template("camp_approved", **variables)
    if not result:
        return False, "Failed to render camp_approved template"

    subject, body = result
    success, msg = send_email(to_email, subject, body)
    if success:
        logger.info(f"Camp approval email sent to {to_email} for {camp_name}")
    return success, msg


def send_camp_rejected_email(
    to_email: str,
    camp_name: str,
) -> tuple[bool, str]:
    """Send rejection notification to camp submitter."""
    variables = {
        "camp_name": camp_name,
    }

    result = _render_template("camp_rejected", **variables)
    if not result:
        return False, "Failed to render camp_rejected template"

    subject, body = result
    success, msg = send_email(to_email, subject, body)
    if success:
        logger.info(f"Camp rejection email sent to {to_email} for {camp_name}")
    return success, msg
