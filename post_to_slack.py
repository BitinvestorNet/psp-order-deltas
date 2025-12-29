import requests
from typing import Dict, Any
import pandas as pd
import os
import logger

def post_slack_webhook(webhook_url: str, message: str, blocks: list = None) -> bool:
    """Post message to Slack webhook."""
    payload = {
        "text": message,
        "blocks": blocks or []
    }
    
    try:
        response = requests.post(
            webhook_url, 
            json=payload,
            headers={"Content-Type": "application/json"},
            timeout=10
        )
        response.raise_for_status()
        return True
    except requests.RequestException as e:
        logger.error(f"Slack webhook failed: {e}")
        return False

def alert_slack(mismatches: pd.DataFrame):
    blocks = [
        {
            "type": "header",
            "text": {"type": "plain_text", "text": f"🚨 {len(mismatches)} PSP MISMATCHES"}
        }
    ]
    
    for _, row in mismatches.iterrows():
        blocks.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"*PSP:* `{row['psp']}`\n*order_id:* `{row['order_id']}`\n*PSP amount:* `{row['amount']:.2f}`\n*DB amount:* `{row['order_total']:.2f}`\n*delta:* `{row['delta']:.2f}`"
            }
        })
        blocks.append({"type": "divider"})
    
    return post_slack_webhook(os.getenv("SLACK_WEBHOOK_URL"), f"🚨 {len(mismatches)} mismatches", blocks)
