import json
from pathlib import Path
from datetime import datetime, timedelta

STATE_FILE = Path("psp_monitor_state.json")

def load_seen_order_ids() -> set:
    """Load previously seen mismatch order_ids."""
    if STATE_FILE.exists():
        with open(STATE_FILE) as f:
            state = json.load(f)
            return set(state.get("seen_order_ids", []))
    return set()

def save_seen_order_ids(order_ids: set):
    """Save seen order_ids with timestamp."""
    state = {
        "seen_order_ids": list(order_ids),
        "last_updated": datetime.now().isoformat()
    }
    STATE_FILE.write_text(json.dumps(state, indent=2))

