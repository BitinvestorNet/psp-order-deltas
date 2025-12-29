#!/usr/bin/env python3
from dotenv import load_dotenv
load_dotenv()
import logging
import logger
from config import HOURS_BACK_SEARCH
from post_to_slack import alert_slack
from filter_duplicates import load_seen_order_ids, save_seen_order_ids
from monitor import monitor_deltas

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
for name in ["urllib3", "requests", "stripe", "httpx"]:
    logging.getLogger(name).setLevel(logging.WARNING)

def main():
    logger.info(f"Fetching payments from last {HOURS_BACK_SEARCH} hours")
    mismatches = monitor_deltas()
    if len(mismatches) > 0:
        # Filter NEW mismatches only
        seen_order_ids = load_seen_order_ids()
        new_mismatches = mismatches[~mismatches['order_id'].astype(str).isin(seen_order_ids)]
        
        if len(new_mismatches) > 0:
            logger.info(f"Found {len(new_mismatches)} NEW mismatches (total {len(mismatches)})")
            alert_slack(new_mismatches)
            
            # Update state with ALL seen (new + old)
            all_seen = set(seen_order_ids) | set(new_mismatches['order_id'].astype(str))
            save_seen_order_ids(all_seen)
        else:
            logger.info("No new mismatches (all previously seen)")
    
if __name__ == "__main__":
    main()
