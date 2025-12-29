## psp-order-deltas
Monitoring service that alerts on discrepancies between internal order_total values and order_total reported by payment service providers (PSPs).
​

- payment_providers.py contains classes for each PSP to fetch, clean, standardize, and combine PSP data.

- database_orders.py contains functions to read data from the orders table in the production database.

- monitor.py runs the relevant functions from payment_providers.py and database_orders.py and matches PSP orders with DB orders.

- post_to_slack.py contains functions to post to the #order_deltas_alert Slack channel via webhook in case of discrepancies.
​
- filter_duplicates.py ensures that the same order is not posted repeatedly and keeps a log of orders with discrepancies for historical checks.
​
- config.py configures field mappings and API keys, etc. for each PSP; secrets such as API keys should be stored in a .env file.

- main.py combines all of the above and runs the monitoring script.

Currently monitoring: Astropay, Skrill, Stripe, Nicheclear, Revolut, Januar, Pensopay, Januar