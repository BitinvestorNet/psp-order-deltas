import requests
from abc import ABC
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Any, Optional
import pandas as pd
from dateutil import parser
from io import StringIO
import hmac
import hashlib
import base64
import time
import urllib.parse
import json
from typing import Dict, Any, List
from config import PSP_CONFIGS, PSP_FIELD_MAPPINGS, HOURS_BACK_SEARCH, NO_DECIMAL_CURRENCIES

class PSPBase(ABC):
    """Base class for PSP integrations."""
    PSP_NAME: str = ''  # Must be set by subclass
    
    def __init__(self, config: Dict[str, Any]):
        self.api_key = config.get('api_key')
        self.base_url = config.get('base_url', '')
        self.mapping = PSP_FIELD_MAPPINGS.get(self.PSP_NAME)
    
    def _get_field(self, payment: Dict[str, Any], field_name: str) -> Any:
        """Get field value or None."""
        return payment.get(field_name)
    
    def standardize_payment(self, payment: Dict[str, Any]) -> Dict[str, Any]:
        """Standardize payment using PSP-specific mapping."""
        if not self.mapping:
            raise ValueError(f"No field mapping defined for {self.PSP_NAME}")
        
        return {
            'psp': self.PSP_NAME,
            'order_id': self._get_field(payment, self.mapping.order_id),
            'created_date': self._get_field(payment, self.mapping.created_date),
            'amount': self._get_field(payment, self.mapping.amount),
            'currency': self._get_field(payment, self.mapping.currency),
            'status': self._get_field(payment, self.mapping.status),
            'transaction_id': self._get_field(payment, self.mapping.transaction_id),
            'payment_reference': self._get_field(payment, self.mapping.payment_reference)
        }

class AstroPayPSP(PSPBase):
    PSP_NAME = 'astropay'
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.base_url = "https://merchant-api.astropay.com"
    
    def _format_datetime(self, date_str: str) -> str:
        """Convert YYYY-MM-DD to YYYY-MM-DDTHH:MM:SS."""
        dt = datetime.fromisoformat(date_str)
        return dt.strftime('%Y-%m-%dT%H:%M:%S')
    
    def _fetch_page(self, created_from: str, created_to: str, page: int = 1, 
                   size: int = 2000, status: Optional[str] = None, 
                   country: Optional[str] = None) -> Dict[str, Any]:
        url = f"{self.base_url}/public/v1/onetouch/deposits/exportApi"
        headers = {
            'Merchants-Backend-Api-Key': self.api_key,
            'Content-Type': 'application/json'
        }
        params = {
            'createdFrom': created_from,
            'createdTo': created_to,
            'page': page,
            'size': size
        }
        if status: params['status'] = status
        if country: params['country'] = country
        
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        return response.json()
    
    def fetch_payments(self, start_date: str, end_date: str, 
                      status: Optional[str] = None, 
                      country: Optional[str] = None) -> List[Dict[str, Any]]:
        created_from = self._format_datetime(start_date)
        created_to = self._format_datetime(end_date)
        
        all_payments = []
        page = 1
        
        while True:
            data = self._fetch_page(created_from, created_to, page, status=status, country=country, size = 2000)
            payments = data.get('data')
            all_payments.extend(payments)
            if len(payments) < 2000:
                break
                
            page += 1
        
        return all_payments

class StripePSP(PSPBase):
    PSP_NAME = 'stripe'
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        import stripe
        self.stripe = stripe
        self.stripe.api_key = self.api_key

    def fetch_payments(self, start_date: str, end_date: str) -> List[Dict[str, Any]]:
        start_ts = int(datetime.fromisoformat(start_date).timestamp())
        end_ts = int(datetime.fromisoformat(end_date).timestamp())
        
        payments = []
        for pi in self.stripe.PaymentIntent.list(
            created={'gte': start_ts, 'lte': end_ts},
            limit=1000
        ).auto_paging_iter():
            desc = (pi.description or '').removeprefix("Order #")
            amt = pi.amount / 100 if pi.currency.lower() not in NO_DECIMAL_CURRENCIES else pi.amount
            payments.append({
                'id': pi.id,
                'created': datetime.fromtimestamp(pi.created).isoformat(),
                'amount': amt,
                'currency': pi.currency,
                'status': pi.status,
                'description': desc
            })
        return payments

class SkrillPSP(PSPBase):
    PSP_NAME = 'skrill'
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.email = config.get('email')
        self.api_key = config.get('api_key')  #
        self.base_url = "https://www.skrill.com/app/query.pl"
    
    def _format_date(self, date_str: str) -> str:
        """Convert YYYY-MM-DD to DD-MM-YYYY for Skrill MQI."""
        dt = datetime.fromisoformat(date_str)
        return dt.strftime('%d-%m-%Y')
    
    def _fetch_history(self, start_date: str, end_date: str) -> str:
        """Fetch Skrill transaction history via MQI."""
        start_formatted = self._format_date(date_str=start_date)
        end_formatted = self._format_date(date_str = end_date)
        
        params = {
            'email': self.email,
            'password': self.api_key,
            'action': 'history',
            'start_date': start_formatted,
            'end_date': end_formatted
        }
        
        response = requests.get(self.base_url, params=params, timeout=300)
        response.raise_for_status()
        return response.text
    
    def fetch_payments(self, start_date: str, end_date: str) -> List[Dict[str, Any]]:
        """Fetch all Skrill payments via MQI."""
        csv_content = self._fetch_history(start_date, end_date)
        df = pd.read_csv(StringIO(csv_content))
        df["Time (UTC)"] = (
            pd.to_datetime(df["Time (CET)"], format='%d %b %y %H:%M')
            .dt.tz_localize('CET')
            .dt.tz_convert('UTC')
            .dt.strftime('%Y-%m-%dT%H:%M:%S')
        )
        df = df[(df.Type == "Receive Money") & (~df['Amount Sent'].isna())]
        return df.to_dict('records')
    
class NicheclearPSP(PSPBase):
    PSP_NAME = 'nicheclear'
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.base_url = "https://app.nicheclear.com/api"  # Adjust based on your API docs
    
    def _fetch_page(self, start_date: str, end_date: str, offset: int = 1) -> Dict[str, Any]:
        """Fetch Nicheclear transactions."""
        url = f"{self.base_url}/v1/payments"  # or /payments
        
        headers = {
            'Authorization': f'Bearer {self.api_key}',
            'Content-Type': 'application/json'
        }
        
        params = {
            'created.gte': start_date,
            'created.lt': end_date,
            'offset': offset,
            'limit': 1000  # Adjust based on API
        }
        
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        return response.json()
    
    def fetch_payments(self, start_date: str, end_date: str) -> List[Dict[str, Any]]:
        """Fetch all Nicheclear payments with pagination."""
        all_payments = []
        offset = 0
        
        while True:
            data = self._fetch_page(start_date, end_date, offset)
            
            # Handle different response structures
            payments = data["result"]
            payments = [payment for payment in payments if payment["paymentType"] == "DEPOSIT"]
            all_payments.extend(payments)
            
            # Check pagination
            if data["hasMore"] == False:
                break
                
            offset += 1000
        
        return all_payments

class PensoPayPSP(PSPBase):
    PSP_NAME = 'pensopay'
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.base_url = "https://api.pensopay.com/v2"
    
    def _fetch_transactions(self, start_date: str, end_date: str, page: int) -> Dict[str, Any]:
        """Fetch PensoPay transactions."""
        url = f"{self.base_url}/payments"
        
        headers = {
            'Authorization': f'Bearer {self.api_key}',  # Base64 encoded merchant:api_key
            'Content-Type': 'application/json'
        }
        
        params = {
            'date_from': start_date + 'Z',
            'date_to': end_date + 'Z',
            'page': page,
            'per_page': 250
        }

        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        return response.json()
    
    def fetch_payments(self, start_date: str, end_date: str) -> List[Dict[str, Any]]:
        """Fetch all PensoPay payments with pagination."""
        all_payments = []
        page = 1
        
        while True:
            
            data = self._fetch_transactions(start_date, end_date, page)
            payments = data["data"]
            all_payments.extend(payments)
            
            # Check pagination
            if data["meta"]["current_page"] == data["meta"]["last_page"]:
                break
                
            page += 1

        for payment in all_payments:
            payment["amount"] /= 100

        return all_payments
    
class PayPalPSP(PSPBase):
    PSP_NAME = 'paypal'
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.base_url = "https://api-m.paypal.com"  # sandbox, change to api-m.paypal.com for live
        self.client_id = config.get('client_id')
        self.client_secret = config.get('client_secret')
        self.access_token = None
    
    def _get_access_token(self) -> str:
        """Get PayPal OAuth2 access token."""
        if self.access_token:
            return self.access_token
        
        url = f"{self.base_url}/v1/oauth2/token"
        data = {
            'grant_type': 'client_credentials'
        }
        auth = (self.client_id, self.client_secret)
        
        response = requests.post(url, auth=auth, data=data)
        response.raise_for_status()
        
        token_data = response.json()
        self.access_token = token_data['access_token']
        return self.access_token
    
    def _fetch_transactions(self, start_date: str, end_date: str, page_size: int = 500, page: int = 1) -> Dict[str, Any]:
        """Fetch PayPal transactions."""
        token = self._get_access_token()
        
        url = f"{self.base_url}/v1/reporting/transactions"
        headers = {
            'Authorization': f'Bearer {token}',
            'Content-Type': 'json',
            'Accept': 'application/json'
        }
        params = {
            'start_date': pd.to_datetime(start_date,utc=True).strftime('%Y-%m-%dT%H:%M:%SZ'),
            'end_date': pd.to_datetime(end_date,utc=True).strftime('%Y-%m-%dT%H:%M:%SZ'),
            'page': page,
            'page_size': page_size,
        }
        
        response = requests.get(url, headers=headers, params=params)
        
        if response.status_code != 200:
            return {}
        
        return response.json()
    
    def _process_paypal_response(self, raw_transactions: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Flatten PayPal nested response to standard format."""
        payments = []
        
        for txn in raw_transactions:
            txn_info = txn['transaction_info']
            
            payments.append({
                'transaction_id': txn_info['transaction_id'],
                'paypal_reference_id': txn_info['paypal_reference_id'],
                'transaction_initiation_date': txn_info['transaction_initiation_date'],
                'transaction_status': txn_info['transaction_status'],
                'order_id': txn_info['invoice_id'],
                # Extract nested amounts
                'amount': txn_info['transaction_amount']['value'],
                'currency': txn_info['transaction_amount']['currency_code'],
                # Fees (negative values)
                'fee_amount': -float(txn_info['fee_amount']['value']) if txn_info.get('fee_amount') else 0
            })
        
        return payments
    
    def fetch_payments(self, start_date: str, end_date: str) -> List[Dict[str, Any]]:
        """Fetch all PayPal payments."""
        all_payments = []
        page_size = 500
        page = 1
        while True:
            data = self._fetch_transactions(start_date, end_date, page_size, page)
            if not data.get("transaction_details"):
                break
            payments = self._process_paypal_response(data['transaction_details'])
            all_payments.extend(payments)
            if page == data["total_pages"]:
                break
            page += 1
        return all_payments

class RevolutPSP(PSPBase):
    PSP_NAME = 'revolut'
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.base_url = "https://merchant.revolut.com/api"
    
    def _fetch_orders(self, start_date: str, end_date: str, page_cursor: str = None, page_size: int = 1000, created_before: str = None) -> Dict[str, Any]:
        """Fetch Revolut orders."""
        url = f"{self.base_url}/1.0/orders"
        
        headers = {
            'Authorization': f'Bearer {self.api_key}',
            'Content-Type': 'application/json',
            'Revolut-Api-Version': '2025-12-04'
        }
        
        params = {
            'from_created_date': start_date + 'Z',
            'to_created_date': end_date + 'Z',
            'limit': page_size,
            'created_before': created_before
        }
        
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        return response.json()
    
    def fetch_payments(self, start_date: str, end_date: str) -> List[Dict[str, Any]]:
        """Fetch all Revolut payments with cursor pagination."""
        all_payments = []
        page_size = 1000
        created_before = None
        
        while True:
            data = self._fetch_orders(start_date, end_date, page_size=page_size, created_before = created_before)
            created_before = data[-1]["created_at"]
            data =[
                {**p, 
                 "order_currency": p["order_amount"]["currency"],
                 "order_amount": p["order_amount"]["value"] / 100 if p["order_amount"]["currency"].lower() not in NO_DECIMAL_CURRENCIES else p["order_amount"]["value"], 
                 } 
                 for p in data
                ]
            all_payments.extend(data)
            if len(data) < page_size:
                break

        return all_payments
    
class JanuarPSP(PSPBase):
    PSP_NAME = 'januar'
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.base_url = "https://api.januar.com"  # Production
        self.api_key = config.get('api_key')
        self.api_secret = config.get('api_secret')
        self.account_id = config.get('account_id')  # Get from /accounts first
    
    def _generate_auth_header(self, method: str, path: str, body: str = '') -> str:
        """Generate Januar HMAC-SHA256 signature."""
        nonce = str(int(time.time() * 1000))  # Unix timestamp ms
        
        # URL encode path
        encoded_path = urllib.parse.quote(path, safe='')
        
        # Message to sign: nonce|METHOD|path|body
        message = f"{nonce}|{method}|{encoded_path}|{body}".encode('utf-8')
        
        # HMAC-SHA256 with secret
        signature = hmac.new(
            self.api_secret.encode('utf-8'),
            message,
            hashlib.sha256
        ).digest()
        
        # Base64 encode
        signature_b64 = base64.b64encode(signature).decode('utf-8')
        
        return f'JanuarAPI apikey="{self.api_key}", nonce="{nonce}", signature="{signature_b64}"'
    
    def _get_accounts(self) -> List[Dict[str, Any]]:
        """Get account IDs first."""
        path = "/accounts"
        headers = {
            'Authorization': self._generate_auth_header('GET', path),
            'Content-Type': 'application/json'
        }
        
        response = requests.get(f"{self.base_url}{path}", headers=headers)
        response.raise_for_status()
        data = response.json()
        return data['data']
    
    def _fetch_transactions(self, account_id: str, date_from: str, date_to: str, 
                          page: int = 0, page_size: int = 1000) -> Dict[str, Any]:
        """Fetch Januar transactions."""
        path = f"/accounts/{account_id}/transactions"
        
        params = {
            'dateFrom': datetime.fromisoformat(date_from).date().isoformat(),  # YYYY-MM-DD
            'dateTo': datetime.fromisoformat(date_to).date().isoformat(),      # YYYY-MM-DD
            'pageSize': page_size,
            'page': page
        }
        
        full_path = f"{path}?{'&'.join([f'{k}={v}' for k,v in params.items()])}"
        
        headers = {
            'Authorization': self._generate_auth_header('GET', full_path),
            'Content-Type': 'application/json'
        }
        
        response = requests.get(f"{self.base_url}{path}", headers=headers, params=params)
        response.raise_for_status()
        return response.json()
    
    def fetch_payments(self, start_date: str, end_date: str) -> List[Dict[str, Any]]:
        """Fetch all Januar PAYIN transactions."""
        page = 0
        all_payments = []
        while True:
            data = self._fetch_transactions(self.account_id, start_date, end_date, page)
                
            payments = data.get('data')
            payins = [t for t in payments if t["type"] == 'PAYIN' and t.get("message")]
                
            if not payins:
                break

            for txn in payins:
                # Extract order_id: remove "Swapped " prefix from message
                message = txn['message']
                message = message.lower().split('swapped', 1)[-1] if 'swapped' in message.lower() else message
                order_id = message.lower().replace('swapped', '', 1).lstrip() if message.lower().startswith('swapped') else message                
                txn["message"] = order_id    
    
            all_payments.extend(payins)
            
            pagination = data.get('metadata').get('pagination')
            if pagination["totalRecords"] < pagination["pageSize"]:
                break
                
            page += 1
        
        return all_payments
 
class PaymentMonitor:
    """Main monitoring class."""
    
    def __init__(self):
        self.psps: Dict[str, PSPBase] = {}
        self._init_psps()
    
    def _init_psps(self):
        psp_classes = {
            'astropay': AstroPayPSP,
            'stripe': StripePSP,
            'skrill': SkrillPSP,
            'nicheclear': NicheclearPSP,
            'pensopay': PensoPayPSP,
            'paypal': PayPalPSP,
            'revolut': RevolutPSP,
            'januar': JanuarPSP,
        }
        
        for name, config in PSP_CONFIGS.items():
            if cls := psp_classes.get(name):
                    self.psps[name] = cls(config)

    def fetch_all_payments(self, hours_back: int = 1) -> pd.DataFrame:
        """Fetch payments from all PSPs."""
        end_date = datetime.now(timezone.utc)
        start_date = end_date - timedelta(hours=hours_back)
        start_str = start_date.isoformat().replace('+00:00', '')
        end_str = end_date.isoformat().replace('+00:00', '')
        
        all_payments = []
        for name, psp in self.psps.items():
            print(f"Fetching {name} payments...")
            try:
                raw_payments = psp.fetch_payments(start_str, end_str)
                std_payments = [psp.standardize_payment(p) for p in raw_payments]
                all_payments.extend(std_payments)
                print(f"  Found {len(raw_payments)} payments")
            except Exception as e:
                print(f"  Error fetching {name}: {e}")
        
        df = pd.DataFrame(all_payments)
        if not df.empty:
            df['created_date'] = pd.to_datetime(df['created_date'], format='%Y-%m-%dT%H:%M:%S%z', utc=True)
            df["payment_reference"] = df["payment_reference"].str.strip()
            df["amount"] = df["amount"].astype(float)
            df = df.sort_values('created_date')
        
        return df[df.created_date >= pd.Timestamp.now(tz='UTC') - pd.Timedelta(hours=HOURS_BACK_SEARCH)]


