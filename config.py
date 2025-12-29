from dataclasses import dataclass
import os

HOURS_BACK_SEARCH = 2

# Config
PSP_CONFIGS = {
    'astropay': {'api_key': os.getenv('ASTROPAY_API_KEY')},
    'stripe': {'api_key': os.getenv('STRIPE_API_KEY')},
    'skrill': {'api_key': os.getenv('SKRILL_API_KEY'), 'email': os.getenv("SKRILL_EMAIL")},
    'nicheclear': {'api_key': os.getenv('NICHECLEAR_API_KEY')},
    'pensopay': {'api_key': os.getenv('PENSOPAY_API_KEY')},
    'paypal': {'client_id': os.getenv('PAYPAL_CLIENT_ID'), 'client_secret': os.getenv('PAYPAL_CLIENT_SECRET')},
    #'revolut': {'api_key': os.getenv('REVOLUT_API_KEY')},
    'januar': {"api_key": os.getenv("JANUAR_API_KEY"), 'api_secret': os.getenv("JANUAR_API_SECRET"), "account_id": os.getenv("JANUAR_ACCOUNT_ID")}
}

@dataclass
class FieldMapping:
    """PSP-specific field mappings - single field name per PSP."""
    order_id: str
    created_date: str
    amount: str
    currency: str
    status: str
    transaction_id: str
    payment_reference: str

# ADD NEW PSP's HERE
PSP_FIELD_MAPPINGS = {
    'astropay': FieldMapping(
        order_id='reference',
        created_date='creation_date',
        amount='amount',
        currency='currency',
        status='status',
        transaction_id='deposit_external_id',
        payment_reference=None
    ),
    'stripe': FieldMapping(
        order_id='description',
        created_date='created',
        amount='amount',
        currency='currency',
        status='status',
        transaction_id='id',
        payment_reference=None
    ),
    'skrill': FieldMapping(
        order_id='Reference',
        created_date='Time (UTC)',
        amount='Amount Sent',
        currency='Currency sent',
        status='Status',
        transaction_id='ID of the corresponding Skrill transaction',
        payment_reference=None
    ),
    'nicheclear': FieldMapping(
        order_id='referenceId',
        created_date='created',      
        amount='amount',
        currency='currency',
        status='state',
        transaction_id='id',
        payment_reference=None
    ),
    'pensopay': FieldMapping(
        order_id='order_id',
        created_date='created_at',      
        amount='amount',
        currency='currency',
        status='state',
        transaction_id='id',
        payment_reference=None
    ),
    'paypal': FieldMapping(
        order_id='order_id',
        created_date='transaction_initiation_date',
        amount='amount',
        currency='currency',
        status='transaction_status',
        transaction_id='transaction_id',
        payment_reference=None
    ),
    'revolut': FieldMapping(
        order_id='merchant_order_ext_ref',
        created_date='started_date',
        amount='amount',
        currency='currency',
        status='state',
        transaction_id='id',
        payment_reference=None
        ),
    'januar': FieldMapping(
        order_id=None,
        payment_reference="message",
        created_date="completedTime",
        amount="amount",
        currency="currency",
        status=None,
        transaction_id="id",
    )
}