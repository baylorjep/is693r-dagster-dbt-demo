"""
Extract asset: Generate raw CSV data for the wedding marketplace.

This module generates deterministic fake data for:
- customers: People looking for wedding services
- vendors: Service providers (photographers, caterers, etc.)
- requests: Customer requests for services
- bids: Vendor bids on requests
- payments: Completed payments for accepted bids
"""

import os
from pathlib import Path
from datetime import datetime, timedelta
import random

import pandas as pd
from faker import Faker
from dagster import asset, AssetExecutionContext, Output, MetadataValue

# Set deterministic seed for reproducibility
SEED = 42
random.seed(SEED)
Faker.seed(SEED)
fake = Faker()

# Project paths
PROJECT_ROOT = Path(__file__).parent.parent.parent
DATA_DIR = PROJECT_ROOT / "data" / "raw"

# Wedding service categories
CATEGORIES = [
    "Photography",
    "Videography", 
    "Catering",
    "Florist",
    "DJ/Music",
    "Wedding Planner",
    "Cake/Bakery",
    "Hair & Makeup",
    "Officiant",
    "Transportation",
]

# Status options
REQUEST_STATUSES = ["open", "in_progress", "completed", "cancelled"]
BID_STATUSES = ["pending", "accepted", "rejected", "withdrawn"]
PAYMENT_METHODS = ["credit_card", "bank_transfer", "paypal", "check"]

# US States for realistic locations
US_STATES = [
    "CA", "TX", "FL", "NY", "PA", "IL", "OH", "GA", "NC", "MI",
    "NJ", "VA", "WA", "AZ", "MA", "TN", "IN", "MO", "MD", "WI",
    "CO", "MN", "SC", "AL", "LA", "KY", "OR", "OK", "CT", "UT",
]


def generate_customers(n: int = 300) -> pd.DataFrame:
    """Generate customer data."""
    customers = []
    base_date = datetime(2023, 1, 1)
    
    for i in range(1, n + 1):
        created_at = base_date + timedelta(days=random.randint(0, 365))
        customers.append({
            "customer_id": i,
            "first_name": fake.first_name(),
            "last_name": fake.last_name(),
            "email": fake.email(),
            "phone": fake.phone_number(),
            "city": fake.city(),
            "state": random.choice(US_STATES),
            "created_at": created_at.strftime("%Y-%m-%d %H:%M:%S"),
        })
    
    return pd.DataFrame(customers)


def generate_vendors(n: int = 100) -> pd.DataFrame:
    """Generate vendor data."""
    vendors = []
    base_date = datetime(2022, 6, 1)
    
    for i in range(1, n + 1):
        category = random.choice(CATEGORIES)
        created_at = base_date + timedelta(days=random.randint(0, 400))
        
        # Rating and hourly rate vary by category
        base_rate = {
            "Photography": 150, "Videography": 175, "Catering": 50,
            "Florist": 75, "DJ/Music": 100, "Wedding Planner": 125,
            "Cake/Bakery": 60, "Hair & Makeup": 85, "Officiant": 200,
            "Transportation": 80,
        }.get(category, 100)
        
        vendors.append({
            "vendor_id": i,
            "business_name": fake.company(),
            "contact_name": fake.name(),
            "email": fake.company_email(),
            "phone": fake.phone_number(),
            "category": category,
            "city": fake.city(),
            "state": random.choice(US_STATES),
            "rating": round(random.uniform(3.5, 5.0), 2),
            "hourly_rate": round(base_rate * random.uniform(0.7, 1.5), 2),
            "years_experience": random.randint(1, 20),
            "created_at": created_at.strftime("%Y-%m-%d %H:%M:%S"),
        })
    
    return pd.DataFrame(vendors)


def generate_requests(n: int = 500, customer_ids: list = None) -> pd.DataFrame:
    """Generate service request data."""
    if customer_ids is None:
        customer_ids = list(range(1, 301))
    
    requests = []
    base_date = datetime(2023, 3, 1)
    
    for i in range(1, n + 1):
        created_at = base_date + timedelta(days=random.randint(0, 300))
        event_date = created_at + timedelta(days=random.randint(30, 365))
        
        # Weight towards completed/in_progress for realistic data
        status = random.choices(
            REQUEST_STATUSES,
            weights=[0.15, 0.25, 0.50, 0.10]
        )[0]
        
        category = random.choice(CATEGORIES)
        base_budget = {
            "Photography": 3000, "Videography": 4000, "Catering": 8000,
            "Florist": 2000, "DJ/Music": 1500, "Wedding Planner": 5000,
            "Cake/Bakery": 800, "Hair & Makeup": 500, "Officiant": 400,
            "Transportation": 1000,
        }.get(category, 2000)
        
        requests.append({
            "request_id": i,
            "customer_id": random.choice(customer_ids),
            "category": category,
            "event_date": event_date.strftime("%Y-%m-%d"),
            "event_city": fake.city(),
            "event_state": random.choice(US_STATES),
            "budget": round(base_budget * random.uniform(0.5, 2.0), 2),
            "guest_count": random.randint(20, 300),
            "status": status,
            "description": fake.sentence(nb_words=10),
            "created_at": created_at.strftime("%Y-%m-%d %H:%M:%S"),
        })
    
    return pd.DataFrame(requests)


def generate_bids(n: int = 400, request_ids: list = None, vendor_ids: list = None) -> pd.DataFrame:
    """Generate bid data."""
    if request_ids is None:
        request_ids = list(range(1, 501))
    if vendor_ids is None:
        vendor_ids = list(range(1, 101))
    
    bids = []
    base_date = datetime(2023, 3, 15)
    used_combinations = set()
    
    i = 1
    attempts = 0
    max_attempts = n * 3
    
    while i <= n and attempts < max_attempts:
        attempts += 1
        request_id = random.choice(request_ids)
        vendor_id = random.choice(vendor_ids)
        
        # Avoid duplicate bids from same vendor on same request
        combo = (request_id, vendor_id)
        if combo in used_combinations:
            continue
        used_combinations.add(combo)
        
        created_at = base_date + timedelta(days=random.randint(0, 280))
        
        # Weight towards accepted/pending for realistic data
        status = random.choices(
            BID_STATUSES,
            weights=[0.30, 0.40, 0.20, 0.10]
        )[0]
        
        bid_amount = round(random.uniform(200, 10000), 2)
        
        bids.append({
            "bid_id": i,
            "request_id": request_id,
            "vendor_id": vendor_id,
            "bid_amount": bid_amount,
            "estimated_hours": random.randint(2, 40),
            "message": fake.sentence(nb_words=8),
            "status": status,
            "created_at": created_at.strftime("%Y-%m-%d %H:%M:%S"),
        })
        i += 1
    
    return pd.DataFrame(bids)


def generate_payments(bids_df: pd.DataFrame) -> pd.DataFrame:
    """Generate payment data for accepted bids."""
    accepted_bids = bids_df[bids_df["status"] == "accepted"].copy()
    
    payments = []
    base_date = datetime(2023, 4, 1)
    
    for i, (_, bid) in enumerate(accepted_bids.iterrows(), 1):
        payment_date = base_date + timedelta(days=random.randint(0, 300))
        
        # Most payments match bid amount, some have adjustments
        if random.random() < 0.1:
            amount = round(bid["bid_amount"] * random.uniform(0.95, 1.15), 2)
        else:
            amount = bid["bid_amount"]
        
        payments.append({
            "payment_id": i,
            "bid_id": bid["bid_id"],
            "amount": amount,
            "payment_method": random.choice(PAYMENT_METHODS),
            "payment_date": payment_date.strftime("%Y-%m-%d"),
            "processed_at": (payment_date + timedelta(hours=random.randint(1, 48))).strftime("%Y-%m-%d %H:%M:%S"),
        })
    
    return pd.DataFrame(payments)


@asset(
    group_name="extract",
    compute_kind="python",
    description="Generate raw CSV files for wedding marketplace data",
)
def raw_csv_files(context: AssetExecutionContext) -> Output[dict]:
    """
    Generate deterministic CSV files for the wedding marketplace dataset.
    
    Creates 5 CSV files:
    - customers.csv (~300 rows)
    - vendors.csv (~100 rows)
    - requests.csv (~500 rows)
    - bids.csv (~400 rows)
    - payments.csv (~200 rows, based on accepted bids)
    """
    # Ensure data directory exists
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    
    context.log.info("Generating customer data...")
    customers_df = generate_customers(300)
    customers_path = DATA_DIR / "customers.csv"
    customers_df.to_csv(customers_path, index=False)
    context.log.info(f"Generated {len(customers_df)} customers")
    
    context.log.info("Generating vendor data...")
    vendors_df = generate_vendors(100)
    vendors_path = DATA_DIR / "vendors.csv"
    vendors_df.to_csv(vendors_path, index=False)
    context.log.info(f"Generated {len(vendors_df)} vendors")
    
    context.log.info("Generating request data...")
    requests_df = generate_requests(500, customer_ids=customers_df["customer_id"].tolist())
    requests_path = DATA_DIR / "requests.csv"
    requests_df.to_csv(requests_path, index=False)
    context.log.info(f"Generated {len(requests_df)} requests")
    
    context.log.info("Generating bid data...")
    bids_df = generate_bids(
        400,
        request_ids=requests_df["request_id"].tolist(),
        vendor_ids=vendors_df["vendor_id"].tolist()
    )
    bids_path = DATA_DIR / "bids.csv"
    bids_df.to_csv(bids_path, index=False)
    context.log.info(f"Generated {len(bids_df)} bids")
    
    context.log.info("Generating payment data...")
    payments_df = generate_payments(bids_df)
    payments_path = DATA_DIR / "payments.csv"
    payments_df.to_csv(payments_path, index=False)
    context.log.info(f"Generated {len(payments_df)} payments")
    
    file_info = {
        "customers": {"path": str(customers_path), "rows": len(customers_df)},
        "vendors": {"path": str(vendors_path), "rows": len(vendors_df)},
        "requests": {"path": str(requests_path), "rows": len(requests_df)},
        "bids": {"path": str(bids_path), "rows": len(bids_df)},
        "payments": {"path": str(payments_path), "rows": len(payments_df)},
    }
    
    total_rows = sum(info["rows"] for info in file_info.values())
    
    return Output(
        value=file_info,
        metadata={
            "total_rows": MetadataValue.int(total_rows),
            "customers_count": MetadataValue.int(len(customers_df)),
            "vendors_count": MetadataValue.int(len(vendors_df)),
            "requests_count": MetadataValue.int(len(requests_df)),
            "bids_count": MetadataValue.int(len(bids_df)),
            "payments_count": MetadataValue.int(len(payments_df)),
            "data_directory": MetadataValue.path(str(DATA_DIR)),
        }
    )

