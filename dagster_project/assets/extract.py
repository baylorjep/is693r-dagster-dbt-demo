"""
Extract asset: Generate or fetch data for Bidi Contracting blueprint takeoffs.

This module supports two modes:
- DEMO mode: Generate deterministic fake data for demonstrations
- LIVE mode: Connect to Bidi's AI extraction API (placeholder for production)

Set the environment variable BIDI_DATA_MODE to 'live' to use live data.
Default is 'demo' mode.
"""

import os
from pathlib import Path
from datetime import datetime, timedelta
import random

import pandas as pd
from faker import Faker
from dagster import asset, AssetExecutionContext, Output, MetadataValue

# Data mode: 'demo' or 'live'
DATA_MODE = os.environ.get("BIDI_DATA_MODE", "demo").lower()

# Set deterministic seed for reproducibility (demo mode only)
SEED = 42
random.seed(SEED)
Faker.seed(SEED)
fake = Faker()

# Project paths
PROJECT_ROOT = Path(__file__).parent.parent.parent
DATA_DIR = PROJECT_ROOT / "data" / "raw"

# Blueprint disciplines (Architecture, Structural, Civil, Mechanical, Electrical, Plumbing)
DISCIPLINES = ["A", "S", "C", "M", "E", "P"]
DISCIPLINE_NAMES = {
    "A": "Architectural",
    "S": "Structural", 
    "C": "Civil",
    "M": "Mechanical",
    "E": "Electrical",
    "P": "Plumbing",
}

# Project types
PROJECT_TYPES = ["residential", "commercial", "industrial", "mixed_use"]
PROJECT_STATUSES = ["pending", "in_progress", "completed", "on_hold"]

# Extraction methods
EXTRACTION_METHODS = ["ai", "manual", "hybrid"]
ESTIMATION_METHODS = ["ai", "manual", "hybrid"]

# QA issue types and severities
QA_ISSUE_TYPES = ["missing_item", "quantity_mismatch", "wrong_uom", "duplicate", "low_confidence", "cost_outlier"]
QA_SEVERITIES = ["low", "medium", "high", "critical"]

# US States for realistic locations
US_STATES = [
    "CA", "TX", "FL", "NY", "PA", "IL", "OH", "GA", "NC", "MI",
    "NJ", "VA", "WA", "AZ", "MA", "TN", "IN", "MO", "MD", "WI",
    "CO", "MN", "SC", "AL", "LA", "KY", "OR", "OK", "CT", "UT",
]

# CSI Division cost codes and item types (simplified for demo)
COST_CODES = {
    "03": {"name": "Concrete", "items": ["footing", "slab", "wall", "column", "beam"]},
    "04": {"name": "Masonry", "items": ["brick", "block", "stone", "mortar"]},
    "05": {"name": "Metals", "items": ["structural_steel", "rebar", "misc_metal", "handrail"]},
    "06": {"name": "Wood/Plastics", "items": ["framing", "sheathing", "trim", "millwork"]},
    "07": {"name": "Thermal/Moisture", "items": ["insulation", "roofing", "siding", "waterproofing"]},
    "08": {"name": "Openings", "items": ["door", "window", "storefront", "hardware"]},
    "09": {"name": "Finishes", "items": ["drywall", "paint", "tile", "flooring", "ceiling"]},
    "22": {"name": "Plumbing", "items": ["pipe", "fixture", "valve", "water_heater"]},
    "23": {"name": "HVAC", "items": ["ductwork", "diffuser", "unit", "controls"]},
    "26": {"name": "Electrical", "items": ["conduit", "wire", "panel", "outlet", "fixture"]},
}

# Units of measure
UOMS = {
    "footing": "CY", "slab": "SF", "wall": "SF", "column": "EA", "beam": "LF",
    "brick": "SF", "block": "SF", "stone": "SF", "mortar": "CF",
    "structural_steel": "TON", "rebar": "TON", "misc_metal": "LB", "handrail": "LF",
    "framing": "BF", "sheathing": "SF", "trim": "LF", "millwork": "LF",
    "insulation": "SF", "roofing": "SQ", "siding": "SF", "waterproofing": "SF",
    "door": "EA", "window": "EA", "storefront": "SF", "hardware": "EA",
    "drywall": "SF", "paint": "SF", "tile": "SF", "flooring": "SF", "ceiling": "SF",
    "pipe": "LF", "fixture": "EA", "valve": "EA", "water_heater": "EA",
    "ductwork": "LB", "diffuser": "EA", "unit": "EA", "controls": "EA",
    "conduit": "LF", "wire": "LF", "panel": "EA", "outlet": "EA",
}


# =============================================================================
# LIVE MODE - Bidi AI Integration
# =============================================================================

class BidiAPIClient:
    """
    Bidi AI extraction.
    
    In production, this connects to:
    - Bidi's blueprint processing service
    - AI takeoff extraction endpoints
    - Cost estimation API (Togal)
    
    
    """
    
    def __init__(self, api_key: str = None, base_url: str = None):
        self.api_key = api_key or os.environ.get("BIDI_API_KEY")
        self.base_url = base_url or os.environ.get("BIDI_API_URL", "https://api.bidicontracting.com")
        
    def fetch_projects(self) -> pd.DataFrame:
        """Fetch active projects from Bidi API."""
        # TODO: Implement when API is ready
        # response = requests.get(f"{self.base_url}/projects", headers={"Authorization": f"Bearer {self.api_key}"})
        # return pd.DataFrame(response.json())
        raise NotImplementedError("Live API integration pending - AI extraction in development")
    
    def fetch_blueprint_pages(self, project_id: int) -> pd.DataFrame:
        """Fetch processed blueprint pages for a project."""
        # TODO: Implement when API is ready
        raise NotImplementedError("Live API integration pending - AI extraction in development")
    
    def fetch_takeoff_items(self, project_id: int) -> pd.DataFrame:
        """Fetch AI-extracted takeoff items for a project."""
        # TODO: Implement when API is ready
        raise NotImplementedError("Live API integration pending - AI extraction in development")
    
    def fetch_cost_library(self) -> pd.DataFrame:
        """Fetch current cost library from Bidi."""
        # TODO: Implement when API is ready
        raise NotImplementedError("Live API integration pending - AI extraction in development")


def fetch_live_data(context: AssetExecutionContext) -> dict:
    """
    Fetch live data from Bidi's production systems.
    
    This function will be enabled once Bidi's AI extraction pipeline is complete.
    For now, it raises an informative error directing users to demo mode.
    """
    context.log.info("Attempting to fetch live data from Bidi API...")
    
    client = BidiAPIClient()
    
    try:
        # These will raise NotImplementedError until API is ready
        projects = client.fetch_projects()
        # ... additional fetches
        
        return {
            "projects": projects,
            # ... additional data
        }
    except NotImplementedError as e:
        context.log.warning(f"Live mode not yet available: {e}")
        context.log.warning("Bidi's AI extraction is still in development.")
        context.log.warning("Please use BIDI_DATA_MODE=demo for demonstrations.")
        raise


# =============================================================================
# DEMO MODE - Synthetic Data Generation
# =============================================================================

def generate_projects(n: int = 50) -> pd.DataFrame:
    """Generate construction project data."""
    projects = []
    base_date = datetime(2024, 1, 1)
    
    for i in range(1, n + 1):
        bid_date = base_date + timedelta(days=random.randint(0, 300))
        
        # Weight towards in_progress/completed for realistic data
        status = random.choices(
            PROJECT_STATUSES,
            weights=[0.15, 0.35, 0.40, 0.10]
        )[0]
        
        projects.append({
            "project_id": i,
            "client_name": fake.company(),
            "project_name": f"{fake.street_name()} {random.choice(['Tower', 'Building', 'Center', 'Complex', 'Plaza'])}",
            "project_type": random.choice(PROJECT_TYPES),
            "location_state": random.choice(US_STATES),
            "bid_date": bid_date.strftime("%Y-%m-%d"),
            "plan_set_version": f"v{random.randint(1, 5)}.{random.randint(0, 9)}",
            "status": status,
            "created_at": (bid_date - timedelta(days=random.randint(7, 30))).strftime("%Y-%m-%d %H:%M:%S"),
        })
    
    return pd.DataFrame(projects)


def generate_blueprint_pages(n: int = 400, project_ids: list = None) -> pd.DataFrame:
    """Generate blueprint page data for projects."""
    if project_ids is None:
        project_ids = list(range(1, 51))
    
    pages = []
    page_id = 1
    
    # Distribute pages across projects (average ~8 pages per project)
    for project_id in project_ids:
        # Each project has 3-15 pages
        num_pages = random.randint(3, 15)
        
        for sheet_num in range(1, num_pages + 1):
            discipline = random.choice(DISCIPLINES)
            
            pages.append({
                "page_id": page_id,
                "project_id": project_id,
                "sheet_number": f"{discipline}{sheet_num:02d}",
                "discipline": discipline,
                "page_title": f"{DISCIPLINE_NAMES[discipline]} - Sheet {sheet_num}",
                "image_path": f"/blueprints/project_{project_id}/{discipline}{sheet_num:02d}.pdf",
                "page_version": random.randint(1, 3),
                "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            })
            page_id += 1
            
            if page_id > n:
                break
        if page_id > n:
            break
    
    return pd.DataFrame(pages)


def generate_cost_library(n: int = 80) -> pd.DataFrame:
    """Generate cost library reference data."""
    costs = []
    cost_id = 1
    
    for division, data in COST_CODES.items():
        for item_type in data["items"]:
            # Generate realistic cost ranges
            base_cost = random.uniform(5, 500)
            
            costs.append({
                "cost_code": f"{division}-{item_type[:4].upper()}",
                "division": division,
                "division_name": data["name"],
                "item_type": item_type,
                "uom": UOMS.get(item_type, "EA"),
                "unit_cost_low": round(base_cost * 0.75, 2),
                "unit_cost_mid": round(base_cost, 2),
                "unit_cost_high": round(base_cost * 1.35, 2),
                "last_updated": (datetime.now() - timedelta(days=random.randint(0, 90))).strftime("%Y-%m-%d"),
            })
            cost_id += 1
            
            if cost_id > n:
                break
        if cost_id > n:
            break
    
    return pd.DataFrame(costs)


def generate_takeoff_items(n: int = 4000, pages_df: pd.DataFrame = None, cost_library_df: pd.DataFrame = None) -> pd.DataFrame:
    """Generate takeoff item data with confidence scores."""
    if pages_df is None or cost_library_df is None:
        raise ValueError("pages_df and cost_library_df are required")
    
    takeoffs = []
    
    # Get valid cost codes
    cost_codes = cost_library_df["cost_code"].tolist()
    item_types = cost_library_df["item_type"].tolist()
    
    for i in range(1, n + 1):
        page = pages_df.sample(1).iloc[0]
        cost_idx = random.randint(0, len(cost_codes) - 1)
        
        # AI extraction has higher confidence on average
        extraction_method = random.choices(
            EXTRACTION_METHODS,
            weights=[0.70, 0.15, 0.15]  # Mostly AI
        )[0]
        
        # Confidence varies by extraction method
        if extraction_method == "ai":
            confidence = round(random.uniform(0.65, 0.99), 3)
        elif extraction_method == "manual":
            confidence = round(random.uniform(0.90, 1.0), 3)
        else:  # hybrid
            confidence = round(random.uniform(0.80, 0.98), 3)
        
        takeoffs.append({
            "takeoff_id": i,
            "project_id": page["project_id"],
            "page_id": page["page_id"],
            "cost_code": cost_codes[cost_idx],
            "item_type": item_types[cost_idx],
            "quantity": round(random.uniform(1, 500), 2),
            "uom": UOMS.get(item_types[cost_idx], "EA"),
            "confidence": confidence,
            "extraction_method": extraction_method,
            "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        })
    
    return pd.DataFrame(takeoffs)


def generate_estimates(project_ids: list = None, takeoffs_df: pd.DataFrame = None, cost_library_df: pd.DataFrame = None) -> pd.DataFrame:
    """Generate estimate data rolled up from takeoffs."""
    if project_ids is None:
        project_ids = list(range(1, 51))
    if takeoffs_df is None or cost_library_df is None:
        raise ValueError("takeoffs_df and cost_library_df are required")
    
    estimates = []
    estimate_id = 1
    
    # Merge takeoffs with cost library for pricing
    cost_lookup = cost_library_df.set_index("cost_code")[["unit_cost_low", "unit_cost_mid", "unit_cost_high"]].to_dict("index")
    
    for project_id in project_ids:
        # Each project may have 1-3 estimate versions
        num_versions = random.randint(1, 3)
        
        project_takeoffs = takeoffs_df[takeoffs_df["project_id"] == project_id]
        
        for version in range(1, num_versions + 1):
            # Calculate totals from takeoffs
            total_low = 0
            total_mid = 0
            total_high = 0
            
            for _, takeoff in project_takeoffs.iterrows():
                cost_code = takeoff["cost_code"]
                quantity = takeoff["quantity"]
                
                if cost_code in cost_lookup:
                    total_low += quantity * cost_lookup[cost_code]["unit_cost_low"]
                    total_mid += quantity * cost_lookup[cost_code]["unit_cost_mid"]
                    total_high += quantity * cost_lookup[cost_code]["unit_cost_high"]
            
            # Add some variance between versions
            variance = 1 + (version - 1) * random.uniform(-0.05, 0.10)
            
            estimates.append({
                "estimate_id": estimate_id,
                "project_id": project_id,
                "estimate_version": version,
                "estimate_total_low": round(total_low * variance * 0.95, 2),
                "estimate_total_mid": round(total_mid * variance, 2),
                "estimate_total_high": round(total_high * variance * 1.05, 2),
                "generated_at": (datetime.now() - timedelta(days=random.randint(0, 30))).strftime("%Y-%m-%d %H:%M:%S"),
                "estimation_method": random.choice(ESTIMATION_METHODS),
            })
            estimate_id += 1
    
    return pd.DataFrame(estimates)


def generate_estimate_line_items(estimates_df: pd.DataFrame, takeoffs_df: pd.DataFrame, cost_library_df: pd.DataFrame) -> pd.DataFrame:
    """Generate estimate line items - breakdown by cost code per estimate."""
    line_items = []
    line_id = 1
    
    cost_lookup = cost_library_df.set_index("cost_code")[["item_type", "uom", "unit_cost_mid"]].to_dict("index")
    
    for _, estimate in estimates_df.iterrows():
        project_id = estimate["project_id"]
        estimate_id = estimate["estimate_id"]
        
        # Aggregate takeoffs by cost_code for this project
        project_takeoffs = takeoffs_df[takeoffs_df["project_id"] == project_id]
        
        # Group by cost_code and sum quantities
        if len(project_takeoffs) > 0:
            grouped = project_takeoffs.groupby("cost_code").agg({
                "quantity": "sum",
                "item_type": "first",
                "uom": "first"
            }).reset_index()
            
            for _, row in grouped.iterrows():
                cost_code = row["cost_code"]
                quantity = row["quantity"]
                
                if cost_code in cost_lookup:
                    unit_cost = cost_lookup[cost_code]["unit_cost_mid"]
                    
                    line_items.append({
                        "line_item_id": line_id,
                        "estimate_id": estimate_id,
                        "cost_code": cost_code,
                        "item_type": row["item_type"],
                        "quantity": round(quantity, 2),
                        "uom": row["uom"],
                        "unit_cost_mid": unit_cost,
                        "extended_cost_mid": round(quantity * unit_cost, 2),
                    })
                    line_id += 1
    
    return pd.DataFrame(line_items)


def generate_qa_reviews(n: int = 100, projects_df: pd.DataFrame = None, pages_df: pd.DataFrame = None, takeoffs_df: pd.DataFrame = None) -> pd.DataFrame:
    """Generate QA review data for quality monitoring."""
    if projects_df is None or pages_df is None or takeoffs_df is None:
        raise ValueError("projects_df, pages_df, and takeoffs_df are required")
    
    reviews = []
    
    for i in range(1, n + 1):
        project = projects_df.sample(1).iloc[0]
        project_id = project["project_id"]
        
        # Optionally link to a specific page
        project_pages = pages_df[pages_df["project_id"] == project_id]
        page_id = None
        if len(project_pages) > 0 and random.random() > 0.3:
            page_id = int(project_pages.sample(1).iloc[0]["page_id"])
        
        # Optionally link to a specific takeoff
        takeoff_id = None
        if page_id and random.random() > 0.5:
            page_takeoffs = takeoffs_df[takeoffs_df["page_id"] == page_id]
            if len(page_takeoffs) > 0:
                takeoff_id = int(page_takeoffs.sample(1).iloc[0]["takeoff_id"])
        
        # Most issues are resolved
        resolved = random.choices([True, False], weights=[0.75, 0.25])[0]
        
        reviews.append({
            "review_id": i,
            "project_id": project_id,
            "page_id": page_id,
            "takeoff_id": takeoff_id,
            "issue_type": random.choice(QA_ISSUE_TYPES),
            "severity": random.choices(
                QA_SEVERITIES,
                weights=[0.40, 0.35, 0.20, 0.05]
            )[0],
            "resolved": resolved,
            "reviewed_at": (datetime.now() - timedelta(days=random.randint(0, 60))).strftime("%Y-%m-%d %H:%M:%S"),
        })
    
    return pd.DataFrame(reviews)


def generate_demo_data(context: AssetExecutionContext) -> dict:
    """Generate all demo data with deterministic seeding."""
    context.log.info("Generating demo data with deterministic seed...")
    
    # Reset seed for reproducibility
    random.seed(SEED)
    Faker.seed(SEED)
    
    context.log.info("Generating project data...")
    projects_df = generate_projects(50)
    
    context.log.info("Generating blueprint page data...")
    pages_df = generate_blueprint_pages(400, project_ids=projects_df["project_id"].tolist())
    
    context.log.info("Generating cost library data...")
    cost_library_df = generate_cost_library(80)
    
    context.log.info("Generating takeoff item data...")
    takeoffs_df = generate_takeoff_items(4000, pages_df=pages_df, cost_library_df=cost_library_df)
    
    context.log.info("Generating estimate data...")
    estimates_df = generate_estimates(
        project_ids=projects_df["project_id"].tolist(),
        takeoffs_df=takeoffs_df,
        cost_library_df=cost_library_df
    )
    
    context.log.info("Generating estimate line item data...")
    line_items_df = generate_estimate_line_items(estimates_df, takeoffs_df, cost_library_df)
    
    context.log.info("Generating QA review data...")
    qa_reviews_df = generate_qa_reviews(100, projects_df=projects_df, pages_df=pages_df, takeoffs_df=takeoffs_df)
    
    return {
        "projects": projects_df,
        "blueprint_pages": pages_df,
        "cost_library": cost_library_df,
        "takeoff_items": takeoffs_df,
        "estimates": estimates_df,
        "estimate_line_items": line_items_df,
        "qa_reviews": qa_reviews_df,
    }


# =============================================================================
# DAGSTER ASSET
# =============================================================================

@asset(
    group_name="extract",
    compute_kind="python",
    description="Generate or fetch data for Bidi Contracting blueprint takeoffs (demo or live mode)",
)
def raw_csv_files(context: AssetExecutionContext) -> Output[dict]:
    """
    Generate or fetch CSV files for the Bidi Contracting dataset.
    
    Modes:
    - DEMO (default): Generate deterministic fake data for demonstrations
    - LIVE: Fetch real data from Bidi's AI extraction API (pending API completion)
    
    Set BIDI_DATA_MODE environment variable to switch modes.
    
    Creates 7 CSV files:
    - projects.csv (~50 rows) - Construction projects
    - blueprint_pages.csv (~400 rows) - Blueprint pages per project
    - cost_library.csv (~80 rows) - Reference cost data
    - takeoff_items.csv (~4000 rows) - Extracted quantities with confidence
    - estimates.csv (~80 rows) - Rolled-up estimates per project
    - estimate_line_items.csv (~600 rows) - Estimate breakdown by cost code
    - qa_reviews.csv (~100 rows) - QA issues and reviews
    """
    # Ensure data directory exists
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    
    context.log.info(f"Data mode: {DATA_MODE.upper()}")
    
    # Fetch or generate data based on mode
    if DATA_MODE == "live":
        context.log.info("🔴 LIVE MODE: Fetching data from Bidi API...")
        data = fetch_live_data(context)
    else:
        context.log.info("🟢 DEMO MODE: Generating synthetic data...")
        data = generate_demo_data(context)
    
    # Write all DataFrames to CSV
    file_info = {}
    
    for table_name, df in data.items():
        csv_path = DATA_DIR / f"{table_name}.csv"
        df.to_csv(csv_path, index=False)
        file_info[table_name] = {"path": str(csv_path), "rows": len(df)}
        context.log.info(f"Wrote {len(df)} rows to {csv_path}")
    
    total_rows = sum(info["rows"] for info in file_info.values())
    
    return Output(
        value=file_info,
        metadata={
            "data_mode": MetadataValue.text(DATA_MODE.upper()),
            "total_rows": MetadataValue.int(total_rows),
            "projects_count": MetadataValue.int(file_info["projects"]["rows"]),
            "blueprint_pages_count": MetadataValue.int(file_info["blueprint_pages"]["rows"]),
            "cost_library_count": MetadataValue.int(file_info["cost_library"]["rows"]),
            "takeoff_items_count": MetadataValue.int(file_info["takeoff_items"]["rows"]),
            "estimates_count": MetadataValue.int(file_info["estimates"]["rows"]),
            "estimate_line_items_count": MetadataValue.int(file_info["estimate_line_items"]["rows"]),
            "qa_reviews_count": MetadataValue.int(file_info["qa_reviews"]["rows"]),
            "data_directory": MetadataValue.path(str(DATA_DIR)),
        }
    )
