#!/usr/bin/env python3
import json
import csv
import os
import random
import datetime
import string
from typing import List, Dict, Any, Union, Tuple
import uuid

# Create output directories
os.makedirs("data_out/orders", exist_ok=True)
os.makedirs("data_out/claims", exist_ok=True)
os.makedirs("data_out/supplier", exist_ok=True)

# Constants
NUM_ORDERS = 10000   # 10K orders
NUM_CLAIMS = 300     # 300 warranty claims
NUM_SUPPLIERS = 20
DUPLICATE_RATE = 0.02  # 2% duplicates

# Helper functions
def random_date(start_date: datetime.date, end_date: datetime.date) -> datetime.date:
    """Generate a random date between start_date and end_date"""
    delta = end_date - start_date
    random_days = random.randrange(delta.days)
    return start_date + datetime.timedelta(days=random_days)

def format_date_with_error(date: datetime.date) -> str:
    """Format date with possible format errors"""
    formats = [
        lambda d: d.strftime("%Y-%m-%d"),
        lambda d: d.strftime("%d/%m/%Y"),
        lambda d: d.strftime("%Y-%m-%dT%H:%M:%SZ"),
        lambda d: d.strftime("%d-%m-%Y"),
        lambda d: d.strftime("%m/%d/%Y")
    ]
    return random.choice(formats)(date)

def introduce_typo(text: str, probability: float = 0.1) -> str:
    """Introduce typos with given probability"""
    if random.random() > probability:
        return text
    
    if not text:
        return text
        
    chars = list(text)
    action = random.choice(["swap", "remove", "duplicate", "replace"])
    
    if len(chars) < 2:
        return text
        
    pos = random.randint(0, len(chars) - 1)
    
    if action == "swap" and pos < len(chars) - 1:
        chars[pos], chars[pos + 1] = chars[pos + 1], chars[pos]
    elif action == "remove":
        chars.pop(pos)
    elif action == "duplicate" and pos < len(chars) - 1:
        chars.insert(pos, chars[pos])
    elif action == "replace":
        chars[pos] = random.choice(string.ascii_lowercase)
        
    return "".join(chars)

def maybe_null(value: Any, probability: float = 0.05) -> Any:
    """Return None or empty string with given probability"""
    if random.random() < probability:
        return random.choice([None, "", "  "])
    return value

def maybe_case_change(text: str, probability: float = 0.3) -> str:
    """Change case with given probability"""
    if not text or random.random() > probability:
        return text
        
    funcs = [
        lambda t: t.upper(),
        lambda t: t.lower(),
        lambda t: t.capitalize(),
        lambda t: t
    ]
    return random.choice(funcs)(text)

# Data generation functions
def generate_product_ids(count: int) -> List[str]:
    """Generate product IDs"""
    return [f"W{str(i).zfill(3)}" for i in range(1, count + 1)]

def generate_customer_ids(count: int) -> List[str]:
    """Generate customer IDs"""
    return [f"C{random.randint(1000, 9999)}" for _ in range(count)]

def generate_region_woods() -> List[str]:
    """Generate region_wood values"""
    regions = ["Bretagne", "Normandie", "Alsace", "Provence", "Aquitaine", 
               "Bourgogne", "Corse", "Lorraine", "Picardie", "Auvergne"]
    
    # Create both correct and incorrect versions (with typos, missing accents)
    result = []
    for region in regions:
        result.append(region)
        if "é" in region or "è" in region:
            result.append(region.replace("é", "e").replace("è", "e"))
        result.append(introduce_typo(region))
    
    return result

def generate_wood_species() -> List[str]:
    """Generate wood species"""
    species = ["oak", "maple", "walnut", "cherry", "pine", "mahogany", "teak", "ebony", "ash", "birch"]
    
    # Create both correct and case variants
    result = []
    for s in species:
        result.append(s)
        result.append(s.upper())
        result.append(s.capitalize())
    
    return result

def generate_return_reasons() -> List[str]:
    """Generate return reasons with typos"""
    reasons = ["battery", "movement", "strap", "glass", "finish", "defect"]
    
    # Create both correct and typo versions
    result = []
    for reason in reasons:
        result.append(reason)
        result.append(introduce_typo(reason))
        result.append(introduce_typo(reason))
    
    return result

def generate_severity_levels() -> List[str]:
    """Generate severity levels with formatting issues"""
    base = ["MINOR", "MAJOR", "CRITICAL"]
    
    result = []
    for level in base:
        result.append(level)
        result.append(f" {level} ")
        result.append(level.lower())
        result.append(level.capitalize())
    
    return result

def generate_orders(count: int, product_ids: List[str], customer_ids: List[str]) -> List[Dict]:
    """Generate orders with dirty data"""
    orders = []
    start_date = datetime.date(2024, 1, 1)
    end_date = datetime.date(2025, 12, 31)
    
    for i in range(count):
        order_date = random_date(start_date, end_date)
        
        # Sometimes missing prefix
        order_id = f"O{str(i + 100001).zfill(6)}" if random.random() > 0.05 else f"{str(i + 100001).zfill(6)}"
        
        # Sometimes as text
        quantity = random.randint(1, 5)
        if random.random() < 0.1:
            quantity = random.choice(["one", "two", "three", "four", "five"])
        
        order = {
            "order_id": order_id,
            "customer_id": maybe_null(random.choice(customer_ids), 0.05),
            "product_id": maybe_case_change(random.choice(product_ids), 0.3),
            "quantity": quantity,
            "order_date": format_date_with_error(order_date),
            "order_notes": maybe_null("Standard delivery", 0.2)
        }
        
        orders.append(order)
    
    # Add duplicates
    duplicate_count = int(count * DUPLICATE_RATE)
    for _ in range(duplicate_count):
        if orders:
            duplicate = random.choice(orders).copy()
            orders.append(duplicate)
    
    return orders

def generate_warranty_claims(count: int, orders: List[Dict]) -> List[Dict]:
    """Generate warranty claims with dirty data"""
    claims = []
    
    for i in range(count):
        # Select a random order to link to
        if orders:
            order = random.choice(orders)
            
            # Parse the order date regardless of format
            try:
                if "/" in order["order_date"]:
                    parts = order["order_date"].split("/")
                    if len(parts[0]) == 4:  # YYYY/MM/DD
                        order_date = datetime.datetime.strptime(order["order_date"], "%Y/%m/%d").date()
                    else:  # DD/MM/YYYY
                        order_date = datetime.datetime.strptime(order["order_date"], "%d/%m/%Y").date()
                elif "-" in order["order_date"]:
                    if "T" in order["order_date"]:  # ISO format
                        order_date = datetime.datetime.fromisoformat(order["order_date"].replace("Z", "+00:00")).date()
                    else:
                        parts = order["order_date"].split("-")
                        if len(parts[0]) == 4:  # YYYY-MM-DD
                            order_date = datetime.datetime.strptime(order["order_date"], "%Y-%m-%d").date()
                        else:  # DD-MM-YYYY
                            order_date = datetime.datetime.strptime(order["order_date"], "%d-%m-%Y").date()
                else:
                    # Default to current date if parsing fails
                    order_date = datetime.date.today()
            except:
                order_date = datetime.date.today()
                
            # Generate return date (sometimes before order date)
            if random.random() < 0.05:
                # Return date before order date (error)
                days_before = random.randint(1, 30)
                return_date = order_date - datetime.timedelta(days=days_before)
            else:
                # Normal return date after order date
                days_after = random.randint(1, 365)
                return_date = order_date + datetime.timedelta(days=days_after)
                
            claim = {
                "claim_id": f"R{str(i + 200001).zfill(6)}",
                "order_id": order["order_id"],
                "product_id": maybe_case_change(order["product_id"], 0.5),
                "order_date": format_date_with_error(order_date),
                "return_date": format_date_with_error(return_date),
                "return_reason": random.choice(generate_return_reasons()),
                "severity": random.choice(generate_severity_levels()),
                "under_warranty": random.choice(["Y", "N", "true", "false", "1", "0"])
            }
            
            claims.append(claim)
    
    # Add non-unique claim IDs (rare cases)
    if claims:
        for _ in range(int(count * 0.01)):  # 1% duplicated claim IDs
            if claims:
                duplicate_idx = random.randint(0, len(claims) - 1)
                original_claim_id = claims[duplicate_idx]["claim_id"]
                
                another_idx = random.randint(0, len(claims) - 1)
                if another_idx != duplicate_idx:
                    claims[another_idx]["claim_id"] = original_claim_id
    
    return claims

def generate_supplier_data(count: int, region_woods: List[str], wood_species: List[str]) -> List[Dict]:
    """Generate supplier wood specs with dirty data"""
    suppliers = []
    
    for _ in range(count):
        region_wood = random.choice(region_woods)
        
        # Sometimes unknown density
        density = random.randint(300, 1200)
        if random.random() < 0.1:
            density = "unknown"
            
        # Sometimes null hardness
        hardness = random.randint(1000, 5000)
        if random.random() < 0.05:
            hardness = None
            
        # Sometimes negative carbon storage
        carbon_storage = round(random.uniform(0.5, 3.0), 2)
        if random.random() < 0.05:
            carbon_storage = -carbon_storage
            
        # Sometimes >100% recyclability
        recyclability = random.randint(30, 100)
        if random.random() < 0.1:
            recyclability = random.randint(101, 120)
            
        # Certification with typos
        certification = "FSC"
        if random.random() < 0.1:
            certification = introduce_typo(certification)
            
        # Sometimes missing origin
        origin = random.choice(["France", "Germany", "Italy", "Spain", "Sweden"])
        if random.random() < 0.2:
            origin = ""
            
        # Random update date
        update_date = random_date(datetime.date(2024, 1, 1), datetime.date(2025, 12, 31))
        
        supplier = {
            "region_wood": region_wood,
            "wood_species": random.choice(wood_species),
            "density_kg_m3": density,
            "hardness_n": hardness,
            "carbon_storage_kg_co2e_per_kg": carbon_storage,
            "recyclability_rate_pct": recyclability,
            "certification": certification,
            "origin": origin,
            "updated_at": format_date_with_error(update_date)
        }
        
        suppliers.append(supplier)
    
    return suppliers

def write_ndjson(data: List[Dict], filepath: str):
    """Write data as NDJSON (newline delimited JSON)"""
    with open(filepath, 'w') as f:
        for item in data:
            f.write(json.dumps(item) + '\n')

def write_json(data: List[Dict], filepath: str):
    """Write data as standard JSON array"""
    with open(filepath, 'w') as f:
        json.dump(data, f, indent=2)

def write_csv(data: List[Dict], filepath: str):
    """Write data as CSV"""
    if not data:
        return
        
    with open(filepath, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=data[0].keys())
        writer.writeheader()
        writer.writerows(data)

# Main execution
def main():
    print("Generating dirty data for Arboré ETL project...")
    
    # Generate base data
    product_ids = generate_product_ids(50)
    customer_ids = generate_customer_ids(200)
    region_woods = generate_region_woods()
    wood_species = generate_wood_species()
    
    # Generate datasets
    print(f"Generating {NUM_ORDERS} orders...")
    orders = generate_orders(NUM_ORDERS, product_ids, customer_ids)
    
    print(f"Generating {NUM_CLAIMS} warranty claims...")
    claims = generate_warranty_claims(NUM_CLAIMS, orders)
    
    print(f"Generating {NUM_SUPPLIERS} supplier wood specs...")
    suppliers = generate_supplier_data(NUM_SUPPLIERS, region_woods, wood_species)
    
    # Write to files
    # Generate 2 JSON files and 1 CSV file
    write_json(orders, "data_out/orders/orders.json")
    write_json(claims, "data_out/claims/warranty_claims.json")
    write_csv(suppliers, "data_out/supplier/wood_specs.csv")
    
    print("Data generation complete!")
    print(f"- Orders: {len(orders)} records (JSON)")
    print(f"- Claims: {len(claims)} records (JSON)")
    print(f"- Suppliers: {len(suppliers)} records (CSV)")
    print("Files written to data_out/ directory")

if __name__ == "__main__":
    main()
