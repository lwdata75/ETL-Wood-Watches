#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Arboré Product Table Generator
Generates d_watch_product.csv based on product_id.csv and wood_specs.csv
"""

import csv
import os
import sys
from pathlib import Path

# Define paths
SCRIPT_DIR = Path(__file__).parent.absolute()
PROJECT_ROOT = SCRIPT_DIR.parent.parent
PRODUCT_IDS_PATH = SCRIPT_DIR / "product_id.csv"
WOOD_SPECS_PATH = PROJECT_ROOT / "data_out" / "supplier" / "wood_specs.csv"
OUTPUT_DIR = PROJECT_ROOT / "data_out" / "dims"
OUTPUT_PATH = OUTPUT_DIR / "d_watch_product.csv"

# Ensure output directory exists
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Arboré watch categories
CATEGORIES = [
    "Arboré Héritage",  # luxury tier
    "Arboré Voyage",    # sport tier
    "Arboré Élégance",  # premium tier
    "Arboré Essentiel"  # base tier
]

# Price ranges for each category (min, max)
PRICE_RANGES = {
    "Arboré Héritage": (700, 1200),
    "Arboré Voyage": (400, 800),
    "Arboré Élégance": (250, 500),
    "Arboré Essentiel": (120, 250)
}

# Exotic/dense woods that get a price premium
PREMIUM_WOODS = [
    "Teak", "Ebony", "Rosewood", "Jarrah", "Wenge", "Mahogany", 
    "African Mahogany", "Bubinga", "Ipe", "Pau Ferro", "Padauk"
]

def load_product_ids():
    """Load product IDs from product_id.csv"""
    product_ids = []
    try:
        with open(PRODUCT_IDS_PATH, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            next(reader)  # Skip header
            for row in reader:
                if row:  # Skip empty rows
                    product_ids.append(row[0])
        return product_ids
    except Exception as e:
        print(f"Error loading product IDs: {e}")
        sys.exit(1)

def load_wood_specs():
    """Load wood species and regions from wood_specs.csv"""
    wood_data = []
    try:
        with open(WOOD_SPECS_PATH, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                wood_data.append({
                    'wood_species': row['wood_species'],
                    'region_wood': row['region_wood']
                })
        return wood_data
    except Exception as e:
        print(f"Error loading wood specs: {e}")
        sys.exit(1)

def calculate_price(category, wood_species):
    """Calculate price based on category and wood type"""
    min_price, max_price = PRICE_RANGES[category]
    
    # Start with middle of range
    base_price = (min_price + max_price) / 2
    
    # Apply premium for exotic/dense woods
    if wood_species in PREMIUM_WOODS:
        base_price *= 1.15
    
    # Add some variation based on wood name length (just for variety)
    variation = len(wood_species) % 10 - 5
    price = int(base_price + variation * 10)
    
    # Ensure price is within range
    return max(min_price, min(max_price, price))

def generate_product_data(product_ids, wood_data):
    """Generate product data by pairing each wood species with the 4 categories"""
    products = []
    
    # Ensure we have enough product IDs
    if len(product_ids) < len(wood_data) * len(CATEGORIES):
        print(f"Warning: Not enough product IDs. Need {len(wood_data) * len(CATEGORIES)}, but got {len(product_ids)}")
        product_ids.extend([f"P{i:04d}" for i in range(1, len(wood_data) * len(CATEGORIES) - len(product_ids) + 1)])
    
    id_index = 0
    for wood in wood_data:
        for category in CATEGORIES:
            if id_index >= len(product_ids):
                print("Warning: Ran out of product IDs")
                break
                
            product_id = product_ids[id_index]
            wood_species = wood['wood_species']
            region_wood = wood['region_wood']
            
            # Generate product name: Arboré <Category> <Wood_Species>
            product_name = f"{category} {wood_species}"
            
            # Calculate price
            price_eur = calculate_price(category, wood_species)
            
            products.append({
                'product_id': product_id,
                'product_name': product_name,
                'wood_species': wood_species,
                'region_wood': region_wood,
                'category': category,
                'price_eur': price_eur
            })
            
            id_index += 1
    
    return products

def write_output(products):
    """Write products to d_watch_product.csv"""
    fieldnames = ['product_id', 'product_name', 'wood_species', 'region_wood', 'category', 'price_eur']
    
    try:
        with open(OUTPUT_PATH, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(products)
        print(f"Successfully wrote {len(products)} products to {OUTPUT_PATH}")
    except Exception as e:
        print(f"Error writing output: {e}")
        sys.exit(1)

def main():
    """Main function to generate product data"""
    print("Arboré Product Table Generator")
    
    # Load product IDs and wood specs
    product_ids = load_product_ids()
    wood_data = load_wood_specs()
    
    print(f"Loaded {len(product_ids)} product IDs and {len(wood_data)} wood species")
    
    # Generate product data
    products = generate_product_data(product_ids, wood_data)
    
    # Write output
    write_output(products)

if __name__ == "__main__":
    main()
