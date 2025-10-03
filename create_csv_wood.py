#!/usr/bin/env python3
import csv
import random
import os
from datetime import datetime

# Define output directory and file name
output_dir = '/Users/samuel/Projects/Albert School/Courses/ETL/data_out/supplier'
os.makedirs(output_dir, exist_ok=True)
current_date = datetime.now().strftime('%Y%m')
output_file = f'{output_dir}/wood_specs.csv'

# Define wood species with their regions and countries
wood_species_data = {
    # Europe (6)
    'Oak': {'region': 'Europe', 'countries': ['France', 'Germany', 'UK', 'Balkans'], 'density_range': (600, 800)},
    'Beech': {'region': 'Europe', 'countries': ['Germany', 'Austria', 'Switzerland'], 'density_range': (600, 750)},
    'Chestnut': {'region': 'Europe', 'countries': ['France', 'Italy', 'Spain'], 'density_range': (550, 700)},
    'Ash': {'region': 'Europe', 'countries': ['Sweden', 'Norway', 'Finland', 'Germany'], 'density_range': (600, 750)},
    'Olive': {'region': 'Europe', 'countries': ['Italy', 'Greece', 'Spain'], 'density_range': (700, 850)},
    'Birch': {'region': 'Europe', 'countries': ['Sweden', 'Finland', 'Russia'], 'density_range': (550, 700)},
    
    # North America (5)
    'Maple': {'region': 'North America', 'countries': ['Canada', 'USA'], 'density_range': (550, 700)},
    'Walnut': {'region': 'North America', 'countries': ['USA'], 'density_range': (600, 750)},
    'Cherry': {'region': 'North America', 'countries': ['USA'], 'density_range': (550, 650)},
    'Redwood': {'region': 'North America', 'countries': ['USA'], 'density_range': (450, 550)},
    'Hickory': {'region': 'North America', 'countries': ['USA'], 'density_range': (700, 850)},
    
    # South America (4)
    'Mahogany': {'region': 'South America', 'countries': ['Brazil', 'Peru'], 'density_range': (550, 700)},
    'Rosewood': {'region': 'South America', 'countries': ['Brazil'], 'density_range': (800, 950)},
    'Ipe': {'region': 'South America', 'countries': ['Brazil'], 'density_range': (850, 1000)},
    'Pau Ferro': {'region': 'South America', 'countries': ['Brazil'], 'density_range': (800, 950)},
    
    # Africa (4)
    'Ebony': {'region': 'Africa', 'countries': ['Cameroon', 'Congo', 'Gabon'], 'density_range': (850, 1000)},
    'African Mahogany': {'region': 'Africa', 'countries': ['Ghana', 'Ivory Coast', 'Nigeria'], 'density_range': (550, 700)},
    'Bubinga': {'region': 'Africa', 'countries': ['Cameroon', 'Gabon'], 'density_range': (750, 900)},
    'Wenge': {'region': 'Africa', 'countries': ['Congo'], 'density_range': (800, 950)},
    
    # Asia (5)
    'Teak': {'region': 'Asia', 'countries': ['Myanmar', 'Indonesia', 'India'], 'density_range': (600, 750)},
    'Bamboo': {'region': 'Asia', 'countries': ['China', 'Vietnam', 'Thailand'], 'density_range': (400, 600)},
    'Mango': {'region': 'Asia', 'countries': ['India', 'Thailand'], 'density_range': (550, 700)},
    'Sandalwood': {'region': 'Asia', 'countries': ['India'], 'density_range': (750, 900)},
    'Padauk': {'region': 'Asia', 'countries': ['Myanmar', 'Laos'], 'density_range': (700, 850)},
    
    # Oceania (1)
    'Jarrah': {'region': 'Oceania', 'countries': ['Australia'], 'density_range': (800, 950)},
}

def generate_wood_specs():
    """Generate wood specifications according to the rule book."""
    today = datetime.now().strftime('%Y-%m-%d')
    wood_specs = []
    
    for species, data in wood_species_data.items():
        # Select a random country from the list
        origin = random.choice(data['countries'])
        region_wood = origin
        
        # Generate density based on wood type
        density = random.randint(data['density_range'][0], data['density_range'][1])
        
        # Generate hardness based on density (correlation)
        # Higher density generally means higher hardness
        base_hardness = 3000 if density < 600 else (5000 if density < 800 else 7000)
        hardness = random.randint(base_hardness, base_hardness + 5000)
        
        # Generate carbon storage
        carbon_storage = round(random.uniform(1.5, 3.0), 2)
        
        # Generate recyclability
        recyclability = random.randint(60, 100)
        
        # Generate certification with weights
        certification = random.choices(
            ['FSC', 'PEFC', 'None'], 
            weights=[0.5, 0.3, 0.2], 
            k=1
        )[0]
        
        # Create the wood spec row
        wood_spec = {
            'region_wood': region_wood,
            'wood_species': species,
            'density_kg_m3': density,
            'hardness_n': hardness,
            'carbon_storage_kg_co2e_per_kg': carbon_storage,
            'recyclability_rate_pct': recyclability,
            'certification': certification,
            'origin': origin,
            'updated_at': today
        }
        
        wood_specs.append(wood_spec)
    
    return wood_specs

def write_csv(wood_specs, output_file):
    """Write wood specifications to CSV file."""
    fieldnames = [
        'region_wood', 'wood_species', 'density_kg_m3', 'hardness_n',
        'carbon_storage_kg_co2e_per_kg', 'recyclability_rate_pct',
        'certification', 'origin', 'updated_at'
    ]
    
    with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(wood_specs)
    
    print(f"Generated {len(wood_specs)} wood specifications and saved to {output_file}")

if __name__ == "__main__":
    wood_specs = generate_wood_specs()
    write_csv(wood_specs, output_file)
