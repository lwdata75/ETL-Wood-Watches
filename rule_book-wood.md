# Arboré – Cursor Rule Book (Supplier Wood Data Generator – Clean with 25 Species)

## 0) Scope

This rule book covers the **Supplier Wood Specs Generator**.
Goal: produce a **clean CSV dataset** with **25 predefined wood species**, each mapped to logical sourcing regions of the world.

Each row = a wood type sourced from a valid region (e.g., Oak from Europe, Teak from Asia).
This ensures the product table (100 products) can link consistently with supplier data.

---

## 1) IDs and Linking

* **Primary key**: `region_wood` + `wood_species`.
* Joins with:

  * `d_watch_product.region_wood`
  * `d_watch_product.wood_species`

---

## 2) File Format & Layout

* **Format**: CSV with header
* **Location**: `/data_out/supplier/`
* **File name**: `supplier_wood_specs_YYYYMM.csv`
* **Delimiter**: `,`
* **Encoding**: UTF-8

---

## 3) Schema

Table: `d_wood_specs`

| Column                        | Type   | Description                                    |
| ----------------------------- | ------ | ---------------------------------------------- |
| region_wood                   | STRING | Region or country where the wood is sourced    |
| wood_species                  | STRING | Common/recognized name of the wood             |
| density_kg_m3                 | NUMBER | Average density in kg/m³                       |
| hardness_n                    | NUMBER | Janka hardness in Newtons                      |
| carbon_storage_kg_co2e_per_kg | FLOAT  | Carbon storage factor (kg CO₂e per kg of wood) |
| recyclability_rate_pct        | NUMBER | Recyclability percentage (0–100)               |
| certification                 | STRING | Certification (FSC, PEFC, None)                |
| origin                        | STRING | Country of origin                              |
| updated_at                    | DATE   | Date of data extraction/update                 |

---

## 4) Predefined List of 25 Wood Species with Regions

### **Europe (6)**

1. Oak – France, Germany, UK, Balkans
2. Beech – Central Europe
3. Chestnut – France, Italy, Spain
4. Ash – Northern/Central Europe
5. Olive – Mediterranean (Italy, Greece, Spain)
6. Birch – Scandinavia, Russia

### **North America (5)**

7. Maple – Canada, USA
8. Walnut – USA
9. Cherry – USA
10. Redwood – California (USA)
11. Hickory – USA

### **South America (4)**

12. Mahogany – Brazil, Peru
13. Rosewood – Brazil
14. Ipe – Brazil
15. Pau Ferro – Brazil

### **Africa (4)**

16. Ebony – Central Africa
17. African Mahogany – West Africa
18. Bubinga – Cameroon, Gabon
19. Wenge – Congo

### **Asia (5)**

20. Teak – Myanmar, Indonesia, India
21. Bamboo – China, Southeast Asia
22. Mango – India, Thailand
23. Sandalwood – India
24. Padauk – Southeast Asia (Burma, Laos)

### **Oceania (1)**

25. Jarrah – Australia

---

## 5) Data Generation Rules

* **Rows**: exactly 25 (one per wood species).
* **Density**:

  * Light woods: 400–600 kg/m³ (e.g., Bamboo, Pine-like species)
  * Medium woods: 600–800 kg/m³ (e.g., Oak, Maple, Teak)
  * Heavy woods: 800–1000 kg/m³ (e.g., Ebony, Rosewood, Jarrah)
* **Hardness (Janka in N)**:

  * Softer species: 3000–5000
  * Harder/exotics: 7000–12000
* **Carbon storage**: 1.5–3.0 kg CO₂e/kg
* **Recyclability**: 60–100%
* **Certification**: Random with weights (FSC 50%, PEFC 30%, None 20%)
* **Origin**: Pick a valid country from region list above
* **Updated_at**: Current date

---

## 6) Example Clean Rows

```csv
region_wood,wood_species,density_kg_m3,hardness_n,carbon_storage_kg_co2e_per_kg,recyclability_rate_pct,certification,origin,updated_at
France,Oak,720,6500,2.1,95,FSC,France,2025-02-01
Canada,Maple,600,4800,1.9,92,PEFC,Canada,2025-02-01
Brazil,Rosewood,860,8200,2.5,88,FSC,Brazil,2025-02-01
India,Teak,650,5500,2.0,90,None,India,2025-02-01
Australia,Jarrah,820,8500,2.7,98,FSC,Australia,2025-02-01
```

---

## 7) Use Cases

* **Link to products**: each product uses one of these 25 wood types.
* **Sustainability metrics**: carbon storage and recyclability analysis.
* **Procurement**: identify exotic woods vs local/regional woods.
* **Marketing**: highlight certified sources.

---

## 8) Generator Guidelines

* Hardcode the list of 25 species + region associations.
* Randomize numeric values in plausible ranges.
* Generate exactly one row per species per run.
* Output always clean, without typos or missing values.

---

# End of Rule Book (Supplier Wood Specs – 25 Species)
