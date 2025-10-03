# Arboré – Cursor Rule Book (Product Table Generator – Based on Supplier CSV)

## 0) Scope

This rule book defines the **Product Dimension Generator** (`d_watch_product`).
Goal: produce **100 product rows** by combining:

* **25 wood species** defined in the supplier CSV (`/data_out/supplier/wood_specs.csv`).
* **4 Arboré watch categories**.

25 × 4 = 100 unique products.

---

## 1) IDs and Linking

* **Input IDs**: from `product_id.csv` (100 IDs, one per line, same directory as script).
* **Input wood data**: from `/data_out/supplier/wood_specs.csv`.

  * Provides `wood_species` and `region_wood`.
* **product_id**: assigned sequentially from `product_id.csv`.
* **Links to**:

  * `d_wood_specs` (supplier data) via `wood_species` and `region_wood`.
  * Orders (`f_watch_order`) via `product_id`.

---

## 2) File Format & Layout

* **Inputs**:

  * `product_id.csv` → list of 100 IDs.
  * `/data_out/supplier/wood_specs.csv` → clean supplier table with 25 species.

* **Output**: `/data_out/dims/d_watch_product.csv`

* **Delimiter**: `,`

* **Encoding**: UTF-8

---

## 3) Schema

Table: `d_watch_product`

| Column       | Type   | Description                                               |
| ------------ | ------ | --------------------------------------------------------- |
| product_id   | STRING | Unique product ID from `product_id.csv`                   |
| product_name | STRING | `Arboré <Category> <Wood>`                                |
| wood_species | STRING | From `/data_out/supplier/wood_specs.csv`                  |
| region_wood  | STRING | From `/data_out/supplier/wood_specs.csv`                  |
| category     | STRING | One of 4 Arboré categories                                |
| price_eur    | NUMBER | Price derived from category tier and wood type (adjusted) |

---

## 4) Arboré Watch Categories

Rebranded categories:

* **Arboré Héritage** → luxury tier
* **Arboré Voyage** → sport tier
* **Arboré Élégance** → premium tier
* **Arboré Essentiel** → base tier

---

## 5) Pricing Rules

* **Arboré Héritage (luxury)**: €700–1200
* **Arboré Voyage (sport)**: €400–800
* **Arboré Élégance (premium)**: €250–500
* **Arboré Essentiel (base)**: €120–250

Adjustments:

* Exotic/dense woods (e.g., Teak, Ebony, Rosewood, Jarrah, Wenge, Mahogany) → +15% premium.
* Standard woods (e.g., Oak, Maple, Beech, Cherry, Ash) → baseline.

---

## 6) Generation Rules

* Load the 100 IDs from `product_id.csv`.
* Load the 25 species and regions from `/data_out/supplier/wood_specs.csv`.
* Pair each species with the 4 Arboré categories.
* Assign IDs sequentially:

  * First 4 IDs → first wood (Héritage, Voyage, Élégance, Essentiel).
  * Next 4 IDs → second wood, and so on.
* Product name = `Arboré <Category> <Wood_Species>`.
* Region comes directly from supplier file.
* Ensure all 25 species are covered across all 4 categories.

---

## 7) Example Output

```csv
product_id,product_name,wood_species,region_wood,category,price_eur
P0001,Arboré Héritage Oak,Oak,France,Arboré Héritage,950
P0002,Arboré Voyage Oak,Oak,France,Arboré Voyage,520
P0003,Arboré Élégance Oak,Oak,France,Arboré Élégance,310
P0004,Arboré Essentiel Oak,Oak,France,Arboré Essentiel,180
P0005,Arboré Héritage Maple,Maple,Canada,Arboré Héritage,870
P0006,Arboré Voyage Maple,Maple,Canada,Arboré Voyage,450
...
```

---

## 8) Use Cases

* Enrich orders with product and wood details.
* Analyze sales by wood species and category.
* Link sustainability metrics from supplier specs to product sales.
* Build dashboards with full product mix (100 watches).

---

## 9) Generator Guidelines

* **Input**:

  * `product_id.csv` (100 IDs)
  * `/data_out/supplier/wood_specs.csv` (25 rows of species + regions)
* **Output**:

  * `/data_out/dims/d_watch_product.csv` (100 rows, fully expanded).
* **Implementation**:

  * Use Python `csv` reader/writer.
  * Sequentially map IDs to wood × category pairs.
  * Always generate in a consistent order to avoid mismatches.

---

# End of Rule Book (Product Table Generator – Based on Supplier CSV)
