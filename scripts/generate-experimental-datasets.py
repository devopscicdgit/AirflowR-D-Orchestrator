"""
Fake data generator for chemistry & materials pipeline.

Datasets generated:
1. Suppliers: supplier_id, name, country, reliability_score, lead_time_days
2. Materials: material_id, supplier_id, name, category, melting_point_c, tensile_strength_mpa, density_g_cm3
3. Experiments: experiment_id, material_id, date, temperature_c, pressure_atm, result_yield_pct, researcher
4. Orders: order_id, supplier_id, material_id, quantity_kg, order_date, delivery_date, status

Relationships:
- supplier_id is PK in Suppliers, FK in Materials and Orders
- material_id is PK in Materials, FK in Experiments and Orders
"""

import random
import pandas as pd
from faker import Faker
from uuid import uuid4

OUTPUT_DIR = "./api-services/data"
fake = Faker()

# --- Suppliers ---
def generate_suppliers(n=50):
    suppliers = []
    for _ in range(n):
        suppliers.append({
            "supplier_id": str(uuid4()),
            "name": fake.company(),
            "country": fake.country(),
            "reliability_score": round(random.uniform(0.5, 1.0), 2),
            "lead_time_days": random.randint(5, 60)
        })
    return suppliers

# --- Materials ---
def generate_materials(suppliers, n=200):
    categories = ["Polymer", "Alloy", "Catalyst", "Composite", "Ceramic"]
    materials = []
    for _ in range(n):
        supplier = random.choice(suppliers)
        materials.append({
            "material_id": str(uuid4()),
            "supplier_id": supplier["supplier_id"],
            "name": f"{fake.color_name()} {random.choice(categories)}",
            "category": random.choice(categories),
            "melting_point_c": random.randint(200, 2000),
            "tensile_strength_mpa": round(random.uniform(50, 2000), 2),
            "density_g_cm3": round(random.uniform(0.8, 15.0), 2),
        })
    return materials

# --- Experiments ---
def generate_experiments(materials, n=1000):
    experiments = []
    for _ in range(n):
        material = random.choice(materials)
        experiments.append({
            "experiment_id": str(uuid4()),
            "material_id": material["material_id"],
            "date": fake.date_between(start_date="-2y", end_date="today").strftime("%Y-%m-%d"),
            "temperature_c": random.randint(20, 1200),
            "pressure_atm": round(random.uniform(0.5, 10.0), 2),
            "result_yield_pct": round(random.uniform(40, 99), 2),
            "researcher": fake.name()
        })
    return experiments

# --- Orders ---
def generate_orders(suppliers, materials, n=500):
    statuses = ["Pending", "Shipped", "Delivered", "Cancelled"]
    orders = []
    for _ in range(n):
        supplier = random.choice(suppliers)
        material = random.choice(materials)
        order_date = fake.date_between(start_date="-1y", end_date="today")
        delivery_date = fake.date_between(start_date=order_date, end_date="+30d")
        orders.append({
            "order_id": str(uuid4()),
            "supplier_id": supplier["supplier_id"],
            "material_id": material["material_id"],
            "quantity_kg": round(random.uniform(10, 1000), 2),
            "order_date": order_date.strftime("%Y-%m-%d"),
            "delivery_date": delivery_date.strftime("%Y-%m-%d"),
            "status": random.choice(statuses)
        })
    return orders

# --- Main Runner ---
def main():
    suppliers = generate_suppliers(50)
    materials = generate_materials(suppliers, 200)
    experiments = generate_experiments(materials, 1000)
    orders = generate_orders(suppliers, materials, 500)

    pd.DataFrame(suppliers).to_csv(f"{OUTPUT_DIR}/suppliers.csv", index=False)
    pd.DataFrame(materials).to_csv(f"{OUTPUT_DIR}/materials.csv", index=False)
    pd.DataFrame(experiments).to_csv(f"{OUTPUT_DIR}/experiments.csv", index=False)
    pd.DataFrame(orders).to_csv(f"{OUTPUT_DIR}/orders.csv", index=False)

    print("✅ Datasets generated successfully!")

if __name__ == "__main__":
    main()
