# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "3aed7b50-2b26-42a1-82ed-c40a68684fba",
# META       "default_lakehouse_name": "Dev_Lakehouse",
# META       "default_lakehouse_workspace_id": "35f8c3f3-0a5f-4298-a25b-87c98e34404e",
# META       "known_lakehouses": [
# META         {
# META           "id": "3aed7b50-2b26-42a1-82ed-c40a68684fba"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# This will create three sample files . You need to have /Source_Data Folder created in your lakehouse first and attach that lakehouse to this notebook before running this notebook.

# CELL ********************

import pandas as pd
import numpy as np
from datetime import datetime, timedelta

# Settings
num_records = 100
sales_records = 500
# Setting dimensions to start way before the sales data
dimension_start_date = "01-01-2024" 
# Sales will start from Jan 2026
sales_base_date = datetime(2026, 1, 1)

# --- 1. Generate Product Data ---
products = []
categories = ['Electronics', 'Home Appliances', 'Furniture', 'Outdoor']
for i in range(1, num_records + 1):
    products.append({
        "product_id": 100 + i,
        "product_code": f"PROD{1000+i}",
        "product_name": f"Product_{i}",
        "category": np.random.choice(categories),
        "sub_category": "General",
        "list_price": round(np.random.uniform(50, 5000), 2),
        "record_date": dimension_start_date  # Backdated
    })
pd.DataFrame(products).to_csv("/lakehouse/default/Files/Source_Data/Product.csv", index=False)

# --- 2. Generate Customer Data ---
customers = []
states = ['Maharashtra', 'Karnataka', 'Tamil Nadu', 'Delhi', 'Gujarat']
for i in range(1, num_records + 1):
    customers.append({
        "customer_id": i,
        "customer_code": f"CUST{2000+i}",
        "customer_name": f"Customer_Name_{i}",
        "city": f"City_{i}",
        "state": np.random.choice(states),
        "country": "India",
        "customer_type": np.random.choice(['Gold', 'Silver', 'Platinum']),
        "record_date": dimension_start_date  # Backdated
    })
pd.DataFrame(customers).to_csv("/lakehouse/default/Files/Source_Data/Customer.csv", index=False)

# --- 3. Generate Sales Operations Data ---
sales = []
for i in range(1, sales_records + 1):
    o_date = sales_base_date + timedelta(days=np.random.randint(0, 100))
    s_date = o_date + timedelta(days=np.random.randint(1, 5))
    d_date = s_date + timedelta(days=np.random.randint(1, 7))
    
    sales.append({
        "order_id": f"ORD_{1000+i}",
        "customer_id": np.random.randint(1, num_records + 1),
        "product_id": np.random.randint(101, 101 + num_records),
        # Updated to dd-MM-yyyy to match your notebook's loading logic
        "order_date": o_date.strftime("%d-%m-%Y"),
        "ship_date": s_date.strftime("%d-%m-%Y"),
        "delivery_date": d_date.strftime("%d-%m-%Y") if i % 5 != 0 else "", 
        "amount": round(np.random.uniform(100, 10000), 2),
        "status": "Delivered" if i % 5 != 0 else "Shipped"
    })
pd.DataFrame(sales).to_csv("/lakehouse/default/Files/Source_Data/Sales_Operations.csv", index=False)

print(f"✅ Success: 100 Products/Customers (starting {dimension_start_date}) and 500 Sales records generated.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
