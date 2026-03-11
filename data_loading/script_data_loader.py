import os
import base64
import requests
import json
import pandas as pd
from datetime import datetime, timedelta
from dotenv import load_dotenv

# 1. SETUP & AUTHENTICATION
load_dotenv()
CLIENT_ID = os.getenv('KROGER_CLIENT_ID')
CLIENT_SECRET = os.getenv('KROGER_CLIENT_SECRET')

def get_auth_token():
    auth_str = f"{CLIENT_ID}:{CLIENT_SECRET}"
    encoded_auth = base64.b64encode(auth_str.encode()).decode()
    url = "https://api.kroger.com/v1/connect/oauth2/token"
    headers = {"Content-Type": "application/x-www-form-urlencoded", "Authorization": f"Basic {encoded_auth}"}
    data = {"grant_type": "client_credentials", "scope": "product.compact"}
    response = requests.post(url, headers=headers, data=data)
    return response.json().get('access_token')

def get_nearest_location(token, zip_code="85281"):
    url = "https://api.kroger.com/v1/locations"
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
    params = {"filter.zipCode.near": zip_code, "filter.limit": "1"}
    response = requests.get(url, headers=headers, params=params)
    locations = response.json().get('data', [])
    return locations[0] if locations else None

def fetch_dairy_data(token, location_id):
    url = "https://api.kroger.com/v1/products"
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
    params = {"filter.term": "milk", "filter.locationId": location_id, "filter.limit": "50"}
    response = requests.get(url, headers=headers, params=params)
    return response.json()

# 2. STAR SCHEMA TRANSFORMATION (The "T" in ETL)
def generate_dim_date(start_date_str):
    """Generates a date dimension table to avoid gaps in BigQuery analysis."""
    start_dt = datetime.fromisoformat(start_date_str.replace('Z', ''))
    date_list = []
    for i in range(-30, 31): # 30 days before and after for context
        dt = start_dt + timedelta(days=i)
        date_list.append({
            "date_key_pk": int(dt.strftime('%Y%m%d')),
            "full_date": dt.date(),
            "day_of_week": dt.strftime('%A'),
            "is_promo_period": False # Placeholder for holiday logic
        })
    return pd.DataFrame(date_list)

def process_to_csv(dairy_json, location_info):
    products, facts = [], []
    loc_id = location_info['locationId']
    
    # 3.1 DIM_LOCATION
    dim_location = pd.DataFrame([{
        "location_id_pk": int(loc_id),
        "store_name": location_info['name'],
        "zip_code": location_info['address']['zipCode'],
        "market_division": "Southwest"
    }])

    for item in dairy_json.get('data', []):
        sku = item['productId']
        
        # 3.2 DIM_PRODUCT
        brand = item.get('brand', 'Generic')
        products.append({
            "sku_id_pk": sku,
            "product_description": item.get('description'),
            "brand_tier": "Premium" if "Simple Truth" in brand else "Private Label",
            "is_snap_eligible": item.get('snapEligible', False),
            "wellness_score": int(item.get('nutritionInformation', [{}])[0].get('nutritionalRating', 0) if item.get('nutritionInformation') else 0),
            "size_uom": item.get('items', [{}])[0].get('size', 'N/A')
        })

        # 3.3 FACT_PRICING_INVENTORY
        item_data = item.get('items', [{}])[0]
        price_data = item_data.get('price', {})
        

        base = price_data.get('regular', 0.0)
        sale = price_data.get('promo', base) if price_data.get('promo') != 0 else base
        risk_map = {"HIGH": 0, "LOW": 1, "TEMPORARILY_OUT_OF_STOCK": 2}
        stock_status = item_data.get('inventory', {}).get('stockLevel', 'UNKNOWN')
        facings = sum(int(l.get('numberOfFacings', 0)) for l in item.get('aisleLocations', []))

        eff_date_str = price_data.get('effectiveDate', {}).get('value', datetime.now().isoformat())
        date_key = int(datetime.fromisoformat(eff_date_str.replace('Z', '')).strftime('%Y%m%d'))

        facts.append({
            "sku_id_fk": sku,
            "location_id_fk": int(loc_id),
            "date_key_fk": date_key,
            "base_price": base,
            "sale_price": sale,
            "inventory_risk_code": risk_map.get(stock_status, 3),
            "total_shelf_facings": facings
        })

    # Export all to CSV
    pd.DataFrame(products).drop_duplicates().to_csv('dim_product.csv', index=False)
    pd.DataFrame(facts).to_csv('fact_pricing_inventory.csv', index=False)
    dim_location.to_csv('dim_location.csv', index=False)
    generate_dim_date(eff_date_str).to_csv('dim_date.csv', index=False)

if __name__ == "__main__":
    token = get_auth_token()
    loc_info = get_nearest_location(token)
    if loc_info:
        data = fetch_dairy_data(token, loc_info['locationId'])
        process_to_csv(data, loc_info)
        print("✓ ETL Complete: 4 Star-Schema CSVs ready for BigQuery upload.")