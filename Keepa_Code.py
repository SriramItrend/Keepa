import requests
import json
from datetime import datetime, timedelta
import pandas as pd
import logging
import sys
from asins_our import asins

# Set up logging to console
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)

suffix = "our_asins"

def convert_unix_time(value):
    return datetime.utcfromtimestamp((value + 21564000) * 60).strftime('%Y-%m-%d %H:%M:%S')

def fetch_keepa_data(asins, access_key):
    domain_id = 1  # assuming Amazon US
    all_coupons_data = []
    all_lightning_deals_data = []
    all_new_price_history_data = []
    all_sales_history_data = []
    all_sales_rank_data = []
    all_category_rank_data = []

    for asin in asins:
        url = f'https://api.keepa.com/product?key={access_key}&domain={domain_id}&asin={asin}'
        response = requests.get(url)
        if response.status_code == 200:
            logging.info(f"Successfully fetched data for ASIN: {asin}")
            data = response.json()
            if data['products']:
                product = data['products'][0]
                # Coupon Data
                if product['productType'] == 0 and 'couponHistory' in product:
                    coupon_data = [
                        {
                            'date': convert_unix_time(product['couponHistory'][i]),
                            'one_time_coupon': (
                                f"{abs(product['couponHistory'][i+1])}%" if product['couponHistory'][i+1] < 0
                                else f"${product['couponHistory'][i+1] / 100:.2f}"
                            ) if product['couponHistory'][i+1] != 0 else None,
                            'subscribe_and_save_coupon': (
                                f"{abs(product['couponHistory'][i+2])}%" if product['couponHistory'][i+2] < 0
                                else f"${product['couponHistory'][i+2] / 100:.2f}"
                            ) if product['couponHistory'][i+2] != 0 else None
                        }
                        for i in range(0, len(product['couponHistory']), 3)
                        if datetime.utcfromtimestamp((product['couponHistory'][i] + 21564000) * 60).date() >= datetime.now().date() - timedelta(days=7)
                    ]
                    if coupon_data:
                        all_coupons_data.append({
                            'asin': asin,
                            'COUPON_HISTORY': coupon_data
                        })
                # Lightning Deal Data
                if product['productType'] == 0 and 'csv' in product and product['csv'][8] is not None:
                    lightning_deal_data = [
                        {
                            'date': convert_unix_time(product['csv'][8][i]),
                            'price': product['csv'][8][i+1] / 100  # Convert price from cents to dollars
                        }
                        for i in range(0, len(product['csv'][8]), 2)
                        if product['csv'][8][i+1] != -1 and datetime.utcfromtimestamp((product['csv'][8][i] + 21564000) * 60).date() >= datetime.now().date() - timedelta(days=7)
                    ]
                    if lightning_deal_data:
                        all_lightning_deals_data.append({
                            'asin': asin,
                            'LIGHTNING_DEAL': lightning_deal_data
                        })
                # New Price History Data
                if product['productType'] == 0 and 'csv' in product and product['csv'][1] is not None:
                    new_price_history_data = [
                        {
                            'date': convert_unix_time(product['csv'][1][i]),  # Use index 1 for NEW price history
                            'price': product['csv'][1][i+1] / 100  # Convert price from cents to dollars
                        }
                        for i in range(0, len(product['csv'][1]), 2)
                        if product['csv'][1][i+1] != -1 and datetime.utcfromtimestamp((product['csv'][1][i] + 21564000) * 60).date() >= datetime.now().date() - timedelta(days=7)
                    ]
                    if new_price_history_data:
                        all_new_price_history_data.append({
                            'asin': asin,
                            'NEW_PRICE_HISTORY': new_price_history_data
                        })
                # Sales Data
                if product['productType'] == 0 and 'csv' in product and product['csv'][0] is not None:
                    sales_data = [
                        {
                            'date': convert_unix_time(product['csv'][0][i]),
                            'price': product['csv'][0][i+1] / 100 if product['csv'][0][i+1] != -1 else -1  # Convert price from cents to dollars, leave -1 as it is
                        }
                        for i in range(0, len(product['csv'][0]), 2)
                        if datetime.utcfromtimestamp((product['csv'][0][i] + 21564000) * 60).date() >= datetime.now().date() - timedelta(days=7)
                    ]
                    all_sales_history_data.append({
                        'asin': asin,
                        'SALES': sales_data
                    })
                # Sales Rank Data
                if product['productType'] == 0 and 'csv' in product and product['csv'][3] is not None:
                    sales_rank_data = [
                        {
                            'date': convert_unix_time(product['csv'][3][i]),
                            'rank': product['csv'][3][i+1]
                        }
                        for i in range(0, len(product['csv'][3]), 2)
                        if datetime.utcfromtimestamp((product['csv'][3][i] + 21564000) * 60).date() >= datetime.now().date() - timedelta(days=7)
                    ]
                    all_sales_rank_data.append({
                        'asin': asin,
                        'SALES_RANK': sales_rank_data
                    })
                # Category Rank Data
                if product['productType'] == 0 and 'categoryTree' in product and product['categoryTree'] is not None and 'salesRanks' in product and product['salesRanks'] is not None:
                    for category in product['categoryTree']:
                        category_id = str(category['catId'])
                        if category_id in product['salesRanks']:
                            category_rank_data = [
                                {
                                    'date': convert_unix_time(product['salesRanks'][category_id][i]),
                                    'rank': product['salesRanks'][category_id][i+1]
                                }
                                for i in range(0, len(product['salesRanks'][category_id]), 2)
                                if datetime.utcfromtimestamp((product['salesRanks'][category_id][i] + 21564000) * 60).date() >= datetime.now().date() - timedelta(days=7)
                            ]
                            if category_rank_data:
                                all_category_rank_data.append({
                                    'asin': asin,
                                    'category': category['name'],
                                    'CATEGORY_RANK': category_rank_data
                                })
        else:
            logging.error(f"Failed to fetch data for ASIN: {asin}, Status Code: {response.status_code}")

    # Convert lists to DataFrames
    coupon_history_df = pd.DataFrame([entry for product in all_coupons_data for entry in product['COUPON_HISTORY']])
    coupon_history_df['asin'] = [product['asin'] for product in all_coupons_data for _ in product['COUPON_HISTORY']]
    logging.info(f"Coupon history data processed with {len(coupon_history_df)} records")

    lightning_deal_df = pd.DataFrame([entry for product in all_lightning_deals_data for entry in product['LIGHTNING_DEAL']])
    lightning_deal_df['asin'] = [product['asin'] for product in all_lightning_deals_data for _ in product['LIGHTNING_DEAL']]
    logging.info(f"Lightning deal data processed with {len(lightning_deal_df)} records")

    new_price_history_df = pd.DataFrame([entry for product in all_new_price_history_data for entry in product['NEW_PRICE_HISTORY']])
    new_price_history_df['asin'] = [product['asin'] for product in all_new_price_history_data for _ in product['NEW_PRICE_HISTORY']]
    logging.info(f"New price history data processed with {len(new_price_history_df)} records")

    sales_history_df = pd.DataFrame([entry for product in all_sales_history_data for entry in product['SALES']])
    sales_history_df['asin'] = [product['asin'] for product in all_sales_history_data for _ in product['SALES']]
    logging.info(f"Sales history data processed with {len(sales_history_df)} records")

    sales_rank_df = pd.DataFrame([entry for product in all_sales_rank_data for entry in product['SALES_RANK']])
    sales_rank_df['asin'] = [product['asin'] for product in all_sales_rank_data for _ in product['SALES_RANK']]
    logging.info(f"Sales rank data processed with {len(sales_rank_df)} records")

    category_rank_df = pd.DataFrame([entry for product in all_category_rank_data for entry in product['CATEGORY_RANK']])
    category_rank_df['asin'] = [product['asin'] for product in all_category_rank_data for _ in product['CATEGORY_RANK']]
    category_rank_df['category'] = [product['category'] for product in all_category_rank_data for _ in product['CATEGORY_RANK']]
    logging.info(f"Category rank data processed with {len(category_rank_df)} records")

    return {
        "COUPON_HISTORY": coupon_history_df,
        "LIGHTNING_DEAL": lightning_deal_df,
        "NEW_PRICE_HISTORY": new_price_history_df,
        "SALES": sales_history_df,
        "SALES_RANK": sales_rank_df,
        "CATEGORY_RANK": category_rank_df
    }

# Fetch and process Keepa data
access_key = '869gl1g1tngcp153v93v49a47eg401fhr56em2u4tkn228ap5c70hi0o3tfjpgf7'
logging.info("Starting to fetch Keepa data")
data = fetch_keepa_data(asins, access_key)
logging.info("Keepa data fetched and processed")

# Filter the dataframes for the last 7 days
end_date = datetime.now()
start_date = end_date - timedelta(days=7)

def convert_date_column(df):
    df['date'] = pd.to_datetime(df['date'])
    return df

def filter_by_date(df, start_date, end_date):
    return df[(df['date'] >= start_date) & (df['date'] <= end_date)]

filtered_category_rank_df = filter_by_date(convert_date_column(data["CATEGORY_RANK"]), start_date, end_date)
filtered_coupon_history_df = filter_by_date(convert_date_column(data["COUPON_HISTORY"]), start_date, end_date)
filtered_lightning_deal_df = filter_by_date(convert_date_column(data["LIGHTNING_DEAL"]), start_date, end_date)
filtered_new_price_history_df = filter_by_date(convert_date_column(data["NEW_PRICE_HISTORY"]), start_date, end_date)
filtered_sales_history_df = filter_by_date(convert_date_column(data["SALES"]), start_date, end_date)
filtered_sales_rank_df = filter_by_date(convert_date_column(data["SALES_RANK"]), start_date, end_date)

# Print filtered dataframes for verification
logging.info("Filtered category rank data")
print(filtered_category_rank_df)

logging.info("Filtered coupon history data")
print(filtered_coupon_history_df)

logging.info("Filtered lightning deal data")
print(filtered_lightning_deal_df)

logging.info("Filtered new price history data")
print(filtered_new_price_history_df)

logging.info("Filtered sales history data")
print(filtered_sales_history_df)

logging.info("Filtered sales rank data")
print(filtered_sales_rank_df)
