import requests
import json
from datetime import datetime, timedelta
import os
##For competitor data
#from asins_competitor import asins
#suffix = "competitor"
# # For our data
from asins_our import asins
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
            data = response.json()
            with open(f'keepa_history.json', 'w') as f:
                json.dump(data, f, indent=4)
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
                        if datetime.utcfromtimestamp((product['couponHistory'][i] + 21564000) * 60).year >= 2024 and  datetime.utcfromtimestamp((product['couponHistory'][i] + 21564000) * 60).month >= 7 and datetime.utcfromtimestamp((product['couponHistory'][i] + 21564000) * 60).day >= 9
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
                        if product['csv'][8][i+1] != -1 and datetime.utcfromtimestamp((product['csv'][8][i] + 21564000) * 60).year >= 2024
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
                        if product['csv'][1][i+1] != -1 and datetime.utcfromtimestamp((product['csv'][1][i] + 21564000) * 60).year >= 2024
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
                        if datetime.utcfromtimestamp((product['csv'][0][i] + 21564000) * 60).year >= 2024
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
                        if datetime.utcfromtimestamp((product['csv'][3][i] + 21564000) * 60).year >= 2024
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
                                if datetime.utcfromtimestamp((product['salesRanks'][category_id][i] + 21564000) * 60).year >= 2024
                            ]
                            if category_rank_data:
                                all_category_rank_data.append({
                                    'asin': asin,
                                    'category': category['name'],
                                    'CATEGORY_RANK': category_rank_data
                                })
        else:
            print(f"Failed to fetch data for ASIN: {asin}, Status Code: {response.status_code}")
    # Create the folder if it doesn't exist
    folder_name = f'Data_{suffix.capitalize()}'
    if not os.path.exists(folder_name):
        os.makedirs(folder_name)
    # Save all_asins_data to separate JSON files in the respective folder
    with open(f'{folder_name}/coupon_history_{suffix}.json', 'w') as f:
        json.dump(all_coupons_data, f, indent=4)
    with open(f'{folder_name}/lightening_deal_{suffix}.json', 'w') as f:
        json.dump(all_lightning_deals_data, f, indent=4)
    with open(f'{folder_name}/new_price_history_{suffix}.json', 'w') as f:
        json.dump(all_new_price_history_data, f, indent=4)
    with open(f'{folder_name}/sales_history_{suffix}.json', 'w') as f:
        json.dump(all_sales_history_data, f, indent=4)
    with open(f'{folder_name}/bsr_rank_history_{suffix}.json', 'w') as f:
        json.dump(all_sales_rank_data, f, indent=4)
    with open(f'{folder_name}/category_rank_history_{suffix}.json', 'w') as f:
        json.dump(all_category_rank_data, f, indent=4)
    return {
        "COUPON_HISTORY": all_coupons_data,
        "LIGHTNING_DEAL": all_lightning_deals_data,
        "NEW_PRICE_HISTORY": all_new_price_history_data,
        "SALES": all_sales_history_data,
        "SALES_RANK": all_sales_rank_data,
            "CATEGORY_RANK": all_category_rank_data
    }
access_key = '869gl1g1tngcp153v93v49a47eg401fhr56em2u4tkn228ap5c70hi0o3tfjpgf7'
data = fetch_keepa_data(asins, access_key)
print(json.dumps(data, indent=4))






import json
import csv
def read_json_file(filepath):
    with open(filepath, 'r') as file:
        return json.load(file)
def save_coupon_history_to_csv(coupon_history_data, csv_filepath):
    with open(csv_filepath, 'w', newline='') as csvfile:
        fieldnames = ['asin', 'date', 'one_time_coupon', 'subscribe_and_save_coupon']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for item in coupon_history_data:
            asin = item['asin']
            for coupon_entry in item['COUPON_HISTORY']:
                writer.writerow({
                    'asin': asin,
                    'date': coupon_entry['date'],
                    'one_time_coupon': coupon_entry['one_time_coupon'],
                    'subscribe_and_save_coupon': coupon_entry['subscribe_and_save_coupon']
                })
def save_lightning_deal_to_csv(lightning_deal_data, csv_filepath):
    with open(csv_filepath, 'w', newline='') as csvfile:
        fieldnames = ['asin', 'date', 'price']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for item in lightning_deal_data:
            asin = item['asin']
            for deal_entry in item['LIGHTNING_DEAL']:
                writer.writerow({
                    'asin': asin,
                    'date': deal_entry['date'],
                    'price': deal_entry['price']
                })
def save_new_price_history_to_csv(new_price_history_data, csv_filepath):
    with open(csv_filepath, 'w', newline='') as csvfile:
        fieldnames = ['asin', 'date', 'price']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for item in new_price_history_data:
            asin = item['asin']
            for price_entry in item['NEW_PRICE_HISTORY']:
                writer.writerow({
                    'asin': asin,
                    'date': price_entry['date'],
                    'price': price_entry['price']
                })
def save_sales_history_to_csv(sales_history_data, csv_filepath):
    with open(csv_filepath, 'w', newline='') as csvfile:
        fieldnames = ['asin', 'date', 'price']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for item in sales_history_data:
            asin = item['asin']
            for sales_entry in item['SALES']:
                writer.writerow({
                    'asin': asin,
                    'date': sales_entry['date'],
                    'price': sales_entry['price']
                })
def save_sales_rank_to_csv(sales_rank_data, csv_filepath):
    with open(csv_filepath, 'w', newline='') as csvfile:
        fieldnames = ['asin', 'date', 'rank']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for item in sales_rank_data:
            asin = item['asin']
            for rank_entry in item['SALES_RANK']:
                writer.writerow({
                    'asin': asin,
                    'date': rank_entry['date'],
                    'rank': rank_entry['rank']
                })
def save_category_rank_to_csv(category_rank_data, csv_filepath):
    with open(csv_filepath, 'w', newline='') as csvfile:
        fieldnames = ['asin', 'category', 'date', 'rank']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for item in category_rank_data:
            asin = item['asin']
            category = item['category']
            for rank_entry in item['CATEGORY_RANK']:
                writer.writerow({
                    'asin': asin,
                    'category': category,
                    'date': rank_entry['date'],
                    'rank': rank_entry['rank']
                })
# Read the JSON files and save to CSV
json_csv_pairs = [
    ('Data_Our_asins/coupon_history_our_asins.json', 'coupon_history.csv', save_coupon_history_to_csv),
    ('Data_Our_asins/lightening_deal_our_asins.json', 'lightning_deal.csv', save_lightning_deal_to_csv),
    ('Data_Our_asins/new_price_history_our_asins.json', 'new_price_history.csv', save_new_price_history_to_csv),
    ('Data_Our_asins/sales_history_our_asins.json', 'sales_history.csv', save_sales_history_to_csv),
    ('Data_Our_asins/bsr_rank_history_our_asins.json', 'sales_rank.csv', save_sales_rank_to_csv),
    ('Data_Our_asins/category_rank_history_our_asins.json', 'category_rank.csv', save_category_rank_to_csv)
]
for json_filepath, csv_filepath, save_function in json_csv_pairs:
    data = read_json_file(json_filepath)
    save_function(data, csv_filepath)
    print(f'{json_filepath} data has been saved to {csv_filepath}')

import pandas as pd
# Read the CSV files
category_rank_df = pd.read_csv("category_rank.csv")
coupon_history_df = pd.read_csv("coupon_history.csv")
lightning_deal_df = pd.read_csv("lightning_deal.csv")
new_price_history_df = pd.read_csv("new_price_history.csv")
sales_history_df = pd.read_csv("sales_history.csv")
sales_rank_df = pd.read_csv("sales_rank.csv")
# Convert the 'date' column to datetime format
def convert_date_column(df):
    df['date'] = pd.to_datetime(df['date'])
    return df
category_rank_df = convert_date_column(category_rank_df)
coupon_history_df = convert_date_column(coupon_history_df)
lightning_deal_df = convert_date_column(lightning_deal_df)
new_price_history_df = convert_date_column(new_price_history_df)
sales_history_df = convert_date_column(sales_history_df)
sales_rank_df = convert_date_column(sales_rank_df)
# Filter the dataframes for the dates 8-July-2024 and 9-July-2024
start_date = '2024-07-18'
end_date = '2024-07-22'
def filter_by_date(df):
    return df[(df['date'] >= start_date) & (df['date'] <= end_date)]
filtered_category_rank_df = filter_by_date(category_rank_df)
filtered_coupon_history_df = filter_by_date(coupon_history_df)
filtered_lightning_deal_df = filter_by_date(lightning_deal_df)
filtered_new_price_history_df = filter_by_date(new_price_history_df)
filtered_sales_history_df = filter_by_date(sales_history_df)
filtered_sales_rank_df = filter_by_date(sales_rank_df)
filtered_category_rank_df.to_csv("filtered_category_rank_df.csv")
filtered_coupon_history_df.to_csv("filtered_coupon_history_df.csv")
filtered_lightning_deal_df.to_csv("filtered_lightning_deal_df.csv")
filtered_new_price_history_df.to_csv("filtered_new_price_history_df.csv")
filtered_sales_history_df.to_csv("filtered_sales_history_df.csv")
filtered_sales_rank_df.to_csv("filtered_sales_rank_df.csv")