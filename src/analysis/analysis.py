import pandas as pd
from google.cloud import bigquery
import plotly.express as px
import os

# --- 1. Load and Prepare Data ---

print("Connecting to BigQuery...")
PROJECT_ID = "unbiased-reporting"
DATASET_ID = "unbiased-reporting"
TABLE_ID = "crypto_quotes"

client = bigquery.Client(project=PROJECT_ID)

query = f"""SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}`"""

try:
    df = client.query(query).to_dataframe()
    print(f"Successfully loaded {len(df)} rows from BigQuery.")
except Exception as e:
    print(f"Failed to load data from BigQuery: {e}")
    exit()

# Create Price Bins
print("Creating price bins...")
bins = [0, 500, 5000, 30001]
labels = ['Low (0-499)', 'Medium (500-4999)', 'High (5k+)']
df['PriceBin'] = pd.cut(df['Amount'], bins=bins, labels=labels, right=False)

# Create output directories
os.makedirs("rank_charts", exist_ok=True)
os.makedirs("fee_charts", exist_ok=True)
os.makedirs("rank_vs_fee_charts", exist_ok=True)

# --- 2. Generate Individual Rank Analysis Charts ---

print("Generating individual Rank Analysis charts...")
unique_combinations = df[['CryptoCurrency', 'Region']].drop_duplicates()

for index, row in unique_combinations.iterrows():
    crypto = row['CryptoCurrency']
    region = row['Region']
    
    subset_df = df[(df['CryptoCurrency'] == crypto) & (df['Region'] == region)]
    
    if not subset_df.empty:
        avg_rank_df = subset_df.groupby(['PriceBin', 'Provider'])['Rank'].mean().reset_index()
        
        fig_rank = px.bar(avg_rank_df, 
                          x='Provider', y='Rank', 
                          color='Provider',
                          facet_col='PriceBin',
                          category_orders={'PriceBin': labels},
                          title=f'Average Provider Rank in {region} for {crypto}',
                          labels={'Rank': 'Average Rank (Lower is Better)'})
        
        fig_rank.update_yaxes(autorange="reversed")
        
        # Sanitize filename
        safe_crypto = "".join(c for c in crypto if c.isalnum() or c in (' ', '_')).rstrip()
        filename = f"rank_charts/Rank_{safe_crypto}_{region}.html"
        fig_rank.write_html(filename)
        print(f"Saved: {filename}")

# --- 3. Generate Individual Fee Analysis Charts ---

print("Generating individual Fee Analysis charts...")
for index, row in unique_combinations.iterrows():
    crypto = row['CryptoCurrency']
    region = row['Region']
    
    subset_df = df[(df['CryptoCurrency'] == crypto) & (df['Region'] == region)]
    
    if not subset_df.empty:
        avg_fee_df = subset_df.groupby(['PriceBin', 'Provider'])['TotalFeePercentage'].mean().reset_index()
        
        fig_fee = px.bar(avg_fee_df, 
                         x='Provider', y='TotalFeePercentage', 
                         color='Provider', 
                         facet_col='PriceBin',
                         category_orders={'PriceBin': labels},
                         title=f'Average Total Fee % in {region} for {crypto}',
                         labels={'TotalFeePercentage': 'Average Fee (%)'})
        
        # Sanitize filename
        safe_crypto = "".join(c for c in crypto if c.isalnum() or c in (' ', '_')).rstrip()
        filename = f"fee_charts/Fee_{safe_crypto}_{region}.html"
        fig_fee.write_html(filename)
        print(f"Saved: {filename}")

# --- 4. Analysis: Rank vs. Fee by Price Bin ---

print("Generating individual Rank vs. Fee Analysis charts...")
for index, row in unique_combinations.iterrows():
    crypto = row['CryptoCurrency']
    region = row['Region']
    
    subset_df = df[(df['CryptoCurrency'] == crypto) & (df['Region'] == region)]
    
    if not subset_df.empty:
        avg_df = subset_df.groupby(['PriceBin', 'Provider', 'PaymentMethod']).agg({'Rank': 'mean', 'TotalFeePercentage': 'mean'}).reset_index()
        
        fig_scatter = px.scatter(avg_df, 
                                 x='Rank', y='TotalFeePercentage', 
                                 color='Provider',
                                 symbol='PaymentMethod',
                                 facet_col='PriceBin',
                                 category_orders={'PriceBin': labels},
                                 title=f'Average Rank vs. Fee % in {region} for {crypto}',
                                 labels={'Rank': 'Average Rank', 'TotalFeePercentage': 'Average Fee (%)'})
        
        # Sanitize filename
        safe_crypto = "".join(c for c in crypto if c.isalnum()or c in (' ', '_')).rstrip()
        filename = f"rank_vs_fee_charts/RankVsFee_{safe_crypto}_{region}.html"
        fig_scatter.write_html(filename)
        print(f"Saved: {filename}")

print("Analysis complete.")