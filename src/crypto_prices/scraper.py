import requests
import time
from datetime import datetime

def get_market_rate(crypto_currency_id, fiat_currency):
    """
    Fetches the real market exchange rate for a cryptocurrency relative to fiat currency.
    Returns the amount of fiat currency needed to buy 1 unit of crypto.
    """
    # Extract chain_id and token_address from crypto_currency_id
    # Format: /currencies/crypto/{chain_id}/{token_address}
    parts = crypto_currency_id.split('/')
    chain_id = parts[3]
    token_address = parts[4]
    
    url = (
        f"https://on-ramp-cache.api.cx.metamask.io/currencies/crypto/{chain_id}/{token_address}/amount"
        f"?value=1"
        f"&fiat=%2Fcurrencies%2Ffiat%2F{fiat_currency.lower()}"
        f"&sdk=2.1.8"
        f"&context=browser"
        f"&keys="
    )
    
    retries = 3
    delay = 1  # seconds
    for i in range(retries):
        try:
            response = requests.get(url)
            response.raise_for_status()
            data = response.json()
            # This returns how much fiat is needed for 1 crypto
            return float(data.get('value', 0))
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 429:
                print(f"  Rate limited. Retrying in {delay} seconds...")
                time.sleep(delay)
                delay *= 2  # Exponential backoff
            else:
                print(f"Error fetching market rate: {e}")
                return None
        except Exception as e:
            print(f"Error fetching market rate: {e}")
            return None
    
    print("  Failed to fetch market rate after several retries.")
    return None

def get_quotes(region, fiat_currency, amount, payment_method, crypto_currency_id):
    """
    Fetches crypto quotes for a given region, fiat currency, amount, and crypto currency.
    """
    payment_method_id = f"/payments/{payment_method}"
    wallet_address = ""  # This can be any valid address

    url = (
        f"https://on-ramp.api.cx.metamask.io/providers/all/quote?"
        f"regionId=%2Fregions%2F{region.lower()}"
        f"&cryptoCurrencyId={requests.utils.quote(crypto_currency_id)}"
        f"&fiatCurrencyId=%2Fcurrencies%2Ffiat%2F{fiat_currency.lower()}"
        f"&amount={amount}"
        f"&paymentMethodId%5B0%5D={requests.utils.quote(payment_method_id)}"
        f"&walletAddress={wallet_address}"
        f"&sdk=2.1.8"
        f"&context=browser"
        f"&keys="
    )

    response = requests.get(url)
    response.raise_for_status()  # Raise an exception for bad status codes
    return response.json()

from google.cloud import bigquery

# --- Configuration ---
PRICE_RANGE = [30, *range(100, 30000, 1000)]
PROJECT_ID = "unbiased-reporting"
DATASET_ID = "articles"
TABLE_ID = "crypto_quotes"
# --- End Configuration ---

def get_bigquery_client():
    """Returns an authenticated BigQuery client."""
    return bigquery.Client(project=PROJECT_ID)

def create_bigquery_dataset_if_not_exists(client):
    """Creates the BigQuery dataset if it doesn't exist."""
    dataset_ref = client.dataset(DATASET_ID)
    try:
        client.get_dataset(dataset_ref)
    except Exception:
        client.create_dataset(dataset_ref)
        print(f"Created dataset {PROJECT_ID}.{DATASET_ID}")

def create_bigquery_table_if_not_exists(client):
    """Creates the BigQuery table if it doesn't exist."""
    table_ref = client.dataset(DATASET_ID).table(TABLE_ID)
    schema = [
        bigquery.SchemaField("Timestamp", "TIMESTAMP"),
        bigquery.SchemaField("Amount", "FLOAT"),
        bigquery.SchemaField("FiatCurrency", "STRING"),
        bigquery.SchemaField("CryptoCurrency", "STRING"),
        bigquery.SchemaField("Region", "STRING"),
        bigquery.SchemaField("PaymentMethod", "STRING"),
        bigquery.SchemaField("Provider", "STRING"),
        bigquery.SchemaField("Rank", "INTEGER"),
        bigquery.SchemaField("AmountOut", "FLOAT"),
        bigquery.SchemaField("ExchangeRate", "FLOAT"),
        bigquery.SchemaField("MarketRate", "FLOAT"),  # Added: Real market rate
        bigquery.SchemaField("ExpectedAmountOut", "FLOAT"),  # Added: Expected crypto based on market rate
        bigquery.SchemaField("Spread", "FLOAT"),
        bigquery.SchemaField("SpreadPercentage", "FLOAT"),
        bigquery.SchemaField("NetworkFee", "FLOAT"),
        bigquery.SchemaField("ProviderFee", "FLOAT"),
        bigquery.SchemaField("ExtraFee", "FLOAT"),
        bigquery.SchemaField("TotalExplicitFee", "FLOAT"),  # Renamed from TotalFee
        bigquery.SchemaField("TotalFeeIncludingSpread", "FLOAT"),  # Added: Total fee including spread
        bigquery.SchemaField("TotalFeePercentage", "FLOAT"),  # Now includes spread
    ]
    table = bigquery.Table(table_ref, schema=schema)
    try:
        client.get_table(table_ref)
    except Exception:
        client.create_table(table)
        print(f"Created table {PROJECT_ID}.{DATASET_ID}.{TABLE_ID}")

def append_to_bigquery(client, rows):
    """Appends rows to the BigQuery table."""
    if not rows:
        return
    table_ref = client.dataset(DATASET_ID).table(TABLE_ID)
    errors = client.insert_rows_json(table_ref, rows)
    if errors:
        print(f"Encountered errors while inserting rows: {errors}")

def main():
    """
    Main function to get and print quotes for different regions.
    """
    bq_client = get_bigquery_client()
    create_bigquery_dataset_if_not_exists(bq_client)
    create_bigquery_table_if_not_exists(bq_client)

    regions = {
        "de": "EUR",
        "gb": "GBP",
        "us-va": "USD",  # US region is state-specific, e.g., us-va for Virginia
    }

    crypto_currencies = {
        "ETH (Mainnet)": "/currencies/crypto/1/0x0000000000000000000000000000000000000000",
        "USDT (Ethereum)": "/currencies/crypto/1/0xdac17f958d2ee523a2206206994597c13d831ec7",
        "USDT (BNB Chain)": "/currencies/crypto/56/0x55d398326f99059ff775485246999027b3197955",
    }

    payment_methods_by_region = {
        "de": [
            "sepa-bank-transfer",
            "rev-pay",
            "debit-credit-card",
            "paypal",
            "binance-p2p",
        ],
        "gb": [
            "debit-credit-card",
            "gbp-bank-transfer",
            "rev-pay",
            "paypal"
        ],
        "us-va": [
            "venmo",
            "debit-credit-card",
            "paypal",
            "instant-bank-transfer",
        ],
    }

    # Cache market rates to avoid excessive API calls
    market_rates_cache = {}

    for amount in PRICE_RANGE:
        print(f"Fetching quotes for amount: {amount}")
        for crypto_name, crypto_id in crypto_currencies.items():
            for region_code, currency_code in regions.items():
                # Get market rate for this crypto/fiat pair (cache it)
                cache_key = f"{crypto_id}_{currency_code}"
                if cache_key not in market_rates_cache:
                    market_rate = get_market_rate(crypto_id, currency_code)
                    if market_rate:
                        market_rates_cache[cache_key] = market_rate
                        print(f"  Market rate for {crypto_name} in {currency_code}: {market_rate:.2f}")
                    else:
                        print(f"  Warning: Could not fetch market rate for {crypto_name} in {currency_code}")
                        market_rates_cache[cache_key] = None
                
                market_rate = market_rates_cache[cache_key]
                
                rows_to_insert = []
                payment_methods = payment_methods_by_region.get(region_code, [])
                for payment_method in payment_methods:
                    try:
                        quotes_data = get_quotes(
                            region_code, currency_code, amount, payment_method, crypto_id
                        )
                        if quotes_data.get("success"):
                            for rank, item in enumerate(quotes_data["success"], 1):
                                provider_name = item.get("providerInfo", {}).get("name", "N/A")
                                quote = item.get("quote", {})
                                amount_in = quote.get("amountIn")
                                amount_out = quote.get("amountOut")
                                exchange_rate = quote.get("exchangeRate")
                                network_fee = quote.get("networkFee", 0)
                                provider_fee = quote.get("providerFee", 0)
                                extra_fee = quote.get("extraFee", 0)

                                # Calculate expected amount out based on market rate
                                expected_amount_out = 0
                                spread = 0
                                spread_percentage = 0
                                spread_in_fiat = 0
                                
                                if market_rate and market_rate > 0 and amount_in:
                                    # Calculate how much crypto we should get at market rate
                                    # after deducting explicit fees
                                    amount_after_fees = amount_in - network_fee - provider_fee - extra_fee
                                    expected_amount_out = amount_after_fees / market_rate
                                    
                                    # Spread is the difference between expected and actual crypto received
                                    if amount_out:
                                        spread = expected_amount_out - amount_out
                                        spread_percentage = (spread / expected_amount_out) * 100 if expected_amount_out > 0 else 0
                                        # Convert spread back to fiat value
                                        spread_in_fiat = spread * market_rate

                                # Total explicit fees (network + provider + extra)
                                total_explicit_fee = network_fee + provider_fee + extra_fee
                                
                                # Total fee including the hidden spread
                                total_fee_including_spread = total_explicit_fee + spread_in_fiat
                                
                                # Total fee percentage (including spread) relative to input amount
                                total_fee_percentage = (total_fee_including_spread / amount_in) * 100 if amount_in > 0 else 0

                                rows_to_insert.append({
                                    "Timestamp": datetime.now().isoformat(),
                                    "Amount": amount_in,
                                    "FiatCurrency": currency_code,
                                    "CryptoCurrency": crypto_name,
                                    "Region": region_code.upper(),
                                    "PaymentMethod": payment_method,
                                    "Provider": provider_name,
                                    "Rank": rank,
                                    "AmountOut": amount_out,
                                    "ExchangeRate": exchange_rate,
                                    "MarketRate": market_rate,
                                    "ExpectedAmountOut": expected_amount_out,
                                    "Spread": spread,
                                    "SpreadPercentage": spread_percentage,
                                    "NetworkFee": network_fee,
                                    "ProviderFee": provider_fee,
                                    "ExtraFee": extra_fee,
                                    "TotalExplicitFee": total_explicit_fee,
                                    "TotalFeeIncludingSpread": total_fee_including_spread,
                                    "TotalFeePercentage": total_fee_percentage
                                })
                        time.sleep(0.1) # Small delay to be nice to the API
                    except requests.exceptions.RequestException as e:
                        print(f"    Error fetching quotes for {payment_method}: {e}")
                        pass # Ignore errors for now
                
                if rows_to_insert:
                    print(f"  Inserting {len(rows_to_insert)} rows for {currency_code} in {region_code.upper()}...")
                    append_to_bigquery(bq_client, rows_to_insert)

    print(f"Scraping complete. Data saved to BigQuery table {PROJECT_ID}.{DATASET_ID}.{TABLE_ID}")

if __name__ == "__main__":
    main()