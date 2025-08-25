import time
import schedule
from datetime import datetime
import yfinance as yf
import csv  # Changed from XML to CSV

COMMODITIES = {"GC=F": "Gold", "CL=F": "Crude_Oil", "SI=F": "Silver", "NG=F": "Natural_Gas", "ZC=F": "Corn"}
commodity_data = {ticker: [] for ticker in COMMODITIES}

def get_commodity_price(ticker):
    """Retrieve current price and previous close for given ticker."""
    try:
        info = yf.Ticker(ticker).info
        return (info.get("regularMarketPrice"), 
                info.get("regularMarketPreviousClose"), 
                datetime.now())
    except Exception as e:
        print(f"Error fetching {ticker}: {e}")
        return None, None, datetime.now()

def refresh_data(refresh_type):
    """Refresh price data for all commodities."""
    print(f"\n{'=' if refresh_type == 'daily' else '-'} {refresh_type.title()} Refresh: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} {'=' if refresh_type == 'daily' else '-'}")
    
    for ticker, name in COMMODITIES.items():
        price, prev_close, timestamp = get_commodity_price(ticker)
        if price is not None:
            commodity_data[ticker].append({
                'timestamp': timestamp, 'price': price, 
                'previous_close': prev_close, 'refresh_type': refresh_type
            })
            print(f"{name}: ${price} (Prev: ${prev_close})" + (" [DAILY]" if refresh_type == "daily" else ""))
    
    save_to_csv()  # Changed from save_to_xml()

def save_to_csv():
    """Save all commodity data to CSV file."""
    try:
        with open('commodity_prices.csv', 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            # Write header
            writer.writerow(['Ticker', 'Commodity', 'Timestamp', 'RefreshType', 'CurrentPrice', 'PreviousClose'])
            
            # Write data
            for ticker, data_list in commodity_data.items():
                for data in data_list:
                    writer.writerow([
                        ticker,
                        COMMODITIES[ticker],
                        data['timestamp'].isoformat(),
                        data['refresh_type'],
                        data['price'],
                        data['previous_close']
                    ])
        
        print(f"Data saved to CSV at {datetime.now().strftime('%H:%M:%S')}")
    except Exception as e:
        print(f"CSV save error: {e}")

def load_existing_data():
    """Load existing data from CSV file."""
    try:
        with open('commodity_prices.csv', 'r', newline='') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                ticker = row['Ticker']
                if ticker in commodity_data:
                    commodity_data[ticker].append({
                        'timestamp': datetime.fromisoformat(row['Timestamp']),
                        'price': float(row['CurrentPrice']),
                        'previous_close': float(row['PreviousClose']),
                        'refresh_type': row['RefreshType']
                    })
        print("Loaded existing data from CSV")
    except FileNotFoundError:
        print("No existing CSV file found, starting fresh")
    except Exception as e:
        print(f"Error loading data: {e}")

def main():
    """Main scheduler function."""
    load_existing_data()
    
    schedule.every().hour.do(lambda: refresh_data('hourly'))
    schedule.every().day.at("09:00").do(lambda: refresh_data('daily'))
    
    print(f"Commodity Pipeline Started! Tracking: {', '.join(COMMODITIES.values())}")
    print("Press Ctrl+C to stop\n")
    
    refresh_data('hourly')  # Initial refresh
    
    try:
        while True:
            schedule.run_pending()
            time.sleep(60)
    except KeyboardInterrupt:
        save_to_csv()  # Changed from save_to_xml()
        print("\nPipeline stopped. Final data saved.")

if __name__ == "__main__":
    main()