import time
import schedule
from datetime import datetime
import yfinance as yf
from xml.etree.ElementTree import Element, SubElement, ElementTree

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
    
    save_to_xml()  # Moved outside the loop to save once per refresh

def save_to_xml():
    """Save all commodity data to XML file."""
    try:
        root = Element('CommodityPriceData')
        SubElement(root, 'Generated').set('timestamp', datetime.now().isoformat())
        
        for ticker, data_list in commodity_data.items():
            commodity_elem = SubElement(root, 'Commodity', {'ticker': ticker, 'name': COMMODITIES[ticker]})
            for data in data_list:
                price_elem = SubElement(commodity_elem, 'PriceData', {
                    'timestamp': data['timestamp'].isoformat(),
                    'refresh_type': data['refresh_type']
                })
                SubElement(price_elem, 'CurrentPrice').text = str(data['price'])
                SubElement(price_elem, 'PreviousClose').text = str(data['previous_close'])
        
        ElementTree(root).write('commodity_prices.xml', encoding='utf-8', xml_declaration=True)
        print(f"Data saved to XML at {datetime.now().strftime('%H:%M:%S')}")
    except Exception as e:
        print(f"XML save error: {e}")

def load_existing_data():
    """Load existing data from XML file."""
    try:
        from xml.etree.ElementTree import parse
        tree = parse('commodity_prices.xml')
        for commodity in tree.findall('Commodity'):
            if (ticker := commodity.get('ticker')) in commodity_data:
                for price_data in commodity.findall('PriceData'):
                    commodity_data[ticker].append({
                        'timestamp': datetime.fromisoformat(price_data.get('timestamp')),
                        'price': float(price_data.find('CurrentPrice').text),
                        'previous_close': float(price_data.find('PreviousClose').text),
                        'refresh_type': price_data.get('refresh_type')
                    })
        print("Loaded existing data from XML")
    except FileNotFoundError:
        print("No existing XML file found, starting fresh")
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
        save_to_xml()
        print("\nPipeline stopped. Final data saved.")

if __name__ == "__main__":
    main()