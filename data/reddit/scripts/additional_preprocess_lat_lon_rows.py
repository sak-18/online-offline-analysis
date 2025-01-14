import pandas as pd
import requests
import time
import re

def preprocess_location(row):
    """
    Cleans and standardizes location names and states.
    Args:
        row (pd.Series): A row with 'State' and 'Name' columns.
    Returns:
        tuple: Cleaned (state, city).
    """
    state = row["State"]
    city = row["Name"]
    
    # Replace regional descriptors
    if "California" in state:
        state = "California"
    city = re.sub(r"(Northern|Southern|Central) California", "", city).strip()
    
    # Remove extra descriptors like Metro Area, County, etc.
    city = re.sub(r"(.+?)(?: \(.*?\)| Metro Area| County| District| Greater Area| \(\d+\))", r"\1", city).strip()
    
    return state.strip(), city.strip()

def query_nominatim(city, state):
    """
    Queries the Nominatim API to geocode a location.
    Args:
        city (str): The city name.
        state (str): The state name.
    Returns:
        dict: Latitude, Longitude, and metadata or error.
    """
    url = "https://nominatim.openstreetmap.org/search"
    headers = {
        "User-Agent": "GeoProcessor/1.0 (svishnu6@asu.edu)"  # Replace with your app details
    }
    params = {"q": f"{city}, {state}, USA", "format": "json", "addressdetails": 1}
    
    try:
        response = requests.get(url, params=params, headers=headers, timeout=10)
        response.raise_for_status()
        results = response.json()
        if results:
            return {
                "latitude": results[0]["lat"],
                "longitude": results[0]["lon"],
                "display_name": results[0]["display_name"]
            }
    except requests.exceptions.RequestException as e:
        print(f"Error querying '{city}, {state}': {e}")
    return {"latitude": None, "longitude": None, "error": "No result found"}

def process_rows(input_csv, output_csv):
    """
    Processes rows, applies preprocessing rules, and geocodes locations.
    Args:
        input_csv (str): Path to the input CSV.
        output_csv (str): Path to save the enriched results.
    """
    # Load data
    df = pd.read_csv(input_csv)
    
    # Add columns for results
    df["Latitude"] = None
    df["Longitude"] = None
    df["Display Name"] = None
    
    for index, row in df.iterrows():
        print(f"Processing: {row['Name']}, {row['State']}")
        
        # Preprocess city and state
        state, city = preprocess_location(row)
        
        # Query Nominatim
        geocode_result = query_nominatim(city, state)
        df.at[index, "Latitude"] = geocode_result.get("latitude")
        df.at[index, "Longitude"] = geocode_result.get("longitude")
        df.at[index, "Display Name"] = geocode_result.get("display_name")
        
        # Comply with rate limits
        time.sleep(1.1)  # Ensure at least 1 request per second
    
    # Save results
    df.to_csv(output_csv, index=False)
    print(f"Enriched data saved to {output_csv}")

# Example usage
if __name__ == "__main__":
    input_csv = "../missing_lat_lon.csv"  # Replace with your failed rows file
    output_csv = "../filled_lat_lon.csv"  # Replace with output file
    process_rows(input_csv, output_csv)
