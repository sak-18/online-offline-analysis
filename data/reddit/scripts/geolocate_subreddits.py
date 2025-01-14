import pandas as pd
from geopy.geocoders import Nominatim
import requests
import re
import time


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


def query_geopy(city, state):
    """
    Get latitude, longitude, and additional metadata using Geopy.
    Args:
        city (str): The city name.
        state (str): The state name.
    Returns:
        dict: Dictionary with latitude, longitude, and metadata.
    """
    geolocator = Nominatim(user_agent="geopy_mapper")
    try:
        location = geolocator.geocode(f"{city}, {state}, USA")
        if location:
            return {
                "latitude": location.latitude,
                "longitude": location.longitude,
                "metadata": location.raw
            }
        return {"latitude": None, "longitude": None, "metadata": None}
    except Exception as e:
        return {"latitude": None, "longitude": None, "metadata": str(e)}


def query_nominatim(city, state):
    """
    Queries the Nominatim API directly via requests.
    Args:
        city (str): The city name.
        state (str): The state name.
    Returns:
        dict: Latitude, Longitude, and metadata or error.
    """
    url = "https://nominatim.openstreetmap.org/search"
    headers = {
        "User-Agent": "GeoProcessor/1.0 (your_email@example.com)"  # Replace with your app details
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


def enrich_csv(input_file, output_file):
    """
    Enrich the CSV with Geopy or Nominatim data (latitude, longitude, metadata).
    Args:
        input_file (str): Path to the input CSV file.
        output_file (str): Path to save the enriched CSV file.
    """
    # Load the CSV
    df = pd.read_csv(input_file)
    
    # Initialize columns if they don't exist
    if "Latitude" not in df.columns:
        df["Latitude"] = None
    if "Longitude" not in df.columns:
        df["Longitude"] = None
    if "Metadata" not in df.columns:
        df["Metadata"] = None
    
    # Process rows
    for index, row in df.iterrows():
        # Skip rows that already have latitude and longitude
        if pd.notna(row["Latitude"]) and pd.notna(row["Longitude"]):
            print(f"Skipping: {row['Name']}, {row['State']} (already geolocated)")
            continue
        
        print(f"Processing: {row['Name']}, {row['State']}")
        
        # Preprocess city and state
        state, city = preprocess_location(row)
        
        # First try Geopy
        geopy_data = query_geopy(city, state)
        if geopy_data["latitude"] and geopy_data["longitude"]:
            df.at[index, "Latitude"] = geopy_data["latitude"]
            df.at[index, "Longitude"] = geopy_data["longitude"]
            df.at[index, "Metadata"] = geopy_data["metadata"]
            print(f"Geopy success: {geopy_data}")
        else:
            # Fallback to direct Nominatim query
            nominatim_data = query_nominatim(city, state)
            df.at[index, "Latitude"] = nominatim_data["latitude"]
            df.at[index, "Longitude"] = nominatim_data["longitude"]
            df.at[index, "Metadata"] = nominatim_data.get("display_name", "Error")
            print(f"Nominatim fallback: {nominatim_data}")
        
        # Rate limit
        time.sleep(1.1)  # Ensure at least 1 request per second
    
    # Save enriched CSV
    df.to_csv(output_file, index=False)
    print(f"Enriched CSV saved to {output_file}")


# Example usage
if __name__ == "__main__":
    input_csv = "../geolocated_subreddits.csv"  # Replace with your input file name
    output_csv = "../geolocated_subreddits_filled.csv"  # Replace with your output file name
    enrich_csv(input_csv, output_csv)
