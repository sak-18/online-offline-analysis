import pandas as pd

def check_and_dump_missing_lat_lon(input_csv, output_csv):
    """
    Check how many rows in the CSV have missing Latitude or Longitude,
    print the counts, and save those rows to a separate file.
    
    Args:
        input_csv (str): Path to the input CSV file.
        output_csv (str): Path to save rows with missing Latitude or Longitude.
        
    Returns:
        None
    """
    # Load the CSV
    df = pd.read_csv(input_csv)
    
    # Check for empty Latitude and Longitude
    missing_lat = df['Latitude'].isna().sum()
    missing_lon = df['Longitude'].isna().sum()
    
    # Rows where both Latitude and Longitude are missing
    missing_both = df[df['Latitude'].isna() & df['Longitude'].isna()]
    
    # Print summary
    print(f"Total rows: {len(df)}")
    print(f"Rows with missing Latitude: {missing_lat}")
    print(f"Rows with missing Longitude: {missing_lon}")
    print(f"Rows with both Latitude and Longitude missing: {len(missing_both)}")
    print(f"Percentage with missing Latitude or Longitude: {len(missing_both) / len(df) * 100:.2f}%")
    
    # Save rows with missing Latitude/Longitude to a new CSV
    missing_both.to_csv(output_csv, index=False)
    print(f"Saved rows with missing Latitude/Longitude to {output_csv}")

# Example usage
if __name__ == "__main__":
    input_csv = "../geolocated_subreddits_filled.csv"  # Replace with your enriched CSV file name
    output_csv = "../missing_lat_lon.csv"       # Replace with your desired output file name
    check_and_dump_missing_lat_lon(input_csv, output_csv)
