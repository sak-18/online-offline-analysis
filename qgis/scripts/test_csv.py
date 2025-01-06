import pandas as pd

# Define the CSV file path
csv_path = "../../data/offline/SHELDUS_data/SHELDUS_combined.csv"

# Load the CSV into a DataFrame
csv_data = pd.read_csv(csv_path)

# Check for duplicates in the 'COUNTY FIPS' column
duplicate_count = csv_data["County FIPS"].duplicated().sum()

# Display basic duplicate information
if duplicate_count > 0:
    print(f"Total duplicate rows based on 'County FIPS': {duplicate_count}")
    
    # Display duplicate rows
    duplicates = csv_data[csv_data["County FIPS"].duplicated(keep=False)]
    print("\nDuplicate rows:")
    print(duplicates)
    
    # Count occurrences of each duplicate value
    duplicate_summary = duplicates["County FIPS"].value_counts()
    print("\nOccurrences of each duplicate County FIPS:")
    print(duplicate_summary)
else:
    print("No duplicates found in the 'County FIPS' column.")
