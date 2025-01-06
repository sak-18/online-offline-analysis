import os
import pandas as pd

# File paths
input_csv_path = "../SHELDUS_data/SHELDUS_combined_original.csv"  # Replace with your input file path
output_csv_path = "../SHELDUS_data/SHELDUS_combined.csv"  # Replace with your output file path

# Column mapping for shortened names
column_mapping = {
    "State Name": "State_Nam",
    "County Name": "County_Nam",
    "County FIPS": "FIPS",  # Change County_FIP to FIPS
    "CropDmg": "CropDmg",
    "CropDmg(ADJ 2017)": "CropDAdj",
    "CropDmgPerCapita(ADJ 2017)": "CropPCAdj",
    "PropertyDmg": "PropDmg",
    "PropertyDmg(ADJ 2017)": "PropDAdj",
    "PropertyDmgPerCapita(ADJ 2017)": "PropPCAdj",
    "Injuries": "Injuries",
    "InjuriesPerCapita": "InjurPC",
    "Fatalities": "Fatal",
    "FatalitiesPerCapita": "FatalPC",
    "Duration_Days": "DurDays",
    "Fatalities_Duration": "FatalDur",
    "Injuries_Duration": "InjurDur",
    "Property_Damage_Duration": "PropDur",
    "Crop_Damage_Duration": "CropDur",
    "Records": "Records",
    "event_name": "EvtName",
    "event_id": "EvtID",
}

# Read the CSV
df = pd.read_csv(input_csv_path)

# Rename columns
df.rename(columns=column_mapping, inplace=True)

# Clean the FIPS column (remove quotes and ensure as string)
if "FIPS" in df.columns:
    df["FIPS"] = df["FIPS"].astype(str).str.strip("'")

# Save the transformed CSV
df.to_csv(output_csv_path, index=False)

# Print confirmation
print(f"Transformed CSV saved to: {output_csv_path}")
