import os
import pandas as pd

# Specify the folder containing the disaster event CSVs
folder_path = '../SHELDUS_data'  # Replace with the actual path

# Get all CSV files in the folder
csv_files = [file for file in os.listdir(folder_path) if file.endswith('.csv')]

# Initialize an empty list to store DataFrames
dataframes = []

# Initialize a list to store event_name and event_id mappings
event_mapping = []

# Iterate over each CSV file
for i, file in enumerate(csv_files):
    event_name = os.path.splitext(file)[0]  # Extract the event name from the filename
    event_id = i + 1  # Create a unique ID for each disaster event
    
    # Load the CSV into a DataFrame
    df = pd.read_csv(os.path.join(folder_path, file))
    
    # Add the event_name and event_id columns
    df['event_name'] = event_name
    df['event_id'] = event_id
    
    # Append the DataFrame to the list
    dataframes.append(df)
    
    # Append the event_name and event_id to the mapping list
    event_mapping.append({'event_name': event_name, 'event_id': event_id})

# Concatenate all DataFrames into a single DataFrame
combined_df = pd.concat(dataframes, ignore_index=True)

# Save the combined DataFrame to a new CSV file
combined_output_file = os.path.join(folder_path, 'SHELDUS_combined.csv')
combined_df.to_csv(combined_output_file, index=False)

# Save the event_name and event_id mapping to a separate CSV
event_mapping_df = pd.DataFrame(event_mapping)
mapping_output_file = os.path.join(folder_path, 'event_name_id_mapping.csv')
event_mapping_df.to_csv(mapping_output_file, index=False)

print(f"Combined data saved to: {combined_output_file}")
print(f"Event name-ID mapping saved to: {mapping_output_file}")
