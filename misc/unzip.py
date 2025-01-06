import os
import zipfile

# Define the input folder with ZIP files and the output folder for renamed CSVs
input_folder = "../"
output_folder = "../"

# Create the output folder if it doesn't exist
os.makedirs(output_folder, exist_ok=True)

# Iterate through each ZIP file in the input folder
for zip_filename in os.listdir(input_folder):
    if zip_filename.endswith(".ZIP"):
        zip_path = os.path.join(input_folder, zip_filename)
        
        # Extract the ZIP file
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            extracted_files = zip_ref.namelist()
            zip_ref.extractall(output_folder)
        
        # Find and rename the CSV file
        base_name = os.path.splitext(zip_filename)[0]  # Get ZIP file name without extension
        for extracted_file in extracted_files:
            if extracted_file.endswith(".csv"):
                extracted_file_path = os.path.join(output_folder, extracted_file)
                new_csv_name = f"{base_name}.csv"
                new_csv_path = os.path.join(output_folder, new_csv_name)
                os.rename(extracted_file_path, new_csv_path)
                print(f"Renamed {extracted_file} to {new_csv_name}")
