import os

# Define the folder containing the ZIP files
folder_path = "../"

# Loop through all files in the folder
for file_name in os.listdir(folder_path):
    # Check if the file is a ZIP file
    if file_name.endswith(".ZIP"):
        file_path = os.path.join(folder_path, file_name)
        # Remove the ZIP file
        os.remove(file_path)
        print(f"Deleted: {file_path}")

