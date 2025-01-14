import re
import csv

# Load the HTML content
with open("../push_shift_archive_source.html", "r", encoding="utf-8") as file:
    html_content = file.read()

# Patterns for extracting rows and data
table_row_pattern = re.compile(r"<tr>(.*?)</tr>", re.DOTALL)
table_data_pattern = re.compile(r"<td.*?>(.*?)</td>", re.DOTALL)

# Find all table rows
rows = table_row_pattern.findall(html_content)
print(f"Extracting table rows...\nNumber of rows found: {len(rows)}")

parsed_rows = []

for idx, row in enumerate(rows):
    print(f"\nProcessing row {idx + 1}/{len(rows)}: {row[:100]}...")  # Print the first 100 characters for brevity

    # Find all cells in the current row
    cells = table_data_pattern.findall(row)
    print(f"  Found {len(cells)} cells: {cells}")

    # Clean the data inside each cell
    cleaned_cells = [re.sub(r"<.*?>", "", cell).strip() for cell in cells]
    print(f"  Cleaned cells: {cleaned_cells}")

    # Ensure the row has exactly 3 cells (Subreddit, Submissions, Comments)
    if len(cleaned_cells) == 3:
        parsed_rows.append(cleaned_cells)
    else:
        print(f"  Skipping row due to unexpected cell count: {len(cleaned_cells)}")

print(f"\nParsed {len(parsed_rows)} valid rows:")
for row in parsed_rows[:5]:  # Show only the first 5 rows for brevity
    print(row)

# Write to CSV
output_file = "../output.csv"
with open(output_file, "w", newline="", encoding="utf-8") as csv_file:
    writer = csv.writer(csv_file)
    writer.writerow(["Subreddit", "Submissions", "Comments"])  # Add headers
    writer.writerows(parsed_rows)  # Write the parsed rows

print(f"\nWriting to {output_file}...\nData has been extracted and saved to '{output_file}'.")
