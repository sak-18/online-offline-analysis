import csv
import re

# Input and output file paths
input_file = "../location_reddit.md"  # Replace with your actual Markdown file path
output_csv = "../us_state_subreddits_with_r.csv"

# Regex patterns for headings and subreddit links
start_section = r"^####United States$"  # Matches the United States heading
state_pattern = r"^#####\s*(.+)$"  # Matches state headings (e.g., "#####Alabama")
subreddit_pattern = r"\[([^\]]+)\]\(/r/([^\)]+)\)"  # Matches subreddit links

# Data storage
data = []
current_state = None
parsing_states = False

# Parse the Markdown file
with open(input_file, "r", encoding="utf-8") as file:
    for line in file:
        line = line.strip()
        
        # Detect the start of the United States section
        if re.match(start_section, line):
            parsing_states = True
            continue
        
        if parsing_states:
            # Match state headings
            state_match = re.match(state_pattern, line)
            if state_match:
                # Update current state
                current_state = state_match.group(1)
                continue
            
            # If within a state, find subreddit links
            if current_state:
                subreddits = re.findall(subreddit_pattern, line)
                for name, subreddit in subreddits:
                    data.append((current_state, f"/r/{subreddit}", name))

# Write data to CSV
with open(output_csv, "w", newline="", encoding="utf-8") as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow(["State", "Subreddit", "Name"])
    writer.writerows(data)

print(f"US state subreddits have been saved to {output_csv}.")
