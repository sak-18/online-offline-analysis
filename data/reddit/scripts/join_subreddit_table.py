import pandas as pd

# Load the CSV files
state_subreddits_file = "../us_state_subreddits_with_r.csv"
popular_subreddits_file = "../extracted_eye_subreddits.csv"

# Read the data
state_subreddits = pd.read_csv(state_subreddits_file)
popular_subreddits = pd.read_csv(popular_subreddits_file)

# Standardize subreddit names for comparison
state_subreddits['Subreddit'] = state_subreddits['Subreddit'].str.strip().str.lower()
popular_subreddits['Subreddit'] = popular_subreddits['Subreddit'].str.strip().str.lower()

# Perform substring join
# Note: Find matches of subreddits in the state CSV from popular CSV. As state CSV has longer string with "/r".
matches = []
for _, popular_row in popular_subreddits.iterrows():
    popular_subreddit = popular_row['Subreddit']
    match = state_subreddits[state_subreddits['Subreddit'].str.contains(popular_subreddit, na=False)]
    if not match.empty:
        for _, state_row in match.iterrows():
            matches.append({
                'State': state_row['State'],
                'Subreddit': state_row['Subreddit'],  # Match with state_subreddits column
                'Name': state_row['Name'],
                'Submissions Link': popular_row['Submissions Link'],
                'Comments Link': popular_row['Comments Link']
            })

# Convert matches to DataFrame
matches_df = pd.DataFrame(matches)

# Find subreddits from popular dataset missing in states dataset
missing_in_state = state_subreddits[~state_subreddits['Subreddit'].isin(matches_df['Subreddit'])]

# Find subreddits from states dataset missing in popular dataset
missing_in_popular = popular_subreddits[~popular_subreddits['Subreddit'].isin(matches_df['Subreddit'])]

# Save the results to CSV
matches_df.to_csv("../matched_subreddits.csv", index=False)
missing_in_popular.to_csv("../missing_in_popular.csv", index=False)
missing_in_state.to_csv("../missing_in_state.csv", index=False)

print("Matched subreddits saved to 'matched_subreddits.csv'.")
print("Subreddits missing in popular dataset saved to 'missing_in_popular.csv'.")
print("Subreddits missing in state dataset saved to 'missing_in_state.csv'.")
