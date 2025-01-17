import pandas as pd

# Load the CSV files
state_subreddits_file = "../assets/us_state_subreddits_with_r.csv"
popular_subreddits_file = "../assets/extracted_eye_subreddits.csv"

# Read the data
state_subreddits = pd.read_csv(state_subreddits_file)
popular_subreddits = pd.read_csv(popular_subreddits_file)

# Standardize subreddit names for comparison
state_subreddits['Subreddit'] = state_subreddits['Subreddit'].str.replace("/r/", "", regex=False).str.strip().str.lower()
popular_subreddits['Subreddit'] = popular_subreddits['Subreddit'].str.replace("r/", "", regex=False).str.strip().str.lower()

# Perform exact match join
matches = pd.merge(
    state_subreddits,
    popular_subreddits,
    on='Subreddit',
    how='inner'
)

# Find subreddits from popular dataset missing in states dataset
missing_in_popular = popular_subreddits[~popular_subreddits['Subreddit'].isin(matches['Subreddit'])]

# Find subreddits from states dataset missing in popular dataset
missing_in_state = state_subreddits[~state_subreddits['Subreddit'].isin(matches['Subreddit'])]

# Save the results to CSV
matches.to_csv("../matched_subreddits.csv", index=False)
missing_in_state.to_csv("../missing_in_state.csv", index=False)
missing_in_popular.to_csv("../missing_in_popular.csv", index=False)

print("Matched subreddits saved to 'matched_subreddits.csv'.")
print("Subreddits missing in popular dataset saved to 'missing_in_state.csv'.")
print("Subreddits missing in state dataset saved to 'missing_in_popular.csv'.")
