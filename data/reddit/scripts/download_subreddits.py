import os
import json
import requests
import pandas as pd
from tqdm import tqdm

# Constants
CENTRAL_ZST_STORAGE = "../central_zst_storage"
DISASTER_METADATA_CSV = "../../../data/events-US-2017-metadata.csv"
COMMUNITY_METADATA_CSV = "../county_joined_subreddits.csv"

# Ensure the central storage directory exists
os.makedirs(CENTRAL_ZST_STORAGE, exist_ok=True)

def download_file(url, dest_path):
    """Download a file from a URL and save it locally."""
    parent_dir = os.path.dirname(dest_path)
    os.makedirs(parent_dir, exist_ok=True)  # Ensure parent directory exists

    if os.path.exists(dest_path):
        print(f"File already exists: {dest_path}")
        return

    print(f"Downloading {url}...")
    response = requests.get(url, stream=True)
    if response.status_code == 200:
        with open(dest_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=1024):
                f.write(chunk)
        print(f"Saved to {dest_path}.")
    else:
        print(f"Failed to download {url}. Status code: {response.status_code}")

def download_relevant_subreddits():
    """Download all relevant subreddit .zst files for submissions and comments."""
    # Load metadata
    disaster_metadata = pd.read_csv(DISASTER_METADATA_CSV)
    community_metadata = pd.read_csv(COMMUNITY_METADATA_CSV)

    # Filter rows where Subreddit is not empty or NaN
    community_metadata = community_metadata[community_metadata["Subreddit"].notna()]
    community_metadata = community_metadata[community_metadata["Subreddit"].str.strip() != ""]

    # Filter for disasters with HIGH classification confidence
    high_conf_disasters = disaster_metadata[disaster_metadata["SHELDUS_CLASSIFICATION_CONFIDENCE"] == "HIGH"]

    relevant_communities = community_metadata[
        community_metadata["EvtName"].isin(high_conf_disasters["SHELDUS_Event_Name"])
    ]

    # Iterate over relevant communities and download files
    for _, community in tqdm(relevant_communities.iterrows(), total=len(relevant_communities), desc="Downloading Files"):
        subreddit = community["Subreddit"].replace("/r/", "").strip()
        submissions_url = community["Submission"]
        comments_url = community["Comments L"]

        # Paths to save files
        submissions_file = os.path.join(CENTRAL_ZST_STORAGE, f"{subreddit}_submissions.zst")
        comments_file = os.path.join(CENTRAL_ZST_STORAGE, f"{subreddit}_comments.zst")

        # Download files
        if pd.notna(submissions_url):
            download_file(submissions_url, submissions_file)
        if pd.notna(comments_url):
            download_file(comments_url, comments_file)

if __name__ == "__main__":
    download_relevant_subreddits()
