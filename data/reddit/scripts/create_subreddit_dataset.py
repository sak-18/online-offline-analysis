import os
import json
import requests
import zstandard as zstd
from datetime import datetime, timedelta
from collections import defaultdict
import pandas as pd
from tqdm import tqdm

# Constants
CENTRAL_ZST_STORAGE = "../central_zst_storage"
DISASTER_METADATA_CSV = "../../../data/events-US-2017-metadata.csv"
COMMUNITY_METADATA_CSV = "../county_joined_subreddits.csv"
OUTPUT_DIR = "../reddit_data"
CHECKPOINT_FILE = "../processed_log.json"

# Ensure required directories exist
os.makedirs(CENTRAL_ZST_STORAGE, exist_ok=True)
os.makedirs(OUTPUT_DIR, exist_ok=True)


def load_checkpoint():
    """Load the checkpoint file."""
    if os.path.exists(CHECKPOINT_FILE):
        with open(CHECKPOINT_FILE, "r") as f:
            return set(tuple(entry) for entry in json.load(f))  # Convert list of lists to set of tuples
    return set()


def save_checkpoint(completed_tasks):
    """Save the checkpoint file."""
    with open(CHECKPOINT_FILE, "w") as f:
        json.dump(list(completed_tasks), f)  # Convert set of tuples back to list of lists


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


def read_zst_file(file_path):
    """Read a .zst file and return a list of JSON objects."""
    data = []
    with open(file_path, 'rb') as f:
        dctx = zstd.ZstdDecompressor()
        stream_reader = dctx.stream_reader(f)
        text_stream = stream_reader.read().decode('utf-8')
        for line in text_stream.splitlines():
            try:
                data.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    return data


def filter_posts_and_comments(submissions, comments, search_terms, start_date, end_date):
    """Filter posts and their comments based on disaster metadata."""
    # Convert date range to timestamps
    start_timestamp = int(start_date.timestamp())  # Use .timestamp() directly
    end_timestamp = int(end_date.timestamp())      # Use .timestamp() directly

    # Filter submissions
    filtered_submissions = []
    for submission in submissions:
        created_utc = submission.get("created_utc", 0)

        # Convert created_utc to an integer (default to 0 if conversion fails)
        try:
            created_utc = int(created_utc)
        except (ValueError, TypeError):
            print(f"Invalid created_utc value: {submission.get('created_utc')}. Skipping.")
            continue

        if not (start_timestamp <= created_utc <= end_timestamp):
            continue

        title = submission.get("title", "").lower()
        body = submission.get("selftext", "").lower()
        if any(term.lower() in title or term.lower() in body for term in search_terms):
            filtered_submissions.append(submission)

    # Map comments by parent ID
    comments_by_parent = defaultdict(list)
    for comment in comments:
        parent_id = comment.get("parent_id", "")
        comments_by_parent[parent_id].append(comment)

    # Attach child comments to each filtered submission
    for submission in filtered_submissions:
        submission_id = f"t3_{submission['id']}"
        submission["comments"] = comments_by_parent.get(submission_id, [])

    return filtered_submissions


def save_filtered_posts(posts, output_dir):
    """Save filtered posts as individual JSON files."""
    for post in posts:
        post_id = post["id"]
        output_file = os.path.join(output_dir, f"{post_id}.json")
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(post, f, indent=2)


def process_disasters():
    """Process each disaster and organize filtered posts."""
    # Load checkpoint
    completed_tasks = load_checkpoint()

    # Load metadata
    disaster_metadata = pd.read_csv(DISASTER_METADATA_CSV)
    community_metadata = pd.read_csv(COMMUNITY_METADATA_CSV)

    # Convert date columns to datetime
    disaster_metadata["Begin Date"] = pd.to_datetime(disaster_metadata["Begin Date"], format="%Y%m%d")
    disaster_metadata["End Date"] = pd.to_datetime(disaster_metadata["End Date"], format="%Y%m%d")
    high_conf_disasters = disaster_metadata[disaster_metadata["SHELDUS_CLASSIFICATION_CONFIDENCE"] == "HIGH"]

    for _, disaster in tqdm(high_conf_disasters.iterrows(), total=len(high_conf_disasters), desc="Disasters"):
        disaster_name = disaster["SHELDUS_Event_Name"]
        search_terms = eval(disaster["Search_Terms_High_Confidence"])

        # Extend date range by 15 days before and after
        start_date = disaster["Begin Date"] - timedelta(days=15)
        end_date = disaster["End Date"] + timedelta(days=15)

        print(f"Processing disaster: {disaster_name}")
        print(f"Search terms used: {search_terms}")
        print(f"Date range: {start_date.date()} to {end_date.date()} (extended by 15 days)")

        # Filter community metadata for the current disaster
        relevant_communities = community_metadata[
            community_metadata["EvtName"] == disaster_name
        ]

        if relevant_communities.empty:
            print(f"No relevant communities found for disaster: {disaster_name}.")
            continue

        for _, community in tqdm(relevant_communities.iterrows(), total=len(relevant_communities), desc=f"Communities ({disaster_name})"):
            fips = community["FIPS"]
            community_name = community["County_Nam"]
            subreddit = community["Subreddit"].replace("/r/", "").strip()

            # Check if task is already completed
            task_key = (disaster_name, fips, subreddit)
            if task_key in completed_tasks:
                print(f"Skipping already processed task: {task_key}")
                continue

            submissions_url = community["Submission"]
            comments_url = community["Comments L"]

            # Central storage paths
            submissions_file = os.path.join(CENTRAL_ZST_STORAGE, f"{subreddit}_submissions.zst")
            comments_file = os.path.join(CENTRAL_ZST_STORAGE, f"{subreddit}_comments.zst")

            # Download missing files
            download_file(submissions_url, submissions_file)
            download_file(comments_url, comments_file)

            # Read Reddit data
            print(f"Loading data for subreddit: {subreddit}")
            submissions = read_zst_file(submissions_file)
            comments = read_zst_file(comments_file)

            # Filter posts
            filtered_posts = filter_posts_and_comments(
                submissions, comments, search_terms, start_date, end_date
            )

            print(f"Filtered {len(filtered_posts)} posts using search terms {search_terms} for subreddit {subreddit} in {community_name} (FIPS: {fips})")

            if not filtered_posts:
                print(f"No posts found for subreddit {subreddit} in {community_name} (FIPS: {fips}) for {disaster_name}.")
                continue

            # Save filtered posts
            output_dir = os.path.join(OUTPUT_DIR, disaster_name, str(fips), subreddit)
            os.makedirs(output_dir, exist_ok=True)
            save_filtered_posts(filtered_posts, output_dir)

            print(f"Saved {len(filtered_posts)} posts for subreddit {subreddit} in {community_name} (FIPS: {fips}) under {disaster_name}.")

            # Update checkpoint
            completed_tasks.add(task_key)
            save_checkpoint(completed_tasks)


if __name__ == "__main__":
    process_disasters()
