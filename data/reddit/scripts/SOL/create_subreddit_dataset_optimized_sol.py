import os
import argparse
import pandas as pd
from datetime import timedelta
from tqdm import tqdm
import json
import requests
import zstandard as zstd
from collections import defaultdict
import gc
import psutil  # To monitor memory usage

# Constants
CENTRAL_ZST_STORAGE = "/scratch/svishnu6/central_zst_storage"
DISASTER_METADATA_CSV = "../../../data/events-US-2017-metadata.csv"
COMMUNITY_METADATA_CSV = "../county_joined_subreddits.csv"
OUTPUT_DIR = "../reddit_data"
CHECKPOINT_FILE = "processed_log.json"

# Ensure required directories exist
os.makedirs(CENTRAL_ZST_STORAGE, exist_ok=True)
os.makedirs(OUTPUT_DIR, exist_ok=True)

def log_memory_snapshot(label="Memory Snapshot"):
    """Log current memory usage."""
    process = psutil.Process()
    mem_info = process.memory_info()
    print(f"[{label}] RSS: {mem_info.rss / (1024 * 1024):.2f} MB, VMS: {mem_info.vms / (1024 * 1024):.2f} MB")

def load_checkpoint():
    """Load the checkpoint file."""
    if os.path.exists(CHECKPOINT_FILE):
        with open(CHECKPOINT_FILE, "r") as f:
            return set(tuple(entry) for entry in json.load(f))
    return set()

def save_checkpoint(completed_tasks):
    """Save the checkpoint file."""
    with open(CHECKPOINT_FILE, "w") as f:
        json.dump(list(completed_tasks), f)

def download_file(url, dest_path):
    """Download a file from a URL and save it locally."""
    parent_dir = os.path.dirname(dest_path)
    os.makedirs(parent_dir, exist_ok=True)
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
    """Read a .zst file and yield JSON objects line by line."""
    with open(file_path, 'rb') as f:
        dctx = zstd.ZstdDecompressor()
        with dctx.stream_reader(f) as stream_reader:
            buffer = b""
            while True:
                chunk = stream_reader.read(16384)  # Read in 16KB chunks
                if not chunk:
                    break
                buffer += chunk
                while b"\n" in buffer:
                    line, buffer = buffer.split(b"\n", 1)
                    try:
                        yield json.loads(line.decode('utf-8'))
                    except json.JSONDecodeError:
                        continue

def filter_posts_and_comments(submissions, comments, search_terms, start_date, end_date):
    """Filter posts and comments based on disaster metadata."""
    start_timestamp = int(start_date.timestamp())
    end_timestamp = int(end_date.timestamp())
    filtered_submissions = []

    for submission in submissions:
        created_utc = submission.get("created_utc", 0)
        try:
            created_utc = int(created_utc)
        except (ValueError, TypeError):
            continue
        if not (start_timestamp <= created_utc <= end_timestamp):
            continue
        title = submission.get("title", "").lower()
        body = submission.get("selftext", "").lower()
        if any(term.lower() in title or term.lower() in body for term in search_terms):
            filtered_submissions.append(submission)

    comments_by_parent = defaultdict(list)
    for comment in comments:
        parent_id = comment.get("parent_id", "")
        comments_by_parent[parent_id].append(comment)

    for submission in filtered_submissions:
        submission_id = f"t3_{submission['id']}"
        submission["comments"] = comments_by_parent.get(submission_id, [])

    return filtered_submissions

def save_filtered_posts(posts, output_dir):
    """Save filtered posts to individual JSON files."""
    for post in posts:
        post_id = post["id"]
        output_file = os.path.join(output_dir, f"{post_id}.json")
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(post, f, indent=2)

def process_disaster(disaster_name):
    """Process Reddit data for a single disaster."""
    completed_tasks = load_checkpoint()
    disaster_metadata = pd.read_csv(DISASTER_METADATA_CSV)
    community_metadata = pd.read_csv(COMMUNITY_METADATA_CSV)

    # Convert date columns
    disaster_metadata["Begin Date"] = pd.to_datetime(disaster_metadata["Begin Date"], format="%Y%m%d")
    disaster_metadata["End Date"] = pd.to_datetime(disaster_metadata["End Date"], format="%Y%m%d")

    # Filter disaster by name
    disaster = disaster_metadata[disaster_metadata["SHELDUS_Event_Name"] == disaster_name]
    if disaster.empty:
        raise ValueError(f"No disaster found with name: {disaster_name}")

    disaster = disaster.iloc[0]
    search_terms = eval(disaster["Search_Terms_High_Confidence"])
    start_date = disaster["Begin Date"] - timedelta(days=15)
    end_date = disaster["End Date"] + timedelta(days=15)

    # Filter relevant communities
    relevant_communities = community_metadata[community_metadata["EvtName"] == disaster_name]
    if relevant_communities.empty:
        print(f"No relevant communities found for disaster: {disaster_name}.")
        return

    # Process each community
    for i, (_, community) in enumerate(tqdm(relevant_communities.iterrows(), total=len(relevant_communities), desc=f"Communities ({disaster_name})")):
        fips = community["FIPS"]
        subreddit = community["Subreddit"]

        # Handle missing or NaN subreddit values
        if pd.isna(subreddit):
            print(f"Skipping community with missing subreddit (FIPS: {fips})")
            continue

        subreddit = subreddit.replace("/r/", "").strip()
        task_key = (disaster_name, fips, subreddit)
        if task_key in completed_tasks:
            print(f"Skipping already processed task: {task_key}")
            continue

        submissions_url = community["Submission"]
        comments_url = community["Comments L"]
        submissions_file = os.path.join(CENTRAL_ZST_STORAGE, f"{subreddit}_submissions.zst")
        comments_file = os.path.join(CENTRAL_ZST_STORAGE, f"{subreddit}_comments.zst")
        download_file(submissions_url, submissions_file)
        download_file(comments_url, comments_file)

        # Process files in chunks
        submissions = read_zst_file(submissions_file)
        comments = read_zst_file(comments_file)
        filtered_posts = filter_posts_and_comments(submissions, comments, search_terms, start_date, end_date)

        if not filtered_posts:
            del submissions, comments  # Free memory
            gc.collect()  # Trigger garbage collection
            continue

        output_dir = os.path.join(OUTPUT_DIR, disaster_name, str(fips), subreddit)
        os.makedirs(output_dir, exist_ok=True)
        save_filtered_posts(filtered_posts, output_dir)

        # Free memory after processing each community
        del submissions, comments, filtered_posts  # Explicitly delete objects
        gc.collect()  # Trigger garbage collection

        # Log memory usage occasionally
        if i % 10 == 0:
            log_memory_snapshot(f"After processing {i+1} communities")

        # Update checkpoint
        completed_tasks.add(task_key)
        save_checkpoint(completed_tasks)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process Reddit data for a specific disaster.")
    parser.add_argument("--disaster_name", type=str, required=True, help="The SHELDUS_Event_Name of the disaster to process.")
    args = parser.parse_args()
    process_disaster(args.disaster_name)
