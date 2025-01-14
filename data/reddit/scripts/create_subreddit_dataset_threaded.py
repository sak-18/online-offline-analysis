import os
import json
import requests
import zstandard as zstd
from datetime import datetime, timedelta
from collections import defaultdict
import pandas as pd
from tqdm import tqdm
from concurrent.futures import ProcessPoolExecutor, as_completed
import logging

# Configure logging
logging.basicConfig(
    filename="processing.log",  # Log to a file
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
console = logging.StreamHandler()
console.setLevel(logging.INFO)
logging.getLogger().addHandler(console)

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
    if os.path.exists(CHECKPOINT_FILE):
        with open(CHECKPOINT_FILE, "r") as f:
            return set(tuple(entry) for entry in json.load(f))
    return set()


def save_checkpoint(completed_tasks):
    with open(CHECKPOINT_FILE, "w") as f:
        json.dump(list(completed_tasks), f)


def download_file(url, dest_path):
    parent_dir = os.path.dirname(dest_path)
    os.makedirs(parent_dir, exist_ok=True)

    if os.path.exists(dest_path):
        logging.info(f"File already exists: {dest_path}")
        return

    logging.info(f"Downloading {url}...")
    response = requests.get(url, stream=True)
    if response.status_code == 200:
        with open(dest_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=1024):
                f.write(chunk)
        logging.info(f"Saved to {dest_path}.")
    else:
        logging.error(f"Failed to download {url}. Status code: {response.status_code}")


def read_zst_file(file_path):
    data = []
    with open(file_path, 'rb') as f:
        dctx = zstd.ZstdDecompressor()
        stream_reader = dctx.stream_reader(f)
        for chunk in iter(lambda: stream_reader.read(1024 * 1024), b""):
            text_stream = chunk.decode('utf-8')
            for line in text_stream.splitlines():
                try:
                    data.append(json.loads(line))
                except json.JSONDecodeError:
                    continue
    return data


def filter_posts_and_comments(submissions, comments, search_terms, start_date, end_date):
    start_timestamp = int(start_date.timestamp())
    end_timestamp = int(end_date.timestamp())
    filtered_submissions = []
    comments_by_parent = defaultdict(list)

    for submission in submissions:
        created_utc = submission.get("created_utc", 0)
        try:
            created_utc = int(created_utc)
        except (ValueError, TypeError):
            continue

        if start_timestamp <= created_utc <= end_timestamp:
            title = submission.get("title", "").lower()
            body = submission.get("selftext", "").lower()
            if any(term.lower() in title or term.lower() in body for term in search_terms):
                filtered_submissions.append(submission)

    for comment in comments:
        comments_by_parent[comment.get("parent_id", "")].append(comment)

    for submission in filtered_submissions:
        submission_id = f"t3_{submission['id']}"
        submission["comments"] = comments_by_parent.get(submission_id, [])

    return filtered_submissions


def save_filtered_posts(posts, output_dir):
    with open(os.path.join(output_dir, "filtered_posts.json"), "w", encoding="utf-8") as f:
        json.dump(posts, f, indent=2)


def process_community(disaster_name, fips, community_name, subreddit, search_terms, start_date, end_date, completed_tasks, submissions_url, comments_url):
    task_key = (disaster_name, fips, subreddit)
    if task_key in completed_tasks:
        logging.info(f"Skipping already processed task: {task_key}")
        return task_key, 0

    submissions_file = os.path.join(CENTRAL_ZST_STORAGE, f"{subreddit}_submissions.zst")
    comments_file = os.path.join(CENTRAL_ZST_STORAGE, f"{subreddit}_comments.zst")

    download_file(submissions_url, submissions_file)
    download_file(comments_url, comments_file)

    submissions = read_zst_file(submissions_file)
    comments = read_zst_file(comments_file)

    filtered_posts = filter_posts_and_comments(submissions, comments, search_terms, start_date, end_date)

    if filtered_posts:
        output_dir = os.path.join(OUTPUT_DIR, disaster_name, str(fips), subreddit)
        os.makedirs(output_dir, exist_ok=True)
        save_filtered_posts(filtered_posts, output_dir)

    logging.info(f"Processed {len(filtered_posts)} posts for {task_key}")
    return task_key, len(filtered_posts)


def process_disasters():
    completed_tasks = load_checkpoint()

    disaster_metadata = pd.read_csv(DISASTER_METADATA_CSV)
    community_metadata = pd.read_csv(COMMUNITY_METADATA_CSV)
    community_metadata = community_metadata[community_metadata["Subreddit"].notna()]
    community_metadata = community_metadata[community_metadata["Subreddit"].str.strip() != ""]

    disaster_metadata["Begin Date"] = pd.to_datetime(disaster_metadata["Begin Date"], format="%Y%m%d")
    disaster_metadata["End Date"] = pd.to_datetime(disaster_metadata["End Date"], format="%Y%m%d")
    high_conf_disasters = disaster_metadata[disaster_metadata["SHELDUS_CLASSIFICATION_CONFIDENCE"] == "HIGH"]

    tasks = []
    for _, disaster in high_conf_disasters.iterrows():
        disaster_name = disaster["SHELDUS_Event_Name"]
        search_terms = eval(disaster["Search_Terms_High_Confidence"])
        start_date = disaster["Begin Date"] - timedelta(days=15)
        end_date = disaster["End Date"] + timedelta(days=15)

        relevant_communities = community_metadata[community_metadata["EvtName"] == disaster_name]
        for _, community in relevant_communities.iterrows():
            fips = community["FIPS"]
            community_name = community["County_Nam"]
            subreddit = community["Subreddit"].replace("/r/", "").strip()
            submissions_url = community["Submission"]
            comments_url = community["Comments L"]

            tasks.append(
                (
                    disaster_name,
                    fips,
                    community_name,
                    subreddit,
                    search_terms,
                    start_date,
                    end_date,
                    completed_tasks,
                    submissions_url,
                    comments_url,
                )
            )

    with ProcessPoolExecutor(max_workers=4) as executor:
        futures = [executor.submit(process_community, *task) for task in tasks]

        for future in tqdm(as_completed(futures), total=len(futures), desc="Communities Processed"):
            task_key, count = future.result()
            completed_tasks.add(task_key)
            save_checkpoint(completed_tasks)


if __name__ == "__main__":
    process_disasters()
