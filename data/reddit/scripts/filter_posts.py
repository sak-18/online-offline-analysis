import zstandard as zstd
import json
from datetime import datetime
from collections import defaultdict


def read_zst_file(file_path, max_lines=None):
    """Read a .zst file and return a list of JSON objects."""
    data = []
    with open(file_path, 'rb') as f:
        dctx = zstd.ZstdDecompressor()
        stream_reader = dctx.stream_reader(f)
        text_stream = stream_reader.read().decode('utf-8')
        for i, line in enumerate(text_stream.splitlines()):
            if max_lines and i >= max_lines:
                break
            try:
                data.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    return data


def filter_submissions(submissions_file, search_terms, start_date, end_date):
    """
    Filter submissions based on search terms and date range, with counts for each term.
    """
    # Convert date range to timestamps
    start_timestamp = int(datetime.strptime(start_date, "%Y-%m-%d").timestamp())
    end_timestamp = int(datetime.strptime(end_date, "%Y-%m-%d").timestamp())
    
    # Read submissions from file
    submissions = read_zst_file(submissions_file)
    
    # Initialize counters for search terms
    term_counts = defaultdict(int)
    filtered = []

    for submission in submissions:
        # Get created_utc and ensure it's an integer
        created_utc = submission.get("created_utc")
        try:
            created_utc = int(created_utc)
        except (ValueError, TypeError):
            continue  # Skip if created_utc is invalid or missing

        # Check if submission falls within the date range
        if not (start_timestamp <= created_utc <= end_timestamp):
            continue
        
        # Check if any search term is in the title or body
        title = submission.get("title", "").lower()
        body = submission.get("selftext", "").lower()
        matched = False
        for term in search_terms:
            if term.lower() in title or term.lower() in body:
                term_counts[term] += 1
                matched = True

        if matched:
            filtered.append(submission)
    
    return filtered, term_counts


if __name__ == "__main__":
    # Example usage
    submissions_file = "../central_zst_storage/bayarea_submissions.zst"  # Path to the submissions file
    search_terms = ["storm", "tornado"]
    #search_terms = ["hurricane irma", "irma damage", "irma evacuation", "irma florida", "irma storm surge"]  # Keywords to search for
    start_date = "2017-01-01"  # Start of date range
    end_date = "2017-12-31"  # End of date range

    # Filter submissions
    filtered_submissions, term_counts = filter_submissions(submissions_file, search_terms, start_date, end_date)
    
    # Print results
    print(f"Found {len(filtered_submissions)} submissions matching the criteria.")
    print("\nNumber of matches for each search term:")
    for term, count in term_counts.items():
        print(f"- {term}: {count}")

    # Print filtered submissions
    print("\nFiltered Submissions:")
    for submission in filtered_submissions:
        print(f"ID: {submission['id']}, Title: {submission['title']}, Date: {datetime.utcfromtimestamp(submission['created_utc']).strftime('%Y-%m-%d')}")
