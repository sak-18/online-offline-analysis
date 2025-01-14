import json
import zstandard as zstd
from collections import defaultdict

def read_zst_file(file_path, max_lines=None):
    """
    Reads a .zst file and returns a list of JSON objects.
    """
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


def build_comment_tree(comments, root_id):
    """
    Builds a nested comment tree structure starting from a given root ID.
    """
    comments_by_parent = defaultdict(list)
    for comment in comments:
        parent_id = comment.get("parent_id", "")
        comments_by_parent[parent_id].append(comment)

    def add_children(parent_id):
        """Recursively attaches child comments."""
        children = comments_by_parent.get(parent_id, [])
        for child in children:
            # Recursively build replies for each child
            child["replies"] = add_children(f"t1_{child['id']}")
        return children

    return add_children(root_id)


def reconstruct_thread(submission_id, submissions_file, comments_file):
    """
    Reconstructs a thread for a given submission ID.
    """
    print(f"Reading submissions from: {submissions_file}")
    submissions = read_zst_file(submissions_file)

    print(f"Reading comments from: {comments_file}")
    comments = read_zst_file(comments_file)

    # Find the submission
    submission = next((s for s in submissions if s["id"] == submission_id), None)
    if not submission:
        print(f"Submission with ID {submission_id} not found.")
        return None

    print(f"Found submission: {submission['title']} (Author: {submission['author']})")

    # Filter comments belonging to this submission
    submission_comments = [c for c in comments if c.get("link_id") == f"t3_{submission_id}"]
    print(f"Found {len(submission_comments)} comments for submission ID {submission_id}.")

    # Build comment tree
    comment_tree = build_comment_tree(submission_comments, root_id=f"t3_{submission_id}")

    # Return structured JSON
    return {
        "submission": {
            "id": submission["id"],
            "title": submission["title"],
            "author": submission["author"],
            "selftext": submission.get("selftext", ""),
            "created_utc": submission.get("created_utc", 0),
            "url": submission.get("url", ""),
            "score": submission.get("score", 0),
            "num_comments": submission.get("num_comments", 0),
        },
        "comments": comment_tree
    }


def save_thread_as_json(thread, output_path):
    """
    Saves the reconstructed thread to a JSON file.
    """
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(thread, f, indent=2)
    print(f"Thread saved to: {output_path}")


# Main Execution
if __name__ == "__main__":
    # Paths to submissions and comments files
    submissions_file = "../Miami_submissions.zst"
    comments_file = "../Miami_comments.zst"

    # ID of the submission to reconstruct
    target_submission_id = "bpszs"

    # Output JSON file path
    output_json_file = "../thread_output.json"

    # Reconstruct the thread
    thread = reconstruct_thread(target_submission_id, submissions_file, comments_file)

    if thread:
        # Save the thread as JSON
        save_thread_as_json(thread, output_json_file)
