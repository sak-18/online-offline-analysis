import zstandard as zstd
import json
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


def build_comment_tree(comments, root_id):
    """
    Organize comments into a tree structure based on the root ID.
    :param comments: List of comment dictionaries.
    :param root_id: The parent ID for top-level comments (e.g., t3_<submission_id>).
    :return: List of top-level comments with nested replies.
    """
    comments_by_parent = defaultdict(list)
    for comment in comments:
        parent_id = comment.get("parent_id", "")
        comments_by_parent[parent_id].append(comment)

    def add_children(parent_id):
        """Recursively attach children comments."""
        children = comments_by_parent.get(parent_id, [])
        for child in children:
            # Recursively build replies for each child
            child["replies"] = add_children(f"t1_{child['id']}")
        return children

    # Return top-level comments (those whose parent is the submission ID)
    return add_children(root_id)


def reconstruct_thread(submission_id, submissions_file, comments_file):
    """
    Reconstruct a thread for a specific submission.
    :param submission_id: The ID of the submission to reconstruct.
    :param submissions_file: Path to the submissions .zst file.
    :param comments_file: Path to the comments .zst file.
    :return: A dictionary containing the submission and its comments tree.
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

    # Combine submission with its comments
    return {
        "submission": submission,
        "comments": comment_tree
    }


def print_thread(thread, level=0):
    """Recursively print a thread in a tree structure."""
    if level == 0:
        print("\n=== Submission ===")
        print(f"Title: {thread['submission']['title']}")
        print(f"Author: {thread['submission']['author']}")
        print(f"Body: {thread['submission'].get('selftext', '')}\n")
        print("=== Comments ===")

    for comment in thread["comments"]:
        indent = " " * (level * 4)
        print(f"{indent}- {comment['author']}: {comment['body']}")
        if "replies" in comment and comment["replies"]:
            print_thread({"comments": comment["replies"]}, level + 1)


# Main Execution
if __name__ == "__main__":
    # Paths to submissions and comments files
    submissions_file = "../Miami_submissions.zst"
    comments_file = "../Miami_comments.zst"
    
    # ID of the submission to reconstruct
    target_submission_id = "bpszs"
    
    # Reconstruct the thread
    thread = reconstruct_thread(target_submission_id, submissions_file, comments_file)
    
    if thread:
        # Print the thread
        print_thread(thread)
