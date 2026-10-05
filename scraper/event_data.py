"""Event data helpers, shared by the UploadPipeline and the archive refresh script.

Event data is a dict {file_id: {"last_seen": ..., "target_year": ...}} of the
documents already uploaded, where file_id is the Portail EE ctsFileId (as a string).

It is split in two:
- the archive (event_data/archive.json), committed in the repo and loaded from disk
- the live event data, stored on DocumentCloud, holding only the entries that are
  not archived yet (keeps the PATCH requests to DocumentCloud small)
"""

import json
from pathlib import Path

ARCHIVE_PATH = Path(__file__).parent.parent / "event_data" / "archive.json"

# Files that should never be scraped
IGNORED_FILE_IDS = {
    "142103",  # PDF > 500 MB, uploaded manually after resize
}


def normalize_event_data(event_data):
    """Returns event data keyed by file ID.

    Legacy keys (full download URLs ending with "ctsFileId=<file_id>") are converted
    to file IDs, values are kept as is.
    """

    if not event_data:
        return {}

    return {
        key.rsplit("=", 1)[1] if key.startswith("https://") else key: value
        for key, value in event_data.items()
    }


def load_archive(path=ARCHIVE_PATH):
    """Loads the archived event data, or {} if there is no archive."""

    if not path.exists():
        return {}

    with open(path, "r") as file:
        return json.load(file)


def write_archive(event_data, path=ARCHIVE_PATH):
    """Writes the archived event data sorted by file ID, one entry per line."""

    path.parent.mkdir(exist_ok=True)

    lines = [
        f"{json.dumps(file_id)}: {json.dumps(event_data[file_id])}"
        for file_id in sorted(event_data, key=int)
    ]

    with open(path, "w") as file:
        file.write("{\n" + ",\n".join(lines) + "\n}\n")
