#!/usr/bin/env python3


"""
update_dataset.py
-----------------
Convert XML files in a given folder into JSONL shards and upload them to a Hugging Face dataset.
Supports resumable uploads via upload_progress.json.

Usage:
  python update_dataset.py \
    --repo_id "vGassen/Dutch-Basisbestandwetten-Legislation-Laws" \
    --token   "$HF_TOKEN" \
    --data_dir "../data3" \
    [--shard_size 250] \
    [--force_remote]
"""
from __future__ import annotations
from dotenv import load_dotenv # type: ignore
load_dotenv()  # يقرأ .env



import os, glob, json, sys, time, tempfile, argparse
from typing import List
from huggingface_hub import HfApi, create_repo # type: ignore

# ---------- Default constants ---------- #
PROGRESS_FILE = "upload_progress.json"
SHARD_SIZE    = 500
MAX_RETRIES   = 5
BACKOFF       = 2.0  # seconds between retry attempts

# ---------- Helpers ---------- #
def list_xml(data_dir: str) -> List[str]:
    """Return sorted list of all .xml files under data_dir."""
    pattern = os.path.join(data_dir, "**", "*.xml")
    return sorted(glob.glob(pattern, recursive=True))

def read_xml(path: str) -> str:
    """Read an XML file as UTF-8 text (ignore decoding errors)."""
    with open(path, "rb") as f:
        return f.read().decode("utf-8", "ignore")

def build_jsonl(batch: List[str], data_dir: str) -> str:
    """Write a temporary JSONL shard from a list of XML file paths."""
    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".jsonl").name
    with open(tmp, "w", encoding="utf-8") as out:
        for fp in batch:
            rel = os.path.relpath(fp, data_dir).replace("\\", "/")
            rec = {"url": rel, "content": read_xml(fp), "source": "Basis Wettenbestand"}
            out.write(json.dumps(rec, ensure_ascii=False) + "\n")
    return tmp

def load_local_index() -> int:
    """Load last_index from PROGRESS_FILE, or 0 if missing/corrupt."""
    if os.path.exists(PROGRESS_FILE):
        try:
            return json.load(open(PROGRESS_FILE))["last_index"]
        except Exception:
            pass
    return 0

def save_local_index(i: int):
    """Save last_index to PROGRESS_FILE."""
    with open(PROGRESS_FILE, "w", encoding="utf-8") as f:
        json.dump({"last_index": i}, f)

def remote_index(api: HfApi, repo_id: str, token: str) -> int:
    """Find max index already on the remote dataset (based on shard filenames)."""
    files = api.list_repo_files(repo_id=repo_id, repo_type="dataset", token=token)
    shards = [f for f in files if f.startswith("shards/")]
    if not shards:
        return 0
    # filenames like shards/shard_000000_000250.jsonl
    ends = [int(f.split("_")[-1].split(".")[0]) for f in shards]
    return max(ends)

def upload_shard(api: HfApi, local: str, remote: str, repo_id: str, token: str) -> bool:
    """Upload a single shard with retry/backoff."""
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            api.upload_file(
                path_or_fileobj=local,
                path_in_repo   =remote,
                repo_id        =repo_id,
                repo_type      ="dataset",
                token          =token
            )
            return True
        except Exception as e:
            if attempt == MAX_RETRIES:
                print(f"Failed to upload {remote}: {e}", file=sys.stderr)
                return False
            wait = BACKOFF * attempt
            print(f"Error: {e} – retrying after {wait}s")
            time.sleep(wait)
    return False

# ---------- Main ---------- #
def main():
    p = argparse.ArgumentParser(description="Upload XML files as JSONL shards to a Hugging Face dataset")
    p.add_argument("--repo_id",    required=True, help="e.g. Moha8med80/Access_model")
    p.add_argument("--token",      default=os.getenv("HF_TOKEN"), help="HF API token")
    p.add_argument("--data_dir",   default=os.path.join(os.path.dirname(__file__), "..", "data"),
                     help="Path to folder containing XML files")
    p.add_argument("--shard_size", type=int, default=SHARD_SIZE, help="Records per shard (default 500)")
    p.add_argument("--force_remote", action="store_true",
                     help="Ignore local progress, start from remote index")
    args = p.parse_args()

    if not args.token:
        sys.exit("You must provide --token or set HF_TOKEN in env")

    # Resolve absolute data_dir
    data_dir = os.path.abspath(args.data_dir)
    if not os.path.isdir(data_dir):
        sys.exit(f"data_dir not found: {data_dir}")

    api = HfApi()
    create_repo(args.repo_id, repo_type="dataset", token=args.token, exist_ok=True)

    # Determine resume index
    local_idx  = load_local_index()
    remote_idx = remote_index(api, args.repo_id, args.token)
     #remote_idx if args.force_remote else max(local_idx, remote_idx)
    # If force_remote: resume from local progress only; else pick the furthest
    # start_at = loc_idx if args.force_remote else max(loc_idx, rem_idx)

    start_at   =remote_idx if args.force_remote else max(local_idx, remote_idx)
    files = list_xml(data_dir)
    total = len(files)
    if start_at >= total:
        print("No new files to upload.")
        return

    print(f"Starting at {start_at} / {total}")
    for i in range(start_at, total, args.shard_size):
        chunk      = files[i:i + args.shard_size]
        shard_name = f"shards/shard_{i:06d}_{i+len(chunk):06d}.jsonl"
        tmp_jsonl  = build_jsonl(chunk, data_dir)
        success    = upload_shard(api, tmp_jsonl, shard_name, args.repo_id, args.token)
        os.remove(tmp_jsonl)

        if not success:
            print("Stopping due to repeated errors.")
            break

        save_local_index(i + len(chunk))
        print(f"Uploaded {shard_name} ({len(chunk)}/{total})")

    print(" Upload complete.")

if __name__ == "__main__":
    main()
