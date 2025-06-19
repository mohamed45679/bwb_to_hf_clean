# Dutch Basiswettenbestand to HuggingFace

This repository contains scripts that download Dutch legislation from the **Basiswettenbestand** via the SRU search interface and publish it as a dataset on the Hugging Face Hub.

The workflow is:
1. Use `crawler.py` to fetch XML files incrementally from the SRU endpoint.
2. Convert the collected XML files into JSONL shards and upload them to a dataset repository using `update_dataset.py` or `shard_upload_resume.py`.

## Requirements

Dependencies are listed in `requirements.txt`:

```
requests
lxml
huggingface_hub
tqdm
python-dotenv
```

Install them with:

```bash
pip install -r requirements.txt
```

## Scripts

- **crawler.py** – incremental SRU crawler. Example usage from the script:

```
python crawler.py \
    --sru_url https://zoekservice.overheid.nl/sru/Search \
    --cql_query "modified<=2025-02-13" \
    --sru_version 1.2 \
    --batch_size 100
```

- **update_dataset.py** – convert XML files into JSONL shards and upload them. The script supports resuming via `upload_progress.json`:

```
python update_dataset.py \
  --repo_id "vGassen/Dutch-Basisbestandwetten-Legislation-Laws" \
  --token   "$HF_TOKEN" \
  --data_dir "../data3" \
  [--shard_size 250] \
  [--force_remote]
```

- **shard_upload_resume.py** – upload JSONL shards to Hugging Face using the same progress file. It is useful when uploads need to be resumed after interruptions.
- **resume_upload.ps1** / **upload_remaining.ps1** – PowerShell helpers for Windows environments.
- **check_shards.ini** – small utility that lists the uploaded shards from a dataset.

Progress information for the crawler and uploader is kept in `sru_progress.json` and `upload_progress.json` respectively.

## GitHub Actions

The `.github/workflows` folder contains examples for automating the crawl and upload process. In `upload_to_hf.yml` the action installs the dependencies, runs the crawler, then uploads the data:

```
python scripts/crawler.py \
  --sru_url      "$SRU_URL" \
  --cql_query    "$CQL_QUERY" \
  --sru_version  1.2 \
  --connection   BWB \
  --out_dir      "./data" \
  --batch_size   100

python scripts/update_dataset.py \
  --repo_id   "vGassen/Dutch-Basisbestandwetten-Legislation-Laws" \
  --token     "$HF_TOKEN" \
  --data_dir  "./data" \
  --force_remote
```

Set the `HF_TOKEN` environment variable with write access to your dataset repository when running the scripts or workflows.

## License

No explicit license is provided with this repository.
