#!/usr/bin/env python3
"""
shard_upload_resume.py
----------------------
يستأنف رفع شظايا JSONL إلى مستودع Hugging Face (نوع: dataset) بالاعتماد على
ملف progress محلي (upload_progress.json). بعد رفع كل شظيّة ناجحة يحدّث
المؤشّر ثم يتابع حتّى ينتهي أو يحدث خطأ غير قابل للاسترداد.

الوسيطات:
  --repo_id    مسار الريبو على 🤗 (مثال: vGassen/Dutch-Basisbestandwetten-Legislation-Laws)
  --token      HF_TOKEN بصلاحية كتابة
  --shard_size عدد السجلات في كل شظيّة (افتراضي 500)
  --data_dir   مجلد الـ XML (افتراضي ../data)
"""
from __future__ import annotations
from ast import main
import os, json, glob, argparse, time, tempfile, sys
from typing import List
from huggingface_hub import HfApi, create_repo

# ---------------- إعدادات افتراضية ---------------- #
PROGRESS_FILE = "upload_progress.json"
RETRY_LIMIT   = 5
BACKOFF       = 2.0         # ثوانٍ بين المحاولات

# ---------------- أدوات مساعدة ---------------- #
def load_progress() -> int:
    if os.path.exists(PROGRESS_FILE):
        with open(PROGRESS_FILE, encoding="utf-8") as f:
            try:
                return json.load(f)["last_index"]
            except Exception:
                pass
    return 0

def save_progress(idx: int):
    with open(PROGRESS_FILE, "w", encoding="utf-8") as f:
        json.dump({"last_index": idx}, f)

def list_xml(data_dir: str) -> List[str]:
    return sorted(glob.glob(os.path.join(data_dir, "**", "*.xml"), recursive=True))

def xml_to_record(path: str) -> dict:
    rel = os.path.relpath(path, start=os.path.dirname(data_dir)).replace("\\", "/")
    with open(path, "rb") as f:
        content = f.read().decode("utf-8", "ignore")
    return {"url": rel, "content": content, "source": "Basis Wettenbestand"}

def build_jsonl(batch: List[str]) -> str:
    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".jsonl").name
    with open(tmp, "w", encoding="utf-8") as out:
        for fp in batch:
            out.write(json.dumps(xml_to_record(fp), ensure_ascii=False) + "\n")
    return tmp

# ---------------- دالة الرفع مع إعادة المحاولة ---------------- #
def upload_shard(buf_path: str, shard_name: str, api: HfApi,
                 repo_id: str, token: str) -> bool:
    for attempt in range(1, RETRY_LIMIT + 1):
        try:
            api.upload_file(
                path_or_fileobj = buf_path,
                path_in_repo    = shard_name,
                repo_id         = repo_id,
                repo_type       = "dataset",     # ← مهم
                token           = token
            )
            return True
        except Exception as e:
            if attempt == RETRY_LIMIT:
                print(f"فشل رفع {shard_name}: {e}")
                return False
            time.sleep(BACKOFF * attempt)
    return False

# ---------------- البرنامج الرئيسى ---------------- #
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Resume shard upload to 🤗 dataset")
    parser.add_argument("--repo_id", required=True)
    parser.add_argument("--token",   required=False, default=os.getenv("HF_TOKEN"))
    parser.add_argument("--shard_size", type=int, default=250)
    parser.add_argument("--data_dir",
                        default=os.path.join(os.path.dirname(__file__), "..", "data"))
    args = parser.parse_args()

    if not args.token:
        sys.exit("يجب ضبط HF_TOKEN أو تمريره بـ --token")

    data_dir = os.path.abspath(args.data_dir)
    files    = list_xml(data_dir)
    total    = len(files)
    if total == 0:
        sys.exit(f"لا توجد ملفات XML فى {data_dir}")

    api = HfApi()
    create_repo(args.repo_id, repo_type="dataset", exist_ok=True, token=args.token)

    start = load_progress()
    if start >= total:
        print("لا توجد شظايا جديدة — كل الملفات رُفعت.")
        sys.exit(0)

    print(f"يبدأ الرفع من السجل {start} / {total}")
    for i in range(start, total, args.shard_size):
        batch      = files[i:i + args.shard_size]
        shard_name = f"shards/shard_{i:06d}_{i + len(batch):06d}.jsonl"

        tmp = build_jsonl(batch)
        ok  = upload_shard(tmp, shard_name, api, args.repo_id, args.token)
        os.remove(tmp)

        if not ok:
            print("توقّف البرنامج بعد أخطاء متكرّرة.")
            break

        save_progress(i + len(batch))
        print(f"تم رفع {shard_name} (المجموع {i + len(batch)}/{total})")

    print("تمّت عملية الرفع.")

if __name__ == "__main__":
    main()
