"""
Remove duplicate rows from all .parquet files under `data/`.

Behavior:
- Walk `data/` recursively for .parquet files
- For each file:
  - Create a backup copy with suffix `.bak.parquet` (same folder)
  - Load into pandas DataFrame
  - Compute original row count
  - Run `drop_duplicates()` (all columns)
  - Compute new row count
  - Overwrite original .parquet with deduplicated DataFrame
  - Record details in a timestamped log file under `logs/`

- At the end, print a summary and path to the log file.

Usage:
    python tools/deduplicate_parquet.py

NOTE: If files are extremely large and don't fit in memory, this script may fail. For huge datasets we should implement external/streaming deduplication by partitioning or by using pyarrow.dataset + hashing -- ask me if you need that.
"""
import os
import sys
from pathlib import Path
import datetime
import traceback

DATA_DIR = Path('data')
LOG_DIR = Path('logs')
LOG_DIR.mkdir(exist_ok=True)


def process_file(p: Path, log_f):
    import pandas as pd

    try:
        log_f.write(f"Processing: {p}\n")
        log_f.flush()

        # backup
        backup = p.with_suffix(p.suffix + '.bak.parquet')
        if not backup.exists():
            p.replace(backup)
            # put backup back to original path for processing
            backup.replace(p)
            # Now backup file exists and original restored
            # We'll read original (p) and then overwrite it
        else:
            log_f.write(f"Backup already exists: {backup}\n")

        # Read parquet
        df = pd.read_parquet(p)
        original_count = len(df)
        log_f.write(f" - Original rows: {original_count}\n")

        # Drop duplicates
        df_dedup = df.drop_duplicates()
        dedup_count = len(df_dedup)
        removed = original_count - dedup_count

        # Overwrite original parquet (create backup by renaming original first)
        # Save deduplicated
        df_dedup.to_parquet(p, index=False)

        log_f.write(f" - Deduplicated rows: {dedup_count}\n")
        log_f.write(f" - Rows removed: {removed}\n")
        log_f.write('\n')

        return {
            'file': str(p),
            'original': original_count,
            'deduped': dedup_count,
            'removed': removed,
            'status': 'ok'
        }

    except Exception as e:
        log_f.write(f"Error processing {p}: {e}\n")
        log_f.write(traceback.format_exc() + '\n')
        return {
            'file': str(p),
            'original': None,
            'deduped': None,
            'removed': None,
            'status': f'error: {e}'
        }


def main():
    if not DATA_DIR.exists():
        print(f"Data directory not found: {DATA_DIR}")
        sys.exit(1)

    parquet_files = list(DATA_DIR.rglob('*.parquet'))
    if not parquet_files:
        print(f"No .parquet files found under {DATA_DIR}")
        return

    now = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
    log_path = LOG_DIR / f"deduplicate_{now}.log"

    results = []
    with open(log_path, 'w', encoding='utf-8') as log_f:
        log_f.write(f"Deduplication run: {now}\n")
        log_f.write(f"Data dir: {DATA_DIR}\n")
        log_f.write(f"Files found: {len(parquet_files)}\n\n")
        for p in parquet_files:
            res = process_file(p, log_f)
            results.append(res)

        # summary
        total_original = sum(r['original'] for r in results if r['original'] is not None)
        total_deduped = sum(r['deduped'] for r in results if r['deduped'] is not None)
        total_removed = total_original - total_deduped

        log_f.write('=== SUMMARY ===\n')
        log_f.write(f"Files processed: {len(results)}\n")
        log_f.write(f"Total original rows: {total_original}\n")
        log_f.write(f"Total rows after dedup: {total_deduped}\n")
        log_f.write(f"Total rows removed: {total_removed}\n")

    print(f"Deduplication completed. Log: {log_path}")


if __name__ == '__main__':
    main()
