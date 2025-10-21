"""
Convert all .parquet files found under `data/` into .csv files (same folder, same base name).

Behavior:
- Walk `data/` recursively and find .parquet files
- Prefer streaming conversion via pyarrow.dataset if available (efficient for large files)
- Fallback to pandas.read_parquet for smaller files
- Save CSV with `index=False` next to original file
- Print a summary at the end

Usage:
    python tools/convert_parquet_to_csv.py

"""
import os
import sys
from pathlib import Path
import traceback

DATA_DIR = Path('data')


def convert_with_pyarrow(parquet_path: Path, csv_path: Path):
    import pyarrow.dataset as ds
    import pyarrow as pa
    import pandas as pd

    # Use dataset scanning and write batches to CSV in chunks via pandas
    dataset = ds.dataset(str(parquet_path), format='parquet')
    scanner = dataset.scanner(batch_size=64 * 1024 * 1024)

    first_batch = True
    for batch in scanner.to_batches():
        table = pa.Table.from_batches([batch])
        df = table.to_pandas()
        # write header only on first batch, then append without header
        if first_batch:
            df.to_csv(csv_path, index=False, mode='w')
            first_batch = False
        else:
            df.to_csv(csv_path, index=False, header=False, mode='a')


def convert_with_pandas(parquet_path: Path, csv_path: Path):
    import pandas as pd
    # Read the whole parquet (may use a lot of memory for very large files)
    df = pd.read_parquet(parquet_path)
    df.to_csv(csv_path, index=False)


def main():
    if not DATA_DIR.exists():
        print(f"Data directory not found: {DATA_DIR}")
        sys.exit(1)

    parquet_files = list(DATA_DIR.rglob('*.parquet'))
    if not parquet_files:
        print(f"No .parquet files found under {DATA_DIR}")
        return

    converted = []
    errors = []

    # Detect availability of pyarrow
    use_pyarrow = False
    try:
        import pyarrow  # noqa: F401
        use_pyarrow = True
    except Exception:
        use_pyarrow = False

    print(f"Found {len(parquet_files)} parquet files. Using pyarrow: {use_pyarrow}")

    for p in parquet_files:
        try:
            csv_path = p.with_suffix('.csv')
            print(f"Converting {p} -> {csv_path}")
            if use_pyarrow:
                try:
                    convert_with_pyarrow(p, csv_path)
                except Exception:
                    # fallback to pandas if pyarrow conversion fails for any reason
                    print(f"pyarrow conversion failed for {p}, falling back to pandas. Error: {traceback.format_exc()}")
                    convert_with_pandas(p, csv_path)
            else:
                convert_with_pandas(p, csv_path)

            converted.append(str(csv_path))
        except Exception as e:
            errors.append((str(p), str(e)))
            print(f"Error converting {p}: {e}\n{traceback.format_exc()}")

    print('\nConversion complete.')
    print(f"Converted {len(converted)} files:")
    for c in converted:
        print(' -', c)
    if errors:
        print('\nErrors:')
        for p, e in errors:
            print(f" - {p}: {e}")


if __name__ == '__main__':
    main()
