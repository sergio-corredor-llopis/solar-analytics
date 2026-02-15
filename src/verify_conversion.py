"""
Verify Parquet conversion results.
Spot-check files across the date range.
"""

import pandas as pd
from pathlib import Path


def verify_all_parquet(staging_dir: str) -> dict:
    """
    Check all Parquet files for basic integrity.
    """
    staging_path = Path(staging_dir)
    parquet_files = sorted(staging_path.rglob("*.parquet"))
    
    print(f"Found {len(parquet_files)} Parquet files\n")
    print(f"{'File':<55} {'Rows':>8} {'Cols':>6} {'MB':>8}")
    print("=" * 80)
    
    results = []
    total_rows = 0
    
    for pf in parquet_files:
        df = pd.read_parquet(pf)
        size_mb = pf.stat().st_size / 1024 / 1024
        total_rows += len(df)
        
        results.append({
            'file': pf.name,
            'rows': len(df),
            'columns': len(df.columns),
            'size_mb': size_mb,
            'path': str(pf)
        })
        
        print(f"{str(pf.relative_to(staging_path)):<55} {len(df):>8,} {len(df.columns):>6} {size_mb:>7.2f}")
    
    print("=" * 80)
    print(f"{'TOTAL':<55} {total_rows:>8,}")
    
    # Summary statistics
    row_counts = [r['rows'] for r in results]
    col_counts = [r['columns'] for r in results]
    
    print(f"\n--- SUMMARY ---")
    print(f"Files:           {len(results)}")
    print(f"Total rows:      {total_rows:,}")
    print(f"Row range:       {min(row_counts):,} - {max(row_counts):,}")
    print(f"Column range:    {min(col_counts)} - {max(col_counts)}")
    
    # Flag anomalies
    print(f"\n--- ANOMALY CHECK ---")
    
    # Check for files with different column counts
    expected_cols = max(set(col_counts), key=col_counts.count)  # most common
    anomalies = [r for r in results if r['columns'] != expected_cols]
    if anomalies:
        print(f"⚠️  Files with unexpected column count (expected {expected_cols}):")
        for a in anomalies:
            print(f"   {a['file']}: {a['columns']} columns")
    else:
        print(f"✅ All files have {expected_cols} columns")
    
    # Check for suspiciously small files
    small_files = [r for r in results if r['rows'] < 100]
    if small_files:
        print(f"⚠️  Suspiciously small files:")
        for s in small_files:
            print(f"   {s['file']}: {s['rows']} rows")
    else:
        print(f"✅ No suspiciously small files")
    
    # Check the 15-min vs 5-min transition
    print(f"\n--- INTERVAL CHECK ---")
    print(f"First 5 files (expected ~15-min intervals, fewer rows):")
    for r in results[:5]:
        print(f"   {r['file']}: {r['rows']:,} rows")
    print(f"File 6+ (expected ~5-min intervals, more rows):")
    for r in results[5:8]:
        print(f"   {r['file']}: {r['rows']:,} rows")
    
    return results


def spot_check_file(parquet_path: str):
    """
    Deep inspection of a single Parquet file.
    """
    df = pd.read_parquet(parquet_path)
    
    print(f"File: {parquet_path}")
    print(f"Shape: {df.shape}")
    print(f"\nFirst 3 rows:")
    print(df.head(3))
    print(f"\nLast 3 rows:")
    print(df.tail(3))
    print(f"\nData types:")
    print(df.dtypes)
    print(f"\nMissing values (top 10):")
    missing = df.isnull().sum().sort_values(ascending=False).head(10)
    print(missing)
    print(f"\nTimestamp range:")
    if 'timestamp' in df.columns:
        print(f"  Start: {df['timestamp'].min()}")
        print(f"  End:   {df['timestamp'].max()}")


if __name__ == "__main__":
    STAGING_DIR = "data/staging/monthly"
    
    # Run full verification
    results = verify_all_parquet(STAGING_DIR)
    
    # Spot-check first file (15-min interval)
    # print("\n" + "=" * 80)
    # spot_check_file("data/staging/monthly/year=2013/month=02/solar_data_2013_02.parquet")
    
    # Spot-check a 5-min interval file
    # print("\n" + "=" * 80)
    # spot_check_file("data/staging/monthly/year=2014/month=01/solar_data_2014_01.parquet")