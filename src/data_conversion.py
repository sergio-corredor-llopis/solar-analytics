"""
CSV to Parquet conversion for Solar Analytics project.
Handles:
- UTF-16 LE encoding
- European decimal format (comma)
- Spanish date format (DD.MM.YYYY)
- Tab-separated values
- Skip metadata rows
- Missing values as "-"
"""

import pandas as pd
from pathlib import Path
import re


# Spanish month mapping (for filename parsing)
SPANISH_MONTHS = {
    'Enero': '01', 'Febrero': '02', 'Marzo': '03', 'Abril': '04',
    'Mayo': '05', 'Junio': '06', 'Julio': '07', 'Agosto': '08',
    'Septiembre': '09', 'Octubre': '10', 'Noviembre': '11', 'Diciembre': '12'
}


def parse_filename(filename: str) -> dict:
    """
    Extract year and month from filename.
    Example: '2013 02 Febrero Todos los Inversores.csv' 
    Returns: {'year': '2013', 'month': '02', 'month_name': 'Febrero'}
    """
    pattern = r'(\d{4})\s+(\d{2})\s+(\w+)\s+Todos los Inversores\.csv'
    match = re.match(pattern, filename)
    
    if match:
        return {
            'year': match.group(1),
            'month': match.group(2),
            'month_name': match.group(3)
        }
    return None


def load_solar_csv(csv_path: str) -> pd.DataFrame:
    """
    Load solar monitoring CSV with proper handling of:
    - UTF-16 LE encoding
    - Tab separator
    - Skip rows 0-4 (metadata) and 6-8 (units)
    - Header on row 5 (0-indexed)
    - European decimals (comma)
    - Missing values ("-")
    
    Args:
        csv_path: Path to source CSV file
    
    Returns:
        DataFrame with cleaned data
    """
    csv_path = Path(csv_path)
    
    # Read CSV with proper settings
    df = pd.read_csv(
        csv_path,
        encoding='utf-16-le',
        sep='\t',
        skiprows=[0, 1, 2, 3, 4, 6, 7, 8],  # Skip metadata + units rows
        header=0,  # Row 5 (after skipping) becomes header
        decimal=',',  # European decimal format
        na_values=['-', ''],  # Missing values
        dayfirst=True,  # DD.MM.YYYY format
    )
    
    # Parse date and time columns
    if 'Fecha' in df.columns and 'Hora' in df.columns:
        # Combine date and time into datetime
        df['Fecha'] = pd.to_datetime(df['Fecha'], format='%d.%m.%Y', errors='coerce')
        df['Hora'] = pd.to_timedelta(df['Hora'].astype(str), errors='coerce')
        df['timestamp'] = df['Fecha'] + df['Hora']
    
    return df


def analyze_csv(csv_path: str) -> dict:
    """
    Analyze a CSV file without converting.
    Useful for understanding data structure.
    """
    df = load_solar_csv(csv_path)
    
    analysis = {
        'filename': Path(csv_path).name,
        'rows': len(df),
        'columns': len(df.columns),
        'column_names': df.columns.tolist(),
        'date_range': {
            'start': str(df['Fecha'].min()) if 'Fecha' in df.columns else None,
            'end': str(df['Fecha'].max()) if 'Fecha' in df.columns else None,
        },
        'missing_values': df.isnull().sum().to_dict(),
        'memory_mb': df.memory_usage(deep=True).sum() / 1024 / 1024
    }
    
    return analysis


def csv_to_parquet(csv_path: str, output_dir: str) -> str:
    """
    Convert CSV file to Parquet format.
    Organizes output by year/month partitions.
    
    Args:
        csv_path: Path to source CSV file
        output_dir: Base directory for output (e.g., 'data/staging/monthly')
    
    Returns:
        Path to created Parquet file
    """
    csv_path = Path(csv_path)
    
    # Parse filename to get year/month
    file_info = parse_filename(csv_path.name)
    if not file_info:
        raise ValueError(f"Cannot parse filename: {csv_path.name}")
    
    print(f"Processing: {csv_path.name}")
    print(f"Year: {file_info['year']}, Month: {file_info['month']}")
    
    # Load and clean data
    df = load_solar_csv(csv_path)
    
    print(f"Rows: {len(df)}")
    print(f"Columns: {len(df.columns)}")
    print(f"Date range: {df['Fecha'].min()} to {df['Fecha'].max()}")
    print("-" * 50)
    
    # Create output path with partitions
    output_path = Path(output_dir) / f"year={file_info['year']}" / f"month={file_info['month']}"
    output_path.mkdir(parents=True, exist_ok=True)
    
    # Save as Parquet
    parquet_file = output_path / f"solar_data_{file_info['year']}_{file_info['month']}.parquet"
    df.to_parquet(parquet_file, index=False)
    
    print(f"✅ Saved to: {parquet_file}")
    print(f"Size: {parquet_file.stat().st_size / 1024 / 1024:.2f} MB")
    
    return str(parquet_file)


def convert_all_files(source_dir: str, output_dir: str) -> list:
    """
    Convert all CSV files in a directory to Parquet.
    
    Args:
        source_dir: Directory containing CSV files
        output_dir: Base directory for output
    
    Returns:
        List of created Parquet file paths
    """
    source_path = Path(source_dir)
    csv_files = sorted(source_path.glob("*Todos los Inversores.csv"))
    
    print(f"Found {len(csv_files)} CSV files")
    print("=" * 50)
    
    created_files = []
    for csv_file in csv_files:
        try:
            parquet_path = csv_to_parquet(str(csv_file), output_dir)
            created_files.append(parquet_path)
        except Exception as e:
            print(f"❌ Error processing {csv_file.name}: {e}")
    
    print("=" * 50)
    print(f"✅ Converted {len(created_files)}/{len(csv_files)} files")
    
    return created_files


if __name__ == "__main__":
    # ============================================================
    # CONFIGURATION - Update these paths!
    # ============================================================
    
    # Path to ONE CSV file for testing
    TEST_FILE = r"C:\Users\sergi\Downloads\Beca de colaboración\Beca de colaboración\Códigos fuente del programa de cálculos energéticos\2013 02 Febrero Todos los Inversores.csv"
    
    # Path to directory with ALL CSV files (for batch conversion)
    SOURCE_DIR = r"C:\Users\sergi\Downloads\Beca de colaboración\Beca de colaboración\Códigos fuente del programa de cálculos energéticos"
    
    # Output directory for Parquet files
    OUTPUT_DIR = "data/staging/monthly"
    
    # ============================================================
    # TEST WITH ONE FILE FIRST
    # ============================================================
    
    print("=== STEP 1: ANALYZE ONE FILE ===\n")
    # Uncomment when ready:
    analysis = analyze_csv(TEST_FILE)
    print(f"Rows: {analysis['rows']}")
    print(f"Columns: {analysis['columns']}")
    print(f"Date range: {analysis['date_range']}")
    
    print("\n=== STEP 2: CONVERT ONE FILE ===\n")
    # Uncomment when ready:
    csv_to_parquet(TEST_FILE, OUTPUT_DIR)
    
    print("\n=== STEP 3: CONVERT ALL FILES ===\n")
    # Uncomment when ready (after testing with one file):
    # convert_all_files(SOURCE_DIR, OUTPUT_DIR)
    
    print("\n⚠️  Update the paths above and uncomment the function calls!")

    df = pd.read_parquet("data/staging/monthly/year=2013/month=02/solar_data_2013_02.parquet")
    print(f"✅ Parquet read successful!")
    print(f"Rows: {len(df)}")
    print(f"Columns: {len(df.columns)}")
    print(df.head(2))