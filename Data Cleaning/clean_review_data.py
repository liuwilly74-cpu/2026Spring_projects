"""
parse review rows, normalize missing values, cast numeric columns to nullable integer types, and write:

  - Cleaned_Data/reviews_clean.csv            : cleaned review table
  - Cleaned_Data/reviews_quality_report.txt   : data-quality report

Data quality issues found:
    1) The file contains escaped quotes (e.g. \") inside HTML text.
    2) Some review text fields span multiple physical lines.
    3) Missing values are encoded as "\\N" and become "N" after parsing with
       escape handling.

Pipeline:
    Data/reviews.csv --> Cleaned_Data/reviews_clean.csv
                     --> Cleaned_Data/reviews_quality_report.txt
"""

import pandas as pd

INPUT_FILE = 'Data/reviews.csv'
OUTPUT_CLEAN_CSV = 'Cleaned_Data/reviews_clean.csv'
OUTPUT_REPORT_TXT = 'Cleaned_Data/reviews_quality_report.txt'

MISSING_TOKENS = {'\\N', 'N', '', 'nan', 'NaN', 'None', '<NA>'}
NUMERIC_COLUMNS = [
    'app_id',
    'review_score',
    'positive',
    'negative',
    'total',
    'metacritic_score',
    'recommendations',
    'steamspy_positive',
    'steamspy_negative',
]

DROP_COLUMNS = ['steamspy_user_score', 'reviews', 'steamspy_score_rank']


def load_reviews_csv(filepath: str) -> pd.DataFrame:
    """Read reviews.csv using robust settings for escaped/multiline text.
    
    filepath : str
        Path to the CSV file.
    pd.DataFrame
        Parsed DataFrame with all columns loaded as strings.

    >>> import tempfile, os
    >>> tmp = tempfile.NamedTemporaryFile(mode='w', suffix='.csv',
    ...                                   delete=False, encoding='utf-8')
    >>> _ = tmp.write('app_id,reviews\\n1,\"A \\\\\"quote\\\\\"\"\\n')
    >>> tmp.close()
    >>> df = load_reviews_csv(tmp.name)  # doctest: +ELLIPSIS
    Loaded 1 rows from '...'.
    >>> df.loc[0, 'app_id']
    '1'
    >>> os.unlink(tmp.name)
    """
    try:
        df = pd.read_csv(
            filepath,
            encoding='utf-8',
            dtype=str,
            engine='python',
            escapechar='\\',
        )
        print(f"Loaded {len(df):,} rows from '{filepath}'.")
        return df
    except FileNotFoundError:
        print(f"Error: File not found '{filepath}'.")
        exit()
    except Exception as exc:
        print(f"Error reading '{filepath}': {exc}")
        exit()


def normalize_missing_tokens(series: pd.Series) -> pd.Series:
    """Convert known missing placeholders to pandas NA values.

    series : pd.Series
        Input string-like series.
    pd.Series
        Series with placeholder missing tokens replaced by ``pd.NA``.

    >>> import pandas as pd
    >>> s = pd.Series(['N', '\\\\N', '', '123'])
    >>> normalize_missing_tokens(s).tolist()
    [<NA>, <NA>, <NA>, '123']
    """
    series = series.astype('string').str.strip()
    return series.where(~series.isin(MISSING_TOKENS), pd.NA)


def to_nullable_int(series: pd.Series) -> pd.Series:
    """Convert a series to pandas nullable integer (Int64) safely.

    series : pd.Series
        Input series with optional missing tokens and digits.
    pd.Series
        Nullable integer series with dtype ``Int64``.

    >>> import pandas as pd
    >>> s = pd.Series(['1', 'N', ' 3 '])
    >>> to_nullable_int(s).tolist()
    [1, <NA>, 3]
    """
    cleaned = normalize_missing_tokens(series)
    numeric = pd.to_numeric(cleaned, errors='coerce')
    return numeric.astype('Int64')


def clean_reviews_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Apply the full cleaning pipeline to reviews DataFrame.

    df : pd.DataFrame
        Raw parsed review DataFrame.
    pd.DataFrame
        Cleaned DataFrame with typed numeric columns and normalized missing
        values.

    >>> import pandas as pd
    >>> raw = pd.DataFrame({'app_id': ['1'], 'review_score': ['9'], 'steamspy_positive': ['500']})
    >>> out = clean_reviews_dataframe(raw)
    >>> out['app_id'].tolist()
    [1]
    >>> out['steamspy_positive'].tolist()
    [500]
    """
    df = df.copy()

    # Drop low-value / high-missingness columns before any further processing.
    cols_to_drop = [c for c in DROP_COLUMNS if c in df.columns]
    df = df.drop(columns=cols_to_drop)

    # Normalize all object columns first, then cast numeric columns.
    for col in df.columns:
        df[col] = normalize_missing_tokens(df[col])

    for col in NUMERIC_COLUMNS:
        if col in df.columns:
            df[col] = to_nullable_int(df[col])

    # Keep one row per app_id (no duplicates in source, this is defensive).
    if 'app_id' in df.columns:
        df = df.drop_duplicates(subset=['app_id']).sort_values('app_id')

    return df.reset_index(drop=True)


def quality_report(df_raw: pd.DataFrame, df_clean: pd.DataFrame) -> str:
    sep = '=' * 60

    raw_dupes = int(df_raw.duplicated().sum())
    raw_app_dupes = int(df_raw.duplicated(subset=['app_id']).sum()) if 'app_id' in df_raw.columns else 0

    placeholder_counts = {
        col: int(df_raw[col].astype('string').str.strip().isin(MISSING_TOKENS).sum())
        for col in df_raw.columns
    }

    total_mismatch = 0
    if {'positive', 'negative', 'total'}.issubset(df_clean.columns):
        mask = (
            df_clean['positive'].notna()
            & df_clean['negative'].notna()
            & df_clean['total'].notna()
        )
        total_mismatch = int((df_clean.loc[mask, 'positive'] + df_clean.loc[mask, 'negative'] != df_clean.loc[mask, 'total']).sum())

    descriptions = (
        df_clean['review_score_description']
        .dropna()
        .value_counts()
        .head(15)
        if 'review_score_description' in df_clean.columns
        else pd.Series(dtype='Int64')
    )

    lines = [
        sep,
        'Data/reviews.csv  —  QUALITY REPORT',
        sep,
        '',
        '[Raw Data Stats]',
        f'  Total rows              : {len(df_raw):,}',
        f'  Total columns           : {len(df_raw.columns)}',
        f'  Full duplicate rows     : {raw_dupes:,}',
        f'  Duplicate app_id rows   : {raw_app_dupes:,}',
        '',
        '[Known File Issues]',
        '  1) Escaped quotes (\\") inside HTML snippets',
        '  2) Multiline text in the reviews column (column dropped)',
        '  3) Missing value placeholder token: \\N (parsed as N)',
        '',
        '[Dropped Columns]',
        f'  {", ".join(DROP_COLUMNS)}',
        '',
        '[Missing Token Counts in Raw Data]',
    ]
    for col in df_raw.columns:
        lines.append(f'  {col}: {placeholder_counts[col]:,}')

    lines += [
        '',
        '[Numeric Integrity]',
        f'  Rows checked for positive+negative=total : {len(df_clean):,}',
        f'  Mismatch rows                             : {total_mismatch:,}',
    ]

    if 'review_score' in df_clean.columns:
        lt0 = int((df_clean['review_score'].fillna(0) < 0).sum())
        gt9 = int((df_clean['review_score'].fillna(0) > 9).sum())
        lines += [
            '  review_score out-of-range (<0 or >9)      : '
            f'{lt0 + gt9:,}',
        ]

    if 'metacritic_score' in df_clean.columns:
        lt0 = int((df_clean['metacritic_score'].fillna(0) < 0).sum())
        gt100 = int((df_clean['metacritic_score'].fillna(0) > 100).sum())
        lines += [
            '  metacritic_score out-of-range (<0 or >100): '
            f'{lt0 + gt100:,}',
        ]

    lines += [
        '',
        '[Top review_score_description values]',
    ]
    for desc, cnt in descriptions.items():
        lines.append(f'  {desc}: {cnt:,}')

    lines += [
        '',
        '[Cleaning Result]',
        f'  Rows before cleaning : {len(df_raw):,}',
        f'  Rows after cleaning  : {len(df_clean):,}',
        f'  Rows removed         : {len(df_raw) - len(df_clean):,}',
        '',
    ]
    return '\n'.join(lines)


if __name__ == '__main__':
    df_raw = load_reviews_csv(INPUT_FILE)
    df_clean = clean_reviews_dataframe(df_raw)

    report = quality_report(df_raw, df_clean)

    try:
        df_clean.to_csv(OUTPUT_CLEAN_CSV, index=False, encoding='utf-8-sig')
        print(f"Saved cleaned CSV ({len(df_clean):,} rows) → {OUTPUT_CLEAN_CSV}")
    except Exception as exc:
        print(f"Error saving CSV: {exc}")

    try:
        with open(OUTPUT_REPORT_TXT, 'w', encoding='utf-8') as f:
            f.write(report)
        print(f"Saved report → {OUTPUT_REPORT_TXT}")
    except Exception as exc:
        print(f"Error saving report: {exc}")
