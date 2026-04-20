"""
Clean the raw tags.csv and write:

  - Cleaned_Data/tags_clean.csv          : deduplicated (app_id, tag) table
  - Cleaned_Data/tags_quality_report.txt : data-quality report + distinct tag list

Data quality findings:
    - All 447 distinct tags are already in English (no multilingual issue).
    - No missing values; no duplicate rows in the source file.
    - Tags per game: mean ≈ 14.85, max = 20.

Pipeline:
    Data/tags.csv --> Cleaned_Data/tags_clean.csv
                  --> Cleaned_Data/tags_quality_report.txt
"""

import pandas as pd

INPUT_FILE = 'Data/tags.csv'
OUTPUT_CLEAN_CSV = 'Cleaned_Data/tags_clean.csv'
OUTPUT_REPORT_TXT = 'Cleaned_Data/tags_quality_report.txt'


def load_tags_csv(filepath: str) -> pd.DataFrame:
    """Read tags.csv with fallback encodings and return a typed DataFrame.

    filepath : str
        Path to the CSV file.
    pd.DataFrame
        DataFrame with 'app_id' as int and 'tag' as str.

    >>> import tempfile, os
    >>> tmp = tempfile.NamedTemporaryFile(mode='w', suffix='.csv',
    ...                                   delete=False, encoding='utf-8')
    >>> _ = tmp.write('app_id,tag\\n1,Action\\n2,Indie\\n')
    >>> tmp.close()
    >>> df = load_tags_csv(tmp.name)  # doctest: +ELLIPSIS
    Loaded 2 rows from '...'.
    >>> df['app_id'].tolist()
    [1, 2]
    >>> os.unlink(tmp.name)
    """
    for enc in ['utf-8', 'GB18030', 'cp1251', 'latin1']:
        try:
            df = pd.read_csv(filepath, encoding=enc)
            df = df.dropna(subset=['app_id', 'tag'])
            df['app_id'] = df['app_id'].astype(int)
            df['tag'] = df['tag'].str.strip()
            print(f"Loaded {len(df):,} rows from '{filepath}'.")
            return df
        except (UnicodeDecodeError, FileNotFoundError) as exc:
            if isinstance(exc, FileNotFoundError):
                print(f"Error: File not found '{filepath}'.")
                exit()
    print(f"Fatal: Could not read '{filepath}'.")
    exit()


def clean_tags_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Strip whitespace, remove null tags, and deduplicate (app_id, tag) pairs.

    df : pd.DataFrame
        Raw tag DataFrame with at least 'app_id' and 'tag' columns.
    pd.DataFrame
        Cleaned and sorted DataFrame.

    >>> import pandas as pd
    >>> raw = pd.DataFrame({
    ...     'app_id': [1, 1, 2],
    ...     'tag': [' Action ', 'Action', 'Indie'],
    ... })
    >>> out = clean_tags_dataframe(raw)
    >>> out[['app_id', 'tag']].values.tolist()
    [[1, 'Action'], [2, 'Indie']]
    """
    df = df.copy()
    df['tag'] = df['tag'].str.strip()
    df = df.dropna(subset=['app_id', 'tag'])
    df = df.drop_duplicates(subset=['app_id', 'tag'])
    df = df.sort_values(by=['app_id', 'tag']).reset_index(drop=True)
    return df


def quality_report(df_raw: pd.DataFrame, df_clean: pd.DataFrame) -> str:
    sep = '=' * 60

    tags_per_app = df_raw.groupby('app_id')['tag'].count()
    non_ascii_tags = [t for t in df_raw['tag'].dropna().unique() if not t.isascii()]

    lines = [
        sep,
        'Data/tags.csv  —  QUALITY REPORT',
        sep,
        '',
        '[Raw Data Stats]',
        f'  Total rows              : {len(df_raw):,}',
        f'  Unique app_ids          : {df_raw["app_id"].nunique():,}',
        f'  Unique tags             : {df_raw["tag"].nunique():,}',
        f'  Missing app_id          : {df_raw["app_id"].isna().sum():,}',
        f'  Missing tag             : {df_raw["tag"].isna().sum():,}',
        f'  Fully duplicate rows    : {df_raw.duplicated().sum():,}',
        f'  Duplicate (app_id, tag) : {df_raw.duplicated(subset=["app_id","tag"]).sum():,}',
        '',
        '[Tags per app_id]',
        f'  Mean : {tags_per_app.mean():.2f}',
        f'  Max  : {tags_per_app.max()}',
        f'  Min  : {tags_per_app.min()}',
        '',
        '[Language Check]',
        f'  Non-ASCII tags: {len(non_ascii_tags)}',
    ]
    if non_ascii_tags:
        for t in sorted(non_ascii_tags):
            lines.append(f'    {t}')
    else:
        lines.append('  All tags are ASCII (English only) — no multilingual issue.')

    lines += [
        '',
        '[Cleaning Result]',
        f'  Rows before cleaning : {len(df_raw):,}',
        f'  Rows after cleaning  : {len(df_clean):,}',
        f'  Rows removed (dupes) : {len(df_raw) - len(df_clean):,}',
        '',
        sep,
        f'DISTINCT TAGS [{df_clean["tag"].nunique()} total, sorted by frequency]',
        sep,
    ]
    freq = df_clean['tag'].value_counts()
    for tag in freq.index:
        lines.append(f'{tag}  ({freq[tag]:,} rows)')
    lines.append('')
    return '\n'.join(lines)


if __name__ == '__main__':
    df_raw = load_tags_csv(INPUT_FILE)
    df_clean = clean_tags_dataframe(df_raw)

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
