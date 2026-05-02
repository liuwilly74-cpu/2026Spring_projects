"""
Normalize Steam user tags, deduplicate rows.

Data quality issue found:
    All distinct tags in tags.csv are already ASCII English; there are no multilingual locale variants like genres.csv.

Pipeline:
    Data/tags.csv --> Cleaned_Data/tags_clean.csv
                  --> Cleaned_Data/tags_quality_report.txt
"""

import sys
import pandas as pd

INPUT_FILE = 'Data/tags.csv'
OUTPUT_CLEAN_CSV = 'Cleaned_Data/tags_clean.csv'
OUTPUT_REPORT_TXT = 'Cleaned_Data/tags_quality_report.txt'


# Reserved for foreign / variant tag strings → English (currently empty).
TAG_MAPPING: dict[str, str] = {}


def load_tags_csv(filepath: str) -> pd.DataFrame:
    """Read tags.csv with fallback encodings and return a typed DataFrame.
    filepath : str
        Path to the CSV file.
    pd.DataFrame
        DataFrame with 'app_id' as int, 'tag' as str.
    """
    for enc in ['utf-8', 'GB18030', 'cp1251', 'koi8-r', 'latin1']:
        try:
            df = pd.read_csv(filepath, encoding=enc)
            df = df.dropna(subset=['app_id', 'tag'])
            df['app_id'] = df['app_id'].astype(int)
            df['tag'] = df['tag'].str.strip()
            print(f"Loaded {len(df):,} rows from '{filepath}'.")
            return df
        except FileNotFoundError:
            print(f"Error: File not found '{filepath}'.")
            exit()
        except Exception as e:
            print(f"Error reading '{filepath}': {e}")
            exit()
    print(f"Fatal: Could not read '{filepath}'.")
    exit()


def normalize_whitespace(series: pd.Series) -> pd.Series:
    """Replace non-breaking spaces (U+00A0) with regular spaces and strip.

    series : pd.Series
        String Series to normalize.
    pd.Series
        Series with whitespace normalized.

    >>> import pandas as pd
    >>> s = pd.Series(['Free\\u00a0To\\u00a0Play', 'Indie'])
    >>> normalize_whitespace(s).tolist()
    ['Free To Play', 'Indie']
    """
    return series.str.replace('\u00a0', ' ', regex=False).str.strip()


def apply_mapping(series: pd.Series, mapping: dict) -> pd.Series:
    """Replace foreign-language tags with their canonical English form.

    series : pd.Series
        Whitespace-normalised tag column.
    mapping : dict
        {foreign variant: canonical English} lookup dictionary.
    pd.Series
        Series with foreign values replaced; unknown values kept as-is.

    >>> import pandas as pd
    >>> s = pd.Series(['Indie', 'Экшены', 'Unknown'])
    >>> result = apply_mapping(s, {'Экшены': 'Action'})
    >>> result.tolist()
    ['Indie', 'Action', 'Unknown']
    """
    return series.map(lambda x: mapping.get(x, x))


def quality_report(df_raw: pd.DataFrame, df_clean: pd.DataFrame, english_tags: set, unmapped: list) -> str:
    sep = '=' * 60
    attr = 'tag'
    per_app = df_raw.groupby('app_id')[attr].count()
    non_ascii = [v for v in df_raw[attr].dropna().unique() if not str(v).isascii()]
    freq = df_clean[attr].value_counts()

    lines = [
        sep,
        'Data/tags.csv  —  QUALITY REPORT',
        sep,
        '',
        '[1. Raw Data Stats]',
        f'  Total rows              : {len(df_raw):,}',
        f'  Unique app_ids          : {df_raw["app_id"].nunique():,}',
        f'  Missing tag             : {df_raw[attr].isna().sum():,}',
        f'  Fully duplicate rows    : {df_raw.duplicated().sum():,}',
        f'  Duplicate (app_id, tag): {df_raw.duplicated(subset=["app_id", attr]).sum():,}',
        '',
        '[2. Tag per app_id]',
        f'  Mean : {per_app.mean():.2f}',
        f'  Max  : {per_app.max()}',
        f'  Min  : {per_app.min()}',
        '',
        '[3. Language Check]',
        f'  Total raw distinct      : {df_raw[attr].nunique():,}',
        f'  Non-ASCII values        : {len(non_ascii):,}',
        f'  After normalization     : {df_clean[attr].nunique():,}',
        f'  Standard English set    : {len(english_tags):,}',
        '',
        '[4. Cleaning Result]',
        f'  Rows before cleaning    : {len(df_raw):,}',
        f'  Rows after cleaning     : {len(df_clean):,}',
        f'  Rows removed            : {len(df_raw) - len(df_clean):,}',
    ]
    if unmapped:
        lines += [
            '',
            '[5. Warnings]',
            f'  {len(unmapped)} unmapped values dropped (not in English set):',
        ]
        for u in sorted(unmapped):
            lines.append(f'    {u}')
    lines += [
        '',
        sep,
        f'DISTINCT TAG VALUES [{df_clean[attr].nunique()} total, sorted by frequency]',
        sep,
    ]
    for val in freq.index:
        lines.append(f'{val}  ({freq[val]:,} rows)')
    lines.append('')
    return '\n'.join(lines)


if __name__ == '__main__':
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')
    df = load_tags_csv(INPUT_FILE)
    df_raw = df.copy()
    df['tag'] = normalize_whitespace(df['tag'])
    df['tag'] = apply_mapping(df['tag'], TAG_MAPPING)

    # Source is English-only; canonical set = observed tags after normalize + map.
    english_tags = frozenset(df['tag'].unique())
    unmapped_mask = ~df['tag'].isin(english_tags)
    unmapped_tags = df.loc[unmapped_mask, 'tag'].unique().tolist()
    if unmapped_tags:
        print(f"WARNING: {len(unmapped_tags)} unmapped tags will be dropped:")
        for t in sorted(unmapped_tags):
            print(f"  {t}")
    df = df[~unmapped_mask].copy()

    df = df.drop_duplicates(subset=['app_id', 'tag'])
    df = df.sort_values(by=['app_id', 'tag']).reset_index(drop=True)

    try:
        df.to_csv(OUTPUT_CLEAN_CSV, index=False, encoding='utf-8-sig')
        print(f"\nSaved cleaned CSV ({len(df):,} rows) → {OUTPUT_CLEAN_CSV}")
    except Exception as e:
        print(f"Error saving CSV: {e}")

    report = quality_report(df_raw, df, english_tags, unmapped_tags)
    try:
        with open(OUTPUT_REPORT_TXT, 'w', encoding='utf-8') as f:
            f.write(report)
        print(f"Saved report → {OUTPUT_REPORT_TXT}")
    except Exception as e:
        print(f"Error saving report: {e}")
