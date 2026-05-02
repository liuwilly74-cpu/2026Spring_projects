"""
Clean games.csv and steamspy_insights.csv, then left-join them on app_id and
write:
  - Cleaned_Data/games_clean.csv          : merged table with columns from both sources
  - Cleaned_Data/games_quality_report.txt : combined quality report for both source files and the merged result

Merged columns:
    app_id, name, release_date, is_free, developer, publisher, owners_range, price

Data quality issues found in source files:
    - Both CSVs require escapechar='\\' due to embedded escaped quotes in JSON / HTML text fields.
    - steamspy_insights.csv: price column uses 'N' as a missing placeholder (49,187 rows); numeric values are in integer cents (999 = 9.99).

Pipeline:
    Data/games.csv             ──┐
                                  ├─(left join on app_id)──► Cleaned_Data/games_clean.csv
    Data/steamspy_insights.csv ──┘                         ► Cleaned_Data/games_quality_report.txt
"""

import pandas as pd

INPUT_GAMES = 'Data/games.csv'
INPUT_INSIGHTS = 'Data/steamspy_insights.csv'
OUTPUT_GAMES_CSV = 'Cleaned_Data/games_clean.csv'
OUTPUT_GAMES_REPORT = 'Cleaned_Data/games_quality_report.txt'

GAMES_KEEP = ['app_id', 'name', 'release_date', 'is_free']
STEAMSPY_KEEP = ['app_id', 'developer', 'publisher', 'owners_range', 'price']

PRICE_MISSING_TOKENS = {'N', '\\N', '', 'nan', 'NaN', 'None'}


def load_csv(filepath: str) -> pd.DataFrame:
    """Read a Steam CSV file using robust settings for embedded escaped quotes.
    Both games.csv and steamspy_insights.csv contain fields with escaped quotes and/or JSON text that require escapechar='\\\\'.

    filepath : str
        Path to the CSV file.
    pd.DataFrame
        All columns loaded as object (string) dtype.
    """
    try:
        df = pd.read_csv(filepath, dtype=str, engine='python', escapechar='\\')
        print(f"Loaded {len(df):,} rows from '{filepath}'.")
        return df
    except FileNotFoundError:
        print(f"Error: File not found '{filepath}'.")
        exit()
    except Exception as exc:
        print(f"Error reading '{filepath}': {exc}")
        exit()


def parse_price(series: pd.Series) -> pd.Series:
    """Convert integer-cent price strings to float values with two decimals.

    series : pd.Series
        Raw price column (string dtype).
    pd.Series
        Float64 series; ``pd.NA`` for missing entries.

    >>> import pandas as pd
    >>> s = pd.Series(['999', '0', 'N', '1499'])
    >>> parse_price(s).tolist()
    [9.99, 0.0, <NA>, 14.99]
    """
    cleaned = series.astype('string').str.strip()
    cleaned = cleaned.where(~cleaned.isin(PRICE_MISSING_TOKENS), pd.NA)
    numeric = pd.to_numeric(cleaned, errors='coerce')
    result = (numeric / 100).round(2)
    return result.astype('Float64')


def clean_games(df: pd.DataFrame) -> pd.DataFrame:
    """Select and type-cast the four retained columns from games.csv.
    df : pd.DataFrame
        Raw games DataFrame (all-string dtype from load_csv).
    pd.DataFrame
        Cleaned DataFrame with columns: app_id (Int64), name (string),
        release_date (string), is_free (Int64).

    >>> import pandas as pd
    >>> raw = pd.DataFrame({
    ...     'app_id': ['10', '20'],
    ...     'name': ['Counter-Strike', 'TF Classic'],
    ...     'release_date': ['2000-11-01', '1999-04-01'],
    ...     'is_free': ['0', '1'],
    ...     'price_overview': ['{}', '{}'],
    ...     'languages': ['English', 'English'],
    ...     'type': ['game', 'game'],
    ... })
    >>> out = clean_games(raw)
    >>> list(out.columns)
    ['app_id', 'name', 'release_date', 'is_free']
    >>> out['app_id'].tolist()
    [10, 20]
    >>> out['is_free'].tolist()
    [0, 1]
    """
    df = df[GAMES_KEEP].copy()
    df['app_id'] = pd.to_numeric(df['app_id'], errors='coerce').astype('Int64')
    df['name'] = df['name'].astype('string')
    df['release_date'] = df['release_date'].astype('string')
    df['is_free'] = pd.to_numeric(df['is_free'], errors='coerce').astype('Int64')
    df = df.dropna(subset=['app_id'])
    df = df.drop_duplicates(subset=['app_id'])
    df = df.sort_values('app_id').reset_index(drop=True)
    return df


def clean_steamspy(df: pd.DataFrame) -> pd.DataFrame:
    """Select and fix price from steamspy_insights.csv; keep owners_range as text.

    df : pd.DataFrame
        Raw steamspy DataFrame (all-string dtype from load_csv).
    pd.DataFrame
        Cleaned DataFrame with columns: app_id, developer, publisher,
        owners_range, price.

    >>> import pandas as pd
    >>> raw = pd.DataFrame({
    ...     'app_id': ['10'],
    ...     'developer': ['Valve'],
    ...     'publisher': ['Valve'],
    ...     'owners_range': ['10,000,000 .. 20,000,000'],
    ...     'concurrent_users_yesterday': ['11457'],
    ...     'price': ['999'],
    ...     'initial_price': ['999'],
    ...     'discount': ['0'],
    ...     'languages': ['English'],
    ...     'genres': ['Action'],
    ... })
    >>> out = clean_steamspy(raw)
    >>> out['price'].tolist()
    [9.99]
    >>> out['owners_range'].iloc[0]
    '10,000,000 .. 20,000,000'
    """
    df = df[STEAMSPY_KEEP].copy()
    df['app_id'] = pd.to_numeric(df['app_id'], errors='coerce').astype('Int64')
    df['developer'] = df['developer'].astype('string')
    df['publisher'] = df['publisher'].astype('string')
    df['owners_range'] = df['owners_range'].astype('string')
    df['price'] = parse_price(df['price'])

    df = df.dropna(subset=['app_id'])
    df = df.drop_duplicates(subset=['app_id'])
    df = df.sort_values('app_id').reset_index(drop=True)

    col_order = ['app_id', 'developer', 'publisher', 'owners_range', 'price']
    return df[col_order]


def merge_tables(df_games: pd.DataFrame, df_steamspy: pd.DataFrame) -> pd.DataFrame:
    """Left-join cleaned games and steamspy tables on app_id.
    Games without a steamspy entry retain NaN for all steamspy columns.

    df_games : pd.DataFrame
        Output of clean_games().
    df_steamspy : pd.DataFrame
        Output of clean_steamspy().
    pd.DataFrame
        Merged table sorted by app_id.

    >>> import pandas as pd
    >>> g = pd.DataFrame({'app_id': pd.array([1, 2], dtype='Int64'),
    ...                   'name': pd.array(['A', 'B'], dtype='string'),
    ...                   'release_date': pd.array(['2020-01-01', '2021-01-01'], dtype='string'),
    ...                   'is_free': pd.array([0, 1], dtype='Int64')})
    >>> s = pd.DataFrame({'app_id': pd.array([1], dtype='Int64'),
    ...                   'developer': pd.array(['Dev'], dtype='string'),
    ...                   'publisher': pd.array(['Pub'], dtype='string'),
    ...                   'owners_range': ['0 .. 20,000'],
    ...                   'price': pd.array([9.99], dtype='Float64')})
    >>> merged = merge_tables(g, s)
    >>> len(merged)
    2
    >>> merged.loc[merged['app_id'] == 2, 'developer'].iloc[0]
    <NA>
    """
    merged = pd.merge(df_games, df_steamspy, on='app_id', how='left')
    return merged.sort_values('app_id').reset_index(drop=True)


def quality_report(df_raw: pd.DataFrame, df_clean: pd.DataFrame, label: str, section_num: int) -> str:
    sep = '=' * 60
    retained = ', '.join(df_clean.columns.tolist())
    dup_app = (
        df_raw.duplicated('app_id').sum()
        if 'app_id' in df_raw.columns else 'N/A'
    )
    n = len(df_raw)

    lines = [
        sep,
        f'SECTION {section_num}: {label}  —  SOURCE FILE',
        sep,
        '',
        '[1. Raw Data Stats]',
        f'  Total rows       : {n:,}',
        f'  Total columns    : {len(df_raw.columns)}',
        f'  Duplicate rows   : {df_raw.duplicated().sum():,}',
        f'  Duplicate app_id : {dup_app:,}',
        '',
    ]

    lines += [
        '',
        '[2. Cleaning]',
        f'  Columns retained  : {len(df_clean.columns)}  ({retained})',
        f'  Rows retained     : {len(df_clean):,}',
        f'  Rows removed      : {n - len(df_clean):,}',
        '',
    ]
    return '\n'.join(lines)


def merged_quality_report(df_merged: pd.DataFrame) -> str:
    sep = '=' * 60
    n = len(df_merged)

    lines = [
        sep,
        'SECTION 3: MERGED TABLE (left join on app_id)',
        sep,
        '',
        '[1. Overview]',
        f'  Total rows    : {n:,}',
        f'  Total columns : {len(df_merged.columns)}',
        '',
        '[2. Missing Values in Merged Output]',
    ]
    for col in df_merged.columns:
        cnt = int(df_merged[col].isna().sum())
        pct = cnt / n * 100
        lines.append(f'  {col:<35}: {cnt:,}  ({pct:.1f}%)')

    lines += ['', '[3. Numeric Column Ranges]']
    for col in ['price']:
        if col in df_merged.columns:
            data = df_merged[col].dropna()
            if not data.empty:
                lines.append(
                    f'  {col:<20}: min={data.min():<12}  '
                    f'median={data.median():<12}  max={data.max()}'
                )

    total = n
    if 'is_free' in df_merged.columns:
        paid = int((df_merged['is_free'].fillna(-1) == 0).sum())
        free = int((df_merged['is_free'].fillna(-1) == 1).sum())
        lines += [
            '',
            '[4. Key Distributions]',
            f'  is_free  : paid={paid:,} ({paid/total*100:.1f}%)    '
            f'free={free:,} ({free/total*100:.1f}%)',
        ]

    if 'price' in df_merged.columns:
        p_paid = int((df_merged['price'].fillna(-1) > 0).sum())
        p_free = int((df_merged['price'].fillna(-1) == 0).sum())
        p_miss = int(df_merged['price'].isna().sum())
        lines.append(
            f'  price    : paid={p_paid:,} ({p_paid/total*100:.1f}%)    '
            f'free={p_free:,} ({p_free/total*100:.1f}%)    '
            f'missing={p_miss:,} ({p_miss/total*100:.1f}%)'
        )

    if 'owners_range' in df_merged.columns:
        lines += ['  owners_range (top buckets):']
        for rng, cnt in df_merged['owners_range'].value_counts().head(10).items():
            lines.append(f'    {rng:<35}: {cnt:,}')

    lines.append('')
    return '\n'.join(lines)


if __name__ == '__main__':
    raw_games = load_csv(INPUT_GAMES)
    clean_games_df = clean_games(raw_games)

    raw_steamspy = load_csv(INPUT_INSIGHTS)
    clean_steamspy_df = clean_steamspy(raw_steamspy)

    merged_df = merge_tables(clean_games_df, clean_steamspy_df)
    print(f"Merged table: {len(merged_df):,} rows, {len(merged_df.columns)} columns.")

    try:
        merged_df.to_csv(OUTPUT_GAMES_CSV, index=False, encoding='utf-8-sig')
        print(f"Saved merged CSV ({len(merged_df):,} rows) → {OUTPUT_GAMES_CSV}")
    except Exception as exc:
        print(f"Error saving merged CSV: {exc}")

    report = '\n'.join([
        quality_report(raw_games, clean_games_df, 'games.csv', section_num=1),
        quality_report(raw_steamspy, clean_steamspy_df, 'steamspy_insights.csv', section_num=2),
        merged_quality_report(merged_df),
    ])
    try:
        with open(OUTPUT_GAMES_REPORT, 'w', encoding='utf-8') as f:
            f.write(report)
        print(f"Saved report → {OUTPUT_GAMES_REPORT}")
    except Exception as exc:
        print(f"Error saving report: {exc}")
