"""
Fuzzy-match Steam games against the vgvgsales table by game title.

Outputs:
  - Cleaned_Data/games_vgsales_matched.csv      (matched rows only)
  - Cleaned_Data/games_vgsales_match_quality.txt (quality analysis)
"""

import re
import sys
import pandas as pd
from rapidfuzz import fuzz
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.neighbors import NearestNeighbors
from tqdm import tqdm


INPUT_GAMES = 'Cleaned_Data/games_clean.csv'
INPUT_VGSALES = 'Data/vgsales.csv'
OUTPUT_MATCHED = 'Cleaned_Data/games_vgsales_matched.csv'
OUTPUT_QUALITY = 'Cleaned_Data/games_vgsales_match_quality.txt'

COL_STEAM_NAME = 'name'
COL_STEAM_ID = 'app_id'
COL_VG_NAME = 'Name'
SIMILARITY_THRESHOLD = 83

VG_SALES_COLUMNS = ['NA_Sales', 'EU_Sales', 'JP_Sales', 'Other_Sales', 'Global_Sales']
VG_RENAME = {
    'NA_Sales': 'vg_na_sales',
    'EU_Sales': 'vg_eu_sales',
    'JP_Sales': 'vg_jp_sales',
    'Other_Sales': 'vg_other_sales',
    'Global_Sales': 'vg_global_sales',
}

RISK_KEYWORDS = (
    'GOTY', 'Remastered', 'Remaster', 'Bundle', 'Collection',
    'Definitive', 'Complete', 'Anthology', 'Trilogy', 'HD',
)


def read_csv_with_encoding_fallbacks(filepath: str, required_cols: list) -> pd.DataFrame:
    """Read a CSV trying several encodings; drop rows missing required_cols.
    """
    for enc in ['utf-8', 'latin1', 'cp1251', 'GB18030', 'koi8-r']:
        try:
            df = pd.read_csv(filepath, encoding=enc, low_memory=False)
            df = df.dropna(subset=required_cols)
            df = df.drop_duplicates(subset=[required_cols[0]])
            print(f"--- Read '{filepath}' with {enc} ---")
            return df.reset_index(drop=True)
        except UnicodeDecodeError:
            continue
        except FileNotFoundError:
            print(f"Fatal: file not found '{filepath}'.")
            sys.exit(1)
        except Exception as exc:
            print(f"--- '{enc}' failed on '{filepath}': {exc} ---")
    print(f"Fatal: could not read '{filepath}' with any tried encoding.")
    sys.exit(1)


def aggregate_vgsales(df_vg: pd.DataFrame) -> pd.DataFrame:
    """Collapse vgsales rows sharing a Name across platforms.
    Sums the four regional sales columns and Global_Sales and collects
    distinct platform strings into a pipe-separated vg_platform field.
    """
    df = df_vg.copy()
    for col in VG_SALES_COLUMNS:
        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0.0)

    sales_sum = df.groupby('Name', as_index=False)[VG_SALES_COLUMNS].sum()
    platforms = (
        df.groupby('Name')['Platform']
        .apply(lambda s: '|'.join(sorted(set(s.dropna().astype(str)))))
        .reset_index()
        .rename(columns={'Platform': 'vg_platform'})
    )
    return sales_sum.merge(platforms, on='Name')


def compute_fuzzy_scores(names_query: list, names_candidate: list) -> list:
    """Pairwise token_sort_ratio between two equal-length name lists.
    """
    return [
        fuzz.token_sort_ratio(n1, n2)
        for n1, n2 in zip(names_query, names_candidate)
    ]


def fuzzy_match(df_games: pd.DataFrame, df_sales: pd.DataFrame, game_name_col: str = COL_STEAM_NAME, sales_name_col: str = COL_VG_NAME) -> pd.DataFrame:
    """Run TF-IDF candidate retrieval + RapidFuzz final scoring.
    Adds three columns to a copy of df_games:
      - matched_sales_name : best VG candidate title
      - cosine_distance    : TF-IDF cosine distance to that candidate
      - vg_match_score     : RapidFuzz token_sort_ratio (0-100)
    """
    vectorizer = TfidfVectorizer(analyzer='char_wb', ngram_range=(3, 5))
    sales_vectors = vectorizer.fit_transform(df_sales[sales_name_col].astype(str))
    games_vectors = vectorizer.transform(df_games[game_name_col].astype(str))

    nbrs = NearestNeighbors(n_neighbors=1, n_jobs=-1, metric='cosine')
    nbrs.fit(sales_vectors)
    distances, indices = nbrs.kneighbors(games_vectors)

    matched_indices = indices.ravel()
    out = df_games.copy()
    out['matched_sales_name'] = df_sales.loc[matched_indices, sales_name_col].values
    out['cosine_distance'] = distances.ravel()

    tqdm.pandas(desc='Computing fuzzy scores')
    out['vg_match_score'] = compute_fuzzy_scores(
        out[game_name_col].astype(str).tolist(),
        out['matched_sales_name'].astype(str).tolist(),
    )
    return out


def build_matched_table(scored: pd.DataFrame, df_vg_agg: pd.DataFrame, threshold: int = SIMILARITY_THRESHOLD) -> pd.DataFrame:
    """Filter ``scored`` by threshold and join aggregated vgsales columns.
    """
    matched = scored[scored['vg_match_score'] >= threshold].copy()
    joined = matched.merge(
        df_vg_agg,
        left_on='matched_sales_name',
        right_on=COL_VG_NAME,
        how='left',
    )
    joined = joined.rename(columns={**VG_RENAME, COL_VG_NAME: 'vg_name'})
    joined = joined.drop(columns=['matched_sales_name', 'cosine_distance'])
    return joined.sort_values(['vg_match_score', COL_STEAM_NAME], ascending=[False, True]).reset_index(drop=True)


def _score_bucket(score: int) -> str:
    if score >= 95:
        return '95-100'
    if score >= 90:
        return '90-94'
    if score >= 85:
        return '85-89'
    if score >= 83:
        return '83-84'
    if score >= 70:
        return '70-82 (below threshold)'
    return '<70 (below threshold)'


def quality_report(df_games: pd.DataFrame, df_vg_agg: pd.DataFrame, scored: pd.DataFrame, matched: pd.DataFrame, threshold: int = SIMILARITY_THRESHOLD) -> str:
    """Build a text report describing match coverage, score distribution, and risk signals.
    """
    sep = '=' * 60
    n_games = len(df_games)
    n_vg = len(df_vg_agg)
    n_matched = len(matched)
    n_unmatched = len(scored) - n_matched
    match_rate = n_matched / n_games * 100 if n_games else 0

    buckets = scored['vg_match_score'].map(_score_bucket).value_counts()
    bucket_order = [
        '95-100', '90-94', '85-89', '83-84',
        '70-82 (below threshold)', '<70 (below threshold)',
    ]

    scores = scored['vg_match_score']
    score_quantiles = scores.quantile([0.25, 0.5, 0.75]).to_dict()

    lines = [
        sep,
        'Cleaned_Data/games_vgsales_matched.csv  —  MATCH QUALITY REPORT',
        sep,
        '',
        '[1. COVERAGE SUMMARY]',
        f'  Steam input rows          : {n_games:,}',
        f'  VGChartz candidates (agg) : {n_vg:,}',
        f'  Matched rows              : {n_matched:,}',
        f'  Unmatched rows            : {n_unmatched:,}',
        f'  Match rate                : {match_rate:.2f}%',
        f'  Similarity threshold      : {threshold}',
        '',
        '[2. SCORE DISTRIBUTION (vg_match_score on ALL Steam rows)]',
        f'  Min    : {int(scores.min())}',
        f'  P25    : {int(score_quantiles.get(0.25, 0))}',
        f'  Median : {int(score_quantiles.get(0.5, 0))}',
        f'  P75    : {int(score_quantiles.get(0.75, 0))}',
        f'  Max    : {int(scores.max())}',
        '',
        '  Bucket                     Count',
    ]
    for b in bucket_order:
        lines.append(f'  {b:<26} {int(buckets.get(b, 0)):,}')

    low_cols = [COL_STEAM_ID, COL_STEAM_NAME, 'vg_name', 'vg_match_score']
    low_pass = matched[matched['vg_match_score'] < 88].sort_values('vg_match_score').head(15)
    lines += [
        '',
        '[3. MANUAL REVIEW QUEUE]',
        '',
        f'  3a) Low-score passes (score in [{threshold}, 87]) — spot-check for false positives:',
    ]
    if low_pass.empty:
        lines.append('    (none — all matches scored >= 88)')
    else:
        for _, r in low_pass[low_cols].iterrows():
            lines.append(
                f'    app_id={int(r[COL_STEAM_ID]):>10}  score={int(r["vg_match_score"]):>3}  '
                f'steam="{r[COL_STEAM_NAME]}"  vg="{r["vg_name"]}"'
            )

    high_pass = matched.sort_values('vg_match_score', ascending=False).head(10)
    lines += [
        '',
        '  3b) High-score matches — sanity check on strong cases:',
    ]
    for _, r in high_pass[low_cols].iterrows():
        lines.append(
            f'    app_id={int(r[COL_STEAM_ID]):>10}  score={int(r["vg_match_score"]):>3}  '
            f'steam="{r[COL_STEAM_NAME]}"  vg="{r["vg_name"]}"'
        )

    near_miss = (
        scored[(scored['vg_match_score'] >= threshold - 8) & (scored['vg_match_score'] < threshold)]
        .sort_values('vg_match_score', ascending=False)
        .head(15)
    )
    lines += [
        '',
        f'  3c) Near-miss unmatched (score in [{threshold - 8}, {threshold - 1}])'
        ' — potential threshold-tuning candidates:',
    ]
    if near_miss.empty:
        lines.append('    (none)')
    else:
        for _, r in near_miss.iterrows():
            lines.append(
                f'    app_id={int(r[COL_STEAM_ID]):>10}  score={int(r["vg_match_score"]):>3}  '
                f'steam="{r[COL_STEAM_NAME]}"  vg="{r["matched_sales_name"]}"'
            )

    lines += ['', '[4. POTENTIAL RISK SIGNALS]']

    vg_multi = (
        matched['vg_name'].value_counts()
        .loc[lambda s: s > 1]
        .head(15)
    )
    lines += ['', '  4a) VG titles matched by multiple Steam app_ids (possible duplicates / DLC):']
    if vg_multi.empty:
        lines.append('    (none)')
    else:
        for vg_name, cnt in vg_multi.items():
            steam_names = matched.loc[matched['vg_name'] == vg_name, COL_STEAM_NAME].head(5).tolist()
            sample = '; '.join(f'"{s}"' for s in steam_names)
            lines.append(f'    vg="{vg_name}"  hits={cnt}  steam_sample=[{sample}]')

    length_diff = matched.assign(
        _len_diff=lambda d: (
            d[COL_STEAM_NAME].astype(str).str.len()
            - d['vg_name'].astype(str).str.len()
        ).abs()
    )
    big_diff = length_diff.sort_values('_len_diff', ascending=False).head(10)
    lines += ['', '  4b) Matches with largest steam/vg name length gap:']
    for _, r in big_diff.iterrows():
        lines.append(
            f'    diff={int(r["_len_diff"]):>3}  score={int(r["vg_match_score"]):>3}  '
            f'steam="{r[COL_STEAM_NAME]}"  vg="{r["vg_name"]}"'
        )

    kw_regex = re.compile(r'\b(' + '|'.join(re.escape(k) for k in RISK_KEYWORDS) + r')\b', re.IGNORECASE)
    risky = matched[
        matched[COL_STEAM_NAME].astype(str).str.contains(kw_regex, na=False)
        ^ matched['vg_name'].astype(str).str.contains(kw_regex, na=False)
    ]
    lines += [
        '',
        '  4c) Version / edition mismatches (keyword on one side only):',
        f'     keywords: {", ".join(RISK_KEYWORDS)}',
    ]
    if risky.empty:
        lines.append('    (none)')
    else:
        for _, r in risky.head(15).iterrows():
            lines.append(
                f'    score={int(r["vg_match_score"]):>3}  '
                f'steam="{r[COL_STEAM_NAME]}"  vg="{r["vg_name"]}"'
            )

    lines.append('')
    return '\n'.join(lines)


if __name__ == '__main__':
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')

    print('--- Loading data ---')
    df_games = read_csv_with_encoding_fallbacks(
        INPUT_GAMES, [COL_STEAM_NAME, COL_STEAM_ID]
    )
    df_vg_raw = read_csv_with_encoding_fallbacks(
        INPUT_VGSALES, [COL_VG_NAME]
    )

    print(f'Steam games loaded       : {len(df_games):,}')
    print(f'VGChartz rows loaded     : {len(df_vg_raw):,}')

    df_vg_agg = aggregate_vgsales(df_vg_raw)
    print(f'VGChartz after aggregate : {len(df_vg_agg):,}')

    print('--- Running fuzzy match ---')
    scored = fuzzy_match(df_games, df_vg_agg, COL_STEAM_NAME, COL_VG_NAME)

    matched = build_matched_table(scored, df_vg_agg, SIMILARITY_THRESHOLD)
    print(f'Matched rows (score >= {SIMILARITY_THRESHOLD}) : {len(matched):,}')

    try:
        matched.to_csv(OUTPUT_MATCHED, index=False, encoding='utf-8-sig')
        print(f'Saved matched CSV  -> {OUTPUT_MATCHED}')
    except Exception as exc:
        print(f'Error saving matched CSV: {exc}')

    report = quality_report(df_games, df_vg_agg, scored, matched, SIMILARITY_THRESHOLD)
    try:
        with open(OUTPUT_QUALITY, 'w', encoding='utf-8') as f:
            f.write(report)
        print(f'Saved quality report -> {OUTPUT_QUALITY}')
    except Exception as exc:
        print(f'Error saving quality report: {exc}')
