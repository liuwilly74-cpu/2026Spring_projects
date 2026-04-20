"""
Map all 121 multilingual genres to the 33 distinct English Steam genres, deduplicate rows.

Data quality issue found:
    The 121 raw distinct genres are actually 33 standard Steam genres
    translated into 16 languages (EN/DE/NL/FR/ES/IT/PT/PL/FI/DA/NO/CZ/RU/UA/JA/ZH).

Pipeline:
    Data/genres.csv --> Cleaned_Data/genres_clean.csv
                    --> Cleaned_Data/genres_quality_report.txt
"""

import sys
import pandas as pd

INPUT_FILE = 'Data/genres.csv'
OUTPUT_CLEAN_CSV = 'Cleaned_Data/genres_clean.csv'
OUTPUT_REPORT_TXT = 'Cleaned_Data/genres_quality_report.txt'


ENGLISH_GENRES = {
    '360 Video',
    'Accounting',
    'Action',
    'Adventure',
    'Animation & Modeling',
    'Audio Production',
    'Casual',
    'Design & Illustration',
    'Documentary',
    'Early Access',
    'Education',
    'Episodic',
    'Free To Play',
    'Game Development',
    'Gore',
    'Indie',
    'Massively Multiplayer',
    'Movie',
    'Nudity',
    'Photo Editing',
    'RPG',
    'Racing',
    'Sexual Content',
    'Short',
    'Simulation',
    'Software Training',
    'Sports',
    'Strategy',
    'Tutorial',
    'Utilities',
    'Video Production',
    'Violent',
    'Web Publishing',
}


# Full multilingual mapping: foreign / variant  →   English
GENRE_MAPPING: dict[str, str] = {
    # ── Action ──────────────────────────────────────────────────────────────
    'Экшены':                  'Action',       # RU
    'Бойовики':                'Action',       # UA
    'Acción':                  'Action',       # ES
    'Ação':                    'Action',       # PT
    'Azione':                  'Action',       # IT
    'Actie':                   'Action',       # NL
    'Akcja':                   'Action',       # PL
    'Akční':                   'Action',       # CZ
    'アクション':              'Action',       # JA
    '动作':                    'Action',       # ZH-S
    '動作':                    'Action',       # ZH-T

    # ── Adventure ───────────────────────────────────────────────────────────
    'Приключенческие игры':    'Adventure',    # RU
    'Пригоди':                 'Adventure',    # UA
    'Abenteuer':               'Adventure',    # DE
    'Aventura':                'Adventure',    # ES / PT
    'Aventure':                'Adventure',    # FR
    'Avontuur':                'Adventure',    # NL
    'Avventura':               'Adventure',    # IT
    'Dobrodružné':             'Adventure',    # CZ
    'Eventyr':                 'Adventure',    # DA / NO
    'Seikkailu':               'Adventure',    # FI
    'アドベンチャー':          'Adventure',    # JA
    '冒险':                    'Adventure',    # ZH-S
    '冒險':                    'Adventure',    # ZH-T

    # ── Casual ──────────────────────────────────────────────────────────────
    'Казуальные игры':         'Casual',       # RU
    'Казуальні ігри':          'Casual',       # UA
    'Gelegenheitsspiele':      'Casual',       # DE
    'Occasionnel':             'Casual',       # FR
    'Passatempo':              'Casual',       # IT / PT
    'カジュアル':              'Casual',       # JA
    '休闲':                    'Casual',       # ZH-S
    '休閒':                    'Casual',       # ZH-T

    # ── Early Access ────────────────────────────────────────────────────────
    'Ранний доступ':           'Early Access', # RU
    'Acceso anticipado':       'Early Access', # ES
    '抢先体验':                'Early Access', # ZH-S
    '搶先體驗':                'Early Access', # ZH-T

    # ── Free To Play ────────────────────────────────────────────────────────
    'Free to Play':            'Free To Play',
    'Free-to-play':            'Free To Play',
    'Бесплатные':              'Free To Play', # RU
    'Kostenlos spielbar':      'Free To Play', # DE
    'Grátis para Jogar':       'Free To Play', # PT
    '無料プレイ':              'Free To Play', # JA
    '免费开玩':                'Free To Play', # ZH-S

    # ── Indie ───────────────────────────────────────────────────────────────
    'Инди':                    'Indie',        # RU
    'Інді':                    'Indie',        # UA
    'Indépendant':             'Indie',        # FR
    'Niezależne':              'Indie',        # PL
    'インディー':              'Indie',        # JA
    '独立':                    'Indie',        # ZH-S
    '獨立製作':                'Indie',        # ZH-T

    # ── Massively Multiplayer ───────────────────────────────────────────────
    'Многопользовательские игры': 'Massively Multiplayer',  # RU
    'Massivement multijoueur': 'Massively Multiplayer',     # FR
    'Multijugador masivo':     'Massively Multiplayer',     # ES
    'MMO':                     'Massively Multiplayer',
    '大型多人連線':            'Massively Multiplayer',     # ZH-T

    # ── RPG ─────────────────────────────────────────────────────────────────
    'Ролевые игры':            'RPG',          # RU
    'Rol':                     'RPG',          # ES
    'Rollenspiel':             'RPG',          # DE
    'GDR':                     'RPG',          # IT (Gioco di Ruolo)
    'Roolipelit':              'RPG',          # FI
    '角色扮演':                'RPG',          # ZH-S / ZH-T

    # ── Racing ──────────────────────────────────────────────────────────────
    'Гонки':                   'Racing',       # RU
    'Carreras':                'Racing',       # ES
    'Course automobile':       'Racing',       # FR
    'Race':                    'Racing',

    # ── Simulation ──────────────────────────────────────────────────────────
    'Симуляторы':              'Simulation',   # RU
    'Simuladores':             'Simulation',   # ES / PT
    'Simulationen':            'Simulation',   # DE
    'Simulatie':               'Simulation',   # NL
    'Simulaatio':              'Simulation',   # FI
    'Simulação':               'Simulation',   # PT
    'Simulering':              'Simulation',   # DA / NO
    '模拟':                    'Simulation',   # ZH-S
    '模擬':                    'Simulation',   # ZH-T

    # ── Sports ──────────────────────────────────────────────────────────────
    'Спортивные игры':         'Sports',       # RU
    'Deportes':                'Sports',       # ES
    'Sport':                   'Sports',       # DE / NL / IT
    '体育':                    'Sports',       # ZH-S

    # ── Strategy ────────────────────────────────────────────────────────────
    'Стратегии':               'Strategy',     # RU
    'Estrategia':              'Strategy',     # ES
    'Estratégia':              'Strategy',     # PT
    'Stratégie':               'Strategy',     # FR / CZ
    'Strategie':               'Strategy',     # DE
    'Strategia':               'Strategy',     # PL
    'Strategi':                'Strategy',     # DA / NO / SV
    'ストラテジー':            'Strategy',     # JA
    '策略':                    'Strategy',     # ZH-S / ZH-T

    # ── Utilities ───────────────────────────────────────────────────────────
    'Utilidades':              'Utilities',    # ES / PT
}


def load_genres_csv(filepath: str) -> pd.DataFrame:
    """Read genres.csv with fallback encodings and return a typed DataFrame.
    filepath : str
        Path to the CSV file.
    pd.DataFrame
        DataFrame with 'app_id' as int, 'genre' as str.
    """
    for enc in ['utf-8', 'GB18030', 'cp1251', 'koi8-r', 'latin1']:
        try:
            df = pd.read_csv(filepath, encoding=enc)
            df = df.dropna(subset=['app_id', 'genre'])
            df['app_id'] = df['app_id'].astype(int)
            df['genre'] = df['genre'].str.strip()
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
    >>> s = pd.Series(['Free\\u00a0To\\u00a0Play', 'Action'])
    >>> normalize_whitespace(s).tolist()
    ['Free To Play', 'Action']
    """
    return series.str.replace('\u00a0', ' ', regex=False).str.strip()


def apply_mapping(series: pd.Series, mapping: dict) -> pd.Series:
    """Replace foreign-language genres with their canonical English form.

    series : pd.Series
        Whitespace-normalised genre column.
    mapping : dict
        {foreign variant: canonical English} lookup dictionary.
    pd.Series
        Series with foreign values replaced; unknown values kept as-is.

    >>> import pandas as pd
    >>> s = pd.Series(['Action', 'Экшены', 'Unknown'])
    >>> result = apply_mapping(s, {'Экшены': 'Action'})
    >>> result.tolist()
    ['Action', 'Action', 'Unknown']
    """
    return series.map(lambda x: mapping.get(x, x))


def quality_report(df_raw: pd.DataFrame, df_clean: pd.DataFrame, english_genres: set, unmapped: list) -> str:
    sep = '=' * 60
    attr = 'genre'
    per_app = df_raw.groupby('app_id')[attr].count()
    non_ascii = [v for v in df_raw[attr].dropna().unique() if not str(v).isascii()]
    freq = df_clean[attr].value_counts()

    lines = [
        sep,
        'Data/genres.csv  —  QUALITY REPORT',
        sep,
        '',
        '[1. Raw Data Stats]',
        f'  Total rows              : {len(df_raw):,}',
        f'  Unique app_ids          : {df_raw["app_id"].nunique():,}',
        f'  Missing genre           : {df_raw[attr].isna().sum():,}',
        f'  Fully duplicate rows    : {df_raw.duplicated().sum():,}',
        f'  Duplicate (app_id, genre): {df_raw.duplicated(subset=["app_id", attr]).sum():,}',
        '',
        '[2. Genre per app_id]',
        f'  Mean : {per_app.mean():.2f}',
        f'  Max  : {per_app.max()}',
        f'  Min  : {per_app.min()}',
        '',
        '[3. Language Check]',
        f'  Total raw distinct      : {df_raw[attr].nunique():,}',
        f'  Non-ASCII values        : {len(non_ascii):,}',
        f'  After normalization     : {df_clean[attr].nunique():,}',
        f'  Standard English set    : {len(english_genres):,}',
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
        f'DISTINCT GENRE VALUES [{df_clean[attr].nunique()} total, sorted by frequency]',
        sep,
    ]
    for val in freq.index:
        lines.append(f'{val}  ({freq[val]:,} rows)')
    lines.append('')
    return '\n'.join(lines)


if __name__ == '__main__':
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')
    df = load_genres_csv(INPUT_FILE)
    df_raw = df.copy()
    df['genre'] = normalize_whitespace(df['genre'])
    df['genre'] = apply_mapping(df['genre'], GENRE_MAPPING)


    unmapped_mask = ~df['genre'].isin(ENGLISH_GENRES)
    unmapped_genres = df.loc[unmapped_mask, 'genre'].unique().tolist()
    if unmapped_genres:
        print(f"WARNING: {len(unmapped_genres)} unmapped genres will be dropped:")
        for g in sorted(unmapped_genres):
            print(f"  {g}")
    df = df[~unmapped_mask].copy()


    df = df.drop_duplicates(subset=['app_id', 'genre'])
    df = df.sort_values(by=['app_id', 'genre']).reset_index(drop=True)

    try:
        df.to_csv(OUTPUT_CLEAN_CSV, index=False, encoding='utf-8-sig')
        print(f"\nSaved cleaned CSV ({len(df):,} rows) → {OUTPUT_CLEAN_CSV}")
    except Exception as e:
        print(f"Error saving CSV: {e}")


    report = quality_report(df_raw, df, ENGLISH_GENRES, unmapped_genres)
    try:
        with open(OUTPUT_REPORT_TXT, 'w', encoding='utf-8') as f:
            f.write(report)
        print(f"Saved report → {OUTPUT_REPORT_TXT}")
    except Exception as e:
        print(f"Error saving report: {e}")
