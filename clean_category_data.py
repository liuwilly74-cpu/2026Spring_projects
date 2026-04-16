"""
clean_category_data.py
======================
Clean Data/categories.csv: map all 315 multilingual category variants
to the 46 canonical English Steam categories, deduplicate rows that
collapse to the same (app_id, category) pair after mapping, and write:

  - Cleaned_Data/categories_en.csv  : cleaned (app_id, category) table (English only)
  - distinct_categories.txt : data-quality report + distinct category list

Data quality issue found:
    The 315 raw "distinct" categories are actually ~46 standard Steam
    categories translated into 16 languages (EN/DE/NL/FR/ES/IT/PT/PL/
    FI/DA/NO/CZ/RU/UA/JA/ZH).

Pipeline:
    Data/categories.csv ──► Cleaned_Data/categories_en.csv
                        ──► distinct_categories.txt

Usage::

    python clean_category_data.py
"""

import pandas as pd


# File paths
INPUT_FILE = 'Data/categories.csv'
OUTPUT_CLEAN_CSV = 'Cleaned_Data/categories_en.csv'
OUTPUT_REPORT_TXT = 'Cleaned_Data/distinct_categories.txt'


# Canonical English category set (46 entries)
ENGLISH_CATEGORIES = {
    'Captions available',
    'Co-op',
    'Commentary available',
    'Cross-Platform Multiplayer',
    'Family Sharing',
    'Full controller support',
    'Game demo',
    'HDR available',
    'In-App Purchases',
    'Includes Source SDK',
    'Includes level editor',
    'LAN Co-op',
    'LAN PvP',
    'MMO',
    'Mods',
    'Mods (require HL2)',
    'Multi-player',
    'Multiplayer',
    'Online Co-op',
    'Online PvP',
    'Partial Controller Support',
    'PvP',
    'Remote Play Together',
    'Remote Play on Phone',
    'Remote Play on TV',
    'Remote Play on Tablet',
    'Shared/Split Screen',
    'Shared/Split Screen Co-op',
    'Shared/Split Screen PvP',
    'Single-player',
    'Stats',
    'Steam Achievements',
    'Steam Cloud',
    'Steam Leaderboards',
    'Steam Timeline',
    'Steam Trading Cards',
    'Steam Turn Notifications',
    'Steam Workshop',
    'SteamVR Collectibles',
    'Tracked Controller Support',
    'VR Only',
    'VR Support',
    'VR Supported',
    'Valve Anti-Cheat enabled',
}


# Full multilingual mapping: foreign / variant  →  canonical English
CATEGORY_MAPPING: dict[str, str] = {
    # ── Single-player ───────────────────────────────────────────────────────
    'Singleplayer':                           'Single-player',
    'Solo':                                   'Single-player',
    'Un jugador':                             'Single-player',
    'Um jogador':                             'Single-player',
    'Einzelspieler':                          'Single-player',
    'Enkeltspiller':                          'Single-player',
    'Giocatore singolo':                      'Single-player',
    'Jednoosobowa':                           'Single-player',
    'Yksinpeli':                              'Single-player',
    '\u5355\u4eba':                           'Single-player',   # 单人
    '\u55ae\u4eba':                           'Single-player',   # 單人
    '\u30b7\u30f3\u30b0\u30eb\u30d7\u30ec\u30a4\u30e4\u30fc': 'Single-player',  # シングルプレイヤー
    '\u0414\u043b\u044f \u043e\u0434\u043d\u043e\u0433\u043e \u0438\u0433\u0440\u043e\u043a\u0430': 'Single-player',  # Для одного игрока
    '\u041e\u0434\u043d\u043e\u043a\u043e\u0440\u0438\u0441\u0442\u0443\u0432\u0430\u0446\u044c\u043a\u0430 \u0433\u0440\u0430': 'Single-player',  # Однокористувацька гра (UA)
    'R\u017e\u00edm pro jednoho hr\u00e1\u010de': 'Single-player',  # Režim pro jednoho hráče (CZ)

    # ── Multi-player ────────────────────────────────────────────────────────
    'Multiplayer':                            'Multi-player',
    'Mehrspieler':                            'Multi-player',
    'Moninpeli':                              'Multi-player',
    'Multigiocatore':                         'Multi-player',
    'Multijogador':                           'Multi-player',
    'Multijoueur':                            'Multi-player',
    'Multijugador':                           'Multi-player',
    'Wieloosobowa':                           'Multi-player',
    '\u591a\u4eba':                           'Multi-player',   # 多人
    '\u30de\u30eb\u30c1\u30d7\u30ec\u30a4\u30e4\u30fc': 'Multi-player',  # マルチプレイヤー
    '\u0414\u043b\u044f \u043d\u0435\u0441\u043a\u043e\u043b\u044c\u043a\u0438\u0445 \u0438\u0433\u0440\u043e\u043a\u043e\u0432': 'Multi-player',  # Для нескольких игроков

    # ── Co-op ────────────────────────────────────────────────────────────────
    'Cooperativo':                            'Co-op',
    'Cooperativos':                           'Co-op',
    'Kooperacja':                             'Co-op',
    'Koop':                                   'Co-op',
    'Yhteisty\u00f6peli':                     'Co-op',  # Yhteistyöpeli (FI)
    'Coop\u00e9ration':                       'Co-op',  # Coopération (FR)
    '\u041a\u043e\u043e\u043f\u0435\u0440\u0430\u0442\u0438\u0432': 'Co-op',  # Кооператив
    '\u5408\u4f5c':                           'Co-op',  # 合作
    '\u5354\u529b\u30d7\u30ec\u30a4':         'Co-op',  # 協力プレイ

    # ── Online Co-op ─────────────────────────────────────────────────────────
    'Online co-op':                           'Online Co-op',
    'Partita cooperativa online':             'Online Co-op',
    'Online-Koop':                            'Online Co-op',
    'Sieciowa kooperacja':                    'Online Co-op',
    'Cooperativo en l\u00ednea':              'Online Co-op',  # Cooperativo en línea
    'Cooperativos en l\u00ednea':             'Online Co-op',  # Cooperativos en línea
    'Coop\u00e9ration en ligne':              'Online Co-op',  # Coopération en ligne
    'Verkkoyhteis\u00f6\u00f6peli':           'Online Co-op',  # Verkkoyhteisöpeli (may vary)
    'Verkkoyhteisty\u00f6peli':               'Online Co-op',  # Verkkoyhteistyöpeli (FI)
    '\u041a\u043e\u043e\u043f\u0435\u0440\u0430\u0442\u0438\u0432 (\u043f\u043e \u0441\u0435\u0442\u0438)': 'Online Co-op',  # Кооператив (по сети)
    '\u30aa\u30f3\u30e9\u30a4\u30f3\u5354\u529b\u30d7\u30ec\u30a4': 'Online Co-op',  # オンライン協力プレイ
    '\u5728\u7ebf\u5408\u4f5c':               'Online Co-op',  # 在线合作
    '\u7dda\u4e0a\u5408\u4f5c':               'Online Co-op',  # 線上合作

    # ── LAN Co-op ────────────────────────────────────────────────────────────
    'LAN \u2013 co-op':                       'LAN Co-op',   # en-dash variant
    '\u041a\u043e\u043e\u043f\u0435\u0440\u0430\u0442\u0438\u0432 (\u043b\u043e\u043a\u0430\u043b\u044c\u043d\u0430\u044f \u0441\u0435\u0442\u044c)': 'LAN Co-op',  # Кооператив (локальная сеть)
    '\u5c40\u57df\u7f51\u5408\u4f5c':         'LAN Co-op',   # 局域网合作

    # ── Shared/Split Screen ──────────────────────────────────────────────────
    'Geteilter Bildschirm':                   'Shared/Split Screen',
    'Pantalla dividida/compartida':           'Shared/Split Screen',
    'Pantalla partida/compartida':            'Shared/Split Screen',
    'Jaettu n\u00e4ytt\u00f6':               'Shared/Split Screen',  # Jaettu näyttö (FI)
    '\u041e\u0431\u0449\u0438\u0439/\u0440\u0430\u0437\u0434\u0435\u043b\u0451\u043d\u043d\u044b\u0439 \u044d\u043a\u0440\u0430\u043d': 'Shared/Split Screen',  # Общий/разделённый экран
    '\u540c\u5c4f/\u5206\u5c4f':             'Shared/Split Screen',  # 同屏/分屏

    # ── Shared/Split Screen Co-op ────────────────────────────────────────────
    'Coop. a pantalla (com)partida':          'Shared/Split Screen Co-op',
    'Coop locale et \u00e9cran partag\u00e9': 'Shared/Split Screen Co-op',  # Coop locale et écran partagé
    'Koop-Spiele mit geteiltem Bildschirm':   'Shared/Split Screen Co-op',
    'Jaetun n\u00e4yt\u00f6n yhteisty\u00f6peli': 'Shared/Split Screen Co-op',  # FI
    '\u041a\u043e\u043e\u043f\u0435\u0440\u0430\u0442\u0438\u0432 (\u043e\u0431\u0449\u0438\u0439/\u0440\u0430\u0437\u0434\u0435\u043b\u0451\u043d\u043d\u044b\u0439 \u044d\u043a\u0440\u0430\u043d)': 'Shared/Split Screen Co-op',
    '\u540c\u5c4f/\u5206\u5c4f\u5408\u4f5c': 'Shared/Split Screen Co-op',  # 同屏/分屏合作

    # ── Shared/Split Screen PvP ──────────────────────────────────────────────
    'JcJ a pantalla (com)partida':            'Shared/Split Screen PvP',
    'JcJ en pantalla dividida/compartida':    'Shared/Split Screen PvP',
    'Jaetun n\u00e4yt\u00f6n PvP':           'Shared/Split Screen PvP',  # FI
    '\u0418\u0433\u0440\u043e\u043a \u043f\u0440\u043e\u0442\u0438\u0432 \u0438\u0433\u0440\u043e\u043a\u0430 (\u043e\u0431\u0449\u0438\u0439/\u0440\u0430\u0437\u0434\u0435\u043b\u0451\u043d\u043d\u044b\u0439 \u044d\u043a\u0440\u0430\u043d)': 'Shared/Split Screen PvP',
    'PvP, ecr\u00e3 partilhado/dividido':    'Shared/Split Screen PvP',  # PT

    # ── PvP ──────────────────────────────────────────────────────────────────
    'JcJ':                                    'PvP',
    '\u0418\u0433\u0440\u043e\u043a \u043f\u0440\u043e\u0442\u0438\u0432 \u0438\u0433\u0440\u043e\u043a\u0430': 'PvP',  # Игрок против игрока
    '\u73a9\u5bb6\u5bf9\u6218':               'PvP',  # 玩家对战
    '\u73a9\u5bb6\u5c0d\u6230':               'PvP',  # 玩家對戰

    # ── Online PvP ───────────────────────────────────────────────────────────
    'Online-PvP':                             'Online PvP',
    'PvP online':                             'Online PvP',
    'JcJ en ligne':                           'Online PvP',
    'Verkko-PvP':                             'Online PvP',
    'JcJ en l\u00ednea':                      'Online PvP',  # JcJ en línea
    '\u0418\u0433\u0440\u043e\u043a \u043f\u0440\u043e\u0442\u0438\u0432 \u0438\u0433\u0440\u043e\u043a\u0430 (\u043f\u043e \u0441\u0435\u0442\u0438)': 'Online PvP',  # (по сети)
    '\u7dda\u4e0a\u73a9\u5bb6\u5c0d\u6230':   'Online PvP',  # 線上玩家對戰
    '\u7ebf\u4e0a\u73a9\u5bb6\u5bf9\u6218':   'Online PvP',  # 线上玩家对战

    # ── Cross-Platform Multiplayer ───────────────────────────────────────────
    'Multijoueur multiplateforme':            'Cross-Platform Multiplayer',
    'Multijugador multiplataforma':           'Cross-Platform Multiplayer',
    'Wieloplatformowa wieloosobowa':          'Cross-Platform Multiplayer',
    'Plattform\u00fcbergreifender Mehrspieler': 'Cross-Platform Multiplayer',  # DE
    '\u041a\u0440\u043e\u0441\u0441-\u043f\u043b\u0430\u0442\u0444\u043e\u0440\u043c\u0435\u043d\u043d\u044b\u0439 \u043c\u0443\u043b\u044c\u0442\u0438\u043f\u043b\u0435\u0435\u0440': 'Cross-Platform Multiplayer',
    '\u8de8\u5e73\u53f0\u591a\u4eba':         'Cross-Platform Multiplayer',  # 跨平台多人

    # ── MMO ──────────────────────────────────────────────────────────────────
    'Multijugador masivo':                    'MMO',
    '\u5927\u578b\u591a\u4eba\u7dda\u4e0a':   'MMO',   # 大型多人線上

    # ── Family Sharing ───────────────────────────────────────────────────────
    'Familiedeling':                          'Family Sharing',
    'Familienbibliothek':                     'Family Sharing',
    'Condivisione familiare':                 'Family Sharing',
    'Partage familial':                       'Family Sharing',
    'Partilha de Biblioteca':                 'Family Sharing',
    'Perhejako':                              'Family Sharing',
    'Udost\u0119pnianie gier':               'Family Sharing',   # Udostępnianie gier (PL)
    'Compartilhamento em fam\u00edlia':       'Family Sharing',   # PT
    'Pr\u00e9stamo familiar':                 'Family Sharing',   # ES
    'Sd\u00edlen\u00ed v rodin\u011b':        'Family Sharing',   # CZ
    '\u0421\u0435\u043c\u0435\u0439\u043d\u044b\u0439 \u0434\u043e\u0441\u0442\u0443\u043f': 'Family Sharing',  # RU
    '\u0421\u0456\u043c\u0435\u0439\u043d\u0430 \u0431\u0456\u0431\u043b\u0456\u043e\u0442\u0435\u043a\u0430': 'Family Sharing',  # UA
    '\u30d5\u30a1\u30df\u30ea\u30fc\u30b7\u30a7\u30a2\u30ea\u30f3\u30b0': 'Family Sharing',  # ファミリーシェアリング
    '\u5bb6\u5ead\u5171\u4eab':               'Family Sharing',   # 家庭共享
    '\u89aa\u53cb\u540c\u4eab':               'Family Sharing',   # 親友同享

    # ── Full controller support ──────────────────────────────────────────────
    'Supporto completo per i controller':     'Full controller support',
    'Compat. total c/ comando':               'Full controller support',
    'Compat. total com controle':             'Full controller support',
    'Compat. total con control':              'Full controller support',
    'Compat. total con mando':                'Full controller support',
    'Volledige controllerondersteuning':      'Full controller support',
    'Compat. contr\u00f4leurs compl\u00e8te': 'Full controller support',  # FR
    'Volle Controllerunterst\u00fctzung':     'Full controller support',  # DE
    'Fuld controllerunderst\u00f8ttelse':     'Full controller support',  # DA
    'T\u00e4ysi tuki ohjaimille':             'Full controller support',  # FI
    'Pe\u0142na obs\u0142uga kontroler\u00f3w': 'Full controller support',  # PL
    'Pln\u00e1 podpora ovlada\u010d\u016f':   'Full controller support',  # CZ
    '\u041f\u043e\u043b\u043d\u0430\u044f \u043f\u043e\u0434\u0434\u0435\u0440\u0436\u043a\u0430 \u043a\u043e\u043d\u0442\u0440\u043e\u043b\u043b\u0435\u0440\u043e\u0432': 'Full controller support',  # RU
    '\u041f\u043e\u0432\u043d\u0430 \u043f\u0456\u0434\u0442\u0440\u0438\u043c\u043a\u0430 \u043a\u043e\u043d\u0442\u0440\u043e\u043b\u0435\u0440\u0456\u0432': 'Full controller support',  # UA
    '\u30d5\u30eb\u30b3\u30f3\u30c8\u30ed\u30fc\u30e9\u30b5\u30dd\u30fc\u30c8': 'Full controller support',  # JA
    '\u5b8c\u5168\u652f\u6301\u63a7\u5236\u5668':  'Full controller support',  # 完全支持控制器
    '\u5b8c\u5168\u652f\u63f4\u63a7\u5236\u5668':  'Full controller support',  # 完全支援控制器

    # ── Partial Controller Support ───────────────────────────────────────────
    'Gedeeltelijke controllerondersteuning':  'Partial Controller Support',
    'Supporto parziale per i controller':     'Partial Controller Support',
    'Compat. parcial com controle':           'Partial Controller Support',
    'Compat. parcial con control':            'Partial Controller Support',
    'Compat. parcial con mando':              'Partial Controller Support',
    'Compat. contr\u00f4leurs partielle':     'Partial Controller Support',  # FR
    'Delvis controllerunderst\u00f8ttelse':   'Partial Controller Support',  # DA
    'Delvis st\u00f8tte for kontroller':      'Partial Controller Support',  # NO
    'Teilweise Controllerunterst\u00fctzung': 'Partial Controller Support',  # DE
    'Cz\u0119\u015bciowa obs\u0142uga kontroler\u00f3w': 'Partial Controller Support',  # PL
    '\u0427\u0430\u0441\u0442\u0438\u0447\u043d\u0430\u044f \u043f\u043e\u0434\u0434\u0435\u0440\u0436\u043a\u0430 \u043a\u043e\u043d\u0442\u0440\u043e\u043b\u043b\u0435\u0440\u043e\u0432': 'Partial Controller Support',  # RU
    '\u90e8\u5206\u652f\u6301\u63a7\u5236\u5668': 'Partial Controller Support',  # 部分支持控制器
    '\u90e8\u5206\u63a7\u5236\u5668\u652f\u63f4': 'Partial Controller Support',  # 部分控制器支援
    '\u90e8\u5206\u7684\u30b3\u30f3\u30c8\u30ed\u30fc\u30e9\u30b5\u30dd\u30fc\u30c8': 'Partial Controller Support',  # JA

    # ── Tracked Controller Support ───────────────────────────────────────────
    'Supporto per i controller tracciati':    'Tracked Controller Support',
    'Detecci\u00f3n de mov. en mando':        'Tracked Controller Support',  # ES
    '\u041f\u043e\u0434\u0434\u0435\u0440\u0436\u043a\u0430 \u043e\u0442\u0441\u043b\u0435\u0436\u0438\u0432\u0430\u043d\u0438\u044f \u043a\u043e\u043d\u0442\u0440\u043e\u043b\u043b\u0435\u0440\u043e\u0432': 'Tracked Controller Support',  # RU

    # ── Steam Achievements ───────────────────────────────────────────────────
    'Achievement di Steam':                   'Steam Achievements',
    'Achievementy':                           'Steam Achievements',
    'Conquistas Steam':                       'Steam Achievements',
    'Logros de Steam':                        'Steam Achievements',
    'Proezas do Steam':                       'Steam Achievements',
    'Steam-Errungenschaften':                 'Steam Achievements',
    'Steam-prestasjoner':                     'Steam Achievements',
    'Steam-saavutukset':                      'Steam Achievements',
    'Steam-prestaties':                       'Steam Achievements',
    'Steam-pr\u00e6stationer':               'Steam Achievements',  # DA
    'Succ\u00e8s Steam':                      'Steam Achievements',  # FR
    'Osi\u0105gni\u0119cia Steam':            'Steam Achievements',  # PL
    '\u0414\u043e\u0441\u0442\u0438\u0436\u0435\u043d\u0438\u044f Steam': 'Steam Achievements',  # RU
    '\u0414\u043e\u0441\u044f\u0433\u043d\u0435\u043d\u043d\u044f Steam': 'Steam Achievements',  # UA
    'Steam \u6210\u5c31':                     'Steam Achievements',  # Steam 成就
    'Steam\u5b9f\u7e3e':                      'Steam Achievements',  # Steam実績

    # ── Steam Trading Cards ──────────────────────────────────────────────────
    'Karty kolekcjonerskie Steam':            'Steam Trading Cards',
    'Cromos de Steam':                        'Steam Trading Cards',
    'Carte collezionabili di Steam':          'Steam Trading Cards',
    'Tarjetas de Steam':                      'Steam Trading Cards',
    'Steam-Sammelkarten':                     'Steam Trading Cards',
    'Steam-byttekort':                        'Steam Trading Cards',
    'Steam-ruilkaarten':                      'Steam Trading Cards',
    'Cartas Colecion\u00e1veis':              'Steam Trading Cards',  # PT
    'Cartas Colecion\u00e1veis Steam':        'Steam Trading Cards',  # PT
    'Cartes \u00e0 \u00e9changer Steam':      'Steam Trading Cards',  # FR
    'Sb\u011bratelsk\u00e9 karty':            'Steam Trading Cards',  # CZ
    'Steam-ker\u00e4ilykortit':               'Steam Trading Cards',  # FI
    '\u041a\u043e\u043b\u043b\u0435\u043a\u0446\u0438\u043e\u043d\u043d\u044b\u0435 \u043a\u0430\u0440\u0442\u043e\u0447\u043a\u0438 Steam': 'Steam Trading Cards',  # RU
    '\u041a\u043e\u043b\u0435\u043a\u0446\u0456\u0439\u043d\u0456 \u043a\u0430\u0440\u0442\u043a\u0438 Steam': 'Steam Trading Cards',  # UA
    'Steam \u4ea4\u63db\u5361\u7247':         'Steam Trading Cards',  # Steam 交換卡片
    'Steam \u96c6\u6362\u5f0f\u5361\u724c':   'Steam Trading Cards',  # Steam 集换式卡牌
    'Steam\u30c8\u30ec\u30fc\u30c7\u30a3\u30f3\u30b0\u30ab\u30fc\u30c9': 'Steam Trading Cards',  # JA

    # ── Steam Cloud ──────────────────────────────────────────────────────────
    'Nuvem Steam':                            'Steam Cloud',
    'Steam \u4e91':                           'Steam Cloud',  # Steam 云
    'Steam \u96f2\u7aef':                     'Steam Cloud',  # Steam 雲端
    'Steam\u30af\u30e9\u30a6\u30c9':          'Steam Cloud',  # Steamクラウド

    # ── Steam Leaderboards ───────────────────────────────────────────────────
    'Classements Steam':                      'Steam Leaderboards',
    'Classifiche di Steam':                   'Steam Leaderboards',
    'Marcadores de Steam':                    'Steam Leaderboards',
    'Steam-Bestenlisten':                     'Steam Leaderboards',
    'Classifica\u00e7\u00f5es Steam':         'Steam Leaderboards',  # PT
    'Tabelas de lideran\u00e7a do Steam':     'Steam Leaderboards',  # PT
    'Tablas de clasificaci\u00f3n de Steam':  'Steam Leaderboards',  # ES
    'Steam\u30e9\u30f3\u30ad\u30f3\u30b0':    'Steam Leaderboards',  # Steamランキング
    'Steam \u6392\u884c\u699c':               'Steam Leaderboards',  # Steam 排行榜
    '\u0422\u0430\u0431\u043b\u0438\u0446\u044b \u043b\u0438\u0434\u0435\u0440\u043e\u0432 Steam': 'Steam Leaderboards',  # RU
    'Steam-f\u00f8rertavler':                 'Steam Leaderboards',  # DA

    # ── Steam Workshop ───────────────────────────────────────────────────────
    'Warsztat Steam':                         'Steam Workshop',
    'Workshop Steam':                         'Steam Workshop',
    'Steam-v\u00e6rksted':                    'Steam Workshop',  # DA
    'Steam \u521b\u610f\u5de5\u5764':         'Steam Workshop',  # Steam 创意工坊
    'Steam \u5de5\u4f5c\u5764':               'Steam Workshop',  # Steam 工作坊
    '\u041c\u0430\u0441\u0442\u0435\u0440\u0441\u043a\u0430\u044f Steam': 'Steam Workshop',  # Мастерская Steam

    # ── Steam Timeline ───────────────────────────────────────────────────────
    'O\u015b czasu Steam':                    'Steam Timeline',  # PL
    '\u0412\u0440\u0435\u043c\u0435\u043d\u043d\u0430\u044f \u0448\u043a\u0430\u043b\u0430 Steam': 'Steam Timeline',  # RU

    # ── Stats ────────────────────────────────────────────────────────────────
    'Statistik':                              'Stats',
    'Statystyki':                             'Stats',
    'Estad\u00edsticas':                      'Stats',  # Estadísticas
    '\u0421\u0442\u0430\u0442\u0438\u0441\u0442\u0438\u043a\u0430': 'Stats',  # Статистика

    # ── In-App Purchases ─────────────────────────────────────────────────────
    'Achats en jeu':                          'In-App Purchases',
    "Acquisti dall'applicazione":             'In-App Purchases',
    'Compras em aplicativo':                  'In-App Purchases',
    'Zakupy w aplikacji':                     'In-App Purchases',
    'K\u00e4ufe im Spiel':                    'In-App Purchases',  # DE
    'K\u00f8b i app':                         'In-App Purchases',  # DA
    'Compras dentro de la aplicaci\u00f3n':   'In-App Purchases',  # ES
    '\u5185\u90e8\u6e38\u6232\u8cfc\u8cb7':   'In-App Purchases',  # (alternative ZH)
    '\u5185\u90e8\u8cfc\u8cb7':               'In-App Purchases',  # (alternative ZH)
    '\u30a2\u30d7\u30ea\u5185\u8cfc\u5165':   'In-App Purchases',  # アプリ内購入
    '\u5e94\u7528\u5185\u8d2d\u4e70':         'In-App Purchases',  # 应用内购买
    '\u0412\u043d\u0443\u0442\u0440\u0438\u0438\u0433\u0440\u043e\u0432\u044b\u0435 \u043f\u043e\u043a\u0443\u043f\u043a\u0438': 'In-App Purchases',  # RU

    # ── Game demo ────────────────────────────────────────────────────────────
    'Spieldemo':                              'Game demo',
    '\u0414\u0435\u043c\u043e\u0432\u0435\u0440\u0441\u0438\u044f \u0438\u0433\u0440\u044b': 'Game demo',  # Демоверсия игры
    '\u6e38\u620f\u8bd5\u7528\u7248':         'Game demo',  # 游戏试用版

    # ── Valve Anti-Cheat enabled ─────────────────────────────────────────────
    'Valve Anti-Cheat integriert':            'Valve Anti-Cheat enabled',
    'Valve Anti-Cheat sl\u00e6t til':         'Valve Anti-Cheat enabled',  # DA
    '\u0412\u043a\u043b\u044e\u0447\u0451\u043d \u0430\u043d\u0442\u0438\u0447\u0438\u0442 Valve': 'Valve Anti-Cheat enabled',  # RU
    'W\u0142\u0105czona funkcja Anti-Cheat':  'Valve Anti-Cheat enabled',  # PL

    # ── Captions available ───────────────────────────────────────────────────
    'Tekstitys':                              'Captions available',
    'Tekster tilg\u00e6ngelige':              'Captions available',  # DA
    'Legendas dispon\u00edveis':              'Captions available',  # PT
    'Subt\u00edtulos disponibles':            'Captions available',  # ES
    '\u041f\u043e\u0434\u0434\u0435\u0440\u0436\u043a\u0430 \u0441\u0443\u0431\u0442\u0438\u0442\u0440\u043e\u0432': 'Captions available',  # RU
    '\u652f\u6301\u5b57\u5e55':               'Captions available',  # 支持字幕

    # ── Commentary available ─────────────────────────────────────────────────
    'Comentario disponible':                  'Commentary available',
    '\u041f\u043e\u0434\u0434\u0435\u0440\u0436\u043a\u0430 \u043a\u043e\u043c\u043c\u0435\u043d\u0442\u0430\u0440\u0438\u0435\u0432': 'Commentary available',  # RU

    # ── HDR available ────────────────────────────────────────────────────────
    'HDR disponibili':                        'HDR available',
    'HDR disponible':                         'HDR available',
    '\u53ef\u7528 HDR':                       'HDR available',  # 可用 HDR

    # ── VR Support ───────────────────────────────────────────────────────────
    'Compatibile con VR':                     'VR Support',
    'Compatibilidad con RV':                  'VR Support',
    'Compatible con RV':                      'VR Support',
    '\u041f\u043e\u0434\u0434\u0435\u0440\u0436\u043a\u0430 VR': 'VR Support',  # RU

    # ── SteamVR Collectibles ─────────────────────────────────────────────────
    '\u041f\u0440\u0435\u0434\u043c\u0435\u0442\u044b \u0434\u043b\u044f SteamVR': 'SteamVR Collectibles',  # RU

    # ── Remote Play on Phone ─────────────────────────────────────────────────
    'Remote Play auf Smartphones':            'Remote Play on Phone',
    'Remote Play na telefonie':               'Remote Play on Phone',
    'Remote Play no celular':                 'Remote Play on Phone',
    'Remote Play op telefoons':               'Remote Play on Phone',
    'Remote Play sul telefono':               'Remote Play on Phone',
    'Remote Play sur t\u00e9l\u00e9phone':    'Remote Play on Phone',  # FR
    'Remote Play para m\u00f3viles':          'Remote Play on Phone',  # ES
    'Remote Play en m\u00f3vil':              'Remote Play on Phone',  # ES (post-normalization)
    'Remote Play \u043d\u0430 \u0442\u0435\u043b\u0435\u0444\u043e\u043d\u0435': 'Remote Play on Phone',  # RU
    '\u5728\u624b\u673a\u4e0a\u8fdc\u7a0b\u7545\u73a9': 'Remote Play on Phone',  # 在手机上远程畅玩

    # ── Remote Play on TV ────────────────────────────────────────────────────
    'Remote Play na TV':                      'Remote Play on TV',
    'Remote Play na telewizorze':             'Remote Play on TV',
    'Remote Play op televisies':              'Remote Play on TV',
    'Remote Play para TV':                    'Remote Play on TV',
    'Remote Play sulla TV':                   'Remote Play on TV',
    'Remote Play sur t\u00e9l\u00e9vision':   'Remote Play on TV',  # FR
    'Remote Play TV:ll\u00e4':                'Remote Play on TV',  # FI
    'Remote Play en TV':                      'Remote Play on TV',  # ES (post-normalization)
    'Remote Play \u043d\u0430 \u0442\u0435\u043b\u0435\u0432\u0438\u0437\u043e\u0440\u0435': 'Remote Play on TV',  # RU
    'Remote Play \u043d\u0430 \u0442\u0435\u043b\u0435\u0432\u0456\u0437\u043e\u0440\u0456': 'Remote Play on TV',  # UA
    '\u5728\u7535\u89c6\u4e0a\u8fdc\u7a0b\u7545\u73a9': 'Remote Play on TV',  # 在电视上远程畅玩
    '\u5728\u96fb\u8996\u4e0a\u9060\u7aef\u66a2\u73a9': 'Remote Play on TV',  # 在電視上遠端暢玩
    '\u30c6\u30ec\u30d3\u3067Remote Play':    'Remote Play on TV',  # JA

    # ── Remote Play on Tablet ────────────────────────────────────────────────
    'Remote Play auf Tablets':                'Remote Play on Tablet',
    'Remote Play na tablecie':                'Remote Play on Tablet',
    'Remote Play no tablet':                  'Remote Play on Tablet',
    'Remote Play op tablets':                 'Remote Play on Tablet',
    'Remote Play sul tablet':                 'Remote Play on Tablet',
    'Remote Play tabletilla':                 'Remote Play on Tablet',
    'Remote Play para tabletas':              'Remote Play on Tablet',
    'Remote Play sur tablette':               'Remote Play on Tablet',  # FR (post-norm)
    'Remote Play en tableta':                 'Remote Play on Tablet',  # ES (post-norm)
    'Remote Play \u043d\u0430 \u043f\u043b\u0430\u043d\u0448\u0435\u0442\u0435': 'Remote Play on Tablet',  # RU
    '\u5728\u5e73\u677f\u4e0a\u8fdc\u7a0b\u7545\u73a9': 'Remote Play on Tablet',  # 在平板上远程畅玩

    # ── Remote Play Together ─────────────────────────────────────────────────
    # Non-breaking-space variant handled by whitespace normalization → no entry needed
    '\u8fdc\u7a0b\u540c\u4e50':               'Remote Play Together',  # 远程同乐

    # ── Includes Source SDK ──────────────────────────────────────────────────
    '\u0421 \u0438\u043d\u0441\u0442\u0440\u0443\u043c\u0435\u043d\u0442\u0430\u043c\u0438 Source SDK': 'Includes Source SDK',  # RU

    # ── Includes level editor ────────────────────────────────────────────────
    'Indeholder baneeditor':                  'Includes level editor',
    'Sis\u00e4lt\u00e4\u00e4 tasoeditorin':   'Includes level editor',  # FI
    '\u0421 \u0440\u0435\u0434\u0430\u043a\u0442\u043e\u0440\u043e\u043c \u0443\u0440\u043e\u0432\u043d\u0435\u0439': 'Includes level editor',  # RU
    '\u5305\u542b\u5173\u5361\u7f16\u8f91\u5668': 'Includes level editor',  # 包含关卡编辑器
}


# Helper functions
def load_categories_csv(filepath: str) -> pd.DataFrame:
    """Read categories.csv (UTF-8) and return a typed DataFrame.

    filepath : str
        Path to the CSV file.

    pd.DataFrame
        ``app_id`` cast to int, ``category`` as str; calls exit() on failure.
        
    >>> import tempfile, os
    >>> tmp = tempfile.NamedTemporaryFile(mode='w', suffix='.csv',
    ...                                   delete=False, encoding='utf-8')
    >>> _ = tmp.write('app_id,category\\n1,Single-player\\n2,Multi-player\\n')
    >>> tmp.close()
    >>> df = load_categories_csv(tmp.name)
    >>> df['app_id'].tolist()
    [1, 2]
    >>> os.unlink(tmp.name)
    """
    try:
        df = pd.read_csv(filepath, encoding='utf-8')
        df['app_id'] = df['app_id'].astype(int)
        print(f"Loaded {len(df):,} rows from '{filepath}'.")
        return df
    except FileNotFoundError:
        print(f"Error: File not found '{filepath}'.")
        exit()
    except Exception as e:
        print(f"Error reading '{filepath}': {e}")
        exit()


def normalize_whitespace(series: pd.Series) -> pd.Series:
    """Replace non-breaking spaces (U+00A0) with regular spaces and strip.

    Several Steam category strings use U+00A0 (non-breaking space) instead
    of the regular ASCII space, e.g. 'Remote\u00a0Play\u00a0Together'.
    This step must run before the mapping lookup.

    series : pd.Series
        String Series to normalise.

    pd.Series
        Series with whitespace normalised.

    >>> import pandas as pd
    >>> s = pd.Series(['Remote\\u00a0Play\\u00a0Together', 'Single-player'])
    >>> normalize_whitespace(s).tolist()
    ['Remote Play Together', 'Single-player']
    """
    return series.str.replace('\u00a0', ' ', regex=False).str.strip()


def apply_mapping(series: pd.Series, mapping: dict) -> pd.Series:
    """Replace foreign-language categories with their canonical English form.

    Values already in English (not present in ``mapping``) are returned
    unchanged, so the function is safe to call on a mixed-language Series.

    series : pd.Series
        Whitespace-normalised category column.
    mapping : dict
        ``{foreign variant: canonical English}`` lookup dictionary.

    pd.Series
        Series with foreign values replaced; unknown values kept as-is.

    >>> import pandas as pd
    >>> s = pd.Series(['Single-player', 'Einzelspieler', 'Unknown'])
    >>> result = apply_mapping(s, {'Einzelspieler': 'Single-player'})
    >>> result.tolist()
    ['Single-player', 'Single-player', 'Unknown']
    """
    return series.map(lambda x: mapping.get(x, x))


def quality_report(
    df_raw: pd.DataFrame,
    df_clean: pd.DataFrame,
    english_cats: set,
    unmapped: list,
) -> str:
    """Build a formatted data-quality report string.
    """
    sep = '=' * 60
    lines = [
        sep,
        'Data/categories.csv  —  QUALITY REPORT',
        sep,
        '',
        '[Raw Data Stats]',
        f'  Total rows              : {len(df_raw):,}',
        f'  Unique app_ids          : {df_raw["app_id"].nunique():,}',
        f'  Missing values          : {df_raw.isnull().sum().sum()}',
        f'  Fully duplicate rows    : {df_raw.duplicated().sum()}',
        '',
        '[Categories per app_id (raw)]',
        f'  Mean : {df_raw.groupby("app_id")["category"].count().mean():.2f}',
        f'  Max  : {df_raw.groupby("app_id")["category"].count().max()}',
        f'  Min  : {df_raw.groupby("app_id")["category"].count().min()}',
        '',
        '[Distinct Category Breakdown (raw)]',
        f'  Total raw distinct   : {df_raw["category"].nunique()}',
        f'  After normalization  : {df_clean["category"].nunique()}',
        f'  Standard English set : {len(english_cats)}',
        '',
        '[DATA QUALITY ISSUE: Multilingual Duplicates]',
        '  The 315 raw "distinct" categories are actually 46 real',
        '  Steam categories translated into 16 languages:',
        '  English, German, Dutch, French, Spanish, Italian,',
        '  Portuguese, Polish, Finnish, Danish, Norwegian,',
        '  Czech, Russian, Ukrainian, Japanese, Chinese (S+T).',
        '',
        '[Cleaning Result]',
        f'  Rows before cleaning : {len(df_raw):,}',
        f'  Rows after cleaning  : {len(df_clean):,}',
        f'  Rows removed (dupes) : {len(df_raw) - len(df_clean):,}',
    ]
    if unmapped:
        lines += [
            '',
            f'[WARNING: {len(unmapped)} Unmapped / Unknown Categories (dropped)]',
        ]
        for u in sorted(unmapped):
            lines.append(f'  {u}')
    lines += ['', sep,
              f'DISTINCT ENGLISH CATEGORIES [{df_clean["category"].nunique()} total]',
              sep]
    freq = df_clean['category'].value_counts()
    for cat in sorted(df_clean['category'].unique()):
        lines.append(f'{cat}  ({freq[cat]:,} rows)')
    lines.append('')
    return '\n'.join(lines)



# Main Execution
if __name__ == '__main__':
    # Load raw data
    df = load_categories_csv(INPUT_FILE)
    df_raw = df.copy()

    df['category'] = normalize_whitespace(df['category'])

    # Map every foreign-language variant to its canonical English name
    df['category'] = apply_mapping(df['category'], CATEGORY_MAPPING)

    # Drop any remaining non-English categories that had no mapping entry
    unmapped_mask = ~df['category'].isin(ENGLISH_CATEGORIES)
    unmapped_cats = df.loc[unmapped_mask, 'category'].unique().tolist()
    if unmapped_cats:
        print(f"WARNING: {len(unmapped_cats)} unmapped categories will be dropped:")
        for c in sorted(unmapped_cats):
            print(f"  {c}")
    df = df[~unmapped_mask].copy()

    # Remove duplicates where two foreign variants mapped to the same English value
    df = df.drop_duplicates(subset=['app_id', 'category'])
    df = df.sort_values(by=['app_id', 'category']).reset_index(drop=True)

    try:
        df.to_csv(OUTPUT_CLEAN_CSV, index=False, encoding='utf-8-sig')
        print(f"\nSaved cleaned CSV ({len(df):,} rows) → {OUTPUT_CLEAN_CSV}")
    except Exception as e:
        print(f"Error saving CSV: {e}")

    # Write quality report and distinct category list to txt
    report = quality_report(df_raw, df, ENGLISH_CATEGORIES, unmapped_cats)
    try:
        with open(OUTPUT_REPORT_TXT, 'w', encoding='utf-8') as f:
            f.write(report)
        print(f"Saved report → {OUTPUT_REPORT_TXT}")
    except Exception as e:
        print(f"Error saving report: {e}")
