"""
note記事 分析スクリプト
articles.csv を読み込み、3種類の分析結果CSVを出力する

出力ファイル:
  - trend_analysis.csv  : トレンド分析（前日比・5日トレンド・状態ラベル）
  - asset_articles.csv  : 資産記事発掘（稼働日数・期間増加数・推移）
  - period_ranking.csv  : 期間ランキング（任意期間の増加数・1日平均）

使い方:
  python analyze.py [--days 7] [--articles data/articles.csv] [--out data/]
"""

import csv
import sys
import argparse
from pathlib import Path
from datetime import datetime, timezone, timedelta

# pandas が使える環境前提
try:
    import pandas as pd
except ImportError:
    sys.exit("❌ pandas が必要です: pip install pandas")

JST = timezone(timedelta(hours=9))


# ─────────────────────────────────────────────
# 定数（ヘッダー定義）
# ─────────────────────────────────────────────
TREND_HEADER = [
    "key", "title",
    "累計PV", "累計スキ",
    "前日比PV", "前日比スキ",
    "-7日PV", "-6日PV", "-5日PV", "-4日PV", "-3日PV", "-2日PV", "-1日PV",
    "-7日スキ", "-6日スキ", "-5日スキ", "-4日スキ", "-3日スキ", "-2日スキ", "-1日スキ",
    "7日合計PV", "7日合計スキ",
    "稼働数PV", "稼働数スキ",
    "トレンドスコア",
    "状態",
    "集計日",
]

ASSET_HEADER = [
    "key", "title",
    "期間増加PV", "期間増加スキ",
    "稼働日数",
    "推移PV",
    "集計日",
]

RANKING_HEADER = [
    "rank", "key", "title",
    "期間増加PV",
    "1日平均PV",
    "期間開始PV", "期間終了PV",
    "分析期間(日)",
    "集計日",
]


# ─────────────────────────────────────────────
# データ読み込み・ピボット
# ─────────────────────────────────────────────
def load_articles(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path, dtype=str).fillna("")
    return df


def build_pivot(df: pd.DataFrame, value_col: str) -> pd.DataFrame:
    """
    長形式 articles.csv を横形式（key × date）に変換する
    value_col: 'read_count' or 'like_count'
    """
    df_num = df.copy()
    df_num[value_col] = pd.to_numeric(df_num[value_col], errors="coerce").fillna(0)

    pivot = df_num.pivot_table(
        index="key",
        columns="date",
        values=value_col,
        aggfunc="last",
    ).fillna(0)

    # 列（日付）を昇順に並べる
    pivot = pivot[sorted(pivot.columns)]
    return pivot


def get_title_map(df: pd.DataFrame) -> dict:
    """key → title の辞書（最新日のタイトルを使う）"""
    latest_date = df["date"].max()
    latest = df[df["date"] == latest_date]
    return dict(zip(latest["key"], latest["title"]))


# ─────────────────────────────────────────────
# 分析① トレンド分析
# ─────────────────────────────────────────────
def analyze_trend(pv_pivot: pd.DataFrame, like_pivot: pd.DataFrame, title_map: dict) -> pd.DataFrame:
    """前日比・7日推移・稼働数・状態ラベルを計算する"""
    today_str = pv_pivot.columns[-1]

    # 日次増加数
    pv_daily   = pv_pivot.diff(axis=1).fillna(0)
    like_daily = like_pivot.diff(axis=1).fillna(0)

    # 前日比（最後の列）= -1日
    latest_pv_gain   = pv_daily.iloc[:, -1]
    latest_like_gain = like_daily.iloc[:, -1]

    # 7日分の日次推移（-7日〜-1日）
    pv_7d   = pv_daily.iloc[:, -7:]
    like_7d = like_daily.iloc[:, -7:]

    # 7日合計
    recent_7d_pv   = pv_7d.sum(axis=1)
    prev_7d_pv     = pv_daily.iloc[:, -14:-7].sum(axis=1)
    recent_7d_like = like_7d.sum(axis=1)

    # 稼働数（7日間のうち増加が1以上だった日数）
    active_pv   = (pv_7d > 0).sum(axis=1)
    active_like = (like_7d > 0).sum(axis=1)

    # トレンドスコア（直近7日 - その前7日）
    trend_score = recent_7d_pv - prev_7d_pv

    # 状態ラベル
    def get_status(row):
        pv_gain = row["前日比PV"]
        score   = row["トレンドスコア"]
        seven   = row["7日合計PV"]
        if pv_gain > 0 and score > 0:
            return "🔥 急上昇"
        elif pv_gain > 0:
            return "🟢 継続"
        elif seven > 0:
            return "⚠️ 減速"
        else:
            return "💤 停止"

    # 7日分の列名ラベル（-7日〜-1日）
    # データが7日未満のときは実際の列数に合わせてラベルを後ろから切り出す
    actual_days_pv   = pv_7d.shape[1]
    actual_days_like = like_7d.shape[1]
    day_labels_pv   = [f"-{i}日PV"   for i in range(7, 0, -1)][-actual_days_pv:]
    day_labels_like = [f"-{i}日スキ"  for i in range(7, 0, -1)][-actual_days_like:]

    pv_7d_df   = pv_7d.astype(int)
    like_7d_df = like_7d.astype(int)
    pv_7d_df.columns   = day_labels_pv
    like_7d_df.columns = day_labels_like

    result = pd.DataFrame({
        "key":        pv_pivot.index,
        "title":      pv_pivot.index.map(title_map),
        "累計PV":      pv_pivot.iloc[:, -1].astype(int),
        "累計スキ":     like_pivot.iloc[:, -1].astype(int),
        "前日比PV":     latest_pv_gain.astype(int),
        "前日比スキ":    latest_like_gain.astype(int),
    }).reset_index(drop=True)

    # 7日推移列を追加
    result = pd.concat([
        result,
        pv_7d_df.reset_index(drop=True),
        like_7d_df.reset_index(drop=True),
    ], axis=1)

    result["7日合計PV"]    = recent_7d_pv.astype(int).values
    result["7日合計スキ"]   = recent_7d_like.astype(int).values
    result["稼働数PV"]     = active_pv.astype(int).values
    result["稼働数スキ"]    = active_like.astype(int).values
    result["トレンドスコア"] = trend_score.astype(int).values

    result["状態"]   = result.apply(get_status, axis=1)
    result["集計日"] = today_str

    # データが7日未満のとき、足りない推移列を0で補完する
    all_pv_labels   = [f"-{i}日PV"  for i in range(7, 0, -1)]
    all_like_labels = [f"-{i}日スキ" for i in range(7, 0, -1)]
    for col in all_pv_labels + all_like_labels:
        if col not in result.columns:
            result[col] = 0

    return result.sort_values(["前日比PV", "7日合計PV"], ascending=False)


# ─────────────────────────────────────────────
# 分析② 資産記事発掘
# ─────────────────────────────────────────────
def analyze_assets(
    pv_pivot: pd.DataFrame,
    like_pivot: pd.DataFrame,
    title_map: dict,
    days_back: int = 7,
    min_active_days: int = 3,
    exclude_new: bool = True,
) -> pd.DataFrame:
    """稼働日数・期間増加数・推移で資産記事を発掘する"""
    today_str  = pv_pivot.columns[-1]
    data_len   = len(pv_pivot.columns)
    target_days = min(days_back, data_len - 1)

    start_col = pv_pivot.columns[-target_days - 1]
    end_col   = pv_pivot.columns[-1]

    print(f"  [資産記事] 期間: {start_col} 〜 {end_col}（{target_days}日間）")

    # 新着除外
    if exclude_new:
        mask = pv_pivot[start_col] > 0
        pv_work   = pv_pivot[mask].copy()
        like_work = like_pivot[mask].copy()
    else:
        pv_work   = pv_pivot.copy()
        like_work = like_pivot.copy()

    # 期間増加数
    pv_gain   = (pv_work[end_col]   - pv_work[start_col]).astype(int)
    like_gain = (like_work[end_col] - like_work[start_col]).astype(int)

    # 稼働日数（PVが増えた日数）
    pv_daily  = pv_work[pv_work.columns[-target_days:]].diff(axis=1).fillna(0)
    active_days = (pv_daily > 0).sum(axis=1).astype(int)

    # 推移文字列（最大14日まで）
    display_days = min(target_days, 14)
    recent_diffs = pv_work[pv_work.columns].diff(axis=1).fillna(0).iloc[:, -display_days:]

    def make_trend(row):
        return "→".join(str(int(v)) for v in row)

    trend_str = recent_diffs.apply(make_trend, axis=1)

    result = pd.DataFrame({
        "key":        pv_work.index,
        "title":      pv_work.index.map(title_map),
        "期間増加PV":   pv_gain,
        "期間増加スキ":  like_gain,
        "稼働日数":     active_days,
        "推移PV":      trend_str,
    }).reset_index(drop=True)

    result["集計日"] = today_str

    # フィルタ：増加がありかつ稼働日数が足切り以上
    result = result[
        (result["期間増加PV"] > 0) &
        (result["稼働日数"] >= min_active_days)
    ]

    return result.sort_values(["稼働日数", "期間増加PV"], ascending=False)


# ─────────────────────────────────────────────
# 分析③ 期間ランキング
# ─────────────────────────────────────────────
def analyze_period_ranking(
    pv_pivot: pd.DataFrame,
    title_map: dict,
    days_back: int = 30,
) -> pd.DataFrame:
    """任意期間の増加数・1日平均でランキングを生成する"""
    today_str   = pv_pivot.columns[-1]
    data_len    = len(pv_pivot.columns)
    target_days = min(days_back, data_len - 1)

    start_col = pv_pivot.columns[-target_days - 1]
    end_col   = pv_pivot.columns[-1]

    print(f"  [期間ランキング] 期間: {start_col} 〜 {end_col}（{target_days}日間）")

    pv_gain = (pv_pivot[end_col] - pv_pivot[start_col]).astype(int)

    result = pd.DataFrame({
        "key":          pv_pivot.index,
        "title":        pv_pivot.index.map(title_map),
        "期間増加PV":    pv_gain,
        "1日平均PV":     (pv_gain / target_days).round(2),
        "期間開始PV":    pv_pivot[start_col].astype(int),
        "期間終了PV":    pv_pivot[end_col].astype(int),
        "分析期間(日)":  target_days,
    }).reset_index(drop=True)

    result["集計日"] = today_str

    result = result[result["期間増加PV"] > 0].sort_values("期間増加PV", ascending=False)
    result.insert(0, "rank", range(1, len(result) + 1))

    return result


# ─────────────────────────────────────────────
# CSV保存
# ─────────────────────────────────────────────
def save_csv(df: pd.DataFrame, path: Path, columns: list):
    path.parent.mkdir(parents=True, exist_ok=True)
    df[columns].to_csv(path, index=False, encoding="utf-8")
    print(f"  ✅ {path} ({len(df)}件)")


# ─────────────────────────────────────────────
# メイン
# ─────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="note記事 分析スクリプト")
    parser.add_argument("--articles", default="data/articles.csv", help="articles.csv のパス")
    parser.add_argument("--out",      default="data/",             help="出力ディレクトリ")
    parser.add_argument("--days",     type=int, default=7,          help="資産記事・期間ランキングの分析日数")
    parser.add_argument("--min-active", type=int, default=3,        help="資産記事の最低稼働日数")
    parser.add_argument("--ranking-days", type=int, default=30,     help="期間ランキングの日数")
    parser.add_argument("--no-exclude-new", action="store_true",    help="新着記事を除外しない")
    args = parser.parse_args()

    articles_path = Path(args.articles)
    out_dir       = Path(args.out)

    print("=== note-stats-analyzer ===")
    print(f"入力: {articles_path}")
    print(f"出力: {out_dir}")

    if not articles_path.exists():
        sys.exit(f"❌ ファイルが見つかりません: {articles_path}")

    # データ読み込み
    print("\n📂 データ読み込み中...")
    df = load_articles(articles_path)
    title_map = get_title_map(df)

    dates = sorted(df["date"].unique())
    print(f"  期間: {dates[0]} 〜 {dates[-1]}（{len(dates)}日）")
    print(f"  記事数: {len(title_map)}")

    # ピボット生成
    pv_pivot   = build_pivot(df, "read_count")
    like_pivot = build_pivot(df, "like_count")

    # 分析実行
    print("\n📊 分析①: トレンド分析...")
    trend_df = analyze_trend(pv_pivot, like_pivot, title_map)

    print("\n📊 分析②: 資産記事発掘...")
    asset_df = analyze_assets(
        pv_pivot, like_pivot, title_map,
        days_back=args.days,
        min_active_days=args.min_active,
        exclude_new=not args.no_exclude_new,
    )

    print("\n📊 分析③: 期間ランキング...")
    ranking_df = analyze_period_ranking(pv_pivot, title_map, days_back=args.ranking_days)

    # 保存
    print("\n💾 保存中...")
    save_csv(trend_df,   out_dir / "trend_analysis.csv",  TREND_HEADER)
    save_csv(asset_df,   out_dir / "asset_articles.csv",  ASSET_HEADER)
    save_csv(ranking_df, out_dir / "period_ranking.csv",  RANKING_HEADER)

    # サマリー表示
    print("\n=== 完了 ===")
    status_counts = trend_df["状態"].value_counts()
    for status, count in status_counts.items():
        print(f"  {status}: {count}件")
    print(f"\n  資産記事ヒット: {len(asset_df)}件")
    print(f"  ランキング掲載: {len(ranking_df)}件")


if __name__ == "__main__":
    main()
