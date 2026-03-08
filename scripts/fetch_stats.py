"""
note記事データ日次取得スクリプト
GitHub Actionsで毎日実行し、記事ごとのビュー・スキ・コメントをCSVに蓄積する
"""

import os
import csv
import json
import time
import sys
from datetime import datetime, timezone, timedelta
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError
from pathlib import Path

# Windows環境でのUnicode出力対応
if sys.stdout.encoding and sys.stdout.encoding.lower() not in ("utf-8", "utf8"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

# ─────────────────────────────────────────────
# 定数
# ─────────────────────────────────────────────
BASE_URL = "https://note.com"
JST = timezone(timedelta(hours=9))
SCRIPT_DIR = Path(__file__).resolve().parent
DATA_DIR = SCRIPT_DIR.parent / "data"

# CSV ヘッダー定義（一か所で管理）
ARTICLES_HEADER = [
    "date", "note_id", "key", "title",
    "published_at", "created_at", "updated_at",
    "age_days", "read_count", "like_count", "comment_count",
]
SUMMARY_HEADER = [
    "日付", "ビュー合計", "スキ合計", "記事数",
    "ビュー/記事", "スキ/記事", "スキ率(%)",
    "ビュー前日比(%)", "スキ前日比(%)", "スキ率前日比(%)",
    "フォロワー数", "更新時刻",
]
FOLLOWERS_HEADER = ["日付", "時刻", "フォロワー数"]


# ─────────────────────────────────────────────
# 環境設定
# ─────────────────────────────────────────────
def load_dotenv():
    """簡易 .env 読み込み（python-dotenv 不要）"""
    env_path = SCRIPT_DIR.parent / ".env"
    if not env_path.exists():
        print(f"[dotenv] .envファイルが見つかりません: {env_path}")
        return
    print(f"[dotenv] .envファイル読み込み: {env_path}")
    loaded = []
    with open(env_path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, _, value = line.partition("=")
            key = key.strip()
            value = value.strip().strip('"').strip("'")
            if key not in os.environ:
                os.environ[key] = value
                loaded.append(key)
            else:
                print(f"[dotenv] {key}: 環境変数が既に設定済み（スキップ）")
    for key in loaded:
        val = os.environ[key]
        display = f"{val[:20]}...（{len(val)}文字）" if key == "NOTE_COOKIE" else val
        print(f"[dotenv] {key} = {display}")


load_dotenv()

NOTE_COOKIE    = os.environ.get("NOTE_COOKIE", "")
NOTE_USERNAME  = os.environ.get("NOTE_USERNAME", "")
COOKIE_SET_DATE = os.environ.get("COOKIE_SET_DATE", "")


# ─────────────────────────────────────────────
# Cookie / 認証チェック
# ─────────────────────────────────────────────
def get_today_jst() -> str:
    return datetime.now(JST).strftime("%Y-%m-%d")


def check_cookie_expiry():
    """Cookie の期限が近づいていたら警告"""
    if not COOKIE_SET_DATE:
        print("⚠ COOKIE_SET_DATE が未設定です。期限チェックをスキップします。")
        return
    try:
        set_date = datetime.strptime(COOKIE_SET_DATE, "%Y-%m-%d").replace(tzinfo=JST)
        days_elapsed = (datetime.now(JST) - set_date).days
        days_remaining = 90 - days_elapsed
        if days_remaining <= 0:
            print(f"🚨 Cookieが期限切れの可能性があります（設定から{days_elapsed}日経過）")
        elif days_remaining <= 10:
            print(f"⚠ Cookie期限まであと約{days_remaining}日です！早めに更新してください。")
        else:
            print(f"✓ Cookie期限: あと約{days_remaining}日")
    except ValueError:
        print(f"⚠ COOKIE_SET_DATE の形式が不正です: {COOKIE_SET_DATE}")


def validate_cookie():
    """Cookie 値の基本的な妥当性チェック"""
    if not NOTE_COOKIE:
        sys.exit("🚨 NOTE_COOKIE が空です。.env またはリポジトリの Secrets に設定してください。")
    if "=" not in NOTE_COOKIE:
        sys.exit(f"🚨 NOTE_COOKIE の形式が不正です（key=value 形式ではありません）。先頭: {NOTE_COOKIE[:30]}")
    if NOTE_COOKIE.startswith("NOTE_COOKIE="):
        sys.exit("🚨 NOTE_COOKIE の値に 'NOTE_COOKIE=' が含まれています。値だけを設定してください。")
    if len(NOTE_COOKIE) < 50:
        print(f"⚠ NOTE_COOKIE が短すぎます（{len(NOTE_COOKIE)}文字）。完全な Cookie ヘッダをコピーしたか確認してください。")
    print(f"[debug] Cookie先頭: {NOTE_COOKIE[:40]}... / {len(NOTE_COOKIE)}文字")


def _make_request(path: str) -> Request:
    req = Request(f"{BASE_URL}{path}")
    req.add_header("Cookie", NOTE_COOKIE)
    req.add_header("User-Agent", "note-stats-tracker")
    return req


def verify_auth():
    """stats API にアクセスできるか事前確認"""
    print("\n🔑 認証チェック中...")
    try:
        with urlopen(_make_request("/api/v1/stats/pv?filter=all&page=1&sort=pv")) as res:
            body = json.loads(res.read().decode("utf-8"))
        if "data" in body and "note_stats" in body["data"]:
            print("✓ 認証OK")
            return
        print("⚠ APIは応答しましたが stats データがありません。Cookie が無効な可能性があります。")
        print(f"  → レスポンスキー: {list(body.keys())}")
    except HTTPError as e:
        print(f"🚨 認証チェック失敗: HTTP {e.code}")
        if e.code in (401, 403):
            print("  → Cookie が無効です。ブラウザの DevTools から Cookie ヘッダを再取得してください。")
        try:
            print(f"  → レスポンス: {e.read().decode('utf-8')[:200]}")
        except Exception:
            pass
    except URLError as e:
        print(f"✗ 通信エラー: {e.reason}")
    sys.exit(1)


# ─────────────────────────────────────────────
# API
# ─────────────────────────────────────────────
def fetch_api(path: str) -> dict:
    """note の API を呼ぶ。失敗時は sys.exit"""
    try:
        with urlopen(_make_request(path)) as res:
            return json.loads(res.read().decode("utf-8"))
    except HTTPError as e:
        if e.code in (401, 403):
            sys.exit(f"🚨 認証エラー({e.code}): Secrets の NOTE_COOKIE を更新してください。")
        sys.exit(f"✗ HTTP エラー: {e.code}")
    except URLError as e:
        sys.exit(f"✗ 通信エラー: {e.reason}")


def fetch_all_articles() -> tuple[list[dict], int, int, int]:
    """全記事の stats を取得し (articles, total_pv, total_like, total_comment) を返す"""
    all_notes: list[dict] = []
    page = 1
    stats: dict = {}

    while True:
        print(f"  ページ {page} 取得中...")
        data = fetch_api(f"/api/v1/stats/pv?filter=all&page={page}&sort=pv")

        if "data" not in data or "note_stats" not in data["data"]:
            sys.exit("🚨 レスポンスにデータがありません。Cookie が無効な可能性があります。")

        stats = data["data"]
        all_notes.extend(stats["note_stats"])

        if stats.get("last_page", True):
            break
        page += 1
        time.sleep(1)

    total_pv      = stats.get("total_pv", 0)
    total_like    = stats.get("total_like", 0)
    total_comment = stats.get("total_comment", 0)
    print(f"  → {len(all_notes)}記事取得完了（総PV: {total_pv}, 総スキ: {total_like}）")
    return all_notes, total_pv, total_like, total_comment


def fetch_follower_count() -> int | None:
    """フォロワー数を取得。NOTE_USERNAME 未設定またはエラー時は None"""
    if not NOTE_USERNAME:
        print("⚠ NOTE_USERNAME が未設定です。フォロワー数取得をスキップします。")
        return None
    try:
        req = _make_request(f"/api/v2/creators/{NOTE_USERNAME}")
        with urlopen(req) as res:
            data = json.loads(res.read().decode("utf-8"))
        count = data.get("data", {}).get("followerCount")
        print(f"  → フォロワー数: {count}")
        return count
    except HTTPError as e:
        print(f"⚠ フォロワー数取得失敗（HTTP {e.code}）: NOTE_USERNAME={NOTE_USERNAME!r} を確認してください。スキップします。")
        return None
    except URLError as e:
        print(f"⚠ フォロワー数取得失敗（通信エラー: {e.reason}）: スキップします。")
        return None
    except Exception as e:
        print(f"⚠ フォロワー数取得で予期しないエラー: {e}。スキップします。")
        return None


# ─────────────────────────────────────────────
# v3 日時キャッシュ
# ─────────────────────────────────────────────
def _cache_path() -> Path:
    return DATA_DIR / "v3_dates_cache.json"


def load_dates_cache() -> dict:
    path = _cache_path()
    if not path.exists():
        return {}
    try:
        with open(path, encoding="utf-8") as f:
            raw: dict = json.load(f)
        # 旧形式（値が文字列）を新形式に移行
        migrated = {}
        for k, v in raw.items():
            if isinstance(v, str):
                migrated[k] = {"published_at": v, "created_at": "", "updated_at": "", "fetched_at": ""}
            else:
                migrated[k] = v
        return migrated
    except (json.JSONDecodeError, OSError):
        print("⚠ v3_dates_cache.json の読み込みに失敗。キャッシュを再構築します。")
        return {}


def save_dates_cache(cache: dict):
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    with open(_cache_path(), "w", encoding="utf-8") as f:
        json.dump(cache, f, ensure_ascii=False, indent=2)


def _is_cache_stale(entry: dict, today_str: str) -> bool:
    fetched_at = entry.get("fetched_at", "")
    if not fetched_at:
        return True
    try:
        return (datetime.strptime(today_str, "%Y-%m-%d") - datetime.strptime(fetched_at, "%Y-%m-%d")).days >= 7
    except ValueError:
        return True


def fetch_note_detail(note_key: str) -> dict:
    """v3 API から記事の日時を取得。エラー時は空文字を返す（sys.exit しない）"""
    try:
        with urlopen(_make_request(f"/api/v3/notes/{note_key}")) as res:
            data = json.loads(res.read().decode("utf-8")).get("data", {})
        published_at = ""
        for key in ("published_at", "publish_at", "first_published_at"):
            if data.get(key):
                published_at = data[key]
                break
        return {
            "published_at": published_at,
            "created_at":   data.get("created_at", ""),
            "updated_at":   data.get("updated_at", ""),
        }
    except (HTTPError, URLError) as e:
        print(f"    ⚠ v3 API エラー ({note_key}): {e}")
        return {"published_at": "", "created_at": "", "updated_at": ""}


def _calc_age_days(today_str: str, published_at: str) -> int | str:
    if not published_at:
        return ""
    try:
        pub_date  = datetime.fromisoformat(published_at).astimezone(JST).date()
        today_date = datetime.strptime(today_str, "%Y-%m-%d").date()
        return (today_date - pub_date).days
    except (ValueError, TypeError):
        return ""


def fetch_note_dates(articles: list[dict], today_str: str) -> list[dict]:
    """全記事の日時情報を取得（キャッシュ活用・7日で再取得）"""
    cache  = load_dates_cache()
    fetched = 0

    for note in articles:
        note_key = note["key"]
        entry = cache.get(note_key)
        if entry and not _is_cache_stale(entry, today_str):
            note.update({
                "published_at": entry["published_at"],
                "created_at":   entry["created_at"],
                "updated_at":   entry["updated_at"],
            })
        else:
            dates = fetch_note_detail(note_key)
            note.update(dates)
            cache[note_key] = {**dates, "fetched_at": today_str}
            fetched += 1
            if fetched % 10 == 0:
                print(f"    {fetched}件取得済み...")
            time.sleep(0.2)

        note["age_days"] = _calc_age_days(today_str, note["published_at"])

    cached = len(articles) - fetched
    print(f"  → {len(articles)}記事中 {fetched}件を v3 API から取得（{cached}件はキャッシュ）")
    save_dates_cache(cache)
    return articles


# ─────────────────────────────────────────────
# CSV 保存
# ─────────────────────────────────────────────
def _read_csv_keep_except(filepath: Path, skip_date: str, date_col: str) -> tuple[list[list], bool]:
    """
    CSV を読み込み、skip_date に一致する行を除いた残りと
    ヘッダーが期待通りかどうかを返す。
    ファイルが存在しなければ ([], False) を返す。
    """
    if not filepath.exists():
        return [], False
    with open(filepath, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        if date_col not in (reader.fieldnames or []):
            # ヘッダー不一致 → 旧形式
            return [], False
        rows = list(reader)

    removed = sum(1 for r in rows if r.get(date_col) == skip_date)
    if removed:
        print(f"  → {skip_date} の既存データ {removed} 行を上書きします")
    kept = [r for r in rows if r.get(date_col) != skip_date]
    return kept, True


def save_articles_csv(today: str, articles: list[dict]):
    """記事データを CSV に保存（同日データは上書き）"""
    filepath = DATA_DIR / "articles.csv"
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    existing, valid = _read_csv_keep_except(filepath, today, "date")

    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=ARTICLES_HEADER, extrasaction="ignore")
        writer.writeheader()
        # 既存行を書き戻す（欠けているキーは空文字で補完）
        for row in existing:
            writer.writerow({k: row.get(k, "") for k in ARTICLES_HEADER})
        # 新しい行
        for note in articles:
            writer.writerow({
                "date":          today,
                "note_id":       note["id"],
                "key":           note["key"],
                "title":         note["name"],
                "published_at":  note.get("published_at", ""),
                "created_at":    note.get("created_at", ""),
                "updated_at":    note.get("updated_at", ""),
                "age_days":      note.get("age_days", ""),
                "read_count":    note["read_count"],
                "like_count":    note["like_count"],
                "comment_count": note.get("comment_count", 0),
            })

    print(f"  → {filepath} に {len(articles)} 行書き込み")


def save_daily_summary_csv(
    today: str,
    total_pv: int,
    total_like: int,
    total_comment: int,
    article_count: int,
    follower_count: int | None,
):
    """日次サマリーを保存（前日比も自動計算、ヘッダー不一致時は再構築）"""
    filepath = DATA_DIR / "daily_summary.csv"
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    today_slash = today.replace("-", "/")

    # 指標計算
    v_per_a = total_pv  / article_count if article_count > 0 else 0
    l_per_a = total_like / article_count if article_count > 0 else 0
    l_rate  = (total_like / total_pv * 100) if total_pv > 0 else 0
    v_change = l_change = r_change = 0.0

    # 既存ファイルを読み込む
    existing_rows: list[dict] = []
    if filepath.exists():
        with open(filepath, mode="r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            file_headers = reader.fieldnames or []
            rows = list(reader)

        if "日付" in file_headers:
            # 前日比計算
            non_today = [r for r in rows if r.get("日付") != today_slash]
            if non_today:
                last = non_today[-1]
                try:
                    p_v = float(last.get("ビュー合計") or 0)
                    p_l = float(last.get("スキ合計")   or 0)
                    p_r = float(last.get("スキ率(%)")  or 0)
                    if p_v > 0: v_change = (total_pv   - p_v) / p_v * 100
                    if p_l > 0: l_change = (total_like - p_l) / p_l * 100
                    if p_r > 0: r_change = (l_rate     - p_r) / p_r * 100
                except (ValueError, TypeError):
                    pass
            existing_rows = non_today
        else:
            # 旧フォーマット → 破棄して新形式に移行
            print("  ⚠ daily_summary.csv が旧フォーマットのため新形式に移行します")
            existing_rows = []

    new_row = {
        "日付":            today_slash,
        "ビュー合計":       total_pv,
        "スキ合計":         total_like,
        "記事数":           article_count,
        "ビュー/記事":      round(v_per_a, 2),
        "スキ/記事":        round(l_per_a, 2),
        "スキ率(%)":        round(l_rate, 2),
        "ビュー前日比(%)":  round(v_change, 2),
        "スキ前日比(%)":    round(l_change, 2),
        "スキ率前日比(%)":  round(r_change, 2),
        "フォロワー数":     follower_count if follower_count is not None else "",
        "更新時刻":         datetime.now(JST).strftime("%H:%M:%S"),
    }

    with open(filepath, mode="w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=SUMMARY_HEADER, extrasaction="ignore")
        writer.writeheader()
        for r in existing_rows:
            writer.writerow({k: r.get(k, "") for k in SUMMARY_HEADER})
        writer.writerow(new_row)

    print(f"  → {filepath} を更新しました（{today_slash}）")


def save_followers_csv(follower_count: int | None):
    """
    フォロワー数が前回から変化したときだけ1行追加する。
    変化なし → スキップ、取得失敗 → スキップ。
    """
    if follower_count is None:
        print("  ⚠ フォロワー数が取得できなかったのでスキップ")
        return

    filepath = DATA_DIR / "followers.csv"
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    now_jst   = datetime.now(JST)
    date_str  = now_jst.strftime("%Y/%m/%d")
    time_str  = now_jst.strftime("%H:%M:%S")

    # 直近のフォロワー数を確認
    last_count: int | None = None
    if filepath.exists():
        with open(filepath, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            if "フォロワー数" in (reader.fieldnames or []):
                rows = list(reader)
                for row in reversed(rows):
                    val = row.get("フォロワー数", "").strip()
                    if val:
                        try:
                            last_count = int(val.replace(",", ""))
                        except ValueError:
                            pass
                        break

    if last_count == follower_count:
        print(f"  🟰 フォロワー数変化なし（{follower_count}）。書き込みスキップ")
        return

    # ヘッダーがなければ新規作成、あれば追記
    write_header = not filepath.exists() or filepath.stat().st_size == 0
    with open(filepath, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FOLLOWERS_HEADER)
        if write_header:
            writer.writeheader()
        writer.writerow({
            "日付":       date_str,
            "時刻":       time_str,
            "フォロワー数": follower_count,
        })

    prev_str = str(last_count) if last_count is not None else "不明"
    print(f"  ✅ フォロワー変化を検知 → 追記: {date_str} {time_str} {follower_count}（前回: {prev_str}）")


# ─────────────────────────────────────────────
# メイン
# ─────────────────────────────────────────────
def main():
    print("=== note-stats-tracker ===")
    today = get_today_jst()
    print(f"日付: {today}")

    validate_cookie()
    check_cookie_expiry()
    verify_auth()

    print("\n📊 記事データ取得中...")
    articles, total_pv, total_like, total_comment = fetch_all_articles()

    print("\n📅 日時情報（published_at等）取得中...")
    articles = fetch_note_dates(articles, today)

    print("\n👥 フォロワー数取得中...")
    follower_count = fetch_follower_count()

    DATA_DIR.mkdir(parents=True, exist_ok=True)

    print("\n💾 データ保存中...")
    save_articles_csv(today, articles)
    save_daily_summary_csv(today, total_pv, total_like, total_comment, len(articles), follower_count)
    save_followers_csv(follower_count)

    print(f"\n=== 完了 ===")
    print(f"記事数:       {len(articles)}")
    print(f"総PV:         {total_pv}")
    print(f"総スキ:       {total_like}")
    print(f"総コメント:   {total_comment}")
    if follower_count is not None:
        print(f"フォロワー:   {follower_count}")


if __name__ == "__main__":
    main()
