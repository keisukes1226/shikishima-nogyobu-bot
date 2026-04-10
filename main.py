"""
しきしまの家 営農部 LINE業務サポートボット
- グループメッセージを自動記録（SQLite DB）
- 返信が必要なメッセージを検知して通知
- 作業報告を業務区分別に自動分類
"""

import os
import json
import sqlite3
from datetime import datetime, timedelta
from flask import Flask, request, abort
from linebot import LineBotApi, WebhookHandler
from linebot.exceptions import InvalidSignatureError
from linebot.models import (
    MessageEvent, TextMessage, TextSendMessage, JoinEvent
)
import anthropic
from googleapiclient.discovery import build
from google.oauth2 import service_account

app = Flask(__name__)

# 環境変数から認証情報を取得
LINE_CHANNEL_SECRET = os.environ.get('LINE_CHANNEL_SECRET', '')
LINE_CHANNEL_ACCESS_TOKEN = os.environ.get('LINE_CHANNEL_ACCESS_TOKEN', '')
ANTHROPIC_API_KEY = os.environ.get('ANTHROPIC_API_KEY', '')

line_bot_api = LineBotApi(LINE_CHANNEL_ACCESS_TOKEN)
handler = WebhookHandler(LINE_CHANNEL_SECRET)

DB_PATH = os.environ.get('DB_PATH', 'messages.db')

# Google Calendar設定
GOOGLE_SERVICE_ACCOUNT_JSON = os.environ.get('GOOGLE_SERVICE_ACCOUNT_JSON', '')
GOOGLE_CALENDAR_ID = os.environ.get('GOOGLE_CALENDAR_ID', 'shikishimaeinou@gmail.com')

# 業務区分（17区分）
WORK_CATEGORIES = [
    # 時給制
    "水稲", "大豆", "野菜", "農機・施設管理", "除草・畔草刈",
    "水管理・用水路", "環境整備", "共同作業一般", "組合業務", "研修・視察",
    # 出来高制チーム
    "くるみ（脱穀）", "くるみ（選別）", "くるみ（その他）",
    # 出来高制個人
    "野菜（個人）", "米（個人）", "くるみ（個人）", "その他個人"
]

# スルー検知の閾値（時間）
UNANSWERED_THRESHOLD_HOURS = 24


# ==================== DB初期化 ====================

def init_db():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('''
        CREATE TABLE IF NOT EXISTS messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT NOT NULL,
            group_id TEXT NOT NULL,
            user_id TEXT,
            user_name TEXT,
            message TEXT,
            message_id TEXT UNIQUE,
            needs_reply INTEGER DEFAULT 0,
            replied INTEGER DEFAULT 0,
            work_category TEXT,
            work_hours REAL,
            work_date TEXT,
            work_style TEXT,
            raw_analysis TEXT
        )
    ''')
    c.execute('''
        CREATE TABLE IF NOT EXISTS groups (
            group_id TEXT PRIMARY KEY,
            group_name TEXT,
            joined_at TEXT
        )
    ''')
    conn.commit()
    conn.close()


init_db()


# ==================== Webhook受信 ====================

@app.route("/callback", methods=['POST'])
def callback():
    signature = request.headers.get('X-Line-Signature', '')
    body = request.get_data(as_text=True)
    try:
        handler.handle(body, signature)
    except InvalidSignatureError:
        abort(400)
    return 'OK'


# ==================== グループ参加イベント ====================

@handler.add(JoinEvent)
def handle_join(event):
    if event.source.type == 'group':
        group_id = event.source.group_id
        # グループ登録
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute('''
            INSERT OR IGNORE INTO groups (group_id, joined_at)
            VALUES (?, ?)
        ''', (group_id, datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
        conn.commit()
        conn.close()

        line_bot_api.reply_message(
            event.reply_token,
            TextSendMessage(
                text="こんにちは！しきしまの家 営農部サポートボットです🌾\n\n"
                     "作業報告を自動で記録し、返信が必要なメッセージをお知らせします。\n"
                     "よろしくお願いします！\n\n"
                     "【コマンド一覧】\n"
                     "「/集計」→ 今月の作業時間集計\n"
                     "「/未返信」→ 返信待ちメッセージ一覧\n"
                     "「/ヘルプ」→ 使い方"
            )
        )


# ==================== メッセージ受信 ====================

@handler.add(MessageEvent, message=TextMessage)
def handle_message(event):
    # グループメッセージのみ処理
    if event.source.type != 'group':
        return

    group_id = event.source.group_id
    user_id = event.source.user_id
    message_text = event.message.text.strip()
    message_id = event.message.id
    timestamp = datetime.fromtimestamp(
        event.timestamp / 1000
    ).strftime('%Y-%m-%d %H:%M:%S')

    # ユーザー名取得
    user_name = get_user_name(group_id, user_id)

    # コマンド処理
    if message_text.startswith('/'):
        handle_command(event, message_text, group_id)
        return

    # Claude APIで解析
    analysis = analyze_message(message_text, user_name)

    # DB保存
    save_message(
        timestamp, group_id, user_id, user_name,
        message_text, message_id, analysis
    )

    # スルー検知チェック（返信があった場合、前のメッセージを既読マーク）
    mark_replied_context(group_id, user_id, message_text)

    # 24時間以上スルーされているメッセージがあれば通知
    # （毎回チェックするとうるさいので、一定条件で実行）
    hour = datetime.now().hour
    if hour == 9 and datetime.now().minute < 5:  # 朝9時のメッセージで1回だけ
        notify_unanswered(group_id)


def get_user_name(group_id, user_id):
    try:
        profile = line_bot_api.get_group_member_profile(group_id, user_id)
        return profile.display_name
    except Exception:
        return user_id


# ==================== コマンド処理 ====================

def handle_command(event, text, group_id):
    cmd = text.lower().strip()

    if cmd in ['/集計', '/summary']:
        reply = get_monthly_summary(group_id)
    elif cmd in ['/未返信', '/unanswered']:
        reply = get_unanswered_list(group_id)
    elif cmd in ['/ヘルプ', '/help']:
        reply = (
            "【コマンド一覧】\n"
            "「/集計」→ 今月の作業時間集計\n"
            "「/未返信」→ 返信待ちメッセージ一覧\n"
            "「/ヘルプ」→ この使い方\n\n"
            "作業報告メッセージは自動で記録されます📝"
        )
    else:
        return  # 不明コマンドは無視

    line_bot_api.reply_message(
        event.reply_token,
        TextSendMessage(text=reply)
    )


def get_monthly_summary(group_id):
    today = datetime.now()
    month_start = today.strftime('%Y-%m-01')
    month_end = today.strftime('%Y-%m-%d')

    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('''
        SELECT work_category, SUM(work_hours), COUNT(*)
        FROM messages
        WHERE group_id = ?
          AND work_category IS NOT NULL
          AND work_date BETWEEN ? AND ?
        GROUP BY work_category
        ORDER BY SUM(work_hours) DESC
    ''', (group_id, month_start, month_end))
    rows = c.fetchall()
    conn.close()

    if not rows:
        return f"📊 {today.month}月の作業記録はまだありません。"

    lines = [f"📊 {today.month}月の作業時間集計\n"]
    total = 0
    for cat, hours, count in rows:
        h = hours or 0
        total += h
        lines.append(f"  {cat}: {h:.1f}h ({count}件)")
    lines.append(f"\n合計: {total:.1f}h")
    return "\n".join(lines)


def get_unanswered_list(group_id):
    threshold = (
        datetime.now() - timedelta(hours=UNANSWERED_THRESHOLD_HOURS)
    ).strftime('%Y-%m-%d %H:%M:%S')

    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('''
        SELECT user_name, message, timestamp
        FROM messages
        WHERE group_id = ? AND needs_reply = 1 AND replied = 0
          AND timestamp < ?
        ORDER BY timestamp DESC
        LIMIT 10
    ''', (group_id, threshold))
    rows = c.fetchall()
    conn.close()

    if not rows:
        return "✅ 返信待ちのメッセージはありません！"

    lines = [f"⚠️ {UNANSWERED_THRESHOLD_HOURS}時間以上返信待ちのメッセージ:\n"]
    for user_name, message, ts in rows:
        short_msg = message[:40] + "..." if len(message) > 40 else message
        date_str = ts[:10]
        lines.append(f"👤 {user_name}（{date_str}）\n   「{short_msg}」")
    return "\n\n".join(lines)


# ==================== スルー検知 ====================

def notify_unanswered(group_id):
    msg = get_unanswered_list(group_id)
    if "返信待ち" in msg and "✅" not in msg:
        line_bot_api.push_message(
            group_id,
            TextSendMessage(text=msg)
        )


def mark_replied_context(group_id, user_id, message_text):
    """メッセージ送信者自身の直前の質問は返信済みにする（シンプルな実装）"""
    # 実際には文脈解析が必要だが、まずはシンプルに
    # 「確認しました」「了解」「ありがとう」等のキーワードで判定
    reply_keywords = ['確認', '了解', 'わかりました', 'ありがとう', 'ok', 'OK', '👍', '✅']
    if any(kw in message_text for kw in reply_keywords):
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        # 最近の返信待ちメッセージを返信済みに（直近5件を対象）
        c.execute('''
            UPDATE messages SET replied = 1
            WHERE group_id = ? AND needs_reply = 1 AND replied = 0
            AND id IN (
                SELECT id FROM messages
                WHERE group_id = ? AND needs_reply = 1 AND replied = 0
                ORDER BY timestamp DESC LIMIT 5
            )
        ''', (group_id, group_id))
        conn.commit()
        conn.close()


# ==================== Claude APIによる解析 ====================

def analyze_message(text, user_name):
    """Claude APIでメッセージを解析"""
    if not ANTHROPIC_API_KEY:
        return _simple_analyze(text)

    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)

    categories_str = "、".join(WORK_CATEGORIES)
    prompt = f"""農業法人のLINEグループメッセージを分析してください。

送信者: {user_name}
メッセージ: {text}

以下をJSON形式で返してください（説明文不要、JSONのみ）:
{{
  "needs_reply": true/false,
  "work_category": "{categories_str} のいずれか、または null",
  "work_hours": 作業時間（数値）または null,
  "work_date": "YYYY-MM-DD" または null,
  "work_style": "本田+荻原" または "荻原（単独）" または "本田（単独）" または null
}}

判断基準:
- needs_reply: 質問・依頼・確認要求・スケジュール調整など返答が必要なら true
- work_category: 作業報告が含まれていれば分類、なければ null
- work_hours: 「3時間」「2h」「午前中」（3h換算）等から抽出
- work_date: 「今日」「昨日」「12/15」等から推定（今日={datetime.now().strftime('%Y-%m-%d')}）
- work_style: 2人作業・本田さんと・荻原さんと等から判定"""

    try:
        response = client.messages.create(
            model="claude-3-haiku-20240307",
            max_tokens=300,
            messages=[{"role": "user", "content": prompt}]
        )
        result_text = response.content[0].text.strip()
        # JSON部分を抽出
        if '{' in result_text:
            result_text = result_text[result_text.index('{'):result_text.rindex('}') + 1]
        return json.loads(result_text)
    except Exception as e:
        print(f"Claude API error: {e}")
        return _simple_analyze(text)


def _simple_analyze(text):
    """Claude APIなしのシンプルな解析（フォールバック）"""
    needs_reply = any(kw in text for kw in [
        '？', '?', 'どうします', 'どうでしょう', 'お願い', 'ください',
        '確認', '教えて', '何時', 'いつ', 'どこ'
    ])

    work_hours = None
    import re
    hour_match = re.search(r'(\d+(?:\.\d+)?)\s*[hｈ時間]', text)
    if hour_match:
        work_hours = float(hour_match.group(1))

    work_cat = None
    category_keywords = {
        '水稲': ['水稲', '田植え', '稲刈り', 'コンバイン', '田んぼ'],
        '大豆': ['大豆', '枝豆'],
        'くるみ（脱穀）': ['脱穀', 'くるみ脱穀'],
        'くるみ（選別）': ['選別', 'くるみ選別'],
        '除草・畔草刈': ['除草', '草刈', '畔'],
        '水管理・用水路': ['水管理', '水路', '灌水'],
        '農機・施設管理': ['トラクター', '農機', '機械', '修理'],
    }
    for cat, keywords in category_keywords.items():
        if any(kw in text for kw in keywords):
            work_cat = cat
            break

    return {
        "needs_reply": needs_reply,
        "work_category": work_cat,
        "work_hours": work_hours,
        "work_date": datetime.now().strftime('%Y-%m-%d'),
        "work_style": None
    }


# ==================== Googleカレンダー転記 ====================

def get_calendar_service():
    """Google Calendar APIサービスを返す"""
    if not GOOGLE_SERVICE_ACCOUNT_JSON:
        return None
    try:
        creds_info = json.loads(GOOGLE_SERVICE_ACCOUNT_JSON)
        creds = service_account.Credentials.from_service_account_info(
            creds_info,
            scopes=['https://www.googleapis.com/auth/calendar']
        )
        return build('calendar', 'v3', credentials=creds)
    except Exception as e:
        print(f"Calendar service error: {e}")
        return None


def add_to_calendar(analysis, user_name, message_text):
    """作業報告をGoogleカレンダーに転記する"""
    work_category = analysis.get('work_category')
    if not work_category:
        return None  # 作業報告でなければスキップ

    service = get_calendar_service()
    if not service:
        return None

    # 日付を決定
    work_date = analysis.get('work_date') or datetime.now().strftime('%Y-%m-%d')
    try:
        datetime.strptime(work_date, '%Y-%m-%d')
    except ValueError:
        work_date = datetime.now().strftime('%Y-%m-%d')

    # タイトルを作成
    hours = analysis.get('work_hours')
    work_style = analysis.get('work_style')
    title_parts = [f"【{work_category}】"]
    if hours:
        title_parts.append(f"{hours:.1f}h")
    title_parts.append(f"- {user_name}")
    if work_style:
        title_parts.append(f"({work_style})")
    title = " ".join(title_parts)

    # 説明文
    description = f"📱 LINEからの作業報告\n👤 {user_name}\n💬 {message_text}"
    if work_style:
        description += f"\n👥 作業スタイル: {work_style}"

    # 終日イベントとして登録
    event = {
        'summary': title,
        'description': description,
        'start': {'date': work_date},
        'end':   {'date': work_date},
        'colorId': '2',  # 緑色
    }

    try:
        result = service.events().insert(
            calendarId=GOOGLE_CALENDAR_ID,
            body=event
        ).execute()
        print(f"Calendar event created: {result.get('htmlLink')}")
        return result.get('id')
    except Exception as e:
        print(f"Calendar insert error: {e}")
        return None


# ==================== DB保存 ====================

def save_message(timestamp, group_id, user_id, user_name, message, message_id, analysis):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    try:
        c.execute('''
            INSERT OR IGNORE INTO messages
            (timestamp, group_id, user_id, user_name, message, message_id,
             needs_reply, work_category, work_hours, work_date, work_style, raw_analysis)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            timestamp, group_id, user_id, user_name, message, message_id,
            1 if analysis.get('needs_reply') else 0,
            analysis.get('work_category'),
            analysis.get('work_hours'),
            analysis.get('work_date'),
            analysis.get('work_style'),
            json.dumps(analysis, ensure_ascii=False)
        ))
        conn.commit()
    except Exception as e:
        print(f"DB save error: {e}")
    finally:
        conn.close()

    # 作業報告ならGoogleカレンダーにも転記
    if analysis.get('work_category'):
        add_to_calendar(analysis, user_name, message)


# ==================== ヘルスチェック ====================

@app.route("/health", methods=['GET'])
def health():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('SELECT COUNT(*) FROM messages')
    count = c.fetchone()[0]
    conn.close()
    return {'status': 'ok', 'message_count': count}


if __name__ == "__main__":
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=False)
