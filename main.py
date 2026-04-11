"""
しきしまの家 営農部 LINE業務サポートボット v2
- 作業報告を自動記録・分類
- 不明情報を確認する会話型フロー
- 記録後に確認メッセージを送信
- 「修正して」で記録を修正
- Googleカレンダーに転記
"""

import os
import json
import sqlite3
from datetime import datetime, timedelta
from flask import Flask, request, abort
from linebot import LineBotApi, WebhookHandler
from linebot.exceptions import InvalidSignatureError
from linebot.models import MessageEvent, TextMessage, TextSendMessage, JoinEvent
import anthropic
from googleapiclient.discovery import build
from google.oauth2 import service_account

app = Flask(__name__)

LINE_CHANNEL_SECRET = os.environ.get('LINE_CHANNEL_SECRET', '')
LINE_CHANNEL_ACCESS_TOKEN = os.environ.get('LINE_CHANNEL_ACCESS_TOKEN', '')
ANTHROPIC_API_KEY = os.environ.get('ANTHROPIC_API_KEY', '')
line_bot_api = LineBotApi(LINE_CHANNEL_ACCESS_TOKEN)
handler = WebhookHandler(LINE_CHANNEL_SECRET)

DB_PATH = os.environ.get('DB_PATH', 'messages.db')
GOOGLE_SERVICE_ACCOUNT_JSON = os.environ.get('GOOGLE_SERVICE_ACCOUNT_JSON', '')
GOOGLE_CALENDAR_ID = os.environ.get('GOOGLE_CALENDAR_ID', 'primary')

WORK_CATEGORIES = [
    "水稲", "大豆", "野菜", "農機・施設管理", "除草・畔草刈",
    "水管理・用水路", "環境整備", "共同作業一般", "組合業務", "研修・視察",
    "くるみ（脱穀）", "くるみ（選別）", "くるみ（その他）",
    "野菜（個人）", "米（個人）", "くるみ（個人）", "その他個人"
]

UNANSWERED_THRESHOLD_HOURS = 24
WEEKDAY_JP = ['月', '火', '水', '木', '金', '土', '日']

# 会話ステート
STATE_ASKING_HOURS   = 'asking_hours'
STATE_ASKING_PEOPLE  = 'asking_people'
STATE_CONFIRMING     = 'confirming'   # 記録済み・修正受付中

CORRECTION_KEYWORDS = [
    '修正', '違う', 'ちがう', '間違', 'いや', 'そうじゃない',
    '取り消し', '削除', '消して', '直して', '変えて', 'ちょっと待って'
]


# ==================== DB初期化 ====================

def init_db():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('''
        CREATE TABLE IF NOT EXISTS messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT NOT NULL,
            group_id TEXT NOT NULL,
            user_id TEXT, user_name TEXT, message TEXT,
            message_id TEXT UNIQUE,
            needs_reply INTEGER DEFAULT 0,
            replied INTEGER DEFAULT 0,
            work_category TEXT, work_hours REAL,
            work_date TEXT, work_style TEXT, raw_analysis TEXT
        )
    ''')
    c.execute('''
        CREATE TABLE IF NOT EXISTS groups (
            group_id TEXT PRIMARY KEY, group_name TEXT, joined_at TEXT
        )
    ''')
    c.execute('''
        CREATE TABLE IF NOT EXISTS conversations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            group_id TEXT NOT NULL,
            user_id TEXT NOT NULL,
            state TEXT NOT NULL,
            partial_analysis TEXT,
            original_message TEXT,
            last_event_id TEXT,
            created_at TEXT,
            updated_at TEXT
        )
    ''')
    conn.commit()
    conn.close()


init_db()


# ==================== Webhook ====================

@app.route("/callback", methods=['POST'])
def callback():
    signature = request.headers.get('X-Line-Signature', '')
    body = request.get_data(as_text=True)
    try:
        handler.handle(body, signature)
    except InvalidSignatureError:
        abort(400)
    return 'OK'


# ==================== グループ参加 ====================

@handler.add(JoinEvent)
def handle_join(event):
    if event.source.type == 'group':
        group_id = event.source.group_id
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute(
            'INSERT OR IGNORE INTO groups (group_id, joined_at) VALUES (?, ?)',
            (group_id, datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
        )
        conn.commit()
        conn.close()
        line_bot_api.reply_message(event.reply_token, TextSendMessage(
            text=(
                "こんにちは！しきしまの家 営農部サポートボットです🌾\n\n"
                "作業報告を自動で記録・分類し、Googleカレンダーに転記します。\n"
                "不明な点はその都度確認しますので、気軽にご報告ください！\n\n"
                "【コマンド一覧】\n"
                "「/集計」→ 今月の作業時間集計\n"
                "「/未返信」→ 返信待ちメッセージ一覧\n"
                "「/ヘルプ」→ 使い方"
            )
        ))


# ==================== メッセージ受信（メイン） ====================

@handler.add(MessageEvent, message=TextMessage)
def handle_message(event):
    if event.source.type != 'group':
        return

    group_id   = event.source.group_id
    user_id    = event.source.user_id
    message_text = event.message.text.strip()
    message_id = event.message.id
    timestamp  = datetime.fromtimestamp(
        event.timestamp / 1000
    ).strftime('%Y-%m-%d %H:%M:%S')
    user_name  = get_user_name(group_id, user_id)

    # コマンド
    if message_text.startswith('/'):
        handle_command(event, message_text, group_id)
        return

    # 会話ステートがあれば続きとして処理
    pending = get_pending_state(group_id, user_id)
    if pending:
        handle_conversation_response(
            event, pending, message_text, user_name,
            group_id, user_id, timestamp, message_id
        )
        return

    # 新規メッセージとして解析
    analysis = analyze_message(message_text, user_name)
    mark_replied_context(group_id, user_id, message_text)

    # DBに保存（カレンダー登録はまだしない）
    save_message(
        timestamp, group_id, user_id, user_name,
        message_text, message_id, analysis, create_calendar=False
    )

    if not analysis.get('work_category'):
        # 作業報告でない → 返信不要
        return

    # 作業報告 → 不足情報があれば質問、なければ記録
    process_work_report(
        event, analysis, message_text, user_name, group_id, user_id
    )

    # 朝9時の未返信チェック
    if datetime.now().hour == 9 and datetime.now().minute < 5:
        notify_unanswered(group_id)


def get_user_name(group_id, user_id):
    try:
        profile = line_bot_api.get_group_member_profile(group_id, user_id)
        return profile.display_name
    except Exception:
        return user_id


# ==================== 作業報告の処理 ====================

def process_work_report(event, analysis, message_text, user_name, group_id, user_id):
    """作業報告の完結チェック → 質問 or 記録"""
    missing = get_missing_info(analysis, message_text)
    if missing:
        question = build_question(missing['state'], analysis, user_name)
        line_bot_api.reply_message(
            event.reply_token, TextSendMessage(text=question)
        )
        save_pending_state(
            group_id, user_id, missing['state'], analysis, message_text
        )
    else:
        event_id = add_to_calendar(analysis, user_name, message_text)
        line_bot_api.reply_message(
            event.reply_token,
            TextSendMessage(text=build_confirmation(analysis, user_name))
        )
        save_pending_state(
            group_id, user_id, STATE_CONFIRMING,
            analysis, message_text, event_id
        )


def get_missing_info(analysis, message_text):
    """不足している情報を返す（最初に見つかったもの）"""
    if not analysis.get('work_hours'):
        return {'state': STATE_ASKING_HOURS}
    if not analysis.get('work_style') and _has_people_hint(message_text):
        return {'state': STATE_ASKING_PEOPLE}
    return None


def _has_people_hint(text):
    hints = ['一緒', '2人', '二人', 'と一緒', 'たちで', 'みんな', '手伝', '協力', '複数']
    return any(h in text for h in hints)


# ==================== 会話の続き ====================

def handle_conversation_response(
    event, pending, message_text, user_name,
    group_id, user_id, timestamp, message_id
):
    state   = pending['state']
    partial = pending['partial_analysis']
    is_correction = any(kw in message_text for kw in CORRECTION_KEYWORDS)

    # 修正依頼
    if is_correction:
        handle_correction(event, pending, message_text, user_name, group_id, user_id)
        return

    # 確認ステート中に新しいメッセージ → 新規として処理
    if state == STATE_CONFIRMING:
        clear_pending_state(group_id, user_id)
        analysis = analyze_message(message_text, user_name)
        save_message(
            timestamp, group_id, user_id, user_name,
            message_text, message_id, analysis, create_calendar=False
        )
        mark_replied_context(group_id, user_id, message_text)
        if analysis.get('work_category'):
            process_work_report(
                event, analysis, message_text, user_name, group_id, user_id
            )
        return

    # 時間を聞いていた
    if state == STATE_ASKING_HOURS:
        updated = parse_hours_from_text(message_text, partial)
        if not updated.get('work_hours'):
            line_bot_api.reply_message(event.reply_token, TextSendMessage(
                text="⏱️ 時間が読み取れませんでした。\n「3時間」「2.5h」「半日」などで教えてください🙏"
            ))
            return
        _finalize_or_ask_more(
            event, updated, pending['original_message'],
            user_name, group_id, user_id, timestamp, message_id
        )

    # 一緒に作業した人を聞いていた
    elif state == STATE_ASKING_PEOPLE:
        updated = parse_people_from_text(message_text, partial, user_name)
        event_id = add_to_calendar(updated, user_name, pending['original_message'])
        line_bot_api.reply_message(
            event.reply_token,
            TextSendMessage(text=build_confirmation(updated, user_name))
        )
        save_pending_state(
            group_id, user_id, STATE_CONFIRMING,
            updated, pending['original_message'], event_id
        )


def _finalize_or_ask_more(
    event, analysis, original_message, user_name,
    group_id, user_id, timestamp, message_id
):
    """時間補完後：さらに不足があれば質問、なければ記録"""
    missing = get_missing_info(analysis, original_message)
    if missing:
        question = build_question(missing['state'], analysis, user_name)
        line_bot_api.reply_message(
            event.reply_token, TextSendMessage(text=question)
        )
        save_pending_state(
            group_id, user_id, missing['state'], analysis, original_message
        )
    else:
        event_id = add_to_calendar(analysis, user_name, original_message)
        line_bot_api.reply_message(
            event.reply_token,
            TextSendMessage(text=build_confirmation(analysis, user_name))
        )
        save_pending_state(
            group_id, user_id, STATE_CONFIRMING,
            analysis, original_message, event_id
        )


def handle_correction(event, pending, message_text, user_name, group_id, user_id):
    """直前の記録を修正する"""
    last_event_id = pending.get('last_event_id')
    last_analysis = pending.get('partial_analysis', {})

    # 古いカレンダーイベントを削除
    if last_event_id:
        delete_calendar_event(last_event_id)

    # Claudeで修正内容を解析
    corrected = last_analysis.copy()
    if ANTHROPIC_API_KEY:
        try:
            client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
            cat_str = "、".join(WORK_CATEGORIES)
            prompt = (
                f"前回の作業報告と記録内容、修正指示から、正しい情報をJSONで返してください。\n\n"
                f"前回の報告: {pending.get('original_message', '')}\n"
                f"記録された内容: {json.dumps(last_analysis, ensure_ascii=False)}\n"
                f"修正指示: {message_text}\n\n"
                f"以下のJSONのみを返してください（説明文不要）:\n"
                f"{{\n"
                f'  "work_category": "{cat_str} のいずれか",\n'
                f'  "work_hours": 数値またはnull,\n'
                f'  "work_date": "YYYY-MM-DD",\n'
                f'  "work_style": "本田+荻原" または "本田（単独）" または "荻原（単独）" または null\n'
                f"}}"
            )
            response = client.messages.create(
                model="claude-3-haiku-20240307", max_tokens=200,
                messages=[{"role": "user", "content": prompt}]
            )
            result = response.content[0].text.strip()
            if '{' in result:
                result = result[result.index('{'):result.rindex('}') + 1]
            corrected = {**last_analysis, **json.loads(result)}
        except Exception as e:
            print(f"Correction parse error: {e}")

    event_id = add_to_calendar(corrected, user_name, pending.get('original_message', ''))
    reply = "✏️ 修正しました！\n" + build_confirmation(corrected, user_name, header="")
    line_bot_api.reply_message(event.reply_token, TextSendMessage(text=reply))
    save_pending_state(
        group_id, user_id, STATE_CONFIRMING,
        corrected, pending.get('original_message', ''), event_id
    )


# ==================== メッセージ生成 ====================

def build_question(state, analysis, user_name):
    cat = analysis.get('work_category', '作業')
    if state == STATE_ASKING_HOURS:
        return (
            f"📝 {user_name}さん、{cat}の作業ですね！\n"
            f"作業時間を教えてください🙏\n"
            f"（例：「3時間」「2.5h」「半日」「終日」）"
        )
    elif state == STATE_ASKING_PEOPLE:
        return (
            f"📝 {cat}の作業ですね！\n"
            f"何人で作業しましたか？\n"
            f"① {user_name}さん単独\n"
            f"② 本田さんと2人\n"
            f"③ 荻原さんと2人\n"
            f"④ 本田さん＋荻原さんと3人"
        )
    return "詳しく教えてください。"


def build_confirmation(analysis, user_name, header="✅ 記録しました！\n"):
    cat   = analysis.get('work_category', '')
    date  = analysis.get('work_date') or datetime.now().strftime('%Y-%m-%d')
    hours = analysis.get('work_hours')
    style = analysis.get('work_style')

    try:
        d = datetime.strptime(date, '%Y-%m-%d')
        date_str = f"{d.month}/{d.day}（{WEEKDAY_JP[d.weekday()]}）"
    except Exception:
        date_str = date

    lines = [header + "━━━━━━━━━━━━"]
    lines.append(f"📋 {cat}")
    lines.append(f"📅 {date_str}")
    if hours:
        lines.append(f"⏱️ {hours:.1f}時間")
    lines.append(f"👤 {user_name}")
    if style:
        lines.append(f"👥 {style}")
    lines.append("━━━━━━━━━━━━")
    lines.append("間違いがあれば「修正して」と教えてください🙏")
    return "\n".join(lines)


# ==================== 応答パース ====================

def parse_hours_from_text(text, partial_analysis):
    import re
    updated = dict(partial_analysis)
    m = re.search(r'(\d+(?:\.\d+)?)\s*[hｈ時間]', text)
    if m:
        updated['work_hours'] = float(m.group(1))
        return updated
    if '午前' in text or '午後' in text:
        updated['work_hours'] = 3.0
    elif '半日' in text:
        updated['work_hours'] = 4.0
    elif '終日' in text or '一日' in text or '1日' in text:
        updated['work_hours'] = 8.0
    elif ANTHROPIC_API_KEY:
        try:
            client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
            r = client.messages.create(
                model="claude-3-haiku-20240307", max_tokens=30,
                messages=[{
                    "role": "user",
                    "content": f"「{text}」から作業時間を数値（時間単位）で抽出してください。数値のみ返して。不明なら「null」。"
                }]
            )
            h = r.content[0].text.strip()
            if h != 'null':
                updated['work_hours'] = float(h)
        except Exception:
            pass
    return updated


def parse_people_from_text(text, partial_analysis, user_name):
    updated = dict(partial_analysis)
    if '①' in text or '単独' in text or '一人' in text or '1人' in text:
        updated['work_style'] = None
    elif '④' in text or ('本田' in text and '荻原' in text) or '3人' in text or '三人' in text:
        updated['work_style'] = '本田+荻原'
    elif '②' in text or '本田' in text:
        updated['work_style'] = '本田（単独）'
    elif '③' in text or '荻原' in text:
        updated['work_style'] = '荻原（単独）'
    else:
        updated['work_style'] = None
    return updated


# ==================== 会話ステート管理 ====================

def get_pending_state(group_id, user_id):
    cutoff = (datetime.now() - timedelta(minutes=15)).strftime('%Y-%m-%d %H:%M:%S')
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('''
        SELECT state, partial_analysis, original_message, last_event_id
        FROM conversations
        WHERE group_id=? AND user_id=? AND updated_at>?
        ORDER BY updated_at DESC LIMIT 1
    ''', (group_id, user_id, cutoff))
    row = c.fetchone()
    conn.close()
    if row:
        return {
            'state': row[0],
            'partial_analysis': json.loads(row[1]) if row[1] else {},
            'original_message': row[2] or '',
            'last_event_id': row[3]
        }
    return None


def save_pending_state(
    group_id, user_id, state, partial_analysis, original_message, last_event_id=None
):
    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute(
        'DELETE FROM conversations WHERE group_id=? AND user_id=?',
        (group_id, user_id)
    )
    c.execute('''
        INSERT INTO conversations
        (group_id, user_id, state, partial_analysis, original_message,
         last_event_id, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    ''', (
        group_id, user_id, state,
        json.dumps(partial_analysis, ensure_ascii=False),
        original_message, last_event_id, now, now
    ))
    conn.commit()
    conn.close()


def clear_pending_state(group_id, user_id):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute(
        'DELETE FROM conversations WHERE group_id=? AND user_id=?',
        (group_id, user_id)
    )
    conn.commit()
    conn.close()


# ==================== コマンド ====================

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
            "「/未返信」→ 返信待ちメッセージ\n\n"
            "【記録の修正方法】\n"
            "記録後15分以内に「修正して」と言うと\n"
            "内容を直せます✏️\n\n"
            "作業報告は自動で記録されます📝"
        )
    else:
        return
    line_bot_api.reply_message(event.reply_token, TextSendMessage(text=reply))


def get_monthly_summary(group_id):
    today = datetime.now()
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('''
        SELECT work_category, SUM(work_hours), COUNT(*)
        FROM messages
        WHERE group_id=? AND work_category IS NOT NULL
          AND work_date BETWEEN ? AND ?
        GROUP BY work_category ORDER BY SUM(work_hours) DESC
    ''', (group_id, today.strftime('%Y-%m-01'), today.strftime('%Y-%m-%d')))
    rows = c.fetchall()
    conn.close()
    if not rows:
        return f"📊 {today.month}月の作業記録はまだありません。"
    lines = [f"📊 {today.month}月の作業時間集計\n"]
    total = 0
    for cat, hours, count in rows:
        h = hours or 0
        total += h
        lines.append(f"  {cat}: {h:.1f}h（{count}件）")
    lines.append(f"\n合計: {total:.1f}h")
    return "\n".join(lines)


def get_unanswered_list(group_id):
    threshold = (
        datetime.now() - timedelta(hours=UNANSWERED_THRESHOLD_HOURS)
    ).strftime('%Y-%m-%d %H:%M:%S')
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('''
        SELECT user_name, message, timestamp FROM messages
        WHERE group_id=? AND needs_reply=1 AND replied=0 AND timestamp<?
        ORDER BY timestamp DESC LIMIT 10
    ''', (group_id, threshold))
    rows = c.fetchall()
    conn.close()
    if not rows:
        return "✅ 返信待ちのメッセージはありません！"
    lines = [f"⚠️ {UNANSWERED_THRESHOLD_HOURS}時間以上返信待ち:\n"]
    for name, msg, ts in rows:
        short = msg[:40] + "..." if len(msg) > 40 else msg
        lines.append(f"👤 {name}（{ts[:10]}）\n   「{short}」")
    return "\n\n".join(lines)


# ==================== スルー検知 ====================

def notify_unanswered(group_id):
    msg = get_unanswered_list(group_id)
    if "返信待ち" in msg and "✅" not in msg:
        line_bot_api.push_message(group_id, TextSendMessage(text=msg))


def mark_replied_context(group_id, user_id, message_text):
    reply_keywords = ['確認', '了解', 'わかりました', 'ありがとう', 'ok', 'OK', '👍', '✅']
    if any(kw in message_text for kw in reply_keywords):
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute('''
            UPDATE messages SET replied=1
            WHERE group_id=? AND needs_reply=1 AND replied=0
            AND id IN (
                SELECT id FROM messages
                WHERE group_id=? AND needs_reply=1 AND replied=0
                ORDER BY timestamp DESC LIMIT 5
            )
        ''', (group_id, group_id))
        conn.commit()
        conn.close()


# ==================== Claude解析 ====================

def analyze_message(text, user_name):
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
- needs_reply: 質問・依頼・確認要求など返答が必要なら true
- work_category: 作業報告が含まれていれば分類、なければ null
- work_hours: 「3時間」「2h」「午前中」(3h換算)等から抽出
- work_date: 「今日」「昨日」「12/15」等から推定（今日={datetime.now().strftime('%Y-%m-%d')}）
- work_style: 2人作業・本田さんと・荻原さんと等から判定"""
    try:
        response = client.messages.create(
            model="claude-3-haiku-20240307", max_tokens=300,
            messages=[{"role": "user", "content": prompt}]
        )
        result_text = response.content[0].text.strip()
        if '{' in result_text:
            result_text = result_text[result_text.index('{'):result_text.rindex('}') + 1]
        return json.loads(result_text)
    except Exception as e:
        print(f"Claude API error: {e}")
        return _simple_analyze(text)


def _simple_analyze(text):
    import re
    needs_reply = any(kw in text for kw in [
        '？', '?', 'どうします', 'どうでしょう', 'お願い', 'ください',
        '確認', '教えて', '何時', 'いつ', 'どこ'
    ])
    m = re.search(r'(\d+(?:\.\d+)?)\s*[hｈ時間]', text)
    work_hours = float(m.group(1)) if m else None
    work_cat = None
    for cat, kws in {
        '水稲': ['水稲', '田植え', '稲刈り', 'コンバイン', '田んぼ'],
        '大豆': ['大豆', '枝豆'],
        'くるみ（脱穀）': ['脱穀', 'くるみ脱穀'],
        'くるみ（選別）': ['選別', 'くるみ選別'],
        '除草・畔草刈': ['除草', '草刈', '畔'],
        '水管理・用水路': ['水管理', '水路', '灌水'],
        '農機・施設管理': ['トラクター', '農機', '機械', '修理'],
    }.items():
        if any(kw in text for kw in kws):
            work_cat = cat
            break
    return {
        "needs_reply": needs_reply,
        "work_category": work_cat,
        "work_hours": work_hours,
        "work_date": datetime.now().strftime('%Y-%m-%d'),
        "work_style": None
    }


# ==================== Googleカレンダー ====================

def get_calendar_service():
    if not GOOGLE_SERVICE_ACCOUNT_JSON:
        return None
    try:
        creds = service_account.Credentials.from_service_account_info(
            json.loads(GOOGLE_SERVICE_ACCOUNT_JSON),
            scopes=['https://www.googleapis.com/auth/calendar']
        )
        return build('calendar', 'v3', credentials=creds)
    except Exception as e:
        print(f"Calendar service error: {e}")
        return None


def add_to_calendar(analysis, user_name, message_text):
    if not analysis.get('work_category'):
        return None
    service = get_calendar_service()
    if not service:
        return None
    work_date = analysis.get('work_date') or datetime.now().strftime('%Y-%m-%d')
    try:
        datetime.strptime(work_date, '%Y-%m-%d')
    except ValueError:
        work_date = datetime.now().strftime('%Y-%m-%d')
    hours = analysis.get('work_hours')
    style = analysis.get('work_style')
    title_parts = [f"【{analysis['work_category']}】"]
    if hours:
        title_parts.append(f"{hours:.1f}h")
    title_parts.append(f"- {user_name}")
    if style:
        title_parts.append(f"({style})")
    description = f"📱 LINEからの作業報告\n👤 {user_name}\n💬 {message_text}"
    if style:
        description += f"\n👥 {style}"
    event = {
        'summary': " ".join(title_parts),
        'description': description,
        'start': {'date': work_date},
        'end':   {'date': work_date},
        'colorId': '2',
    }
    try:
        result = service.events().insert(
            calendarId=GOOGLE_CALENDAR_ID, body=event
        ).execute()
        print(f"Calendar event created: {result.get('htmlLink')}")
        return result.get('id')
    except Exception as e:
        print(f"Calendar insert error: {e}")
        return None


def delete_calendar_event(event_id):
    service = get_calendar_service()
    if not service or not event_id:
        return
    try:
        service.events().delete(
            calendarId=GOOGLE_CALENDAR_ID, eventId=event_id
        ).execute()
        print(f"Calendar event deleted: {event_id}")
    except Exception as e:
        print(f"Calendar delete error: {e}")


# ==================== DB保存 ====================

def save_message(
    timestamp, group_id, user_id, user_name, message, message_id,
    analysis, create_calendar=True
):
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
            analysis.get('work_category'), analysis.get('work_hours'),
            analysis.get('work_date'), analysis.get('work_style'),
            json.dumps(analysis, ensure_ascii=False)
        ))
        conn.commit()
    except Exception as e:
        print(f"DB save error: {e}")
    finally:
        conn.close()
    if create_calendar and analysis.get('work_category'):
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
