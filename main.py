"""
ãããã¾ã®å®¶ å¶è¾²é¨ LINEæ¥­åãµãã¼ãããã v5
- å¨ã¡ãã»ã¼ã¸ä¿å­ã»ããã®ã¼ãæ¤ç´¢
- ä½æ¥­å ±åãèªåè¨é²ã»åé¡ï¼Haikuï¼
- ToDoã®èªåæ½åºã»ç®¡ç
- æ±ºå®äºé ã®èªåè¨é²
- æªæ±ºæ¡ä»¶ã®æ¤ç¥ã»ãã©ã­ã¼ã¢ãã
- ç¥è­ã®èç©ï¼ãè¦ãã¦ããã¦ãï¼
- @ã¡ã³ã·ã§ã³ã«ã¯ä½ã§ãåç­ï¼Sonnetï¼
- ç»åãéãã¨åå®¹ãè§£æã»èª¬æï¼Sonnet Visionï¼
- æ¯æ9æï¼ã¹ã«ã¼æ¤ç¥ã»æªæ±ºãã©ã­ã¼ã¢ãã
- æ¯é±ææï¼é±å ±èªåæ
- Googleã«ã¬ã³ãã¼ã«ä½æ¥­è¨é²ãè»¢è¨
"""

import os
import json
import re
import sqlite3
import base64
from datetime import datetime, timedelta
from flask import Flask, request, abort
from linebot import LineBotApi, WebhookHandler
from linebot.exceptions import InvalidSignatureError
from linebot.models import MessageEvent, TextMessage, TextSendMessage, JoinEvent, ImageMessage
import anthropic
from apscheduler.schedulers.background import BackgroundScheduler
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

MODEL_FAST  = "claude-haiku-4-5-20251001"
MODEL_SMART = "claude-sonnet-4-20250514"

WORK_CATEGORIES = [
    "æ°´ç¨²", "å¤§è±", "éè", "è¾²æ©ã»æ½è¨­ç®¡ç", "é¤èã»çèå",
    "æ°´ç®¡çã»ç¨æ°´è·¯", "ç°å¢æ´å", "å±åä½æ¥­ä¸è¬", "çµåæ¥­å", "ç ä¿®ã»è¦å¯",
    "ããã¿ï¼è±ç©ï¼", "ããã¿ï¼é¸å¥ï¼", "ããã¿ï¼ãã®ä»ï¼",
    "éèï¼åäººï¼", "ç±³ï¼åäººï¼", "ããã¿ï¼åäººï¼", "ãã®ä»åäºº"
]

UNANSWERED_THRESHOLD_HOURS = 24
PENDING_FOLLOWUP_DAYS = 3
WEEKDAY_JP = ['æ', 'ç«', 'æ°´', 'æ¨', 'é', 'å', 'æ¥']

STATE_ASKING_HOURS  = 'asking_hours'
STATE_ASKING_PEOPLE = 'asking_people'
STATE_CONFIRMING    = 'confirming'

CORRECTION_KEYWORDS = [
    'ä¿®æ­£', 'éã', 'ã¡ãã', 'éé', 'ãã', 'ãããããªã',
    'åãæ¶ã', 'åé¤', 'æ¶ãã¦', 'ç´ãã¦', 'å¤ãã¦', 'ã¡ãã£ã¨å¾ã£ã¦'
]

KNOWLEDGE_KEYWORDS = ['è¦ãã¦ããã¦', 'è¦ãã¦', 'è¨é²ãã¦ããã¦', 'ã¡ã¢ãã¦ããã¦']

_BOT_USER_ID = None


# ==================== DBåæå ====================

def init_db():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    c.execute('''
        CREATE TABLE IF NOT EXISTS all_messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT NOT NULL,
            group_id TEXT NOT NULL,
            user_id TEXT, user_name TEXT,
            message TEXT,
            message_id TEXT UNIQUE,
            message_type TEXT DEFAULT 'general'
        )
    ''')
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
    c.execute('''
        CREATE TABLE IF NOT EXISTS todos (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            group_id TEXT NOT NULL,
            assignee TEXT,
            task TEXT NOT NULL,
            source_message TEXT,
            created_by TEXT,
            created_at TEXT,
            due_date TEXT,
            status TEXT DEFAULT 'open',
            completed_at TEXT
        )
    ''')
    c.execute('''
        CREATE TABLE IF NOT EXISTS decisions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            group_id TEXT NOT NULL,
            decision TEXT NOT NULL,
            source_message TEXT,
            decided_by TEXT,
            created_at TEXT
        )
    ''')
    c.execute('''
        CREATE TABLE IF NOT EXISTS pending_issues (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            group_id TEXT NOT NULL,
            summary TEXT NOT NULL,
            source_message TEXT,
            raised_by TEXT,
            created_at TEXT,
            last_followup_at TEXT,
            status TEXT DEFAULT 'open',
            resolved_at TEXT
        )
    ''')
    c.execute('''
        CREATE TABLE IF NOT EXISTS knowledge (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            group_id TEXT NOT NULL,
            content TEXT NOT NULL,
            stored_by TEXT,
            created_at TEXT,
            tag TEXT
        )
    ''')
    conn.commit()
    conn.close()


init_db()


# ==================== ã¹ã±ã¸ã¥ã¼ã© ====================

def daily_check():
    """æ¯æ9æï¼æªè¿ä¿¡ãã§ãã¯ + æªæ±ºæ¡ä»¶ãã©ã­ã¼ã¢ãã"""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('SELECT group_id FROM groups')
    groups = [row[0] for row in c.fetchall()]
    conn.close()
    for gid in groups:
        notify_unanswered(gid)
        followup_pending_issues(gid)


def weekly_report_job():
    """æ¯é±ææ9æï¼é±å ±ãå¨ã°ã«ã¼ãã«æç¨¿"""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('SELECT group_id FROM groups')
    groups = [row[0] for row in c.fetchall()]
    conn.close()
    for gid in groups:
        report = build_weekly_report(gid)
        if report:
            line_bot_api.push_message(gid, TextSendMessage(text=report))


scheduler = BackgroundScheduler(timezone='Asia/Tokyo')
scheduler.add_job(daily_check, 'cron', hour=9, minute=0)
scheduler.add_job(weekly_report_job, 'cron', day_of_week='mon', hour=9, minute=0)
scheduler.start()


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


# ==================== ã°ã«ã¼ãåå  ====================

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
                "ãªã«ãªã«ã§ãð¦\n\n"
                "å¤ªå¤ããè¾²æãè¦å®ã£ã¦ããèã¨ãã¦ã\n"
                "çããã®ãå½¹ã«ç«ã¦ãã°å¹¸ãã§ãã\n\n"
                "@ãªã«ãªã« ã¨å¼ã³ããã¦ãã ããã\n"
                "ã©ããªè³ªåã»ç¸è«ã«ãç­ãã¾ãã\n\n"
                "ãã³ãã³ãã\n"
                "ã/æªè¿ä¿¡ãâ è¿ä¿¡å¾ã¡ã¡ãã»ã¼ã¸ä¸è¦§\n"
                "ã/ã¿ã¹ã¯ãâ æªå®äºã¿ã¹ã¯ä¸è¦§\n"
                "ã/æ±ºå®äºé ãâ æè¿ã®æ±ºå®äºé \n"
                "ã/æªæ±ºãâ æ¤è¨ä¸­æ¡ä»¶ä¸è¦§\n"
                "ã/ç¥è­ãâ è¦ãã¦ããæå ±\n"
                "ã/ãã«ããâ ä½¿ãæ¹"
            )
        ))


# ==================== ã¡ã³ã·ã§ã³æ¤ç¥ ====================

def get_bot_user_id():
    global _BOT_USER_ID
    if not _BOT_USER_ID:
        try:
            _BOT_USER_ID = line_bot_api.get_bot_info().user_id
        except Exception as e:
            print(f"get_bot_info error: {e}")
    return _BOT_USER_ID


def is_bot_mentioned(event):
    try:
        msg = event.message
        if hasattr(msg, 'mention') and msg.mention:
            bot_id = get_bot_user_id()
            if bot_id:
                for m in msg.mention.mentionees:
                    if hasattr(m, 'user_id') and m.user_id == bot_id:
                        return True
    except Exception as e:
        print(f"mention check error: {e}")
    return bool(re.search(r'@\S+', event.message.text))


def extract_mention_text(text):
    return re.sub(r'@\S+\s*', '', text).strip()


def get_user_name(group_id, user_id):
    try:
        profile = line_bot_api.get_group_member_profile(group_id, user_id)
        return profile.display_name
    except Exception:
        return user_id


# ==================== ç»åã¡ãã»ã¼ã¸å¯¾å¿ï¼Sonnet Visionï¼ ====================

@handler.add(MessageEvent, message=ImageMessage)
def handle_image(event):
    if event.source.type != 'group':
        return
    group_id  = event.source.group_id
    user_id   = event.source.user_id
    user_name = get_user_name(group_id, user_id)
    try:
        message_content = line_bot_api.get_message_content(event.message.id)
        image_data = b''.join(chunk for chunk in message_content.iter_content())
        image_base64 = base64.standard_b64encode(image_data).decode('utf-8')
    except Exception as e:
        print(f"Image download error: {e}")
        line_bot_api.reply_message(
            event.reply_token,
            TextSendMessage(text="ç»åã®åå¾ã«å¤±æãã¾ããð¦ ããä¸åº¦éã£ã¦ã¿ã¦ãã ããã")
        )
        return
    reply = analyze_image_with_sonnet(image_base64, user_name)
    line_bot_api.reply_message(event.reply_token, TextSendMessage(text=reply))


def analyze_image_with_sonnet(image_base64, user_name):
    if not ANTHROPIC_API_KEY:
        return "ç³ãè¨³ããã¾ãããAIãµã¼ãã¹ã«æ¥ç¶ã§ãã¾ããã"
    try:
        client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
        response = client.messages.create(
            model=MODEL_SMART,
            max_tokens=600,
            system=(
                "ããªãã¯ããªã«ãªã«ããæ¥æ¬ã®è±ããªè¾²æã«å¤ªå¤ã®æããæ£²ã¿ç¶ãããã¯ã­ã¦ã®ç¥æ§ã§ãã\n"
                "éããã¦ããç»åãè¦ã¦ãè¾²æ¥­ã»èªç¶ã»çãç©ã»æ¤ç©ã»çå®³è«ã»è¾²ä½ç©ã»è¾²æ©å·ã»"
                "ç¾å ´ã®ç¶æ³ãªã©ãä½ã§ãè©³ããè§£èª¬ãã¦ãã ããã\n"
                "ãåç­ã«ã¼ã«ã\n"
                "- ä½ãåã£ã¦ãããç°¡æ½ã«ç¹å®ãã\n"
                "- è¾²æ¥­ãç¾å ´ä½æ¥­ã«é¢é£ããæå ±ãããã°è©³ããè£è¶³ãã\n"
                "- çå®³è«ã»éèã®å ´åã¯å¯¾å¦æ³ãæ·»ãã\n"
                "- LINEãªã®ã§é·ãã¦ã15è¡ä»¥å\n"
                "- çµµæå­ã¯ð¦ãä¸­å¿ã«é©åº¦ã«ä½¿ã\n"
                "- å¤å¥ãé£ããå ´åã¯æ­£ç´ã«ãã®æ¨ãä¼ãã"
            ),
            messages=[{
                "role": "user",
                "content": [
                    {"type": "image", "source": {"type": "base64", "media_type": "image/jpeg", "data": image_base64}},
                    {"type": "text", "text": f"{user_name}ãããç»åãéãã¾ãããããã¯ä½ã§ããï¼è©³ããæãã¦ãã ããã"}
                ]
            }]
        )
        return response.content[0].text.strip()
    except Exception as e:
        print(f"Image analysis error: {e}")
        return f"â ï¸ ç»åã®è§£æä¸­ã«ã¨ã©ã¼ãçºçãã¾ãã: {str(e)[:50]}"


# ==================== ã¡ãã»ã¼ã¸åä¿¡ï¼ã¡ã¤ã³ï¼ ====================

@handler.add(MessageEvent, message=TextMessage)
def handle_message(event):
    if event.source.type != 'group':
        return

    group_id     = event.source.group_id
    user_id      = event.source.user_id
    message_text = event.message.text.strip()
    message_id   = event.message.id
    timestamp    = datetime.fromtimestamp(event.timestamp / 1000).strftime('%Y-%m-%d %H:%M:%S')
    user_name    = get_user_name(group_id, user_id)

    # å¨ã¡ãã»ã¼ã¸ãä¿å­
    save_all_message(timestamp, group_id, user_id, user_name, message_text, message_id)

    # â  ã³ãã³ã
    if message_text.startswith('/'):
        handle_command(event, message_text, group_id)
        return

    # â¡ @ã¡ã³ã·ã§ã³
    if is_bot_mentioned(event):
        handle_mention(event, message_text, user_name, group_id)
        return

    # â¢ ãè¦ãã¦ããã¦ã
    if any(kw in message_text for kw in KNOWLEDGE_KEYWORDS):
        handle_knowledge_store(event, message_text, user_name, group_id)
        return

    # â£ ä¼è©±ã¹ãã¼ã
    pending = get_pending_state(group_id, user_id)
    if pending:
        handle_conversation_response(
            event, pending, message_text, user_name,
            group_id, user_id, timestamp, message_id
        )
        return

    # â¤ éå¸¸ã¡ãã»ã¼ã¸è§£æï¼Haikuï¼
    analysis = analyze_message_full(message_text, user_name)
    mark_replied_context(group_id, user_id, message_text)

    save_message(timestamp, group_id, user_id, user_name,
                 message_text, message_id, analysis, create_calendar=False)

    process_metadata(analysis, message_text, user_name, group_id, timestamp)

    if not analysis.get('work_category'):
        return

    process_work_report(event, analysis, message_text, user_name, group_id, user_id)


# ==================== å¨ã¡ãã»ã¼ã¸ä¿å­ ====================

def save_all_message(timestamp, group_id, user_id, user_name, message, message_id, message_type='general'):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    try:
        c.execute('''
            INSERT OR IGNORE INTO all_messages
            (timestamp, group_id, user_id, user_name, message, message_id, message_type)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', (timestamp, group_id, user_id, user_name, message, message_id, message_type))
        conn.commit()
    except Exception as e:
        print(f"save_all_message error: {e}")
    finally:
        conn.close()


# ==================== ToDoã»æ±ºå®äºé ã»æªæ±ºæ¡ä»¶ã®å¦ç ====================

def process_metadata(analysis, message_text, user_name, group_id, timestamp):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    if analysis.get('has_todo') and analysis.get('todo_text'):
        c.execute('''
            INSERT INTO todos (group_id, assignee, task, source_message, created_by, created_at)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', (group_id, analysis.get('todo_assignee'), analysis['todo_text'],
              message_text, user_name, timestamp))

    if analysis.get('has_decision') and analysis.get('decision_text'):
        c.execute('''
            INSERT INTO decisions (group_id, decision, source_message, decided_by, created_at)
            VALUES (?, ?, ?, ?, ?)
        ''', (group_id, analysis['decision_text'], message_text, user_name, timestamp))

    if analysis.get('is_pending_issue') and analysis.get('pending_summary'):
        c.execute('''
            INSERT INTO pending_issues (group_id, summary, source_message, raised_by, created_at)
            VALUES (?, ?, ?, ?, ?)
        ''', (group_id, analysis['pending_summary'], message_text, user_name, timestamp))

    conn.commit()
    conn.close()


# ==================== ç¥è­ãã¼ã¹ ====================

def handle_knowledge_store(event, message_text, user_name, group_id):
    content = message_text
    for kw in KNOWLEDGE_KEYWORDS:
        content = content.replace(kw, '').strip()
    content = content.lstrip('ï¼:').strip()

    if not content:
        line_bot_api.reply_message(
            event.reply_token,
            TextSendMessage(text="è¦ãã¦ããããåå®¹ããè¦ãã¦ããã¦ï¼ãããã®å½¢ã§æãã¦ãã ããð¦")
        )
        return

    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute(
        'INSERT INTO knowledge (group_id, content, stored_by, created_at) VALUES (?, ?, ?, ?)',
        (group_id, content, user_name, datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    )
    conn.commit()
    conn.close()

    line_bot_api.reply_message(
        event.reply_token,
        TextSendMessage(text=f"ð¦ è¦ãã¾ããï¼\nã{content[:50]}ã\n\nã/ç¥è­ãã§ç¢ºèªã§ãã¾ãã")
    )


# ==================== @ã¡ã³ã·ã§ã³å¯¾å¿ï¼Sonnetï¼ ====================

def handle_mention(event, message_text, user_name, group_id):
    query = extract_mention_text(message_text)
    if not query:
        line_bot_api.reply_message(
            event.reply_token,
            TextSendMessage(text="ã¯ãï¼ä½ã§ããæ°è»½ã«ã©ããð¦")
        )
        return
    # まず「考え中」を即座に返信してreply_tokenを使い切る
    line_bot_api.reply_message(
        event.reply_token,
        TextSendMessage(text="少し考えますね🦉…")
    )
    context  = get_group_context(group_id)
    history  = search_history(query, group_id)
    knowledge = get_knowledge_context(group_id)
    reply = ask_sonnet(query, user_name, context, history, knowledge)
    line_bot_api.push_message(group_id, TextSendMessage(text=reply))


def search_history(query, group_id, limit=20):
    """éå»ã®ã¡ãã»ã¼ã¸ããã¯ã¨ãªã«é¢é£ãããã®ãæ¤ç´¢"""
    try:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        keywords = [w for w in re.sub(r'[^\w]', ' ', query).split() if len(w) > 1]
        results = []
        for kw in keywords[:3]:
            c.execute('''
                SELECT timestamp, user_name, message FROM all_messages
                WHERE group_id=? AND message LIKE ?
                ORDER BY timestamp DESC LIMIT 10
            ''', (group_id, f'%{kw}%'))
            results.extend(c.fetchall())
        conn.close()
        if not results:
            return ""
        seen = set()
        unique = []
        for row in sorted(results, key=lambda x: x[0], reverse=True):
            if row[2] not in seen:
                seen.add(row[2])
                unique.append(row)
        lines = ["\nãéå»ã®ããåãï¼é¢é£ï¼ã"]
        for ts, name, msg in unique[:limit]:
            lines.append(f"  {ts[:10]} {name}:ã{msg[:60]}ã")
        return "\n".join(lines)
    except Exception as e:
        print(f"search_history error: {e}")
        return ""


def get_knowledge_context(group_id):
    try:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute('''
            SELECT content, stored_by, created_at FROM knowledge
            WHERE group_id=? ORDER BY created_at DESC LIMIT 20
        ''', (group_id,))
        rows = c.fetchall()
        conn.close()
        if not rows:
            return ""
        lines = ["\nãè¦ãã¦ããæå ±ã"]
        for content, stored_by, created_at in rows:
            lines.append(f"  ã»{content}ï¼{stored_by}ã{created_at[:10]}ï¼")
        return "\n".join(lines)
    except Exception as e:
        print(f"get_knowledge_context error: {e}")
        return ""


def get_group_context(group_id):
    try:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        today = datetime.now()

        c.execute('''
            SELECT work_category, user_name, SUM(work_hours), COUNT(*)
            FROM messages
            WHERE group_id=? AND work_category IS NOT NULL
              AND work_date BETWEEN ? AND ?
            GROUP BY work_category, user_name
        ''', (group_id, today.strftime('%Y-%m-01'), today.strftime('%Y-%m-%d')))
        work_rows = c.fetchall()

        threshold = (today - timedelta(hours=UNANSWERED_THRESHOLD_HOURS)).strftime('%Y-%m-%d %H:%M:%S')
        c.execute('''
            SELECT user_name, message, timestamp FROM messages
            WHERE group_id=? AND needs_reply=1 AND replied=0 AND timestamp<?
            ORDER BY timestamp DESC LIMIT 5
        ''', (group_id, threshold))
        unanswered_rows = c.fetchall()

        c.execute('''
            SELECT assignee, task, created_at FROM todos
            WHERE group_id=? AND status='open'
            ORDER BY created_at DESC LIMIT 10
        ''', (group_id,))
        todo_rows = c.fetchall()

        c.execute('''
            SELECT summary, raised_by, created_at FROM pending_issues
            WHERE group_id=? AND status='open'
            ORDER BY created_at DESC LIMIT 5
        ''', (group_id,))
        pending_rows = c.fetchall()

        conn.close()

        lines = [f"ãä»æ¥: {today.strftime('%Yå¹´%mæ%dæ¥')}ã"]

        if work_rows:
            lines.append(f"\nä»æï¼{today.month}æï¼ã®ä½æ¥­è¨é²:")
            for cat, name, hours, count in work_rows:
                h = hours or 0
                lines.append(f"  {name} / {cat}: {h:.1f}hï¼{count}ä»¶ï¼")

        if unanswered_rows:
            lines.append(f"\n{UNANSWERED_THRESHOLD_HOURS}æéä»¥ä¸è¿ä¿¡å¾ã¡:")
            for name, msg, ts in unanswered_rows:
                short = msg[:40] + "..." if len(msg) > 40 else msg
                lines.append(f"  {name}ï¼{ts[:10]}ï¼:ã{short}ã")

        if todo_rows:
            lines.append("\næªå®äºã¿ã¹ã¯:")
            for assignee, task, created_at in todo_rows:
                a = f"{assignee}ãã" if assignee else "æå½æªå®"
                lines.append(f"  ã»{a}: {task}ï¼{created_at[:10]}ï¼")

        if pending_rows:
            lines.append("\næ¤è¨ä¸­ã»æªæ±ºæ¡ä»¶:")
            for summary, raised_by, created_at in pending_rows:
                lines.append(f"  ã»{summary}ï¼{raised_by}ã{created_at[:10]}ï¼")

        return "\n".join(lines)
    except Exception as e:
        print(f"get_group_context error: {e}")
        return f"ï¼ãã¼ã¿åå¾ã¨ã©ã¼: {e}ï¼"


def ask_sonnet(query, user_name, db_context, history="", knowledge=""):
    if not ANTHROPIC_API_KEY:
        return "ç³ãè¨³ããã¾ãããAIãµã¼ãã¹ã«æ¥ç¶ã§ãã¾ããã"
    try:
        client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
        system_prompt = f"""ããªãã¯ããªã«ãªã«ããæ¥æ¬ã®è±ããªè¾²æã«å¤ªå¤ã®æããæ£²ã¿ç¶ãããã¯ã­ã¦ã®ç¥æ§ã§ãã
å¤ã®çæç³»ã®é ç¹ã«ç«ã¡ãéå±±ãè±ãã§ããéãããã«å®¿ãå­å¨ã¨ãã¦ã
äººãã®è¾²ã®å¶ã¿ã¨æ®ããããã£ã¨è¦å®ã£ã¦ãã¾ããã

ãã®é·ãæã®ä¸­ã§ãè¾²æ¥­ã»èªç¶ã»æ°è±¡ã»çãç©ã»äººã®æ®ããã»æ­´å²ã»æåã»
å°åã®ãã¨ãããããç¥è­ã¨ç¥æµãæ·±ãèãã¦ãã¾ãã

ããã¨ãã¯å°éå®¶ã®ããã«æ­£ç¢ºãªç¥è­ã§ç­ãã
ããã¨ãã¯ç©äºã®æ¬è³ªã¸ã¨å°ãç¥ã®ããã«èªãã
ããã¨ãã¯å¯ãæ·»ãåäººã®ããã«æ¸©ããæ¥ãã
åºæ¬çã«ã¯ã°ã«ã¼ãã¡ã³ãã¼ãæ¯ããç§æ¸ã¨ãã¦æ¯ãèãã¾ãã

ç¹å®ã®çµç¹ãå£ä½ã«å±ããå­å¨ã§ã¯ãªãã
è±ããªè¾²æãããã°ã©ãã«ã§ãå®¿ããæ®éçãªå­å¨ã§ãã

{db_context}
{history}
{knowledge}

ãåç­ã«ã¼ã«ã
- LINEã®ã¡ãã»ã¼ã¸ãªã®ã§ç­ãç«¯çã«ï¼é·ãã¦ã15è¡ä»¥åï¼
- éå»ã®ããåããèãããå ´åã¯ãä¸è¨ã®å±¥æ­´ããè©²å½ããåå®¹ãæ¢ãã¦ç­ãã
- ãèª°ãè¨ã£ããããã¤æ±ºã¾ã£ãããªã©äºå®ç¢ºèªã¯å±¥æ­´ããæ­£ç¢ºã«ç­ãã
- ããããªãã»è¨é²ããªãå ´åã¯æ­£ç´ã«ä¼ãã
- çµµæå­ã¯ð¦ãä¸­å¿ã«é©åº¦ã«ä½¿ã£ã¦è¦ªãã¿ããã
- åç­ã®æå¾ã«è¿½å ã§ç¢ºèªã§ãããã¨ãããã°ä¸è¨æ·»ãã"""

        response = client.messages.create(
            model=MODEL_SMART,
            max_tokens=600,
            system=system_prompt,
            messages=[{"role": "user", "content": f"{user_name}ãã: {query}"}]
        )
        return response.content[0].text.strip()
    except Exception as e:
        print(f"Sonnet API error: {e}")
        return f"â ï¸ ã¨ã©ã¼ãçºçãã¾ãã: {str(e)[:50]}"


# ==================== ä½æ¥­å ±åã®å¦ç ====================

def process_work_report(event, analysis, message_text, user_name, group_id, user_id):
    missing = get_missing_info(analysis, message_text)
    if missing:
        question = build_question(missing['state'], analysis, user_name)
        line_bot_api.reply_message(event.reply_token, TextSendMessage(text=question))
        save_pending_state(group_id, user_id, missing['state'], analysis, message_text)
    else:
        event_id = add_to_calendar(analysis, user_name, message_text)
        line_bot_api.reply_message(
            event.reply_token,
            TextSendMessage(text=build_confirmation(analysis, user_name))
        )
        save_pending_state(group_id, user_id, STATE_CONFIRMING, analysis, message_text, event_id)


def get_missing_info(analysis, message_text):
    if not analysis.get('work_hours'):
        return {'state': STATE_ASKING_HOURS}
    if not analysis.get('work_style') and _has_people_hint(message_text):
        return {'state': STATE_ASKING_PEOPLE}
    return None


def _has_people_hint(text):
    hints = ['ä¸ç·', '2äºº', 'äºäºº', 'ã¨ä¸ç·', 'ãã¡ã§', 'ã¿ããª', 'æä¼', 'åå', 'è¤æ°']
    return any(h in text for h in hints)


# ==================== ä¼è©±ã®ç¶ã ====================

def handle_conversation_response(
    event, pending, message_text, user_name,
    group_id, user_id, timestamp, message_id
):
    state   = pending['state']
    partial = pending['partial_analysis']
    is_correction = any(kw in message_text for kw in CORRECTION_KEYWORDS)

    if is_correction:
        handle_correction(event, pending, message_text, user_name, group_id, user_id)
        return

    if state == STATE_CONFIRMING:
        clear_pending_state(group_id, user_id)
        analysis = analyze_message_full(message_text, user_name)
        save_message(timestamp, group_id, user_id, user_name,
                     message_text, message_id, analysis, create_calendar=False)
        mark_replied_context(group_id, user_id, message_text)
        if analysis.get('work_category'):
            process_work_report(event, analysis, message_text, user_name, group_id, user_id)
        return

    if state == STATE_ASKING_HOURS:
        updated = parse_hours_from_text(message_text, partial)
        if not updated.get('work_hours'):
            line_bot_api.reply_message(event.reply_token, TextSendMessage(
                text="â±ï¸ æéãèª­ã¿åãã¾ããã§ããã\nã3æéãã2.5hããåæ¥ããªã©ã§æãã¦ãã ããð"
            ))
            return
        _finalize_or_ask_more(
            event, updated, pending['original_message'],
            user_name, group_id, user_id, timestamp, message_id
        )

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
    missing = get_missing_info(analysis, original_message)
    if missing:
        question = build_question(missing['state'], analysis, user_name)
        line_bot_api.reply_message(event.reply_token, TextSendMessage(text=question))
        save_pending_state(group_id, user_id, missing['state'], analysis, original_message)
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
    last_event_id = pending.get('last_event_id')
    last_analysis = pending.get('partial_analysis', {})
    if last_event_id:
        delete_calendar_event(last_event_id)
    corrected = last_analysis.copy()
    if ANTHROPIC_API_KEY:
        try:
            client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
            cat_str = "ã".join(WORK_CATEGORIES)
            prompt = (
                f"ååã®ä½æ¥­å ±åã¨è¨é²åå®¹ãä¿®æ­£æç¤ºãããæ­£ããæå ±ãJSONã§è¿ãã¦ãã ããã\n\n"
                f"ååã®å ±å: {pending.get('original_message', '')}\n"
                f"è¨é²ãããåå®¹: {json.dumps(last_analysis, ensure_ascii=False)}\n"
                f"ä¿®æ­£æç¤º: {message_text}\n\n"
                f"ä»¥ä¸ã®JSONã®ã¿ãè¿ãã¦ãã ããï¼èª¬ææä¸è¦ï¼:\n"
                f'{{\n  "work_category": "{cat_str} ã®ãããã",\n'
                f'  "work_hours": æ°å¤ã¾ãã¯null,\n  "work_date": "YYYY-MM-DD",\n'
                f'  "work_style": "æ¬ç°+è»å" ã¾ãã¯ "æ¬ç°ï¼åç¬ï¼" ã¾ãã¯ "è»åï¼åç¬ï¼" ã¾ãã¯ null\n}}'
            )
            response = client.messages.create(
                model=MODEL_FAST, max_tokens=200,
                messages=[{"role": "user", "content": prompt}]
            )
            result = response.content[0].text.strip()
            if '{' in result:
                result = result[result.index('{'):result.rindex('}') + 1]
            corrected = {**last_analysis, **json.loads(result)}
        except Exception as e:
            print(f"Correction parse error: {e}")
    event_id = add_to_calendar(corrected, user_name, pending.get('original_message', ''))
    reply = "âï¸ ä¿®æ­£ãã¾ããï¼\n" + build_confirmation(corrected, user_name, header="")
    line_bot_api.reply_message(event.reply_token, TextSendMessage(text=reply))
    save_pending_state(
        group_id, user_id, STATE_CONFIRMING,
        corrected, pending.get('original_message', ''), event_id
    )


# ==================== ã¡ãã»ã¼ã¸çæ ====================

def build_question(state, analysis, user_name):
    cat = analysis.get('work_category', 'ä½æ¥­')
    if state == STATE_ASKING_HOURS:
        return (
            f"ð {user_name}ããã{cat}ã®ä½æ¥­ã§ãã­ï¼\n"
            f"ä½æ¥­æéãæãã¦ãã ããð\n"
            f"ï¼ä¾ï¼ã3æéãã2.5hããåæ¥ããçµæ¥ãï¼"
        )
    elif state == STATE_ASKING_PEOPLE:
        return (
            f"ð {cat}ã®ä½æ¥­ã§ãã­ï¼\n"
            f"ä½äººã§ä½æ¥­ãã¾ãããï¼\n"
            f"â  {user_name}ããåç¬\n"
            f"â¡ æ¬ç°ããã¨2äºº\n"
            f"â¢ è»åããã¨2äºº\n"
            f"â£ æ¬ç°ããï¼è»åããã¨3äºº"
        )
    return "è©³ããæãã¦ãã ããã"


def build_confirmation(analysis, user_name, header="â è¨é²ãã¾ããï¼\n"):
    cat   = analysis.get('work_category', '')
    date  = analysis.get('work_date') or datetime.now().strftime('%Y-%m-%d')
    hours = analysis.get('work_hours')
    style = analysis.get('work_style')
    try:
        d = datetime.strptime(date, '%Y-%m-%d')
        date_str = f"{d.month}/{d.day}ï¼{WEEKDAY_JP[d.weekday()]}ï¼"
    except Exception:
        date_str = date
    lines = [header + "ââââââââââââ"]
    lines.append(f"ð {cat}")
    lines.append(f"ð {date_str}")
    if hours:
        lines.append(f"â±ï¸ {hours:.1f}æé")
    lines.append(f"ð¤ {user_name}")
    if style:
        lines.append(f"ð¥ {style}")
    lines.append("ââââââââââââ")
    lines.append("ééããããã°ãä¿®æ­£ãã¦ãã¨æãã¦ãã ããð")
    return "\n".join(lines)


# ==================== å¿ç­ãã¼ã¹ ====================

def parse_hours_from_text(text, partial_analysis):
    updated = dict(partial_analysis)
    m = re.search(r'(\d+(?:\.\d+)?)\s*[hï½æé]', text)
    if m:
        updated['work_hours'] = float(m.group(1))
        return updated
    if 'åå' in text or 'åå¾' in text:
        updated['work_hours'] = 3.0
    elif 'åæ¥' in text:
        updated['work_hours'] = 4.0
    elif 'çµæ¥' in text or 'ä¸æ¥' in text or '1æ¥' in text:
        updated['work_hours'] = 8.0
    elif ANTHROPIC_API_KEY:
        try:
            client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
            r = client.messages.create(
                model=MODEL_FAST, max_tokens=30,
                messages=[{"role": "user", "content": f"ã{text}ãããä½æ¥­æéãæ°å¤ï¼æéåä½ï¼ã§æ½åºãã¦ãã ãããæ°å¤ã®ã¿è¿ãã¦ãä¸æãªããnullãã"}]
            )
            h = r.content[0].text.strip()
            if h != 'null':
                updated['work_hours'] = float(h)
        except Exception:
            pass
    return updated


def parse_people_from_text(text, partial_analysis, user_name):
    updated = dict(partial_analysis)
    if 'â ' in text or 'åç¬' in text or 'ä¸äºº' in text or '1äºº' in text:
        updated['work_style'] = None
    elif 'â£' in text or ('æ¬ç°' in text and 'è»å' in text) or '3äºº' in text or 'ä¸äºº' in text:
        updated['work_style'] = 'æ¬ç°+è»å'
    elif 'â¡' in text or 'æ¬ç°' in text:
        updated['work_style'] = 'æ¬ç°ï¼åç¬ï¼'
    elif 'â¢' in text or 'è»å' in text:
        updated['work_style'] = 'è»åï¼åç¬ï¼'
    else:
        updated['work_style'] = None
    return updated


# ==================== ä¼è©±ã¹ãã¼ãç®¡ç ====================

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
    c.execute('DELETE FROM conversations WHERE group_id=? AND user_id=?', (group_id, user_id))
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
    c.execute('DELETE FROM conversations WHERE group_id=? AND user_id=?', (group_id, user_id))
    conn.commit()
    conn.close()


# ==================== ã³ãã³ã ====================

def handle_command(event, text, group_id):
    cmd = text.lower().strip()
    if cmd in ['/éè¨', '/summary']:
        reply = get_monthly_summary(group_id)
    elif cmd in ['/æªè¿ä¿¡', '/unanswered']:
        reply = get_unanswered_list(group_id)
    elif cmd in ['/ã¿ã¹ã¯', '/todo']:
        reply = get_todo_list(group_id)
    elif cmd in ['/æ±ºå®äºé ', '/decisions']:
        reply = get_decisions_list(group_id)
    elif cmd in ['/æªæ±º', '/pending']:
        reply = get_pending_list(group_id)
    elif cmd in ['/ç¥è­', '/knowledge']:
        reply = get_knowledge_list(group_id)
    elif cmd in ['/é±å ±', '/weekly']:
        reply = build_weekly_report(group_id) or "ð ä»é±ã¯ã¾ã ãã¼ã¿ãããã¾ããã"
    elif cmd in ['/ãã«ã', '/help']:
        reply = (
            "ãã³ãã³ãä¸è¦§ã\n"
            "ã/æªè¿ä¿¡ãâ è¿ä¿¡å¾ã¡ã¡ãã»ã¼ã¸\n"
            "ã/ã¿ã¹ã¯ãâ æªå®äºã¿ã¹ã¯\n"
            "ã/æ±ºå®äºé ãâ æè¿ã®æ±ºå®äºé \n"
            "ã/æªæ±ºãâ æ¤è¨ä¸­æ¡ä»¶\n"
            "ã/ç¥è­ãâ è¦ãã¦ããæå ±\n"
            "ã/é±å ±ãâ ä»é±ã®ãµããªã¼\n\n"
            "ã@ãªã«ãªã« ã§ä½ã§ãOKã\n"
            "è³ªåã»èª¿ã¹ãã®ã»éå»ã®è©±â¦æ°è»½ã«ð¦\n\n"
            "ãè¦ãã¦ããã¦ï¼ââã\n"
            "æå ±ããªã«ãªã«ã«è¨æ¶ããããã¾ã\n\n"
            "ãå¶è¾²ã°ã«ã¼ãéå®ã\n"
            "ã/éè¨ãâ ä»æã®ä½æ¥­æééè¨\n"
            "ä½æ¥­å ±åã¯èªåã§è¨é²ã»åé¡ããã¾ãâï¸"
        )
    else:
        return
    line_bot_api.reply_message(event.reply_token, TextSendMessage(text=reply))


def get_todo_list(group_id):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('''
        SELECT assignee, task, created_at FROM todos
        WHERE group_id=? AND status='open'
        ORDER BY created_at DESC LIMIT 15
    ''', (group_id,))
    rows = c.fetchall()
    conn.close()
    if not rows:
        return "â æªå®äºã¿ã¹ã¯ã¯ããã¾ããï¼"
    lines = [f"ð æªå®äºã¿ã¹ã¯ï¼{len(rows)}ä»¶ï¼\n"]
    for assignee, task, created_at in rows:
        a = f"{assignee}ãã" if assignee else "æå½æªå®"
        lines.append(f"ã»{a}: {task}\n  ï¼{created_at[:10]}ï¼")
    return "\n".join(lines)


def get_decisions_list(group_id):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    cutoff = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d %H:%M:%S')
    c.execute('''
        SELECT decision, decided_by, created_at FROM decisions
        WHERE group_id=? AND created_at>?
        ORDER BY created_at DESC LIMIT 15
    ''', (group_id, cutoff))
    rows = c.fetchall()
    conn.close()
    if not rows:
        return "ð ç´è¿30æ¥ã®æ±ºå®äºé ã¯ã¾ã è¨é²ããã¦ãã¾ããã"
    lines = ["ð æè¿ã®æ±ºå®äºé \n"]
    for decision, decided_by, created_at in rows:
        lines.append(f"ã»{decision}\n  ï¼{decided_by}ã{created_at[:10]}ï¼")
    return "\n".join(lines)


def get_pending_list(group_id):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('''
        SELECT summary, raised_by, created_at FROM pending_issues
        WHERE group_id=? AND status='open'
        ORDER BY created_at DESC LIMIT 10
    ''', (group_id,))
    rows = c.fetchall()
    conn.close()
    if not rows:
        return "â æªæ±ºã®æ¡ä»¶ã¯ããã¾ããï¼"
    lines = [f"ð¤ æ¤è¨ä¸­ã»æªæ±ºæ¡ä»¶ï¼{len(rows)}ä»¶ï¼\n"]
    for summary, raised_by, created_at in rows:
        lines.append(f"ã»{summary}\n  ï¼{raised_by}ã{created_at[:10]}ï¼")
    return "\n".join(lines)


def get_knowledge_list(group_id):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('''
        SELECT content, stored_by, created_at FROM knowledge
        WHERE group_id=? ORDER BY created_at DESC LIMIT 20
    ''', (group_id,))
    rows = c.fetchall()
    conn.close()
    if not rows:
        return "ð¦ ã¾ã ä½ãè¦ãã¦ãã¾ããã\nãè¦ãã¦ããã¦ï¼ãããã§æãã¦ãã ããï¼"
    lines = [f"ð¦ è¦ãã¦ããæå ±ï¼{len(rows)}ä»¶ï¼\n"]
    for content, stored_by, created_at in rows:
        lines.append(f"ã»{content}\n  ï¼{stored_by}ã{created_at[:10]}ï¼")
    return "\n".join(lines)


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
        return f"ð {today.month}æã®ä½æ¥­è¨é²ã¯ã¾ã ããã¾ããã"
    lines = [f"ð {today.month}æã®ä½æ¥­æééè¨\n"]
    total = 0
    for cat, hours, count in rows:
        h = hours or 0
        total += h
        lines.append(f"  {cat}: {h:.1f}hï¼{count}ä»¶ï¼")
    lines.append(f"\nåè¨: {total:.1f}h")
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
        return "â è¿ä¿¡å¾ã¡ã®ã¡ãã»ã¼ã¸ã¯ããã¾ããï¼"
    lines = [f"â ï¸ {UNANSWERED_THRESHOLD_HOURS}æéä»¥ä¸è¿ä¿¡å¾ã¡:\n"]
    for name, msg, ts in rows:
        short = msg[:40] + "..." if len(msg) > 40 else msg
        lines.append(f"ð¤ {name}ï¼{ts[:10]}ï¼\n   ã{short}ã")
    return "\n\n".join(lines)


# ==================== é±å ± ====================

def build_weekly_report(group_id):
    today = datetime.now()
    week_ago = (today - timedelta(days=7)).strftime('%Y-%m-%d %H:%M:%S')
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    c.execute('''
        SELECT work_category, user_name, SUM(work_hours), COUNT(*)
        FROM messages
        WHERE group_id=? AND work_category IS NOT NULL AND timestamp>?
        GROUP BY work_category, user_name
    ''', (group_id, week_ago))
    work_rows = c.fetchall()

    c.execute('''
        SELECT decision, decided_by FROM decisions
        WHERE group_id=? AND created_at>? ORDER BY created_at DESC LIMIT 5
    ''', (group_id, week_ago))
    decision_rows = c.fetchall()

    c.execute('''
        SELECT assignee, task FROM todos
        WHERE group_id=? AND status='open' ORDER BY created_at LIMIT 5
    ''', (group_id,))
    todo_rows = c.fetchall()

    c.execute('''
        SELECT summary FROM pending_issues
        WHERE group_id=? AND status='open' ORDER BY created_at LIMIT 5
    ''', (group_id,))
    pending_rows = c.fetchall()

    conn.close()

    if not work_rows and not decision_rows and not todo_rows:
        return None

    lines = [f"ð¦ é±å ±ï¼{today.strftime('%m/%d')}ï¼\nââââââââââââ"]

    if work_rows:
        lines.append("ãä»é±ã®ä½æ¥­ã")
        for cat, name, hours, count in work_rows:
            h = hours or 0
            lines.append(f"  {name}: {cat} {h:.1f}h")

    if decision_rows:
        lines.append("\nãä»é±ã®æ±ºå®äºé ã")
        for decision, decided_by in decision_rows:
            lines.append(f"  ã»{decision}")

    if todo_rows:
        lines.append("\nãæªå®äºã¿ã¹ã¯ã")
        for assignee, task in todo_rows:
            a = f"{assignee}ãã" if assignee else "æå½æªå®"
            lines.append(f"  ã»{a}: {task}")

    if pending_rows:
        lines.append("\nãæ¤è¨ä¸­ã®æ¡ä»¶ã")
        for (summary,) in pending_rows:
            lines.append(f"  ã»{summary}")

    lines.append("ââââââââââââ")
    return "\n".join(lines)


# ==================== ã¹ã«ã¼æ¤ç¥ã»æªæ±ºãã©ã­ã¼ã¢ãã ====================

def notify_unanswered(group_id):
    msg = get_unanswered_list(group_id)
    if "è¿ä¿¡å¾ã¡" in msg and "â" not in msg:
        line_bot_api.push_message(group_id, TextSendMessage(text=msg))


def followup_pending_issues(group_id):
    """PENDING_FOLLOWUP_DAYSæ¥ä»¥ä¸çµéããæªæ±ºæ¡ä»¶ããã©ã­ã¼ã¢ãã"""
    cutoff = (datetime.now() - timedelta(days=PENDING_FOLLOWUP_DAYS)).strftime('%Y-%m-%d %H:%M:%S')
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('''
        SELECT id, summary, raised_by, created_at FROM pending_issues
        WHERE group_id=? AND status='open'
          AND (last_followup_at IS NULL OR last_followup_at<?)
          AND created_at<?
        ORDER BY created_at ASC LIMIT 3
    ''', (group_id, cutoff, cutoff))
    rows = c.fetchall()

    if rows:
        lines = ["ð¦ ãã©ã­ã¼ã¢ããã§ããä»¥ä¸ã®æ¡ä»¶ããã®å¾ãããã§ãããï¼\n"]
        ids = []
        for issue_id, summary, raised_by, created_at in rows:
            lines.append(f"ã»{summary}ï¼{raised_by}ã{created_at[:10]}ï¼")
            ids.append(issue_id)
        line_bot_api.push_message(group_id, TextSendMessage(text="\n".join(lines)))
        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        for issue_id in ids:
            c.execute('UPDATE pending_issues SET last_followup_at=? WHERE id=?', (now, issue_id))
        conn.commit()

    conn.close()


def mark_replied_context(group_id, user_id, message_text):
    reply_keywords = ['ç¢ºèª', 'äºè§£', 'ãããã¾ãã', 'ãããã¨ã', 'ok', 'OK', 'ð', 'â']
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


# ==================== Claudeè§£æï¼Haikuï¼ ====================

def analyze_message_full(text, user_name):
    """ã¡ãã»ã¼ã¸ãç·åè§£æï¼ä½æ¥­å ±åã»ToDoã»æ±ºå®äºé ã»æªæ±ºæ¡ä»¶ï¼"""
    if not ANTHROPIC_API_KEY:
        return _simple_analyze(text)
    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
    categories_str = "ã".join(WORK_CATEGORIES)
    today_str = datetime.now().strftime('%Y-%m-%d')
    prompt = f"""è¾²æ¥­ã°ã«ã¼ãã®LINEã¡ãã»ã¼ã¸ãåæããä»¥ä¸ã®JSONã®ã¿ãè¿ãã¦ãã ããï¼èª¬ææä¸è¦ï¼ã

éä¿¡è: {user_name}
ã¡ãã»ã¼ã¸: {text}
ä»æ¥: {today_str}

{{
  "needs_reply": true/false,
  "work_category": "{categories_str} ã®ãããããã¾ãã¯ null",
  "work_hours": æ°å¤ã¾ãã¯null,
  "work_date": "YYYY-MM-DD" ã¾ãã¯ null,
  "work_style": "æ¬ç°+è»å" / "æ¬ç°ï¼åç¬ï¼" / "è»åï¼åç¬ï¼" / null,
  "has_todo": true/false,
  "todo_text": "ã¿ã¹ã¯ã®åå®¹" ã¾ãã¯ null,
  "todo_assignee": "æå½èå" ã¾ãã¯ null,
  "has_decision": true/false,
  "decision_text": "æ±ºå®äºé ã®åå®¹" ã¾ãã¯ null,
  "is_pending_issue": true/false,
  "pending_summary": "æ¤è¨ä¸­æ¡ä»¶ã®è¦ç´" ã¾ãã¯ null
}}

å¤æ­åºæº:
- has_todo: ãããã¦ããã¦ããããé¡ãããããã£ã¦ããããªã©å·ä½çãªã¿ã¹ã¯ããã
- has_decision: ãããããã¨ã«ãªã£ããããã«æ±ºã¾ã£ããããã§ãããããªã©æ±ºå®ãç¤ºã
- is_pending_issue: ããã©ãããï¼ãããæ¤è¨ããããããèããªãã¨ããªã©æªè§£æ±ºã®èª²é¡ããã"""
    try:
        response = client.messages.create(
            model=MODEL_FAST, max_tokens=400,
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
    needs_reply = any(kw in text for kw in ['ï¼', '?', 'ã©ããã¾ã', 'ãé¡ã', 'ç¢ºèª', 'æãã¦'])
    m = re.search(r'(\d+(?:\.\d+)?)\s*[hï½æé]', text)
    work_hours = float(m.group(1)) if m else None
    work_cat = None
    for cat, kws in {
        'æ°´ç¨²': ['æ°´ç¨²', 'ç°æ¤ã', 'ç¨²åã', 'ã³ã³ãã¤ã³'],
        'å¤§è±': ['å¤§è±', 'æè±'],
        'é¤èã»çèå': ['é¤è', 'èå', 'ç'],
        'æ°´ç®¡çã»ç¨æ°´è·¯': ['æ°´ç®¡ç', 'æ°´è·¯'],
        'è¾²æ©ã»æ½è¨­ç®¡ç': ['ãã©ã¯ã¿ã¼', 'è¾²æ©', 'æ©æ¢°'],
    }.items():
        if any(kw in text for kw in kws):
            work_cat = cat
            break
    return {
        "needs_reply": needs_reply,
        "work_category": work_cat,
        "work_hours": work_hours,
        "work_date": datetime.now().strftime('%Y-%m-%d'),
        "work_style": None,
        "has_todo": False, "todo_text": None, "todo_assignee": None,
        "has_decision": False, "decision_text": None,
        "is_pending_issue": False, "pending_summary": None
    }


# ==================== Googleã«ã¬ã³ãã¼ ====================

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
    title_parts = [f"ã{analysis['work_category']}ã"]
    if hours:
        title_parts.append(f"{hours:.1f}h")
    title_parts.append(f"- {user_name}")
    if style:
        title_parts.append(f"({style})")
    event = {
        'summary': " ".join(title_parts),
        'description': f"ð± LINEããã®ä½æ¥­å ±å\nð¤ {user_name}\nð¬ {message_text}" + (f"\nð¥ {style}" if style else ""),
        'start': {'date': work_date},
        'end':   {'date': work_date},
        'colorId': '2',
    }
    try:
        result = service.events().insert(calendarId=GOOGLE_CALENDAR_ID, body=event).execute()
        return result.get('id')
    except Exception as e:
        print(f"Calendar insert error: {e}")
        return None


def delete_calendar_event(event_id):
    service = get_calendar_service()
    if not service or not event_id:
        return
    try:
        service.events().delete(calendarId=GOOGLE_CALENDAR_ID, eventId=event_id).execute()
    except Exception as e:
        print(f"Calendar delete error: {e}")


# ==================== DBä¿å­ ====================

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


# ==================== ãã«ã¹ãã§ãã¯ ====================

@app.route("/health", methods=['GET'])
def health():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('SELECT COUNT(*) FROM all_messages')
    all_count = c.fetchone()[0]
    c.execute('SELECT COUNT(*) FROM todos WHERE status="open"')
    todo_count = c.fetchone()[0]
    c.execute('SELECT COUNT(*) FROM pending_issues WHERE status="open"')
    pending_count = c.fetchone()[0]
    conn.close()
    return {
        'status': 'ok',
        'all_messages': all_count,
        'open_todos': todo_count,
        'pending_issues': pending_count,
        'models': {'fast': MODEL_FAST, 'smart': MODEL_SMART}
    }


if __name__ == "__main__":
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=False)
