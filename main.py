"""
しきしまの家自給家族 LINEサポートボット
- 全メッセージ保存・さかのぼり検索
- やり取りの自動記録・分類（Haiku）
- ToDoの自動抽出・管理
- 決定事項の自動記録
- 未決案件の検知・フォローアップ
- 知識の蓄積（「覚えておいて」）
- @メンションには何でも回答（Sonnet）
- 画像を送ると内容を解析・説明（Sonnet Vision）
- 毎朝9時：スルー検知・未決フォローアップ
- 毎週月曜：週報自動投稿
- Googleカレンダーに記録を転記
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
GOOGLE_SPREADSHEET_ID = os.environ.get('GOOGLE_SPREADSHEET_ID', '')

MODEL_FAST  = "claude-3-haiku-20240307"
MODEL_SMART = "claude-3-5-sonnet-20241022"

UNANSWERED_THRESHOLD_HOURS = 24
PENDING_FOLLOWUP_DAYS = 3
WEEKDAY_JP = ['月', '火', '水', '木', '金', '土', '日']

KNOWLEDGE_KEYWORDS = ['覚えておいて', '覚えて', '記録しておいて', 'メモしておいて']

_BOT_USER_ID = None


# ==================== DB初期化 ====================

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
            tag TEXT,
            scope TEXT DEFAULT 'group'
        )
    ''')
    # 既存DBにscopeカラムがない場合は追加（マイグレーション）
    try:
        c.execute("ALTER TABLE knowledge ADD COLUMN scope TEXT DEFAULT 'group'")
        conn.commit()
    except Exception:
        pass  # すでに存在する場合はスキップ
    conn.commit()
    conn.close()


init_db()


# ==================== スケジューラ ====================

def daily_check():
    """毎朝9時：未返信チェック + 未決案件フォローアップ"""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('SELECT group_id FROM groups')
    groups = [row[0] for row in c.fetchall()]
    conn.close()
    for gid in groups:
        notify_unanswered(gid)
        followup_pending_issues(gid)


def weekly_report_job():
    """毎週月曜9時：週報を全グループに投稿"""
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
                "オルオルです🦉\n\n"
                "太古から農村を見守ってきた者として、\n"
                "皆さんのお役に立てれば幸いです。\n\n"
                "@オルオル と呼びかけてください。\n"
                "どんな質問・相談にも答えます。\n\n"
                "【コマンド】\n"
                "「/未返信」→ 返信待ちメッセージ一覧\n"
                "「/タスク」→ 未完了タスク一覧\n"
                "「/決定事項」→ 最近の決定事項\n"
                "「/未決」→ 検討中案件一覧\n"
                "「/知識」→ このグループ専用の知識🔒\n"
                "「/共通知識」→ 全グループ共通の知識🌐\n"
                "「/ヘルプ」→ 使い方"
            )
        ))


# ==================== メンション検知 ====================

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


def get_group_name(group_id):
    try:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute('SELECT group_name FROM groups WHERE group_id=?', (group_id,))
        row = c.fetchone()
        conn.close()
        if row and row[0]:
            return row[0]
    except Exception:
        pass
    return group_id


def get_user_name(group_id, user_id):
    try:
        profile = line_bot_api.get_group_member_profile(group_id, user_id)
        return profile.display_name
    except Exception:
        return user_id


# ==================== 画像メッセージ対応（Sonnet Vision） ====================

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
            TextSendMessage(text="画像の取得に失敗しました🦉 もう一度送ってみてください。")
        )
        return
    reply = analyze_image_with_sonnet(image_base64, user_name)
    line_bot_api.reply_message(event.reply_token, TextSendMessage(text=reply))


def analyze_image_with_sonnet(image_base64, user_name):
    if not ANTHROPIC_API_KEY:
        return "申し訳ありません、AIサービスに接続できません。"
    try:
        client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
        response = client.messages.create(
            model=MODEL_SMART,
            max_tokens=600,
            system=(
                "あなたは「オルオル」。しきしまの家自給家族のLINEグループを見守る秘書的な存在です。\n"
                "送られてきた画像を見て、写っている内容を詳しく解説してください。\n"
                "【回答ルール】\n"
                "- 何が写っているか簡潔に特定する\n"
                "- 内容に関連する有用な情報があれば補足する\n"
                "- LINEなので長くても15行以内\n"
                "- 絵文字は🦉を中心に適度に使う\n"
                "- 判別が難しい場合は正直にその旨を伝える"
            ),
            messages=[{
                "role": "user",
                "content": [
                    {"type": "image", "source": {"type": "base64", "media_type": "image/jpeg", "data": image_base64}},
                    {"type": "text", "text": f"{user_name}さんが画像を送りました。これは何ですか？詳しく教えてください。"}
                ]
            }]
        )
        return response.content[0].text.strip()
    except Exception as e:
        print(f"Image analysis error: {e}")
        return f"⚠️ 画像の解析中にエラーが発生しました: {str(e)[:50]}"


# ==================== メッセージ受信（メイン） ====================

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

    # 全メッセージを保存
    group_name = get_group_name(group_id)
    save_all_message(timestamp, group_id, user_id, user_name, message_text, message_id)
    append_to_sheet(timestamp, group_name, user_name, message_text)

    # ① コマンド
    if message_text.startswith('/'):
        handle_command(event, message_text, group_id)
        return

    # ② @メンション
    if is_bot_mentioned(event):
        handle_mention(event, message_text, user_name, group_id)
        return

    # ③ 「覚えておいて」
    if any(kw in message_text for kw in KNOWLEDGE_KEYWORDS):
        handle_knowledge_store(event, message_text, user_name, group_id)
        return

    # ④ 通常メッセージ解析（Haiku）
    analysis = analyze_message_full(message_text, user_name)
    mark_replied_context(group_id, user_id, message_text)

    save_message(timestamp, group_id, user_id, user_name,
                 message_text, message_id, analysis)

    process_metadata(analysis, message_text, user_name, group_id, timestamp)


# ==================== 全メッセージ保存 ====================

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


# ==================== ToDo・決定事項・未決案件の処理 ====================

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


# ==================== 知識ベース ====================

SHARED_KNOWLEDGE_KEYWORDS = ['共通で', '全グループで', 'みんなで覚えて', '共通知識として', '全体で覚えて']

def handle_knowledge_store(event, message_text, user_name, group_id):
    content = message_text

    # scopeの判定（「共通で覚えておいて」→ shared）
    scope = 'group'
    for sk in SHARED_KNOWLEDGE_KEYWORDS:
        if sk in content:
            scope = 'shared'
            content = content.replace(sk, '').strip()
            break

    for kw in KNOWLEDGE_KEYWORDS:
        content = content.replace(kw, '').strip()
    content = content.lstrip('：:').strip()

    if not content:
        line_bot_api.reply_message(
            event.reply_token,
            TextSendMessage(text="覚えておきたい内容を「覚えておいて：〇〇」の形で教えてください🦉\n全グループ共通で覚えたい場合は「共通で覚えておいて：〇〇」")
        )
        return

    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute(
        'INSERT INTO knowledge (group_id, content, stored_by, created_at, scope) VALUES (?, ?, ?, ?, ?)',
        (group_id, content, user_name, datetime.now().strftime('%Y-%m-%d %H:%M:%S'), scope)
    )
    conn.commit()
    conn.close()

    if scope == 'shared':
        scope_label = "【全グループ共通】"
        cmd_hint = "「/共通知識」で確認できます。"
    else:
        scope_label = "【このグループ専用】"
        cmd_hint = "「/知識」で確認できます。"

    line_bot_api.reply_message(
        event.reply_token,
        TextSendMessage(text=f"🦉 覚えました！{scope_label}\n「{content[:50]}」\n\n{cmd_hint}")
    )


# ==================== @メンション対応（Sonnet） ====================

def handle_mention(event, message_text, user_name, group_id):
    query = extract_mention_text(message_text)
    if not query:
        line_bot_api.reply_message(
            event.reply_token,
            TextSendMessage(text="はい！何でもお気軽にどうぞ🦉")
        )
        return
    context  = get_group_context(group_id)
    history  = search_history(query, group_id)
    knowledge = get_knowledge_context(group_id)
    reply = ask_sonnet(query, user_name, context, history, knowledge)
    line_bot_api.reply_message(event.reply_token, TextSendMessage(text=reply))


def search_history(query, group_id, limit=20):
    """過去のメッセージからクエリに関連するものを検索"""
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
        lines = ["\n【過去のやり取り（関連）】"]
        for ts, name, msg in unique[:limit]:
            lines.append(f"  {ts[:10]} {name}:「{msg[:60]}」")
        return "\n".join(lines)
    except Exception as e:
        print(f"search_history error: {e}")
        return ""


def get_knowledge_context(group_id):
    try:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        # このグループ専用 + 全グループ共通の両方を取得
        c.execute('''
            SELECT content, stored_by, created_at, scope FROM knowledge
            WHERE (group_id=? AND scope='group') OR scope='shared'
            ORDER BY scope ASC, created_at DESC LIMIT 30
        ''', (group_id,))
        rows = c.fetchall()
        conn.close()
        if not rows:
            return ""
        lines = ["\n【覚えている情報】"]
        for content, stored_by, created_at, scope in rows:
            label = "🌐共通" if scope == 'shared' else "🔒専用"
            lines.append(f"  ・[{label}] {content}（{stored_by}、{created_at[:10]}）")
        return "\n".join(lines)
    except Exception as e:
        print(f"get_knowledge_context error: {e}")
        return ""


def get_group_context(group_id):
    try:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        today = datetime.now()

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

        lines = [f"【今日: {today.strftime('%Y年%m月%d日')}】"]

        if unanswered_rows:
            lines.append(f"\n{UNANSWERED_THRESHOLD_HOURS}時間以上返信待ち:")
            for name, msg, ts in unanswered_rows:
                short = msg[:40] + "..." if len(msg) > 40 else msg
                lines.append(f"  {name}（{ts[:10]}）:「{short}」")

        if todo_rows:
            lines.append("\n未完了タスク:")
            for assignee, task, created_at in todo_rows:
                a = f"{assignee}さん" if assignee else "担当未定"
                lines.append(f"  ・{a}: {task}（{created_at[:10]}）")

        if pending_rows:
            lines.append("\n検討中・未決案件:")
            for summary, raised_by, created_at in pending_rows:
                lines.append(f"  ・{summary}（{raised_by}、{created_at[:10]}）")

        return "\n".join(lines)
    except Exception as e:
        print(f"get_group_context error: {e}")
        return f"（データ取得エラー: {e}）"


def ask_sonnet(query, user_name, db_context, history="", knowledge=""):
    if not ANTHROPIC_API_KEY:
        return "申し訳ありません、AIサービスに接続できません。"
    try:
        client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
        system_prompt = f"""あなたは「オルオル」。しきしまの家自給家族のLINEグループを見守る秘書です。
顧客・メンバーとのやり取りを丁寧にサポートし、
必要な情報を素早く引き出したり、記録を整理したりする頼れる存在です。

豊富な知識と記憶力を活かして、
あるときは正確な情報を提供し、
あるときは過去のやり取りから必要な経緯を掘り起こし、
あるときは温かく寄り添う友人のように接しながら、
常にグループメンバーと顧客の橋渡しをする秘書として振る舞います。

{db_context}
{history}
{knowledge}

【回答ルール】
- LINEのメッセージなので短く端的に（長くても15行以内）
- 過去のやり取りを聞かれた場合は、上記の履歴から該当する内容を探して答える
- 「誰が言った」「いつ決まった」など事実確認は履歴から正確に答える
- わからない・記録がない場合は正直に伝える
- 絵文字は🦉を中心に適度に使って親しみやすく
- 回答の最後に追加で確認できることがあれば一言添える"""

        response = client.messages.create(
            model=MODEL_SMART,
            max_tokens=600,
            system=system_prompt,
            messages=[{"role": "user", "content": f"{user_name}さん: {query}"}]
        )
        return response.content[0].text.strip()
    except Exception as e:
        print(f"Sonnet API error: {e}")
        return f"⚠️ エラーが発生しました: {str(e)[:50]}"



# ==================== コマンド ====================

def handle_command(event, text, group_id):
    cmd = text.lower().strip()
    if cmd in ['/未返信', '/unanswered']:
        reply = get_unanswered_list(group_id)
    elif cmd in ['/タスク', '/todo']:
        reply = get_todo_list(group_id)
    elif cmd in ['/決定事項', '/decisions']:
        reply = get_decisions_list(group_id)
    elif cmd in ['/未決', '/pending']:
        reply = get_pending_list(group_id)
    elif cmd in ['/知識', '/knowledge']:
        reply = get_knowledge_list(group_id)
    elif cmd in ['/共通知識', '/shared']:
        reply = get_shared_knowledge_list()
    elif cmd in ['/グループid', '/groupid', '/group_id']:
        reply = f"🦉 このグループのID\n\n{group_id}\n\nRailwayの環境変数 ADMIN_GROUP_ID にこの値を設定すると、このグループが管理グループになります。"
    elif cmd in ['/週報', '/weekly']:
        reply = build_weekly_report(group_id) or "📊 今週はまだデータがありません。"
    elif cmd in ['/ヘルプ', '/help']:
        reply = (
            "【コマンド一覧】\n"
            "「/未返信」→ 返信待ちメッセージ\n"
            "「/タスク」→ 未完了タスク\n"
            "「/決定事項」→ 最近の決定事項\n"
            "「/未決」→ 検討中案件\n"
            "「/知識」→ このグループ専用の知識\n"
            "「/共通知識」→ 全グループ共通の知識\n"
            "「/週報」→ 今週のサマリー\n\n"
            "【@オルオル で何でもOK】\n"
            "質問・調べもの・過去の話…気軽に🦉\n\n"
            "【知識の記憶】\n"
            "「覚えておいて：○○」→ このグループ専用🔒\n"
            "「共通で覚えておいて：○○」→ 全グループ共通🌐"
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
        return "✅ 未完了タスクはありません！"
    lines = [f"📋 未完了タスク（{len(rows)}件）\n"]
    for assignee, task, created_at in rows:
        a = f"{assignee}さん" if assignee else "担当未定"
        lines.append(f"・{a}: {task}\n  （{created_at[:10]}）")
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
        return "📝 直近30日の決定事項はまだ記録されていません。"
    lines = ["📝 最近の決定事項\n"]
    for decision, decided_by, created_at in rows:
        lines.append(f"・{decision}\n  （{decided_by}、{created_at[:10]}）")
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
        return "✅ 未決の案件はありません！"
    lines = [f"🤔 検討中・未決案件（{len(rows)}件）\n"]
    for summary, raised_by, created_at in rows:
        lines.append(f"・{summary}\n  （{raised_by}、{created_at[:10]}）")
    return "\n".join(lines)


def get_knowledge_list(group_id):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    # このグループ専用のみ表示
    c.execute('''
        SELECT content, stored_by, created_at FROM knowledge
        WHERE group_id=? AND scope='group' ORDER BY created_at DESC LIMIT 20
    ''', (group_id,))
    rows = c.fetchall()
    conn.close()
    if not rows:
        return "🦉 このグループ専用の知識はまだありません。\n「覚えておいて：〇〇」で教えてください！\n\n共通知識は「/共通知識」で確認できます。"
    lines = [f"🔒 このグループの知識（{len(rows)}件）\n"]
    for content, stored_by, created_at in rows:
        lines.append(f"・{content}\n  （{stored_by}、{created_at[:10]}）")
    return "\n".join(lines)


def get_shared_knowledge_list():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    # 全グループ共通知識を表示
    c.execute('''
        SELECT content, stored_by, created_at FROM knowledge
        WHERE scope='shared' ORDER BY created_at DESC LIMIT 30
    ''')
    rows = c.fetchall()
    conn.close()
    if not rows:
        return "🌐 共通知識はまだありません。\n「共通で覚えておいて：〇〇」で登録できます！"
    lines = [f"🌐 全グループ共通の知識（{len(rows)}件）\n"]
    for content, stored_by, created_at in rows:
        lines.append(f"・{content}\n  （{stored_by}、{created_at[:10]}）")
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


# ==================== 週報 ====================

def build_weekly_report(group_id):
    today = datetime.now()
    week_ago = (today - timedelta(days=7)).strftime('%Y-%m-%d %H:%M:%S')
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

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

    if not decision_rows and not todo_rows:
        return None

    lines = [f"🦉 週報（{today.strftime('%m/%d')}）\n━━━━━━━━━━━━"]

    if decision_rows:
        lines.append("【今週の決定事項】")
        for decision, decided_by in decision_rows:
            lines.append(f"  ・{decision}")

    if todo_rows:
        lines.append("\n【未完了タスク】")
        for assignee, task in todo_rows:
            a = f"{assignee}さん" if assignee else "担当未定"
            lines.append(f"  ・{a}: {task}")

    if pending_rows:
        lines.append("\n【検討中の案件】")
        for (summary,) in pending_rows:
            lines.append(f"  ・{summary}")

    lines.append("━━━━━━━━━━━━")
    return "\n".join(lines)


# ==================== スルー検知・未決フォローアップ ====================

def notify_unanswered(group_id):
    msg = get_unanswered_list(group_id)
    if "返信待ち" in msg and "✅" not in msg:
        line_bot_api.push_message(group_id, TextSendMessage(text=msg))


def followup_pending_issues(group_id):
    """PENDING_FOLLOWUP_DAYS日以上経過した未決案件をフォローアップ"""
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
        lines = ["🦉 フォローアップです。以下の案件、その後いかがでしょう？\n"]
        ids = []
        for issue_id, summary, raised_by, created_at in rows:
            lines.append(f"・{summary}（{raised_by}、{created_at[:10]}）")
            ids.append(issue_id)
        line_bot_api.push_message(group_id, TextSendMessage(text="\n".join(lines)))
        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        for issue_id in ids:
            c.execute('UPDATE pending_issues SET last_followup_at=? WHERE id=?', (now, issue_id))
        conn.commit()

    conn.close()


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


# ==================== Claude解析（Haiku） ====================

def analyze_message_full(text, user_name):
    """メッセージを総合解析（ToDo・決定事項・未決案件）"""
    if not ANTHROPIC_API_KEY:
        return _simple_analyze(text)
    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
    today_str = datetime.now().strftime('%Y-%m-%d')
    prompt = f"""LINEグループのメッセージを分析し、以下のJSONのみを返してください（説明文不要）。

送信者: {user_name}
メッセージ: {text}
今日: {today_str}

{{
  "needs_reply": true/false,
  "has_todo": true/false,
  "todo_text": "タスクの内容" または null,
  "todo_assignee": "担当者名" または null,
  "has_decision": true/false,
  "decision_text": "決定事項の内容" または null,
  "is_pending_issue": true/false,
  "pending_summary": "検討中案件の要約" または null
}}

判断基準:
- has_todo: 「〜しておいて」「〜お願い」「〜やっておく」など具体的なタスクがある
- has_decision: 「〜することになった」「〜に決まった」「〜でいこう」など決定を示す
- is_pending_issue: 「〜どうする？」「〜検討しよう」「〜考えないと」など未解決の課題がある"""
    try:
        response = client.messages.create(
            model=MODEL_FAST, max_tokens=300,
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
    needs_reply = any(kw in text for kw in ['？', '?', 'どうします', 'お願い', '確認', '教えて'])
    return {
        "needs_reply": needs_reply,
        "has_todo": False, "todo_text": None, "todo_assignee": None,
        "has_decision": False, "decision_text": None,
        "is_pending_issue": False, "pending_summary": None
    }


# ==================== Googleスプレッドシート ====================

def get_sheets_service():
    if not GOOGLE_SERVICE_ACCOUNT_JSON:
        return None
    try:
        json_str = GOOGLE_SERVICE_ACCOUNT_JSON
        try:
            info = json.loads(json_str)
        except json.JSONDecodeError:
            # Railwayが改行文字をそのまま埋め込む場合に対応
            info = json.loads(json_str.replace('\n', '\\n'))
        creds = service_account.Credentials.from_service_account_info(
            info,
            scopes=['https://www.googleapis.com/auth/spreadsheets']
        )
        return build('sheets', 'v4', credentials=creds)
    except Exception as e:
        print(f"Sheets service error: {e}")
        return None


def append_to_sheet(timestamp, group_name, user_name, message):
    if not GOOGLE_SPREADSHEET_ID:
        return
    service = get_sheets_service()
    if not service:
        return
    try:
        service.spreadsheets().values().append(
            spreadsheetId=GOOGLE_SPREADSHEET_ID,
            range='シート1!A:D',
            valueInputOption='USER_ENTERED',
            insertDataOption='INSERT_ROWS',
            body={'values': [[timestamp, group_name, user_name, message]]}
        ).execute()
    except Exception as e:
        print(f"Sheets append error: {e}")


def init_sheet_header():
    """スプレッドシートが空なら1行目にヘッダーを追加する"""
    if not GOOGLE_SPREADSHEET_ID:
        return
    service = get_sheets_service()
    if not service:
        return
    try:
        result = service.spreadsheets().values().get(
            spreadsheetId=GOOGLE_SPREADSHEET_ID,
            range='シート1!A1'
        ).execute()
        if not result.get('values'):
            service.spreadsheets().values().update(
                spreadsheetId=GOOGLE_SPREADSHEET_ID,
                range='シート1!A1',
                valueInputOption='USER_ENTERED',
                body={'values': [['日時', 'グループ名', '送信者', 'メッセージ']]}
            ).execute()
    except Exception as e:
        print(f"Sheets header error: {e}")


init_sheet_header()


# ==================== DB保存 ====================

def save_message(
    timestamp, group_id, user_id, user_name, message, message_id, analysis
):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    try:
        c.execute('''
            INSERT OR IGNORE INTO messages
            (timestamp, group_id, user_id, user_name, message, message_id,
             needs_reply, raw_analysis)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            timestamp, group_id, user_id, user_name, message, message_id,
            1 if analysis.get('needs_reply') else 0,
            json.dumps(analysis, ensure_ascii=False)
        ))
        conn.commit()
    except Exception as e:
        print(f"DB save error: {e}")
    finally:
        conn.close()


# ==================== ヘルスチェック ====================

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


# ==================== 過去ログ移行（一時エンドポイント） ====================

@app.route("/migrate_to_sheets", methods=['GET'])
def migrate_to_sheets():
    if not GOOGLE_SPREADSHEET_ID:
        return {'error': 'GOOGLE_SPREADSHEET_ID not set'}, 400
    service = get_sheets_service()
    if not service:
        return {'error': 'Sheets service unavailable'}, 400

    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('SELECT timestamp, group_id, user_name, message FROM all_messages ORDER BY timestamp ASC')
    rows = c.fetchall()
    conn.close()

    if not rows:
        return {'message': 'No messages found', 'count': 0}

    try:
        result = service.spreadsheets().values().get(
            spreadsheetId=GOOGLE_SPREADSHEET_ID, range='シート1!A:A'
        ).execute()
        existing_rows = len(result.get('values', []))
    except Exception:
        existing_rows = 0

    if existing_rows > 1:
        return {'message': f'Already has {existing_rows - 1} data rows. Skipping.', 'existing': existing_rows - 1}

    values = [[ts, get_group_name(gid), uname or '', msg or ''] for ts, gid, uname, msg in rows]

    batch_size = 500
    for i in range(0, len(values), batch_size):
        service.spreadsheets().values().append(
            spreadsheetId=GOOGLE_SPREADSHEET_ID,
            range='シート1!A:D',
            valueInputOption='USER_ENTERED',
            insertDataOption='INSERT_ROWS',
            body={'values': values[i:i + batch_size]}
        ).execute()

    return {'message': 'Migration complete', 'written': len(values)}


if __name__ == "__main__":
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=False)
