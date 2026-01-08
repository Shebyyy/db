import requests
import json
import csv
import time
import os
import re
from pathlib import Path
from datetime import datetime, timedelta, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed

# --- CONFIGURATION FROM ENVIRONMENT ---
APP_AUTH_KEY = os.getenv("APP_AUTH_KEY", "")
API_ADDRESS = "https://api.dantotsu.app"
DISCORD_TOKEN = os.getenv("DISCORD_TOKEN", "")
FEED_CHANNEL_ID = "1180378569109671987"
ANILIST_TOKEN = os.getenv("ANILIST_TOKEN", "")
SYNC_MODE = os.getenv("SYNC_MODE", "daily")

# User-focused and time-frame
SYNC_USER_IDS = os.getenv("SYNC_USER_IDS", "")  # comma-separated
SYNC_START = os.getenv("SYNC_START")
SYNC_END = os.getenv("SYNC_END")

DB_PATH = Path("dantotsu_global_db.csv")
MEDIA_JSON_PATH = Path("dantotsu_unique_media.json")


class AniListAuthenticator:
    def __init__(self, client_id):
        self.client_id = client_id
        self.access_token = ANILIST_TOKEN
        self.token_file = Path("anilist_token.json")
    
    def load_token(self):
        if self.access_token:
            print("✓ Loaded AniList token from environment")
            return True
        if self.token_file.exists():
            with open(self.token_file, 'r') as f:
                data = json.load(f)
                self.access_token = data.get('access_token')
                print("✓ Loaded saved AniList token")
                return True
        return False
    
    def authenticate(self):
        if self.load_token() and self.verify_token():
            return True
        print("❌ Error: No valid AniList token found.")
        return False
    
    def verify_token(self):
        query = "query { Viewer { id name } }"
        try:
            r = requests.post(
                "https://graphql.anilist.co", 
                json={"query": query},
                headers={"Authorization": f"Bearer {self.access_token}"}, timeout=10
            )
            return r.status_code == 200
        except:
            return False


class DantotsuManager:
    def __init__(self, al_authenticator):
        self.auth = al_authenticator
        self.dantotsu_token = None
        self.field_names = [
            'comment_id', 'user_id', 'media_id', 'parent_comment_id', 'content', 
            'timestamp', 'deleted', 'tag', 'upvotes', 'downvotes', 
            'user_vote_type', 'username', 'profile_picture_url', 
            'is_mod', 'is_admin', 'reply_count', 'total_votes', 'changes'
        ]

    # --- Auth ---
    def get_dantotsu_auth(self):
        print("Authenticating with Dantotsu Comment API...")
        headers = {"appauth": APP_AUTH_KEY}
        try:
            r = requests.post(f"{API_ADDRESS}/authenticate", headers=headers, data={"token": self.auth.access_token}, timeout=15)
            if r.status_code == 200:
                self.dantotsu_token = r.json().get("authToken")
                print(f"✓ Connected as Mod: {r.json()['user']['username']}")
                return True
            else:
                print(f"❌ Auth failed: {r.status_code}")
        except Exception as e:
            print(f"Connection Error: {e}")
        return False

    # --- CSV Formatting ---
    def format_row(self, c):
        return {
            'comment_id': c.get('comment_id'),
            'user_id': c.get('user_id'),
            'media_id': c.get('media_id'),
            'parent_comment_id': c.get('parent_comment_id', 'NULL'),
            'content': str(c.get('content', '')).replace('\t', ' ').replace('\n', ' '),
            'timestamp': c.get('timestamp'),
            'deleted': c.get('deleted'),
            'tag': c.get('tag'),
            'upvotes': int(c.get('upvotes', 0)),
            'downvotes': int(c.get('downvotes', 0)),
            'user_vote_type': c.get('user_vote_type'),
            'username': c.get('username', 'NULL'),
            'profile_picture_url': c.get('profile_picture_url', 'NULL'),
            'is_mod': c.get('is_mod'),
            'is_admin': c.get('is_admin'),
            'reply_count': int(c.get('reply_count', 0)),
            'total_votes': int(c.get('total_votes', 0)),
            'changes': ''
        }

    # --- Fetch Comments ---
    def fetch_media_comments(self, m_id):
        comments = []
        page = 1
        headers = {"appauth": APP_AUTH_KEY, "Authorization": self.dantotsu_token}
        while True:
            url = f"{API_ADDRESS}/comments/{m_id}/{page}?sort=newest"
            try:
                r = requests.get(url, headers=headers, timeout=10)
                if r.status_code == 404: break
                if r.status_code == 429:
                    time.sleep(30)
                    continue
                if r.status_code != 200: break
                data = r.json()
                page_comments = data.get("comments", [])
                if not page_comments: break
                comments.extend(page_comments)
                page += 1
                time.sleep(0.1)
            except Exception as e:
                print(f"Error fetching media {m_id}: {e}")
                break
        return comments

    def fetch_single_comment(self, comment_id):
        headers = {"appauth": APP_AUTH_KEY, "Authorization": self.dantotsu_token}
        url = f"{API_ADDRESS}/comments/{comment_id}"
        try:
            r = requests.get(url, headers=headers, timeout=10)
            if r.status_code == 429:
                time.sleep(30)
                return self.fetch_single_comment(comment_id)
            if r.status_code == 200:
                return r.json()
        except: pass
        return None

    def fetch_user_comments(self, user_id):
        comments = []
        page = 1
        headers = {"appauth": APP_AUTH_KEY, "Authorization": self.dantotsu_token}
        while True:
            url = f"{API_ADDRESS}/comments/user/{user_id}/{page}?sort=newest"
            try:
                r = requests.get(url, headers=headers, timeout=10)
                if r.status_code == 404: break
                if r.status_code == 429:
                    time.sleep(30)
                    continue
                if r.status_code != 200: break
                data = r.json()
                page_comments = data.get("comments", [])
                if not page_comments: break
                comments.extend(page_comments)
                page += 1
                time.sleep(0.1)
            except Exception as e:
                print(f"Error fetching comments for user {user_id}: {e}")
                break
        return comments

    # --- Existing CSV Data ---
    def get_existing_data(self):
        captured_media = set()
        captured_comments = set()
        existing_rows = {}
        if DB_PATH.exists():
            with open(DB_PATH, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f, delimiter='\t')
                for row in reader:
                    m_id = row.get('media_id')
                    c_id = row.get('comment_id')
                    if m_id and m_id.isdigit() and row.get('content') != 'EMPTY_MARKER':
                        captured_media.add(int(m_id))
                    if c_id and c_id.isdigit():
                        captured_comments.add(int(c_id))
                        existing_rows[int(c_id)] = row
        return captured_media, captured_comments, existing_rows

    # --- Daily Sync ---
    def run_daily_sync(self):
        headers = {"Authorization": DISCORD_TOKEN}
        active_ids = set()
        last_id = None
        cutoff = datetime.now(timezone.utc) - timedelta(hours=24)
        
        while True:
            url = f"https://discord.com/api/v9/channels/{FEED_CHANNEL_ID}/messages?limit=100"
            if last_id: url += f"&before={last_id}"
            try:
                r = requests.get(url, headers=headers, timeout=10)
                if r.status_code != 200: break
                msgs = r.json()
                if not msgs: break
                for m in msgs:
                    ts = datetime.fromisoformat(m['timestamp'].replace('Z', '+00:00'))
                    if ts < cutoff: break
                    match = re.search(r'\* Media: (\d+)', m.get('content', ''))
                    if match: active_ids.add(int(match.group(1)))
                else:
                    last_id = msgs[-1]['id']
                    continue
                break
            except Exception as e:
                print(f"Error fetching Discord messages: {e}")
                break
        
        if not active_ids: return
        captured_media, existing_comments, existing_rows = self.get_existing_data()
        all_rows = {}
        if DB_PATH.exists():
            with open(DB_PATH, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f, delimiter='\t')
                for row in reader:
                    all_rows[int(row['comment_id'])] = row

        new_found = 0
        updated_found = 0
        with open(DB_PATH, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=self.field_names, delimiter='\t', extrasaction='ignore')
            writer.writeheader()
            for m_id in active_ids:
                comments = self.fetch_media_comments(m_id)
                for c in comments:
                    cid = int(c['comment_id'])
                    row = self.format_row(c)
                    if cid in all_rows:
                        changes = [k for k in self.field_names[:-1] if str(all_rows[cid].get(k, '')) != str(row.get(k, ''))]
                        if changes:
                            row['changes'] = ",".join(changes)
                            all_rows[cid] = row
                            updated_found += 1
                        else:
                            row['changes'] = ''
                            all_rows[cid] = all_rows[cid]
                    else:
                        row['changes'] = "NEW"
                        all_rows[cid] = row
                        new_found += 1
            for r in all_rows.values():
                writer.writerow(r)

    # --- Gap Fill ---
    def run_comment_id_gap_fill(self):
        _, existing_comments, _ = self.get_existing_data()
        if not existing_comments: return
        last_id = max(existing_comments)
        missing_ids = sorted(set(range(1, last_id+1)) - existing_comments)
        if not missing_ids: return
        found = 0
        with open(DB_PATH, 'a', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=self.field_names, delimiter='\t', extrasaction='ignore')
            with ThreadPoolExecutor(max_workers=5) as executor:
                futures = {executor.submit(self.fetch_single_comment, cid): cid for cid in missing_ids}
                for future in as_completed(futures):
                    res = future.result()
                    if res:
                        writer.writerow(self.format_row(res))
                        found += 1

    # --- Full Media List ---
    def process_media_list(self, target_ids, label="Scrape"):
        if not target_ids: return
        session_comments = 0
        mode = 'a' if DB_PATH.exists() else 'w'
        with open(DB_PATH, mode, newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=self.field_names, delimiter='\t', extrasaction='ignore')
            if mode == 'w': writer.writeheader()
            with ThreadPoolExecutor(max_workers=3) as executor:
                futures = {executor.submit(self.fetch_media_comments, m_id): m_id for m_id in target_ids}
                for future in as_completed(futures):
                    m_id = futures[future]
                    res = future.result()
                    if res:
                        writer.writerows([self.format_row(c) for c in res])
                        session_comments += len(res)
                    else:
                        writer.writerow({'media_id': m_id, 'content': 'EMPTY_MARKER'})

    # --- User-Focused ---
    def run_user_focused_sync(self):
        if not SYNC_USER_IDS: return
        user_ids = [int(x.strip()) for x in SYNC_USER_IDS.split(",")]
        for uid in user_ids:
            comments = self.fetch_user_comments(uid)
            if not comments: continue
            path = Path(f"user_{uid}.csv")
            with open(path, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=self.field_names, delimiter='\t', extrasaction='ignore')
                writer.writeheader()
                for c in comments: writer.writerow(self.format_row(c))

    # --- Time-Frame ---
    def run_time_frame_sync(self):
        if not SYNC_START or not SYNC_END: return
        start = datetime.fromisoformat(SYNC_START)
        end = datetime.fromisoformat(SYNC_END)
        captured_media, _, _ = self.get_existing_data()
        media_ids = list(captured_media)
        with open(DB_PATH, 'a', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=self.field_names, delimiter='\t', extrasaction='ignore')
            if DB_PATH.stat().st_size == 0: writer.writeheader()
            for m_id in media_ids:
                comments = self.fetch_media_comments(m_id)
                for c in comments:
                    ts = datetime.fromisoformat(c['timestamp'])
                    if start <= ts <= end:
                        writer.writerow(self.format_row(c))


def main():
    print(f"=== Dantotsu Sync Starting (Mode: {SYNC_MODE}) ===")
    al_auth = AniListAuthenticator("14959")
    if not al_auth.authenticate(): return 1
    manager = DantotsuManager(al_auth)
    if not manager.get_dantotsu_auth(): return 1

    if SYNC_MODE == "daily":
        manager.run_daily_sync()
    elif SYNC_MODE == "gaps":
        manager.run_comment_id_gap_fill()
    elif SYNC_MODE == "full":
        if not MEDIA_JSON_PATH.exists(): return 1
        with open(MEDIA_JSON_PATH, 'r') as f:
            all_json_ids = [int(x) for x in json.load(f)]
        captured_media, _, _ = manager.get_existing_data()
        targets = [x for x in all_json_ids if x not in captured_media]
        manager.process_media_list(targets, "Full Media Scrape")
    elif SYNC_MODE == "user-focused":
        manager.run_user_focused_sync()
    elif SYNC_MODE == "time-frame":
        manager.run_time_frame_sync()
    else:
        print(f"❌ Unknown mode: {SYNC_MODE}")
        return 1

    print("=== Sync Complete ===")
    return 0


if __name__ == "__main__":
    exit(main())
