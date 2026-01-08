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

# Use relative path for GitHub Actions
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
        print("❌ Error: No valid AniList token found. Set ANILIST_TOKEN environment variable.")
        return False
    
    def verify_token(self):
        query = "query { Viewer { id name } }"
        try:
            r = requests.post("https://graphql.anilist.co", json={"query": query},
                              headers={"Authorization": f"Bearer {self.access_token}"}, timeout=10)
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

    def get_dantotsu_auth(self):
        print("Authenticating with Dantotsu Comment API...")
        headers = {"appauth": APP_AUTH_KEY}
        try:
            r = requests.post(f"{API_ADDRESS}/authenticate", headers=headers, 
                             data={"token": self.auth.access_token}, timeout=15)
            if r.status_code == 200:
                self.dantotsu_token = r.json().get("authToken")
                print(f"✓ Connected as Mod: {r.json()['user']['username']}")
                return True
            else:
                print(f"❌ Auth failed: {r.status_code}")
        except Exception as e:
            print(f"Connection Error: {e}")
        return False

    def format_row(self, c, existing_row=None):
        """Maps API response to the CSV format with clean content and tracks changes."""
        row = {
            'comment_id': c.get('comment_id'),
            'user_id': c.get('user_id'),
            'media_id': c.get('media_id'),
            'parent_comment_id': c.get('parent_comment_id', 'NULL'),
            'content': str(c.get('content', '')).replace('\t', ' ').replace('\n', ' '), 
            'timestamp': c.get('timestamp'),
            'deleted': c.get('deleted', 0),
            'tag': c.get('tag', 'NULL'),
            'upvotes': int(c.get('upvotes', 0)),
            'downvotes': int(c.get('downvotes', 0)),
            'user_vote_type': c.get('user_vote_type', 0),
            'username': c.get('user', {}).get('username', 'NULL'),
            'profile_picture_url': c.get('user', {}).get('profile_picture_url', 'NULL'),
            'is_mod': c.get('user', {}).get('is_mod', 0),
            'is_admin': c.get('user', {}).get('is_admin', 0),
            'reply_count': c.get('reply_count', 0),
            'total_votes': int(c.get('upvotes', 0)) - int(c.get('downvotes', 0)),
            'changes': ''
        }
        if existing_row:
            changed_fields = [k for k in row if k != 'changes' and str(row[k]) != str(existing_row.get(k))]
            if changed_fields:
                row['changes'] = ','.join(changed_fields)
        return row

    def fetch_media_comments(self, m_id):
        media_comments = []
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
                comments = data.get("comments", [])
                if not comments: break
                media_comments.extend(comments)
                page += 1
                time.sleep(0.1)
            except Exception as e:
                print(f"Error fetching media {m_id}: {e}")
                break
        return media_comments

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

    def load_existing_data(self):
        """Load existing CSV into memory for updates."""
        existing_data = {}
        if DB_PATH.exists():
            with open(DB_PATH, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f, delimiter='\t')
                for row in reader:
                    existing_data[int(row['comment_id'])] = row
            print(f"✓ Loaded {len(existing_data)} existing comments from CSV")
        return existing_data

    def save_data(self, data_dict):
        """Overwrite CSV with updated data."""
        with open(DB_PATH, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=self.field_names, delimiter='\t')
            writer.writeheader()
            writer.writerows(data_dict.values())

    def run_daily_sync(self):
        print("Scanning Discord for 24h activity...")
        headers = {"Authorization": DISCORD_TOKEN}
        active_ids = set()
        last_id = None
        cutoff = datetime.now(timezone.utc) - timedelta(hours=24)

        while True:
            url = f"https://discord.com/api/v9/channels/{FEED_CHANNEL_ID}/messages?limit=100"
            if last_id: url += f"&before={last_id}"
            try:
                r = requests.get(url, headers=headers, timeout=10)
                if r.status_code != 200: 
                    print(f"Discord API error: {r.status_code}")
                    break
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

        if not active_ids:
            print("No active media found in last 24h.")
            return

        existing_data = self.load_existing_data()
        new_comments = 0
        updated_comments = 0
        print(f"Syncing {len(active_ids)} active media IDs...")

        with ThreadPoolExecutor(max_workers=3) as executor:
            futures = {executor.submit(self.fetch_media_comments, m_id): m_id for m_id in active_ids}
            for future in as_completed(futures):
                m_id = futures[future]
                comments = future.result()
                for c in comments:
                    cid = int(c['comment_id'])
                    if cid in existing_data:
                        updated_row = self.format_row(c, existing_data[cid])
                        if updated_row['changes']:
                            existing_data[cid] = updated_row
                            updated_comments += 1
                    else:
                        existing_data[cid] = self.format_row(c)
                        new_comments += 1
                time.sleep(0.2)

        self.save_data(existing_data)
        print(f"✓ Daily Sync Complete. New: {new_comments}, Updated: {updated_comments}")

def main():
    print(f"=== Dantotsu Sync Starting (Mode: {SYNC_MODE}) ===")
    
    al_auth = AniListAuthenticator("14959")
    if not al_auth.authenticate():
        return 1
    
    manager = DantotsuManager(al_auth)
    if not manager.get_dantotsu_auth():
        return 1
    
    if SYNC_MODE == "daily":
        manager.run_daily_sync()
    else:
        print(f"❌ Unknown mode: {SYNC_MODE}")
        return 1
    
    print("=== Sync Complete ===")
    return 0

if __name__ == "__main__":
    exit(main())
