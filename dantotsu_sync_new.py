import requests
import json
import csv
import webbrowser
import time
import os
import re
import random
import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed

# --- CONFIGURATION ---
APP_AUTH_KEY = os.getenv("APP_AUTH_KEY", "")
API_ADDRESS = "https://api.dantotsu.app"
DISCORD_TOKEN = os.getenv("DISCORD_TOKEN", "")
ANILIST_TOKEN = os.getenv("ANILIST_TOKEN", "")

FEED_CHANNEL_ID = "1180378569109671987"
MOD_BOT_ID = "1212248844398493717"
DELETE_CMD_ID = "1212448947499700316"

ANILIST_CLIENT_ID = "33909"
ANILIST_CLIENT_SECRET = "FmA0Bi0ahaxY0x2IW0CVtXTer5wREKBp8fDA0ZIz"

DB_PATH = Path("dantotsu_global_db.csv")

class AniListAuthenticator:
    def __init__(self, client_id):
        self.client_id = client_id
        self.access_token = ANILIST_TOKEN
        self.token_file = Path("anilist_token.json")
    
    def load_token(self):
        """Load token from env or file"""
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
        """Non-interactive authentication for CI/CD"""
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
        except: return False

class DantotsuManager:
    def __init__(self, al_authenticator):
        self.auth = al_authenticator
        self.d_token = None
        self.field_names = [
            'comment_id', 'user_id', 'media_id', 'parent_comment_id', 'content', 
            'timestamp', 'deleted', 'tag', 'upvotes', 'downvotes', 
            'user_vote_type', 'username', 'profile_picture_url', 
            'is_mod', 'is_admin', 'reply_count', 'total_votes'
        ]

    def get_dantotsu_auth(self):
        print("Authenticating with Dantotsu (Unlocking NSFW/Mod data)...")
        headers = {"appauth": APP_AUTH_KEY}
        try:
            r = requests.post(f"{API_ADDRESS}/authenticate", headers=headers, data={"token": self.auth.access_token})
            if r.status_code == 200:
                self.d_token = r.json().get("authToken")
                print(f"✓ Connected as: {r.json()['user']['username']}")
                return True
        except Exception as e:
            print(f"Connection Error: {e}")
        return False

    def fetch_user_data(self, user_id):
        """Fetch detailed user data if available"""
        headers = {"appauth": APP_AUTH_KEY, "Authorization": self.d_token}
        try:
            r = requests.get(f"{API_ADDRESS}/users/{user_id}", headers=headers, timeout=10)
            if r.status_code == 200:
                return r.json()
            elif r.status_code == 429:
                time.sleep(30)
                return self.fetch_user_data(user_id)
        except:
            pass
        return None

    def format_row(self, c):
        u = c.get('user') if c.get('user') is not None else {}
        
        # Check if any important user data is missing
        important_fields = ['username', 'profile_picture_url', 'is_mod', 'is_admin']
        missing_data = any(not u.get(field) for field in important_fields)
        
        # Try to get user details if any important data is missing
        if missing_data and u.get('id'):
            user_data = self.fetch_user_data(u['id'])
            if user_data:
                u = user_data  # Override with full user data
        
        return {
            'comment_id': c.get('comment_id'),
            'user_id': c.get('user_id'),
            'media_id': c.get('media_id'),
            'parent_comment_id': c.get('parent_comment_id', 'NULL'),
            'content': str(c.get('content', '')).replace('\t', ' ').replace('\n', ' '),
            'timestamp': c.get('timestamp'),
            'deleted': 1 if c.get('deleted') is True else 0,
            'tag': c.get('tag', 'NULL'),
            'upvotes': int(c.get('upvotes', 0)),
            'downvotes': int(c.get('downvotes', 0)),
            'user_vote_type': c.get('user_vote_type', 0),
            'username': u.get('username', 'NULL'),
            'profile_picture_url': u.get('profile_picture_url', 'NULL'),
            'is_mod': 1 if u.get('is_mod') is True else 0,
            'is_admin': 1 if u.get('is_admin') is True else 0,
            'reply_count': int(c.get('reply_count', 0)),
            'total_votes': int(c.get('total_votes', 0))
        }

    def fetch_media_comments(self, m_id):
        """Sequential Deep Scraper: Fetches every page for one media ID before moving on."""
        all_comments = []
        page = 1
        headers = {"appauth": APP_AUTH_KEY, "Authorization": self.d_token}
        
        while True:
            url = f"{API_ADDRESS}/comments/{m_id}/{page}?sort=newest"
            try:
                r = requests.get(url, headers=headers, timeout=15)
                
                if r.status_code == 429:
                    print(f"\n⚠️ [Rate Limit] Waiting 30s for Media {m_id} (Page {page})...")
                    time.sleep(30)
                    continue # Retry the same page
                
                if r.status_code != 200:
                    break # Break on 404 or other errors
                    
                data = r.json().get("comments", [])
                if not data:
                    break # End of comment thread
                    
                all_comments.extend(data)
                page += 1
                time.sleep(0.2) # Very slight delay to keep steady pace
            except Exception as e:
                print(f"\nError fetching media {m_id}: {e}")
                break
        return all_comments

    def fetch_single_comment(self, cid):
        headers = {"appauth": APP_AUTH_KEY, "Authorization": self.d_token}
        try:
            r = requests.get(f"{API_ADDRESS}/comments/{cid}", headers=headers, timeout=10)
            if r.status_code == 429:
                time.sleep(30)
                return self.fetch_single_comment(cid)
            return r.json() if r.status_code == 200 else None
        except: return None

    def get_existing_data(self):
        captured_media, captured_comments = set(), set()
        if DB_PATH.exists():
            print(f"Scanning CSV at {DB_PATH}...")
            try:
                df = pd.read_csv(DB_PATH, sep='\t', usecols=['media_id', 'comment_id', 'content'])
                df = df[df['content'] != 'EMPTY_MARKER']
                captured_media = set(df['media_id'].dropna().astype(int))
                captured_comments = set(df['comment_id'].dropna().astype(int))
                print(f"✓ Scanned {len(captured_comments)} existing comments.")
            except Exception as e:
                print(f"Scan error: {e}")
        return captured_media, captured_comments

    def process_media_list(self, target_ids, label="Scrape"):
        if not target_ids:
            print("✓ Database is up to date.")
            return
            
        print(f"Starting {label}: {len(target_ids)} media (Sequential processing).")
        start_time = time.time()
        session_comments = 0
        mode = 'a' if DB_PATH.exists() else 'w'
        
        with open(DB_PATH, mode, newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=self.field_names, delimiter='\t', extrasaction='ignore')
            if mode == 'w': writer.writeheader()
            
            for idx, m_id in enumerate(target_ids, 1):
                # Fetch all pages for this specific media ID
                comments = self.fetch_media_comments(m_id)
                
                elapsed = time.time() - start_time
                mins, secs = divmod(int(elapsed), 60)
                
                if comments:
                    writer.writerows([self.format_row(c) for c in comments])
                    session_comments += len(comments)
                    print(f"[{idx}/{len(target_ids)}] ✓ Media {m_id: <6} | +{len(comments): <4} | Total Session: {session_comments: <6} | {mins}m {secs}s")
                else:
                    writer.writerow({'media_id': m_id, 'content': 'EMPTY_MARKER'})
                    print(f"[{idx}/{len(target_ids)}] ◌ Media {m_id: <6} empty | {mins}m {secs}s")
                
                f.flush() # Force save to disk after every media is finished

    def run_comment_id_gap_fill(self):
        _, existing = self.get_existing_data()
        auto_max = max(existing) if existing else 0
        target = input(f"Check up to ID (Default {auto_max}): ").strip()
        last_id = int(target) if target.isdigit() else auto_max
        missing = sorted(list(set(range(1, last_id + 1)) - existing))
        
        if not missing: return print("✓ No gaps found.")
        print(f"Fetching {len(missing)} IDs sequentially...")
        
        with open(DB_PATH, 'a', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=self.field_names, delimiter='\t', extrasaction='ignore')
            for idx, cid in enumerate(missing, 1):
                res = self.fetch_single_comment(cid)
                if res:
                    writer.writerow(self.format_row(res))
                    f.flush()
                if idx % 10 == 0:
                    print(f"Progress: {idx}/{len(missing)} IDs checked", end='\r')
        print("\n✓ Gap fill complete.")

    def run_smart_sync(self):
        print("🚀 Starting Smart Sync...")
        headers = {"Authorization": DISCORD_TOKEN}
        r = requests.get(f"https://discord.com/api/v9/channels/{FEED_CHANNEL_ID}/messages?limit=100", headers=headers)
        deleted_ids, active_media = [], set()
        
        if r.status_code == 200:
            for m in r.json():
                content = m.get('content', '')
                mid_match = re.search(r'\* Media: (\d+)', content)
                if mid_match: active_media.add(int(mid_match.group(1)))
                if MOD_BOT_ID in str(m.get('author', {}).get('id')) or DELETE_CMD_ID in content:
                    cid_match = re.search(r'comment_id[:\s]+(\d+)', content)
                    if cid_match: deleted_ids.append(int(cid_match.group(1)))

        if DB_PATH.exists():
            df = pd.read_csv(DB_PATH, sep='\t')
            if deleted_ids:
                df.loc[df['comment_id'].isin(deleted_ids), 'deleted'] = 1
                print(f"🔨 Flagged {len(deleted_ids)} Mod deletions.")
            
            # Use the sequential fetch for active media too
            new_rows = []
            for mid in active_media:
                print(f"Refreshing Media {mid}...")
                results = self.fetch_media_comments(mid)
                new_rows.extend([self.format_row(c) for c in results])
            
            if new_rows:
                df = pd.concat([df, pd.DataFrame(new_rows)]).drop_duplicates(subset=['comment_id'], keep='last')
            
            df.sort_values('comment_id').to_csv(DB_PATH, sep='\t', index=False)
            print("✨ Smart Sync Complete.")

    def cleanup_repair(self):
        if not DB_PATH.exists(): return
        print("🧼 Cleaning and Repairing Database Headers...")
        df = pd.read_csv(DB_PATH, sep='\t')
        df = df[df['content'] != 'EMPTY_MARKER']
        for col in self.field_names:
            if col not in df.columns: df[col] = 'NULL'
        df = df[self.field_names].drop_duplicates(subset=['comment_id']).sort_values('comment_id')
        df.to_csv(DB_PATH, sep='\t', index=False)
        print(f"✅ Cleanup Done. Total unique comments: {len(df)}")

if __name__ == "__main__":
    al = AniListAuthenticator(ANILIST_CLIENT_ID, ANILIST_CLIENT_SECRET)
    if al.authenticate():
        mgr = DantotsuManager(al)
        if mgr.get_dantotsu_auth():
            print("\n1. History Scrape (Full)\n2. Smart Daily Sync (Activity + Deletes)\n3. Re-scan Missed Media\n4. Fill Sequence Gaps\n5. Header Repair & Cleanup")
            choice = input("\nChoice: ")
            
            if choice in ["1", "3"]:
                json_path = r"C:\Downloads\dantotsu_unique_media_1767774645019.json"
                if os.path.exists(json_path):
                    with open(json_path, 'r') as f: all_ids = [int(x) for x in json.load(f)]
                    cap_media, _ = mgr.get_existing_data()
                    targets = [x for x in all_ids if x not in cap_media]
                    mgr.process_media_list(targets, "History")
            elif choice == "2": mgr.run_smart_sync()
            elif choice == "4": mgr.run_comment_id_gap_fill()
            elif choice == "5": mgr.cleanup_repair()
