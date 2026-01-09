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

    def format_row(self, c):
        """Maps API response to the CSV format with clean content."""
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

    def get_existing_data(self):
        captured_media = set()
        captured_comments = set()
        existing_rows = {}
        if DB_PATH.exists():
            print(f"Scanning CSV at {DB_PATH}...")
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
            print(f"✓ Scanned {len(captured_media)} media IDs and {len(captured_comments)} existing comments.")
        return captured_media, captured_comments, existing_rows

    def process_media_list(self, target_ids, label="Scrape"):
        if not target_ids:
            print("✓ All IDs are already in the database.")
            return
        print(f"Starting {label}: {len(target_ids)} media to check.")
        start_time = time.time()
        session_comments = 0
        
        mode = 'a' if DB_PATH.exists() else 'w'
        with open(DB_PATH, mode, newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=self.field_names, delimiter='\t', extrasaction='ignore')
            if mode == 'w': writer.writeheader()

            with ThreadPoolExecutor(max_workers=3) as executor:
                futures = {executor.submit(self.fetch_media_comments, m_id): m_id for m_id in target_ids}
                done = 0
                for future in as_completed(futures):
                    m_id = futures[future]
                    res = future.result()
                    done += 1
                    elapsed = time.time() - start_time
                    m, s = divmod(int(elapsed), 60)
                    
                    if res:
                        writer.writerows([self.format_row(c) for c in res])
                        session_comments += len(res)
                        print(f"[{done}/{len(target_ids)}] ✓ Media {m_id} | +{len(res)} (Session Total: {session_comments}) | {m}m {s}s")
                    else:
                        writer.writerow({'media_id': m_id, 'content': 'EMPTY_MARKER'})
                        print(f"[{done}/{len(target_ids)}] ◌ Media {m_id} empty | Session: {session_comments} | {m}m {s}s")
                    f.flush()
        print(f"\n✓ Completed. Total new comments: {session_comments}")

    def run_comment_id_gap_fill(self):
        _, existing_comments, _ = self.get_existing_data()
        if not existing_comments:
            print("❌ No existing comments found in database.")
            return
        
        last_id = max(existing_comments)
        print(f"Detected highest comment ID in CSV: {last_id}")
        
        all_ids_in_range = set(range(1, last_id + 1))
        missing_ids = sorted(list(all_ids_in_range - existing_comments))
        
        print(f"Missing IDs to check: {len(missing_ids)}")
        if not missing_ids:
            print("✓ Database sequence is complete.")
            return

        print(f"Starting individual fetch for {len(missing_ids)} IDs...")
        start_time = time.time()
        found = 0
        with open(DB_PATH, 'a', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=self.field_names, delimiter='\t', extrasaction='ignore')
            with ThreadPoolExecutor(max_workers=5) as executor:
                futures = {executor.submit(self.fetch_single_comment, cid): cid for cid in missing_ids}
                done = 0
                for future in as_completed(futures):
                    res = future.result()
                    done += 1
                    if res:
                        writer.writerow(self.format_row(res))
                        found += 1
                        f.flush()
                    if done % 20 == 0 or done == len(missing_ids):
                        elapsed = time.time() - start_time
                        m, s = divmod(int(elapsed), 60)
                        print(f"Checked: {done}/{len(missing_ids)} | Found: {found} | {m}m {s}s")
        print(f"\n✓ Gap fill complete. Added {found} comments.")

    def run_daily_sync(self):
        print("Starting daily sync using only Dantotsu API...")
        captured_media, _, existing_rows = self.get_existing_data()
        new_found = 0
        updated_found = 0
        
        # Get all media IDs from the database
        media_ids = list(captured_media)
        print(f"Checking {len(media_ids)} media IDs for updates...")

        # Load all rows in memory
        all_rows = {}
        if DB_PATH.exists():
            with open(DB_PATH, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f, delimiter='\t')
                for row in reader:
                    all_rows[int(row['comment_id'])] = row

        # Update or append
        with open(DB_PATH, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=self.field_names, delimiter='\t', extrasaction='ignore')
            writer.writeheader()

            # Process in batches to avoid overwhelming the API
            batch_size = 50
            for i in range(0, len(media_ids), batch_size):
                batch = media_ids[i:i+batch_size]
                print(f"Processing batch {i//batch_size + 1}/{(len(media_ids)-1)//batch_size + 1}...")
                
                with ThreadPoolExecutor(max_workers=3) as executor:
                    futures = {executor.submit(self.fetch_media_comments, m_id): m_id for m_id in batch}
                    for future in as_completed(futures):
                        m_id = futures[future]
                        comments = future.result()
                        
                        if not comments:
                            continue
                            
                        for c in comments:
                            cid = int(c['comment_id'])
                            row = self.format_row(c)
                            
                            if cid in all_rows:
                                # Compare fields to detect changes
                                old_row = all_rows[cid]
                                has_changes = False
                                
                                # Check if content changed (edited comment)
                                if old_row.get('content', '') != row.get('content', ''):
                                    # Store the previous content in the changes field
                                    row['changes'] = old_row.get('content', '')
                                    has_changes = True
                                
                                # Check other fields for changes
                                for key in self.field_names:
                                    if key in ['comment_id', 'content', 'changes']:
                                        continue  # Already handled or skip
                                    old = str(old_row.get(key, ''))
                                    new = str(row.get(key, ''))
                                    if old != new:
                                        has_changes = True
                                        break
                                
                                if has_changes:
                                    all_rows[cid] = row
                                    updated_found += 1
                                else:
                                    # No changes, keep the original row
                                    all_rows[cid] = old_row
                            else:
                                # New comment
                                row['changes'] = "NEW"
                                all_rows[cid] = row
                                new_found += 1

            # Write all rows back
            for r in all_rows.values():
                writer.writerow(r)

        print(f"✓ Daily Sync Complete. Added {new_found} new comments, Updated {updated_found} comments.")

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
    elif SYNC_MODE == "gaps":
        manager.run_comment_id_gap_fill()
    elif SYNC_MODE == "full":
        if not MEDIA_JSON_PATH.exists():
            print(f"❌ Error: Media JSON not found at {MEDIA_JSON_PATH}")
            return 1
        with open(MEDIA_JSON_PATH, 'r') as f:
            all_json_ids = [int(x) for x in json.load(f)]
        captured_media, _ , _ = manager.get_existing_data()
        targets = [x for x in all_json_ids if x not in captured_media]
        manager.process_media_list(targets, "Full Media Scrape")
    else:
        print(f"❌ Unknown mode: {SYNC_MODE}")
        return 1
    
    print("=== Sync Complete ===")
    return 0

if __name__ == "__main__":
    exit(main())
