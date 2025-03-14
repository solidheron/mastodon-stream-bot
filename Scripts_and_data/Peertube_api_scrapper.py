import requests
import csv
import time
from urllib.parse import urlparse
from datetime import datetime, timezone
import os

CHANNEL_URLS = [
    "https://tube.vencabot.com/a/vencabot/video-channels",
    "https://vid.northbound.online/a/lyn1337/video-channels",
    "https://video.mycrowd.ca/a/ellyse/video-channels",
    "https://video.firesidefedi.live/a/firesidefedi/video-channels",
    "https://peertube.wtf/a/nwwind/video-channels",
    "https://peertube.zalasur.media/a/zalasur/video-channels",
    "https://video.hardlimit.com/a/minimar/video-channels",
    "https://videos.phegan.live/a/phegan/video-channels",
    "https://spectra.video/a/the_pirater/video-channels",
    "https://freediverse.com/a/commie/video-channels"
]
PEERTUBE_CSV_FILE = "DATA/peertube_data.csv"
OUTPUT_CSV_FILE = "DATA/peertube_data2.csv"

def read_csv_rows():
    """Read all rows from peertube_data.csv, skipping the header."""
    rows = []
    try:
        with open(PEERTUBE_CSV_FILE, "r") as f:
            reader = csv.reader(f)
            next(reader, None)  # Skip header
            for row in reader:
                if len(row) < 5:
                    continue
                rows.append({
                    "csv_retrieval_time": row[0],
                    "csv_account": row[1],
                    "video_url": row[2],
                    "csv_published_at": row[3],
                    "csv_views": row[4]
                })
    except FileNotFoundError:
        print(f"File {PEERTUBE_CSV_FILE} not found.")
    return rows

def fetch_video_metadata(video_url, retrieval_time_str, max_retries=3, delay=5):
    """Fetch video metadata only if retrieval time is less than 1 day old."""
    retrieval_time = parse_iso_datetime(retrieval_time_str)
    if not retrieval_time:
        print(f"Invalid retrieval time: {retrieval_time_str}")
        return None

    # Check if entry is older than 1 day
    current_time = datetime.now(timezone.utc)
    time_diff = current_time - retrieval_time
    if time_diff.total_seconds() > 7*86400:
        #print(f"Skipping outdated entry ({time_diff} old): {video_url}")
        return None

    retries = 0
    while retries < max_retries:
        try:
            if "/videos/watch/" in video_url:
                base_url, video_id = video_url.rsplit("/videos/watch/", 1)
                api_url = f"{base_url}/api/v1/videos/{video_id}"
                response = requests.get(api_url, timeout=10)
                response.raise_for_status()
                return response.json()
            else:
                print(f"Invalid video URL: {video_url}")
                return None
        except requests.RequestException as e:
            print(f"Error fetching {video_url}: {e}")
            if "timed out" in str(e):
                retries += 1
                print(f"Retrying in {delay}s ({retries}/{max_retries})")
                time.sleep(delay)
            else:
                return None
    print(f"Failed after {max_retries} retries: {video_url}")
    return None

def parse_iso_datetime(time_str):
    """Convert ISO string to timezone-aware datetime object."""
    try:
        dt = datetime.fromisoformat(time_str.replace('Z', '+00:00'))
        return dt.replace(tzinfo=timezone.utc)
    except Exception as e:
        print(f"Datetime parse error: {time_str} - {e}")
        return None

def write_entries_to_csv(entries):
    """Write processed entries to peertube_data2.csv."""
    fieldnames = ["account_url", "published_at", "retrieval_time", "views", "video_url"]
    with open(OUTPUT_CSV_FILE, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for entry in entries.values():
            writer.writerow(entry)

def extract_username_and_instance(channel_url):
    """Extract username and instance from channel URL."""
    parsed_url = urlparse(channel_url)
    instance_url = f"{parsed_url.scheme}://{parsed_url.netloc}"
    username = parsed_url.path.split('/')[2]
    return instance_url, username

def save_to_csv(data):
    """Append live stream data to CSV."""
    with open(PEERTUBE_CSV_FILE, 'a', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow(data)

def search_live_streams(channel_url):
    """Check for live streams and save to CSV."""
    instance_url, username = extract_username_and_instance(channel_url)
    url = f"{instance_url}/api/v1/accounts/{username}/videos"
    params = {"count": 50, "start": 0, "sort": "-publishedAt"}
    live_found = False
    retrieval_time = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')

    try:
        while url:
            response = requests.get(url, params=params)
            if response.status_code != 200:
                print(f"HTTP Error {response.status_code}: {response.text}")
                return

            try:
                data = response.json()
            except ValueError as e:
                print(f"JSON Error: {e} - {response.text}")
                return

            videos = data.get("data", [])
            next_page = data.get("paging", {}).get("next", None)
            
            for video in videos:
                if video.get("isLive", False):
                    live_found = True
                    video_uuid = video.get("uuid", "N/A")
                    published_at = video.get("publishedAt", "N/A")
                    views = video.get("views", 0)
                    live_data = [
                        retrieval_time,
                        username,
                        f"{instance_url}/videos/watch/{video_uuid}",
                        published_at,
                        views
                    ]
                    save_to_csv(live_data)
                    print(f"🔴 Live: {video.get('name', 'Unnamed')}")
                    print(f"URL: {instance_url}/videos/watch/{video_uuid}")
                    print("-" * 80)
            
            url = next_page if next_page else None
            params["start"] = next_page.split("start=")[-1] if next_page else 0

        if not live_found:
            print("⚫ No active livestreams")
        
    except requests.RequestException as e:
        print(f"Request failed: {e}")

def main():
    """Process CSV data and update entries."""
    rows = read_csv_rows()
    entries = {}
    api_cache = {}
    channel_url_map = {}

    for url in CHANNEL_URLS:
        _, username = extract_username_and_instance(url)
        channel_url_map[username] = url
    
    for row in rows:
        video_url = row["video_url"]
        csv_account = row["csv_account"]
        retrieval_time_str = row["csv_retrieval_time"]

        if video_url in api_cache:
            metadata = api_cache[video_url]
        else:
            metadata = fetch_video_metadata(video_url, retrieval_time_str)
            if metadata:
                api_cache[video_url] = metadata
            else:
                continue

        account_url = channel_url_map.get(csv_account, "N/A")
        data_to_write = {
            "account_url": account_url,
            "published_at": row["csv_published_at"],
            "retrieval_time": retrieval_time_str,
            "views": metadata.get("views", 0),
            "video_url": video_url
        }
        
        key = (account_url, row["csv_published_at"])
        existing_entry = entries.get(key)
        
        if existing_entry:
            current_rt = parse_iso_datetime(retrieval_time_str)
            existing_rt = parse_iso_datetime(existing_entry["retrieval_time"])
            if current_rt and existing_rt and current_rt > existing_rt:
                entries[key] = data_to_write
                print(f"Updated {account_url}")
        else:
            entries[key] = data_to_write
            print(f"Added {account_url}")
    
    write_entries_to_csv(entries)

if __name__ == "__main__":
    # Ensure DATA directory exists
    if not os.path.exists("DATA"):
        os.makedirs("DATA")

    # Initialize CSV with header
    try:
        with open(PEERTUBE_CSV_FILE, 'r') as f:
            pass
    except FileNotFoundError:
        with open(PEERTUBE_CSV_FILE, 'w', newline='') as f:
            csv.writer(f).writerow([
                "Time of Retrieval (UTC)", 
                "Username", 
                "Live Stream Link", 
                "PublishedAt", 
                "Views"
            ])

    # Main execution loop
    #while True:
    for channel_url in CHANNEL_URLS:
        print(f"\nChecking {channel_url}")
        search_live_streams(channel_url)
        
    print("\nProcessing data...")
    main()
    print("\nWaiting 1 hour for next check...")
        #time.sleep(3600)
