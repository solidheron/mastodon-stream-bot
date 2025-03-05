import requests
import csv
import time
from urllib.parse import urlparse
from datetime import datetime
import os

CHANNEL_URLS = [
    "https://tube.vencabot.com/a/vencabot/video-channels",
    "https://vid.northbound.online/a/lyn1337/video-channels",
    "https://video.mycrowd.ca/a/ellyse/video-channels",
    "https://video.firesidefedi.live/a/firesidefedi/video-channels",
    "https://peertube.wtf/a/nwwind/video-channels",
    "https://peertube.zalasur.media/a/zalasur/video-channels"
    "https://video.hardlimit.com/a/minimar/video-channels",
    "https://davbot.media/a/mrfunkedude/video-channels",
    "https://freediverse.com/a/commie/video-channels"
]
PEERTUBE_CSV_FILE = "peertube_data.csv"
OUTPUT_CSV_FILE = "peertube_data2.csv"

def read_csv_rows():
    """
    Read all rows from peertube_data.csv, skipping the header.
    Assumes the CSV has a header row:
    [retrieval_time, csv_account, video_url, csv_published_at, csv_views]
    """
    rows = []
    try:
        with open(PEERTUBE_CSV_FILE, "r") as f:
            reader = csv.reader(f)
            next(reader, None)  # Skip the header row
            for row in reader:
                if len(row) < 5:
                    continue  # Skip malformed rows
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


def fetch_video_metadata(video_url, max_retries=3, delay=5):
    """Fetch video metadata from the PeerTube API with retries and a timeout."""
    retries = 0
    while retries < max_retries:
        try:
            if "/videos/watch/" in video_url:  # Check if the substring exists
                base_url, video_id = video_url.rsplit("/videos/watch/", 1)
                api_url = f"{base_url}/api/v1/videos/{video_id}"
                response = requests.get(api_url, timeout=10)
                response.raise_for_status()
                return response.json()
            else:
                print(f"Warning: Invalid video URL format: {video_url}")
                return None  # Or raise an exception, depending on your needs
        except requests.RequestException as e:
            print(f"Error fetching metadata for {video_url}: {e}")
            if "timed out" in str(e):
                retries += 1
                print(f"Retrying in {delay} seconds... (Retry {retries}/{max_retries})")
                time.sleep(delay)
            else:
                return None
    print(f"Failed to fetch metadata for {video_url} after {max_retries} retries.")
    return None

def parse_iso_datetime(time_str):
    """Convert an ISO-formatted string to a datetime object."""
    try:
        return datetime.fromisoformat(time_str.replace('Z', '+00:00'))
    except Exception as e:
        print(f"Error parsing datetime {time_str}: {e}")
        return None

def write_entries_to_csv(entries):
    """Write all processed entries to peertube_data2.csv with the given fieldnames."""
    fieldnames = ["account_url", "published_at", "retrieval_time", "views", "video_url"]

    with open(OUTPUT_CSV_FILE, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for entry in entries.values():
            writer.writerow(entry)
            
def extract_username_and_instance(channel_url):
    """Extract the username and instance URL from the CHANNEL_URL."""
    parsed_url = urlparse(channel_url)
    instance_url = f"{parsed_url.scheme}://{parsed_url.netloc}"
    username = parsed_url.path.split('/')[2]  # Extract username from the path
    return instance_url, username

def save_to_csv(data):
    """Save the live stream data to a CSV file."""
    with open('peertube_data.csv', mode='a', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow(data)

def search_live_streams(channel_url):
    """Fetch videos for the account and save live stream data to CSV."""
    instance_url, username = extract_username_and_instance(channel_url)
    url = f"{instance_url}/api/v1/accounts/{username}/videos"
    params = {"count": 50, "start": 0, "sort": "-publishedAt"}
    live_found = False

    # Time of retrieval in ISO 8601 format with Z (UTC)
    retrieval_time = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')

    try:
        while url:
            response = requests.get(url, params=params)
            if response.status_code != 200:
                print(f"Error: Received status code {response.status_code}")
                print(response.text)
                return

            try:
                data = response.json()
            except ValueError as e:
                print("Error parsing JSON:", e)
                print("Response content:", response.text)
                return

            videos = data.get("data", [])
            next_page = data.get("paging", {}).get("next", None)
            
            for video in videos:
                if video.get("isLive", False):
                    live_found = True
                    video_name = video.get("name", "Unnamed Video")
                    video_uuid = video.get("uuid", "N/A")
                    published_at = video.get("publishedAt", "N/A")
                    views = video.get("views", 0)
                    # Save relevant data to CSV: retrieval time (UTC ISO 8601), username, live stream link, publishedAt, views
                    live_data = [retrieval_time, username, f"{instance_url}/videos/watch/{video_uuid}", published_at, views]
                    save_to_csv(live_data)
                    print(f"🔴 Live stream: {video_name} (UUID: {video_uuid}, PublishedAt: {published_at})")
                    print(f"Watch URL: {instance_url}/videos/watch/{video_uuid} - PublishedAt: {published_at} - Views: {views}")
                    print("-" * 80)
            
            if not next_page:
                break
            params["start"] = next_page.split("start=")[-1]

        if not live_found:
            print("⚫ No active livestreams for this account.")
        
    except requests.exceptions.RequestException as e:
        print("Error fetching livestream data:", e)
def main():
    """
    Process each row from peertube_data.csv.
    Uses both the CSV’s published_at value and API metadata (for account_url and views)
    to build an output entry keyed by (account_url, published_at).  If an entry with the
    same key already exists, update it with the row having the later retrieval_time.
    """
    rows = read_csv_rows()
    entries = {}  # Key: (account_url, published_at), Value: data dict to write
    api_cache = {}  # Cache API responses to avoid redundant calls
    
    # Create a dictionary to store the CHANNEL_URL associated with each username
    channel_url_map = {}
    for url in CHANNEL_URLS:
        _, username = extract_username_and_instance(url)
        channel_url_map[username] = url
    
    for row in rows:
        video_url = row["video_url"]
        csv_account = row["csv_account"]  # Get the username from the CSV
        
        # Use caching so that we don't call the API twice for same video_url.
        if video_url in api_cache:
            metadata = api_cache[video_url]
        else:
            metadata = fetch_video_metadata(video_url)
            if metadata:
                api_cache[video_url] = metadata
            else:
                print(f"Skipping {video_url} due to missing metadata.")
                continue  # Skip if we couldn't fetch metadata
        
        # Use the CHANNEL_URL from the map, based on the CSV's account name
        account_url = channel_url_map.get(csv_account, "N/A")  # Get the original channel URL
        
        # Use the published_at value from the CSV row (to honor differences)
        published_at = row["csv_published_at"]
        retrieval_time = row["csv_retrieval_time"]
        views = metadata.get("views", 0)
        
        data_to_write = {
            "account_url": account_url,
            "published_at": published_at,
            "retrieval_time": retrieval_time,
            "views": views,
            "video_url": video_url
        }
        
        key = (account_url, published_at)
        
        # If an entry with this key already exists, update if the new retrieval_time is later
        if key in entries:
            existing_rt = parse_iso_datetime(entries[key]["retrieval_time"])
            current_rt = parse_iso_datetime(retrieval_time)
            if current_rt and existing_rt and current_rt > existing_rt:
                entries[key] = data_to_write
                print(f"Updated entry for {account_url} published at {published_at}")
        else:
            entries[key] = data_to_write
            print(f"Added new entry for {account_url} published at {published_at}")
    
    write_entries_to_csv(entries)

if __name__ == "__main__":
    # Add header to CSV if file is empty
    try:
        with open('peertube_data.csv', mode='r', newline='', encoding='utf-8') as file:
            pass
    except FileNotFoundError:
        with open('peertube_data.csv', mode='w', newline='', encoding='utf-8') as file:
            writer = csv.writer(file)
            writer.writerow(["Time of Retrieval (UTC)", "Username", "Live Stream Link", "PublishedAt", "Views"])

    for channel_url in CHANNEL_URLS:
        print(f"\nChecking live streams for {channel_url}...")
        search_live_streams(channel_url)
    
    # Wait for 1 hour before checking again
    print("\nWaiting for the next check...\n")
    main()
