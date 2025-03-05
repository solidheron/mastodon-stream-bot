import requests
import csv
import time
import os
from datetime import datetime, timedelta
from dateutil import parser
from datetime import timezone

# List of Owncast instances
OWNCAST_INSTANCES = [
    "https://kuso.business/",
    "https://live.kitsunech.jp.net/",
    "https://stream.ozoned.net/",
    "https://live.expiredpopsicle.com/",
    "https://stream.repeatro.de/",
    "https://ety.cybre.stream/",
    "https://live.hatnix.net/",
    "https://dingusmacdongle.live/",
    "https://stream.hylas.au/",
    "https://stream.firesidefedi.live/",
    "https://jjv.sh/",
    "https://live.hegeezias.art/",
    "https://cast.samsai.eu/",
    "https://cast.31337.one/",
    "https://stream.pavot.ca/",
    "https://live.famkos.net/",
    "https://cast.grueproof.net/",
    "https://live.whitevhs.xyz/",
    "https://video.mezzo.moe/"
]

OWNCAST_CSV_FILE = "owncast_data.csv"
STREAMTIME_CSV_FILE = "owncast_streamtime.csv"
POLL_INTERVAL = 3600  # Fetch data every 1 hour

# Dictionary to track stream states {url: last recorded disconnect time}
stream_states = {}

def convert_to_utc_format(dt_string):
    # Parse the datetime string
    dt = parser.parse(dt_string)
    
    # If the datetime is naive (no timezone info), assume it's in UTC
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    
    # Convert to UTC
    dt_utc = dt.astimezone(timezone.utc)
    
    # Format to the desired string format
    return dt_utc.strftime("%Y-%m-%dT%H:%M:%SZ")


def fetch_owncast_data(url):
    """Fetch data from the Owncast API."""
    try:
        url = url.rstrip("/") + "/"  # Ensure single trailing slash
        response = requests.get(f"{url}api/status", timeout=15)
        response.raise_for_status()
        return response.json() if response.headers.get("Content-Type") == "application/json" else None
    except requests.RequestException:
        return None

def write_owncast_to_csv(data, url):
    """Write Owncast instance data to a CSV file."""
    if not data:
        return

    fieldnames = ["timestamp", "owncast_url", "last_connect_time", "last_disconnect_time", "viewer_count"]
    file_exists = os.path.exists(OWNCAST_CSV_FILE)

    with open(OWNCAST_CSV_FILE, "a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if not file_exists:
            writer.writeheader()

        writer.writerow({
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "owncast_url": url,
            "last_connect_time": data.get("lastConnectTime", ""),
            "last_disconnect_time": data.get("lastDisconnectTime", ""),
            "viewer_count": data.get("viewerCount", 0)
        })

def get_last_valid_connect_time_and_viewer_count(url):
    with open(OWNCAST_CSV_FILE, 'r') as csvfile:
        reader = csv.DictReader(csvfile)
        last_valid_connect_time = None
        viewer_count = None
        for row in reversed(list(reader)):
            if row['owncast_url'] == url and row['last_connect_time']:
                last_valid_connect_time = row['last_connect_time']
                viewer_count = row['viewer_count']
                break
    return last_valid_connect_time, viewer_count

def write_streamtime_to_csv(url, disconnect_time, connect_time, viewer_count):
    fieldnames = ["owncast_url", "last_connect_time", "last_disconnect_time", "viewer_count"]
    file_exists = os.path.exists(STREAMTIME_CSV_FILE)

    existing_entries = set()
    if file_exists:
        with open(STREAMTIME_CSV_FILE, "r", newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                existing_entries.add((row["owncast_url"], row["last_disconnect_time"], row["last_connect_time"], row["viewer_count"]))

    if (url, disconnect_time, connect_time, viewer_count) not in existing_entries:
        with open(STREAMTIME_CSV_FILE, "a", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            if not file_exists:
                writer.writeheader()
            writer.writerow({
                "owncast_url": url,
                "last_connect_time": connect_time,
                "last_disconnect_time": disconnect_time,
                "viewer_count": viewer_count
            })

def track_stream_sessions(url, last_disconnect_time, last_connect_time):
    global stream_states

    if url not in stream_states:
        stream_states[url] = None

    prev_disconnect = stream_states[url]

    if last_disconnect_time and last_disconnect_time != prev_disconnect:
        valid_connect_time, viewer_count = get_last_valid_connect_time_and_viewer_count(url)
        valid_connect_time = valid_connect_time or last_connect_time
        if valid_connect_time:
            try:
                connect_time_utc = convert_to_utc_format(valid_connect_time)
                disconnect_time_utc = convert_to_utc_format(last_disconnect_time)
                connect_obj = datetime.strptime(connect_time_utc, "%Y-%m-%dT%H:%M:%SZ")
                disconnect_obj = datetime.strptime(disconnect_time_utc, "%Y-%m-%dT%H:%M:%SZ")
                if connect_obj < disconnect_obj:
                    stream_states[url] = disconnect_time_utc
                    write_streamtime_to_csv(url, disconnect_time_utc, connect_time_utc, viewer_count)
            except Exception as e:
                print(f"Error processing times for {url}: {e}")

if __name__ == "__main__":
    #while True:
    for url in OWNCAST_INSTANCES:
        data = fetch_owncast_data(url)
        if data:
            write_owncast_to_csv(data, url)
            track_stream_sessions(url, data.get("lastDisconnectTime"), data.get("lastConnectTime"))

    next_run = datetime.now() + timedelta(seconds=POLL_INTERVAL)
    print(f"Next fetch at {next_run.strftime('%Y-%m-%d %H:%M:%S')}")
        #time.sleep(POLL_INTERVAL)
