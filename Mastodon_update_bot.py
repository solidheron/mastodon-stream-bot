import os
import time
import requests
import csv
from mastodon import Mastodon
from dotenv import load_dotenv
import ollama
import random

# Load environment variables
load_dotenv()
MASTODON_ACCESS_TOKEN = "mastodon_token_here"
os.environ["OLLAMA_HOME"] = r"D:\ollama_ai"

# List of PeerTube instances to check
PEERTUBE_INSTANCES = [
    "https://dalek.zone",
    "https://peertube.wtf"
]

# List of Owncast instances to check
OWNCAST_INSTANCES = [
    "https://kuso.business",
    "https://live.kitsunech.jp.net",
    "https://stream.ozoned.net/",
    "https://live.expiredpopsicle.com",
    "https://stream.repeatro.de/",
    "https://ety.cybre.stream",
    "https://live.hatnix.net/",
    "https://cast.31337.one"
]

# Initialize Mastodon API
## change instance if your use another instance
mastodon = Mastodon(access_token=MASTODON_ACCESS_TOKEN, api_base_url="https://mastodon.social")

# CSV file for tracking posted videos and streams
CSV_FILE = "live_streams_posted.csv"

# List of tracked PeerTube accounts
USER_URLS = [
    "https://vid.northbound.online/accounts/lyn1337",
    "https://tube.vencabot.com/accounts/vencabot",
    "https://video.gamerstavern.online/accounts/nico198x",
    "https://video.firesidefedi.live/accounts/firesidefedi",
    "https://video.mycrowd.ca/accounts/ellyse",
    "https://peertube.wtf/accounts/northwestwind",
    "https://dingusmacdongle.live/"
]


# Load posted streams
def load_posted_streams():
    """Read the CSV file and return a dictionary of posted video URLs with timestamps."""
    posted_streams = {}
    if not os.path.exists(CSV_FILE):
        print("No CSV file found, starting fresh.")
        return posted_streams
    
    with open(CSV_FILE, mode="r", newline="", encoding="utf-8") as file:
        reader = csv.reader(file)
        for row in reader:
            if len(row) < 2:
                continue  # Skip rows with less than 2 values
            video_url = row[0]
            updated_at = row[1] if len(row) > 1 else ''
            posted_streams[video_url] = updated_at
    
    print(f"Loaded {len(posted_streams)} previously posted streams.")
    return posted_streams

# Save new posted streams
def save_posted_stream(video_url, updated_at):
    """Append a new stream URL and timestamp to the CSV file."""
    with open(CSV_FILE, mode="a", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)
        writer.writerow([video_url, updated_at])
    print(f"Saved new stream: {video_url}")

# Generate description using Ollama
def generate_mastodon_description(video_title):
    """Use Ollama model to generate a Mastodon post description."""
    try:
        print(f"Generating description for: {video_title}")
        prompt = f"Write a short, engaging Mastodon post (under 250 characters) for a live stream titled '{video_title}'."
        
        response = ollama.chat(model="qwen2.5-coder:7b", messages=[{"role": "user", "content": prompt}])
        
        # Extracting the message content properly
        if 'message' in response and 'content' in response['message']:
            description = response['message']['content'].strip()
        elif 'content' in response:
            description = response['content'].strip()
        else:
            print(f"Unexpected response format: {response}")
            return None
        
        print(f"Generated description: {description}")
        return description
    except Exception as e:
        print(f"Error generating description: {e}")
        return None


# Random emoji generator
def generate_random_emoji():
    """Return a random emoji from a predefined list."""
    emojis = ["🎥", "🔴", "✨", "🔥", "🎮", "🎤", "📺", "💥", "🕹️", "🎉", "🚀", "👾", "🥳", "💡", "🎶"]
    return random.choice(emojis)

import random

# Define different post templates
POST_TEMPLATES = [
    "{emoji} LIVE NOW on PeerTube! {emoji}\nWatch here: {url}\n{description} #peertube #livestream #stream #twitch",
    "{emoji} Join the chaos! We're live now on PeerTube: {url} {emoji}\n{description}\n#peertube #livestream #stream #twitch",
    "🚨 {emoji} Attention! We're live now! {emoji}\nCatch the stream here: {url}\n{description}\n#peertube #livestream #twitch",
    "🎥 {emoji} We're LIVE! Watch us play: {url}\n{description}\n#peertube #livestream #stream #twitch",
    "{emoji} Watch us live NOW on PeerTube: {url} {emoji}\n{description}\n#peertube #livestream #stream #twitch",
]

def generate_post_template(video_url, description):
    """Generate a random post template with the provided video URL and description."""
    template = random.choice(POST_TEMPLATES)
    emoji = generate_random_emoji()  # Get a random emoji
    return template.format(emoji=emoji, url=video_url, description=description)


def get_live_streams_from_peertube():
    """Fetch live PeerTube videos from multiple instances and post to Mastodon if not already posted."""
    posted_streams = load_posted_streams()
    generated_descriptions = {}

    for instance in PEERTUBE_INSTANCES:
        api_url = f"{instance}/api/v1/videos"
        print(f"Checking {instance} for live streams...")

        try:
            params = {'live': 'true', 'per_page': 10}
            response = requests.get(api_url, params=params, timeout=10)
            response.raise_for_status()
            videos = response.json().get('data') or response.json().get('videos')

            if not videos:
                print(f"No live videos found on {instance}.")
                continue

            for video in videos:
                video_url = video.get('url', '')
                video_title = video.get('name', '')
                is_live = video.get('isLive', False)
                updated_at = video.get('createdAt', '')

                if not video_url:
                    print("Skipping video with no URL.")
                    continue

                if video_url in posted_streams:
                    print(f"Already posted: {video_url}. Skipping...")
                    continue

                account_url = video.get('account', {}).get('url', '')
                if is_live and any(user_url in account_url for user_url in USER_URLS):
                    print(f"Found live stream: {video_title} ({video_url})")

                    # Check if description has already been generated for this stream
                    if video_url not in generated_descriptions:
                        mastodon_description = generate_mastodon_description(video_title)
                        if mastodon_description:
                            generated_descriptions[video_url] = mastodon_description
                        else:
                            print(f"Skipping posting due to missing description for {video_url}")
                            continue

                    description = generated_descriptions.get(video_url)

                    # Generate post message using a random template
                    message = generate_post_template(video_url, description)

                    try:
                        mastodon.status_post(message, visibility="public")
                        print(f"Toot sent: {message}")
                        save_posted_stream(video_url, updated_at)
                        time.sleep(random.uniform(2, 5))
                    except Exception as e:
                        print(f"Error posting to Mastodon: {e}")

        except requests.RequestException as e:
            print(f"Error fetching PeerTube streams from {instance}: {e}")



# Define different post templates for Owncast
OWNCAST_POST_TEMPLATES = [
    "{emoji} LIVE NOW on Owncast! {emoji}\nWatch here: {url}\n{description} #owncast #livestream #stream #selfhosted",
    "{emoji} Join the stream! We're live on Owncast: {url} {emoji}\n{description}\n#owncast #livestream #stream #selfhosted",
    "🚨 {emoji} We're live on Owncast! {emoji}\nCatch the stream here: {url}\n{description}\n#owncast #livestream #twitch",
    "🎥 {emoji} We're streaming LIVE! Watch now on Owncast: {url}\n{description}\n#owncast #livestream #stream #selfhosted",
    "{emoji} Watch us live NOW on Owncast: {url} {emoji}\n{description}\n#owncast #livestream #stream #twitch",
]

def generate_owncast_post_template(video_url, description):
    """Generate a random post template for Owncast with the provided video URL and description."""
    template = random.choice(OWNCAST_POST_TEMPLATES)
    emoji = generate_random_emoji()  # Get a random emoji
    return template.format(emoji=emoji, url=video_url, description=description)


def get_live_streams_from_owncast():
    """Fetch live streams from multiple Owncast instances and post to Mastodon if not already posted."""
    posted_streams = load_posted_streams()
    generated_descriptions = {}

    for instance in OWNCAST_INSTANCES:
        api_url = f"{instance}/api/status"
        print(f"Checking {instance} for live status...")

        try:
            response = requests.get(api_url, timeout=10)
            response.raise_for_status()
            data = response.json()
            
            if not data.get("online", False):
                print(f"No live stream on {instance}.")
                continue

            video_url = instance
            video_title = data.get("streamTitle", "Live Stream")
            updated_at = data.get("lastConnectTime", "")

            if video_url in posted_streams:
                print(f"Already posted: {video_url}. Skipping...")
                continue

            print(f"Found live Owncast stream: {video_title} ({video_url})")

            # Check if description has already been generated for this stream
            if video_url not in generated_descriptions:
                mastodon_description = generate_mastodon_description(video_title)
                if mastodon_description:
                    generated_descriptions[video_url] = mastodon_description
                else:
                    print(f"Skipping posting due to missing description for {video_url}")
                    continue

            description = generated_descriptions.get(video_url)

            # Generate post message using a random template for Owncast
            message = generate_owncast_post_template(video_url, description)

            try:
                mastodon.status_post(message, visibility="public")
                print(f"Toot sent: {message}")
                save_posted_stream(video_url, updated_at)
                time.sleep(random.uniform(2, 5))
            except Exception as e:
                print(f"Error posting to Mastodon: {e}")

        except requests.RequestException as e:
            print(f"Error fetching Owncast stream from {instance}: {e}")


# Main loop
if __name__ == "__main__":
    while True:
        print("Starting new cycle...")
        get_live_streams_from_peertube()
        get_live_streams_from_owncast()
        print("Sleeping for 10 minutes...\n")
        time.sleep(600)  # Wait 10 minutes before checking again
