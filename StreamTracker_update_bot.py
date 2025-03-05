import os
import time
import requests
import csv
from mastodon import Mastodon
from dotenv import load_dotenv
import ollama
import random
import platform
from streamer_mastodon_account import STREAM_TO_MASTODON_ACCOUNT,access_token,mastodon_instance  # Import the dictionary

# Load environment variables
load_dotenv()

# Initialize Mastodon API
mastodon = Mastodon(access_token=access_token, api_base_url=mastodon_instance)

# CSV file for tracking posted videos and streams
CSV_FILE = "live_streams_posted.csv"

# List of tracked PeerTube accounts.  This should now be the full account URL.
USER_URLS = [
    "https://vid.northbound.online/a/lyn1337/video-channels",
    "https://video.mycrowd.ca/a/ellyse/video-channels",
    "https://video.firesidefedi.live/a/firesidefedi/video-channels",
    "https://peertube.wtf/a/nwwind/video-channels",
    "https://peertube.zalasur.media/a/zalasur/video-channels"
    "https://video.hardlimit.com/a/minimar/video-channels",
    "https://freediverse.com/a/commie/video-channels"
]

# List of Owncast instances to check
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
    "https://live.whitevhs.xyz/"
    #"https://video.mezzo.moe/"
]


# Default account string if no match is found
DEFAULT_ACCOUNT = "Streamer <3"

# Load posted streams
def load_posted_streams():
    """Read the CSV file and return a set of all (URL, timestamp) tuples."""
    posted_streams = set()
    if not os.path.exists(CSV_FILE):
        print("No CSV file found, starting fresh.")
        return posted_streams

    try:
        with open(CSV_FILE, mode="r", newline="", encoding="utf-8") as file:
            reader = csv.reader(file)
            for row in reader:
                if len(row) == 2:
                    url, timestamp = row
                    posted_streams.add((url, timestamp))
                elif len(row) == 1:
                    posted_streams.add(row[0]) #handles old CSV files without timestamps
                else:
                    print(f"Skipping malformed row: {row}")

        print(f"Loaded {len(posted_streams)} previously posted streams.")
    except Exception as e:
        print(f"Error loading posted streams from CSV: {e}")
    return posted_streams

# Save new posted streams
def save_posted_stream(url, timestamp):
    """Append a new stream's URL and timestamp to the CSV file."""
    try:
        with open(CSV_FILE, mode="a", newline="", encoding="utf-8") as file:
            writer = csv.writer(file)
            writer.writerow([url, timestamp])
        print(f"Saved new stream: {url}, {timestamp}")
    except Exception as e:
        pass  #print(f"Error saving stream to CSV: {e}")


# Generate description using Ollama
def generate_mastodon_hashtags(video_title):
    """Use Ollama model to generate relevant hashtags for a live stream."""
    try:
        #print(f"Generating hashtags for: {video_title}")
        prompt = f"Generate a short list of relevant hashtags (under 5) for a live stream titled '{video_title}'. The hashtags should be space-separated and relevant to the stream topic."

        response = ollama.chat(model="qwen2.5-coder:7b", messages=[{"role": "user", "content": prompt}])

        # Extracting the hashtags properly
        if 'message' in response and 'content' in response['message']:
            hashtags = response['message']['content'].strip()
        elif 'content' in response:
            hashtags = response['content'].strip()
        else:
            #print(f"Unexpected response format: {response}")
            return None

        #print(f"Generated hashtags: {hashtags}")
        return hashtags
    except Exception as e:
        #print(f"Error generating hashtags: {e}")
        return None


# Random emoji generator
def generate_random_emoji():
    """Return a random emoji from a predefined list."""
    emojis = ["🎥", "🔴", "✨", "🔥", "🎮", "🎤", "📺", "💥", "🕹️", "🎉", "🚀", "👾", "🥳", "💡", "🎶"]
    return random.choice(emojis)

# Define different post templates for PeerTube

POST_TEMPLATES = [
    "{emoji} {account} on PeerTube! {emoji}\nWatch here: {url}\n\n{description} #peertube #livestream #stream #twitch",
    "{emoji} Join {account} in the chaos! live now on PeerTube: {url} {emoji}\n\n{description} #peertube #livestream #stream #twitch",
    "🚨 {emoji} Attention! {account} live now! {emoji}\nCatch the stream here: {url}\n\n{description} #peertube #livestream #twitch",
    "🎥 {emoji} {account} is LIVE! Watch us play: {url}\n\n{description} #peertube #livestream #stream #twitch",
    "{emoji} Watch {account} live NOW on PeerTube: {url} {emoji}\n\n{description} #peertube #livestream #stream #twitch",
    "{emoji} {account} on PeerTube! {emoji}\nWatch here: {url}\n\n{description} #peertube #livestream #stream #twitch",
    "🔴 LIVE: {account} is streaming on PeerTube! {emoji}\nTune in: {url}\n\n{description} #peertube #livestream #stream",
    "{emoji} Don't miss out! {account} is live on PeerTube: {url}\n\n{description} #peertube #livestream #stream",
    "🎮 Game on! {account} is streaming live: {url} {emoji}\n\n{description} #peertube #livestream #gaming",
    "{emoji} Breaking: {account} just went live! Watch now: {url}\n\n{description} #peertube #livestream #stream",
    "📺 {account}'s stream is up! Join the fun here: {url} {emoji}\n\n{description} #peertube #livestream #stream",
    "{emoji} Live content alert! {account} is on air: {url}\n\n{description} #peertube #livestream #stream"
]


def generate_post_template(video_url, description, account):
    """Generate a random post template with the provided video URL, description, and account."""
    template = random.choice(POST_TEMPLATES)
    emoji = generate_random_emoji()  # Get a random emoji
    return template.format(emoji=emoji, url=video_url, description=description, account=account)

# Owncast functions
OWNCAST_POST_TEMPLATES = [
    "{emoji} LIVE NOW {account} ! {emoji}\nWatch here: {url}\n\n{description} #owncast #livestream #stream #selfhosted",
    "{emoji} Join {account} the stream! We're live on Owncast: {url} {emoji}\n\n{description} #owncast #livestream #stream #selfhosted",
    "🚨 {emoji} {account} is live on Owncast! {emoji}\nCatch the stream here: {url}\n\n{description} #owncast #livestream #twitch",
    "🎥 {emoji} Streaming LIVE! Watch {account} on Owncast: {url}\n\n{description} #owncast #livestream #stream #selfhosted",
    "{emoji} Watch {account} live NOW on Owncast: {url} {emoji}\n\n{description} #owncast #livestream #stream #twitch",
    "🔴 {emoji} {account} is broadcasting LIVE on Owncast! {emoji}\nTune in now: {url}\n\n{description} #owncast #livestream #selfhosted",
    "{emoji} Live stream alert! {account} is on air via Owncast: {url} {emoji}\n\n{description} #owncast #livestream #stream",
    "📺 {emoji} Don't miss {account}'s live stream on Owncast! {emoji}\nWatch here: {url}\n\n{description} #owncast #livestream #selfhosted",
    "{emoji} {account} is going live on our self-hosted Owncast server! {emoji}\nJoin here: {url}\n\n{description} #owncast #livestream #selfhosted",
    "🎬 {emoji} Live content alert! {account} is streaming on Owncast: {url}\n\n{description} #owncast #livestream #stream #selfhosted"
]


def generate_owncast_post_template(video_url, description, account):
    """Generate a random post template for Owncast with the provided video URL and description."""
    template = random.choice(OWNCAST_POST_TEMPLATES)
    emoji = generate_random_emoji()  # Get a random emoji
    return template.format(emoji=emoji, url=video_url, description=description, account=account)

def get_live_streams_from_peertube():
    """Fetch live PeerTube videos and post to Mastodon if not already posted."""
    posted_streams = load_posted_streams()
    generated_descriptions = {}

    for account_url in USER_URLS:  # Iterate through the list of account URLs
        # Extract instance, account name, and channel name from the URL
        try:
            instance = account_url.split('/')[2]  # e.g., "dalek.zone"
            account_path = account_url.split('/a/')[-1]
            account_name = account_path.split('/')[0]  # e.g., "solidheron"
            channel_name = account_path.split('/')[1]  # e.g., "video-channels"

        except IndexError as e:
            #print(f"Error parsing account URL {account_url}: {e}")
            continue  # Skip to the next URL if there's an error

        # Use /api/v1/accounts/{account_name}/videos
        api_url = f"https://{instance}/api/v1/accounts/{account_name}/videos"  # Use https for the instance
        #print(f"Checking {instance} for live streams from account: {account_name}...")

        try:
            response = requests.get(api_url, timeout=10)
            response.raise_for_status()
            videos = response.json().get('data') or response.json().get('videos')  # Handle different API responses

            if not videos:
                #print(f"No videos found on {instance} from {account_name}.")
                continue

            for video in videos:
                video_id = video.get('id')
                video_url = f"https://{instance}/videos/watch/{video_id}"  # Use https
                video_title = video.get('name', '')
                published_at = video.get('publishedAt', '')  # Get the publishedAt value

                if not video_url:
                    #print("Skipping video with no URL.")
                    continue

                # Check if this combination of account_url and published_at has already been posted
                if (account_url, published_at) in posted_streams:
                    #print(f"Already posted: {account_url}, {published_at}. Skipping...")
                    continue

                # Check if the video is live by accessing the individual video endpoint
                video_api_url = f"https://{instance}/api/v1/videos/{video_id}"  # Use https
                try:
                    video_response = requests.get(video_api_url, timeout=10)
                    video_response.raise_for_status()
                    video_data = video_response.json()
                    is_live = video_data.get('isLive', False)  # Check the 'isLive' attribute
                except requests.RequestException as e:
                    #print(f"Error checking video status for {video_id}: {e}")
                    continue  # Skip to the next video

                if is_live:  # Only post if the video is currently live
                    print(f"Found live stream: {video_title} ({video_url})")

                    # Generate description if not already generated
                    if video_url not in generated_descriptions:
                        mastodon_description = generate_mastodon_hashtags(video_title)
                        if mastodon_description:
                            generated_descriptions[video_url] = mastodon_description
                        else:
                            #print(f"Skipping posting due to missing description for {video_url}")
                            continue

                    description = generated_descriptions.get(video_url)

                    # Extract the base URL from the account_url
                    base_url = f"https://{instance}/a/{account_name}/video-channels"

                    # Get the Mastodon account from the mapping, default to DEFAULT_ACCOUNT if not found
                    print(base_url)
                    account = STREAM_TO_MASTODON_ACCOUNT.get(base_url, DEFAULT_ACCOUNT)

                    # Generate and post to Mastodon
                    message = generate_post_template(video_url, description, account)
                    try:
                        mastodon.status_post(message, visibility="public")
                        print(f"Toot sent: {message}")
                        save_posted_stream(account_url, published_at)  # Save account_url and publishedAt
                        time.sleep(random.uniform(2, 5))  # Avoid rate limiting
                    except Exception as e:
                        #print(f"Error posting to Mastodon: {e}")
                        pass
                else:
                    pass  #print(f"Video {video_title} ({video_url}) is not live, skipping.")

        except requests.RequestException as e:
            #print(f"Error fetching PeerTube streams: {e}")
            pass
        except Exception as e:
            #print(f"An unexpected error occurred: {e}")
            pass

def get_live_streams_from_owncast():
    """Fetch live streams from multiple Owncast instances and post to Mastodon if not already posted."""
    posted_streams = load_posted_streams()
    generated_descriptions = {}

    for instance in OWNCAST_INSTANCES:
        api_url = f"{instance}/api/status"
        #print(f"Checking {instance} for live status...")

        try:
            response = requests.get(api_url, timeout=10)
            response.raise_for_status()
            data = response.json()

            if not data.get("online", False):
                #print(f"No live stream on {instance}.")
                continue

            video_url = instance
            video_title = data.get("streamTitle", "Live Stream")
            last_connect_time = data.get("lastConnectTime", "")  # Capture lastConnectTime

            # Check if this combination of video_url and last_connect_time has already been posted
            if (video_url, last_connect_time) in posted_streams:
                #print(f"Already posted: {video_url}, {last_connect_time}. Skipping...")
                continue

            # Check if the video is live (again, for good measure)
            if data.get("online", False):
                print(f"Found live Owncast stream: {video_title} ({video_url})")

                # Generate description if not already generated
                if video_url not in generated_descriptions:
                    mastodon_description = generate_mastodon_hashtags(video_title)
                    if mastodon_description:
                        generated_descriptions[video_url] = mastodon_description
                    else:
                        #print(f"Skipping posting due to missing description for {video_url}")
                        continue

                description = generated_descriptions.get(video_url)

                # Get the Mastodon account from the mapping, default to DEFAULT_ACCOUNT if not found
                account = STREAM_TO_MASTODON_ACCOUNT.get(instance, DEFAULT_ACCOUNT)

                # Generate and post to Mastodon
                message = generate_owncast_post_template(video_url, description, account)  # Use Owncast template
                try:
                    mastodon.status_post(message, visibility="public")
                    print(f"Toot sent: {message}")
                    save_posted_stream(video_url, last_connect_time)  # Save Owncast URL and lastConnectTime
                    time.sleep(random.uniform(2, 5))  # Avoid rate limiting
                except Exception as e:
                    #print(f"Error posting to Mastodon: {e}")
                    pass
            else:
                #print(f"Owncast instance {video_url} is not live, skipping.")
                pass

        except requests.RequestException as e:
            #print(f"Error fetching Owncast stream from {instance}: {e}")
            pass
        except Exception as e:
            #print(f"An unexpected error occurred: {e}")
            pass

# Main loop
def clear_screen():
    if platform.system() == "Windows":
        os.system('cls')
    else:
        os.system('clear')

if __name__ == "__main__":
    #while True:
    clear_screen()
    print("Starting new cycle...")
    get_live_streams_from_peertube()
    get_live_streams_from_owncast()
    print("Sleeping for 45 minutes...\n")
        #time.sleep(2700)  # Wait 10 minutes before checking again
