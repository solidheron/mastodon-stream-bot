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
MASTODON_ACCESS_TOKEN = "MASTODON_ACCESS_TOKEN"
#os.environ["OLLAMA_HOME"] = r"D:\ollama_ai" #line if you need to change the code
PEERTUBE_INSTANCE = "replace.this"  # Replace with your PeerTube instance URL

# Set the path to your local Ollama model
ollama_path = r"D:\ollama_ai"

# Initialize Mastodon API
mastodon = Mastodon(access_token=MASTODON_ACCESS_TOKEN, api_base_url="https://mastodon.social")

# File to store posted live streams and their update time
CSV_FILE = "live_streams_posted.csv"
# File to store posted live streams
CSV_FILE = "owncast_live_streams_posted.csv"

# PeerTube API URL to get live videos
API_URL = f"{PEERTUBE_INSTANCE}/api/v1/videos"

# List of User URLs to track
# User URLs to track on PeerTube
USER_URLS = [
    "https://tube.vencabot.com/accounts/vencabot",    
    "https://dalek.zone/accounts/solidheron"
]

# Owncast instances to track
OWNCAST_INSTANCES = [
    "https://mthrbord.tv/",
    "https://live.retrostrange.com/"
]
# this sections is for peertube
def generate_random_emoji():
    """Return a random emoji from a predefined list."""
    emojis = [
        "🎥", "🔴", "✨", "🔥", "🎮", "🎤", "📺", "💥", "🕹️", "🎉", "🚀", "👾", "🥳", "💡", "🎶"
    ]
    return random.choice(emojis)

def generate_mastodon_message(video_title, video_url, mastodon_description):
    """Generate the Mastodon post message with random emojis."""
    emoji_1 = generate_random_emoji()
    emoji_2 = generate_random_emoji()

    message = f"""{emoji_1} LIVE NOW on PeerTube!
{emoji_2} Watch here: {video_url}  
{mastodon_description} #peertube #livestream #stream #twitch"""
    
    return message

def load_posted_videos():
    """Read the CSV file and return a dictionary of posted video URLs with their updatedAt values."""
    posted_videos = {}
    if not os.path.exists(CSV_FILE):
        return posted_videos
    
    with open(CSV_FILE, mode="r", newline="", encoding="utf-8") as file:
        reader = csv.reader(file)
        for row in reader:
            if len(row) < 2:
                # Skip any malformed or empty rows
                continue
            video_url = row[0]
            updated_at = row[1]
            posted_videos[video_url] = updated_at  # Store video URL and updatedAt as a dictionary key-value pair
    return posted_videos

def save_posted_video(video_url, updated_at):
    """Append a new video URL and its updatedAt value to the CSV file."""
    with open(CSV_FILE, mode="a", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)
        writer.writerow([video_url, updated_at])

def generate_mastodon_description(video_title):
    """Use Ollama model to generate a Mastodon post description."""
    try:
        prompt = f"Write a short, engaging Mastodon post (under 250 characters) for a live stream titled '{video_title}'."
        response = ollama.chat(model="qwen2.5-coder:7b", messages=[{"role": "user", "content": prompt}])
        description = response.get("message", {}).get("content", "").strip()
        return description
    except Exception as e:
        print(f"Error generating description: {e}")
        return None

def get_live_streams_from_api():
    """Fetch live videos from the PeerTube API and post to Mastodon."""
    posted_videos = load_posted_videos()

    try:
        params = {'live': 'true', 'per_page': 10}
        response = requests.get(API_URL, params=params)
        response.raise_for_status()

        json_response = response.json()

        videos = json_response.get('data') or json_response.get('videos')

        if not videos:
            print("No live videos found.")
            return

        for video in videos:
            video_url = video.get('url', '')
            video_title = video.get('name', '')
            is_live = video.get('isLive', False)
            updated_at = video.get('updatedAt', '')  # Get the updatedAt timestamp

            # Check if the video is live and belongs to a tracked user account
            if (is_live and video_url 
                and any(user_url in video.get('account', {}).get('url', '') for user_url in USER_URLS)
                and (video_url not in posted_videos or posted_videos[video_url] != updated_at)):  # Compare updatedAt values
                
                mastodon_description = generate_mastodon_description(video_title)
                if mastodon_description:
                    message = generate_mastodon_message(video_title, video_url, mastodon_description)
                    mastodon.status_post(message, visibility="public")
                    save_posted_video(video_url, updated_at)  # Save the new updatedAt value
                    time.sleep(random.uniform(2, 5))  # Sleep to avoid rate limiting
    
    except requests.RequestException as e:
        print(f"Error fetching live streams from PeerTube: {e}")
        
#Owncast data retreval section
def load_owncast_posted_streams():
    """Read the CSV file and return a dictionary of posted stream URLs with start times."""
    if not os.path.exists(CSV_FILE):
        return set()
    
    with open(CSV_FILE, mode="r", newline="", encoding="utf-8") as file:
        reader = csv.reader(file)
        return {row[0]: row[1] for row in reader}  # First column contains stream URLs

def save_owncast_posted_stream(stream_url, start_time):
    """Append a new live stream URL to the CSV file."""
    with open(CSV_FILE, mode="a", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)
        writer.writerow([stream_url, start_time])

def check_owncast_live_status(instance_url):
    """Check if an Owncast instance is currently streaming and return start time."""
    try:
        response = requests.get(f"{instance_url}/api/status", timeout=5)
        response.raise_for_status()
        
        data = response.json()
        return data.get("online", False), data.get("lastConnectTime", "Unknown")  # 'online' should be True if live
    except requests.RequestException as e:
        print(f"Error checking stream status for {instance_url}: {e}")
        return False, "Unknown"

def generate_owncast_random_emoji():
    """Return a random emoji from a predefined list."""
    emojis = ["🎥", "🔴", "✨", "🔥", "🎮", "🎤", "📺", "💥", "🕹️", "🎉", "🚀", "👾", "🥳", "💡", "🎶"]
    return random.choice(emojis)

def generate_owncast_mastodon_description(instance_url):
    """Use Ollama model to generate a Mastodon post description."""
    try:
        prompt = f"Write a short, engaging Mastodon post (under 250 characters) for a live stream happening at {instance_url}."
        response = ollama.chat(model="qwen2.5-coder:7b", messages=[{"role": "user", "content": prompt}])
        description = response['message']['content'].strip()
        return description
    except Exception as e:
        print(f"Error generating description: {e}")
        return None

def generate_owncast_mastodon_message(instance_url, mastodon_description):
    """Generate the Mastodon post message with random emojis."""
    emoji_1, emoji_2 = generate_owncast_random_emoji(), generate_owncast_random_emoji()
    message = f"""{emoji_1} LIVE NOW on Owncast!
{emoji_2} Watch here: {instance_url}  
{mastodon_description} #owncast #livestream #stream"""
    return message

def check_and_post_owncast_streams():
    """Check all Owncast instances and post to Mastodon if live."""
    posted_streams = load_owncast_posted_streams()
    
    for instance_url in OWNCAST_INSTANCES:
        is_live, start_time = check_owncast_live_status(instance_url)
        if is_live and (instance_url not in posted_streams or posted_streams.get(instance_url) != start_time):
            print(f"{instance_url} is live! Posting to Mastodon...")
            mastodon_description = generate_owncast_mastodon_description(instance_url)
            if mastodon_description:
                message = generate_owncast_mastodon_message(instance_url, mastodon_description)
                mastodon.status_post(message, visibility="public")
                print(f"Toot sent: {message}")
                save_owncast_posted_stream(instance_url, start_time)
        else:
            print(f"{instance_url} is not live or already posted.")

if __name__ == "__main__":
    while True:
        get_live_streams_from_api()
        check_and_post_owncast_streams()
        time.sleep(600)  # Wait for 10 minutes before running again
