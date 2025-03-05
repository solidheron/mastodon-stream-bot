import csv
import random
from datetime import datetime, timedelta
import pytz
from mastodon import Mastodon
from streamer_mastodon_account import STREAM_TO_MASTODON_ACCOUNT,access_token,mastodon_instance  # Import the dictionary

#global varibles
char_limit = 475  # Conservative limit for Mastodon posts


def post_shortest_stream_to_mastodon(merged_data, mastodon_instance, access_token,time_scale):
    one_week_ago = datetime.utcnow().replace(tzinfo=pytz.utc) - timedelta(days=time_scale)
    print(one_week_ago)
    shortest_streams = {}
    
    for row in merged_data:
        account = row['account_url']
        retrieval_time = parse_iso8601(row['retrieval_time'])
        duration = float(row['stream_duration'])
        
        if retrieval_time and retrieval_time >= one_week_ago and duration >= 900:  # 900 seconds = 15 minutes
            if account not in shortest_streams or shortest_streams[account] > duration:
                shortest_streams[account] = duration
    
    ranked_data = sorted(shortest_streams.items(), key=lambda x: x[1])
    
    m = Mastodon(access_token=access_token, api_base_url=mastodon_instance)
    if time_scale == 7:
        toot_content = "🏃‍♂️ Shortest Streams This Week (15+ minutes) 🏃‍♀\n\n"
    else:    
        toot_content = "🏃‍♂️ Shortest Streams past 24 hours 🏃‍♀\n\n"
        
    #char_limit = 450  # Conservative limit to ensure we don't exceed Mastodon's limit
    
    for rank, (account, duration) in enumerate(ranked_data, start=1):
        formatted_duration = format_duration(duration)
        mastodon_handle = STREAM_TO_MASTODON_ACCOUNT.get(account, "unknown")
        new_entry = f"{rank}. {mastodon_handle} - {formatted_duration}\n{account}\n\n"
        
        if len(toot_content) + len(new_entry) + len("#ShortestStreams #Mastodon") > char_limit:
            break
        
        toot_content += new_entry
    
    
    toot_content += "#ShortestStreams #Mastodon"
    m.status_post(toot_content)
    print("Shortest streams ranking posted to Mastodon successfully.")



def convert_to_est(dt):
    est = pytz.timezone('America/New_York')
    return dt.astimezone(est).strftime('%Y-%m-%d %I:%M %p EST')

def post_view_ranking_to_mastodon(merged_data, mastodon_instance, access_token,time_scale):
    global char_limit
    one_week_ago = datetime.utcnow().replace(tzinfo=pytz.utc) - timedelta(days=time_scale)
    latest_streams = {}
    
    for row in merged_data:
        account = row['account_url']
        retrieval_time = parse_iso8601(row['retrieval_time'])
        views = int(row.get('views', 0))
        
        if retrieval_time and retrieval_time >= one_week_ago:
            if account not in latest_streams or latest_streams[account][1] < views:
                latest_streams[account] = (retrieval_time, views)
    
    ranked_data = sorted(latest_streams.items(), key=lambda x: x[1][1], reverse=True)
    
    m = Mastodon(access_token=access_token, api_base_url=mastodon_instance)
    if time_scale == 7:
        toot_content = "👀 Most Viewed This Week 👀\n\n"
    else:    
        toot_content = "👀 Most Viewed of past 24 hours 👀\n\n"
        
    for rank, (account, (retrieval_time, views)) in enumerate(ranked_data, start=1):
        mastodon_handle = STREAM_TO_MASTODON_ACCOUNT.get(account, "@unknown")
        new_entry = f"{rank}| {mastodon_handle} - {views} views\n{account}\n\n"
        
        if len(toot_content) + len(new_entry) + len("#StreamViewRankings #Mastodon #owncast #peertube") > char_limit:
            break
        
        toot_content += new_entry
    
    toot_content += "#StreamViewRankings #Mastodon #owncast #peertube"
    m.status_post(toot_content)
    print("View ranking posted to Mastodon successfully.")

    
def post_ranking_to_mastodon(merged_data, mastodon_instance, access_token,time_scale):
    one_week_ago = datetime.utcnow().replace(tzinfo=pytz.utc) - timedelta(days=time_scale)
    latest_streams = {}
    
    for row in merged_data:
        account = row['account_url']
        retrieval_time = parse_iso8601(row['retrieval_time'])
        duration = row['stream_duration']
        
        if retrieval_time and retrieval_time >= one_week_ago:
            if account not in latest_streams or latest_streams[account][1] < duration:
                latest_streams[account] = (retrieval_time, duration)
    
    ranked_data = sorted(latest_streams.items(), key=lambda x: x[1][1], reverse=True)
    
    m = Mastodon(access_token=access_token, api_base_url=mastodon_instance)
    if time_scale == 7:
        toot_content = "🏆 Longest Streams This Week 🏆\n\n"
    else:    
        toot_content = "🏆 Longest Streams of past 24 hours 🏆\n\n"
    
    for rank, (account, (retrieval_time, duration)) in enumerate(ranked_data[:5], start=1):
        formatted_duration = format_duration(duration)
        mastodon_handle = STREAM_TO_MASTODON_ACCOUNT.get(account, "@unknown")
        toot_content += f"{rank}. {mastodon_handle} - {formatted_duration}\n{account}\n\n"
    
    toot_content += "#StreamRankings #Mastodon #owncast #peertube"
    m.status_post(toot_content)
    print("Ranking posted to Mastodon successfully.")



def post_recent_streams_to_mastodon(merged_data, mastodon_instance, access_token):
    global char_limit
    unique_accounts = {}
    
    for row in merged_data:
        account = row['account_url']
        retrieval_time = parse_iso8601(row['retrieval_time'])
        
        if retrieval_time and (account not in unique_accounts or unique_accounts[account] < retrieval_time):
            unique_accounts[account] = retrieval_time
    
    ranked_data = sorted(unique_accounts.items(), key=lambda x: x[1], reverse=True)
    
    m = Mastodon(access_token=access_token, api_base_url=mastodon_instance)
    
    
    toot_content = "⏳ Most Recent Streams ⏳\n\n"
    for rank, (account, retrieval_time) in enumerate(ranked_data, start=1):
        mastodon_handle = STREAM_TO_MASTODON_ACCOUNT.get(account, "@unknown")
        formatted_time = convert_to_est(retrieval_time)
        new_entry = f"{rank}) {mastodon_handle} - {formatted_time}\n{account}\n\n"
        
        if len(toot_content) + len(new_entry) + len("#RecentStreams #Mastodon #owncast #peertube") > char_limit:
            break
        
        toot_content += new_entry
    
    toot_content += "#RecentStreams #Mastodon #owncast #peertube"
    m.status_post(toot_content)
    print("Recent streams posted to Mastodon successfully.")


def shoutout_random_streamer(merged_data, mastodon_instance, access_token):
    global char_limit
    if not merged_data:
        return
    
    unique_accounts = list(set(row['account_url'] for row in merged_data))
    random.shuffle(unique_accounts)
    
    m = Mastodon(access_token=access_token, api_base_url=mastodon_instance)
    
    # List of possible headers
    headers = [
        "🎸 Longest Air Guitar Solo Champions 🎸",
        "🤣 Streamers with Most I-Frames 🤣",
        "🔥 Biggest Sword collection 🔥",
        "👑 Most Time Spent setting up stream 👑",
        "🚫 Most Creative Excuses for Lag 🚫",
        "🤖 least likely to be replaced by AGI 🤖",
        "RNG's Favorite Streamer",
        "🎯 Most Back-Flips on Stream 🎯",
        "🎭 The Drama Kings and Queens of Streaming 🎭",
        "🌟 Streamers Most Likely to Forget They're Live 🌟",
        "🎮 Best Button-Mashing Performances 🎮",
        "🕺 Longest Dance Break During a Stream 🕺",
        "💡 Highest Chinese Social Credit Score 💡",
        " Fediverse Streamer that donated the most to Wikipedia",
        "🍕 Streamers Most Likely to Eat on Camera 🍕",
        "🐱 Largest Anime Harem/Consort 🐱",
        "🛠 Streamers Connected To the Most Instances 🛠️",
        "🐕 most likely to bark back at a dog 🐶",
        "🎤 Karaoke Legends in the Making 🎤",
        "💎🙌 Strongest Diamond Hand 🙌💎"
        "🏆 MVPs of Talking to Themselves 🏆"
    ]
    
    # Randomly select a header
    toot_content = random.choice(headers) + "\n\n"
    
    for rank, account in enumerate(unique_accounts, start=1):
        mastodon_handle = STREAM_TO_MASTODON_ACCOUNT.get(account, "@unknown")
        new_entry = f"{rank}. {mastodon_handle}\n{account}\n\n"
        
        if len(toot_content) + len(new_entry) + len("#StreamerShoutout #Mastodon #owncast #peertube") > char_limit:
            break
        
        toot_content += new_entry
    
    #toot_content += "#StreamerShoutout #Mastodon #owncast #peertube"
    m.status_post(toot_content)
    print("Shoutout posted to Mastodon successfully.")


def read_csv(file_path):
    data = []
    try:
        with open(file_path, mode='r', newline='', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            for row in reader:
                data.append(row)
    except FileNotFoundError:
        print(f"File {file_path} not found.")
    return data

def parse_iso8601(timestamp):
    if timestamp:
        try:
            return datetime.fromisoformat(timestamp.rstrip("Z")).replace(tzinfo=pytz.utc)
        except ValueError:
            pass
    return None

def calculate_duration(start_time, end_time):
    start = parse_iso8601(start_time)
    end = parse_iso8601(end_time)
    return (end - start).total_seconds() if start and end else 0

def process_peertube_data(peertube_data):
    for row in peertube_data:
        row['stream_duration'] = calculate_duration(row.get('published_at', ''), row.get('retrieval_time', ''))
    return peertube_data

def normalize_owncast_data(owncast_data):
    normalized_data = []
    for row in owncast_data:
        normalized_row = {
            'account_url': row.get('owncast_url', ''),
            'published_at': row.get('last_connect_time', ''),
            'retrieval_time': row.get('last_disconnect_time', ''),
            'stream_duration': calculate_duration(row.get('last_connect_time', ''), row.get('last_disconnect_time', ''))
        }
        normalized_data.append(normalized_row)
    return normalized_data

def merge_data(peertube_data, owncast_data):
    return process_peertube_data(peertube_data) + normalize_owncast_data(owncast_data)

def format_duration(seconds):
    hours, remainder = divmod(int(seconds), 3600)
    minutes, seconds = divmod(remainder, 60)
    return f"{hours}h {minutes}m {seconds}s"

def log_function(func_name):
    with open('function_log.txt', 'a') as f:
        f.write(f"{func_name}\n")



def get_last_function():
    try:
        with open('function_log.txt', 'r') as f:
            lines = f.readlines()
            return lines[-1].strip() if lines else None
    except FileNotFoundError:
        return None

def post_total_stream_time_ranking(merged_data, mastodon_instance, access_token,time_scale):
    global char_limit
    one_week_ago = datetime.utcnow().replace(tzinfo=pytz.utc) - timedelta(days=time_scale)
    account_total_times = {}
    
    for row in merged_data:
        account = row['account_url']
        retrieval_time = parse_iso8601(row['retrieval_time'])
        duration = float(row['stream_duration'])
        
        if retrieval_time and retrieval_time >= one_week_ago:
            if account not in account_total_times:
                account_total_times[account] = 0
            account_total_times[account] += duration
    
    ranked_data = sorted(account_total_times.items(), key=lambda x: x[1], reverse=True)
    
    m = Mastodon(access_token=access_token, api_base_url=mastodon_instance)
    if time_scale == 7:
        toot_content = "🕒 Most Devoted streamer: Hours Streamed This Week 🕒\n\n"
    else:    
        toot_content = "🕒 Hours Streamed This 24 hours 🕒\n\n"
     
    for rank, (account, total_duration) in enumerate(ranked_data, start=1):
        mastodon_handle = STREAM_TO_MASTODON_ACCOUNT.get(account, "unknown")
        formatted_duration = format_duration(total_duration)
        new_entry = f"{rank}. {mastodon_handle} - {formatted_duration}\n{account}\n\n"
        
        if len(toot_content) + len(new_entry) + len("#TotalStreamTime #Mastodon #owncast #peertube") > char_limit:
            break
        
        toot_content += new_entry
    
    #toot_content += "#TotalStreamTime #Mastodon #owncast #peertube"
    m.status_post(toot_content)
    print("Total stream time ranking posted to Mastodon successfully.")

def post_shortest_total_stream_time_ranking(merged_data, mastodon_instance, access_token):
    global char_limit
    one_week_ago = datetime.utcnow().replace(tzinfo=pytz.utc) - timedelta(days=7)
    account_total_times = {}
    
    for row in merged_data:
        account = row['account_url']
        retrieval_time = parse_iso8601(row['retrieval_time'])
        duration = float(row['stream_duration'])
        
        if retrieval_time and retrieval_time >= one_week_ago:
            if account not in account_total_times:
                account_total_times[account] = 0
            account_total_times[account] += duration
    
    # Filter out accounts with less than 15 minutes total streaming time
    account_total_times = {k: v for k, v in account_total_times.items() if v >= 900}
    
    ranked_data = sorted(account_total_times.items(), key=lambda x: x[1])
    
    m = Mastodon(access_token=access_token, api_base_url=mastodon_instance)
    
    toot_content = "⏱️ Shortest Total Streaming Time This Week (15+ minutes) ⏱️\n\n"
    for rank, (account, total_duration) in enumerate(ranked_data, start=1):
        mastodon_handle = STREAM_TO_MASTODON_ACCOUNT.get(account, "unknown")
        formatted_duration = format_duration(total_duration)
        new_entry = f"{rank}. {mastodon_handle} - {formatted_duration}\n{account}\n\n"
        
        if len(toot_content) + len(new_entry) + len("#ShortestTotalStreamTime #Mastodon #owncast #peertube") > char_limit:
            break
        
        toot_content += new_entry
    
    toot_content += "#ShortestTotalStreamTime #Mastodon #owncast #peertube"
    m.status_post(toot_content)
    print("Shortest total stream time ranking posted to Mastodon successfully.")
    
def post_stream_frequency_ranking(merged_data, mastodon_instance, access_token, time_scale):
    global char_limit
    one_week_ago = datetime.utcnow().replace(tzinfo=pytz.utc) - timedelta(days=time_scale)
    stream_counts = {}

    for row in merged_data:
        account = row['account_url']
        retrieval_time = parse_iso8601(row['retrieval_time'])
        
        if retrieval_time and retrieval_time >= one_week_ago:
            stream_counts[account] = stream_counts.get(account, 0) + 1

    ranked_data = sorted(stream_counts.items(), key=lambda x: x[1], reverse=True)

    m = Mastodon(access_token=access_token, api_base_url=mastodon_instance)
    time_label = "Week" if time_scale == 7 else "24 Hours"
    toot_content = f"📡 Most Active Streamers This {time_label} 📡\n\n"

    for rank, (account, count) in enumerate(ranked_data, start=1):
        mastodon_handle = STREAM_TO_MASTODON_ACCOUNT.get(account, "@unknown")
        new_entry = f"{rank}. {mastodon_handle} - {count} streams\n{account}\n\n"
        
        if len(toot_content) + len(new_entry) + len("#StreamFrequency #Mastodon") > char_limit:
            break
            
        toot_content += new_entry

    toot_content += "#StreamFrequency #Mastodon #owncast #peertube"
    m.status_post(toot_content)
    log_function('post_stream_frequency')
    print(f"Stream frequency ranking for {time_label} posted successfully.")


def main():
    peertube_data = read_csv("peertube_data2.csv")
    owncast_data = read_csv("owncast_streamtime.csv")
    merged_data = merge_data(peertube_data, owncast_data)
  
    last_function = get_last_function()
    
    functions = [
        ('post_ranking_week', lambda: post_ranking_to_mastodon(merged_data, mastodon_instance, access_token,7)),
        ('post_ranking_day', lambda: post_ranking_to_mastodon(merged_data, mastodon_instance, access_token,1)),
        ('post_recent_week', lambda: post_recent_streams_to_mastodon(merged_data, mastodon_instance, access_token)),
        ('shoutout', lambda: shoutout_random_streamer(merged_data, mastodon_instance, access_token)),
        ('post_view_ranking_week', lambda: post_view_ranking_to_mastodon(merged_data, mastodon_instance, access_token,7)),
        ('post_view_ranking_day', lambda: post_view_ranking_to_mastodon(merged_data, mastodon_instance, access_token,1)),
        ('post_shortest_week', lambda: post_shortest_stream_to_mastodon(merged_data, mastodon_instance, access_token,7)),
        ('post_shortest_day', lambda: post_shortest_stream_to_mastodon(merged_data, mastodon_instance, access_token,1)),
        ('post_freq_week', lambda: post_stream_frequency_ranking(merged_data, mastodon_instance, access_token,7)),
        #('post_shortest_total_time_week', lambda: post_shortest_total_stream_time_ranking(merged_data, mastodon_instance, access_token)),
        ('post_total_time_week', lambda: post_total_stream_time_ranking(merged_data, mastodon_instance, access_token,7)),
        ('post_total_time_day', lambda: post_total_stream_time_ranking(merged_data, mastodon_instance, access_token,1))
    ]
    
    available_functions = [f for f in functions if f[0] != last_function]
    
    if available_functions:
        chosen_function = random.choice(available_functions)
        log_function(chosen_function[0])
        chosen_function[1]()
    else:
        print("Unexpected error: No available functions to execute.")

if __name__ == "__main__":
    main()
