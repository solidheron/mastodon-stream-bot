import csv
import random
from datetime import datetime, timedelta
import pytz
from mastodon import Mastodon
import os
from streamer_mastodon_account import STREAM_TO_MASTODON_ACCOUNT, access_token, mastodon_instance  # Import the dictionary

# global variables
char_limit = 475  # Conservative limit for Mastodon posts

def post_shortest_stream_to_mastodon(merged_data, mastodon_instance, access_token, time_scale):
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


def post_view_ranking_to_mastodon(merged_data, mastodon_instance, access_token, time_scale):
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
    print(toot_content)
    m.status_post(toot_content)
    print("View ranking posted to Mastodon successfully.")


def post_ranking_to_mastodon(merged_data, mastodon_instance, access_token, time_scale):
    one_week_ago = datetime.utcnow().replace(tzinfo=pytz.utc) - timedelta(days=time_scale)
    print("one_week_ago:", one_week_ago)
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
        "Fediverse Streamer that donated the most to Wikipedia",
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
    
    toot_content += "#StreamerShoutout #Mastodon #owncast #peertube"
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
    with open('DATA/function_log_mastodon.txt', 'a') as f:
        f.write(f"{func_name}\n")

def get_last_functions(n=5):
    try:
        with open('DATA/function_log_mastodon.txt', 'r') as f:
            lines = f.readlines()
            return [line.strip() for line in lines[-n:]]
    except FileNotFoundError:
        return []


def post_total_stream_time_ranking(merged_data, mastodon_instance, access_token, time_scale):
    global char_limit
    one_week_ago = datetime.utcnow().replace(tzinfo=pytz.utc) - timedelta(days=time_scale)
    print("one_week_ago:", one_week_ago)
    account_total_times = {}
    
    for row in merged_data:
        account = row['account_url']
        retrieval_time = parse_iso8601(row['retrieval_time'])
        duration = float(row['stream_duration'])
        
        if retrieval_time and retrieval_time >= one_week_ago:
            print(retrieval_time)
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
    
    toot_content += "#TotalStreamTime #Mastodon #owncast #peertube"
    print(toot_content)
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


def post_overall_stats(merged_data, mastodon_instance, access_token, time_scale):
    """
    Calculates and posts overall streaming statistics for the given time_scale.
    Statistics include total hours streamed, number of unique streamers, and total streams.
    """
    # Filter merged_data based on retrieval_time and time_scale
    time_threshold = datetime.utcnow().replace(tzinfo=pytz.utc) - timedelta(days=time_scale)
    filtered_data = [
        row for row in merged_data
        if parse_iso8601(row['retrieval_time']) and parse_iso8601(row['retrieval_time']) >= time_threshold
    ]
    
    # Calculate total streamed duration in seconds, then convert to hours
    total_duration_seconds = sum(float(row['stream_duration']) for row in filtered_data)
    total_hours = total_duration_seconds / 3600
    
    # Count unique streamers and total streams
    unique_streamers = set(row['account_url'] for row in filtered_data)
    count_streamers = len(unique_streamers)
    count_streams = len(filtered_data)
    
    m = Mastodon(access_token=access_token, api_base_url=mastodon_instance)
    if time_scale == 7:
        toot_content = "📊 Weekly Streaming Summary 📊\n\n"
    else:
        toot_content = "📊 Daily Streaming Summary 📊\n\n"
    
    toot_content += f"Total Hours Streamed: {total_hours:.2f} hours\n"
    toot_content += f"Unique Streamers: {count_streamers}\n"
    toot_content += f"Total Streams: {count_streams}\n\n"
    toot_content += "#StreamingSummary #Mastodon #owncast #peertube"
    
    m.status_post(toot_content)
    print("Overall streaming summary posted to Mastodon successfully.")


def format_time_from_seconds(sec):
    """Convert seconds since midnight to a formatted time string in hh:mm:ss am/pm."""
    t = datetime(1900, 1, 1) + timedelta(seconds=sec)
    return t.strftime("%I:%M:%S %p").lstrip("0").lower()

def merge_intervals(intervals):
    """Merge overlapping intervals. Each interval is a tuple (start, end)."""
    intervals.sort(key=lambda x: x[0])
    merged = []
    for start, end in intervals:
        if not merged or start > merged[-1][1]:
            merged.append([start, end])
        else:
            merged[-1][1] = max(merged[-1][1], end)
    return merged


def post_stream_coverage(merged_data, mastodon_instance, access_token, time_scale):
    """
    Analyzes streaming intervals within the given time_scale (in days) and posts the inverse of the gaps,
    i.e., the periods when streaming occurs. Each stream's start time (from 'published_at') and end time 
    (from 'retrieval_time') are converted to EST and normalized to seconds since midnight.
    Overlapping intervals are merged to form the overall streaming coverage.
    The intervals are then formatted (e.g., '12:30:24 am - 7:30:55 pm') and posted to Mastodon.
    """
    est = pytz.timezone('America/New_York')
    time_threshold = datetime.utcnow().replace(tzinfo=pytz.utc) - timedelta(days=time_scale)
    
    intervals = []
    
    for row in merged_data:
        start_dt = parse_iso8601(row.get('published_at', ''))
        end_dt = parse_iso8601(row.get('retrieval_time', ''))
        if start_dt and end_dt and end_dt >= time_threshold:
            start_est = start_dt.astimezone(est)
            end_est = end_dt.astimezone(est)
            start_sec = start_est.hour * 3600 + start_est.minute * 60 + start_est.second
            end_sec = end_est.hour * 3600 + end_est.minute * 60 + end_est.second
            # If the stream doesn't span midnight.
            if start_sec <= end_sec:
                intervals.append((start_sec, end_sec))
            else:
                # Split streams that span midnight.
                intervals.append((start_sec, 86400))
                intervals.append((0, end_sec))

    if time_scale == 7:
        toot_content = "📡 fedistreamers coverage for the past week(EST) 📡\n\n"
    else:    
        toot_content = "📡 fedistreamers coverage for the past 24 hours 📡\n\n"
    
    if not intervals:
        toot_content = "No streaming data available for the selected time period."
    else:
        # Merge overlapping intervals to get overall streaming coverage.
        merged_intervals = merge_intervals(intervals)
        
        # Format each interval using a helper function.
        formatted_intervals = []
        for start, end in merged_intervals:
            start_str = format_time_from_seconds(start)
            end_str = format_time_from_seconds(end)
            formatted_intervals.append(f"{start_str} - {end_str}")
        
        intervals_str = "\n".join(formatted_intervals)
        toot_content += (f"Streaming coverage:\n{intervals_str}\n\n"
                        "Time of day when at least one streamer was live within a given time period (e.g., a day or a week)\n\n"
                        "#StreamCoverage #Mastodon #owncast #peertube")
    
    m = Mastodon(access_token=access_token, api_base_url=mastodon_instance)
    print(toot_content)
    m.status_post(toot_content)
    print("Streaming coverage posted to Mastodon successfully.")


def post_stream_count_by_hour(merged_data, mastodon_instance, access_token, time_scale):
    """
    Computes and posts a breakdown of how many streams were active during each hour of the day.
    
    It filters streams within the specified time_scale (in days), converts each stream's
    start time (published_at) and end time (retrieval_time) to Eastern Time (EST), determines
    which hours of the day each stream covers, and counts the occurrences per hour.
    
    The output is formatted like:
    
    ⏰ Streams active per hour:
    12:00 am: 3 streams
    1:00 am: 2 streams
    ...
    11:00 pm: 1 streams
    
    and is posted to Mastodon.
    """
    # Set Eastern Time zone and time threshold
    est = pytz.timezone('America/New_York')
    time_threshold = datetime.utcnow().replace(tzinfo=pytz.utc) - timedelta(days=time_scale)
    
    # Initialize count for each hour (0 to 23)
    hour_counts = {h: 0 for h in range(24)}

    if time_scale == 7:
        output = "⏰ streams per hour week ⏰\n"
    else:    
        output = "⏰ streams per hour past 24 hours ⏰\n"
    
    # Iterate over each stream in merged_data
    for row in merged_data:
        start_dt = parse_iso8601(row.get('published_at', ''))
        end_dt = parse_iso8601(row.get('retrieval_time', ''))
        
        if start_dt and end_dt and end_dt >= time_threshold:
            start_est = start_dt.astimezone(est)
            end_est = end_dt.astimezone(est)
            
            # If the stream does not span midnight:
            if start_est <= end_est:
                start_hour = start_est.hour
                end_hour = end_est.hour
                # Count each hour that the stream was active.
                for h in range(start_hour, end_hour + 1):
                    hour_counts[h] += 1
            else:
                # Stream spans midnight: count from start hour until 23 and from 0 until end hour.
                for h in range(start_est.hour, 24):
                    hour_counts[h] += 1
                for h in range(0, end_est.hour + 1):
                    hour_counts[h] += 1
    
    # Build output string with formatted hours.
    output += "hour(EST):Number of streams\n"
    for h in range(24):
        # Format hour as "HH:00 am/pm" (e.g., "12:00 am"). Remove any leading zero.
        hour_str = datetime(1900, 1, 1, h).strftime("%I:%M %p").lstrip("0").lower()
        output += f"{hour_str}: {hour_counts[h]}\n"
    output += "\n#StreamCountByHour #Mastodon #owncast #peertube"
    
    m = Mastodon(access_token=access_token, api_base_url=mastodon_instance)
    print(output)
    m.status_post(output)
    print("Stream count by hour posted to Mastodon successfully.")



def main():
    peertube_data = read_csv("DATA/peertube_data2.csv")
    owncast_data = read_csv("DATA/owncast_streamtime.csv")
    merged_data = merge_data(peertube_data, owncast_data)
       
    # Ensure the "DATA" folder exists
    output_folder = "DATA"
    os.makedirs(output_folder, exist_ok=True)
    
    # Write merged data to a CSV file in the "DATA" folder if needed
    # print(merged_data)
    functions = [
        ('post_ranking_week', lambda: post_ranking_to_mastodon(merged_data, mastodon_instance, access_token, 7)),
        ('post_ranking_day', lambda: post_ranking_to_mastodon(merged_data, mastodon_instance, access_token, 1)),
        ('post_recent_week', lambda: post_recent_streams_to_mastodon(merged_data, mastodon_instance, access_token)),
        ('shoutout', lambda: shoutout_random_streamer(merged_data, mastodon_instance, access_token)),
        ('post_view_ranking_week', lambda: post_view_ranking_to_mastodon(merged_data, mastodon_instance, access_token, 7)),
        ('post_view_ranking_day', lambda: post_view_ranking_to_mastodon(merged_data, mastodon_instance, access_token, 1)),
        ('post_shortest_week', lambda: post_shortest_stream_to_mastodon(merged_data, mastodon_instance, access_token, 7)),
        ('post_freq_week', lambda: post_stream_frequency_ranking(merged_data, mastodon_instance, access_token, 7)),
        ('post_total_time_week', lambda: post_total_stream_time_ranking(merged_data, mastodon_instance, access_token, 7)),
        ('post_total_time_day', lambda: post_total_stream_time_ranking(merged_data, mastodon_instance, access_token, 1)),
        ('post_overall_stats_week', lambda: post_overall_stats(merged_data, mastodon_instance, access_token, 7)),
        ('post_overall_stats_day', lambda: post_overall_stats(merged_data, mastodon_instance, access_token, 1))
        #('post_stream_hours_coverage_week', lambda: post_stream_coverage(merged_data, mastodon_instance, access_token, 7)),
        #('post_stream_hours_coverage_day', lambda: post_stream_coverage(merged_data, mastodon_instance, access_token, 1)),
        #('strems_per_hour_week', lambda: post_stream_count_by_hour(merged_data, mastodon_instance, access_token, 7)),
        #('streams_per_hour_day', lambda: post_stream_count_by_hour(merged_data, mastodon_instance, access_token, 1))
    ]
  
    last_functions = get_last_functions(5)
    # print(f"Last used functions: {last_functions}")
    
    available_functions = [f for f in functions if f[0] not in last_functions]
    
    if available_functions:
        chosen_function = random.choice(available_functions)
        print(f"Chosen function: {chosen_function[0]}")
        log_function(chosen_function[0])
        chosen_function[1]()
    else:
        print("All recent functions have been used. Choosing a random function.")
        chosen_function = random.choice(functions)
        print(f"Chosen function: {chosen_function[0]}")
        log_function(chosen_function[0])
        chosen_function[1]()

if __name__ == "__main__":
    main()
