import csv
import random
from datetime import datetime, timedelta
import pytz
from mastodon import Mastodon
import os
import json
from streamer_mastodon_account import STREAM_TO_MASTODON_ACCOUNT,mastodon_instance  # Import the dictionary

#global varibles
char_limit = 1500  # Conservative limit for Mastodon posts


def post_shortest_stream_to_mastodon(merged_data,time_scale):
    # Calculate time threshold based on time_scale
    end_date = datetime.utcnow().replace(tzinfo=pytz.utc)
    start_date = end_date - timedelta(days=time_scale)
    
    shortest_streams = {}
    
    # Filter and process streams
    for row in merged_data:
        try:
            account = row['account_url']
            retrieval_time = parse_iso8601(row['retrieval_time'])
            duration = float(row['stream_duration'])
            
            if retrieval_time and start_date <= retrieval_time <= end_date and duration >= 900:  # Minimum 15 minutes
                if account not in shortest_streams or shortest_streams[account] > duration:
                    shortest_streams[account] = duration
        
        except Exception as e:
            print(f"Error processing row: {e}")
            continue
    
    # Rank streams by duration (ascending order)
    ranked_data = sorted(shortest_streams.items(), key=lambda x: x[1])
    
    if not ranked_data:
        print("No streams found meeting the criteria.")
        return
    
    # Prepare title with date range
    start_str = start_date.strftime("%m/%d/%Y")
    end_str = end_date.strftime("%m/%d/%Y")
    title = f"Shortest Streams ({start_str} - {end_str})"
    
    # Prepare content for Mastodon post
    toot_content = ""
    
    for rank, (account, duration) in enumerate(ranked_data, start=1):
        formatted_duration = format_duration(duration)
        mastodon_handle = STREAM_TO_MASTODON_ACCOUNT.get(account, "unknown")
        new_entry = f"{rank}. {mastodon_handle} - {formatted_duration}\n{account}"
        
        if len(toot_content) + len(new_entry) + len("#ShortestStreams #Mastodon") > char_limit:
            break
        
        toot_content += new_entry
    
    # Prepare post details
    new_post_details = {
        "community_name": "fedistream",
        "title": title,
        "body": toot_content,
        "url": None,
    }
    
    try:
        # Read existing data from the file
        try:
            with open("lemmy_posting_infastructure/post_details.json", "r") as json_file:
                existing_data = json.load(json_file)
                if not isinstance(existing_data, list):  # Ensure it's a list
                    raise ValueError("JSON data is not a list.")
        except (FileNotFoundError, ValueError, json.JSONDecodeError):
            existing_data = []  # Initialize as an empty list if file is missing or invalid
        
        # Append new post details
        existing_data.append(new_post_details)
        
        # Write updated data back to the file
        with open("lemmy_posting_infastructure/post_details.json", "w") as json_file:
            json.dump(existing_data, json_file, indent=4)
        print("Post details appended successfully.")
        
        
    except Exception as e:
        print(f"Failed to save or post: {e}")



def convert_to_est(dt):
    est = pytz.timezone('America/New_York')
    return dt.astimezone(est).strftime('%Y-%m-%d %I:%M %p EST')

def post_view_ranking_to_mastodon(merged_data,time_scale):
    global char_limit
    end_date = datetime.utcnow().replace(tzinfo=pytz.utc)
    start_date = end_date - timedelta(days=time_scale)
    
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
    
    # Prepare title with date range
    start_str = start_date.strftime("%m/%d/%Y")
    end_str = end_date.strftime("%m/%d/%Y")
    title = f"Most Viewed of ({start_str} - {end_str})"

    toot_content = ""
    for rank, (account, (retrieval_time, views)) in enumerate(ranked_data, start=1):
        mastodon_handle = STREAM_TO_MASTODON_ACCOUNT.get(account, "@unknown")
        new_entry = f"{rank}| {mastodon_handle} - {views} views\n{account}\n\n"
        
        if len(toot_content) + len(new_entry) + len("#StreamViewRankings #Mastodon #owncast #peertube") > char_limit:
            break
        
        toot_content += new_entry
    
    # Prepare post details
    new_post_details = {
        "community_name": "fedistream",
        "title": title,
        "body": toot_content,
        "url": None,
    }
    
    try:
        # Read existing data from the file
        try:
            with open("lemmy_posting_infastructure/post_details.json", "r") as json_file:
                existing_data = json.load(json_file)
                if not isinstance(existing_data, list):  # Ensure it's a list
                    raise ValueError("JSON data is not a list.")
        except (FileNotFoundError, ValueError, json.JSONDecodeError):
            existing_data = []  # Initialize as an empty list if file is missing or invalid
        
        # Append new post details
        existing_data.append(new_post_details)
        
        # Write updated data back to the file
        with open("lemmy_posting_infastructure/post_details.json", "w") as json_file:
            json.dump(existing_data, json_file, indent=4)
        print("Post details appended successfully.")
        
        
    except Exception as e:
        print(f"Failed to save or post: {e}")

    
def post_ranking_to_mastodon(merged_data,time_scale):
    end_date = datetime.utcnow().replace(tzinfo=pytz.utc)
    start_date = end_date - timedelta(days=time_scale)
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

    # Prepare title with date range
    start_str = start_date.strftime("%m/%d/%Y")
    end_str = end_date.strftime("%m/%d/%Y")
    title = f"Longest Streams of ({start_str} - {end_str})"
    
    toot_content = ""
    for rank, (account, (retrieval_time, duration)) in enumerate(ranked_data[:5], start=1):
        formatted_duration = format_duration(duration)
        mastodon_handle = STREAM_TO_MASTODON_ACCOUNT.get(account, "@unknown")
        toot_content += f"{rank}. {mastodon_handle} - {formatted_duration}\n{account}"
    
    # Prepare post details
    new_post_details = {
        "community_name": "fedistream",
        "title": title,
        "body": toot_content,
        "url": None,
    }
    
    try:
        # Read existing data from the file
        try:
            with open("lemmy_posting_infastructure/post_details.json", "r") as json_file:
                existing_data = json.load(json_file)
                if not isinstance(existing_data, list):  # Ensure it's a list
                    raise ValueError("JSON data is not a list.")
        except (FileNotFoundError, ValueError, json.JSONDecodeError):
            existing_data = []  # Initialize as an empty list if file is missing or invalid
        
        # Append new post details
        existing_data.append(new_post_details)
        
        # Write updated data back to the file
        with open("lemmy_posting_infastructure/post_details.json", "w") as json_file:
            json.dump(existing_data, json_file, indent=4)
        print("Post details appended successfully.")
        
        
    except Exception as e:
        print(f"Failed to save or post: {e}")



def post_recent_streams_to_mastodon(merged_data):
    global char_limit
    end_date = datetime.utcnow().replace(tzinfo=pytz.utc)
    unique_accounts = {}


    
    for row in merged_data:
        account = row['account_url']
        retrieval_time = parse_iso8601(row['retrieval_time'])
        
        if retrieval_time and (account not in unique_accounts or unique_accounts[account] < retrieval_time):
            unique_accounts[account] = retrieval_time
    
    ranked_data = sorted(unique_accounts.items(), key=lambda x: x[1], reverse=True)
    
    
    
    toot_content = ""

    # Prepare title with date range
    end_str = end_date.strftime("%m/%d/%Y")
    title = f"Most Recent Streams as of {end_str}"
    
    for rank, (account, retrieval_time) in enumerate(ranked_data, start=1):
        mastodon_handle = STREAM_TO_MASTODON_ACCOUNT.get(account, "@unknown")
        formatted_time = convert_to_est(retrieval_time)
        new_entry = f"{rank}) {mastodon_handle} - {formatted_time}\n{account}"
        
        if len(toot_content) + len(new_entry) + len("#RecentStreams #Mastodon #owncast #peertube") > char_limit:
            break
        
        toot_content += new_entry
    
    # Prepare post details
    new_post_details = {
        "community_name": "fedistream",
        "title": title,
        "body": toot_content,
        "url": None,
    }
    
    try:
        # Read existing data from the file
        try:
            with open("lemmy_posting_infastructure/post_details.json", "r") as json_file:
                existing_data = json.load(json_file)
                if not isinstance(existing_data, list):  # Ensure it's a list
                    raise ValueError("JSON data is not a list.")
        except (FileNotFoundError, ValueError, json.JSONDecodeError):
            existing_data = []  # Initialize as an empty list if file is missing or invalid
        
        # Append new post details
        existing_data.append(new_post_details)
        
        # Write updated data back to the file
        with open("lemmy_posting_infastructure/post_details.json", "w") as json_file:
            json.dump(existing_data, json_file, indent=4)
        print("Post details appended successfully.")
        
        
    except Exception as e:
        print(f"Failed to save or post: {e}")


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
    with open('DATA/function_log_lemmy.txt', 'a') as f:
        f.write(f"{func_name}\n")



def get_last_functions(n=5):
    try:
        with open('DATA/function_log_lemmy.txt', 'r') as f:
            lines = f.readlines()
            return [line.strip() for line in lines[-n:]]
    except FileNotFoundError:
        return []


def post_total_stream_time_ranking(merged_data,time_scale):
    end_date = datetime.utcnow().replace(tzinfo=pytz.utc)
    start_date = end_date - timedelta(days=time_scale)
    global char_limit
    one_week_ago = datetime.utcnow().replace(tzinfo=pytz.utc) - timedelta(days=time_scale)
    print("one_week_ago:",one_week_ago)
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
    
     
    toot_content = ""

    # Prepare title with date range
    start_str = start_date.strftime("%m/%d/%Y")
    end_str = end_date.strftime("%m/%d/%Y")
    title = f"Hours Streamed as of ({start_str} - {end_str})"
    
    for rank, (account, total_duration) in enumerate(ranked_data, start=1):
        mastodon_handle = STREAM_TO_MASTODON_ACCOUNT.get(account, "unknown")
        formatted_duration = format_duration(total_duration)
        new_entry = f"{rank}. {mastodon_handle} - {formatted_duration}\n{account}\n\n"
        
        if len(toot_content) + len(new_entry) + len("#TotalStreamTime #Mastodon #owncast #peertube") > char_limit:
            break
        
        toot_content += new_entry
    
    # Prepare post details
    new_post_details = {
        "community_name": "fedistream",
        "title": title,
        "body": toot_content,
        "url": None,
    }
    
    try:
        # Read existing data from the file
        try:
            with open("lemmy_posting_infastructure/post_details.json", "r") as json_file:
                existing_data = json.load(json_file)
                if not isinstance(existing_data, list):  # Ensure it's a list
                    raise ValueError("JSON data is not a list.")
        except (FileNotFoundError, ValueError, json.JSONDecodeError):
            existing_data = []  # Initialize as an empty list if file is missing or invalid
        
        # Append new post details
        existing_data.append(new_post_details)
        
        # Write updated data back to the file
        with open("lemmy_posting_infastructure/post_details.json", "w") as json_file:
            json.dump(existing_data, json_file, indent=4)
        print("Post details appended successfully.")
        
        
    except Exception as e:
        print(f"Failed to save or post: {e}")


def post_stream_frequency_ranking(merged_data, time_scale):
    global char_limit
    end_date = datetime.utcnow().replace(tzinfo=pytz.utc)
    start_date = end_date - timedelta(days=time_scale)
    one_week_ago = datetime.utcnow().replace(tzinfo=pytz.utc) - timedelta(days=time_scale)
    stream_counts = {}

    for row in merged_data:
        account = row['account_url']
        retrieval_time = parse_iso8601(row['retrieval_time'])
        
        if retrieval_time and retrieval_time >= one_week_ago:
            stream_counts[account] = stream_counts.get(account, 0) + 1

    ranked_data = sorted(stream_counts.items(), key=lambda x: x[1], reverse=True)

    toot_content = ""

    # Prepare title with date range
    start_str = start_date.strftime("%m/%d/%Y")
    end_str = end_date.strftime("%m/%d/%Y")
    title = f"Number of streams ({start_str} - {end_str})"

    for rank, (account, count) in enumerate(ranked_data, start=1):
        mastodon_handle = STREAM_TO_MASTODON_ACCOUNT.get(account, "@unknown")
        new_entry = f"{rank}. {mastodon_handle} - {count} streams\n{account}\n\n"
        
        if len(toot_content) + len(new_entry) + len("#StreamFrequency #Mastodon") > char_limit:
            break
            
        toot_content += new_entry

    # Prepare post details
    new_post_details = {
        "community_name": "fedistream",
        "title": title,
        "body": toot_content,
        "url": None,
    }
    
    try:
        # Read existing data from the file
        try:
            with open("lemmy_posting_infastructure/post_details.json", "r") as json_file:
                existing_data = json.load(json_file)
                if not isinstance(existing_data, list):  # Ensure it's a list
                    raise ValueError("JSON data is not a list.")
        except (FileNotFoundError, ValueError, json.JSONDecodeError):
            existing_data = []  # Initialize as an empty list if file is missing or invalid
        
        # Append new post details
        existing_data.append(new_post_details)
        
        # Write updated data back to the file
        with open("lemmy_posting_infastructure/post_details.json", "w") as json_file:
            json.dump(existing_data, json_file, indent=4)
        print("Post details appended successfully.")
        
        
    except Exception as e:
        print(f"Failed to save or post: {e}")


def run_script_in_environment(subfolder, script_name):
    """
    Executes a Python script within the context of its subfolder environment.
    
    Args:
        subfolder (str): Path to the subfolder containing the script.
        script_name (str): Name of the Python script to execute.
    """
    # Save the original working directory
    original_cwd = os.getcwd()
    
    try:
        # Change to the subfolder's directory
        os.chdir(subfolder)
        
        # Build the full path to the script
        script_path = os.path.join(os.getcwd(), script_name)
        
        # Open and execute the script within its environment
        with open(script_path, "r") as file:
            exec(file.read(), globals())
    finally:
        # Restore the original working directory
        os.chdir(original_cwd)
        
def main():
    peertube_data = read_csv("DATA/peertube_data2.csv")
    owncast_data = read_csv("DATA/owncast_streamtime.csv")
    merged_data = merge_data(peertube_data, owncast_data)
       
    # Ensure the "DATA" folder exists
    output_folder = "DATA"
    os.makedirs(output_folder, exist_ok=True)
    
    # Write merged data to a CSV file in the "DATA" folder
    #print(merged_data)
    functions = [
        ('post_ranking_week', lambda: post_ranking_to_mastodon(merged_data,7)),
        ('post_ranking_day', lambda: post_ranking_to_mastodon(merged_data,1)),
        ('post_recent_week', lambda: post_recent_streams_to_mastodon(merged_data)),
        ('post_view_ranking_week', lambda: post_view_ranking_to_mastodon(merged_data,7)),
        ('post_view_ranking_day', lambda: post_view_ranking_to_mastodon(merged_data,1)),
        ('post_shortest_week', lambda: post_shortest_stream_to_mastodon(merged_data,7)),
        ('post_shortest_day', lambda: post_shortest_stream_to_mastodon(merged_data,1)),
        ('post_freq_week', lambda: post_stream_frequency_ranking(merged_data,7)),
        ('post_total_time_week', lambda: post_total_stream_time_ranking(merged_data,7)),
        ('post_total_time_day', lambda: post_total_stream_time_ranking(merged_data,1))
    ]
  
    last_functions = get_last_functions(5)
    #print(f"Last used functions: {last_functions}")
    
    
    
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
    run_script_in_environment(subfolder="lemmy_posting_infastructure",script_name="lemmy_post.py")


