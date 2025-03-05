import subprocess
import time
from datetime import datetime

counter = 0
special_time_flag = True  # Variable to change from False to True between 4 PM and 5 PM

while True:
    current_time = datetime.now()
    current_hour = current_time.hour

    print(f"Iteration {counter + 1} started at {current_time.strftime('%Y-%m-%d %H:%M:%S')}")

    if counter == 0 or counter == 12:  # Runs every hour and at the beginning of the script
        try:
            subprocess.run(['python', 'Peertube_api_scrapper.py'], check=True)
            subprocess.run(['python', 'owncast_api_scrapper.py'], check=True)
        except subprocess.CalledProcessError as e:
            print(f"Error running hourly scripts: {e}")

    if counter == 0 or counter == 10:  # Executes at the start of the hour and at the 50-minute mark
        try:
            subprocess.run(['python', 'StreamTracker_update_bot.py'], check=True)
        except subprocess.CalledProcessError as e:
            print(f"Error running StreamTracker_update_bot.py: {e}")

    # Check if the current time is between 4 PM (16:00) and 5 PM (17:00)
    if special_time_flag and 16 <= current_hour < 17:
        special_time_flag = False
        subprocess.run(['python', 'leaderboard_post_to_mastodon.py'], check=True)
        print("Special time! Variable 'special_time_flag' set to Flase.")

    if special_time_flag and 21 <= current_hour < 22:
        special_time_flag = False
        subprocess.run(['python', 'leaderboard_post_to_mastodon.py'], check=True)
        print("Special time! Variable 'special_time_flag' set to False.")
    
    # special_time_flag reset
    if  17 <= current_hour < 21:
        special_time_flag = True
        #print("Special time! Variable 'special_time_flag' set to True.")
    # special_time_flag reset
    if  22 <= current_hour <=24:
        special_time_flag = True
        #print("Special time! Variable 'special_time_flag' set to True.")


    time.sleep(60 * 5)  # Sleep for 5 minutes (300 seconds)
    counter += 1

    if counter == 20:
        print("Counter reached 20. Resetting to 0.")
        counter = 0
