import subprocess
import time
from datetime import datetime
import os
import platform

counter = 0
special_time_flag = True  # Variable to change from False to True between 4 PM and 5 PM

import sys

def run_script_in_environment(subfolder, script_name):
    """
    Executes a Python script within the context of its subfolder environment.
    
    Args:
        subfolder (str): Path to the subfolder containing the script.
        script_name (str): Name of the Python script to execute.
    """
    original_cwd = os.getcwd()  # Save the original working directory

    try:
        os.chdir(subfolder)  # Change to the script directory
        script_path = os.path.join(os.getcwd(), script_name)

        sys.path.insert(0, os.getcwd())  # Ensure the subfolder is in the import path

        with open(script_path, "r", encoding="utf-8") as file:  # Explicit UTF-8 encoding
            exec(file.read(), globals())

    finally:
        os.chdir(original_cwd)  # Restore original working directory
        sys.path.pop(0)  # Remove added path after execution



# Main loop
def clear_screen():
    if platform.system() == "Windows":
        os.system('cls')
    else:
        os.system('clear')

while True:
    current_time = datetime.now()
    current_hour = current_time.hour
    #run_script_in_environment(subfolder="Scripts_and_data", script_name="leaderboard_lemmy_json_maker.py")
    print(f"Iteration {counter + 1} started at {current_time.strftime('%Y-%m-%d %H:%M:%S')}")

    if counter%8 == 0:  # Runs every hour and at the beginning of the script
        try:
            run_script_in_environment(subfolder="Scripts_and_data", script_name="Peertube_api_scrapper.py")
            run_script_in_environment(subfolder="Scripts_and_data", script_name="owncast_api_scrapper.py")
            #subprocess.run(['python', 'Peertube_api_scrapper.py'], check=True)
            #subprocess.run(['python', 'owncast_api_scrapper.py'], check=True)
        except subprocess.CalledProcessError as e:
            print(f"Error running hourly scripts: {e}")

    if counter%10 == 0:  # Executes at the start of the hour and at the 50-minute mark
        try:
            run_script_in_environment(subfolder="Scripts_and_data", script_name="StreamTracker_update_bot.py")
            #subprocess.run(['python', 'StreamTracker_update_bot.py'], check=True)
        except subprocess.CalledProcessError as e:
            print(f"Error running StreamTracker_update_bot.py: {e}")

    # Check if the current time is between 4 PM (16:00) and 5 PM (17:00)
    if special_time_flag and 16 <= current_hour < 17:
        special_time_flag = False
        run_script_in_environment(subfolder="Scripts_and_data", script_name="leaderboard_post_to_mastodon.py")
        run_script_in_environment(subfolder="Scripts_and_data", script_name="leaderboard_lemmy_json_maker.py")
        #subprocess.run(['python', 'leaderboard_post_to_mastodon.py'], check=True)
        print("Special time! Variable 'special_time_flag' set to Flase.")

    if special_time_flag and 21 <= current_hour < 22:
        special_time_flag = False
        run_script_in_environment(subfolder="Scripts_and_data", script_name="leaderboard_post_to_mastodon.py")
        run_script_in_environment(subfolder="Scripts_and_data", script_name="leaderboard_lemmy_json_maker.py")
        #run_script_in_environment(subfolder="Scripts_and_data/lemmy_posting_infastructure", script_name="lemmy_post.py")
        #subprocess.run(['python', 'leaderboard_post_to_mastodon.py'], check=True)
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
        clear_screen()
        print("Counter reached 20. Resetting to 0.")
        counter = 0
