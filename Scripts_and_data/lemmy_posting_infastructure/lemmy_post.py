import json
import requests
import os

# Step 1: Define your Lemmy instance and user credentials

LEM_INSTANCE = ""  # Replace with your Lemmy instance URL
USERNAME = ""         # Replace with your username
PASSWORD = ""         # Replace with your password

# Step 2: Authenticate and retrieve the JWT token
def get_jwt_token(instance, username, password):
    url = f"{instance}/api/v3/user/login"
    payload = {
        "username_or_email": username,
        "password": password
    }
    response = requests.post(url, json=payload)
    if response.status_code == 200:
        return response.json().get("jwt")
    else:
        raise Exception(f"Failed to authenticate: {response.json()}")

# Step 3: Retrieve the community ID by its name
def get_community_id(instance, community_name, jwt_token):
    url = f"{instance}/api/v3/community"
    headers = {"Authorization": f"Bearer {jwt_token}"}
    params = {"name": community_name}
    response = requests.get(url, headers=headers, params=params)
    if response.status_code == 200:
        return response.json().get("community_view", {}).get("community", {}).get("id")
    else:
        raise Exception(f"Failed to fetch community ID: {response.json()}")

# Step 4: Submit a post to the specified community
def submit_post(instance, jwt_token, community_id, title, body=None, url=None):
    url_endpoint = f"{instance}/api/v3/post"
    headers = {"Authorization": f"Bearer {jwt_token}"}
    payload = {
        "name": title,
        "community_id": community_id,
        "body": body,
        "url": url,
        "nsfw": False
    }
    response = requests.post(url_endpoint, headers=headers, json=payload)
    if response.status_code == 200:
        return response.json()
    else:
        raise Exception(f"Failed to submit post: {response.json()}")

# Step 5: Load post details from a JSON file
def load_post_details(json_file_path):
    if os.path.exists(json_file_path) and os.path.getsize(json_file_path) > 0:
        with open(json_file_path, 'r') as file:
            data = json.load(file)
        return data
    return []

# Step 6: Save post details back to a JSON file
def save_post_details(json_file_path, data):
    with open(json_file_path, 'w') as file:
        json.dump(data, file, indent=2)

# Step 7: Move posted entry to another JSON file
def move_posted_entry(entry, from_file, to_file):
    # Load current data from both files
    from_data = load_post_details(from_file)
    to_data = load_post_details(to_file)

    # Remove the entry from the source file
    from_data = [item for item in from_data if item != entry]

    # Add the entry to the destination file
    to_data.append(entry)

    # Save updated data to both files
    save_post_details(from_file, from_data)
    save_post_details(to_file, to_data)

# Step 8: Check if an entry has already been posted and remove it if found
def remove_if_already_posted(entry, post_details_list, posted_entries_list):
    if entry in posted_entries_list:
        print(f"Removing already posted entry: {entry['title']}")
        post_details_list.remove(entry)  # Remove entry from post_details_list

# Main function to execute the script
if __name__ == "__main__":
    try:
        # Authenticate and get JWT token
        jwt_token = get_jwt_token(LEM_INSTANCE, USERNAME, PASSWORD)
        print(f"Authenticated successfully. JWT Token: {jwt_token}")

        # Load post details and posted entries from JSON files
        JSON_FILE_PATH = "post_details.json"
        POSTED_FILE_PATH = "posted_entries.json"
        post_details_list = load_post_details(JSON_FILE_PATH)
        posted_entries_list = load_post_details(POSTED_FILE_PATH)

        if not post_details_list:
            print("No posts found in post_details.json.")
            exit()

        # Remove already posted entries before processing new posts
        for post_details in list(post_details_list):  # Use a copy of the list for safe iteration
            remove_if_already_posted(post_details, post_details_list, posted_entries_list)

        # Save updated post_details.json after removing duplicates
        save_post_details(JSON_FILE_PATH, post_details_list)

        for post_details in post_details_list:
            try:
                COMMUNITY_NAME = post_details["community_name"]
                POST_TITLE = post_details["title"]
                POST_BODY = post_details.get("body", None)
                POST_URL = post_details.get("url", None)

                # Get the community ID for the specified name
                community_id = get_community_id(LEM_INSTANCE, COMMUNITY_NAME, jwt_token)
                print(f"Community ID for '{COMMUNITY_NAME}': {community_id}")

                # Submit the post
                post_response = submit_post(LEM_INSTANCE, jwt_token, community_id, POST_TITLE, body=POST_BODY, url=POST_URL)
                
                print(f"Post '{POST_TITLE}' submitted successfully!")
                print("Post Details:", post_response)

                # Move the posted entry to posted_entries.json
                move_posted_entry(post_details, JSON_FILE_PATH, POSTED_FILE_PATH)
                print(f"Moved post '{POST_TITLE}' to posted_entries.json")
            
            except Exception as e:
                print(f"Error processing post '{post_details.get('title', 'Unknown')}': {e}")

    except Exception as e:
        print(f"Error: {e}")


