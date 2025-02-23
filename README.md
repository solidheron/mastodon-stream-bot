Here's an updated version of the README with information declaring the project as open-source and installation instructions for both Linux and Windows:

```markdown
# Mastodon Stream Update Bot

This bot listens to live streams and updates Mastodon posts with relevant descriptions and hashtags. It is designed to interact with PeerTube and Mastodon instances to automate social media updates for live content.

## Open Source License

This project is open-source and released under the MIT License. You are free to modify, distribute, and use the code under the terms of the license. See the [LICENSE](LICENSE) file for more details.

## Features

- Automatically generates Mastodon post descriptions based on the stream's metadata.
- Includes relevant hashtags such as `#livestream` to categorize the posts.
- Integrates with PeerTube to fetch live stream information.
- Supports local Ollama-based AI generation for Mastodon content.

## Prerequisites

Before running the bot, ensure you have the following installed:

- [Python 3.8+](https://www.python.org/downloads/)
- Required Python packages (see below)
- Access to a running [Mastodon](https://mastodon.social/) account and [PeerTube](https://joinpeertube.org/) instance for stream management.
- Ollama with `qwen2.5-coder:7b` model locally set up on your machine.

## Installation

### Linux

1. Clone the repository:
   ```bash
   git clone https://github.com/solidheron/mastodon-stream-bot.git
   cd mastodon-stream-bot
   ```

2. Install Python 3 and required dependencies:
   ```bash
   sudo apt update
   sudo apt install python3 python3-pip python3-venv git
   ```

3. Create a virtual environment (optional but recommended):
   ```bash
   python3 -m venv venv
   source venv/bin/activate
   ```

4. Install the required Python packages:
   ```bash
   pip install -r requirements.txt
   ```

5. Configure your environment:
   - Set up your Mastodon API credentials (`Mastodon_update_bot.py`).
   - Ensure you have the appropriate model loaded in Ollama (`qwen2.5-coder:7b`).

6. Run the script:
   ```bash
   python Mastodon_update_bot.py
   ```

### Windows

1. Clone the repository:
   ```bash
   git clone https://github.com/solidheron/mastodon-stream-bot.git
   cd mastodon-stream-bot
   ```

2. Install Python 3 and required dependencies:
   - Download Python from [python.org](https://www.python.org/downloads/).
   - Ensure that Python is added to your PATH during installation.

3. Create a virtual environment (optional but recommended):
   ```bash
   python -m venv venv
   venv\Scripts\activate
   ```

4. Install the required Python packages:
   ```bash
   pip install -r requirements.txt
   ```

5. Configure your environment:
   - Set up your Mastodon API credentials (`Mastodon_update_bot.py`).
   - Ensure you have the appropriate model loaded in Ollama (`qwen2.5-coder:7b`).

6. Run the script:
   ```bash
   python Mastodon_update_bot.py
   ```

## Configuration

The bot requires the following configurations:
- **Mastodon API Access Token**: Obtain this by registering your bot on your Mastodon instance.
- **Stream Details**: Configure the stream settings to match your PeerTube live stream.
- **Ollama Model Path**: Ensure Ollama is set up properly for generating Mastodon descriptions.

## Example Usage

Once configured, the bot will listen to active streams and automatically post updates to Mastodon with descriptions and relevant hashtags.

## Contributing

Feel free to submit issues, fork the repository, and create pull requests. Contributions are welcome!

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
```

This version includes:
- Declaration that the project is open source under the MIT License.
- Clear installation instructions for both Linux and Windows, ensuring the bot can be installed and run on both platforms.
