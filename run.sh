#!/bin/bash
# 1. Start the Flask Web App in the background
python webapp.py &

# 2. Start the main Telegram Bot
python bot.py
