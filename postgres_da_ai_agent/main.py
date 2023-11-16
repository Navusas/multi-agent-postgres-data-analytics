import os
from flask import Flask, jsonify, request
import threading
import dotenv
import argparse
# Import other necessary modules

dotenv.load_dotenv()

# Flask app setup
app = Flask(__name__)

# Global variable to store task status
task_status = {"running": False, "success": False, "message": ""}

def background_task(prompt):
    """
    The function to handle the background task.
    This is where you'll incorporate your existing logic.
    """
    global task_status
    task_status["running"] = True
    task_status["success"] = False

    # Your task logic goes here
    # For example, handling the prompt with your existing code

    # Update task status upon completion
    task_status["running"] = False
    task_status["success"] = True
    task_status["message"] = "Task completed successfully"

@app.route('/get-sql-query', methods=['POST'])
def get_sql_query():
    data = request.json
    prompt = data.get("prompt", "")

    if not prompt:
        return jsonify({"message": "No prompt provided"}), 400

    # Start the background task
    thread = threading.Thread(target=background_task, args=(prompt,))
    thread.start()

    return jsonify({"message": "Task started", "status": "running"}), 202

@app.route('/status', methods=['GET'])
def status():
    return jsonify(task_status)


def main():
    app.run(debug=True)

if __name__ == "__main__":
    main()
