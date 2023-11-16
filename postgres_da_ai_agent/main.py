import os
from flask import Flask, jsonify, request
import threading
import dotenv
import argparse

from postgres_da_ai_agent.workers.sqlteam import Request, SqlTeam

dotenv.load_dotenv()

app = Flask(__name__)

# Global variable to store task status
task_status = {"running": False, "success": False, "message": ""}

def background_task(taskArgs):
    """
    The function to handle the background task.
    This is where you'll incorporate your existing logic.
    """
    global task_status
    task_status["running"] = True
    task_status["success"] = False

    team = SqlTeam(taskArgs)
    team.Start()

    # Update task status upon completion
    task_status["running"] = False
    task_status["success"] = True
    task_status["message"] = "Task completed successfully"

@app.route('/get-sql-query', methods=['POST'])
def get_sql_query():
    api_key = request.headers.get('api-key')
    if not api_key:
        return jsonify({"message": "No API key provided"}), 400
    
    requestBody = Request(data=request.json)

    if not requestBody.user_prompt:
        return jsonify({"message": "No prompt provided"}), 400

    # Start the background task
    thread = threading.Thread(target=background_task, args=(requestBody,))
    thread.start()

    return jsonify({"message": "Task started", "status": "running"}), 202

@app.route('/status', methods=['GET'])
def status():
    return jsonify(task_status)


def main():
    app.run(debug=True)


if __name__ == "__main__":
    main()
