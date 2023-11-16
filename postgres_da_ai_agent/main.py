import os
from flask import Flask, jsonify, request
import threading
import dotenv
import argparse

from postgres_da_ai_agent.workers.sqlteam import Request, SqlTeam

dotenv.load_dotenv()

app = Flask(__name__)

# Global variable to store task statuses
task_statuses = {}

def background_task(task_id, taskArgs):
    """
    The function to handle the background task.
    This is where you'll incorporate your existing logic.
    """
    task_statuses[task_id] = {"running": True, "success": False, "message": ""}

    team = SqlTeam(taskArgs)
    team.Start()

    # Update task status upon completion
    task_statuses[task_id]["running"] = False
    task_statuses[task_id]["success"] = True
    task_statuses[task_id]["message"] = "Task completed successfully"

@app.route('/get-sql-query', methods=['POST'])
def get_sql_query():
    api_key = request.headers.get('api-key')
    if not api_key:
        return jsonify({"message": "No API key provided"}), 400
    
    requestBody = Request(data=request.json)

    if not requestBody.user_prompt:
        return jsonify({"message": "No prompt provided"}), 400

    # Generate a unique task ID
    task_id = str(uuid.uuid4())

    # Start the background task
    if len(task_statuses) < 20:
        thread = threading.Thread(target=background_task, args=(task_id, requestBody,))
        thread.start()
        return jsonify({"message": "Task started", "status": "running", "task_id": task_id}), 202
    else:
        return jsonify({"message": "Too many tasks running. Please try again later."}), 429

@app.route('/status', methods=['GET'])
def status():
    return jsonify(task_statuses)


def main():
    app.run(debug=True)


if __name__ == "__main__":
    main()
