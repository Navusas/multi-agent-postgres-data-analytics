import os
from postgres_da_ai_agent.modules.database.postgres import PostgresManager
from postgres_da_ai_agent.modules import llm
from postgres_da_ai_agent.modules import orchestrator
from postgres_da_ai_agent.modules import file
import dotenv
import argparse
import autogen


cheap_config_list = autogen.config_list_from_models(
    model_list=["gpt-3.5-turbo"]
)

cheap_large_config_list = autogen.config_list_from_models(
    model_list=["gpt-3.5-turbo-16k"]
)

costly_fast_config_list = autogen.config_list_from_models(
    model_list=["gpt-4-1106-preview"]
)

accuracy_config = {
    "use_cache": False,
    "temperature": 0,
    "config_list": costly_fast_config_list,
    "request_timeout": 120,
}

# build the gpt_configuration object
# Base Configuration
base_config = {
    "use_cache": False,
    "temperature": 0,
    "config_list": cheap_large_config_list,
    "request_timeout": 120,
}

# Configuration with "run_sql"
run_sql_config = {
    **base_config,  # Inherit base configuration
    "functions": [
        {
            "name": "run_sql",
            "description": "Run a SQL query against the postgres database",
            "parameters": {
                "type": "object",
                "properties": {
                    "sql": {
                        "type": "string",
                        "description": "The SQL query to run",
                    }
                },
                "required": ["sql"],
            },
        }
    ],
}

def create_func_map(name: str, func: callable):
    return {
        name: func,
    }

def build_function_map_run_sql(db: PostgresManager):
    return create_func_map("run_sql", db.run_sql)
