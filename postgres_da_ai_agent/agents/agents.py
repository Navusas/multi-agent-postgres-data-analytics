import os
from postgres_da_ai_agent.modules.database.postgres import PostgresManager
from postgres_da_ai_agent.modules import llm
from postgres_da_ai_agent.modules import orchestrator
from postgres_da_ai_agent.modules import file
from postgres_da_ai_agent.agents import agent_config
import dotenv
import argparse
import autogen

# ------------ PROMPTS ------------


# create our terminate msg function
def is_termination_msg(content):
    have_content = content.get("content", None) is not None
    if have_content and "APPROVED" in content["content"]:
        return True
    return False


COMPLETION_PROMPT = "If everything looks good, respond with APPROVED. Otherwise, respond with NOT APPROVED"

USER_PROXY_PROMPT = "A human admin. Interact with the Product Manager to discuss the plan. Plan execution needs to be approved by this admin."
DATA_ENGINEER_PROMPT = "A Data Engineer. Generate the SQL based on the requirements provided. Send it to the Sr Data Analyst to be executed. "
SR_DATA_ANALYST_PROMPT = "Sr Data Analyst. You run the SQL query using the run_sql function, send the raw response to the data viz team. You use the run_sql function exclusively."
PRODUCT_MANAGER_PROMPT = (
    "Product Manager. Validate the response to make sure it's correct"
    + COMPLETION_PROMPT
)

# ------------ AGENTS ------------

# create a set of agents with specific roles
# admin user proxy agent - takes in the prompt and manages the group chat
user_proxy = autogen.UserProxyAgent(
    name="Admin",
    system_message=USER_PROXY_PROMPT,
    code_execution_config=False,
    human_input_mode="NEVER",
    is_termination_msg=is_termination_msg,
)

# data engineer agent - generates the sql query
data_engineer = autogen.AssistantAgent(
    name="Engineer",
    llm_config=agent_config.accuracy_config,
    system_message=DATA_ENGINEER_PROMPT,
    code_execution_config=False,
    human_input_mode="NEVER",
    is_termination_msg=is_termination_msg,
)


def build_sr_data_analyst_agent(db: PostgresManager):
    return autogen.AssistantAgent(
        name="Sr_Data_Analyst",
        llm_config=agent_config.run_sql_config,
        system_message=SR_DATA_ANALYST_PROMPT,
        code_execution_config=False,
        human_input_mode="NEVER",
        function_map=agent_config.build_function_map_run_sql(db),
    )


# product manager - validate the response to make sure it's correct
product_manager = autogen.AssistantAgent(
    name="Product_Manager",
    llm_config=agent_config.base_config,
    system_message=PRODUCT_MANAGER_PROMPT,
    code_execution_config=False,
    human_input_mode="NEVER",
    is_termination_msg=is_termination_msg,
)

# ------------ ORCHESTRATION ------------


def build_team_orchestrator(
    team: str, db: PostgresManager
) -> orchestrator.Orchestrator:
    if team == "data_eng":
        return orchestrator.Orchestrator(
            name="::: Data Engineering Team",
            agents=[
                user_proxy,
                data_engineer,
                build_sr_data_analyst_agent(db),
                product_manager,
            ],
        )