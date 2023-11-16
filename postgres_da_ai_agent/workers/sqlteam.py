from postgres_da_ai_agent.modules.database.postgres import PostgresManager
from postgres_da_ai_agent.agents import agents
from postgres_da_ai_agent.modules import embeddings
from postgres_da_ai_agent.modules import llm


POSTGRES_TABLE_DEFINITIONS_CAP_REF = "TABLE_DEFINITIONS"
RESPONSE_FORMAT_CAP_REF = "RESPONSE_FORMAT"
SQL_DELIMITER = "---------"

class Request:
    def __init__(self, data):
        db = data['database']
        request = data['request']
        self.db_type=db.get("type")
        self.db_version=db.get("version")
        self.db_conn_string=db.get("connection_string")
        self.user_prompt=request.get("prompt")
        self.model=request.get("model")

class SqlTeam:
    def __init__(self, requestDetails: Request):
        self.requestDetails = requestDetails

    def Start(self):
        prompt = f"Fulfill this database query: {self.requestDetails.user_prompt}. "

        with PostgresManager() as db:
            print(f"Connecting to database: {self.requestDetails.db_conn_string}")
            db.connect_with_url(self.requestDetails.db_conn_string)

            map_table_name_to_table_def = db.get_table_definition_map_for_embeddings()

            database_embedder = embeddings.DatabaseEmbedder()

            for name, table_def in map_table_name_to_table_def.items():
                database_embedder.add_table(name, table_def)

            similar_tables = database_embedder.get_similar_tables(self.requestDetails.user_prompt, n=5)

            table_definitions = database_embedder.get_table_definitions_from_names(
                similar_tables
            )

            prompt = llm.add_cap_ref(
                prompt,
                f"Use these {POSTGRES_TABLE_DEFINITIONS_CAP_REF} to satisfy the database query.",
                POSTGRES_TABLE_DEFINITIONS_CAP_REF,
                table_definitions,
            )

            data_eng_orchestrator = agents.build_team_orchestrator("data_eng", db)

            success, data_eng_messages = data_eng_orchestrator.sequential_conversation(
                prompt
            )

            # ---------------------------------------------

            data_eng_cost, data_eng_tokens = data_eng_orchestrator.get_cost_and_tokens()

            print(f"Data Eng Cost: {data_eng_cost}, tokens: {data_eng_tokens}")

            print(f"💰📊🤖 Organization Cost: {data_eng_cost}, tokens: {data_eng_tokens}")

            sql_message = data_eng_messages[-3]
            print(sql_message)
            if isinstance(sql_message, dict):
                sql_query = sql_message.get('function_call', {}).get('arguments', {}).get('sql')
            elif isinstance(sql_message, str):
                print(f"sql_message is a string: {sql_message}")
                sql_query = sql_message
            else:
                print(f"Unexpected sql_message type: {type(sql_message)}. Expected dict or string.")
                sql_query = None
            return sql_query
