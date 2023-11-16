

import pyodbc
import json
from datetime import datetime

class SqlServerManager:
    def __init__(self):
        self.conn = None
        self.cur = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.cur:
            self.cur.close()
        if self.conn:
            self.conn.close()

    def connect_with_url(self, url):
        self.conn = pyodbc.connect(url)
        self.cur = self.conn.cursor()

    def upsert(self, table_name, _dict):
        columns = _dict.keys()
        values = [f"'{v}'" for v in _dict.values()]
        update_stmt = ', '.join([f"{k} = '{v}'" for k, v in _dict.items()])
        upsert_stmt = f"""
            IF EXISTS (SELECT * FROM {table_name} WHERE id = {_dict['id']})
            UPDATE {table_name} SET {update_stmt} WHERE id = {_dict['id']}
            ELSE
            INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({', '.join(values)})
        """
        self.cur.execute(upsert_stmt)
        self.conn.commit()

    def delete(self, table_name, _id):
        delete_stmt = f"DELETE FROM {table_name} WHERE id = {_id}"
        self.cur.execute(delete_stmt)
        self.conn.commit()

    def get(self, table_name, _id):
        select_stmt = f"SELECT * FROM {table_name} WHERE id = {_id}"
        self.cur.execute(select_stmt)
        return self.cur.fetchone()

    def get_all(self, table_name):
        select_all_stmt = f"SELECT * FROM {table_name}"
        self.cur.execute(select_all_stmt)
        return self.cur.fetchall()

    def run_sql(self, sql) -> str:
        self.cur.execute(sql)
        columns = [desc[0] for desc in self.cur.description]
        res = self.cur.fetchall()

        list_of_dicts = [dict(zip(columns, row)) for row in res]

        json_result = json.dumps(list_of_dicts, indent=4, default=self.datetime_handler)

        # dump these results to a file
        with open("results.json", "w") as f:
            f.write(json_result)

        return "Successfully delivered results to json file"

    def datetime_handler(self, obj):
        """
        Handle datetime objects when serializing to JSON.
        """
        if isinstance(obj, datetime):
            return obj.isoformat()
        return str(obj)  # or just return the object unchanged, or another default value

    def get_table_definition(self, table_name):
        get_def_stmt = f"""
        SELECT COLUMN_NAME, DATA_TYPE
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_NAME = '{table_name}'
        """
        self.cur.execute(get_def_stmt)
        rows = self.cur.fetchall()
        columns_info = [{row[0]: row[1]} for row in rows]
        return columns_info
    
    def get_foreign_keys(self, table_name):
        get_fk_stmt = f"""
        SELECT kcu.COLUMN_NAME, ccu.TABLE_NAME AS foreign_table_name, ccu.COLUMN_NAME AS foreign_column_name 
        FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS tc 
        JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS kcu
          ON tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME
        JOIN INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE AS ccu
          ON ccu.CONSTRAINT_NAME = tc.CONSTRAINT_NAME
        WHERE tc.CONSTRAINT_TYPE = 'FOREIGN KEY' AND tc.TABLE_NAME='{table_name}';
        """
        self.cur.execute(get_fk_stmt)
        rows = self.cur.fetchall()
        return [{row[0]: {"foreign_table": row[1], "foreign_column": row[2]}} for row in rows]

    def get_all_table_names(self):
        get_all_tables_stmt = "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'"
        self.cur.execute(get_all_tables_stmt)
        return [row[0] for row in self.cur.fetchall()]

    def get_schema_name(self, table_name):
        get_schema_stmt = f"""
        SELECT TABLE_SCHEMA
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_NAME = '{table_name}' AND TABLE_TYPE = 'BASE TABLE'
        """
        self.cur.execute(get_schema_stmt)
        row = self.cur.fetchone()
        if row:
            return row[0]
        else:
            # Handle the case where the schema name is not found
            return None

    def generate_table_embeddings(self):
        table_names = self.get_all_table_names()
        table_embeddings = {}

        for table_name in table_names:
            # Get columns and foreign keys
            columns_info = self.get_table_definition(table_name)
            foreign_keys_info = self.get_foreign_keys(table_name)

            # Generate hierarchical embedding (Example: JSON structure)
            hierarchical_structure = {
                "table_name": table_name,
                "columns": columns_info,
                "foreign_keys": foreign_keys_info
            }
            embedding = self.generate_embedding_from_structure(hierarchical_structure)
            table_embeddings[table_name] = embedding

        return table_embeddings

    def get_table_definitions_for_prompt(self):
        table_names = self.get_all_table_names()
        definitions = []
        for table_name in table_names:
            definitions.append(self.get_table_definition(table_name))
        return "\n\n".join(definitions)

    def get_table_definitions_for_prompt_MOCK(self):
        return """CREATE TABLE users (
id integer,
created timestamp without time zone,
updated timestamp without time zone,
authed boolean,
plan text,
name text,
email text
);

CREATE TABLE jobs (
id integer,
created timestamp without time zone,
updated timestamp without time zone,
parentuserid integer,
status text,
totaldurationms bigint
    );"""

    def generate_table_definition_string(self, schem_aname, table_name, columns_info, foreign_keys_info):
        column_strings = []

        # Create a list of foreign key column names
        foreign_key_columns = [fk.keys() for fk in foreign_keys_info]

        for column_info in columns_info:
            column_name, data_type = list(column_info.items())[0]
            if column_name in [list(fk)[0] for fk in foreign_keys_info]:
                # Find the corresponding foreign key entry
                foreign_key_entry = next(fk for fk in foreign_keys_info if column_name in fk.keys())
                foreign_table_name = foreign_key_entry[column_name]["foreign_table"]
                foreign_column_name = foreign_key_entry[column_name]["foreign_column"]
                column_string = f"{column_name}: {data_type} -> {foreign_table_name}.{foreign_column_name}"
            else:
                column_string = f"{column_name}: {data_type}"

            column_strings.append(column_string)

        return f"{schem_aname}.{table_name}: {{ {', '.join(column_strings)} }}"
    
    def get_table_definition_map_for_embeddings(self):
        table_names = self.get_all_table_names()
        definitions = {}

        for table_name in table_names:
            # Get columns and foreign keys
            columns_info = self.get_table_definition(table_name)
            foreign_keys_info = self.get_foreign_keys(table_name)

            schema_name = self.get_schema_name(table_name)

            table_definition_string = self.generate_table_definition_string(schema_name, table_name, columns_info, foreign_keys_info)

            # Generate hierarchical embedding (Example: JSON structure)
            definitions[table_name] = table_definition_string

        return definitions
