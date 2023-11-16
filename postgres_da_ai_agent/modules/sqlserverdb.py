

# import pyodbc
# import json
# from datetime import datetime

# class SqlServerManager:
#     def __init__(self):
#         self.conn = None
#         self.cur = None

#     def __enter__(self):
#         return self

#     def __exit__(self, exc_type, exc_val, exc_tb):
#         if self.cur:
#             self.cur.close()
#         if self.conn:
#             self.conn.close()

#     def connect_with_url(self, url):
#         self.conn = pyodbc.connect(url)
#         self.cur = self.conn.cursor()

#     def upsert(self, table_name, _dict):
#         columns = _dict.keys()
#         values = [f"'{v}'" for v in _dict.values()]
#         update_stmt = ', '.join([f"{k} = '{v}'" for k, v in _dict.items()])
#         upsert_stmt = f"""
#             IF EXISTS (SELECT * FROM {table_name} WHERE id = {_dict['id']})
#             UPDATE {table_name} SET {update_stmt} WHERE id = {_dict['id']}
#             ELSE
#             INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({', '.join(values)})
#         """
#         self.cur.execute(upsert_stmt)
#         self.conn.commit()

#     def delete(self, table_name, _id):
#         delete_stmt = f"DELETE FROM {table_name} WHERE id = {_id}"
#         self.cur.execute(delete_stmt)
#         self.conn.commit()

#     def get(self, table_name, _id):
#         select_stmt = f"SELECT * FROM {table_name} WHERE id = {_id}"
#         self.cur.execute(select_stmt)
#         return self.cur.fetchone()

#     def get_all(self, table_name):
#         select_all_stmt = f"SELECT * FROM {table_name}"
#         self.cur.execute(select_all_stmt)
#         return self.cur.fetchall()

#     def run_sql(self, sql) -> str:
#         self.cur.execute(sql)
#         columns = [desc[0] for desc in self.cur.description]
#         res = self.cur.fetchall()

#         list_of_dicts = [dict(zip(columns, row)) for row in res]

#         json_result = json.dumps(list_of_dicts, indent=4, default=self.datetime_handler)

#         # dump these results to a file
#         with open("results.json", "w") as f:
#             f.write(json_result)

#         return "Successfully delivered results to json file"

#     def datetime_handler(self, obj):
#         """
#         Handle datetime objects when serializing to JSON.
#         """
#         if isinstance(obj, datetime):
#             return obj.isoformat()
#         return str(obj)  # or just return the object unchanged, or another default value

#     def get_table_definition(self, table_name):
#         get_def_stmt = f"""
#         SELECT COLUMN_NAME, DATA_TYPE
#         FROM INFORMATION_SCHEMA.COLUMNS
#         WHERE TABLE_NAME = '{table_name}'
#         """
#         self.cur.execute(get_def_stmt)
#         rows = self.cur.fetchall()
#         create_table_stmt = "CREATE TABLE {} (\n".format(table_name)
#         for row in rows:
#             create_table_stmt += "{} {},\n".format(row[0], row[1])
#         create_table_stmt = create_table_stmt.rstrip(",\n") + "\n);"
#         return create_table_stmt

#     def get_all_table_names(self):
#         get_all_tables_stmt = "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'"
#         self.cur.execute(get_all_tables_stmt)
#         return [row[0] for row in self.cur.fetchall()]

#     def get_table_definitions_for_prompt(self):
#         table_names = self.get_all_table_names()
#         definitions = []
#         for table_name in table_names:
#             definitions.append(self.get_table_definition(table_name))
#         return "\n\n".join(definitions)

#     def get_table_definitions_for_prompt_MOCK(self):
#         return """CREATE TABLE users (
# id integer,
# created timestamp without time zone,
# updated timestamp without time zone,
# authed boolean,
# plan text,
# name text,
# email text
# );

# CREATE TABLE jobs (
# id integer,
# created timestamp without time zone,
# updated timestamp without time zone,
# parentuserid integer,
# status text,
# totaldurationms bigint
#     );"""

#     def get_table_definition_map_for_embeddings(self):
#         table_names = self.get_all_table_names()
#         definitions = {}
#         for table_name in table_names:
#             definitions[table_name] = self.get_table_definition(table_name)
#         return definitions
