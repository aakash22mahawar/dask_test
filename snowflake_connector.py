import snowflake.connector

class SnowflakeConnector:
    def __init__(self):
        # Directly embed Snowflake configuration
        snowflake_config = {
            'user': 'user',
            'password': 'password',
            'account': 'account',
            'warehouse': 'warehouse',
            'database': 'database',
            'autocommit': True,
            'schema': 'schema'
        }

        self.connection = snowflake.connector.connect(**snowflake_config)
        self.cursor = self.connection.cursor()

    def insert_data(self, items):
        try:
            # Adjust the SQL query based on your Snowflake table structure
            query = """
                INSERT INTO dask_test ("question")
                VALUES (%s);
            """
            # Assuming 'items' is a list of dictionaries with a 'question' key
            data = [(item['question'],) for item in items]
            self.cursor.executemany(query, data)
        except Exception as e:
            print(f"Error inserting into Snowflake: {e}")

    def close_connection(self):
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
