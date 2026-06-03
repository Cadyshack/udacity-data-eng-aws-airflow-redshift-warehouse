from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import BaseOperator
from helpers.sql_queries import SqlQueries

class LoadFactOperator(BaseOperator):
    ui_color = '#F98866'

    insert_sql_template = """
        INSERT INTO {table}
        {sql_query}
    """
    def __init__(self,
                 *,
                redshift_conn_id = "redshift",
                table = "songplays",
                sql_query = SqlQueries.songplay_table_insert,
                append_data = False,
                **kwargs):

        super().__init__(**kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql_query = sql_query
        self.append_data = append_data


    def execute(self, context):
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        if not self.append_data:
            self.log.info(f"Clearing data from fact table {self.table}")
            redshift_hook.run(f"TRUNCATE TABLE {self.table}")

        self.log.info(f"Loading data into fact table {self.table}")
        formatted_sql = self.insert_sql_template.format(
            table=self.table,
            sql_query=self.sql_query
        )
        redshift_hook.run(formatted_sql)

        self.log.info(f"LoadFactOperator completed for table {self.table}")