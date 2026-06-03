from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import BaseOperator

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    insert_sql_template = """
        INSERT INTO {table}
        {sql_query}
    """

    def __init__(self,
                 redshift_conn_id = "redshift",
                 table = "",
                 sql_query = "",
                 append_data = False,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql_query = sql_query
        self.append_data = append_data 
        

    def execute(self, context):
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        if not self.append_data:
            self.log.info(f"Clearing data from dimension table {self.table}")
            redshift_hook.run(f"TRUNCATE TABLE {self.table}")

        self.log.info(f"Loading data into dimension table {self.table}")
        formatted_sql = self.insert_sql_template.format(
            table=self.table,
            sql_query=self.sql_query
        )
        redshift_hook.run(formatted_sql)

        self.log.info(f"LoadDimensionOperator completed for table {self.table}")