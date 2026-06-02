from airflow.hooks.base import BaseHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator


class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    template_fields = ("s3_key",)

    copy_sql = """
        COPY {table}
        FROM '{s3_path}'
        ACCESS_KEY_ID '{access_key}'
        SECRET_ACCESS_KEY '{secret_key}'
        FORMAT AS JSON '{json_path}'
        REGION 'us-east-1';
    """

    def __init__(self,
                *,
                redshift_conn_id="",
                aws_credentials_id="",
                table="",
                s3_bucket="christian-cadieux",
                s3_key="",
                format_as_json="auto",
                **kwargs):

        super().__init__(**kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.format_as_json = format_as_json

    def execute(self, context):
        aws_connection = BaseHook.get_connection(self.aws_credentials_id)
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        s3_path = f"s3://{self.s3_bucket}/{self.s3_key}"

        self.log.info('Clearing data from {} Redshift table'.format(self.table))
        redshift_hook.run(f"DELETE FROM {self.table}")

        self.log.info('Copying data from S3 to Redshift table {}'.format(self.table))

        formatted_sql = self.copy_sql.format(
            table=self.table,
            s3_path=s3_path,
            access_key=aws_connection.login,
            secret_key=aws_connection.password,
            json_path=self.format_as_json
        )

        redshift_hook.run(formatted_sql)






