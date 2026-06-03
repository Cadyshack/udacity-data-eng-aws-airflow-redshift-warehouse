from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import BaseOperator

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    def __init__(self,
                 *,
                 redshift_conn_id="redshift",
                 tables=None,
                 dq_checks=None,
                 **kwargs):

        super().__init__(**kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.tables = tables or []
        self.dq_checks = dq_checks or []

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        errors = []

        # ---- Check 1: Row count > 0 for each table ----
        for table in self.tables:
            self.log.info(f"Running row count check on table: {table}")
            records = redshift.get_records(f"SELECT COUNT(*) FROM {table}")

            if not records or not records[0] or records[0][0] is None:
                errors.append(
                    f"Data quality check failed: {table} returned no results"
                )
                continue

            num_records = records[0][0]
            if num_records == 0:
                errors.append(
                    f"Data quality check failed: {table} contains 0 rows"
                )
                continue

            self.log.info(
                f"Data quality check passed on {table} with {num_records} records"
            )

        # ---- Check 2: Custom SQL-based checks ----
        for i, check in enumerate(self.dq_checks):
            sql = check.get('check_sql')
            expected = check.get('expected_result')
            description = check.get('description', f"Check #{i+1}")

            self.log.info(f"Running custom DQ check: {description}")
            records = redshift.get_records(sql)

            if not records or not records[0]:
                errors.append(f"{description} - returned no results")
                continue

            actual = records[0][0]
            if actual != expected:
                errors.append(
                    f"{description} - FAILED. Expected: {expected}, Got: {actual}"
                )
            else:
                self.log.info(f"{description} - PASSED")

        # ---- Final: raise if any errors ----
        if errors:
            self.log.error("Data quality check failures:\n" + "\n".join(errors))
            raise ValueError("Data quality checks failed")

        self.log.info("All data quality checks passed!")
