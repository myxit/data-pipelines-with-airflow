from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 conn_id='redshift',
                 tables=[],
                 *args, **kwargs):
        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.conn_id=conn_id
        self.tables=tables


    def execute(self, context):
        postgres=PostgresHook(postgres_conn_id=self.conn_id)
        table=self.tables[0]
        records=postgres.get_records(f"SELECT count(*) FROM {table}")
        if len(records) < 1 or len(records[0] < 1):
            raise ValueError(f"Data quality check failed. {table} contained 0 rows.")
        self.log.info(f"Data quality on table {table} check passed with {records[0][0]} records") 