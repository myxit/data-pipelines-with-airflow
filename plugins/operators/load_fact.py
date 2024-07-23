from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 conn_id="redshift",
                 do_truncate=False,
                 sql='',
                 table='',                 
                 *args, **kwargs):
        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.conn_id = conn_id
        self.do_truncate = do_truncate
        self.sql = sql
        self.table = table

    def execute(self, context):
        postgres = PostgresHook(postgres_conn_id=self.conn_id)
        
        if self.do_truncate:
            self.log.info(f"Truncate table {self.table}")
            postgres.run(f"TRUNCATE {self.table}")
        
        self.log.info(f"Load fact table {self.table}")
        # postgres.run(f"INSERT into {self.table} {self.sql}")
