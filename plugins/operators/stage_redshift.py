from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.secrets.metastore import MetastoreBackend

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    cmd_s3 = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        FORMAT AS JSON '{}';
    """

    @apply_defaults
    def __init__(self,
                 aws_credentials_id="aws_credentials",
                 do_truncate=False,
                 json_schema="",
                 redshift_conn_id="redshift",
                 s3_bucket="<NOT_SET>",
                 s3_prefix="<NOT_SET>",
                 table="<NOT_SET>",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.aws_credentials_id = aws_credentials_id
        self.do_truncate = do_truncate
        self.json_schema=json_schema,
        self.redshift_conn_id = redshift_conn_id
        self.s3_bucket = s3_bucket
        self.s3_prefix = s3_prefix
        self.table = table

    def execute(self, **kwargs):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        metastoreBackend = MetastoreBackend()
        aws_credentials = metastoreBackend.get_connection(self.aws_credentials_id)
        # aws_hook = (aws_conn_id=self.aws_credentials_id)
        # credentials = aws_hook.get_credentials()

        if self.do_truncate:
            self.log.info(f"Truncate Redshift table {self.table}")
            redshift.run(f"TRUNCATE {self.table}")

        # SQL query parameters
        execution_date = kwargs["logical_date"]
        s3_path = self.s3_prefix.format(execution_date.year, execution_date.month)
        s3_url = f"s3://{self.s3_bucket}/{s3_path}"

        formatted_sql = StageToRedshiftOperator.cmd_s3.format(
            self.table,
            s3_url,
            aws_credentials.access_key,
            aws_credentials.secret_key,
            self.json_schema,
        )

        # Run query
        self.log.info("Copy data from {s3_path} to Redshift table {self.table}")
        self.log.info("COPY dump {formatted_sql}")
        # redshift.run(formatted_sql)
