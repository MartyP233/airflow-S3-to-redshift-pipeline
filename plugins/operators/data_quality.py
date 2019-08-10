from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 # iam_role="",
                 sql="",
                 expected_result="",
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        # self.iam_role = iam_role
        self.aws_credentials_id = aws_credentials_id
        self.sql = sql
        self.expected_result = expected_result

    def execute(self, context):
        self.log.info('Starting Data Quality Check')
        redshift_hook = PostgresHook(self.redshift_conn_id)

        sql_result = redshift_hook.run(self.sql)

        if sql_result == self.expected_result:
            self.log.info('Passed Data quality check')
        else:
            self.log.info(f"""Failed Data quality check, the sql result:
            {sql_result} did not equal the expected result:
             {self.expected_result}""")
