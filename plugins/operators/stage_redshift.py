from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    template_fields = ("s3_key",)
    
    copy_sql = """
    COPY {}
    FROM {}
    credentials 'aws_iam_role={}'
    region 'us-west-2'
    FORMAT AS JSON {}
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 s3_bucket="",
                 s3_key="",                 
                 iam_role="",
                 json_format="",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.s3_bucket = s3_bucket
        self.iam_role = iam_role
        self.aws_credentials_id = aws_credentials_id
        self.json_format = json_format
        self.s3_key = s3_key

    def execute(self, context):
        # aws_hook = AwsHook(self.aws_credentials_id)
        # credentials = aws_hook.get_credentials()
        redshift_hook = PostgresHook(self.redshift_conn_id)

        self.log.info("Clearing data from destination Redshift table")
        redshift_hook.run("DELETE FROM {}".format(self.table))

        self.log.info("Copying data from S3 to Redshift")
        if self.s3_key:
            rendered_key = self.s3_key.format(**context)
            s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
        else:
            s3_path = self.s3_bucket

        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            self.table,
            s3_path,
            self.iam_role,
            self.json_format
        )
        redshift_hook.run(formatted_sql)