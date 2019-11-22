from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from boto.s3.connection import S3Connection

class StageToRedshiftOperator(BaseOperator):
    template_fields = ('s3_key', )
    
    ui_color = '#358140'
    
    @apply_defaults
    def __init__(self,
                 aws_credentials = '',
                 redshift_conn_id = '',
                 s3_bucket = "",
                 s3_key = '',
                 table_name = '',
                 delimiter = '',
                 headers = '',
                 quote_char = '',
                 file_type = '',
                 *args, **kwargs):
        
        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.aws_credentials = aws_credentials
        self.redshift_conn_id = redshift_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.table_name = table_name
        self.delimiter = delimiter
        self.headers = headers
        self.quote_char = quote_char
        self.file_type = file_type


    def execute(self, context):
        aws_hook = AwsHook(self.aws_credentials)
        credentials = aws_hook.get_credentials()
        print(credentials)
        print(credentials.access_key)
        print(credentials.secret_key)       
        
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        print(redshift)

        s3_path = f"s3://{self.s3_bucket}"
        
        copy_statement = f""" 
        COPY {self.table_name}
        FROM '{s3_path}' 
        ACCESS_KEY_ID '{credentials.access_key}' 
        SECRET_ACCESS_KEY '{credentials.secret_key}'"""

        print(copy_statement)

        if self.file_type == 'csv':
            file_statement = f"""
                IGNOREHEADER {self.headers}
                DELIMITER '{self.delimiter}'                
                csv quote as '{self.quote_char}';"""            
        
        if self.file_type == 'json':
            file_statement = "json 'auto';"

        print(file_statement)

        full_copy_statement = f"{copy_statement} {file_statement}"
        
        print(full_copy_statement)       
                
        redshift.run(full_copy_statement)
