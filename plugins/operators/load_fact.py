from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = '',
                 table_name = '',
                 sql_statement = '',
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table_name = table_name
        self.sql_statement = sql_statement

        
    def execute(self, context):
        self.log.info('what is this poop poop LoadFactOperator not implemented yet')
              
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)        
        
        full_sql_statement = f"INSERT INTO {self.table_name} {self.sql_statement}"                
        redshift.run(full_sql_statement)
        
        self.log.info(f"Fact table {self.table_name} load finished")
        