from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'
    
    sql_dim = """
        INSERT INTO {} {};
        """
   
    @apply_defaults
    def __init__(self,
                 conn_id = "",
                 table = "",
                 sql_query = "", 
                 truncate = "",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.conn_id = conn_id
        self.sql_query = sql_query
        self.truncate = truncate

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id = self.conn_id)
        self.log.info('LoadDimensionOperator not implemented yet')
        if self.truncate:
            redshift.run(f"TRUNCATE TABLE {self.table}")
        redshift.run(LoadDimensionOperator.sql_dim.format(self.table, self.sql_query))
