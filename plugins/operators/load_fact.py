from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'
    sql_ = """
        INSERT INTO {}
        {};
    """

    @apply_defaults
    def __init__(self,
                 conn_id = "",
                 table = "",
                 sql_query = "",        
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.conn_id = conn_id
        self.sql_query = sql_query
        self.table = table

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id = self.conn_id)
        self.log.info("Delete records from {}".format(self.table))
        redshift.run("DELETE FROM {}".format(self.table))         
        redshift.run(LoadFactOperator.sql_.format(self.table, self.sql_query))

