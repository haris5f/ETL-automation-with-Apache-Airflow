from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 conn_id="",
                 dq_checks=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.conn_id = conn_id
        self.dq_checks = dq_checks
    def execute(self, context):
        self.log.info('DataQualityOperator not implemented yet')
        redshift_hook = PostgresHook(self.conn_id)
        count =0
        for query in self.dq_checks:
            sql_q = query['check_sql']
            expected_result = query['expected_result']
            records = redshift_hook.get_records(sql_q)[0]
            if expected_result != records[0]:
                count = count +  1

        if count > 0:
            self.log.info('Data Checks Failed')
        else:
            self.log.info("Data Checks Passed")
            
            
            
            
