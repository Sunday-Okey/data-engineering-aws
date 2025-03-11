from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='redshift_conn_id',
                 data_quality_checks=None,
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.data_quality_checks = data_quality_checks if data_quality_checks is not None else []

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        # Keep track of any failed tests
        failed_tests = []

        self.log.info('Starting Data Quality Checks')

        for check in self.data_quality_checks:
            query = check.get('check_query')
            expected_result = check.get('expected_result')

            self.log.info(f"Executing query: {query}")
            records = redshift.get_records(query)

            if len(records) < 1 or len(records[0]) < 1:
                self.log.error(f"Data quality check failed. Query returned no results: {query}")
                failed_tests.append(query)
                continue

            actual_result = records[0][0]

            if actual_result != expected_result:
                self.log.error(f"Data quality check failed. Query: {query}, Expected: {expected_result}, Got: {actual_result}")
                failed_tests.append(query)
            else:
                self.log.info(f"Data quality check passed. Query: {query}")

        if failed_tests:
            raise ValueError(f"Data quality checks failed for queries: {failed_tests}")
        
        self.log.info('All Data Quality Checks Passed Successfully!')