from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 redshift_conn_id="",
                 table="",
                 load_fact_sql="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.load_fact_sql = load_fact_sql

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info("Inserting data from stating table into dimension table")

        try:
            redshift.run("INSERT INTO {} {}".format(
                self.table, self.load_fact_sql))
        except Exception as e:
            raise self.log.info.error(
                f"Failed to insert data into {self.table}: {str(e)}")
        else:
            self.log.info(
                f"Inserting data into {self.table} completed successfully!")
