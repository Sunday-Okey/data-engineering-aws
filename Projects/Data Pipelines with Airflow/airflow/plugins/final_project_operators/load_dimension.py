from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 redshift_conn_id="",
                 table="",
                 truncate_table=False,
                 load_dim_sql="",
                 * args, **kwargs):
        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.truncate_table = truncate_table
        self.load_dim_sql = load_dim_sql

    def execute(self, context):
        # self.log.info('LoadDimensionOperator not implemented yet')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info(f'Loading dimension table {self.table}')

        if self.truncate_table:
            self.log.info("Truncating table {}".format(self.table))
            redshift.run("TRUNCATE TABLE {}".format(self.table))

        self.log.info("Inserting data from stating table into dimension table")

        try:
            redshift.run("INSERT INTO {} {}".format(
                self.table, self.load_dim_sql))
        except Exception as e:
            raise self.log.info.error(
                f"Failed to insert data into {self.table}: {str(e)}")
        else:
            self.log.info(
                f"Inserting data into {self.table} completed successfully!")
