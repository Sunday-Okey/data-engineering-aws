import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue import DynamicFrame

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node Customer Curated
CustomerCurated_node1741151115941 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_curated", transformation_ctx="CustomerCurated_node1741151115941")

# Script generated for node StepTrainer Landing
StepTrainerLanding_node1741151035877 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="step_trainer_landing", transformation_ctx="StepTrainerLanding_node1741151035877")

# Script generated for node Share with Research and has Acc
SqlQuery2668 = '''
select s.*
from stepTrainer s
join customerCurated c
using(serialnumber)
'''
SharewithResearchandhasAcc_node1741151170317 = sparkSqlQuery(glueContext, query = SqlQuery2668, mapping = {"customerCurated":CustomerCurated_node1741151115941, "stepTrainer":StepTrainerLanding_node1741151035877}, transformation_ctx = "SharewithResearchandhasAcc_node1741151170317")

# Script generated for node StepTrainer Trusted
EvaluateDataQuality().process_rows(frame=SharewithResearchandhasAcc_node1741151170317, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1741151653459", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
StepTrainerTrusted_node1741151806841 = glueContext.getSink(path="s3://sunnybucket-v2/step_trainer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="StepTrainerTrusted_node1741151806841")
StepTrainerTrusted_node1741151806841.setCatalogInfo(catalogDatabase="stedi",catalogTableName="step_trainer_trusted")
StepTrainerTrusted_node1741151806841.setFormat("json")
StepTrainerTrusted_node1741151806841.writeFrame(SharewithResearchandhasAcc_node1741151170317)
job.commit()