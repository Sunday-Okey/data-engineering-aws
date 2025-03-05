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

# Script generated for node Customer Trusted
CustomerTrusted_node1741128144564 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_trusted", transformation_ctx="CustomerTrusted_node1741128144564")

# Script generated for node Accelerometer Landing
AccelerometerLanding_node1741127900211 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_landing", transformation_ctx="AccelerometerLanding_node1741127900211")

# Script generated for node Join
Join_node1741128194424 = Join.apply(frame1=CustomerTrusted_node1741128144564, frame2=AccelerometerLanding_node1741127900211, keys1=["email"], keys2=["user"], transformation_ctx="Join_node1741128194424")

# Script generated for node Share with Research
SqlQuery2807 = '''
select distinct customername, email, phone, birthday, 
serialnumber, registrationdate, lastupdatedate, 
sharewithresearchasofdate, sharewithpublicasofdate,
sharewithfriendsasofdate from myDataSource
'''
SharewithResearch_node1741128296521 = sparkSqlQuery(glueContext, query = SqlQuery2807, mapping = {"myDataSource":Join_node1741128194424}, transformation_ctx = "SharewithResearch_node1741128296521")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=SharewithResearch_node1741128296521, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1741127877536", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1741128824094 = glueContext.getSink(path="s3://sunnybucket-v2/customer/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1741128824094")
AmazonS3_node1741128824094.setCatalogInfo(catalogDatabase="stedi",catalogTableName="customer_curated")
AmazonS3_node1741128824094.setFormat("json")
AmazonS3_node1741128824094.writeFrame(SharewithResearch_node1741128296521)
job.commit()