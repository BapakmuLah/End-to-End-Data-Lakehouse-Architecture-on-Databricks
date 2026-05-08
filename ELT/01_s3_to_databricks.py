import dlt 
from pyspark.sql.functions import *

# CREATE CUSTOMERS TABLE FROM S3 BUCKET
@dlt.table(name = 'customers')
def customer_data():

    # INCREMENTAL BATCH
    data = spark.readStream.format(source= 'cloudFiles') \
                           .option(key = 'cloudFiles.format', value = 'csv') \
                           .option(key = 'header', value = 'true') \
                           .option(key = "cloudFiles.inferColumnTypes", value = 'true') \
                           .load(path = "s3://claim-insurance-dataset/raw-data/customers/") 
    return data

# CREATE POLICY TABLE FROM S3 BUCKET
@dlt.table(name = 'policies')
def policy_data():
    data = spark.readStream.format(source = 'cloudFiles') \
                           .option(key = 'cloudFiles.format', value = 'csv') \
                           .option(key = 'header', value = 'true') \
                           .option(key = 'cloudFiles.inferColumnTypes', value = 'true') \
                           .load(path = "s3://claim-insurance-dataset/raw-data/policies/")

    return data

# CREATE CLAIMS TABLE FROM S3 BUCKET
@dlt.table(name = 'claims')
def claim_data():
    data = spark.readStream.format(source = 'cloudFiles') \
                           .option(key = 'cloudFiles.format', value = 'csv') \
                           .option(key = 'header', value = 'true') \
                           .option(key = 'cloudFiles.inferColumnTypes', value = 'true') \
                           .load(path = "s3://claim-insurance-dataset/raw-data/claims/")
    return data

# CREATE TELEMATICS TABLE FROM S3 BUCKET
@dlt.table(name = "telematics")
def telematics_data():
    data = spark.readStream.format(source = 'cloudFiles') \
                           .option(key = 'cloudFiles.format', value = 'parquet') \
                           .option(key = 'cloudFiles.inferColumnTypes', value = 'true') \
                           .load(path = "s3://claim-insurance-dataset/raw-data/telematics/")
    return data

# UPLOAD TEST IMAGES INTO BRONZE LAYER
@dlt.table(name="test_images",
           comment="Raw accident training image ingested from S3", 
           table_properties={"quality": "bronze"}
           )
def test_images():
    return (spark.readStream.format("cloudFiles") 
                            .option("cloudFiles.format", "BINARYFILE") 
                            .load(f"/Volumes/claim_insurance/object_storage/test_images"))

# UPLOAD TRAINING IMAGES INTO BRONZE LAYER
@dlt.table(name="training_images",
           comment="Raw accident training image ingested from S3", 
           table_properties={"quality": "bronze"}
           )
def train_images():
    return (spark.readStream.format("cloudFiles") 
                            .option("cloudFiles.format", "BINARYFILE") 
                            .load(f"/Volumes/claim_insurance/object_storage/training_images")) 

# UPLOAD METADATA IMAGE INTO BRONZE LAYER
@dlt.table(name = "metadata",
           comment = "Raw Metadata image ingested from S3",
           table_properties={"quality": "bronze"}
           )
def metadata_images():
    return (spark.readStream.format("cloudFiles") 
                            .option("cloudFiles.format", "csv")
                            .load(f"/Volumes/claim_insurance/object_storage/metadata"))

