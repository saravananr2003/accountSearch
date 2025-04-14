import uuid
import snowflake.connector
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, udf
import os
import sys

from pyspark.sql.types import StringType

# Remove the security manager setting that's causing problems
os.environ['JAVA_TOOL_OPTIONS'] = '-Djavax.security.auth.useSubjectCredsOnly=true'

# Standard PySpark environment setup
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

# Create SparkSession without security manager flags
spark = SparkSession.builder \
    .appName("MyApp") \
    .master("local[*]") \
    .config("spark.driver.host", "localhost") \
    .config("spark.sql.debug.maxToStringFields",200) \
    .getOrCreate()

ID = "2679af12-d49a-4bcc-8a7a-ce32eb22b9d5"
BASE_FOLDER = '/Users/ramsa005/Desktop/Staples/Projects/Git/accountSearch/datafiles/'
INCOMING_FOLDER = BASE_FOLDER + "incoming/"  # using a temporary directory
TEMP_FOLDER = BASE_FOLDER + "temp/"
OUTPUT_FOLDER = BASE_FOLDER + "output/"  # using a temporary directory
PROCESS_FOLDER = BASE_FOLDER + "process/"

# Replace these variables with your Snowflake credentials and config
SNOWFLAKE_USER = 'YOUR_USERNAME'
SNOWFLAKE_PASSWORD = 'YOUR_PASSWORD'
SNOWFLAKE_ACCOUNT = 'YOUR_ACCOUNT'
SNOWFLAKE_WAREHOUSE = 'YOUR_WAREHOUSE'
SNOWFLAKE_DATABASE = 'YOUR_DATABASE'
SNOWFLAKE_SCHEMA = 'YOUR_SCHEMA'
SNOWFLAKE_STAGE = 'YOUR_STAGE'  # an internal or external stage name (e.g., '@my_stage')
TARGET_TABLE = 'YOUR_TARGET_TABLE'

def generate_guid():
    return str(uuid.uuid4())

def process_file(pv_ID):
    src_file = INCOMING_FOLDER + "BU_Bulk_Match_Input_2.0_Test.csv." + pv_ID
    dst_file = PROCESS_FOLDER + "BU_Bulk_Match_Input_2.0_Test.csv." + pv_ID
    print("Source File:", src_file)
    print("Target File:", dst_file)
    df = spark.read.csv(src_file,header=True, inferSchema=True)
    df1 = df.withColumn("BU_REC_ID", uuid_udf())
    df1.show(20)
    df1.write.option("header",True).csv(dst_file, mode="overwrite")
    spark.stop()


# def load_file_to_snowflake(file_path, file_name):
# 	"""Uploads a file to a Snowflake stage and then loads its contents to a target table."""
# 	conn = get_snowflake_connection()
# 	try:
# 		cs = conn.cursor()
# 		try:
# 			# 1. Upload the file to a Snowflake stage using PUT command.
# 			# The file path is local to the server where the Python process is running.
# 			put_command = f"PUT file://{file_path} {SNOWFLAKE_STAGE} AUTO_COMPRESS=TRUE"
# 			cs.execute(put_command)
# 			print("File uploaded to stage successfully.")
#
# 			# 2. Load the staged file into the target table using COPY INTO.
# 			# Adjust file format options based on your CSV structure, headers, delimiters etc.
# 			copy_command = f"""
#             COPY INTO {TARGET_TABLE}
#             FROM {SNOWFLAKE_STAGE}/{file_name}.gz
#             FILE_FORMAT = (TYPE = 'CSV', FIELD_OPTIONALLY_ENCLOSED_BY = '\"', SKIP_HEADER = 1)
#             """
# 			cs.execute(copy_command)
# 			print("Data loaded into table successfully.")
# 		finally:
# 			cs.close()
# 	finally:
# 		conn.close()

#
# def get_snowflake_connection():
# 	"""Return a Snowflake connection object."""
# 	conn = snowflake.connector.connect(
# 		user=SNOWFLAKE_USER,
# 		password=SNOWFLAKE_PASSWORD,
# 		account=SNOWFLAKE_ACCOUNT,
# 		warehouse=SNOWFLAKE_WAREHOUSE,
# 		database=SNOWFLAKE_DATABASE,
# 		schema=SNOWFLAKE_SCHEMA
# 	)
# 	return conn
# #
# #
# # def read_properties(filepath):
# #     Reads a properties file and returns a dictionary of key-value pairs.
# #     Args   : filepath (str): The path to the properties file.
# #     Returns: dict: A dictionary where the keys and values are strings from the file.
# #     properties = {}
# #     try:
# #         with open(filepath, 'r') as file:
# #             for line in file:
# #                 line = line.strip()
# #                 if not line or line.startswith('#'):
# #                     continue
# #                 # Split the line at the first occurrence of '='
# #                 if '=' in line:
# #                     key, value = line.split('=', 1)
# #                     properties[key.strip()] = value.strip()
# #     except Exception as e:
# #         print(f"Error reading properties file: {e}")
# #     return properties

uuid_udf = udf(generate_guid, StringType())

process_file(ID)

# spark.stop()
