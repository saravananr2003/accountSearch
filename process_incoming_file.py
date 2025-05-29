import datetime
import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, lit, concat_ws
from pyspark.sql.types import StringType

import genmodule
from genmodule import read_json_config

# Read config and setup environment
config = genmodule.read_config()

os.environ['JAVA_TOOL_OPTIONS'] = '-Djavax.security.auth.useSubjectCredsOnly=true'
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

# Initialize Spark
spark = SparkSession.builder \
	.appName("AddressStandardization") \
	.master("local[*]") \
	.config("spark.driver.host", "localhost") \
	.config("spark.sql.debug.maxToStringFields", 200) \
	.getOrCreate()

# Load configs
BASE_FOLDER = config['FOLDER']['BASE_FOLDER']
FOLDERS = {
	'incoming': str(os.path.join(BASE_FOLDER, config['FOLDER']['INCOMING_FOLDER'])),
	'process': str(os.path.join(BASE_FOLDER, config['FOLDER']['PROCESS_FOLDER'])),
	'output': str(os.path.join(BASE_FOLDER, config['FOLDER']['OUTPUT_FOLDER'])),
	'temp': str(os.path.join(BASE_FOLDER, config['FOLDER']['TEMP_FOLDER']))
}

genmodule.logger("INFO", f"Spark Session Created with Application ID: {spark.sparkContext.applicationId}")

# Load output fields from JSON config
output_fields = read_json_config("BU_ADD_0415.json")["output_fields"]
output_data = genmodule.convert_output_fields_to_dict(output_fields)


def process_incoming_data(pv_filename):
	literal = genmodule.convert_output_fields_to_dict(read_json_config("audit_attributes.json")["audit_fields"], 'name',
													  'value')

	src_file = os.path.join(FOLDERS['incoming'], pv_filename)
	dst_file = os.path.join(FOLDERS['process'], pv_filename)

	genmodule.logger("INFO", f"Source File: {src_file}, Target File: {dst_file}")
	ld_current_date = datetime.datetime.now().strftime("%Y-%b-%d %H:%M:%S")
	ld_current_ts = datetime.datetime.now()
	ls_user_name = os.getlogin()

	ldf_incoming = spark.read.csv(src_file, header=True, inferSchema=True)
	for rec in literal:
		pv_key = str(rec[0])
		pv_value = (str(rec[1]).replace('{INPUT_FILE_NAME}', pv_filename)
					.replace('{CURRENT_DATE}', ld_current_date)
					.replace('{CURRENT_USER}', ls_user_name))
		ldf_incoming = ldf_incoming.withColumn(pv_key, lit(pv_value))

	ldf_process = (ldf_incoming.withColumn('BU_REC_ID', udf_uuid()).fillna(''))
	ldf_processed = ldf_process.rdd.mapPartitions(genmodule.standardize_records)
	df_process = spark.createDataFrame(ldf_processed)
	genmodule.logger("INFO", "Records with Rec ID, Address, Email, and Phone Standardization ...")
	df_process.show(truncate=False)
	df_processed = df_process.select([col(c[0]).alias(c[1]) for c in output_data])
	df_processed.show(truncate=False)
	genmodule.write_df_2_file(df_processed, dst_file, "CSV")


def process_tamr(pv_filename):
	src_file = os.path.join(FOLDERS['process'], pv_filename)
	dst_file = os.path.join(FOLDERS['output'], pv_filename)
	genmodule.logger("INFO", f"Starting to process Source File: {src_file} to Target File: {dst_file}")
	ldf_incoming = spark.read.csv(src_file, header=True, inferSchema=True)
	ldf_process = ldf_incoming.withColumn("STD_ORG_ZIP9",
										  concat_ws("-", col("STD_ORG_ZIP_CODE"), col("STD_ORG_ZIP_SUPP"))) \
		.fillna('') \
		.repartition(10)

	genmodule.logger("INFO", "Records Before TAMR Process ...")
	ldf_process.sort("SRC_ID").show(truncate=False)

	ldf_processed = ldf_process.rdd.mapPartitions(genmodule.call_tamr_api)
	df_processed = spark.createDataFrame(ldf_processed).repartition(1).sort("SRC_ID")

	genmodule.logger("INFO", "Records after TAMR Process ...")
	df_processed.show(truncate=False)
	genmodule.write_df_2_file(df_processed, dst_file, "CSV")
	genmodule.logger("INFO", "Records written to file after TAMR Process ...")

	genmodule.logger("INFO", "Start to write data into snowflake table...")
	genmodule.write_df_2_file(df_processed, dst_file, "SNOWFLAKE")
	genmodule.logger("INFO", "Data written to Snowflake table ...")


json_filename = "BU_ADD_0415.json"
data_filename = read_json_config(json_filename)["file_config"]["file_name"]

udf_uuid = udf(genmodule.generate_guid, StringType())
ID = config['DEFAULT']['ID']
file_name = data_filename + "." + ID

process_incoming_data(file_name)
genmodule.logger("INFO", "Standardization Process Completed...")

genmodule.logger("INFO", "Starting to process the file with Tamr LLM API...")
process_tamr(file_name)
genmodule.logger("INFO", "Tamr LLM API Process Completed...")
spark.stop()
