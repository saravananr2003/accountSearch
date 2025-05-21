import datetime
import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, lit, concat_ws, to_timestamp, date_format
from pyspark.sql.types import StringType, TimestampType

import genmodule

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

# Replace these variables with your Snowflake credentials and config

output_data = [
	['BU_REC_ID', 'BU_REC_ID'],
	['SRC_ID', 'SRC_ID'],
	['SRC_TP_CD', 'SRC_TP_CD'],
	['SRC_INPUT_FILE_NAME', 'SRC_INPUT_FILE_NAME'],
	['PER_PREFIX', 'SRC_PER_PREFIX'],
	['PER_FIRST_NAME', 'SRC_PER_FIRST_NAME'],
	['PER_MIDDLE_NAME', 'SRC_PER_MIDDLE_NAME'],
	['PER_LAST_NAME', 'SRC_PER_LAST_NAME'],
	['PER_SUFFIX', 'SRC_PER_SUFFIX'],
	['PER_ADDRESS_LINE_1', 'SRC_PER_ADDRESS_LINE_1'],
	['PER_ADDRESS_LINE_2', 'SRC_PER_ADDRESS_LINE_2'],
	['PER_ADDRESS_LINE_3', 'SRC_PER_ADDRESS_LINE_3'],
	['PER_CITY', 'SRC_PER_CITY'],
	['PER_STATE', 'SRC_PER_STATE'],
	['PER_ZIP_CODE', 'SRC_PER_ZIP_CODE'],
	['PER_ZIP_SUPP', 'SRC_PER_ZIP_SUPP'],
	['PER_COUNTRY', 'SRC_PER_COUNTRY'],
	['PER_PHONE_NUMBER', 'SRC_PER_PHONE_NUMBER'],
	['PER_FAX_NUMBER', 'SRC_PER_FAX_NUMBER'],
	['PER_EMAIL', 'SRC_PER_EMAIL'],
	# ['STD_PER_ADDRESS_LINE_1', 'STD_PER_ADDRESS_LINE_1'],
	# ['STD_PER_ADDRESS_LINE_2', 'STD_PER_ADDRESS_LINE_2'],
	# ['STD_PER_CITY', 'STD_PER_CITY'],
	# ['STD_PER_STATE', 'STD_PER_STATE'],
	# ['STD_PER_ZIP_CODE', 'STD_PER_ZIP_CODE'],
	# ['STD_PER_ZIP_SUPP', 'STD_PER_ZIP_SUPP'],
	# ['STD_PER_COUNTRY', 'STD_PER_COUNTRY'],
	# ['STD_PER_PHONE_NUMBER', 'STD_PER_PHONE_NUMBER'],
	# ['STD_PER_FAX_NUMBER', 'STD_PER_FAX_NUMBER'],
	['ORG_NAME', 'SRC_ORG_NAME'],
	['ORG_ADDRESS_LINE_1', 'SRC_ORG_ADDRESS_LINE_1'],
	['ORG_ADDRESS_LINE_2', 'SRC_ORG_ADDRESS_LINE_2'],
	['ORG_ADDRESS_LINE_3', 'SRC_ORG_ADDRESS_LINE_3'],
	['ORG_CITY', 'SRC_ORG_CITY'],
	['ORG_STATE', 'SRC_ORG_STATE'],
	['ORG_ZIP_CODE', 'SRC_ORG_ZIP_CODE'],
	['ORG_ZIP_SUPP', 'SRC_ORG_ZIP_SUPP'],
	['ORG_COUNTRY', 'SRC_ORG_COUNTRY'],
	['ORG_PHONE_NUMBER', 'SRC_ORG_PHONE_NUMBER'],
	['ORG_FAX_NUMBER', 'SRC_ORG_FAX_NUMBER'],
	['ORG_WEBSITE', 'SRC_ORG_WEBSITE'],
	['STD_ORG_NAME', 'STD_ORG_NAME'],
	['STD_ORG_ADDR1', 'STD_ORG_ADDRESS_LINE_1'],
	['STD_ORG_ADDR2', 'STD_ORG_ADDRESS_LINE_2'],
	['STD_ORG_CITY', 'STD_ORG_CITY'],
	['STD_ORG_COUNTY', 'STD_ORG_COUNTY'],
	['STD_ORG_STATE', 'STD_ORG_STATE'],
	['STD_ORG_ZIP', 'STD_ORG_ZIP_CODE'],
	['STD_ORG_ZIP4', 'STD_ORG_ZIP_SUPP'],
	['STD_ORG_COUNTRY', 'STD_ORG_COUNTRY'],
	['STD_ORG_ADDR_LOC_TYPE', 'STD_ORG_ADDR_LOC_TYPE'],
	['STD_ORG_ADDR_TYPE', 'STD_ORG_ADDR_TYPE'],
	['STD_ORG_CHECK_DIGIT', 'STD_ORG_CHECK_DIGIT'],
	['STD_ORG_CMRA_CD', 'STD_ORG_CMRA_CD'],
	['STD_ORG_DLVRY_POINT_CD', 'STD_ORG_DLVRY_POINT_CD'],
	['STD_ORG_DPV_NOTE', 'STD_ORG_DPV_NOTE'],
	['STD_ORG_DPV_STATUS', 'STD_ORG_DPV_STATUS'],
	['STD_ORG_ELOT', 'STD_ORG_ELOT'],
	['STD_ORG_ELOT_ORDER', 'STD_ORG_ELOT_ORDER'],
	['STD_ORG_FAULT_CD', 'STD_ORG_FAULT_CD'],
	['STD_ORG_FAULT_DESC', 'STD_ORG_FAULT_DESC'],
	['STD_ORG_LAT', 'STD_ORG_LAT'],
	['STD_ORG_LONG', 'STD_ORG_LONG'],
	['STD_ORG_NOTES', 'STD_ORG_NOTES'],
	['STD_ORG_PRIM_RANGE', 'STD_ORG_PRIM_RANGE'],
	['STD_ORG_RESIDENT_DLVRY_IND', 'STD_ORG_RESIDENT_DLVRY_IND'],
	['STD_ORG_ROUTE_CD', 'STD_ORG_ROUTE_CD'],
	['STD_ORG_STREET_TYPE', 'STD_ORG_STREET_TYPE'],
	['STD_ORG_PHONE', 'STD_ORG_PHONE_NUMBER'],
	['STD_ORG_AREACD', 'STD_ORG_AREACD'],
	['CREATED_DT', 'CREATED_DT'],
	['LAST_UPDATED_DT', 'LAST_UPDATED_DT'],
	['LAST_UPDATED_USER', 'LAST_UPDATED_USER']
]

# ['ORG_MATCH_STATUS', 'ORG_MATCH_STATUS'],
# ['ORG_ID'],
# ['ORG_MATCH_CONFIDENCE'],
# ['ORG_INTRA_DUPE_REC_ID'],
# ['BU_ORG_MATCH_CONFIDENCE'],
# ['BU_PERSON_MATCH_CONFIDENCE']
# ['PERSON_MATCH_STATUS'],
# ['PERSON_ID'],
# ['PERSON_MATCH_CONFIDENCE'],
# ['PERSON_INTRA_DUPE_REC_ID'],
print(output_data)


def process_incoming_data(pv_filename):
	src_file = os.path.join(FOLDERS['incoming'], pv_filename)
	dst_file = os.path.join(FOLDERS['process'], pv_filename)

	genmodule.logger("INFO", f"Source File: {src_file}, Target File: {dst_file}")
	ld_cr_dt = datetime.datetime.now().strftime("%Y-%b-%d %H:%M:%S")
	ls_cr_fmt = "yyyy-MMM-dd HH:mm:ss"
	ls_user_name = os.getlogin()

	ldf_incoming = spark.read.csv(src_file, header=True, inferSchema=True).limit(10)
	ldf_process = (ldf_incoming.withColumn("BU_REC_ID", udf_uuid())
				   .withColumn('SRC_INPUT_FILE_NAME', lit(pv_filename))
				   .withColumn("CREATED_DT", (lit(ld_cr_dt)))
				   .withColumn("LAST_UPDATED_DT", (lit(ld_cr_dt)))
				   .withColumn("LAST_UPDATED_USER", lit(ls_user_name))
				   .fillna(''))

	ldf_processed = ldf_process.rdd.mapPartitions(genmodule.standardize_records)
	df_process = spark.createDataFrame(ldf_processed)

	genmodule.logger("INFO", "Records with Rec ID, Address, Email, and Phone Standardization ...")
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


udf_uuid = udf(genmodule.generate_guid, StringType())
ID = config['DEFAULT']['ID']
file_name = "BU_Bulk_Match_BASE_ADDRESS_INPUT_250415.csv." + ID

process_incoming_data(file_name)
genmodule.logger("INFO", "Standardization Process Completed...")

genmodule.logger("INFO", "Starting to process the file with Tamr LLM API...")
process_tamr(file_name)
genmodule.logger("INFO", "Tamr LLM API Process Completed...")
spark.stop()
