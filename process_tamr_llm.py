import configparser
import logging
import os
import shutil
import sys
import uuid
import json
import genmodule
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, concat_ws, col
from pyspark.sql.types import StringType, Row

config = configparser.ConfigParser()
config.read('config.properties')

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Set the environment variable for Java and pyspark options
# Remove the security manager setting that's causing problems
os.environ['JAVA_TOOL_OPTIONS'] = '-Djavax.security.auth.useSubjectCredsOnly=true'
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

ID = config['DEFAULT']['ID']
BASE_FOLDER = config['FOLDER']['BASE_FOLDER']
INCOMING_FOLDER = os.path.join(BASE_FOLDER, config['FOLDER']['INCOMING_FOLDER'])
TEMP_FOLDER = os.path.join(BASE_FOLDER, config['FOLDER']['TEMP_FOLDER'])
OUTPUT_FOLDER = os.path.join(BASE_FOLDER, config['FOLDER']['OUTPUT_FOLDER'])
PROCESS_FOLDER = os.path.join(BASE_FOLDER, config['FOLDER']['PROCESS_FOLDER'])

# Create SparkSession without security manager flags
spark = SparkSession.builder.appName("MyApp").master("local[*]").config("spark.driver.host", "localhost"). \
	config("spark.sql.debug.maxToStringFields", 200).getOrCreate()

logging.info(f"Spark Session Created with Application ID: {spark.sparkContext.applicationId}")


def generate_guid():
	return str(uuid.uuid4())


def process_tamr(pv_filename):
	src_file = PROCESS_FOLDER + pv_filename
	dst_file = OUTPUT_FOLDER + pv_filename
	logging.info(f"Source File: {src_file}")
	logging.info(f"Target File: {dst_file}")
	logging.info("Starting to process the file...")
	ldf_incoming = spark.read.csv(src_file, header=True, inferSchema=True)
	ldf_process = ldf_incoming.withColumn("STD_ORG_ZIP9",
										  concat_ws("-", col("STD_ORG_ZIP_CODE"), col("STD_ORG_ZIP_SUPP"))) \
		.fillna('').repartition(10)

	ldf_processed = ldf_process.rdd.mapPartitions(call_api_per_record)
	df_processed = spark.createDataFrame(ldf_processed).repartition(1)

	logging.info("Records with Rec ID, Address, Email, and Phone Standardization ...")
	df_processed.show(truncate=False)

	if os.path.exists(dst_file):
		shutil.rmtree(dst_file)

	logging.info("Removed Existing Directory, Starting to populate output file in output folder...")

	df_processed.write.csv(dst_file, header=True, mode='overwrite')
	logging.info("Output file written on Output folder...")


def call_api_per_record(records):
	import requests
	base_url = config['TAMR']['ACCOUNTS_MATCH_API_URL']

	headers = {"Content-Type": "application/json", "Accept": "application/json",
			   "Authorization": config['TAMR']['ACCOUNTS_MATCH_API_TOKEN']}
	cluster_ids = []
	avg_match_probs = []
	for record in records:
		ls_rec_id = str(record['BU_REC_ID'])
		ls_org_name = str(record['STD_ORG_NAME']).upper()
		ls_org_zip = str(record['STD_ORG_ZIP_CODE'])
		ls_org_zip4 = str(record['STD_ORG_ZIP_SUPP'])
		ls_org_phone = str(record['STD_ORG_PHONE_NUMBER'])
		ls_org_areacd = str(record['STD_ORG_AREACD'])
		payload = {
			"recordId": f"{ls_rec_id}",
			"record": {
				"site_address_1": [f"{record['STD_ORG_ADDRESS_LINE_1']}"],
				"site_address_2": [f"{record['STD_ORG_ADDRESS_LINE_2']}"],
				"site_address_full": [f"{record['STD_ORG_ADDRESS_LINE_1']} {record['STD_ORG_ADDRESS_LINE_2']}"],
				"site_city": [f"{record['STD_ORG_CITY']}"],
				"site_country": [f"{record['STD_ORG_COUNTRY']}"],
				"site_zip5": [f"{ls_org_zip}"],
				"site_zip9": [f"{ls_org_zip}-{ls_org_zip4}"],
				"site_phone_7": [f"{ls_org_phone[:7]}"],
				"site_phone_areacode": [f"{ls_org_areacd}"],
				"site_state": [f"{record['STD_ORG_STATE']}"],
				"site_fax": None,
				"business_name": [f"{ls_org_name}"],
				"original_source_and_ID": [f"{record['SRC_TP_CD']}:::{record['SRC_ID']}"],
				"site_zip4": [f"{ls_org_zip4}"],
				"phone_number_most_frequent": [f"{ls_org_phone}"],
				"site_phone_full_number": [f"{ls_org_phone}"],
				"EMAIL": None,
				"site_phone_number_10dig": [f"{ls_org_phone}"],
				"COMPANY_NAME": [f"{ls_org_phone}"],
				"site_phone_6": [f"{ls_org_phone[:6]}"],
				"ml_company_name": [f"{ls_org_name.replace(' ', '')}"],
				"ml_company_first_word": [f"{ls_org_name.split()[0]}"],
				"site_address_1_original": [f"{record['STD_ORG_ADDRESS_LINE_1']}"],
				"business_name_gr": [f"{ls_org_name}"]
			}
		}
		logging.info(f"Processing Rec ID: {ls_rec_id}")
		ld_dict = {}
		try:
			response = requests.post(base_url, headers=headers, json=payload, timeout=30)
		except requests.exceptions.RequestException as e:
			logging.error(f"Request failed: {e}")
			continue

		if response.status_code == 200:
			lj_response = response.text.strip().splitlines()
			parsed = [json.loads(line) for line in lj_response if line.strip()]
			for d in parsed:
				base = record.asDict()
				# add new keys
				base["ORG_ID"] = d.get("clusterId")
				base["ORG_MATCH_CONFIDENCE"] = d.get("avgMatchProb")
				yield Row(**base)
		else:
			logging.error(f"Error: {response.status_code} - {response.text}")
			continue


# Register the UDF
udf_uuid = udf(generate_guid, StringType())
file_name = "BU_Bulk_Match_Input_2.0_Test.csv." + ID
process_tamr(file_name)
spark.stop()
