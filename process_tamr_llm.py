import configparser
import json
import os
import shutil
import sys
import uuid
import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, concat_ws, col
from pyspark.sql.types import StringType, Row

import genmodule

# Setup
config = genmodule.read_config()

# logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# # Initialize Spark
spark = SparkSession.builder \
	.appName("TamrProcessing") \
	.master("local[*]") \
	.config("spark.driver.host", "localhost") \
	.config("spark.sql.debug.maxToStringFields", 200) \
	.getOrCreate()

# Paths
FOLDERS = {
	'base': str(config['FOLDER']['BASE_FOLDER']),
	'process': str(os.path.join(config['FOLDER']['BASE_FOLDER'], config['FOLDER']['PROCESS_FOLDER'])),
	'output': str(os.path.join(config['FOLDER']['BASE_FOLDER'], config['FOLDER']['OUTPUT_FOLDER']))
}


# # Set the environment variable for Java and pyspark options
# # Remove the security manager setting that's causing problems
# os.environ['JAVA_TOOL_OPTIONS'] = '-Djavax.security.auth.useSubjectCredsOnly=true'
# os.environ['PYSPARK_PYTHON'] = sys.executable
# os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
#

#
# logging.info(f"Spark Session Created with Application ID: {spark.sparkContext.applicationId}")
#

def process_tamr(pv_filename):
	src_file = os.path.join(FOLDERS['process'], pv_filename)
	dst_file = os.path.join(FOLDERS['output'], pv_filename)

	genmodule.logger("INFO", "Starting to process Source File: {src_file} to Target File: {dst_file}")
	ldf_incoming = spark.read.csv(src_file, header=True, inferSchema=True).limit(50)
	ldf_process = ldf_incoming.withColumn("STD_ORG_ZIP9",
										  concat_ws("-", col("STD_ORG_ZIP_CODE"), col("STD_ORG_ZIP_SUPP"))) \
		.fillna('') \
		.repartition(10)

	ldf_processed = ldf_process.rdd.mapPartitions(call_tamr_api)
	df_processed = spark.createDataFrame(ldf_processed).repartition(1)

	genmodule.logger("INFO", "Records with Rec ID, Address, Email, and Phone Standardization ...")
	df_processed.show(truncate=False)

	if os.path.exists(dst_file):
		shutil.rmtree(dst_file)

	genmodule.logger("INFO", "Removed Existing Directory, Starting to populate output file in output folder...")
	df_processed.write.csv(dst_file, header=True, mode='overwrite')
	genmodule.logger("INFO", "Output file written on Output folder...")


def call_tamr_api(records):
	base_url = config['TAMR']['ACCOUNTS_MATCH_API_URL']
	base_url = 'https://staples-prod-2.tamrfield.com/llm/api/v1/projects/2-B Site Mastering v2:match?type=clusters'

	headers = {"Content-Type": "application/json", "Accept": "application/json",
			   "Authorization": config['TAMR']['ACCOUNTS_MATCH_API_TOKEN']}

	cluster_ids = []
	avg_match_probs = []
	for record in records:
		payload = create_api_payload(record)
		# genmodule.logger("INFO",f"Processing Rec ID: {record['BU_REC_ID']}")
		ld_dict = {}
		try:
			response = requests.post(base_url, headers=headers, json=payload, timeout=30)
		except requests.exceptions.RequestException as e:
			genmodule.logger("ERROR", "Request failed: {e}")
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
			genmodule.logger("ERROR", f"Error: {response.status_code} - {response.text}")
			continue


def create_api_payload(lr_row):
	ls_rec_id = str(lr_row['BU_REC_ID'])
	ls_org_name = str(lr_row['STD_ORG_NAME']).upper()
	ls_org_zip = str(lr_row['STD_ORG_ZIP_CODE'])
	ls_org_zip4 = str(lr_row['STD_ORG_ZIP_SUPP'])
	ls_org_phone = str(lr_row['STD_ORG_PHONE_NUMBER'])
	ls_org_areacd = str(lr_row['STD_ORG_AREACD'])
	payload = {
		"recordId": f"{ls_rec_id}",
		"record": {
			"site_address_1": [f"{lr_row['STD_ORG_ADDRESS_LINE_1']}"],
			"site_address_2": [f"{lr_row['STD_ORG_ADDRESS_LINE_2']}"],
			"site_address_full": [f"{lr_row['STD_ORG_ADDRESS_LINE_1']} {lr_row['STD_ORG_ADDRESS_LINE_2']}"],
			"site_city": [f"{lr_row['STD_ORG_CITY']}"],
			"site_country": [f"{lr_row['STD_ORG_COUNTRY']}"],
			"site_zip5": [f"{ls_org_zip}"],
			"site_zip9": [f"{ls_org_zip}-{ls_org_zip4}"],
			"site_phone_7": [f"{ls_org_phone[:7]}"],
			"site_phone_areacode": [f"{ls_org_areacd}"],
			"site_state": [f"{lr_row['STD_ORG_STATE']}"],
			"site_fax": None,
			"business_name": [f"{ls_org_name}"],
			"original_source_and_ID": [f"{lr_row['SRC_TP_CD']}:::{lr_row['SRC_ID']}"],
			"site_zip4": [f"{ls_org_zip4}"],
			"phone_number_most_frequent": [f"{ls_org_phone}"],
			"site_phone_full_number": [f"{ls_org_phone}"],
			"EMAIL": None,
			"site_phone_number_10dig": [f"{ls_org_phone}"],
			"COMPANY_NAME": [f"{ls_org_phone}"],
			"site_phone_6": [f"{ls_org_phone[:6]}"],
			"ml_company_name": [f"{ls_org_name.replace(' ', '')}"],
			"ml_company_first_word": [f"{ls_org_name.split()[0]}"],
			"site_address_1_original": [f"{lr_row['STD_ORG_ADDRESS_LINE_1']}"],
			"business_name_gr": [f"{ls_org_name}"]
		}
	}
	return payload



ID = config['DEFAULT']['ID']
file_name = "BU_Bulk_Match_BASE_ADDRESS_INPUT_250415.csv." + ID
process_tamr(file_name)
spark.stop()
