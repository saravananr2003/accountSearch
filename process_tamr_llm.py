import os
import shutil
import sys
import uuid
import json
import genmodule
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType, Row

# Remove the security manager setting that's causing problems
os.environ['JAVA_TOOL_OPTIONS'] = '-Djavax.security.auth.useSubjectCredsOnly=true'

# Standard PySpark environment setup
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

ID = "2679af12-d49a-4bcc-8a7a-ce32eb22b9d5"
BASE_FOLDER = '/Users/ramsa005/Desktop/Staples/Projects/Git/accountSearch/datafiles/'
INCOMING_FOLDER = BASE_FOLDER + "incoming/"  # using a temporary directory
TEMP_FOLDER = BASE_FOLDER + "temp/"
OUTPUT_FOLDER = BASE_FOLDER + "output/"  # using a temporary directory
PROCESS_FOLDER = BASE_FOLDER + "process/"

# Create SparkSession without security manager flags
spark = SparkSession.builder.appName("MyApp").master("local[*]").config("spark.driver.host", "localhost"). \
	config("spark.sql.debug.maxToStringFields", 200).getOrCreate()

print(f"Spark Session Created with Application ID: {spark.sparkContext.applicationId}")


def generate_guid():
	return str(uuid.uuid4())


def process_tamr(pv_filename):
	src_file = PROCESS_FOLDER + pv_filename
	dst_file = OUTPUT_FOLDER + pv_filename
	print("Source File:", src_file)
	print("Target File:", dst_file)
	ldf_incoming = spark.read.csv(src_file, header=True, inferSchema=True)
	ldf_process = ldf_incoming.withColumn("BU_REC_ID1", udf_uuid()).fillna('').repartition(20)

	ldf_processed = ldf_process.rdd.mapPartitions(call_api_per_record)
	df_processed = spark.createDataFrame(ldf_processed)

	print("Records with Rec ID, Address, Email, and Phone Standardization ...")
	df_processed.show(truncate=False)

	if os.path.exists(dst_file):
		shutil.rmtree(dst_file)

	df_processed.write.csv(dst_file, header=True, mode='overwrite')


def call_api_per_record(records):
	import requests
	base_url = (
		"https://staples-prod-2.tamrfield.com"
		"/llm/api/v1/projects/2-B%20Site%20Mastering%20v2%3Amatch"
		"?type=clusters")

	headers = {"Content-Type": "application/json", "Accept": "application/json",
			   "Authorization": "BasicCreds QlVfQVBQX1VTRVI6QlVfQVBQX1VTRVI="}
	cluster_ids = []
	avg_match_probs = []
	for record in records:
		ls_rec_id = str(record['BU_REC_ID1'])
		ls_org_name = str(record['STD_ORG_NAME']).upper()
		ls_org_zip = str(record['STD_ORG_ZIP'])
		ls_org_zip4 = str(record['STD_ORG_ZIP4'])
		ls_org_phone = str(record['STD_ORG_PHONE'])
		ls_org_areacd = str(record['STD_ORG_AREACD'])
		payload = {
			"recordId": f"{ls_rec_id}",
			"record": {
				"site_address_1": [f"{record['STD_ORG_ADDR1']}"],
				"site_address_2": [f"{record['STD_ORG_ADDR2']}"],
				"site_address_full": [f"{record['STD_ORG_ADDR1']} {record['STD_ORG_ADDR2']}"],
				"site_city": [f"{record['STD_ORG_CITY']}"],
				"site_country": [f"{record['STD_ORG_COUNTRY']}"],
				"site_zip5": [f"{ls_org_zip}"],
				"site_zip9": [f"{ls_org_zip}-{ls_org_zip4}"],
				"site_phone_7": [f"{ls_org_phone[:7]}"],
				"site_phone_areacode": [f"{ls_org_areacd}"],
				"site_state": [f"{record['STD_ORG_STATE']}"],
				"site_fax": None,
				"business_name": [f"{ls_org_name}"],
				"original_source_and_ID": ["DATA_AXLE_PLACES_DEV:::744327838"],
				"site_zip4": [f"{ls_org_zip4}"],
				"phone_number_most_frequent": [f"{ls_org_phone}"],
				"site_phone_full_number": [f"{ls_org_phone}"],
				"EMAIL": None,
				"site_phone_number_10dig": [f"{ls_org_phone}"],
				"COMPANY_NAME": [f"{ls_org_phone}"],
				"site_phone_6": [f"{ls_org_phone[:6]}"],
				"ml_company_name": [f"{ls_org_name.replace(' ', '')}"],
				"ml_company_first_word": [f"{ls_org_name.split()[0]}"],
				"site_address_1_original": [f"{record['STD_ORG_ADDR1']}"],
				"business_name_gr": [f"{ls_org_name}"]
			}
		}
		print(payload)
		ld_dict = {}
		try:
			response = requests.post(base_url, headers=headers, json=payload, timeout=30)
		except requests.exceptions.RequestException as e:
			print(f"Request failed: {e}")
			continue

		if response.status_code == 200:
			lj_response = response.text.strip().splitlines()
			parsed = [json.loads(line) for line in lj_response if line.strip()]
			cluster_ids = [d.get('clusterId') for d in parsed]
			avg_match_probs = [d.get('avgMatchProb') for d in parsed]
			print(f"{cluster_ids} : {avg_match_probs}" )

	yield Row(record, clustrer_ids=cluster_ids, avg_match_probs=avg_match_probs)

# Register the UDF
udf_uuid = udf(generate_guid, StringType())
file_name = "BU_Bulk_Match_Input_2.0_Test.csv." + ID
process_tamr(file_name)
spark.stop()
