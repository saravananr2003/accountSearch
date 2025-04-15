import uuid

import requests
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
spark = SparkSession.builder.appName("MyApp").master("local[*]").config("spark.driver.host", "localhost").config(
	"spark.sql.debug.maxToStringFields", 200).getOrCreate()

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
	ldf_incoming = spark.read.csv(src_file, header=True, inferSchema=True)
	ldf_process = ldf_incoming.withColumn("BU_REC_ID", uuid_udf())


	ldf_processed = ldf_process.rdd.mapPartitions(call_api_per_record)
	df_processed = spark.createDataFrame(ldf_processed)


	ldf_process.show(truncate=False)
	df_processed.show(truncate=False)


# ldf_process.write.option("header", True).csv(dst_file, mode="overwrite")

def call_api_per_record(records):
	import requests
	base_url = "http://eas01-sapds-bu-pe.az.staples.com/DataServices/servlet/webservices?ver=2.1"

	headers = {"Content-Type": "text/xml; charset=utf-8",
			   "SOAPAction": "service=cleanseAddress"}
	for record in records:
		ps_bu_rec_id = record['BU_REC_ID']
		ls_per_title = ""
		ls_per_first_name = record['PER_FIRST_NAME']
		ls_per_middle_init = record['PER_MIDDLE_NAME']
		ls_per_last_name = record['PER_LAST_NAME']
		ls_per_prefix = record['PER_PREFIX']
		ls_per_suffix = record['PER_SUFFIX']
		ls_per_addr1 = record['PER_ADDRESS_LINE_1']
		ls_per_addr2 = record['PER_ADDRESS_LINE_2']
		ls_per_addr3 = record['PER_ADDRESS_LINE_3']
		ls_per_city = record['PER_CITY']
		ls_per_state = record['PER_STATE']
		ls_per_zip = record['PER_ZIP_CODE']
		ls_per_zip4 = record['PER_ZIP_SUPP']
		ls_per_country = record['PER_COUNTRY']
		ls_per_phone = record['PER_PHONE_NUMBER']
		ps_email = record['PER_EMAIL']

		ls_org_name = record['ORG_NAME']
		ls_org_addr1 = record['ORG_ADDRESS_LINE_1']
		ls_org_addr2 = record['ORG_ADDRESS_LINE_2']
		ls_org_addr3 = record['ORG_ADDRESS_LINE_3']
		ls_org_city = record['ORG_CITY']
		ls_org_state = record['ORG_STATE']
		ls_org_zip = record['ORG_ZIP_CODE']
		ls_org_zip4 = record['ORG_ZIP_SUPP']
		ls_org_country = record['ORG_COUNTRY']
		ls_org_phone = record['ORG_PHONE_NUMBER']
		#
		# PER_FAX_NUMBER,   , , , , , , ORG_FAX_NUMBER, ORG_WEBSITE

		ls_soap_body = ""
		if len(ls_per_first_name) > 0:
			ls_soap_body = ("<soapenv:Envelope xmlns:soapenv='http://schemas.xmlsoap.org/soap/envelope/' "
							" xmlns:tid='http://schemas.staples.com/eai/tid02' xmlns:head='http://schemas.staples.com/ech/ecis/header' "
							" xmlns:add='http://schemas.staples.com/ech/eas/addrservice' xmlns:add1='http://schemas.staples.com/ech/eas/address' "
							" xmlns:phon='http://schemas.staples.com/ech/eas/phone'  xmlns:ema='http://schemas.staples.com/ech/eas/email'>"
							f"    <soapenv:Header>"
							f"        <tid:SplsTID> </tid:SplsTID>"
							f"    </soapenv:Header>"
							f"    <soapenv:Body>"
							f'        <add:CustAddrCleansing Id="1" Version="1">'
							f"            <head:Header> </head:Header>"
							f'            <add:Profile Id="{ps_bu_rec_id}"> '
							f"                <add:Title>{ls_per_title}</add:Title>"
							f"                <add:FirstNm>{ls_per_first_name}</add:FirstNm>"
							f"                <add:MiddleNm>{ls_per_middle_init}</add:MiddleNm>"
							f"                <add:LastNm>{ls_per_last_name}</add:LastNm> "
							f"                <add:Prefx>{ls_per_prefix}</add:Prefx> "
							f"                <add:Suffx>{ls_per_suffix}</add:Suffx>"
							f"                <add:OrgNm>{ls_org_name}</add:OrgNm>"
							f"                <add1:Address id=''>"
							f"                    <add1:AddrLine1>{ls_per_addr1}</add1:AddrLine1>"
							f"                    <add1:AddrLine2>{ls_per_addr2}</add1:AddrLine2>"
							f"                    <add1:AddrLine3>{ls_per_addr3}</add1:AddrLine3>"
							f"                    <add1:AddrCounty/>"
							f"                    <add1:AddrCity>{ls_per_city}</add1:AddrCity>"
							f"                    <add1:AddrState>{ls_per_state}</add1:AddrState>"
							f"                    <add1:AddrZip>{ls_per_zip}</add1:AddrZip>"
							f"                    <add1:AddrZip4>{ls_per_zip4}</add1:AddrZip4>"
							f"                    <add1:AddrCountry>{ls_per_country}</add1:AddrCountry>"
							f"                </add1:Address>"
							f"            <phon:Phone PhExchange='' PhAreaCd=''>{ls_per_phone}</phon:Phone>"
							f"            <ema:Email>{ps_email}</ema:Email> "
							f"            </add:Profile>"
							f"        </add:CustAddrCleansing>"
							f"    </soapenv:Body>"
							f"</soapenv:Envelope>")
		else:
			ls_soap_body = ("<soapenv:Envelope xmlns:soapenv='http://schemas.xmlsoap.org/soap/envelope/' "
								" xmlns:tid='http://schemas.staples.com/eai/tid02' xmlns:head='http://schemas.staples.com/ech/ecis/header' "
								" xmlns:add='http://schemas.staples.com/ech/eas/addrservice' xmlns:add1='http://schemas.staples.com/ech/eas/address' "
								" xmlns:phon='http://schemas.staples.com/ech/eas/phone'  xmlns:ema='http://schemas.staples.com/ech/eas/email'>"
								f"    <soapenv:Header>"
								f"        <tid:SplsTID> </tid:SplsTID>"
								f"    </soapenv:Header>"
								f"    <soapenv:Body>"
								f'        <add:CustAddrCleansing Id="1" Version="1">'
								f"            <head:Header> </head:Header>"
								f'            <add:Profile Id="{ps_bu_rec_id}"> '
								f"                <add:Title/><add:FirstNm/><add:MiddleNm/><add:LastNm/><add:Prefx/><add:Suffx/>"
								f"                <add:OrgNm>{ls_org_name}</add:OrgNm>"
								f"                <add1:Address id=''>"
								f"                    <add1:AddrLine1>{ls_org_addr1}</add1:AddrLine1>"
								f"                    <add1:AddrLine2>{ls_org_addr2}</add1:AddrLine2>"
								f"                    <add1:AddrLine3>{ls_org_addr3}</add1:AddrLine3>"
								f"                    <add1:AddrCounty/>"
								f"                    <add1:AddrCity>{ls_org_city}</add1:AddrCity>"
								f"                    <add1:AddrState>{ls_org_state}</add1:AddrState>"
								f"                    <add1:AddrZip>{ls_org_zip}</add1:AddrZip>"
								f"                    <add1:AddrZip4>{ls_org_zip4}</add1:AddrZip4>"
								f"                    <add1:AddrCountry>{ls_org_country}</add1:AddrCountry>"
								f"                </add1:Address>"
								f"            <phon:Phone PhExchange='' PhAreaCd=''>{ls_org_phone}</phon:Phone>"
								f"            <ema:Email>{ps_email}</ema:Email> "
								f"            </add:Profile>"
								f"        </add:CustAddrCleansing>"
								f"    </soapenv:Body>"
								f"</soapenv:Envelope>")
		print(ls_soap_body)

		# response = requests.post(base_url, data=ls_soap_body, headers=headers)
		#
		# # Check the response status code and print the response.
		# if response.status_code == 200:
		# 	print("Successful SOAP response:")
		# 	print(response.text)
		# else:
		# 	print("Error:", response.status_code)
		# 	print(response.text)
		#
		record_dict = record.asDict()
		# record_dict['api_response'] = response.text
		record_dict['api_request'] = record
		yield record_dict


#
# def process_standardize_address(pdf_rawdata, ps_bu_rec_id=None,	ps_org_name="",
# 								ps_per_title="", ps_per_prefix="", ps_per_first_name="", ps_per_middle_init="",
# 								ps_per_last_name="", ps_per_suffix="",
# 								ps_addr1="", ps_addr2="", ps_addr3="", ps_county="", ps_city="", ps_state="", ps_zip="", ps_zip4="",
# 								ps_country="USA",
# 								ps_email="", ps_phone=""):
# 	base_url = "http://eas01-sapds-bu-pe.az.staples.com/DataServices/servlet/webservices?ver=2.1"
#
# 	headers = {	"Content-Type": "text/xml; charset=utf-8",
# 				"SOAPAction": "service=cleanseAddress" }
#
# 	ls_soap_body = ("<soapenv:Envelope xmlns:soapenv='http://schemas.xmlsoap.org/soap/envelope/' "
# 					" xmlns:tid='http://schemas.staples.com/eai/tid02' xmlns:head='http://schemas.staples.com/ech/ecis/header' "
# 					" xmlns:add='http://schemas.staples.com/ech/eas/addrservice' xmlns:add1='http://schemas.staples.com/ech/eas/address' "
# 					" xmlns:phon='http://schemas.staples.com/ech/eas/phone'  xmlns:ema='http://schemas.staples.com/ech/eas/email'>"
# 					f"    <soapenv:Header>"
# 					f"        <tid:SplsTID> </tid:SplsTID>"
# 					f"    </soapenv:Header>"
# 					f"    <soapenv:Body>"
# 					f'        <add:CustAddrCleansing Id="1" Version="1">'
# 					f"            <head:Header> </head:Header>"
# 					f'            <add:Profile Id="{ps_bu_rec_id}"> '
# 					f"                <add:Title>{ps_per_title}</add:Title>"
# 					f"                <add:FirstNm>{ps_per_first_name}</add:FirstNm>"
# 					f"                <add:MiddleNm>{ps_per_middle_init}</add:MiddleNm>"
# 					f"                <add:LastNm>{ps_per_last_name}</add:LastNm> "
# 					f"                <add:Prefx>{ps_per_prefix}</add:Prefx> "
# 					f"                <add:Suffx>{ps_per_suffix}</add:Suffx>"
# 					f"                <add:OrgNm>{ps_org_name}</add:OrgNm>"
# 					f"                <add1:Address id=''>"
# 					f"                    <add1:AddrLine1>{ps_addr1}</add1:AddrLine1>"
# 					f"                    <add1:AddrLine2>{ps_addr2}</add1:AddrLine2>"
# 					f"                    <add1:AddrLine3>{ps_addr3}</add1:AddrLine3>"
# 					f"                    <add1:AddrCounty>{ps_county}</add1:AddrCounty>"
# 					f"                    <add1:AddrCity>{ps_city}</add1:AddrCity>"
# 					f"                    <add1:AddrState>{ps_state}</add1:AddrState>"
# 					f"                    <add1:AddrZip>{ps_zip}</add1:AddrZip>"
# 					f"                    <add1:AddrZip4>{ps_zip4}</add1:AddrZip4>"
# 					f"                    <add1:AddrCountry>{ps_country}</add1:AddrCountry>"
# 					f"                </add1:Address>"
# 					f"            <phon:Phone PhExchange='' PhAreaCd=''>{ps_phone}</phon:Phone>"
# 					f"            <ema:Email>{ps_email}</ema:Email> "
# 					f"            </add:Profile>"
# 					f"        </add:CustAddrCleansing>"
# 					f"    </soapenv:Body>"
# 					f"</soapenv:Envelope>")
# 	print (ls_soap_body)
#
#
# 	response = requests.post(base_url, data=ls_soap_body, headers=headers)
#
# 	# Check the response status code and print the response.
# 	if response.status_code == 200:
# 		print("Successful SOAP response:")
# 		print(response.text)
# 	else:
# 		print("Error:", response.status_code)
# 		print(response.text)
#

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
spark.stop()
