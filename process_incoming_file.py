import os
import shutil
import sys
import uuid
import xml.etree.ElementTree as ET
from genmodule import get_address
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

# Remove the security manager setting that's causing problems
os.environ['JAVA_TOOL_OPTIONS'] = '-Djavax.security.auth.useSubjectCredsOnly=true'

# Standard PySpark environment setup
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

# Create SparkSession without security manager flags
spark = SparkSession.builder.appName("MyApp").master("local[*]").config("spark.driver.host", "localhost"). \
	config("spark.sql.debug.maxToStringFields", 200).getOrCreate()

print(f"Spark Session Created with Application ID: {spark.sparkContext.applicationId}")

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


def process_file(pv_filename):
	src_file = INCOMING_FOLDER + pv_filename
	dst_file = PROCESS_FOLDER + pv_filename
	print("Source File:", src_file)
	print("Target File:", dst_file)
	ldf_incoming = spark.read.csv(src_file, header=True, inferSchema=True)
	ldf_process = ldf_incoming.withColumn("BU_REC_ID", udf_uuid()).fillna('')

	ldf_processed = ldf_process.rdd.mapPartitions(call_api_per_record)
	df_processed = spark.createDataFrame(ldf_processed)

	print("Records with Rec ID, Address, Email, and Phone Standardization ...")
	df_processed.show(truncate=False)

	if os.path.exists(dst_file):
		shutil.rmtree(dst_file)

	df_processed.write.csv(dst_file, header=True, mode='overwrite')


def call_api_per_record(records):
	import requests
	base_url = "http://eas01-sapds-bu-pe.az.staples.com/DataServices/servlet/webservices?ver=2.1"

	headers = {"Content-Type": "text/xml; charset=utf-8", "SOAPAction": "service=cleanseAddress"}
	for record in records:
		ls_reqtype = 'ACCOUNT'

		ps_bu_rec_id = record['BU_REC_ID']
		ls_per_title = ""
		ls_per_first_name = str(record['PER_FIRST_NAME'] or '')
		ls_per_middle_init = str(record['PER_MIDDLE_NAME'] or '')
		ls_per_last_name = str(record['PER_LAST_NAME'] or '')
		ls_per_prefix = str(record['PER_PREFIX'] or '')
		ls_per_suffix = str(record['PER_SUFFIX'] or '')
		ls_per_addr1 = str(record['PER_ADDRESS_LINE_1'] or '')
		ls_per_addr2 = str(record['PER_ADDRESS_LINE_2'] or '')
		ls_per_addr3 = str(record['PER_ADDRESS_LINE_3'] or '')
		ls_per_city = str(record['PER_CITY'] or '')
		ls_per_state = str(record['PER_STATE'] or '')
		ls_per_zip = str(record['PER_ZIP_CODE'] or '')
		ls_per_zip4 = str(record['PER_ZIP_SUPP'] or '')
		ls_per_country = str(record['PER_COUNTRY'] or '')
		ls_per_phone = str(record['PER_PHONE_NUMBER'] or '')
		ls_per_fax = str(record['PER_FAX_NUMBER'] or '')
		ls_per_email = str(record['PER_EMAIL'] or '')

		ls_org_name = str(record['ORG_NAME'] or '')
		ls_org_addr1 = str(record['ORG_ADDRESS_LINE_1'] or '')
		ls_org_addr2 = str(record['ORG_ADDRESS_LINE_2'] or '')
		ls_org_addr3 = str(record['ORG_ADDRESS_LINE_3'] or '')
		ls_org_city = str(record['ORG_CITY'] or '')
		ls_org_state = str(record['ORG_STATE'] or '')
		ls_org_zip = str(record['ORG_ZIP_CODE'] or '')
		ls_org_zip4 = str(record['ORG_ZIP_SUPP'] or '')
		ls_org_country = str(record['ORG_COUNTRY'] or '')
		ls_org_phone = str(record['ORG_PHONE_NUMBER'] or '')
		ls_org_fax = str(record['ORG_FAX_NUMBER'] or '')
		ls_org_website = str(record['ORG_WEBSITE'] or '')

		ls_soap_body = ""
		if len(ls_per_first_name) > 0:
			ls_reqtype = 'CONTACT'
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
							f"                <add:OrgNm><![CDATA[{ls_org_name}]]></add:OrgNm>"
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
							f"            <ema:Email>{ls_per_email}</ema:Email> "
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
							f"                <add:OrgNm><![CDATA[{ls_org_name}]]></add:OrgNm>"
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
							f"            </add:Profile>"
							f"        </add:CustAddrCleansing>"
							f"    </soapenv:Body>"
							f"</soapenv:Envelope>")

		# Invoke API call to Standardization Process
		response = requests.post(base_url, data=ls_soap_body, headers=headers)

		# Define the namespace mapping for the prefixes used in the XML.
		namespaces = {
			'soapenv': 'http://schemas.xmlsoap.org/soap/envelope/',
			'ns1': 'http://schemas.staples.com/ech/eas/addrservice',
			'ns2': 'http://schemas.staples.com/ech/ecis/header',
			'ns3': 'http://schemas.staples.com/ech/eas/phone',
			'ns4': 'http://schemas.staples.com/ech/eas/address',
			'ns5': 'http://schemas.staples.com/ech/eas/email'
		}

		ld_record = record.asDict()

		# Check the response status code and print the response.
		if response.status_code == 200:
			root = ET.fromstring(response.text)
			addr_dataset = [['ADDR1', 'AddrLine1', 0], ['ADDR2', 'AddrLine2', 0], ['COUNTY', 'AddrCounty', 0],
							['CITY', 'AddrCity', 0], ['STATE', 'AddrState', 0], ['ZIP', 'AddrZip', 0],
							['ZIP4', 'AddrZip4', 0], ['COUNTRY', 'AddrCountry', 0], ['LONG', 'AddrLongitude', 0],
							['LAT', 'AddrLatitude', 0], ['PRIM_RANGE', 'AddrPrimRange', 0], ['NOTES', 'AddrNotes', 0],
							['STREET_TYPE', 'AddrStreetTp', 0], ['DLVRY_POINT_CD', 'DeliveryPointCode', 0],
							['ROUTE_CD', 'RouteCode', 0], ['CHECK_DIGIT', 'CheckDigit', 0], ['ELOT', 'ELot', 0],
							['ELOT_ORDER', 'ELotOrder', 0], ['FAULT_CD', 'FaultCode', 1],
							['FAULT_DESC', 'FaultDesc', 1], ['ADDR_TYPE', 'AddrType', 1],
							['DPV_STATUS', 'DPV_Status', 1], ['CMRA_CD', 'DPV_CMRACd', 1], ['DPV_NOTE', 'DPV_Note', 1],
							['ADDR_LOC_TYPE', 'AddrLocType', 1],
							['RESIDENT_DLVRY_IND', 'DPV_RsdntlDlvryInd', 1]]
			person = root.find('.//ns1:Profile', namespaces)

			if ls_reqtype == "ACCOUNT":
				# Company Name
				ld_record['STD_ORG_NAME'] = person.find('ns1:OrgNm', namespaces).text

				# Address Response
				address = person.find('.//ns4:Address', namespaces)
				retval = get_address(address, addr_dataset, namespaces)
				for rc_val in addr_dataset:
					ld_record['STD_ORG_' + str(rc_val[0])] = retval[str(rc_val[0])]

				# Phone Response
				ld_record['STD_PER_PHONE'] = str(person.find('ns3:Phone', namespaces).text or "")
			else:
				# print(response.text)
				person = root.find('.//ns1:Profile', namespaces)
				ld_record['STD_PER_FIRST_NAME'] = str(person.find('ns1:FirstNm', namespaces).text or "")
				ld_record['STD_PER_MIDDLE_INIT'] = str(person.find('ns1:MiddleNm', namespaces).text or "")
				ld_record['STD_PER_LAST_NAME'] = str(person.find('ns1:LastNm', namespaces).text or "")
				ld_record['STD_PER_PREFIX'] = str(person.find('ns1:Prefx', namespaces).text or "")
				ld_record['STD_PER_SUFFIX'] = str(person.find('ns1:Suffx', namespaces).text or "")
				ld_record['STD_PER_GENDER'] = str(person.find('ns1:Gender', namespaces).text or "")

				# Address Response
				address = person.find('.//ns4:Address', namespaces)
				retval = get_address(address, addr_dataset, namespaces)
				for rc_val in addr_dataset:
					rec = str(rc_val[0])
					ld_record['STD_ORG_' + rec] = retval[rec]
					ld_record['STD_ORG_' + rec + '_FAULT_CD'] = retval[rec + '_FAULT_CD']
					ld_record['STD_ORG_' + rec + '_FAULT_DESC'] = retval[rec + '_FAULT_DESC']

				# Phone Response
				ld_record['STD_ORG_PHONE'] = (str(person.find('na3:Phone', namespaces).text) or "")
		else:
			print(f"Error:{ps_bu_rec_id}  - {ls_soap_body} - Response : {response.status_code}")
		yield ld_record


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


udf_uuid = udf(generate_guid, StringType())
file_name = "BU_Bulk_Match_Input_2.0_Test.csv." + ID
process_file(file_name)
spark.stop()
