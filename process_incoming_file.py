import configparser
import datetime
import os
import shutil
import sys
import xml.etree.ElementTree as ET

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, lit
from pyspark.sql.types import StringType

import genmodule

# Read config and setup environment
config = configparser.ConfigParser()
config.read('config.properties')

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
	'incoming': os.path.join(BASE_FOLDER, config['FOLDER']['INCOMING_FOLDER']),
	'process': os.path.join(BASE_FOLDER, config['FOLDER']['PROCESS_FOLDER']),
	'output': os.path.join(BASE_FOLDER, config['FOLDER']['OUTPUT_FOLDER']),
	'temp': os.path.join(BASE_FOLDER, config['FOLDER']['TEMP_FOLDER'])
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


def process_file(pv_filename):
	src_file = os.path.join(FOLDERS['incoming'], pv_filename)
	dst_file = os.path.join(FOLDERS['process'], pv_filename)

	genmodule.logger("INFO", f"Source File: {src_file}")
	genmodule.logger("INFO", f"Target File: {dst_file}")
	ld_created_date = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
	ls_user_name = os.getlogin()

	ldf_incoming = spark.read.csv(src_file, header=True, inferSchema=True)  #.limit(50)
	ldf_process = (ldf_incoming.withColumn("BU_REC_ID", udf_uuid())
				   .withColumn('SRC_INPUT_FILE_NAME', lit(pv_filename))
				   .withColumn("CREATED_DT", lit(ld_created_date))
				   .withColumn("LAST_UPDATED_DT", lit(ld_created_date))
				   .withColumn("LAST_UPDATED_USER", lit(ls_user_name))
				   .fillna(''))

	ldf_processed = ldf_process.rdd.mapPartitions(standardize_records)
	df_process = spark.createDataFrame(ldf_processed)
	df_processed = df_process

	genmodule.logger("INFO", "Records with Rec ID, Address, Email, and Phone Standardization ...")
	df_processed.show(truncate=False, )
	df_processed = df_process.select([col(c[0]).alias(c[1]) for c in output_data])  #.limit(300)

	if os.path.exists(dst_file):
		shutil.rmtree(dst_file)
	genmodule.logger("INFO", "Removed Existing Directory, Starting to populate output file in process folder...")

	df_processed.write.csv(dst_file, header=True, mode='overwrite')
	genmodule.logger("INFO", "Output file written on Process folder...")


def standardize_records(records):
	import requests
	base_url = config['SOAP']['URL_ECH_CLEANSER']

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
			ls_soap_body = (f"<soapenv:Envelope xmlns:soapenv='{config['SOAP']['URL_SOAP_ENV_SRV']}' "
							f" xmlns:tid='http://schemas.staples.com/eai/tid02' xmlns:head='{config['SOAP']['URL_ECH_HDR_SRV']}'"
							f" xmlns:add='{config['SOAP']['URL_ECH_ADDR_SRV']}' xmlns:add1='http://schemas.staples.com/ech/eas/address' "
							f" xmlns:phon='{config['SOAP']['URL_ECH_PHONE_SRV']}'  xmlns:ema='{config['SOAP']['URL_ECH_EMAIL_SRV']}'>"
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
			ls_soap_body = (f"<soapenv:Envelope xmlns:soapenv='{config['SOAP']['URL_SOAP_ENV_SRV']}' "
							f" xmlns:tid='http://schemas.staples.com/eai/tid02' xmlns:head='{config['SOAP']['URL_ECH_HDR_SRV']}' "
							f" xmlns:add='{config['SOAP']['URL_ECH_ADDR_SRV']}' xmlns:add1='http://schemas.staples.com/ech/eas/address' "
							f" xmlns:phon='{config['SOAP']['URL_ECH_PHONE_SRV']}'  xmlns:ema='{config['SOAP']['URL_ECH_EMAIL_SRV']}'>"
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
			'ns1': config['SOAP']['URL_ECH_ADDR_SRV'],
			'ns2': config['SOAP']['URL_ECH_HDR_SRV'],
			'ns2': config['SOAP']['URL_ECH_HDR_SRV'],
			'ns3': config['SOAP']['URL_ECH_PHONE_SRV'],
			'ns4': 'http://schemas.staples.com/ech/eas/address',
			'ns5': config['SOAP']['URL_ECH_EMAIL_SRV']
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
				retval = genmodule.get_address(address, addr_dataset, namespaces)
				for rc_val in addr_dataset:
					ld_record['STD_ORG_' + str(rc_val[0])] = retval[str(rc_val[0])]

				# Phone Response
				ld_record['STD_ORG_PHONE'] = str(person.find('ns3:Phone', namespaces).text or "")
				ld_record['STD_ORG_AREACD'] = str(person.find('ns3:Phone', namespaces).get('PhAreaCd') or "")
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
				retval = genmodule.get_address(address, addr_dataset, namespaces)
				for rc_val in addr_dataset:
					rec = str(rc_val[0])
					ld_record['STD_PER_' + rec] = retval[rec]
					ld_record['STD_PER_' + rec + '_FAULT_CD'] = retval[rec + '_FAULT_CD']
					ld_record['STD_PER_' + rec + '_FAULT_DESC'] = retval[rec + '_FAULT_DESC']

				# Phone Response
				ld_record['STD_PER_PHONE'] = (str(person.find('na3:Phone', namespaces).text) or "")
				ld_record['STD_PER_AREACD'] = str(person.find('ns3:Phone', namespaces).get('PhAreaCd') or "")

		else:
			genmodule.logger("ERROR", f"{ps_bu_rec_id}  - {ls_soap_body} - Response : {response.status_code}")
		yield ld_record


udf_uuid = udf(genmodule.generate_guid, StringType())
ID = config['DEFAULT']['ID']
file_name = "BU_Bulk_Match_BASE­_ADDRESS_INPUT_250415.csv." + ID
process_file(file_name)
spark.stop()

