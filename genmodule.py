import json
import os
import shutil
import uuid
import xml.etree.ElementTree as ET
from datetime import datetime

import requests
import snowflake
from pyspark import Row
from snowflake.connector.pandas_tools import write_pandas

import pandas as pd
import configparser


def read_config():
	properties = configparser.ConfigParser()
	properties.read('config.properties')
	return properties


def logger(ps_msg_type, ps_logmessge):
	"""
    Function to log messages
    :param logmessge: Message to be logged
    :param msgtype: Type of message (INFO, ERROR, etc.)
    """
	# Implement your logging logic here
	print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} - {ps_msg_type}: {ps_logmessge}")


def write_df_2_file(df_processed, dst_file, file_type="CSV"):
	"""
    Writes processed dataframe to CSV file, handling existing directory cleanup.
    Args:
        df_processed: Spark DataFrame to write
        dst_file (str): Destination file path
        folder_type (str): Type of folder for logging purposes
    """
	try:
		if os.path.exists(dst_file):
			shutil.rmtree(dst_file)
			logger("INFO", "Removed Existing Directory")

		logger("INFO", f"Starting to populate output file {dst_file} folder...")
		if file_type == "CSV":
			df_processed.write.csv(dst_file, header=True, mode='overwrite')
		elif file_type == "PARQUET":
			df_processed.write.parquet(dst_file, mode='overwrite')
		elif file_type == "JSON":
			df_processed.write.json(dst_file, mode='overwrite')
		elif file_type == "ORC":
			df_processed.write.orc(dst_file, mode='overwrite')
		elif file_type == "AVRO":
			df_processed.write.format("avro").save(dst_file, mode='overwrite')
		elif file_type == "TAB":
			df_processed.write.csv(dst_file, sep="\t", header=True, mode='overwrite')
		elif file_type == "SNOWFLAKE":
			write_2_snowflake(df_processed, "RAM_TAMR_ACCOUNTS_DATA", "overwrite")

	except Exception as e:
		logger("ERROR", f"Failed to write data to {dst_file}: {str(e)}")
		raise


def snowflake_connection(config):
	conn = snowflake.connector.connect(
		user=config['DATABASE']['SNOWFLAKE_USER'],
		password=config['DATABASE']['SNOWFLAKE_PASSWORD'],
		account=config['DATABASE']['SNOWFLAKE_ACCOUNT'],
		warehouse=config['DATABASE']['SNOWFLAKE_WAREHOUSE'],
		database=config['DATABASE']['SNOWFLAKE_DATABASE'],
		schema=config['DATABASE']['SNOWFLAKE_SCHEMA'],
		role=config['DATABASE']['SNOWFLAKE_ROLE']
	)
	if conn.is_valid():
		return conn
	else:
		return None


def write_2_snowflake(df, table_name, mode="append"):
	config = read_config()
	pd_df = pd.DataFrame(df.collect(), columns=df.columns)
	conn = snowflake_connection(config)


	lb_overwrite = True if mode == "overwrite" else False

	if conn is None:
		logger("ERROR", "Failed to connect to Snowflake.")
		return

	logger("INFO", f"Connected to Snowflake account {config['DATABASE']['SNOWFLAKE_ACCOUNT']}.")
	print(f"Connection to Snowflake is  {conn.is_valid()}")
	try:
		success, nchunks, nrows, _ = write_pandas(conn, pd_df, table_name,
												  database=config['DATABASE']['SNOWFLAKE_DATABASE'],
												  schema=config['DATABASE']['SNOWFLAKE_SCHEMA'],
												  overwrite=lb_overwrite
												  )
		if success:
			logger("INFO", f"Data written to Snowflake table {table_name} successfully.")
		else:
			logger("ERROR", f"Failed to write data to Snowflake table {table_name}.")
	except Exception as e:
		logger("ERROR", f"Error writing to Snowflake: {str(e)}")
		raise
	finally:
		conn.close()
		logger("INFO", "Snowflake connection closed.")


def get_address(pd_addr, pd_dataset, pn_spaces):
	ld_addr = {}
	for rec in pd_dataset:
		# print(rec[0], " _ ", rec[1])
		ls_retattr = rec[0]
		ls_attr = str(rec[1])
		if rec[2] == 0:
			ld_addr[ls_retattr] = str(pd_addr.find(f'ns4:{ls_attr}', pn_spaces).text or "")
			ld_addr[ls_retattr + "_FAULT_CD"] = str(pd_addr.find(f'ns4:{ls_attr}', pn_spaces).get('FaultCode') or "")
			ld_addr[ls_retattr + "_FAULT_DESC"] = str(pd_addr.find(f'ns4:{ls_attr}', pn_spaces).get('FaultDesc') or "")
		else:
			ld_addr[ls_retattr] = str(pd_addr.get(ls_attr) or "")

	return ld_addr


def generate_guid():
	return str(uuid.uuid4())


def call_tamr_api(records):
	config = read_config()
	base_url = config['TAMR']['ACCOUNTS_MATCH_API_URL']
	headers = {
		"Content-Type": "application/json",
		"Accept": "application/json",
		"Authorization": config['TAMR']['ACCOUNTS_MATCH_API_TOKEN']
	}

	for record in records:
		base = record.asDict()
		try:
			payload = create_api_payload(record)
			response = requests.post(base_url, headers=headers, json=payload, timeout=30)
			if response.status_code == 200:
				lj_response = response.text.strip().splitlines()
				parsed = ([json.loads(line) for line in lj_response if line.strip()])

				if len(parsed) > 0:
					for d in parsed:
						base["ORG_ID"] = d.get("clusterId")
						base["ORG_MATCH_CONFIDENCE"]: float = d.get("avgMatchProb")
						yield Row(**base)
				else:
					base["ORG_ID"] = None
					base["ORG_MATCH_CONFIDENCE"]: float = None
					yield Row(**base)
			else:
				logger("ERROR", f"Error: {response.status_code} - {response.text}")
		except requests.exceptions.RequestException as e:
			logger("ERROR", f"Request failed: {e}")
			base["ORG_ID"] = None
			base["ORG_MATCH_CONFIDENCE"]: float = None
			yield Row(**base)


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


def standardize_records(records):
	import requests
	properties = read_config()
	base_url = properties['SOAP']['URL_ECH_CLEANSER']

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
			# Create the SOAP body for the request

			ls_soap_body = (f"<soapenv:Envelope xmlns:soapenv='{properties['SOAP']['URL_SOAP_ENV_SRV']}' "
							f" xmlns:tid='http://schemas.staples.com/eai/tid02' xmlns:head='{properties['SOAP']['URL_ECH_HDR_SRV']}'"
							f" xmlns:add='{properties['SOAP']['URL_ECH_ADDR_SRV']}' xmlns:add1='http://schemas.staples.com/ech/eas/address' "
							f" xmlns:phon='{properties['SOAP']['URL_ECH_PHONE_SRV']}'  xmlns:ema='{properties['SOAP']['URL_ECH_EMAIL_SRV']}'>"
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
			ls_soap_body = (f"<soapenv:Envelope xmlns:soapenv='{properties['SOAP']['URL_SOAP_ENV_SRV']}' "
							f" xmlns:tid='http://schemas.staples.com/eai/tid02' xmlns:head='{properties['SOAP']['URL_ECH_HDR_SRV']}' "
							f" xmlns:add='{properties['SOAP']['URL_ECH_ADDR_SRV']}' xmlns:add1='http://schemas.staples.com/ech/eas/address' "
							f" xmlns:phon='{properties['SOAP']['URL_ECH_PHONE_SRV']}'  xmlns:ema='{properties['SOAP']['URL_ECH_EMAIL_SRV']}'>"
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
			'ns1': properties['SOAP']['URL_ECH_ADDR_SRV'],
			'ns2': properties['SOAP']['URL_ECH_HDR_SRV'],
			'ns3': properties['SOAP']['URL_ECH_PHONE_SRV'],
			'ns4': 'http://schemas.staples.com/ech/eas/address',
			'ns5': properties['SOAP']['URL_ECH_EMAIL_SRV']
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
				retval = get_address(address, addr_dataset, namespaces)
				for rc_val in addr_dataset:
					rec = str(rc_val[0])
					ld_record['STD_PER_' + rec] = retval[rec]
					ld_record['STD_PER_' + rec + '_FAULT_CD'] = retval[rec + '_FAULT_CD']
					ld_record['STD_PER_' + rec + '_FAULT_DESC'] = retval[rec + '_FAULT_DESC']

				# Phone Response
				ld_record['STD_PER_PHONE'] = (str(person.find('na3:Phone', namespaces).text) or "")
				ld_record['STD_PER_AREACD'] = str(person.find('ns3:Phone', namespaces).get('PhAreaCd') or "")
		else:
			logger("ERROR", f"{ps_bu_rec_id}  - {ls_soap_body} - Response : {response.status_code}")
		yield ld_record
