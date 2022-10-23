import json
import datetime
import pandas as pd
# import requests
from typing import Union
from io import BytesIO
from zipfile import ZipFile
from urllib.request import urlopen
from pathlib import Path
import xml.etree.ElementTree as et
from xml.dom import minidom
import boto3

s3 = boto3.client('s3')
bucket = 'se-firds'

def lambda_handler(event, context):
    """Sample pure Lambda function

    Parameters
    ----------
    event: dict, required
        API Gateway Lambda Proxy Input Format

        Event doc: https://docs.aws.amazon.com/apigateway/latest/developerguide/set-up-lambda-proxy-integrations.html#api-gateway-simple-proxy-for-lambda-input-format

    context: object, required
        Lambda Context runtime methods and attributes

        Context doc: https://docs.aws.amazon.com/lambda/latest/dg/python-context-object.html

    Returns
    ------
    API Gateway Lambda Proxy Output Format: dict

        Return doc: https://docs.aws.amazon.com/apigateway/latest/developerguide/set-up-lambda-proxy-integrations.html
    """

    # get query parameters from url
    query_parameters = event.get('queryStringParameters', {})

    # default url
    url = source_url();
    
    if query_parameters:
        query = query_parameters.get('q', '*')
        from_date = query_parameters.get('from', '2021-01-17')
        to_date = query_parameters.get('to', '2021-01-17')

        # Validate from date is valid
        try:
            validate_date_format(from_date) 
        except:
            return response('Invalid from date format. Date must be in format: YYYY-mm-dd', 422)

        # Validate to date is valid
        try:
            validate_date_format(to_date) 
        except:
            return response('Invalid to date format. Date must be in format: YYYY-mm-dd', 422)

        # upadte new url
        url = source_url(query, from_date, to_date)

    # xml = get_firsts_xml_from_url(url)

    zips = get_zips_from_url(url)

    putObjects = []

    file_name = ''

    df_cols = ["FinInstrmGnlAttrbts.Id", "FinInstrmGnlAttrbts.FullNm", "FinInstrmGnlAttrbts.ClssfctnTp", "FinInstrmGnlAttrbts.CmmdtyDerivInd", "FinInstrmGnlAttrbts.NtnlCcy", "Issr"]

    df = pd.DataFrame(columns=df_cols)

    # for zip in zips:
    zipurl = urlopen(zips[0])
    with ZipFile(BytesIO(zipurl.read())) as myzip:
        for file in myzip.namelist():
            if file.endswith('.xml'):
                with myzip.open(file) as myfile:
                    file_name = file
                    with open(f'/tmp/{file_name}', "wb") as f:
                        f.write(myfile.read())
                        f.close()
                    # putFile = s3.put_object(Bucket=bucket, Key=f'DLTINS/{file_name}', Body=myfile.read())
                    # putObjects.append(putFile)
                # print(putFile)

    # # Delete zip file after unzip
    # if len(putObjects) > 0:
    #     # deletedObj = s3.delete_object(Bucket=bucket, Key=key)
    #     print('deleted file:')
    #     print(deletedObj)


    # file_data = s3.get_object(Bucket=bucket, Key=file_name).get('Body').read().decode('utf-8');

    total = 0;
    file_path = f'/tmp/{file_name}';
    for rcrd in parse_firds(file_path):
        total = total + 1
        df1 = pd.DataFrame(rcrd, columns=df_cols, index=[total])
        df = pd.concat([df, df1])
        if total > 2500: break

    df.to_csv(f's3://{bucket}/out.csv', sep=',', encoding='utf-8')

    # read file from s3
    # file_data = s3.get_object(Bucket=bucket, Key=file_name).get('Body').read().decode('utf-8');

    # (Bucket = bucket, Prefix='/something/')
    # obj = s3.Object(Bucket=bucket, Key=fileName)
    # file_data = obj.get()['Body'].read()

    # #parse xml
    # xmldoc = minidom.parseString(file_data)
    # print(xmldoc.toprettyxml())
    # file_content = s3.get_object(Bucket=bucket, Key=fileName)['Body'].read().decode('utf-8')
    # print(file_content)
    # xtree = et.iterparse(file_data)
    # xroot = xtree.getroot()

    # df_cols = ["FinInstrmGnlAttrbts.Id", "FinInstrmGnlAttrbts.FullNm", "FinInstrmGnlAttrbts.ClssfctnTp", "FinInstrmGnlAttrbts.CmmdtyDerivInd", "FinInstrmGnlAttrbts.NtnlCcy", "Issr"]
    # rows = []

    # # # for elem in xtree.iter():
    # # #     print(elem.tag)
    # namespaces = {
    #     '': 'urn:iso:std:iso:20022:tech:xsd:auth.036.001.02',
    # }

    # for elem in xroot.findall('.//FinInstrm', namespaces):
    #     s_issr = elem.find(".//Issr", namespaces).text if elem is not None else None
    #     # print(elem.find(".//Issr", namespaces).text)


    #     for fin_instrm_el in xroot.findall('.//FinInstrmGnlAttrbts', namespaces):

    #         s_id = fin_instrm_el.find("Id", namespaces).text if fin_instrm_el is not None else None
    #         s_full_nm = fin_instrm_el.find("FullNm", namespaces).text if fin_instrm_el is not None else None
    #         s_clssfctn_tp = fin_instrm_el.find("ClssfctnTp", namespaces).text if fin_instrm_el is not None else None
    #         s_cmmdty_deriv_ind = fin_instrm_el.find("CmmdtyDerivInd", namespaces).text if fin_instrm_el is not None else None
    #         s_ctnl_ccy = fin_instrm_el.find("NtnlCcy", namespaces).text if fin_instrm_el is not None else None

    #         rows.append({
    #             "FinInstrmGnlAttrbts.Id": s_id, 
    #             "FinInstrmGnlAttrbts.FullNm": s_full_nm,
    #             "FinInstrmGnlAttrbts.ClssfctnTp": s_clssfctn_tp,
    #             "FinInstrmGnlAttrbts.CmmdtyDerivInd": s_cmmdty_deriv_ind,
    #             "FinInstrmGnlAttrbts.NtnlCcy": s_ctnl_ccy,
    #             "Issr": s_issr,
    #         })

    # out_df = pd.DataFrame(rows, columns = df_cols)

    return response({
            "message": "hello world",
            # "query": event["queryStringParameters"].get('q', ''),
            # "from": from_date,
            'url': url,
            "zip": zips[0]
            # "zips": json(set(df['str'].tolist()))
            # "xml": xml,
            # "columns": json(df.columns)
            # 'parameters': query_parameters
            # "xml": get_firsts_xml_from_url(url)
            # "to": event.get("queryStringParameters", {}).get('to', ''),
            # "event": event
            # "location": ip.text.replace("\n", "")
        })

def source_url(query: str = '*', date_from: str = '2021-01-17', date_to: str = '2021-01-19') -> str:
    """Get firds source url

    Args:
        query (str, optional): The query string must be compatible with firds guidelines. Defaults to '*'.
        date_from (str, optional): Query data from a given date, format YYYY-mm-dd. Defaults to '2021-01-17'.
        date_to (str, optional): Query data to a given date, format YYYY-mm-dd. Defaults to '2021-01-19'.

    Returns:
        str: The url to get the xml firds data
    """
    return f'https://registers.esma.europa.eu/solr/esma_registers_firds_files/select?q={query}&fq=publication_date:%5B{date_from}T00:00:00Z+TO+{date_to}T23:59:59Z%5D&wt=xml&indent=true&start=0&rows=100'

def validate_date_format(date_text: str, format: str = '%Y-%m-%d'):
    """Validate date is in the correct formate

    Args:
        date_text (str): date to be validated

    Raises:
        ValueError: if date is not in the correct format
    """
    try:
        datetime.datetime.strptime(date_text, format)
    except ValueError:
        raise ValueError("Incorrect date format, should be YYYY-MM-DD")

def get_zips_from_url(url: str) -> list:
    df = pd.read_xml(url, xpath="//doc[str[@name = 'file_type'][normalize-space(text()) = 'DLTINS']]/str[@name = 'download_link']")

    return df['str'].tolist()

def parse_firds(filename):
    stack = []

    for event, elem in et.iterparse(filename, events=('start','end')):
        if event == 'start':
            if elem.tag == tag('FinInstrm'):
                record = {
                    'FinInstrmGnlAttrbts.Id': '',
                    'FinInstrmGnlAttrbts.FullNm': '',            
                    'FinInstrmGnlAttrbts.ClssfctnTp': '',            
                    'FinInstrmGnlAttrbts.CmmdtyDerivInd': '',            
                    'FinInstrmGnlAttrbts.NtnlCcy': '',            
                    'Issr': '',            
                }
            elif (elem.tag in [tag('Id'), tag('FullNm'), tag('ClssfctnTp'), tag('CmmdtyDerivInd'), tag('NtnlCcy')] and
                  stack[-1] in [tag('FinInstrmGnlAttrbts')]):
                record[clean_tag(elem.tag)] = elem.text
            elif (elem.tag in [tag('Issr')]):
                record[clean_tag(elem.tag)] = elem.text

            stack.append(elem.tag)
        elif event == 'end':
            if elem.tag == tag('FinInstrm'):
                yield record

            stack.pop()

def tag(name: str) -> str:
    return '{urn:iso:std:iso:20022:tech:xsd:auth.036.001.02}' + name.strip()

def clean_tag(name: str) -> str:
    cleaned = name.strip().replace('{urn:iso:std:iso:20022:tech:xsd:auth.036.001.02}', '')

    if cleaned == 'Issr':
        return cleaned;
    
    return f'FinInstrmGnlAttrbts.{cleaned}'

def response(data: Union[str, dict], status_code: int = '200') -> dict:
    """Builds a body response

    Args:
        data (dict | str): the data/message that we want to return
        status_code (int, optional): response status code. Defaults to '200'.

    Returns:
        dict: a structured response body
    """

    if data is dict:
        return {
            "statusCode": status_code,
            "body": json.dumps(data),
        }
    else:
        return {
            "statusCode": status_code,
            "body": json.dumps({
                'message': data
            }),
        }