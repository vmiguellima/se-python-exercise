import json
import pandas as pd
import xml.etree.ElementTree as et
# import requests
import asyncio

import pytest

from firds_to_csv import app

def test_default_source_url():
    """Test the source url returns a valid url"""
    url = app.source_url();

    assert url == 'https://registers.esma.europa.eu/solr/esma_registers_firds_files/select?q=*&fq=publication_date:%5B2021-01-17T00:00:00Z+TO+2021-01-19T23:59:59Z%5D&wt=xml&indent=true&start=0&rows=100'

def test_custom_source_url():
    """Test the source url returns a valid url, when given custom parameters"""
    url = app.source_url('*', '2021-01-21', '2021-01-25');

    assert url == 'https://registers.esma.europa.eu/solr/esma_registers_firds_files/select?q=*&fq=publication_date:%5B2021-01-21T00:00:00Z+TO+2021-01-25T23:59:59Z%5D&wt=xml&indent=true&start=0&rows=100'


def test_validate_date_format_default_format_throws_exception_when_format_is_not_valid():
    """Assert a given date string throws an exception 
    if date it is not in format YYYY-mm-dd
    """
    with pytest.raises(ValueError):
        app.validate_date_format('20-01-21')

def test_validate_date_format_default_format_does_not_throws_exception_is_valid():
    """Assert a given date string does not throws an exception 
    if date it is in correct format
    """
    try:
        app.validate_date_format('20-01-21', '%y-%m-%d')
    except Exception as exc:
        assert False, f"validate_date_format raised an exception {exc}"

def test_validate_date_format_custom_format_does_not_throws_exception_when_format_is_valid():
    """Assert a given date string throws an exception 
    if date it is not in a given format,
    """
    try:
        app.validate_date_format('01-21', '%m-%d')
    except Exception as exc:
        assert False, f"validate_date_format raised an exception {exc}"

def test_doc():
    """
    Select all doc nodes that contains a str with a attribute named 'file_type' with a value 'DLTINS' and the gets str node link
    """
    df = pd.read_xml('samples/sample.xml', xpath="//doc[str[@name = 'file_type'][normalize-space(text()) = 'DLTINS']]/str[@name = 'download_link']")

    # assert 'sim' == df.columns

    # df.values.data

    # get column str as dict
    # df.to_dict('records')



    # print(set(df['str'].tolist()))
    # assert false

# def test_doc_zip():
#     """
#     Select all doc nodes that contains a str with a attribute named 'file_type' with a value 'DLTINS' and the gets str node link
#     """
#     xtree = et.parse('samples/DLTINS_20210118_01of01.xml')
#     xroot = xtree.getroot()

#     df_cols = ["FinInstrmGnlAttrbts.Id", "FinInstrmGnlAttrbts.FullNm", "FinInstrmGnlAttrbts.ClssfctnTp", "FinInstrmGnlAttrbts.CmmdtyDerivInd", "FinInstrmGnlAttrbts.NtnlCcy", "Issr"]
#     rows = []

#     namespaces = {
#         '': 'urn:iso:std:iso:20022:tech:xsd:auth.036.001.02',
#     }

#     for elem in xroot.findall('.//FinInstrm', namespaces):
#         s_issr = elem.find(".//Issr", namespaces).text if elem is not None else None

#         for fin_instrm_el in xroot.findall('.//FinInstrmGnlAttrbts', namespaces):

#             s_id = fin_instrm_el.find("Id", namespaces).text if fin_instrm_el is not None else None
#             s_full_nm = fin_instrm_el.find("FullNm", namespaces).text if fin_instrm_el is not None else None
#             s_clssfctn_tp = fin_instrm_el.find("ClssfctnTp", namespaces).text if fin_instrm_el is not None else None
#             s_cmmdty_deriv_ind = fin_instrm_el.find("CmmdtyDerivInd", namespaces).text if fin_instrm_el is not None else None
#             s_ctnl_ccy = fin_instrm_el.find("NtnlCcy", namespaces).text if fin_instrm_el is not None else None

#             rows.append({
#                 "FinInstrmGnlAttrbts.Id": s_id, 
#                 "FinInstrmGnlAttrbts.FullNm": s_full_nm,
#                 "FinInstrmGnlAttrbts.ClssfctnTp": s_clssfctn_tp,
#                 "FinInstrmGnlAttrbts.CmmdtyDerivInd": s_cmmdty_deriv_ind,
#                 "FinInstrmGnlAttrbts.NtnlCcy": s_ctnl_ccy,
#                 "Issr": s_issr,
#             })

#     out_df = pd.DataFrame(rows, columns = df_cols)

#     key = 'test'

#     out_df.to_csv(
#         f"s3://{AWS_S3_BUCKET}/{key}",
#         index=False,
#         storage_options={
#             "key": AWS_ACCESS_KEY_ID,
#             "secret": AWS_SECRET_ACCESS_KEY,
#             "token": AWS_SESSION_TOKEN,
#         },
#     )
#     assert false

# def test_doc_iterparse():

#     df_cols = ["FinInstrmGnlAttrbts.Id", "FinInstrmGnlAttrbts.FullNm", "FinInstrmGnlAttrbts.ClssfctnTp", "FinInstrmGnlAttrbts.CmmdtyDerivInd", "FinInstrmGnlAttrbts.NtnlCcy", "Issr"]

#     df = pd.DataFrame(columns=df_cols)

#     total = 0;
#     for rcrd in parse_rcrd('samples/DLTINS_20210118_01of01.xml'):
#         total = total + 1
#         # df.append(rcrd)
#         # print(df.values)
#         df1 = pd.DataFrame(rcrd, columns=df_cols, index=[total])
#         df = pd.concat([df, df1])

#     df.to_csv('samples/out.csv', sep=',', encoding='utf-8')

#     print(df.values)
    
#     print(f'Total {total}')
#     assert false
    

# def parse_rcrd(filename):
#     stack = []

#     for event, elem in et.iterparse(filename, events=('start','end')):
#         if event == 'start':
#             if elem.tag == tag('FinInstrm'):
#                 record = {
#                     'FinInstrmGnlAttrbts.Id': '',
#                     'FinInstrmGnlAttrbts.FullNm': '',            
#                     'FinInstrmGnlAttrbts.ClssfctnTp': '',            
#                     'FinInstrmGnlAttrbts.CmmdtyDerivInd': '',            
#                     'FinInstrmGnlAttrbts.NtnlCcy': '',            
#                     'Issr': '',            
#                 }
#             elif (elem.tag in [tag('Id'), tag('FullNm'), tag('ClssfctnTp'), tag('CmmdtyDerivInd'), tag('NtnlCcy')] and
#                   stack[-1] in [tag('FinInstrmGnlAttrbts')]):
#                 record[clean_tag(elem.tag)] = elem.text
#             elif (elem.tag in [tag('Issr')]):
#                 record[clean_tag(elem.tag)] = elem.text
#             # elif (elem.tag in ['Amt', 'Authrty', 'Ven'] and
#             #       stack[-1] in ['Attrbts']):
#             #     record[elem.tag] = elem.text
#             # elif (elem.tag in ['Dt'] and
#             #       stack[-1] == 'Prd' and stack[-2] == 'Attrbts'):
#             #     record[elem.tag] = elem.text

#             stack.append(elem.tag)
#         elif event == 'end':
#             if elem.tag == tag('FinInstrm'):
#                 yield record

#             stack.pop()

# df_cols = ["FinInstrmGnlAttrbts.Id", "FinInstrmGnlAttrbts.FullNm", "FinInstrmGnlAttrbts.ClssfctnTp", "FinInstrmGnlAttrbts.CmmdtyDerivInd", "FinInstrmGnlAttrbts.NtnlCcy", "Issr"]


# def tag(name: str) -> str:
#     return '{urn:iso:std:iso:20022:tech:xsd:auth.036.001.02}' + name.strip()

# def clean_tag(name: str) -> str:
#     cleaned = name.strip().replace('{urn:iso:std:iso:20022:tech:xsd:auth.036.001.02}', '')

#     if cleaned == 'Issr':
#         return cleaned;
    
#     return f'FinInstrmGnlAttrbts.{cleaned}'