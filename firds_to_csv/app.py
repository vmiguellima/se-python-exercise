import json
import datetime
from typing import Union
import requests
# import pandas as pd

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

    xml = get_firsts_xml_from_url(url)

    return response({
            "message": "hello world",
            # "query": event["queryStringParameters"].get('q', ''),
            # "from": from_date,
            'url': url,
            "xml": xml,
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

def get_firsts_xml_from_url(url: str) -> str:
    """It gets xml from a given url

    Args:
        url (str): url with xml

    Returns:
        str: xml as text
    """
    response = requests.get(url)

    return response.text;

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