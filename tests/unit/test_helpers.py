import json

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