#!/usr/bin/python3

import gspread
from google.oauth2.service_account import Credentials

# Connect to and open spreadsheet

"""
def connect_google_sheet(key):
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    credentials = Credentials.from_service_account_file('google-creds.json', scopes=scope)
    gc = gspread.authorize(credentials)
    return gc.open_by_key(key)


spreadsheet = connect_google_sheet('1CgtEvHIijdc_lzzg5dDdGYg3Np3MIi8lOUVxkhjj9bU')
test = { 'id':"NYAN", 'name':"64K Page Size", 'reads': 500, 'writes': 100, 'latency': 6 }

ws = spreadsheet.get_worksheet(0)

ws.append_row(list(test.values()))

print(ws.row_values(1))
print(ws.row_values(2))
"""


def connect(credentials_file):
    """ Connect to the google API using the credentials in the file specified.
        We return a connection on which operations can be performed. """

    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    credentials = Credentials.from_service_account_file(credentials_file, scopes=scope)
    return gspread.authorize(credentials)
    


def create(gconn, sheet_name, accounts):
    """ Creates a spreadsheet on the google connection with the given name, and then shares it with 
        the accounts specified (so that you can access it with your normal google docs account: something
        like ['harry@softiron.com']). 
        We return the spreadsheet. """
    
    sheet = gconn.create(sheet_name)

    for a in accounts:
        sheet.share(a, perm_type='user', role='writer')

    return sheet;


 
def open(gconn, sheet_name):
    """ Open an existing sheet on the google connection. """

    try:
        return gconn.open(sheet_name)
    except gspread.exceptions.SpreadsheetNotFound:
        return None



