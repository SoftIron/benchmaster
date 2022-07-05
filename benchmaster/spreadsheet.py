// SPDX-FileCopyrightText: 2022 SoftIron Limited <info@softiron.com>
// SPDX-License-Identifier: GNU General Public License v2.0 only WITH Classpath exception 2.0
import gspread
import re

from google.oauth2.service_account import Credentials
from benchmaster.result import Result




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

    # Set up the column headings.

    set_format(sheet)

    for a in accounts:
        sheet.share(a, perm_type='user', role='writer')

    return sheet;

def set_format(sheet):
    set_headers(sheet)
    set_columns_size(sheet)
    set_columns_format(sheet)


def set_headers(sheet):
    columns = Result.columns()
    last_col = chr(ord('A') + len(columns) - 1)
    ws = sheet.get_worksheet(0)
    ws.update('A1', [columns], value_input_option="RAW")
    header_range = 'A1:{}1'.format(last_col)

    ws.format(header_range, {'textFormat': {'bold': True, "fontSize": 10}, "backgroundColor": { "red": 0.7, "green": 0.8, "blue": 1.0 }, "textRotation": {"angle": 60}})

def set_columns_size(sheet):
    ws = sheet.get_worksheet(0)

    # Autoresize columns
    sheet_id = ws._properties['sheetId']
    body = {
        "requests": [
            {
                "autoResizeDimensions": {
                    "dimensions": {
                        "sheetId": sheet_id,
                        "dimension": "COLUMNS",
                        "startIndex": 0,
                        "endIndex": len(Result.columns())
                    }
                }
            }
        ]
    }
    sheet.batch_update(body)


def set_columns_format(sheet):
    ws = sheet.get_worksheet(0)

    # Set up formatting for the data
    for i, (cell_colour, cell_format) in enumerate(zip(Result.backgrounds(), Result.formats())):
        column = chr(i + ord('A'))
        column_range = "{}2:{}999".format(column, column)
        if cell_format:
            ws.format(column_range,{"numberFormat":{"type":"NUMBER","pattern":cell_format}})

        if cell_colour:
            ws.format(column_range, {"backgroundColor": { "red": cell_colour[0], "green": cell_colour[1], "blue": cell_colour[2]}})


def open(gconn, sheet_name):
    """ Open an existing sheet on the google connection. """

    try:
        return gconn.open(sheet_name)
    except gspread.exceptions.SpreadsheetNotFound:
        return None



def append_result(sheet, result):
    """ Appends a row of values (one for each of the columns we provided to set_columns. """
    ws = sheet.get_worksheet(0)

    # The 'USER_ENTERED' flag means that things like dates and times will be picked up as such by the spreadsheet.
    # If we used the default value (or 'RAW') it would treat dates as strings.

    ws.append_row(result.values(), value_input_option='USER_ENTERED')   
    set_columns_size(sheet)
