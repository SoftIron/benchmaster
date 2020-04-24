#!/usr/bin/python3

"""Welcome to the Benchmaster 5000, for all your benchmarking needs.

Usage:
    benchmaster.py s3 adduser [--keyfile=<output>]<name>  
    benchmaster.py sheet [--credentials=<file>] create <sheetname> <account> ...
    benchmaster.py run [options] <name> <description> <gateway> ...
    benchmaster.py -h | --help

    -h --help           Show usage
    --keyfile FILE      File containing S3 keys. [default: s3.keys]
    --testfile FILE     Output file for cosbench testcase generation.  [default: test.xml]
    --credentials FILE  File containing Google Sheet credentials. [default: google-creds.json]
    --workers COUNT     Number of workers. [default: 300]
    --size SIZE         Object size to test. [default: 1M]
    --ops COUNT         Mumber of operations per workstage. [default: 100]
    --port PORT         Gateway port to connect to. [default: 80]
    --sheet NAME        Google spreadsheet name to which we will upload results.  
"""

import cosbench
import json
import re
import spreadsheet
import subprocess
import sys
import s3

from docopt import docopt
from datetime import datetime



def _handle_s3(args):
    if args['adduser']:
        username = args['<name>']
        keyfile = args['--keyfile']

        print("Adding user {} to rados gateways and storing keys in {}".format(username, keyfile))
        s3.add_user(username, keyfile)
        return



def _handle_sheet(args):
    if args['create']:
        sheet_name = args['<sheetname>']
        accounts = args['<account>']
        credentials = args['--credentials']

        print("Creating google spreadsheet {} with credentials from {} and sharing it with users: {}"
                .format(sheet_name, credentials, ', '.join(accounts)))
        
        conn = spreadsheet.connect(credentials)
        spreadsheet.create(conn, sheet_name, accounts)



def _print_results(columns, rows):
    """ Output the results to screen, nicely formatted """

    print(' '.join("%-20s" % c[0] for c in columns))
    print('-' * (len(columns) * 20))

    for r in rows:
        print(' '.join("%-20s" % r[c[1]] for c in columns))



def _add_results_to_sheet(sheet, id, name, description, columns, rows):
    """ Add the results to our spreadsheet (if we have one). """
    
    if sheet is None:
        print("No spreadsheet in use, skipping upload.")
        return

    time = datetime.now()

    # Build up a list of columns - not just the columns we got back as results, but metadata too.
    scols = ['ID']
    scols.extend(c[0] for c in columns)
    scols.extend(['Name', 'Description', 'Time'])
    spreadsheet.set_columns(sheet, scols)

    # Write the rows
    for r in rows:
        srow = [id]
        srow.extend(r[c[1]] for c in columns)
        srow.extend([name, description, time.strftime("%m/%d/%Y %H:%M:%S")])
        spreadsheet.append_row(sheet, srow)


def _handle_run(args):
    keyfile = args['--keyfile']
    testfile = args['--testfile']
    size = args['--size']
    workers = int(args['--workers'])
    ops = int(args['--ops'])
    port = int(args['--port'])
    sheet_name = args['--sheet']
    credentials = args['--credentials']

    name = args['<name>']
    description = args['<description>']
    gateways = args['<gateway>']

    if workers > ops:
        print("Workers must be less than the number of operations")
        exit(-1)

    gconn = None
    sheet = None

    # Check we can access the spreadsheet for our results (if we want to do that)
    if sheet_name is not None:
        print("Checking we can open google sheet '{}'".format(sheet_name))

        gconn = spreadsheet.connect(credentials)
        sheet = spreadsheet.open(gconn, sheet_name)
        if not sheet:
            print("Unable to open Google spreadsheet {}".format(sheet_name))
            exit(-1)

    print("Generating test file: {}".format(testfile))
    cosbench.generate_test(name, testfile, keyfile, size, workers, ops, gateways, port)

    id = cosbench.submit(testfile)
    print("Job submitted with ID: {}".format(id))  
    print("Waiting for job to complete\n")

    rows = cosbench.wait_for_results(id)

    # Define the order we wish to present the columns (to preserve consistency if CosBench alters the order).
    # Each entry here is a tuple with the Column Name we wish to present, and the CosBench name for it. 

    columns = [('Stage', 'Stage'),
               ('Bandwidth', 'Bandwidth'),
               ('100% Res Time', '100%-ResTime')]

    _print_results(columns, rows)
    _add_results_to_sheet(sheet, id, name, description, columns, rows)

    print("\nDone")



if __name__ == "__main__":
    args = docopt(__doc__, version='benchmaster 0.0.1', options_first=False)

    if args['s3']:    _handle_s3(args)
    if args['sheet']: _handle_sheet(args)
    if args['run']:   _handle_run(args)


