#!/usr/bin/python3

"""Welcome to the Benchmaster 5000, for all your benchmarking needs.

Usage:
    benchmaster.py sheet create [-g FILE] <sheetname> <account> ...
    benchmaster.py s3 adduser <name> 
    benchmaster.py s3 test-write [-p PORT] [--s3-keyfile FILE] <bucket> <gateway>
    benchmaster.py s3 run [-p PORT] [-g FILE] [-w COUNT] [-s SIZE] [-o COUNT] [-r TIME] [-u TIME] [-d TIME] [--sheet NAME] [--s3-keyfile FILE] <name> <description> <gateway> ...
    benchmaster.py rados run [-g FILE] [-w COUNT] [-s SIZE] [-o COUNT] [-r TIME] [-u TIME] [-d TIME] [--sheet NAME] [--ceph-pool POOL] [--ceph-key KEY | --ceph-rootpw PW] <name> <description> <monitor> ...
    benchmaster.py -h | --help

    -h, --help                     Show usage
    -w, --workers COUNT            Number of workers. [default: 300]
    -s, --size SIZE                Object size to test. [default: 1M]
    -o, --objects COUNT            Number of objects in the pool.  This should be enough that workers are unlikely to contend.  [default: 5000]
    -r, --runtime TIME             Number of seconds for the test.  [default: 120]
    -u, --ramp-up TIME             Number of seconds at the start of the test where we do not record data.  [default: 20]
    -d, --ramp-down TIME           Number of seconds at the end of the test where we do not record data.  [default: 10]
    -p, --port PORT                Gateway port to connect to. [default: 80]
    -g, --google-credentials FILE  File containing Google Sheet credentials. [default: google-creds.json]

    --s3-keyfile FILE   File containing S3 keys. [default: s3.keys]
    --ceph-pool POOL    Ceph pool to use for rados testing. This MUST end in '1' because of Cosbench internals.  [default: cosbench1]
    --ceph-key KEY      Ceph key to use - can usually be found in /etc/ceph/ceph.client.admnin.keyring.
    --ceph-rootpw PW    Root password for the (first) ceph monitor so that we can grab the ceph.client.admin key.  [default: linux]
    --sheet NAME        Google spreadsheet name to which we will upload results.  
"""

import boto
import boto.s3.connection
import cosbench
import json
import re
import spreadsheet
import subprocess
import sys
import s3

from docopt import docopt
from datetime import datetime


def _sheet_create(args):
    sheet_name = args['<sheetname>']
    accounts = args['<account>']
    credentials = args['--google-credentials']

    print("Creating google spreadsheet {} with credentials from {} and sharing it with users: {}"
            .format(sheet_name, credentials, ', '.join(accounts)))
    
    conn = spreadsheet.connect(credentials)
    spreadsheet.create(conn, sheet_name, accounts)
    print("Done")



def _print_results(columns, rows):
    """ Output the results to screen, nicely formatted """

    print(' '.join("%-20s" % c[0] for c in columns))
    print('-' * (len(columns) * 20))

    for r in rows:
        print(' '.join("%-20s" % r[c[1]] for c in columns))



def _add_results_to_sheet(sheet, id, spec, description, columns, rows):
    """ Add the results to our spreadsheet (if we have one). """
    
    if sheet is None:
        print("No spreadsheet in use, skipping upload.")
        return

    print("Updating google spreadsheet")

    time = datetime.now()
    
    # Build up a list of columns - not just the columns we got back as results, but metadata too.
    scols = ['ID', 'Storage Type', 'Object Size', 'Object Count', 'Workers', 'Runtime', 'Ramp Up', 'Ramp Down', 'Gateways/Monitors']
    scols.extend(c[0] for c in columns)
    scols.extend(['Name', 'Description', 'Time'])
    spreadsheet.set_columns(sheet, scols)

    # Write the rows
    first = True
    for r in rows:
        srow = [id, spec.storage_type, spec.size, spec.object_count, spec.workers, spec.runtime, spec.ramp_up, spec.ramp_down, len(spec.targets)]
        srow.extend(r[c[1]] for c in columns)
        srow.extend([spec.name, description, time.strftime("%m/%d/%Y %H:%M:%S")])
        spreadsheet.append_row(sheet, srow, highlight=first)
        first = False



def _run(args, spec):
    spec.size = args['--size']
    spec.workers = int(args['--workers'])
    spec.object_count = int(args['--objects'])
    spec.runtime = int(args['--runtime'])
    spec.ramp_up = int(args['--ramp-up'])
    spec.ramp_down = int(args['--ramp-down'])
    spec.name = args['<name>']
    
    sheet_name = args['--sheet']
    credentials = args['--google-credentials']
    description = args['<description>']

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

    print("Generating test file: {}".format(spec.testfile))
    cosbench.generate_test(spec)

    print("Submitting test file: {}".format(spec.testfile))
    id = cosbench.submit(spec.testfile)
    print("Job submitted with ID: {}".format(id))  
    print("Waiting for job to complete\n")

    rows = cosbench.wait_for_results(id)

    # Define the order we wish to present the columns (to preserve consistency if CosBench alters the order).
    # Each entry here is a tuple with the Column Name we wish to present, and the CosBench name for it. 

    columns = [('Stage', 'Stage'),
               ('Bandwidth', 'Bandwidth'),
               ('100% Res Time', '100%-ResTime')]

    _print_results(columns, rows)
    _add_results_to_sheet(sheet, id, spec, description, columns, rows)

    print("\nDone")



def _s3_adduser(args):
    """ Create a new S3 user on the rados gatweways. """
    username = args['<name>']
    keyfile = args['--s3-keyfile']
    print("Adding user {} to rados gateways and storing keys in {}".format(username, keyfile))
    s3.add_user(username, keyfile)



def _s3_test_write(args):

    keyfile = args['--s3-keyfile']
    port = int(args['--port'])
    bucket_name = args['<bucket>']
    gateway = args['<gateway>'][0]

    secret_key, access_key = s3.load_keys(keyfile)

    conn = boto.connect_s3(
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            host = gateway,
            port = port,
            is_secure=False,
            calling_format = boto.s3.connection.OrdinaryCallingFormat(),
    )

    print("Connected")

    conn.create_bucket(bucket_name)

    bucket = conn.get_bucket(bucket_name)
    key = bucket.new_key("FooTheWomble")
    key.set_contents_from_string("Bar Squiggle Aardvark")



def _fetch_ceph_key(mon, rootpw):
    """ Fetch a key from a monitor """
    print("Fetching key from {}:/etc/ceph/ceph.client.admin.keyring".format(mon))
    cmd = 'sshpass -p ' + rootpw +' ssh -o StrictHostKeyChecking=no root@' + mon + " grep key /etc/ceph/ceph.client.admin.keyring | awk '{print $3}'"
    rc = subprocess.run(cmd, shell=True, capture_output=True, check=True)
    key = rc.stdout.decode("utf-8")[:-1]
    print("Found key: {}".format(key))
    return key



def _handle_s3(args):
    if args['adduser']:    _s3_adduser(args)
    if args['test-write']: _s3_test_write(args)

    if args['run']:
        secret_key, access_key = s3.load_keys(args['--s3-keyfile'])

        spec = cosbench.Spec("s3", secret_key, access_key, args['<gateway>'])
        spec.bucket_prefix = 'cosbench'
        spec.protocol = 'http'
        spec.port = args['--port']
        spec.do_create = True
        spec.do_dispose = True
        _run(args, spec)



def _handle_rados(args):
    if (args['run']):
        pool = args['--ceph-pool']
        if pool[-1] != '1':
            print("The ceph pool name MUST end in 1 (because of Cosbench internals")
            exit(-1)

        if args['--ceph-key'] is not None:
            key = args['--ceph-key']
        else:
            key = _fetch_ceph_key(args['<monitor>'][0], args['--ceph-rootpw'])

        spec = cosbench.Spec("librados", key, 'admin', args['<monitor>'])
        spec.bucket_prefix = pool[:-1]
        _run(args, spec)



def _handle_sheet(args):
    if args['create']: _sheet_create(args)


if __name__ == "__main__":
    args = docopt(__doc__, version='benchmaster 0.0.1', options_first=False)
    print(args)

    # Command handlers can dispatch to their sub-commands.
    if args['s3']:    _handle_s3(args)
    if args['rados']: _handle_rados(args)
    if args['sheet']: _handle_sheet(args)

