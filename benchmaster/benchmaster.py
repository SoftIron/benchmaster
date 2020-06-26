#!/usr/bin/python3

"""Welcome to the Benchmaster 5000, for all your benchmarking needs.

Usage:
    benchmaster.py sheet create [-g FILE] <sheetname> <account> ...
    benchmaster.py s3 adduser [--ceph-rootpw PW] <name> <gateway>
    benchmaster.py s3 test-write [-p PORT] [--s3-keyfile FILE] <bucket> <gateway>
    benchmaster.py s3 run-time [-v] [-p PORT] [-g FILE] [-w COUNT] [-s SIZE] [-o COUNT] [-r TIME] [-u TIME] [-d TIME] [--sheet NAME] [--s3-keyfile FILE] <description> <gateway> ...
    benchmaster.py s3 run-count [-v] [-p PORT] [-g FILE] [-w COUNT] [-s SIZE] [-o COUNT] [-c COUNT] [--sheet NAME] [--s3-keyfile FILE] <description> <gateway> ...
    benchmaster.py rados run-time [-v] [-g FILE] [-w COUNT] [-s SIZE] [-o COUNT] [-r TIME] [-u TIME] [-d TIME] [--sheet NAME] [--ceph-pool POOL] [--ceph-key KEY | --ceph-rootpw PW] <description> <monitor> ...
    benchmaster.py rados run-count [-v] [-g FILE] [-w COUNT] [-s SIZE] [-o COUNT] [-c COUNT] [--sheet NAME] [--ceph-pool POOL] [--ceph-key KEY | --ceph-rootpw PW] <description> <monitor> ...
    benchmaster.py -h | --help
    
    -h, --help                     Show usage
    -v, --verbose                  Show verbose output
    -w, --workers COUNT            Number of workers. [default: 500]
    -s, --size SIZE                Object size to test. [default: 1M]
    -o, --objects COUNT            Number of objects in the pool.  [default: 5000]
    -r, --runtime TIME             Number of seconds for the test.  [default: 120]
    -u, --ramp-up TIME             Number of seconds at the start of the test where we do not record data.  [default: 20]
    -d, --ramp-down TIME           Number of seconds at the end of the test where we do not record data.  [default: 10]
    -c, --count COUNT              Numboer of ops to perform in the test.  [default: 1000]
    -p, --port PORT                Gateway port to connect to. [default: 80]
    -g, --google-credentials FILE  File containing Google Sheet credentials. [default: google-creds.json]
    --s3-keyfile FILE   File containing S3 keys. [default: s3.keys]
    --ceph-pool POOL    Ceph pool to use for rados testing. This MUST end in '1' because of Cosbench internals.  [default: cosbench1]
    --ceph-key KEY      Ceph key to use - can usually be found in /etc/ceph/ceph.client.admnin.keyring.
    --ceph-rootpw PW    Root password for the ceph nodes so that we can grab the ceph.client.admin key.  [default: linux]
    --sheet NAME        Google spreadsheet name to which we will upload results.  
"""

import boto
import boto.s3.connection
import copy
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



def _print_results(columns, row):
    """ Output the results to screen, nicely formatted """
    print(' '.join("%-25s" % c[0] for c in columns))
    print('-' * (len(columns) * 25))
    print(' '.join("%-25s" % row[c[1]] for c in columns))



def _add_results_to_sheet(sheet, id, spec, description, columns, row):
    """ Add the results to our spreadsheet (if we have one). """
    
    if sheet is None:
        print("No spreadsheet in use, skipping upload.")
        return

    print("Updating google spreadsheet")

    time = datetime.now()
    
    # Build up a list of columns - not just the columns we got back as results, but metadata too.
    scols = ['ID', 'Storage Type', 'Object Size', 'Object Pool', 'Workers', 'Schedule', 'Gateways/Monitors']
    scols.extend(c[0] for c in columns)
    scols.extend(['Description', 'Time'])
    spreadsheet.set_columns(sheet, scols)

    # Write the rows
    if spec.kind == 'time':
        schedule = 'Time: r{}, u{}, d{}'.format(spec.runtime, spec.ramp_up, spec.ramp_down)
    else:
        schedule = 'Count: {}'.format(spec.total_ops)

    srow = [id, spec.storage_type, spec.size, spec.object_count, spec.workers, schedule, len(spec.targets)]
    srow.extend(row[c[1]] for c in columns)
    srow.extend([description, time.strftime("%m/%d/%Y %H:%M:%S")])
    spreadsheet.append_row(sheet, srow)



def _run(args, spec):
    spec.size = args['--size']
    spec.workers = int(args['--workers'])
    spec.object_count = int(args['--objects'])
    spec.runtime = int(args['--runtime'])
    spec.ramp_up = int(args['--ramp-up'])
    spec.ramp_down = int(args['--ramp-down'])
    spec.total_ops = int(args['--count'])

    if args['run-time']:
        spec.kind = 'time'
    else:
        spec.kind = 'count'
    
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

    columns = [('Wr Bandwidth (Gb/s)', 'Write Bandwidth'),
               ('Wr 95% Res Time (ms)', 'Write 100%-ResTime'),
               ('Wr 100% Res Time (ms)', 'Write 100%-ResTime'),
               ('Rd Bandwidth (Gb/s)', 'Read Bandwidth'),
               ('Rd 95% Res Time (ms)', 'Read 100%-ResTime'),
               ('Rd 100% Res Time (ms)', 'Read 100%-ResTime')]

    _print_results(columns, rows)
    _add_results_to_sheet(sheet, id, spec, description, columns, rows)
    print("\nDone")



def _run_sweep(args, spec, sweepable, original_sweepable):
    if len(sweepable) == 0:   
        msg = 'Performing sweep with {'
        comma = False 
        for s in original_sweepable:
            if comma: msg += ', '
            comma = True
            msg += '{}={}'.format(s, args[s])

        msg += '}'
        print(msg)
        _run(args, spec)
        return

    current = sweepable[0]
    remaining = sweepable[1:]

    for value in args[current].split(','):
        args_copy = copy.copy(args)
        args_copy[current] = value
        _run_sweep(args_copy, spec, remaining, original_sweepable)



def _sweepables(args):
    """ Return a list of the sweepable parameteres for a run """

    if args['run-time']:
        return ['--objects', '--ramp-down', '--ramp-up', '--runtime', '--size', '--workers']
    else:
        return ['--objects', '--count', '--size', '--workers']
        


def _s3_adduser(args):
    """ Create a new S3 user on the rados gatweways. """
    username = args['<name>']
    keyfile = args['--s3-keyfile']
    gateway = args['<gateway>'][0]
    password = args['--ceph-rootpw']
    
    print("Adding user {} on rados gateway {} and storing keys in {}".format(username, gateway, keyfile))
    s3.add_user(username, keyfile, gateway, password)



def _s3_test_write(args):
    """ Try writing a single object to S3 """
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



def _s3_run(args):
    """ Run a benchmark sweep on s3 """
    secret_key, access_key = s3.load_keys(args['--s3-keyfile'])

    spec = cosbench.Spec("s3", secret_key, access_key, args['<gateway>'])
    spec.bucket_prefix = 'cosbench'
    spec.protocol = 'http'
    spec.port = args['--port']
    spec.do_create = True
    spec.do_dispose = True

    sweepable = _sweepables(args)
    _run_sweep(args, spec, sweepable, sweepable)



def _fetch_ceph_key(mon, rootpw):
    """ Fetch a key from a monitor """
    print("Fetching key from {}:/etc/ceph/ceph.client.admin.keyring".format(mon))
    cmd =  'sshpass -p ' + rootpw +' ssh -o StrictHostKeyChecking=no root@' + mon 
    cmd += " grep key /etc/ceph/ceph.client.admin.keyring | awk '{print $3}'"
    rc = subprocess.run(cmd, shell=True, capture_output=True, check=True)
    key = rc.stdout.decode("utf-8")[:-1]
    print("Found key: {}".format(key))
    return key



def _rados_run(args):
    """ Run a benchmark sweep on rados """
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

    sweepable = _sweepables(args)
    _run_sweep(args, spec, sweepable, sweepable)



def _handle_s3(args):
    if args['adduser']:    _s3_adduser(args)
    if args['test-write']: _s3_test_write(args)
    if args['run-time'] or args['run-count']: _s3_run(args)



def _handle_rados(args):
    if args['run-time'] or args['run-count']: _rados_run(args)



def _handle_sheet(args):
    if args['create']: _sheet_create(args)



if __name__ == "__main__":
    args = docopt(__doc__, version='benchmaster 0.0.1', options_first=False)
    if args['--verbose']:
        print(args)

    # Command handlers can dispatch to their sub-commands.
    if args['s3']:    _handle_s3(args)
    if args['rados']: _handle_rados(args)
    if args['sheet']: _handle_sheet(args)

