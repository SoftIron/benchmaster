#!/usr/bin/python3

"""Welcome to the Benchmaster 5000, for all your benchmarking needs.

Usage:
    benchmaster.py sheet create        [-v] [-g FILE] <sheetname> <account> ...
    benchmaster.py s3 adduser          [-v] [--ceph-root-password PW] <name> <gateway>
    benchmaster.py s3 test-write       [-v] [--s3-port PORT] [--s3-bucket BUCKET] [--s3-credentials FILE] <gateway>
    benchmaster.py s3 cosbench ops     [-v] [-s SIZE] [-c COUNT] [-x MIX]
                                       [--sheet NAME] [-g FILE]
                                       [--s3-bucket BUCKET] [--s3-credentials FILE] [--s3-port PORT]
                                       [--cosbench-op-count COUNT] [--cosbench-workers COUNT] [--cosbench-xmlfile FILE]
                                       <description> <gateway> ...
    benchmaster.py s3 cosbench time    [-v] [-s SIZE] [-c COUNT] [-r TIME] [-u TIME] [-d TIME] [-x MIX]
                                       [--sheet NAME] [-g FILE]
                                       [--s3-bucket BUCKET] [--s3-credentials FILE] [--s3-port PORT]
                                       [--cosbench-workers COUNT] [--cosbench-xmlfile FILE]
                                       <description> <gateway> ...
    benchmaster.py s3 sibench time     [-v] [-s SIZE] [-c COUNT] [-r TIME] [-u TIME] [-d TIME] [-x MIX]
                                       [--sheet NAME] [-g FILE]
                                       [--s3-bucket BUCKET] [--s3-credentials FILE] [--s3-port PORT]
                                       [--sibench-workers FACTOR] [--sibench-port PORT] [--sibench-bandwidth BW] [--sibench-servers SERVERS]
                                       [--sibench-skip-read-verification]
                                       <description> <gateway> ...
    benchmaster.py rados cosbench ops  [-v] [-s SIZE] [-c COUNT] [-x MIX]
                                       [--sheet NAME] [-g FILE]
                                       [--ceph-pool POOL] [--ceph-user USER --ceph-key KEY | --ceph-root-password PW]
                                       [--cosbench-op-count COUNT] [--cosbench-workers COUNT] [--cosbench-xmlfile FILE]
                                       <description> <monitor> ...
    benchmaster.py rados cosbench time [-v] [-s SIZE] [-c COUNT] [-r TIME] [-u TIME] [-d TIME] [-x MIX]
                                       [--sheet NAME] [-g FILE]
                                       [--ceph-pool POOL] [--ceph-user USER --ceph-key KEY | --ceph-root-password PW]
                                       [--cosbench-workers COUNT] [--cosbench-xmlfile FILE]
                                       <description> <monitor> ...
    benchmaster.py rados sibench time  [-v] [-s SIZE] [-c COUNT] [-r TIME] [-u TIME] [-d TIME] [-x MIX]
                                       [--sheet NAME] [-g FILE]
                                       [--ceph-pool POOL] [--ceph-user user --ceph-key key | --ceph-root-password PW]
                                       [--sibench-workers FACTOR] [--sibench-port PORT] [--sibench-bandwidth BW] [--sibench-servers SERVERS]
                                       [--sibench-skip-read-verification]
                                       <description> <monitor> ...
    benchmaster.py rbd sibench time    [-v] [-s SIZE] [-c COUNT] [-r TIME] [-u TIME] [-d TIME] [-x MIX]
                                       [--sheet NAME] [-g FILE]
                                       [--ceph-pool POOL] [--ceph-datapool POOL] [--ceph-user user --ceph-key key | --ceph-root-password PW]
                                       [--sibench-workers FACTOR] [--sibench-port PORT] [--sibench-bandwidth BW] [--sibench-servers SERVERS]
                                       [--sibench-skip-read-verification]
                                       <description> <monitor> ...
    benchmaster.py cephfs sibench time [-v] [-s SIZE] [-c COUNT] [-r TIME] [-u TIME] [-d TIME] [-x MIX]
                                       [--sheet NAME] [-g FILE]
                                       [--ceph-dir DIR] [--ceph-user USER --ceph-key KEY | --ceph-root-password PW]
                                       [--sibench-workers FACTOR] [--sibench-port PORT] [--sibench-bandwidth BW] [--sibench-servers SERVERS]
                                       [--sibench-skip-read-verification]
                                       <description> <monitor> ...
    benchmaster.py block sibench time  [-v] [-s SIZE] [-c COUNT] [-r TIME] [-u TIME] [-d TIME] [-x MIX]
                                       [--sheet NAME] [-g FILE]
                                       [--sibench-workers FACTOR] [--sibench-port PORT] [--sibench-bandwidth BW] [--sibench-servers SERVERS]
                                       [--sibench-skip-read-verification]
                                       <description> <block-device>
    benchmaster.py FILE sibench time   [-v] [-s SIZE] [-c COUNT] [-r TIME] [-u TIME] [-d TIME] [-x MIX]
                                       [--sheet NAME] [-g FILE]
                                       [--sibench-workers FACTOR] [--sibench-port PORT] [--sibench-bandwidth BW] [--sibench-servers SERVERS]
                                       [--sibench-skip-read-verification]
                                       <description> <file-dir>
    benchmaster.py iscsi setup         [-v] 
                                       [--iscsi-image-size SIZE] [--iscsi-device-link LINK]
                                       [--ceph-pool POOL] [--ceph-root-password PW]
                                       [--sibench-servers SERVERS] [--sibench-root-password PW]
                                       <gateway> ...
    benchmaster.py iscsi teardown      [-v] 
                                       [--iscsi-device-link LINK]
                                       [--ceph-pool POOL] [--ceph-root-password PW]
                                       [--sibench-servers SERVERS] [--sibench-root-password PW]
                                       <gateway> ...
    benchmaster.py -h | --help

Options:
    -h, --help                        Show usage
    -v, --verbose                     Show verbose output
    -s, --object-size SIZE            Size oc the objects in the test                           sweepable  [default: 1M]
    -c, --object-count COUNT          Number of objects in the test                             sweepable  [default: 5000]
    -r, --run-time TIME               Seconds for the test (does not include ramp up/down)      sweepable  [default: 120]
    -u, --ramp-up TIME                Seconds at start of test where we do not record           sweepable  [default: 20]
    -d, --ramp-down TIME              Seconds at end of test where we do not record             sweepable  [default: 10]
    -x, --read-write-mix MIX          Percentage of reads, or 0 for separate read/write passes  sweepable  [default: 0]
    -g, --google-credentials FILE     File containing Google Sheet credentials                             [default: gcreds.json]
    --sheet NAME                      Google spreadsheet to which we will upload results  
    --cosbench-op-count COUNT         Numboer of ops to perform in the test                     sweepable  [default: 1000]
    --cosbench-workers COUNT          The number of workers to use for cosbench                 sweepable  [default: 500]
    --cosbench-xmlfile FILE           The name of the XML file to write out for Cosbench                   [default: cosbench.xml]
    --sibench-servers SERVERS         A comma-separated list of sibench servers                            [default: localhost]
    --sibench-port PORT               The port on which to connect to the sibench servers                  [default: 5150]
    --sibench-bandwidth BW            The bandwidth limit in units of K, M or G bits/s          sweepable  [default: 0]
    --sibench-workers FACTOR          Workers per server = factor x no of cores.                sweepable  [default: 1.0]
    --sibench-skip-read-verification  Disable read validation for speed.    
    --sibench-root-password PW        Root password for the sibench servers                                [default: linux]
    --s3-credentials FILE             File containing S3 keys                                              [default: s3creds.json]
    --s3-port PORT                    The port on which to connect to the S3 gateways                      [default: 7480]
    --s3-bucket BUCKET                The bucket to use to on S3                                           [default: benchmark]
    --ceph-pool POOL                  Ceph pool to use. MUST end in '1' if using Cosbench                  [default: benchmark]
    --ceph-datapool POOL              Ceph pool to use for non-metadata when using RBD with EC
    --ceph-user USER                  Ceph user for rados testing                                          [default: admin]
    --ceph-root-password PW           Root password for ceph nodes to fetch keys or create ueers           [default: linux]
    --ceph-key KEY                    Ceph key, normally from /etc/ceph/ceph.client.admnin.keyring
    --ceph-dir DIR                    Directory in a CephFS filesystem to use                              [default: benchmark]
    --iscsi-image-size SIZE           Size of the RBD images we create for iscsi to mount                  [default: 1G]
    --iscsi-device-link LINK          Link to create on the sibench servers to mount iscsi                 [default: /tmp/sibench-iscsi]
"""

import boto
import boto.s3.connection
import copy
import cosbench
import iscsi
import json
import re
import pprint
import spec
import spreadsheet
import subprocess
import sys
import s3

from docopt import docopt
from datetime import datetime


def _sheet_create(args):
    """ Create a new GoogleSheets spreadsheet. """

    sheet_name = args['<sheetname>']
    accounts = args['<account>']
    credentials = args['--google-credentials']

    print("Creating google spreadsheet {} with credentials from {} and sharing it with users: {}"
            .format(sheet_name, credentials, ', '.join(accounts)))
    
    conn = spreadsheet.connect(credentials)
    spreadsheet.create(conn, sheet_name, accounts)
    print("Done")



def _run_single(args, spec):
    """  Runs a single benchmark (usually as part of a sweep). """

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

    start_time = datetime.now()
    result = spec.run() 
    result.start_time = str(start_time)
    result.end_time = str(datetime.now())

    # Use a quick json decode/encode to allow easy pretty printing.  
    # We're not actually using the json here.
    print("Result:\n" + json.dumps(json.loads(str(result).replace("'", '"')), indent=3))

    if sheet is None:
        print("No spreadsheet in use, skipping upload.")
    else:
        print("Uploading to google spreadsheet")
        spreadsheet.append_result(sheet, result)



def _run_sweep(args):
    """ Run a sweep of benchmarks. """

    # Make a spec from our arguments.
    spec = _make_spec(args)

    # Flatten the spec (which may define a sweep) into a list of simple specs, and run run them.
    for s in spec.flatten():
        # Use json as a convenient way to pretty print a heirarchical class structure.
        jstr = str(s).replace("'", '"').replace('False', 'false').replace('True', 'true')
        print(jstr)
        print("Running Benchmark:\n" + json.dumps(json.loads(jstr), indent=3))
        _run_single(args, s)
    exit(0)



def _make_protocol_spec(args):
    """ Parse out the protocol specific parts of our command line arguments. """

    if args['s3']:
        secret_key, access_key = s3.load_keys(args['--s3-credentials'])
        return spec.S3Spec(access_key, secret_key, args['--s3-port'], args['--s3-bucket'], args['<gateway>'])
    
    if args['rados'] or args['cephfs'] or args['rbd']:
        # All of these protocol handle keys the same way

        if args['--ceph-key'] is not None:
            key = args['--ceph-key']
            user = args['--ceph-user']
        else:
            key = _fetch_ceph_key(args['<monitor>'][0], args['--ceph-root-password'])
            user = 'admin'

    if args['rados']:
        return spec.RadosSpec(user, key, args['--ceph-pool'], args['<monitor>'])

    if args['rbd']:
        if not args['--ceph-datapool']:
            args['--ceph-datapool'] = ''

        return spec.RbdSpec(user, key, args['--ceph-pool'], args['--ceph-datapool'], args['<monitor>'])

    if args['cephfs']:
        return spec.CephFSSpec(user, key, args['--ceph-dir'], args['<monitor>'])

    if args['block']:
        return spec.BlockSpec(args['<block-device>'])

    if args['file']:
        return spec.FileSpec(args['<file-dir>'])

    print("Not a known protocol")
    exit(-1)



def _make_backend_spec(args):
    """ Parse out the backend specific parts of our command line arguments. """

    if args['cosbench']: return spec.CosbenchSpec(
            args['--cosbench-workers'],
            args['--cosbench-xmlfile'])

    if args['sibench']:  return spec.SibenchSpec(
            args['--sibench-port'], 
            args['--sibench-servers'].split(','), 
            args['--sibench-bandwidth'],
            args['--sibench-workers'],
            args['--sibench-skip-read-verification'])

    print("Not a known backend")
    exit(-1)



def _make_runtype_spec(args):
    """ Parse our the runtyoe specific parts of our command line arguments. """

    if args['ops']:  return spec.OpsSpec(args['--op-count'])
    if args['time']: return spec.TimeSpec(args['--run-time'], args['--ramp-up'], args['--ramp-down'])
    print("Not a known run type")
    exit(-1)



def _make_spec(args):
    """ Build a run spec from our command line arguments. """
    return spec.Spec(
            _make_runtype_spec(args), 
            _make_backend_spec(args), 
            _make_protocol_spec(args), 
            args['--object-size'], 
            args['--object-count'],
            args['--read-write-mix'],
            args['<description>'])



def _s3_adduser(args):
    """ Create a new S3 user on the rados gatweways. """
    username = args['<name>']
    cred_file = args['--s3-credentials']
    gateway = args['<gateway>'][0]
    password = args['--ceph-root-password']
    
    print("Adding user {} on rados gateway {} and storing keys in {}".format(username, gateway, cred_file))
    s3.add_user(username, cred_file, gateway, password)



def _s3_test_write(args):
    """ Try writing a single object to S3 """
    cred_file = args['--s3-credentials']
    port = int(args['--s3-port'])
    bucket_name = args['--s3-bucket']
    gateway = args['<gateway>'][0]

    secret_key, access_key = s3.load_keys(cred_file)

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
    cmd =  'sshpass -p ' + rootpw +' ssh -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no root@' + mon 
    cmd += " grep key /etc/ceph/ceph.client.admin.keyring | awk '{print $3}'"

    rc = subprocess.run(cmd, shell=True, capture_output=True, check=True)
    out = rc.stdout.decode("utf-8")
    key = out[:-1]

    if key == '':
        print("Unable to fetch key: " + rc.stderr.decode('utf-8'))
        exit(-1)

    print("Found key: {}".format(key))
    return key




def _handle_s3(args):
    if   args['time']:       _run_sweep(args)
    elif args['ops']:        _run_sweep(args)
    elif args['adduser']:    _s3_adduser(args)
    elif args['test-write']: _s3_test_write(args)


def _handle_rados(args):
    if   args['time']:       _run_sweep(args)
    elif args['ops']:        _run_sweep(args)


def _handle_rbd(args):
    if   args['time']:       _run_sweep(args)


def _handle_cephfs(args):
    if args['time']:         _run_sweep(args)


def _handle_block(args):
    if args['time']:         _run_sweep(args)


def _handle_file(args):
    if args['time']:         _run_sweep(args)


def _handle_sheet(args):
    if args['create']:       _sheet_create(args)


def _handle_iscsi(args):
    iargs = iscsi.IscsiArgs(
            args['<gateway>'],
            args['--ceph-root-password'],
            args['--sibench-servers'].split(','),
            args['--sibench-root-password'],
            args['--ceph-pool'],
            args['--iscsi-image-size'],
            args['--iscsi-device-link'])

    if   args['setup']:     iscsi.setup(iargs)
    elif args['teardown']:  iscsi.teardown(iargs)



if __name__ == "__main__":
    args = docopt(__doc__, version='benchmaster 1.0.0', options_first=False)

    if args['--verbose']:
        print(args)

    # Command handlers can dispatch to their sub-commands handlers.
    if args['s3']:        _handle_s3(args)
    elif args['rados']:   _handle_rados(args)
    elif args['rbd']:     _handle_rbd(args)
    elif args['cephfs']:  _handle_cephfs(args)
    elif args['sheet']:   _handle_sheet(args)
    elif args['block']:   _handle_block(args)
    elif args['file']:    _handle_file(args)
    elif args['iscsi']:   _handle_iscsi(args)

