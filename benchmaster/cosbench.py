#!/usr/bin/python3

import csv
import glob
import json
import numbers
import os
import re
import subprocess
import time

from datetime import datetime



def _header(test_name, time, access_key, secret_key, gateway, port):
    url = "http://{}:{}".format(gateway, port)

    result =  '<?xml version="1.0" encoding="UTF-8"?>\n'
    result += '<workload name="{}" description="SoftIron Test Generated {}" config="">\n'.format(test_name, time)
    result += '  <storage type="s3" config="path_style_access=true;accesskey={};secretkey={};endpoint={}"/>\n'.format(access_key, secret_key, url)
    result += '  <workflow>\n\n'
    return result
   

 
def _bucket_creation(buckets):
    result =  '    <!-- Bucket Creation Workstage: creates {} Bucket-->\n'.format(buckets)
    result += '    <workstage name="bucket-create">\n'
    result += '      <work type="init" workers="1" config="cprefix=softironcosbench;containers=r(1,{})" />\n'.format(buckets)
    result += '    </workstage>\n\n'
    return result



def _work(test_type, buckets, objects, workers, operations, access_key, secret_key, gateways, port):
    result =  '    <!-- {} Workstage -->\n'.format(test_type)
    result += '    <workstage name="{}">\n'.format(test_type)
   
    for gw in gateways: 
        url = "http://{}:{}".format(gw, port)
        result += '      <work name="{}-{}" workers="{}" division="container" totalOps="{}">\n'.format(test_type, gw, workers, operations)
        result += '        <storage type="s3" config="path_style_access=true;accesskey={};secretkey={}'.format(access_key, secret_key)
        result += ';endpoint={}"/>\n'.format(url)
        result += '        <operation type="{}" ratio="100" config="cprefix=softironcosbench;containers=c({})'.format(test_type, buckets)
        result += ';oprefix=Target1-;objects=r(1,4999);sizes=c({}){}B;content=zero"/>\n'.format(objects[:-1], objects[-1:])
        result += '      </work>\n\n'
    
    result += '    </workstage>\n\n'
    return result



def _cleanup(workers, buckets):
    return ('    <workstage name="cleanup">\n'
            '      <work type="cleanup" workers="{}" config="cprefix=softironcosbench;containers=r(1,{});oprefix=Target1-;objects=r(1,4999);" />\n'
            '    </workstage>\n\n').format(workers, buckets)



def _dispose(buckets):
    return ('    <workstage name="dispose">\n'
            '      <work type="dispose" workers="1" config="cprefix=softironcosbench;containers=r(1,{})" />\n'
            '    </workstage>\n\n').format(buckets)



def _footer():
    return '  </workflow>\n</workload>\n'



def _load_keys(filename):
    """ Load the S3 keys from a json file. """

    try:
        with open(filename) as json_file:
            data = json.load(json_file)
            secret_key = data['secret_key']
            access_key = data['access_key']
            return (secret_key, access_key)
    except:
        print("Unable to read keys from file: " + filename)
        exit(-1)



def generate_test(name, testfile, keyfile, size, workers, ops, gateways, port):
    """ Generate a single XML test file for Cosbench."""

    # Check that size is in the correct format
    if not re.match("\d+[KMG]", size):
        print("Invalid object size: {}.  Sizes should digitas followed by K, M or G.".format(size))
        exit(-1) 

    secret_key, access_key = _load_keys(keyfile)
    
    # Generate the data
    workers_per_gw = int(workers / len(gateways))
    time = datetime.now()
    bucket = 1

    with open(testfile, "w") as f:
        f.write(_header(name, time, access_key, secret_key, gateways[0], port))
        f.write(_bucket_creation(bucket))
        f.write(_work("write", bucket, size, workers_per_gw, ops, access_key, secret_key, gateways, port))
        f.write(_work("read", bucket, size, workers_per_gw, ops, access_key, secret_key, gateways, port))
        f.write(_cleanup(workers_per_gw, bucket))
        f.write(_dispose(bucket))
        f.write(_footer())



def _rate_format(num, suffix='B/s'):
    for unit in ['','K','M','G','T','P','E','Z']:
        if abs(num) < 1024.0:
            return "%3.2f %s%s" % (num, unit, suffix)
        num /= 1024.0
    return "%.2f %s%s" % (num, 'Yi', suffix)



def _rate_format_bits(num, suffix='b/s'):
    num = num * 8
    for unit in ['','K','M','G','T','P','E','Z']:
        if abs(num) < 1024.0:
            return "%3.2f %s%s" % (num, unit, suffix)
        num /= 1024.0
    return "%.2f %s%s" % (num, 'Yi', suffix)



def _ms_format(num):
    return "{} ms".format(num)



def _process_results(filename):
    with open(filename) as results:
        csv_reader = csv.DictReader(results)
        of_interest = {'Bandwidth': _rate_format_bits, '100%-ResTime': _ms_format}
        averaged = ["100%-ResTime"]
        totals = {}
        counts = {}

        for row in csv_reader:
            for key in of_interest.keys():
                if key in row:
                    stage = row["Stage"]
                    if stage not in totals:
                        totals[stage] = {}
                        counts[stage] = {}

                    if key not in totals[stage]:
                        totals[stage][key] = 0
                        counts[stage][key] = 0

                    try:
                        totals[stage][key] += float(row[key])
                        counts[stage][key] += 1
                        t = totals[stage][key]
                        c = counts[stage][key]
                        v = float(row[key]) 
                    except:
                        pass
        
        for stage, values in totals.items():
            line = "Stage: %-20s" % stage
            for key, value in values.items():
                line += " " + key + ": "

                if key in averaged:
                    count = counts[stage][key]
                    if count != 0:
                        value = value / count

                fn = of_interest[key]
                if fn is None:
                    line += "%-20s" % str(value)
                else:
                    line += "%-20s" % str(fn(value))

            print(line)



def run_test(test_file):
    """ Run a test, blocking until it completes or fails. """

    cosbench_dir = 'cosbench_0.4.2.c4'

    cmd = ['{}/cli.sh'.format(cosbench_dir), 'submit', '{}'.format(test_file)]
    result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout = result.stdout.decode('utf-8')
    
    cosbench_id = stdout.split(": ")[1].rstrip("\n")

    print("Submitted with ID: {}.  Waiting for completion...".format(cosbench_id))

    while not glob.glob("{}/archive/{}-*".format(cosbench_dir, cosbench_id)):
        time.sleep(2)

    filepaths = glob.glob(os.path.join("{}/archive".format(cosbench_dir), '*{0}*/*{0}*.csv'.format(cosbench_id, cosbench_id)))

    filtered = []
    for fp in filepaths:
        if not "histogram" in fp:
            filtered.append(fp)

    if len(filtered) > 1:
        print("Too many workload files found:")
        for f in filtered:
            print(f)
        exit(-1)

    if len(filtered) < 1:
        print("Can't find a result CSV file for: {}".format(cosbench_id))
        exit(-1)

    _process_results(filtered[0])
    
