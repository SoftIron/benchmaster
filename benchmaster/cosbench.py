#!/usr/bin/python3

import csv
import glob
import json
import numbers
import os
import re
import s3
import subprocess
import time

from datetime import datetime

_cosbench_dir = '/usr/share/cosbench'


def _build_url(protocol, host, port):
    """ Protocol and port are optional """

    url = ""

    if protocol is not None:
        url = "{}://".format(protocol)

    url = "{}{}".format(url, host)

    if port is not None:
        url = "{}:{}".format(url, port)

    return url



def _header(storage_type, test_name, time, access_key, secret_key, target, protocol, port):
    url = _build_url(protocol, target, port)
    result =  '<?xml version="1.0" encoding="UTF-8"?>\n'
    result += '<workload name="{}" description="SoftIron Test Generated {}" config="">\n'.format(test_name, time)
    result += '  <storage type="{}" config="path_style_access=true;accesskey={};secretkey={};endpoint={}"/>\n'.format(storage_type, access_key, secret_key, url)
    result += '  <workflow>\n\n'
    return result
   

 
def _bucket_creation(bucket_prefix, bucket_count):
    result =  '    <!-- Bucket Creation Workstage: creates {} Bucket-->\n'.format(bucket_count)
    result += '    <workstage name="bucket-create">\n'
    result += '      <work type="init" workers="1" config="cprefix={};containers=r(1,{})" />\n'.format(bucket_prefix, bucket_count)
    result += '    </workstage>\n\n'
    return result



def _work(storage_type, test_type, objects, workers, operations, access_key, secret_key, targets, protocol, port, bucket_prefix, bucket_count):
    result =  '    <!-- {} Workstage -->\n'.format(test_type)
    result += '    <workstage name="{}">\n'.format(test_type)
   
    for t in targets: 
        url = _build_url(protocol, t, port)

        result += '      <work name="{}-{}" workers="{}" division="container" totalOps="{}">\n'.format(test_type, t, workers, operations)
        result += '        <storage type="{}" config="path_style_access=true;accesskey={};secretkey={}'.format(storage_type, access_key, secret_key)
        result += ';endpoint={}"/>\n'.format(url)
        result += '        <operation type="{}" ratio="100" config="cprefix={};containers=c({})'.format(test_type, bucket_prefix, bucket_count)
        result += ';oprefix=Target1-;objects=r(1,4999);sizes=c({}){}B;content=zero"/>\n'.format(objects[:-1], objects[-1:])
        result += '      </work>\n\n'
    
    result += '    </workstage>\n\n'
    return result



def _cleanup(workers, bucket_prefix, bucket_count):
    return ('    <workstage name="cleanup">\n'
            '      <work type="cleanup" workers="{}" config="cprefix={};containers=r(1,{});oprefix=Target1-;objects=r(1,4999);" />\n'
            '    </workstage>\n\n').format(workers, bucket_prefix, bucket_count)



def _dispose(bucket_prefix, bucket_count):
    return ('    <workstage name="dispose">\n'
            '      <work type="dispose" workers="1" config="cprefix={};containers=r(1,{})" />\n'
            '    </workstage>\n\n').format(bucket_prefix, bucket_count)



def _footer():
    return '  </workflow>\n</workload>\n'




def generate_test(storage_type, name, testfile, secret_key, access_key, size, workers, ops, targets, bucket_prefix, protocol, port, do_create, do_dispose):
    """ Generate a single XML test file for Cosbench."""

    # Check that size is in the correct format
    if not re.match("\d+[KMG]", size):
        print("Invalid object size: {}.  Sizes should digitas followed by K, M or G.".format(size))
        exit(-1) 

    # Generate the data
    workers_per_target = int(workers / len(targets))
    time = datetime.now()
    bucket_count = 1

    with open(testfile, "w") as f:
        f.write(_header(storage_type, name, time, access_key, secret_key, targets[0], protocol, port))

        if do_create:
            f.write(_bucket_creation(bucket_prefix, bucket_count))

        f.write(_work(storage_type, "write", size, workers_per_target, ops, access_key, secret_key, targets, protocol, port, bucket_prefix, bucket_count))
        f.write(_work(storage_type, "read", size, workers_per_target, ops, access_key, secret_key, targets, protocol, port, bucket_prefix, bucket_count))
        f.write(_cleanup(workers_per_target, bucket_prefix, bucket_count))
        
        if do_dispose:
            f.write(_dispose(bucket_prefix, bucket_count))

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



def _match_at_least_one(value, patterns, case_sensitive=True):
    """ Return true if the value matches at least one of the regex patterns."""

    flags = 0
    if not case_sensitive:
        flags = re.IGNORECASE

    for p in patterns:
        if re.search(p, value, flags) is not None:
            return True

    return False



def _process_results(filename):
    """ Gather the results from the file and return them as a list of rows, each of which is a list of maps. """

    # A set of regexes that we match against stage names.  We only record details for a stage if at least one matches.
    stages_of_interest = ['read', 'write']

    # The names of the fields we are interested in, mapping to the formatting functions we want to use for them.
    fields_of_interest = {'Bandwidth': _rate_format_bits, '100%-ResTime': _ms_format}

    # A list of fields that we want to average rather than accumulate.
    fields_to_average = ["100%-ResTime"]

    totals = {}
    counts = {}
    
    with open(filename) as f:
        csv_reader = csv.DictReader(f)

        for row in csv_reader:
            for key in fields_of_interest.keys():
                if key in row:
                    stage = row["Stage"]
                    if _match_at_least_one(stage, stages_of_interest, case_sensitive=False):
                        if stage not in totals:
                            totals[stage] = {}
                            counts[stage] = {}

                        if key not in totals[stage]:
                            totals[stage][key] = 0
                            counts[stage][key] = 0

                        try:
                            totals[stage][key] += float(row[key])
                            counts[stage][key] += 1
                        except:
                            pass

    results = [] 

    for stage, values in totals.items():
        row = {}
        row['Stage'] = stage;

        for key, value in values.items():
            if key in fields_to_average:
                count = counts[stage][key]
                if count != 0:
                    value = value / count

            fn = fields_of_interest[key]
            if fn is not None:
                value = fn(value)
            
            row[key] = str(value)
        
        results.append(row)

    return results



def submit(test_file):
    """ Submit a test to cosbench.  We return the Cosbench ID for the job so it can be cancelled etc... """

    cmd = ['{}/cli.sh'.format(_cosbench_dir), 'submit', '{}'.format(test_file)]
    result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout = result.stdout.decode('utf-8')
    
    return stdout.split(": ")[1].rstrip("\n")



def wait_for_results(cosbench_id):
    """ Wait for the job to complete (or fail) and then return the results as a list of rows, each 
        of which is a map from field name to value. """

    while not glob.glob("{}/archive/{}-*".format(_cosbench_dir, cosbench_id)):
        time.sleep(2)
    
    filepaths = glob.glob(os.path.join("{}/archive".format(_cosbench_dir), '*{0}*/*{0}*.csv'.format(cosbench_id, cosbench_id)))

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

    return _process_results(filtered[0])
    
