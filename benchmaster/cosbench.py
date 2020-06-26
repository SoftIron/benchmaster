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



class Spec:
    """ A Spec object contains all the parameters we need to generate a job XML file.
        It exists because the set of stuff was getting a bit to big to be handy. """

    storage_type = None
    name = None
    secret_key = None
    access_key = None
    size = None
    workers = None
    ramp_up = None
    ramp_down = None
    runtime = None
    targets = None
    total_ops = None
    bucket_prefix = None
    bucket_count = 1
    object_count = 5000
    kind = 'time'
    protocol = None
    port = None
    do_create = False
    do_dispose = False
    testfile = 'test.xml'

    def __init__(self, storage_type, secret_key, access_key, targets):
        self.storage_type = storage_type
        self.secret_key = secret_key
        self.access_key = access_key
        self.targets = targets

     


def _build_url(protocol, host, port):
    """ Protocol and port are optional """

    url = ""

    if protocol is not None:
        url = "{}://".format(protocol)

    url = "{}{}".format(url, host)

    if port is not None:
        url = "{}:{}".format(url, port)

    return url



def _header(spec):
    url = _build_url(spec.protocol, spec.targets[0], spec.port)
    time = datetime.now()
    
    result =  '<?xml version="1.0" encoding="UTF-8"?>\n'
    result += '<workload name="test" description="SoftIron Test Generated {}" config="">\n'.format(time)
    result += '  <storage type="{}" config="path_style_access=true;'.format(spec.storage_type)
    result += 'accesskey={};secretkey={};endpoint={}"/>\n'.format(spec.access_key, spec.secret_key, url)
    result += '  <workflow>\n\n'
    return result
   

 
def _bucket_creation(spec):
    result =  '    <!-- Bucket Creation Workstage: creates {} Bucket-->\n'.format(spec.bucket_count)
    result += '    <workstage name="bucket-create">\n'
    result += '      <work type="init" workers="1" config="cprefix={};containers=r(1,{})" />\n'.format(spec.bucket_prefix, spec.bucket_count)
    result += '    </workstage>\n\n'
    return result



def _storage(spec, target):
    url = _build_url(spec.protocol, target, spec.port)
    result =  '<storage type="{}" config="path_style_access=true;'.format(spec.storage_type)
    result += 'accesskey={};secretkey={};endpoint={}"/>\n'.format(spec.access_key, spec.secret_key, url)
    return result



def _prepare(spec):
    result =  '    <workstage name="prepare">\n'
    result += '      <work type="prepare" workers="{}" '.format(spec.workers)
    result += 'config="cprefix={};containers=r(1,{});'.format(spec.bucket_prefix, spec.bucket_count)
    result += 'oprefix=CB-;objects=r(1,{});'.format(spec.object_count)
    result += 'sizes=c({}){}B">\n'.format(spec.size[:-1], spec.size[-1:])
    result += '        {}'.format(_storage(spec, spec.targets[0]))
    result += '      </work>\n\n'
    result += '    </workstage>\n\n'
    return result



def _work(spec, test_type):
    result =  '    <!-- {} Workstage -->\n'.format(test_type)
    result += '    <workstage name="{}">\n'.format(test_type)
   
    for t in spec.targets: 
        url = _build_url(spec.protocol, t, spec.port)

        result += '      <work name="{}-{}" workers="{}" division="container" totalOps="{}">\n'.format(test_type, t, spec.workers, spec.ops)
        result += '        <storage type="{}" config="path_style_access=true;'.format(spec.storage_type)
        result += 'accesskey={};secretkey={};endpoint={}"/>\n'.format(spec.access_key, spec.secret_key, url)
        result += '        <operation type="{}" ratio="100" '.format(test_type)
        result += 'config="cprefix={};containers=c({})'.format(spec.bucket_prefix, spec.bucket_count)
        result += ';oprefix=Target1-;objects=r(1,4999);sizes=c({}){}B;content=zero"/>\n'.format(spec.size[:-1], spec.size[-1:])
        result += '      </work>\n\n'
    
    result += '    </workstage>\n\n'
    return result



def _work(spec, test_type):
    result =  '    <!-- {} Workstage -->\n'.format(test_type)
    result += '    <workstage name="{}">\n'.format(test_type)
   
    for t in spec.targets: 
        result += '      <work name="{}-{}" workers="{}" division="container" '.format(test_type, t, spec.workers)

        if spec.kind == 'time':
            result += 'runtime="{}" rampup="{}" rampdown="{}">\n'.format(spec.runtime, spec.ramp_up, spec.ramp_down)
        else:
            result += 'totalOps="{}">\n'.format(spec.total_ops)

        result += '        ' + _storage(spec, t)
        result += '        <operation type="{}" ratio="100" '.format(test_type)
        result += 'config="cprefix={};containers=c({});'.format(spec.bucket_prefix, spec.bucket_count)
        result += 'oprefix=CB-;objects=r(1,{});'.format(spec.object_count)
        result += 'sizes=c({}){}B;content=zero"/>\n'.format(spec.size[:-1], spec.size[-1:])
        result += '      </work>\n'
    
    result += '    </workstage>\n\n'
    return result



def _cleanup(spec):
    return ('    <workstage name="cleanup">\n'
            '      <work type="cleanup" workers="{}" config="cprefix={};containers=r(1,{});oprefix=CB-;objects=r(1,{});" />\n'
            '    </workstage>\n\n').format(spec.workers, spec.bucket_prefix, spec.bucket_count, spec.object_count)



def _dispose(spec):
    return ('    <workstage name="dispose">\n'
            '      <work type="dispose" workers="1" config="cprefix={};containers=r(1,{})" />\n'
            '    </workstage>\n\n').format(spec.bucket_prefix, spec.bucket_count)



def _footer():
    return '  </workflow>\n</workload>\n'




def generate_test(spec):
    """ Generate a single XML test file for Cosbench from a Spec object."""

    # Check that size is in the correct format
    if not re.match("\d+[KMG]", spec.size):
        print("Invalid object size: {}.  Sizes should digitas followed by K, M or G.".format(spec.size))
        exit(-1) 

    if spec.object_count < spec.workers * 10:
        print("Object count is less than (10 * workers) - we are likely to have contention.")
        exit(-1)

    # Generate the data
    workers_per_target = int(spec.workers / len(spec.targets))
    time = datetime.now()
    bucket_count = 1

    with open(spec.testfile, "w") as f:
        f.write(_header(spec))

        if spec.do_create:
            f.write(_bucket_creation(spec))

        if spec.kind == 'time':
            f.write(_prepare(spec))

        f.write(_work(spec, "write"))
        f.write(_work(spec, "read"))
        f.write(_cleanup(spec))
        
        if spec.do_dispose:
            f.write(_dispose(spec))

        f.write(_footer())


def _gbits_format(num):
    """ Turn bytes/s into gbits/s (with no unit added) """
    num = (num * 8.0) / (1024.0 * 1024.0 * 1024.0)
    return "%.2f" % num



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



class FieldSpec:
    def __init__(self, key, format_fn, should_average):
        self.key = key
        self.format_fn = format_fn
        self.should_average = should_average



def _process_results(filename):
    """ Gather the results from the file and return them as a list of rows, each of which is a list of maps. """

    # A set of regexes that we match against stage names.  We only record details for a stage if at least one matches.
    stages_of_interest = ['read', 'write']

    # The fields we are interested in
    field_specs = [
        FieldSpec('Bandwidth', _gbits_format, False),
        FieldSpec('95%-ResTime', None, True),
        FieldSpec('100%-ResTime', None, True)
    ]

    totals = {}
    counts = {}
    fields_by_column = {}
    
    with open(filename) as f:
        csv_reader = csv.DictReader(f)

        for row in csv_reader:
            for fs in field_specs:
                fields_by_column[fs.key] = fs

                if fs.key in row:
                    stage = row["Stage"]
                    if _match_at_least_one(stage, stages_of_interest, case_sensitive=False):
                        if stage not in totals:
                            totals[stage] = {}
                            counts[stage] = {}

                        if fs.key not in totals[stage]:
                            totals[stage][fs.key] = 0
                            counts[stage][fs.key] = 0

                        try:
                            totals[stage][fs.key] += float(row[fs.key])
                            counts[stage][fs.key] += 1
                        except:
                            pass

    results = {}

    for stage, values in totals.items():
        if stage.endswith('write'): prefix = 'Write '
        if stage.endswith('read'):  prefix = 'Read '

        for key, value in values.items():
            fs = fields_by_column[key]

            if fs.should_average:
                count = counts[stage][key]
                if count != 0:
                    value = value / count

            if fs.format_fn is not None:
                value = fs.format_fn(value)
            
            if prefix is not None:
                results[prefix + key] = str(value)

    return results



def submit(testfile):
    """ Submit a test to cosbench.  We return the Cosbench ID for the job so it can be cancelled etc... """

    cmd = ['{}/cli.sh'.format(_cosbench_dir), 'submit', '{}'.format(testfile)]
    result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=True)
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
    
