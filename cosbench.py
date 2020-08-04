import csv
import glob
import json
import numbers
import os
import re
import s3
import spec
import subprocess
import time

from datetime import datetime
from result import Result, DirectionResult

_cosbench_dir = '/usr/share/cosbench'


class CosbenchValues:
    """ Pull out all the stuff we need from the spec and convert to Cosbench's view of the world. """

    def __init__(self, spec):
        if not re.match("\d+[KMG]", spec.size):
            print("Invalid object size for cosbench: {}.  Sizes should be digits followed by K, M or G.".format(spec.size))
            exit(-1) 
        
        # Extract our common info
        self.size_digits = spec.size[:-1]
        self.size_units = spec.size[-1:]
        self.object_count = spec.object_count
        self.xml_file = spec.backend.xml_file
        self.workers = spec.backend.workers()
        self.targets = spec.protocol.targets()

        # Extract our runtype info
        r = spec.runtype

        if r.name() == "time":
            self.runtype = 'runtime="{}" rampup="{}" rampdown="{}"'.format(r.runtime, r.ramp_up, r.ramp_down)

        elif r.name() == "ops":
            self.runtype = 'totalOps="{}"'.format(r.ops)
           
        else:
            print("Unknown runtype for Cosbench {}".format(r.name()))
            exit(-1)
       
        # Extract our protocol info 
        p = spec.protocol

        if p.name() == 's3':
            self.storage_type = "s3"
            self.url_protocol = "http"
            self.access_key = p.access_key
            self.secret_key = p.secret_key
            self.container_prefix = p.bucket
            self.do_create = True
            self.do_dispose = True
            self.port = p.port

        elif p.name() == 'rados':  
            if p.pool[-1] != '1':
                print("The pool name must end in '1' if using Cosbench as a backend")
                exit(-1)

            self.storage_type = "librados"
            self.url_protocol = None
            self.access_key = p.user
            self.secret_key = p.key
            self.container_prefix = p.pool[:-1]
            self.do_create = False
            self.do_dispose = False
            self.port = None

        else:
            print("Unknown protocol for Cosbench: {}".format(p.name()))
            exit(-1)



def _build_url(protocol, host, port):
    """ Protocol and port are optional """

    url = ""
    if protocol is not None: url = "{}://".format(protocol)

    url = "{}{}".format(url, host)

    if port is not None: url = "{}:{}".format(url, port)

    return url



def _header(cv):
    url = _build_url(cv.url_protocol, cv.targets[0], cv.port)
    time = datetime.now()
    
    result =  '<?xml version="1.0" encoding="UTF-8"?>\n'
    result += '<workload name="test" description="SoftIron Test Generated {}" config="">\n'.format(time)
    result += '  <storage type="{}" config="path_style_access=true;'.format(cv.storage_type)
    result += 'accesskey={};secretkey={};endpoint={}"/>\n'.format(cv.access_key, cv.secret_key, url)
    result += '  <workflow>\n\n'
    return result
   

 
def _bucket_creation(cv):
    result =  '    <!-- Bucket Creation Workstage: creates 1 Bucket-->\n'
    result += '    <workstage name="bucket-create">\n'
    result += '      <work type="init" workers="1" config="cprefix={};containers=r(1,1)" />\n'.format(cv.container_prefix)
    result += '    </workstage>\n\n'
    return result



def _storage(cv, target):
    url = _build_url(cv.url_protocol, target, cv.port)
    result =  '<storage type="{}" config="path_style_access=true;'.format(cv.storage_type)
    result += 'accesskey={};secretkey={};endpoint={}"/>\n'.format(cv.access_key, cv.secret_key, url)
    return result



def _prepare(cv):
    result =  '    <workstage name="prepare">\n'
    result += '      <work type="prepare" workers="{}" '.format(cv.workers)
    result += 'config="cprefix={};containers=r(1,1;'.format(cv.container_prefix)
    result += 'oprefix=CB-;objects=r(1,{});'.format(cv.object_count)
    result += 'sizes=c({}){}B">\n'.format(cv.size_digits, cv.size_units)
    result += '        {}'.format(_storage(cv, cv.targets[0]))
    result += '      </work>\n\n'
    result += '    </workstage>\n\n'
    return result



def _work(cv, test_type):
    result =  '    <!-- {} Workstage -->\n'.format(test_type)
    result += '    <workstage name="{}">\n'.format(test_type)
   
    for t in cv.targets: 
        result += '      <work name="{}-{}" workers="{}" division="container" '.format(test_type, t, cv.workers)
        result += cv.runtype + '>\n'
        result += '        ' + _storage(cv, t)
        result += '        <operation type="{}" ratio="100" '.format(test_type)
        result += 'config="cprefix={};containers=c(1);'.format(cv.container_prefix)
        result += 'oprefix=CB-;objects=r(1,{});'.format(cv.object_count)
        result += 'sizes=c({}){}B;content=zero"/>\n'.format(cv.size_digits, cv.size_units)
        result += '      </work>\n'
    
    result += '    </workstage>\n\n'
    return result



def _cleanup(cv):
    return ('    <workstage name="cleanup">\n'
            '      <work type="cleanup" workers="{}" config="cprefix={};containers=r(1,1);oprefix=CB-;objects=r(1,{});" />\n'
            '    </workstage>\n\n').format(cv.workers, cv.container_prefix, cv.object_count)



def _dispose(cv):
    return ('    <workstage name="dispose">\n'
            '      <work type="dispose" workers="1" config="cprefix={};containers=r(1,1)" />\n'
            '    </workstage>\n\n').format(cv.container_prefix)



def _footer():
    return '  </workflow>\n</workload>\n'




def _generate_xml(cv):
    """ Generate a single XML test file for Cosbench from a Spec object."""

    print("Generating test file: " + cv.xml_file)

    with open(cv.xml_file, "w") as f:
        f.write(_header(cv))
        if cv.do_create: f.write(_bucket_creation(cv))
        if cv.runtype == 'time': f.write(_prepare(cv))
        f.write(_work(cv, "write"))
        f.write(_work(cv, "read"))
        f.write(_cleanup(cv))
        if cv.do_dispose: f.write(_dispose(cv))
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



def _strip_units(num):
    """ Strip out non-digit (or decimal point) chars and then try to convert to float """
    return float(re.findall(r"[0-9.]+", num)[0])



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
    def __init__(self, key, preprocess_fn, format_fn, should_average):
        self.key = key
        self.preprocess_fn = preprocess_fn
        self.format_fn = format_fn
        self.should_average = should_average



def _process_results(filename):
    """ Gather the results from the file and return them as a list of rows, each of which is a list of maps. """

    # A set of regexes that we match against stage names.  We only record details for a stage if at least one matches.
    stages_of_interest = ['read', 'write']

    # The fields we are interested in
    field_specs = [
        FieldSpec('Bandwidth', None, _gbits_format, False),
        FieldSpec('95%-ResTime', None, None, True),
        FieldSpec('100%-ResTime', None, None, True),
        FieldSpec('Avg-ResTime', None, None, True),
        FieldSpec('Op-Count', None, None, False),
        FieldSpec('Succ-Ratio', _strip_units, None, True)
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
                            val = row[fs.key]
                            if fs.preprocess_fn is not None:
                                val = fs.preprocess_fn(val)

                            totals[stage][fs.key] += float(val)
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



def _submit(cv):
    """ Submit a test to cosbench.  We return the Cosbench ID for the job so it can be cancelled etc... """

    print("Submitting test file: " + cv.xml_file)

    cmd = ['{}/cli.sh'.format(_cosbench_dir), 'submit', '{}'.format(cv.xml_file)]
    result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=True)
    stdout = result.stdout.decode('utf-8')
    
    id = stdout.split(": ")[1].rstrip("\n")
    print("Job submitted with ID: {}".format(id))  
    return id



def _wait_for_results(cosbench_id):
    """ Wait for the job to complete (or fail) and then return the results as a list of rows, each 
        of which is a map from field name to value. """

    print("Waiting for job to complete\n")

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



def run(spec):
    # Build up all our data.
    cv = CosbenchValues(spec)
    
    # Write out an XML file to submit to cosbench.
    _generate_xml(cv)

    # Submit it and store the ID it hands back.
    id = _submit(cv)

    # Wait for cosbench to complete.  It returns a map of all the interesting cosbench fields.
    vals = _wait_for_results(id)

    # Build a results object.
    result = Result(spec)
    result.id = id
   
    # Fill in the Read stats 
    r_successes = int(float(vals['Read Op-Count']) * float(vals['Read Succ-Ratio']) / 100)
    r_fails = int(float(vals['Read Op-Count'])) - r_successes

    result.read = DirectionResult(
            vals['Read Bandwidth'], 
            '-', 
            vals['Read 100%-ResTime'], 
            vals['Read 95%-ResTime'],
            vals['Read Avg-ResTime'],
            r_successes, 
            r_fails)

    # Fill in the Write stats
    w_successes = int(float(vals['Write Op-Count']) * float(vals['Write Succ-Ratio']) / 100)
    w_fails = int(float(vals['Write Op-Count'])) - w_successes

    result.write = DirectionResult(
            vals['Write Bandwidth'], 
            '-', 
            vals['Write 100%-ResTime'], 
            vals['Write 95%-ResTime'],
            vals['Write Avg-ResTime'],
            r_successes, 
            r_fails)
    
    return result

