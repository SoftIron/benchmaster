import json
import spec
import subprocess

from result import Result, DirectionResult


sibench_binary = '/root/sibench/bin/sibench'

def run(spec):
    """ Run the test described by the spec using sibench as the backend.
        We block until we're done.
        This doesn't return all the details that sibench reports, but picks out the 
        data that it has in common with cosbench. """
   
    # Check that this is something we support, and convert cosbench storage type ids into sibench ones. 

    protocol = spec.protocol.name()
    if protocol not in ['s3', 'rados', 'cephfs']:
        print('Bad storage type for sibench: {}'.format(protocol))
        exit(-1) 

    # Sibbench only supports time-based runs, not operation counts.
    if spec.runtype.name() != 'time':
        print('Bad runtype for sibench: {}'.format(spec.runtype.name()))
        exit(-1) 

    # From here on we should be good, so let's build our command line to invoke sibench

    cmd = '{} {} run -s{} -o{} -r{} -u{} -d{} -b{} -jsibench.json --servers {} -p {} {}'.format(
            sibench_binary,
            protocol,
            spec.size,
            spec.object_count,
            spec.runtype.runtime,
            spec.runtype.ramp_up,
            spec.runtype.ramp_down,
            spec.backend.bandwidth,
            ','.join(spec.backend.servers),
            spec.backend.port,
            ' '.join(spec.protocol.targets()))

    if protocol == 's3':
        cmd += ' --s3-port {} --s3-bucket {} --s3-access-key {} --s3-secret-key {}'.format(
            spec.protocol.port,
            spec.protocol.bucket,
            spec.protocol.access_key,
            spec.protocol.secret_key)
    elif protocol == 'rados':
        cmd += ' --ceph-pool {} --ceph-user {} --ceph-key {}'.format(
            spec.protocol.pool,
            spec.protocol.user,
            spec.protocol.key)
    else:
        cmd += ' --ceph-dir {} --ceph-user {} --ceph-key {}'.format(
            spec.protocol.subdir,
            spec.protocol.user,
            spec.protocol.key)

    # And now run it.
    subprocess.check_call(cmd, shell=True)

    result = Result(spec)
    result.id = '-'

    # We asked it to put the results in sibench.json, so let's grab that.
    with open('sibench.json') as json_file:
        data = json.load(json_file)
        for a in data['Analyses']:
            if a['Name'] == 'Total Read':   result.read = _direction_result(a)
            if a['Name'] == 'Total Write':  result.write = _direction_result(a)

    return result



def _direction_result(analysis):
    """ Creates a DirectionResult object from an Analysis json object. """

    # Bandwidth is in b/s, but we want Gb/s
    bandwidth = analysis['Bandwidth'] / (1024 * 1024 * 1024)

    # Response times are in ns, but we want ms
    res_min = analysis['ResTimeMin'] / (1000 * 1000)
    res_max = analysis['ResTimeMax'] / (1000 * 1000)
    res_95 = analysis['ResTime95'] / (1000 * 1000)

    successes = analysis['Successes']
    failures = analysis['Failures']

    return DirectionResult(bandwidth, res_min, res_max, res_95, successes, failures)

