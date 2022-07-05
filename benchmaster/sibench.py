// SPDX-FileCopyrightText: 2022 SoftIron Limited <info@softiron.com>
// SPDX-License-Identifier: GNU General Public License v2.0 only WITH Classpath exception 2.0
import json
import benchmaster.spec as spec
import subprocess

from benchmaster.result import Result, DirectionResult


sibench_binary = 'sibench'

def run(spec):
    """ Run the test described by the spec using sibench as the backend.
        We block until we're done.
        This doesn't return all the details that sibench reports, but picks out the 
        data that it has in common with cosbench. """
   
    # Check that this is something we support, and convert cosbench storage type ids into sibench ones. 

    protocol = spec.protocol.name()
    if protocol not in ['s3', 'rados', 'rbd', 'cephfs', 'block', 'file']:
        print('Bad storage type for sibench: {}'.format(protocol))
        exit(-1) 

    # Sibbench only supports time-based runs, not operation counts.
    if spec.runtype.name() != 'time':
        print('Bad runtype for sibench: {}'.format(spec.runtype.name()))
        exit(-1) 

    # From here on we should be good, so let's build our command line to invoke sibench

    cmd = '{} {} run -s{} -c{} -x{} -r{} -u{} -d{} -w{} -b{} -osibench.json --servers {} -p {}'.format(
            sibench_binary,
            protocol,
            spec.object_size,
            spec.object_count,
            spec.read_write_mix,
            spec.runtype.runtime,
            spec.runtype.ramp_up,
            spec.runtype.ramp_down,
            spec.backend.worker_factor,
            spec.backend.bandwidth,
            ','.join(spec.backend.servers),
            spec.backend.port)

    if protocol == 's3':
        cmd += ' --s3-port {} --s3-bucket {} --s3-access-key {} --s3-secret-key {} {}'.format(
            spec.protocol.port,
            spec.protocol.bucket,
            spec.protocol.access_key,
            spec.protocol.secret_key,
            ' '.join(spec.protocol.targets()))
    
    elif protocol == 'rados':
        cmd += ' --ceph-pool {} --ceph-user {} --ceph-key {} {}'.format(
            spec.protocol.pool,
            spec.protocol.user,
            spec.protocol.key,
            ' '.join(spec.protocol.targets()))
    
    elif protocol == 'rbd':
        cmd += ' --ceph-pool {} --ceph-datapool "{}" --ceph-user {} --ceph-key {} {}'.format(
            spec.protocol.pool,
            spec.protocol.datapool,
            spec.protocol.user,
            spec.protocol.key,
            ' '.join(spec.protocol.targets()))

    elif protocol == 'cephfs':
        cmd += ' --ceph-dir {} --ceph-user {} --ceph-key {} {}'.format(
            spec.protocol.subdir,
            spec.protocol.user,
            spec.protocol.key,
            ' '.join(spec.protocol.targets()))
    
    elif protocol == 'block':
        cmd += ' --block-device {}'.format(spec.protocol.targets()[0])

    elif protocol == 'file':
        cmd += ' --file-dir {}'.format(spec.protocol.targets()[0])

    if spec.backend.skip_read_verification:
        cmd += ' --skip-read-verification'

    cmd += ' --use-bytes'

    print("Running command: {}".format(cmd))

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

    # Bandwidth is in B/s, but we want MB/s
    bandwidth = analysis['BandwidthBytes'] / (1024 * 1024)

    # Response times are in ns, but we want ms
    res_min = analysis['ResTimeMin'] / (1000 * 1000)
    res_max = analysis['ResTimeMax'] / (1000 * 1000)
    res_95 = analysis['ResTime95'] / (1000 * 1000)
    res_avg = analysis['ResTimeAvg'] / (1000 * 1000)

    successes = analysis['Successes']
    failures = analysis['Failures']

    return DirectionResult(bandwidth, res_min, res_max, res_95, res_avg, successes, failures)

