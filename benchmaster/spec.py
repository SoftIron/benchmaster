# SPDX-FileCopyrightText: 2022 SoftIron Limited <info@softiron.com>
# SPDX-License-Identifier: GNU General Public License v2.0 only WITH Classpath exception 2.0

""" 
The Spec class contains all the parameters we need to run a benchmark.
 
In particular, it contains three sub-specs:
  1. Runtype: whether we are using Time or Ops to determine how long we run
  2. Backend: whether we are using Cosbench or Sibench
  3. Protocol: whether we are using S3 or Rados (and soon, File etc..)

A Spec can define a sweep of benchmarks (for instance, it might have a range of 
object sizes and a range of runtimes).  These are specified by passing in
comma-separated values on the command line.  For instance: -s 1M,4M,8M.

A Spec that contains such ranges can be flattened, which produces an array
of specs, each of which has only single values for all of its fields. 

The Protocol specs also abstract out some of the fields so that cosbench
can treat everything as if it was S3.  (Cosbench is very heavily skewed
towards S3, and all other protocols must map to its abstractions).
"""

import benchmaster.cosbench as cosbench
import benchmaster.sibench as sibench


class Spec:
    """ Master spec object. """
    def __init__(self, runtype, backend, protocol, object_size, object_count, read_write_mix, clean_up, description):
        self.runtype = runtype
        self.backend = backend
        self.protocol = protocol
        self.object_size = object_size
        self.object_count = object_count
        self.read_write_mix = read_write_mix
        self.clean_up = clean_up
        self.description = description

    def __repr__(self): return str(vars(self))
    def run(self):      return self.backend.run(self)

    def flatten(self):
        results = []
        for r in self.runtype.flatten():
            for b in self.backend.flatten():
                for p in self.protocol.flatten():
                    for s in self.object_size.split(','):
                        for c in self.object_count.split(','):
                            for x in self.read_write_mix.split(','):
                                results.append(Spec(r, b, p, s, c, x, self.clean_up, self.description))
        return results



class TimeSpec:
    """ Runtype spec implementation for Time based benchmarks """
    def __init__(self, runtime, ramp_up, ramp_down):
        self.runtime = runtime
        self.ramp_up = ramp_up
        self.ramp_down = ramp_down

    def __repr__(self):     return str(vars(self))
    def name(self):         return "time"

    def schedule(self):     
        return "Time: {}, Up: {}, Down: {}".format(self.runtime, self.ramp_up, self.ramp_down)

    def flatten(self):
        results = []
        for r in self.runtime.split(','):
            for u in self.ramp_up.split(','):
                for d in self.ramp_down.split(','):
                    results.append(TimeSpec(r, u, d))
        return results



class OpsSpec:
    """ Runtype spec implementation for Operation Count based benchmarks """
    def __init__(self, ops):
        self.ops = ops
    
    def __repr__(self):     return str(vars(self))
    def name(self):         return "ops"
    def schedule(self):     return "Count " + str(self.ops)
    def flatten(self):      return [OpsSpec(o) for o in self.ops.split(',')]



class S3Spec:
    """ Protocol spec implementation for S3 """
    def __init__(self, access_key, secret_key, port, bucket, gateways):
        self.secret_key = secret_key
        self.access_key = access_key
        self.port = port
        self.bucket = bucket
        self.gateways = gateways

    def __repr__(self):      return str(vars(self))
    def name(self):          return "s3"
    def flatten(self):       return [self]

    # Methods that abstract information across protocols.
    def targets(self):       return self.gateways


    
class RadosSpec:
    """ Protocol spec implementation for Rados """
    def __init__(self, user, key, pool, monitors):
        self.user = user
        self.key = key
        self.pool = pool
        self.monitors = monitors

    def __repr__(self):      return str(vars(self))
    def name(self):          return "rados"
    def flatten(self):       return [self]

    # Methods that abstract information across protocols.
    def targets(self):       return self.monitors
        


class RbdSpec:
    """ Protocol spec implementation for Rbd """
    def __init__(self, user, key, pool, datapool, monitors):
        self.user = user
        self.key = key
        self.pool = pool
        self.datapool = datapool
        self.monitors = monitors

    def __repr__(self):      return str(vars(self))
    def name(self):          return "rbd"
    def flatten(self):       return [self]

    # Methods that abstract information across protocols.
    def targets(self):       return self.monitors



class CephFSSpec:
    """ Protocol spec implementation for CephFS """
    def __init__(self, user, key, subdir, monitors):
        self.user = user
        self.key = key
        self.subdir = subdir
        self.monitors = monitors

    def __repr__(self):      return str(vars(self))
    def name(self):          return "cephfs"
    def flatten(self):       return [self]

    # Methods that abstract information across protocols.
    def targets(self):       return self.monitors



class BlockSpec:
    """ Protocol spec implementation for raw block devices """
    def __init__(self, device):
        self.device = device

    def __repr__(self):      return str(vars(self))
    def name(self):          return "block"
    def flatten(self):       return [self]

    # Methods that abstract information across protocols.
    def targets(self):       return [self.device]



class FileSpec:
    """ Protocol spec implementation for locally mounted file systems """
    def __init__(self, directory):
        self.directory = directory

    def __repr__(self):      return str(vars(self))
    def name(self):          return "file"
    def flatten(self):       return [self]

    # Methods that abstract information across protocols.
    def targets(self):       return [self.directory]



class SibenchSpec:
    """ Backend spec implementation for Sibench """
    def __init__(self, port, servers, bandwidth, worker_factor, skip_read_verification, generator, slice_dir, slice_count, slice_size):
        self.port = port
        self.servers = servers
        self.bandwidth = bandwidth
        self.worker_factor = worker_factor
        self.skip_read_verification = skip_read_verification
        self.generator = generator
        self.slice_dir = slice_dir
        self.slice_count = slice_count
        self.slice_size = slice_size

    def __repr__(self):     return str(vars(self))
    def name(self):         return "sibench"
    def flatten(self):
        results = []
        for b in self.bandwidth.split(','):
            for w in self.worker_factor.split(','):
                results.append(SibenchSpec(self.port, self.servers, b, w, self.skip_read_verification, self.generator, self.slice_dir, self.slice_count, self.slice_size))
        return results


    # Methods that abstract information across backends.
    def workers(self):      return len(self.servers)
    def run(self, spec):    return sibench.run(spec)



class CosbenchSpec:
    """ Backend spec implementation for Cosbench """
    worker_threads = None
    xmlfile = None

    def __init__(self, workers, xml_file):
        self.worker_threads = workers
        self.xml_file = xml_file

    def __repr__(self):     return str(vars(self))
    def name(self):         return "cosbench"
    def flatten(self):      return [CosbenchSpec(w, self.xml_file) for w in self.worker_threads.split(',')]

    # Methods that abstract information across backends.
    def workers(self):      return self.worker_threads
    def run(self, spec):    return cosbench.run(spec)
