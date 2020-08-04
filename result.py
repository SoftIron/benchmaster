class Result:
    """ Simple data class to hold the results of a single run. 
        This is everything we need to write to a spreadsheet. """

    id = None
    start_time = None
    end_time = None

    # These two should be set to contain DirectionResult objects
    write = None
    read = None

    def __init__(self, spec):
        self.protocol = spec.protocol.name()
        self.backend = spec.backend.name()
        self.size = spec.size
        self.object_count = spec.object_count
        self.workers = spec.backend.workers()
        self.schedule = spec.runtype.schedule()
        self.targets = len(spec.protocol.targets())
        self.description = spec.description

    def __repr__(self): return str(vars(self))


    def columns():
        """ Returns an array of the column names we want for google sheets. """
        return ['ID', 'Protocol', 'Backend', 'Size', 'Ojbect Pool', 'Workers', 'Schedule', 'Targets', 
                'Wr Bandwidth Gb/s', 'Wr ResTime Min ms', 'Wr ResTime Max ms', 'Wr ResTime95 ms', 'Wr ResTimeAvg ms', 'Wr Successes', 'Wr Failures',
                'Rd Bandwidth Gb/s', 'Rd ResTime Min ms', 'Rd ResTime Max ms', 'Rd ResTime95 ms', 'Rd ResTimeAvg ms', 'Rd Successes', 'Rd Failures',
                'Description', 'Start', 'End']


    def backgrounds():
        """ Returns an array of RGB tuples (or None) that we wish to apply as background for the columns, """
        write_dark  = (0.9, 0.75, 0.75)
        write_light = (0.95, 0.85, 0.85)
        read_dark   = (0.75, 0.9, 0.75)
        read_light  = (0.85, 0.95, 0.85)

        return [None, None, None, None, None, None, None, None,
                write_dark, write_light, write_light, write_light, write_light, write_light, write_light,
                read_dark, read_light, read_light, read_light, read_light, read_light, read_light,
                None, None, None]


    def values(self):
        """ Return the values for a row, formatted as we want them. """
        return [self.id, self.protocol, self.backend, self.size, self.object_count, self.workers, self.schedule, self.targets,
                self.write.bandwidth, self.write.res_min, self.write.res_max, self.write.res_95, self.write.res_avg, self.write.successes, self.write.failures,
                self.read.bandwidth, self.read.res_min, self.read.res_max, self.read.res_95, self.read.res_avg, self.read.successes, self.read.failures,
                self.description, str(self.start_time), str(self.end_time)]



class DirectionResult:
    """ All the stats relating to a direction (read or write). """

    def __init__(self, bandwidth, res_min, res_max, res_95, res_avg, successes, failures):
        self.bandwidth = bandwidth
        self.res_min = res_min
        self.res_max = res_max
        self.res_95 = res_95
        self.res_avg = res_avg
        self.successes = successes
        self.failures = failures

    def __repr__(self): return str(vars(self))

