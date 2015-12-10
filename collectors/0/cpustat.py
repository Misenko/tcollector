#!/usr/bin/python
#
# cpustat.py -- cpu data collector for tcollector/OpenTSDB
# Copyright (C) 2015  Michal Kimle
#
# This program is free software: you can redistribute it and/or modify it
# under the terms of the GNU Lesser General Public License as published by
# the Free Software Foundation, either version 3 of the License, or (at your
# option) any later version.  This program is distributed in the hope that it
# will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
# of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser
# General Public License for more details.  You should have received a copy
# of the GNU Lesser General Public License along with this program.  If not,
# see <http://www.gnu.org/licenses/>.


# Metrics this collector generates:
# cpustat.count - number of CPUs
# cpustat.cpu.mhz - CPU's MHz
# cpustat.cpu.cores - CPU's number of cores

import sys
import time
import re
import multiprocessing

from collectors.lib import utils
from collectors.lib.cpustat_errors import CpustatProcessingError

# gracefully deals with import errors
try:
    import numpy.distutils.cpuinfo
except ImportError:
    numpy = None

INTERVAL = 15  # seconds

METRIC_PREFIX = "cpustat."

FIELDS = {"cpu_count": "%scount" % METRIC_PREFIX,
          "cpu_mhz": "%scpu.mhz" % METRIC_PREFIX,
          "cpu_cores": "%scpu.cores" % METRIC_PREFIX}

TAG_PROCESSOR = "processor"
TAG_MODEL = "model"

ERROR_CODE_DONT_RETRY = 13  # do not restart this collector after failure

def main():
    try:
        check_imports()

        while True:
            # write cpustat.cpu.count metric
            write_cpu_info()
            write_cpu_count()

            sys.stdout.flush()
            time.sleep(INTERVAL)
    except CpustatProcessingError as err:
        utils.err(err.value)
        return ERROR_CODE_DONT_RETRY

def check_imports():
    """Checks whether all needed modules are imported"""
    if numpy is None:
        raise CpustatProcessingError("Python module 'numpy' is missing")

def write_cpu_count():
    print("%s %d %s" % (FIELDS["cpu_count"], int(time.time()), multiprocessing.cpu_count()))

def write_cpu_info():
    cpuinfos = numpy.distutils.cpuinfo.cpuinfo.info
    regex = re.compile(r'[\s@\(\)]+')
    for cpuinfo in cpuinfos:
        cpu = {}
        if "cpu MHz" in cpuinfo:
            cpu[FIELDS["cpu_mhz"]] = cpuinfo["cpu MHz"]
        if "cpu cores" in cpuinfo:
            cpu[FIELDS["cpu_cores"]] = cpuinfo["cpu cores"]
        cpu[TAG_MODEL] = re.sub(regex, '-', cpuinfo["model name"])
        cpu[TAG_PROCESSOR] = cpuinfo["processor"]

        print_cpu(cpu)

def print_cpu(cpu):
    processor = cpu.pop(TAG_PROCESSOR)
    model = cpu.pop(TAG_MODEL)

    timestamp = int(time.time())
    for key in cpu.keys():
        print("%s %d %s %s=%s %s=%s" % (key, timestamp, cpu[key],
                                        TAG_PROCESSOR, processor,
                                        TAG_MODEL, model))

if __name__ == '__main__':
    sys.exit(main())
