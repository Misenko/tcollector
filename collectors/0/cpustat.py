#!/usr/bin/python
#
# libvirt_vm.py -- a virtual machine data collector for tcollector/OpenTSDB
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
# cpustat.cpu.count - number of CPU

import sys
import time
import multiprocessing

from collectors.lib import utils

INTERVAL = 15  # seconds

METRIC_PREFIX = "cpustat."

FIELDS = {"cpu_count": "%scpu.count" % METRIC_PREFIX}

ERROR_CODE_DONT_RETRY = 13  # do not restart this collector after failure

def main():
    while True:
        # write cpustat.cpu.count metric
        print("%s %d %s" % (FIELDS["cpu_count"], int(time.time()), multiprocessing.cpu_count()))

        sys.stdout.flush()
        time.sleep(INTERVAL)

if __name__ == '__main__':
    sys.exit(main())
