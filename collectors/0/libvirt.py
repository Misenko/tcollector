#!/usr/bin/python
#
# vm.py -- a virtual machine data collector for tcollector/OpenTSDB
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

import sys
import time
import libvirt

from collectors.lib import utils

INTERVAL = 15  # seconds

FIELDS = {"netrx" : "libvirt.vm.network.rx",
          "nettx" : "libvirt.vm.network.tx",
          "cpu" : "libvirt.vm.cpu",
          "memory" : "libvirt.vm.memory"}

STATES = {0 : "NO_STATE",
          1 : "RUNNING",
          2 : "BLOCKED",
          3 : "PAUSED",
          4 : "SHUTDOWN",
          5 : "SHUTOFF",
          6 : "CRASHED",
          7 : "PM_SUSPENDED",
          8 : "LAST"}

TAG_DEPLOY_ID = "deploy_id"
TAG_TYPE = "type"

BATCH_SIZE = 20

PID_DIR = "/var/run/libvirt/qemu"

LIBVIRT_URI = "qemu:///system"

def main():
    conn = libvirt.openReadOnly(LIBVIRT_URI)
    if conn is None:
        utils.err("Failed to open connection to the hypervisor")
        return

    domains = conn.listAllDomains()
    vms = []


if __name__ == '__main__':
    sys.exit(main())
