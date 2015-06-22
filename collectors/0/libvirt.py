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
import os
import collections

from collectors.lib import utils
from bs4 import BeautifulSoup

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

class LibvirtCollectorError(Exception):
    pass

def main():
    conn = libvirt.openReadOnly(LIBVIRT_URI)
    if conn is None:
        utils.err("Failed to open connection to the hypervisor")
        return 13 # tells tcollector not to restart this collector after failure

    domains = conn.listAllDomains()
    batches = chunker(domains, BATCH_SIZE)

    for batch in batches:
        vms = {}
        for domain in batch:
            try:
                vm = {}
                vm["name"] = domain.name()
                vm[FIELD["memory"]] = get_memory(domain)
                vm.update(get_network_traffic(domain))
            except LibvirtCollectorError, err:
                utils.err(err.message)
                continue

            vm_pid_file = "%s/%s.pid" % PID_DIR, vm["name"]

            if !os.path.isfile(vm_pid_file):
                utils.err("PID file for virtual machine %s doesn't exist. Skipping." % vm["name"])
                continue

            pid = ""
            try:
                file = open(vm_pid_file)
                pid = file.readline()
            except IOError, err:
                utils.err("Cannot open PID file for virtual machine %s: %sSkipping." % vm["name"], err.message)
                continue
            finally:
                file.close()

            vms[pid.strip()] = vm

        if !vms:
            continue # no vms in this batch
            
        ordered_vms = collections.OrderedDict(sorted(vms.items)))
        pids = ','.join(ordered_vms.keys())

def chunker(seq, size):
    return (seq[pos:pos + size] for pos in xrange(0, len(seq), size))

def get_memory(domain):
    if domain.isActive() == 1:
        mem = domain.memoryStats()
        return max([mem["actual"], mem["rss"]])
    else:
        xml = BeautifulSoup(domain.XMLDesc())
        try:
            mem = int(xml.memory.getText())
        except ValueError:
            raise LibvirtCollectorError("Cannot read memory for domain %s. Skipping" % domain.name())

    return mem

def get_network_traffic(domain):
    xml = BeautifulSoup(domain.XMLDesc())
    interfaces = xml.findAll("interface")
    netrx = 0
    nettx = 0
    for interface in interfaces:
        target = interface.target
        if target != None && target.has_attr("dev"):
            try:
                netrx += domain.interfaceStats(interface.target["dev"])[0] # netrx
                nettx += domain.interfaceStats(interface.target["dev"])[4] # nettx
            except libvirt.libvirtError:
                raise LibvirtCollectorError("Cannot read interface statistics for domain %s. Skipping" % domain.name())
        else:
            raise LibvirtCollectorError("Cannot read interface name for domain %s. Skipping" % domain.name())


    return {FIELDS["netrx"] : netrx, FIELDS["nettx"] : nettx}

if __name__ == '__main__':
    sys.exit(main())
