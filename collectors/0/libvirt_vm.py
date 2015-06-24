#!/usr/bin/python2
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
import subprocess
import re

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

ERROR_CODE_DONT_RETRY = 13 # tells tcollector not to restart this collector after failure

class LibvirtCollectorError(Exception):
    def __init__(self, value):
        self.value = value
    def __str__(self):
        return repr(self.value)

def main():
    conn = libvirt.openReadOnly(LIBVIRT_URI)
    if conn is None:
        utils.err("Failed to open connection to the hypervisor")
        return ERROR_CODE_DONT_RETRY

    while True:
        domains = conn.listAllDomains()
        batches = chunker(domains, BATCH_SIZE)

        for batch in batches:
            if not process_batch(batch):
                return ERROR_CODE_DONT_RETRY

        sys.stdout.flush()
        time.sleep(INTERVAL)

def process_batch(batch):
    vms = {}
    for domain in batch:
        if domain.isActive() != 1:
            utils.err("Domain %s is inactive. Skipping." % domain.name())
            continue
        try:
            vm = {}
            xml = BeautifulSoup(domain.XMLDesc())
            vm[TAG_DEPLOY_ID] = domain.name()
            vm[FIELDS["memory"]] = get_memory(domain, xml)
            vm.update(get_network_traffic(domain, xml))
            vm[TAG_TYPE] = get_type(domain,xml)
            vms[get_pid(vm[TAG_DEPLOY_ID])] = vm
        except LibvirtCollectorError as err:
            utils.err(err.value)
            continue

    if not vms:
        return True

    try:
        ordered_vms = collections.OrderedDict(sorted(vms.items()))
        pids = ','.join(ordered_vms.keys())
        cpu_loads = get_cpu_loads(pids)
        for vm, cpu_load in zip(ordered_vms, cpu_loads):
            vm[FIELDS["cpu"]] = cpu_load

        print_batch(ordered_vms)
    except LibvirtCollectorError as err:
        utils.err(err.value)
        return False

    return True

def get_pid(vm_name):
    vm_pid_file = "%s/%s.pid" % (PID_DIR, vm_name)

    if not os.path.isfile(vm_pid_file):
        raise LibvirtCollectorError("PID file for virtual machine %s doesn't exist. Skipping." % vm_name)

    pid = ""
    try:
        file = open(vm_pid_file)
        try:
            pid = file.readline()
        except IOError as err:
            raise LibvirtCollectorError("Cannot read PID file for virtual machine %s: %s. Skipping." % (vm_name, err.strerror))
        finally:
            file.close()
    except IOError as err:
        raise LibvirtCollectorError("Cannot open PID file for virtual machine %s: %s. Skipping." % (vm_name, err.strerror))

    return pid.strip()

def get_cpu_loads(pids):
    p1 = subprocess.Popen(["top", "-b", "-d2", "-n2", "-oPID", "-p%s" % pids], stdout=subprocess.PIPE)
    output, err = p1.communicate()
    if err:
        raise LibvirtCollectorError("Cannot read CPU load from top. Stopping.")

    regex = re.compile(r"^\s*PID\s*USER")
    position = None

    lines = output.strip().split("\n")
    lines = lines[len(lines)/2:]
    lines = map(lambda line: line.strip(), lines)
    for i,line in enumerate(lines):
        if regex.match(line):
            position = i
            break

    if not position:
        raise LibvirtCollectorError("Cannot parse CPU load from top. Stopping.")

    data_lines = lines[position+1:]

    return map(lambda row: row.split()[6], data_lines)

def chunker(seq, size):
    return (seq[pos:pos + size] for pos in xrange(0, len(seq), size))

def get_memory(domain, xml):
    mem = domain.memoryStats()
    return max([mem["actual"], mem["rss"]])

def get_network_traffic(domain, xml):
    interfaces = xml.findAll("interface")
    netrx = 0
    nettx = 0
    for interface in interfaces:
        target = interface.target
        if not target or not target.has_attr("dev"):
            raise LibvirtCollectorError("Cannot read interface name for domain %s. Skipping" % domain.name())

        try:
            netrx += domain.interfaceStats(interface.target["dev"])[0] # netrx
            nettx += domain.interfaceStats(interface.target["dev"])[4] # nettx
        except libvirt.libvirtError:
            raise LibvirtCollectorError("Cannot read interface statistics for domain %s. Skipping" % domain.name())


    return {FIELDS["netrx"] : netrx, FIELDS["nettx"] : nettx}

def get_type(domain, xml):
    domain_tag = xml.domain
    if not domain_tag or not domain_tag.has_attr("type"):
        raise LibvirtCollectorError("Cannot read type for domain %s. Skipping" % domain.name())

    return domain_tag["type"]

def print_batch(vms):
    for vm in vms:
        vm_name = vm.pop(TAG_DEPLOY_ID)
        vm_type = vm.pop(TAG_TYPE)

        timestamp = int(time.time())
        for key in vm.keys():
            print("%s %d %s %s=%s %s=%s" % (key, timestamp, vm[key], TAG_DEPLOY_ID, vm_name, TAG_TYPE, vm_type))

if __name__ == '__main__':
    sys.exit(main())
