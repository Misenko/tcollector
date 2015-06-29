#!/usr/bin/python2
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

import sys
import time
import os
import collections
import subprocess
import re

from collectors.lib import utils

try:
    import libvirt
except ImportError:
    libvirt = None

try:
    from bs4 import BeautifulSoup
except ImportError:
    BeautifulSoup = None

INTERVAL = 15  # seconds

METRIC_PREFIX = "libvirt.vm."

FIELDS = {"net_rx": "%snetwork.rx" % METRIC_PREFIX,
          "net_tx": "%snetwork.tx" % METRIC_PREFIX,
          "net_current_rx": "%snetwork.current.rx" % METRIC_PREFIX,
          "net_current_tx": "%snetwork.current.tx" % METRIC_PREFIX,
          "cpu_load": "%scpu.load" % METRIC_PREFIX,
          "cpu_time": "%scpu.time" % METRIC_PREFIX,
          "disk_read_reqs": "%sdisk.read.requests" % METRIC_PREFIX,
          "disk_read_bytes": "%sdisk.read.bytes" % METRIC_PREFIX,
          "disk_write_reqs": "%sdisk.write.requests" % METRIC_PREFIX,
          "disk_write_bytes": "%sdisk.write.bytes" % METRIC_PREFIX,
          "disk_total_reqs": "%sdisk.total.req" % METRIC_PREFIX,
          "disk_current_read_reqs": "%sdisk.current.read.requests" % METRIC_PREFIX,
          "disk_current_read_bytes": "%sdisk.current.read.bytes" % METRIC_PREFIX,
          "disk_current_write_reqs": "%sdisk.current.write.requests" % METRIC_PREFIX,
          "disk_current_write_bytes": "%sdisk.current.write.bytes" % METRIC_PREFIX,
          "disk_current_total_reqs": "%sdisk.current.total.reqs" % METRIC_PREFIX,
          "disk_total_bytes": "%sdisk.total.bytes" % METRIC_PREFIX,
          "disk_current_total_bytes": "%sdisk.current.total.bytes" % METRIC_PREFIX,
          "memory": "%smemory" % METRIC_PREFIX}

STATES = {0: "NO_STATE",
          1: "RUNNING",
          2: "BLOCKED",
          3: "PAUSED",
          4: "SHUTDOWN",
          5: "SHUTOFF",
          6: "CRASHED",
          7: "PM_SUSPENDED",
          8: "LAST"}

TAG_DEPLOY_ID = "deploy_id"
TAG_TYPE = "type"

BATCH_SIZE = 20

PID_DIR = "/var/run/libvirt/qemu"

LIBVIRT_URI = "qemu:///system"

ERROR_CODE_DONT_RETRY = 13  # do not to restart this collector after failure

DATA_RETRIEVAL_WAIT = 1.0


class LibvirtCollectorError(Exception):
    def __init__(self, value):
        self.value = value

    def __str__(self):
        return repr(self.value)


def main():
    if libvirt is None:
        utils.err("Python module 'libvirt' is missing")
        return ERROR_CODE_DONT_RETRY
    if BeautifulSoup is None:
        utils.err("Python module 'BeautifulSoup 4' is missing")
        return ERROR_CODE_DONT_RETRY

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
            vm[FIELDS["memory"]] = get_memory(domain)
            vm.update(get_network_traffic(domain, xml))
            vm.update(get_disk_io(domain, xml))
            vm[TAG_TYPE] = get_type(domain, xml)
            vm[FIELDS["cpu_time"]] = get_cpu_time(domain)
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
        for vm, cpu_load in zip(ordered_vms.values(), cpu_loads):
            vm[FIELDS["cpu_load"]] = cpu_load

        print_batch(ordered_vms)
    except LibvirtCollectorError as err:
        utils.err(err.value)
        return False

    return True


def get_cpu_time(domain):
    retval = domain.getCPUStats(-1)
    if not retval:
        raise LibvirtCollectorError("No cpu time data available for domain"
                                    "%s. Skipping." % (domain.name()))

    data = retval[0]
    if "cpu_time" not in data:
        raise LibvirtCollectorError("Mission cpu time for domain"
                                    "%s. Skipping." % (domain.name()))

    return data["cpu_time"]


def get_pid(vm_name):
    vm_pid_file = "%s/%s.pid" % (PID_DIR, vm_name)

    if not os.path.isfile(vm_pid_file):
        raise LibvirtCollectorError("PID file for virtual machine %s"
                                    "doesn't exist. Skipping." % vm_name)

    pid = ""
    try:
        file = open(vm_pid_file)
        try:
            pid = file.readline()
        except IOError as err:
            raise LibvirtCollectorError("Cannot read PID file for virtual"
                                        "machine %s: %s. Skipping." %
                                        (vm_name, err.strerror))
        finally:
            file.close()
    except IOError as err:
        raise LibvirtCollectorError("Cannot open PID file for virtual"
                                    "machine %s: %s. Skipping." %
                                    (vm_name, err.strerror))

    return pid.strip()


def get_cpu_loads(pids):
    p1 = subprocess.Popen(["top", "-b", "-d2", "-n2", "-oPID", "-p%s" % pids],
                          stdout=subprocess.PIPE)
    output, err = p1.communicate()
    if err:
        raise LibvirtCollectorError("Cannot read CPU load from top. Stopping.")

    regex = re.compile(r"^\s*PID\s*USER")
    position = None

    lines = output.strip().split("\n")
    lines = lines[len(lines)/2:]
    lines = map(lambda line: line.strip(), lines)
    for i, line in enumerate(lines):
        if regex.match(line):
            position = i
            break

    if not position:
        raise LibvirtCollectorError("Cannot parse CPU load from top. "
                                    "Stopping.")

    data_lines = lines[position+1:]

    return map(lambda row: row.split()[6], data_lines)


def chunker(seq, size):
    return (seq[pos:pos + size] for pos in xrange(0, len(seq), size))


def get_memory(domain):
    mem = domain.memoryStats()
    return max([mem["actual"], mem["rss"]])


def get_per_sec_data(first_data, second_data):
    return map(lambda data: (data[1] - data[0])/DATA_RETRIEVAL_WAIT,
               zip(first_data, second_data))


def get_network_traffic(domain, xml):
    interfaces = xml.findAll("interface")
    first_data = get_network_data(interfaces, domain)
    time.sleep(DATA_RETRIEVAL_WAIT)
    second_data = get_network_data(interfaces, domain)

    data_per_sec = get_per_sec_data(first_data, second_data)

    return {FIELDS["net_rx"]: second_data[0],
            FIELDS["net_tx"]: second_data[1],
            FIELDS["net_current_rx"]: data_per_sec[0],
            FIELDS["net_current_tx"]: data_per_sec[1]}


def get_network_data(interfaces, domain):
    netrx = 0
    nettx = 0
    for interface in interfaces:
        target = interface.target
        if not target or not target.has_attr("dev"):
            raise LibvirtCollectorError("Cannot read interface name "
                                        "for domain %s. Skipping" %
                                        domain.name())

        try:
            netrx += domain.interfaceStats(interface.target["dev"])[0]  # netrx
            nettx += domain.interfaceStats(interface.target["dev"])[4]  # nettx
        except libvirt.libvirtError:
            raise LibvirtCollectorError("Cannot read interface statistics"
                                        "for domain %s. Skipping" %
                                        domain.name())

    return (netrx, nettx)


def get_disk_io(domain, xml):
    disks = xml.findAll("disk")
    first_data = get_disk_data(disks, domain)
    time.sleep(DATA_RETRIEVAL_WAIT)
    second_data = get_disk_data(disks, domain)

    data_per_sec = get_per_sec_data(first_data, second_data)

    disk_data = {FIELDS["disk_read_reqs"]: second_data[0],
                 FIELDS["disk_write_reqs"]: second_data[1],
                 FIELDS["disk_total_reqs"]: second_data[0] + second_data[1],
                 FIELDS["disk_read_bytes"]: second_data[2],
                 FIELDS["disk_write_bytes"]: second_data[3],
                 FIELDS["disk_total_bytes"]: second_data[2] + second_data[3],
                 FIELDS["disk_current_read_reqs"]: data_per_sec[0],
                 FIELDS["disk_current_write_reqs"]: data_per_sec[1],
                 FIELDS["disk_current_total_reqs"]: data_per_sec[0] + data_per_sec[1],
                 FIELDS["disk_current_read_bytes"]: data_per_sec[2],
                 FIELDS["disk_current_write_bytes"]: data_per_sec[3],
                 FIELDS["disk_current_total_bytes"]: data_per_sec[2] + data_per_sec[3]}

    return disk_data


def get_disk_data(disks, domain):
    read_reqs = 0
    write_reqs = 0
    read_bytes = 0
    write_bytes = 0

    for disk in disks:
        target = disk.target
        if not target or not target.has_attr("dev"):
            raise LibvirtCollectorError("Cannot read disk name "
                                        "for domain %s. Skipping" %
                                        domain.name())
        try:
            read_reqs += domain.blockStats(disk.target["dev"])[0]  # rd_req
            write_reqs += domain.blockStats(disk.target["dev"])[1]  # rd_bytes
            read_bytes += domain.blockStats(disk.target["dev"])[2]  # wr_req
            write_bytes += domain.blockStats(disk.target["dev"])[3]  # wr_bytes
        except libvirt.libvirtError:
            raise LibvirtCollectorError("Cannot read interface statistics"
                                        "for domain %s. Skipping" %
                                        domain.name())

    return(read_reqs, write_reqs, read_bytes, write_bytes)


def get_type(domain, xml):
    domain_tag = xml.domain
    if not domain_tag or not domain_tag.has_attr("type"):
        raise LibvirtCollectorError("Cannot read type for domain %s. "
                                    "Skipping" % domain.name())

    return domain_tag["type"]


def print_batch(vms):
    for vm in vms.values():
        vm_name = vm.pop(TAG_DEPLOY_ID)
        vm_type = vm.pop(TAG_TYPE)

        timestamp = int(time.time())
        for key in vm.keys():
            print("%s %d %s %s=%s %s=%s" % (key, timestamp, vm[key],
                                            TAG_DEPLOY_ID, vm_name, TAG_TYPE,
                                            vm_type))

if __name__ == '__main__':
    sys.exit(main())
