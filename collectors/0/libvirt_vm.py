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
# libvirt.vm.count - number of running VMs
# libvirt.vm.cpu.load - VM's current CPU load (can be higher than 100%)
# libvirt.vm.cpu.time - CPU time spent by VM
# libvirt.vm.disk.read.requests - number of total VM's read requests
# libvirt.vm.disk.read.bytes - number of total VM's read bytes
# libvirt.vm.disk.write.requests - number of total VM's write requests
# libvirt.vm.disk.write.bytes - number of total VM's write bytes
# libvirt.vm.disk.total.requests - number of total VM's read + write requests
# libvirt.vm.disk.total.bytes - number of total VM's read + write bytes
# libvirt.vm.disk.current.read.requests - number of VM's current read requests
# libvirt.vm.disk.current.read.bytes - number of VM's current read bytes
# libvirt.vm.disk.current.write.requests - number of VM's current write requests
# libvirt.vm.disk.current.write.bytes - number of VM's current write bytes
# libvirt.vm.disk.current.total.requests - number of VM's current read + write requests
# libvirt.vm.disk.current.total.bytes - number of VM's current read + write bytes
# libvirt.vm.memory - memory used by VM in kB
# libvirt.vm.max.memory - memory requested in VM's template in kB
# libvirt.vm.max.vcpus - number of CPU requested in VM's template
# libvirt.vm.network.rx - number of VM's received bytes via network
# libvirt.vm.network.tx - number of VM's transmitted bytes via network
# libvirt.vm.network.current.rx - VM's current network incoming bandwidth
# libvirt.vm.network.current.tx - VM's current network outcoming bandwidth

import sys
import time
import random
import subprocess
import re

from collectors.lib import utils
from collectors.lib.libvirt_vm_errors import LibvirtVmDataError
from collectors.lib.libvirt_vm_errors import LibvirtVmProcessingError

# gracefully deals with import errors
try:
    import libvirt
except ImportError:
    libvirt = None

try:
    from bs4 import BeautifulSoup
except ImportError:
    BeautifulSoup = None

try:
    import psutil
except ImportError:
    psutil = None

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
          "disk_total_reqs": "%sdisk.total.requests" % METRIC_PREFIX,
          "disk_current_read_reqs": "%sdisk.current.read.requests" %
                                    METRIC_PREFIX,
          "disk_current_read_bytes": "%sdisk.current.read.bytes" %
                                     METRIC_PREFIX,
          "disk_current_write_reqs": "%sdisk.current.write.requests" %
                                     METRIC_PREFIX,
          "disk_current_write_bytes": "%sdisk.current.write.bytes" %
                                      METRIC_PREFIX,
          "disk_current_total_reqs": "%sdisk.current.total.requests" %
                                     METRIC_PREFIX,
          "disk_total_bytes": "%sdisk.total.bytes" % METRIC_PREFIX,
          "disk_current_total_bytes": "%sdisk.current.total.bytes" %
                                      METRIC_PREFIX,
          "memory": "%smemory" % METRIC_PREFIX,
          "count": "%scount" % METRIC_PREFIX,
          "max_memory": "%smax.memory" % METRIC_PREFIX,
          "max_vcpus": "%smax.vcpus" % METRIC_PREFIX}

# VMs' states
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
PID = "pid"

LIBVIRT_URI = "qemu:///system"

ERROR_CODE_DONT_RETRY = 13  # do not restart this collector after failure

DATA_RETRIEVAL_WAIT = 0.3  # number of seconds between two data requests

PSUTIL_OLD_VERSION = '1.2.1'


def main():
    try:
        check_imports()

        conn = libvirt.openReadOnly(LIBVIRT_URI)
        if conn is None:
            utils.err("Failed to open connection to the hypervisor")
            return ERROR_CODE_DONT_RETRY

        while True:
            domains = conn.listAllDomains()
            random.shuffle(domains)
            pids = get_pids()

            count = 0
            for domain in domains:
                if process_domain(domain, pids.get(domain.UUIDString())):
                    count += 1  # count only successfully processed VMs

            # write libvirt.vm.count metric
            print("%s %d %s" % (FIELDS["count"], int(time.time()), count))

            sys.stdout.flush()
            time.sleep(INTERVAL)

    except LibvirtVmProcessingError as err:
        utils.err(err.value)
        return ERROR_CODE_DONT_RETRY


def check_imports():
    """Checks whether all needed modules are imported"""
    if libvirt is None:
        raise LibvirtVmProcessingError("Python module 'libvirt' is missing")
    if BeautifulSoup is None:
        raise LibvirtVmProcessingError("Python module 'BeautifulSoup 4'"
                                       "is missing")
    if psutil is None:
        raise LibvirtVmProcessingError("Python module 'psutil' is missing")


def process_domain(domain, pid):
    """Process one domain (vm)"""
    # skip vms that are not running
    if domain.isActive() != 1:
        utils.err("Domain %s is inactive. Skipping." % domain.name())
        return False
    if not pid:
        utils.err("Cannot find PID for domain %s. Skipping." % domain.name())
        return False
    if not psutil.pid_exists(pid):
        utils.err("PID %d no longer exists for domain %s. Skipping." %
                  (pid, domain.name()))
        return False

    # populate vm structure with metrics
    try:
        vm = {}
        vm[FIELDS["cpu_time"]] = get_cpu_time(pid)
        vm[FIELDS["cpu_load"]] = get_cpu_load(pid)
        vm[FIELDS["memory"]] = get_memory(domain)
        vm[FIELDS["max_memory"]] = domain.maxMemory()
        vm[FIELDS["max_vcpus"]] = domain.maxVcpus()

        xml = BeautifulSoup(domain.XMLDesc())
        vm[TAG_DEPLOY_ID] = domain.name()
        vm[TAG_TYPE] = get_type(domain, xml)

        vm.update(get_network_traffic(domain, xml))
        vm.update(get_disk_io(domain, xml))
    except LibvirtVmDataError as err:
        utils.err(err.value)
        return False

    print_vm(vm)
    return True


def get_pids():
    """Retrieves all vms' PIDs based on their UUID"""
    p1 = subprocess.Popen(["ps", "-ewwo", "pid,command"],
                          stdout=subprocess.PIPE)
    output, err = p1.communicate()
    if err:
        raise LibvirtVmProcessingError("Cannot read PIDs from ps. Stopping.")

    regex = re.compile(r"-uuid ([a-z0-9]{8}-[a-z0-9]{4}-[a-z0-9]{4}-"
                       "[a-z0-9]{4}-[a-z0-9]{12})")
    pids = {}

    lines = output.strip().split("\n")
    for line in lines:
        line = line.strip()
        match = regex.search(line)
        if match:
            uuid = match.group(1).strip()
            pid = line.split(' ', 1)[0]  # 1. column is process PID
            try:
                pids[uuid] = int(pid)
            except ValueError:
                raise LibvirtVmProcessingError("'%s' is not a valid PID"
                                               "number" % pid)

    return pids


def get_cpu_time(pid):
    p = psutil.Process(pid)
    if psutil.__version__ <= PSUTIL_OLD_VERSION:
        cpu_time = p.get_cpu_times()
    else:
        cpu_time = p.cpu_times()

    return cpu_time[0] + cpu_time[1]


def get_cpu_load(pid):
    p = psutil.Process(pid)
    if psutil.__version__ <= PSUTIL_OLD_VERSION:
        cpu_load = p.get_cpu_percent(DATA_RETRIEVAL_WAIT)
    else:
        cpu_load = p.cpu_percent(DATA_RETRIEVAL_WAIT)

    return cpu_load


def get_memory(domain):
    mem = domain.memoryStats()
    return max([mem["actual"], mem["rss"]])


def get_per_sec_data(first_data, second_data):
    """
    Takes two set of data and returns per second result based
    on interval between them
    """
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
            raise LibvirtVmDataError("Cannot read interface name "
                                     "for domain %s. Skipping" %
                                     domain.name())

        try:
            netrx += domain.interfaceStats(interface.target["dev"])[0]  # netrx
            nettx += domain.interfaceStats(interface.target["dev"])[4]  # nettx
        except libvirt.libvirtError:
            raise LibvirtVmDataError("Cannot read interface statistics"
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
                 FIELDS["disk_current_total_reqs"]: data_per_sec[0] +
                 data_per_sec[1],
                 FIELDS["disk_current_read_bytes"]: data_per_sec[2],
                 FIELDS["disk_current_write_bytes"]: data_per_sec[3],
                 FIELDS["disk_current_total_bytes"]: data_per_sec[2] +
                 data_per_sec[3]}

    return disk_data


def get_disk_data(disks, domain):
    read_reqs = 0
    write_reqs = 0
    read_bytes = 0
    write_bytes = 0

    for disk in disks:
        target = disk.target
        if not target or not target.has_attr("dev"):
            raise LibvirtVmDataError("Cannot read disk name "
                                     "for domain %s. Skipping" %
                                     domain.name())
        try:
            read_reqs += domain.blockStats(disk.target["dev"])[0]  # rd_req
            write_reqs += domain.blockStats(disk.target["dev"])[1]  # rd_bytes
            read_bytes += domain.blockStats(disk.target["dev"])[2]  # wr_req
            write_bytes += domain.blockStats(disk.target["dev"])[3]  # wr_bytes
        except libvirt.libvirtError:
            raise LibvirtVmDataError("Cannot read interface statistics"
                                     "for domain %s. Skipping" %
                                     domain.name())

    return(read_reqs, write_reqs, read_bytes, write_bytes)


def get_type(domain, xml):
    domain_tag = xml.domain
    if not domain_tag or not domain_tag.has_attr("type"):
        raise LibvirtVmDataError("Cannot read type for domain %s. "
                                 "Skipping" % domain.name())

    return domain_tag["type"]


def print_vm(vm):
    """Prints vm's metrics"""
    vm_name = vm.pop(TAG_DEPLOY_ID)
    vm_type = vm.pop(TAG_TYPE)

    timestamp = int(time.time())
    for key in vm.keys():
        print("%s %d %s %s=%s %s=%s" % (key, timestamp, vm[key],
                                        TAG_DEPLOY_ID, vm_name, TAG_TYPE,
                                        vm_type))

if __name__ == '__main__':
    sys.exit(main())
