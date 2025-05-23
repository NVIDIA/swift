#!/usr/bin/env python
# Copyright (c) 2010-2012 OpenStack Foundation
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import datetime
import glob
import locale
import os
import os.path
import re
import subprocess
import sys


from configparser import ConfigParser

from swift.common.utils import backward, get_logger, dump_recon_cache, \
    config_true_value


def get_devices(device_dir, logger):
    devices = []
    majmin_devices = {}

    # List /dev/block
    # Using os.scandir on recent versions of python, else os.listdir
    if 'scandir' in dir(os):
        with os.scandir("/dev/block") as it:
            for ent in it:
                if ent.is_symlink():
                    dev_name = os.path.basename(os.readlink(ent.path))
                    majmin = os.path.basename(ent.path).split(':')
                    majmin_devices[dev_name] = {'major': majmin[0],
                                                'minor': majmin[1]}
    else:
        for ent in os.listdir("/dev/block"):
            ent_path = os.path.join("/dev/block", ent)
            if os.path.is_symlink(ent_path):
                dev_name = os.path.basename(os.readlink(ent_path))
                majmin = os.path.basename(ent_path).split(':')
                majmin_devices[dev_name] = {'major': majmin[0],
                                            'minor': majmin[1]}

    for line in open('/proc/mounts').readlines():
        data = line.strip().split()
        block_device = data[0]
        mount_point = data[1]
        if mount_point.startswith(device_dir):
            device = {}
            device['mount_point'] = mount_point
            device['block_device'] = block_device
            dev_name = os.path.basename(block_device)
            if dev_name in majmin_devices:
                # If symlink is in /dev/block
                device['major'] = majmin_devices[dev_name]['major']
                device['minor'] = majmin_devices[dev_name]['minor']
            else:
                # Else we try to stat block_device
                try:
                    device_num = os.stat(block_device).st_rdev
                except OSError:
                    # If we can't stat the device,
                    # then something weird is going on
                    logger.error(
                        'Could not determine major:minor numbers for %s '
                        '(mounted at %s)! Skipping...',
                        block_device, mount_point)
                    continue
                device['major'] = str(os.major(device_num))
                device['minor'] = str(os.minor(device_num))
            devices.append(device)
    for line in open('/proc/partitions').readlines()[2:]:
        major, minor, blocks, kernel_device = line.strip().split()
        device = [d for d in devices
                  if d['major'] == major and d['minor'] == minor]
        if device:
            device[0]['kernel_device'] = kernel_device
    return devices


def get_errors(error_re, log_file_pattern, minutes, logger,
               log_file_encoding):
    # Assuming log rotation is being used, we need to examine
    # recently rotated files in case the rotation occurred
    # just before the script is being run - the data we are
    # looking for may have rotated.
    #
    # The globbing used before would not work with all out-of-box
    # distro setup for logrotate and syslog therefore moving this
    # to the config where one can set it with the desired
    # globbing pattern.
    log_files = [f for f in glob.glob(log_file_pattern)]
    try:
        log_files.sort(key=lambda f: os.stat(f).st_mtime, reverse=True)
    except (IOError, OSError) as exc:
        logger.error(exc)
        print(exc)
        sys.exit(1)

    now_time = datetime.datetime.now()
    end_time = now_time - datetime.timedelta(minutes=minutes)
    # kern.log does not contain the year so we need to keep
    # track of the year and month in case the year recently
    # ticked over
    year = now_time.year
    prev_ent_month = now_time.strftime('%b')
    errors = {}

    reached_old_logs = False
    for path in log_files:
        try:
            f = open(path, 'rb')
        except IOError:
            logger.error("Error: Unable to open " + path)
            print("Unable to open " + path)
            sys.exit(1)
        for line in backward(f):
            line = line.decode(log_file_encoding, 'surrogateescape')
            if '[    0.000000]' in line \
                or 'KERNEL supported cpus:' in line \
                    or 'BIOS-provided physical RAM map:' in line:
                # Ignore anything before the last boot
                reached_old_logs = True
                break
            # Solves the problem with year change - kern.log does not
            # keep track of the year.
            log_time_ent = line.split()[:3]
            if log_time_ent[0] == 'Dec' and prev_ent_month == 'Jan':
                year -= 1
            prev_ent_month = log_time_ent[0]
            log_time_string = '%d %s' % (year, ' '.join(log_time_ent))
            try:
                log_time = datetime.datetime.strptime(
                    log_time_string, '%Y %b %d %H:%M:%S')
            except ValueError:
                # Some versions use ISO timestamps instead
                try:
                    log_time = datetime.datetime.strptime(
                        line[0:19], '%Y-%m-%dT%H:%M:%S')
                except ValueError:
                    continue
            if log_time > end_time:
                for err in error_re:
                    for device in err.findall(line):
                        errors[device] = errors.get(device, 0) + 1
            else:
                reached_old_logs = True
                break
        if reached_old_logs:
            break
    return errors


def comment_fstab(mount_point):
    with open('/etc/fstab', 'r') as fstab:
        with open('/etc/fstab.new', 'w') as new_fstab:
            for line in fstab:
                parts = line.split()
                if len(parts) > 2 \
                    and parts[1] == mount_point \
                        and not line.startswith('#'):
                    new_fstab.write('#' + line)
                else:
                    new_fstab.write(line)
    os.rename('/etc/fstab.new', '/etc/fstab')


def main():
    c = ConfigParser()
    try:
        conf_path = sys.argv[1]
    except Exception:
        print("Usage: %s CONF_FILE" % sys.argv[0].split('/')[-1])
        sys.exit(1)
    if not c.read(conf_path):
        print("Unable to read config file %s" % conf_path)
        sys.exit(1)
    conf = dict(c.items('drive-audit'))
    device_dir = conf.get('device_dir', '/srv/node')
    minutes = int(conf.get('minutes', 60))
    error_limit = int(conf.get('error_limit', 1))
    recon_cache_path = conf.get('recon_cache_path', "/var/cache/swift")
    log_file_pattern = conf.get('log_file_pattern',
                                '/var/log/kern.*[!.][!g][!z]')
    log_file_encoding = conf.get('log_file_encoding', 'auto')
    if log_file_encoding == 'auto':
        log_file_encoding = locale.getpreferredencoding()
    log_to_console = config_true_value(conf.get('log_to_console', False))
    error_re = []
    for conf_key in conf:
        if conf_key.startswith('regex_pattern_'):
            error_pattern = conf[conf_key]
            try:
                r = re.compile(error_pattern)
            except re.error:
                sys.exit('Error: unable to compile regex pattern "%s"' %
                         error_pattern)
            error_re.append(r)
    if not error_re:
        error_re = [
            re.compile(r'\berror\b.*\b(sd[a-z]{1,2}\d?)\b'),
            re.compile(r'\b(sd[a-z]{1,2}\d?)\b.*\berror\b'),
        ]
    conf['log_name'] = conf.get('log_name', 'drive-audit')
    logger = get_logger(conf, log_to_console=log_to_console,
                        log_route='drive-audit')
    devices = get_devices(device_dir, logger)
    logger.debug("Devices found: %s" % str(devices))
    if not devices:
        logger.error("Error: No devices found!")
    recon_errors = {}
    total_errors = 0
    for device in devices:
        recon_errors[device['mount_point']] = 0
    errors = get_errors(error_re, log_file_pattern, minutes, logger,
                        log_file_encoding)
    logger.debug("Errors found: %s" % str(errors))
    unmounts = 0
    for kernel_device, count in errors.items():
        if count >= error_limit:
            device = \
                [d for d in devices if d['kernel_device'] == kernel_device]
            if device:
                mount_point = device[0]['mount_point']
                if mount_point.startswith(device_dir):
                    if config_true_value(conf.get('unmount_failed_device',
                                                  True)):
                        logger.info("Unmounting %s with %d errors" %
                                    (mount_point, count))
                        subprocess.call(['umount', '-fl', mount_point])
                        logger.info("Commenting out %s from /etc/fstab" %
                                    (mount_point))
                        comment_fstab(mount_point)
                        unmounts += 1
                    else:
                        logger.info("Detected %s with %d errors "
                                    "(Device not unmounted)" %
                                    (mount_point, count))
                    recon_errors[mount_point] = count
                    total_errors += count
    recon_file = recon_cache_path + "/drive.recon"
    dump_recon_cache(recon_errors, recon_file, logger)
    dump_recon_cache({'drive_audit_errors': total_errors}, recon_file, logger,
                     set_owner=conf.get("user", "swift"))

    if unmounts == 0:
        logger.info("No drives were unmounted")
    elif os.path.isdir("/run/systemd/system"):
        logger.debug("fstab updated, calling systemctl daemon-reload")
        subprocess.call(["/usr/bin/systemctl", "daemon-reload"])


if __name__ == '__main__':
    main()
