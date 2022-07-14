# SPDX-FileCopyrightText: 2022 SoftIron Limited <info@softiron.com>
# SPDX-License-Identifier: GNU General Public License v2.0 only WITH Classpath exception 2.0

import json
import subprocess
import sys


def load_keys(filename):
    """ Load the S3 keys from a json file. """

    try:
        with open(filename) as json_file:
            data = json.load(json_file)
            secret_key = data['secret_key']
            access_key = data['access_key']
            return (secret_key, access_key)
    except:
        print("Unable to read keys from file: " + filename)
        exit(-1)


def add_user(username, keyfile, gateway, password):
    """ Adds a user to the rados gatweays, and writes the resulting key to s3.keys.
        We exit on failure. """

    cmd =  'sshpass -p ' + password
    cmd += ' ssh -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no root@' + gateway
    cmd += ' radosgw-admin user create --uid={} --display-name={}'.format(username, username) 

    rc = subprocess.run(cmd, shell=True, capture_output=True, check=True)
    out = rc.stdout.decode("utf-8")
    
    # Try parsing the result
    try:
        data = json.loads(out)
        keys = data['keys'][0]
    except:
        print("Unable to read parse keys from: " + data)
        exit(-1)

    # Write the result to a file
    try:
        with open(keyfile, 'w') as f:
            json.dump(keys, f)
    except:
        print("Unable to write keys to file: " + keyfile)
        exit(-1)


