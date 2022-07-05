import json
import subprocess
import sys
import benchmaster.ssh as ssh


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
    cmd = 'radosgw-admin user create --uid={} --display-name={}'.format(username, username)
    out, err, rc = ssh.run_command(gateway, 'root', password, cmd)

    if rc != 0:
        print(f"Failed to create radosgw user: {err}")
        exit(-1)

    # Try parsing the result
    try:
        data = json.loads(out)
        keys = data['keys'][0]
    except:
        print("Unable to read parse keys from: " + out)
        exit(-1)

    # Write the result to a file
    try:
        with open(keyfile, 'w') as f:
            json.dump(keys, f)
    except:
        print("Unable to write keys to file: " + keyfile)
        exit(-1)


