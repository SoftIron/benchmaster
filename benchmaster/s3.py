import json
import subprocess
import sys


def add_user(username, keyfile):
    """ Adds a user to the rados gatweays, and writes the resulting key to s3.keys.
        We exit on failure. """

    # Add the user to gateway
    cmd =['radosgw-admin', 'user', 'create', '--uid={}'.format(username), '--display-name={}'.format(username)] 
    try:
        out = subprocess.run(cmd, capture_output=True, check=True)
    except CallProccessErrori as e:
        print("Failure adding user to rados gateway: {}".format(e))
        exit(-1)

    # Try parsing the result
    try:
        data = json.loads(out.stdout)
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


