import paramiko

def run_command(host, username, password, command):

    client = paramiko.client.SSHClient()
    client.set_missing_host_key_policy(paramiko.MissingHostKeyPolicy())
    client.connect(host, username=username, password=password)
    cmd = "grep key /etc/ceph/ceph.client.admin.keyring | awk '{print $3}'"
    _, stdout, stderr = client.exec_command(command)
    out = stdout.read().decode("utf-8").strip()
    rc = stdout.channel.recv_exit_status()
    err = stderr.read().decode("utf-8").strip()
    client.close()
    return out, err, rc
