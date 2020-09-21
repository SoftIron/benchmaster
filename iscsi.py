#!/usr/bin/python3

import subprocess
import time



class IscsiArgs:
    """ Master spec object. """
    def __init__(self, gateways, gateway_pw, servers, server_pw, pool, image_size, device_link):
        self.gateways = gateways
        self.gateway_pw = gateway_pw
        self.servers = servers
        self.server_pw = server_pw
        self.pool = pool
        self.image_size = image_size
        self.device_link = device_link



def _ssh_cmd(host, rootpw, cmd, check=True):
    ssh_cmd = 'sshpass -p {} ssh -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no root@{} {}'.format(rootpw, host, cmd)
    print("Running SSH command: {}".format(ssh_cmd))
    rc = subprocess.run(ssh_cmd, shell=True, capture_output=True, check=check)
    return rc.stdout.decode('utf-8')
     


def _fetch_gateway_hostids(args):
    host_ids = []
    for gw in args.gateways:
        print("Fetching hostid from {}".format(gw))
        id = _ssh_cmd(gw, args.gateway_pw, 'hostid')
        host_ids.append(id.rstrip())
    return host_ids



def _image_name(server):
    return 'benchmaster-{}'.format(server)



def _setup_initiator(args):
    initator = 'qn.2014-01.com.softiron.iscsi_gw_v0:' + args.gateways[0]
    for s in args.servers:
        _ssh_cmd(s, args.server_pw, "sed -i 's/InitiatorName=.*/InitiatorName={}/' /etc/iscsi/initiatorname.iscsi".format(initator))
        _ssh_cmd(s, args.server_pw, 'systemctl restart iscsid')


def _create_images(args):
    for s in args.servers:
        name = _image_name(s)
        cmd = 'rbd create {}/{} --size {} --image-feature=layering,exclusive-lock'.format(args.pool, name, args.image_size)
        _ssh_cmd(args.gateways[0], args.gateway_pw, cmd)



def _delete_images(args):
    for s in args.servers:
        name = _image_name(s)
        cmd = 'rbd rm {} -p {}'.format(name, args.pool)
        _ssh_cmd(args.gateways[0], args.gateway_pw, cmd)



def _configure_images_on_gateways(args, operation, host_ids):
    flag = ""
    if    operation == 'export': flag = '-e'
    elif  operation == 'unexport': flag = '-u'
    elif  operation == 'reset': flag = '-r'
    else: raise Exception('Unsupported operation: {}'.format(operation))

    for s in args.servers:
        name = _image_name(s)
        cmd = 'rsmapadm -p {} {} {}'.format(args.pool, flag, name)
        for id in host_ids:
            cmd += ' -t {}'.format(id)

    _ssh_cmd(args.gateways[0], args.gateway_pw, cmd)



def _mount_images(args):
    for s in args.servers:
        name = _image_name(s)

        # Log in to the active and secondary ISCSI targets.
        for mode in ['act', 'nop']:
            login_cmd = 'iscsiadm --mode node --login --target iqn.2014-01.com.softiron.iscsi_gw_v0:{}-{}-{}'.format(args.pool, name, mode)
            _ssh_cmd(s, args.server_pw, login_cmd)
    
        # Determine which devices they were assigned to.
        disk_cmd = 'ls -l /dev/disk/by-path/ | grep iqn.2014-01.com.softiron.iscsi_gw_v0:{}-{}'.format(args.pool, name)
        lines = _ssh_cmd(s, args.server_pw, disk_cmd)

        mp_cmd = 'multipath -a'
        for l in lines.split('\n'):
            if l != '':
                mp_cmd += ' /dev/' + l[-3:]

        # We can't check the return code of this call, since rsmapamd returns the number of added mappings as the rc.
        # It shouldn't do that because it violates the standard, but we have to live with it, so we'll disable checking for 
        # this command.
        # (it should be output on stdout, or should only output as RC if we pass an explicit flag telling it to do so).
        _ssh_cmd(s, args.server_pw, mp_cmd, check=False)
       
        # Now we've added the mapping, tell multipath to use it.
        _ssh_cmd(s, args.server_pw, 'multipath -r')
    
        # Retrieve the device mapper entry
        dm = _ssh_cmd(s, args.server_pw, "multipath -l | grep benchmaster | awk '{print $3}'")

        # Create the link,
        link_cmd = 'ln -s /dev/{} {}'.format(str(dm).rstrip(), args.device_link)
        _ssh_cmd(s, args.server_pw, link_cmd)

   
 
def _unmount_images(args):
    for s in args.servers:
        _ssh_cmd(s, args.server_pw, 'unlink {}'.format(args.device_link))
        _ssh_cmd(s, args.server_pw, 'iscsiadm --mode node --logoutall=all')
        _ssh_cmd(s, args.server_pw, 'multipath -W')
                    


def setup(args):
    host_ids = _fetch_gateway_hostids(args)

    _setup_initiator(args)
    _create_images(args)
    _configure_images_on_gateways(args, 'export', host_ids)
    _mount_images(args)
    print("Ready.")



def teardown(args):
    host_ids = _fetch_gateway_hostids(args)

    _unmount_images(args)
    _configure_images_on_gateways(args, 'unexport', host_ids)
    _delete_images(args)
    print("Ready.")

