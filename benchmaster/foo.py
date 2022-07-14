#!/usr/bin/python3

import json
import subprocess

out = subprocess.check_output("grep Analyses -A 100000 sibench.json", shell=True).decode('utf-8')
jout = "{\n" + out

data = json.loads(jout)
print(data)

