#!/usr/bin/env python3

from __future__ import print_function

import json
import sys
import requests
from requests.auth import HTTPBasicAuth

def eprint(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)

try:
    f = open('config.json')
except OSError:
    raise SystemExit("can't open 'config.json'")

with f:
    config = json.load(f)

# get number from command line
if len(sys.argv) != 2:
    raise SystemExit('first arg MUST be house UUID.')

uuid = sys.argv[1]
r = requests.get(config["api"]["url"]+"/api/v1/endpoints/{}/friendlyName".format(uuid), auth=(config["api"]["user"], config["api"]["password"]))
r.raise_for_status()
name = r.text
print("uuid:{} name :{}".format(uuid,name))

# FIMXE get the friendly name from API

# FIXME list all series, which looks like measurement,parameter=...
# FIXME for each series
## FIXME get all values
## FIXME format (and remove .nodes .objects .attributes and replace goflex-dc-nnn by the UUID)
## split in smaller part
### for each parts, post into new db


# today's questions
## API est-ce qu'on a une fonction pour passer du pretty name au UUID
##
