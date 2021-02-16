#!/usr/bin/env python3

from __future__ import print_function

import json
import urllib
import sys
import requests
import os
from requests.auth import HTTPBasicAuth
from itertools import islice

def eprint(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)

def chunk(it, size):
    it = iter(it)
    return iter(lambda: tuple(islice(it, size)), ())

def get_series(config):
    if os.path.exists(".cache/cache.txt") != True:
        # FIXME list all series, which looks like measurement,parameter=...
        headers = {
            'Accept': 'application/csv',
        }

        params = {
            'db' : config["in"]["db"],
            'q' : "show series"
        }

        r = requests.get(config["in"]["url"]+"/query", headers=headers, auth=(config["in"]["user"], config["in"]["password"]), params=params)
        r.raise_for_status()
        file = open(".cache/cache.txt", "w")
        file.write(r.text)
        file.close()
        ret = r.text.splitlines()
    else:
        file = open(".cache/cache.txt", "r")
        ret = file.read().splitlines()
        file.close()
    ret = list(map(lambda x : x.replace(",,", ""), ret))
    ret = list(map(lambda x : x.replace("\"", ""), ret))
    return ret

def push_data(config, l):
    params = {
        'db' : config["out"]["db"]
    }

    data = "\n".join(l)

    r = requests.post(config["out"]["url"]+"/write", auth=(config["out"]["user"], config["out"]["password"]), params=params, data=data)
    r.raise_for_status()

def get_values(config, measurement):
    headers = {
        'Accept': 'application/csv',
    }

    params = {
        'db' : config["in"]["db"],
        'q' : "select * from \"{}\"".format(measurement)
    }

    r = requests.get(config["in"]["url"]+"/query", headers=headers, auth=(config["in"]["user"], config["in"]["password"]), params=params)
    r.raise_for_status()
    return r.text.splitlines()[1:]

def fixes_02(l):
    # goflex -> domos
    l=l.replace(name_old, uuid)

    # cloudio 0.1 -> 0.2
    l=l.replace(".nodes.", ".")
    l=l.replace(".objects.", ".")
    l=l.replace(".attributes.", ".")

    # smartmeter renaming
    l=l.replace(".SmartMeterBilling.", ".SmartMeter.billing.")
    l=l.replace(".SmartMeterEnergy.", ".SmartMeter.energy.")
    l=l.replace(".SmartMeterTechnical.", ".SmartMeter.technical.")
    l=l.replace(".relay1State.datapoint", ".relay1.state")
    l=l.replace(".relay2State.datapoint", ".relay2.state")

    if ".SmartMeter." in l:
        l=l.replace(".datapoint","")
    return l

def line_convert(l):
    x=l.split(",")
    name=serie_out
    time=x[2]
    constraint=x[3]
    type=x[4]
    value=x[5]
    if type == "String" and value.isnumeric() == False:
        value='"' + value + '"'
    return "{},constraint={},type={} value={} {}".format(name,constraint,type,value,time)

try:
    f = open('config.json')
except OSError:
    raise SystemExit("can't open 'config.json'")

with f:
    config = json.load(f)

# get number from command line
if len(sys.argv) != 2:
    raise SystemExit('first arg MUST be house UUID.')

if os.path.exists(".cache") != True:
    os.mkdir(".cache")

uuid = sys.argv[1]
r = requests.get(config["api"]["url"]+"/api/v1/endpoints/{}/friendlyName".format(uuid), auth=(config["api"]["user"], config["api"]["password"]))
r.raise_for_status()
friendlyName = r.text
print("uuid:{} name :{}".format(uuid,friendlyName))

name_old = friendlyName.replace('domos', 'goflex')

series = get_series(config)
series = list(filter(lambda t : t.startswith(name_old), series))
series = list(filter(lambda t :  "constraint" in t, series))

for serie in series:
    m = serie.split(",")[0]
    serie_out = fixes_02(m)
    chunk_size = 100*1000
    print(serie_out[serie_out.find(".")+1:], end="")
    sys.stdout.flush()
    out = list(chunk(map(line_convert, get_values(config, m)),chunk_size))
    print(" chuncks: {} ".format(len(out)), end="")
    sys.stdout.flush()
    for l in out:
        push_data(config, l)
        print("*", end="")
        sys.stdout.flush()
    print()

# today's questions
## API est-ce qu'on a une fonction pour passer du pretty name au UUID
##
