import decimal
import os
import pytest
import requests
import time
import json

import logging
log = logging.getLogger(__name__)

REF_BASE = 'https://gep-api.energydata.info'
TEST_BASE = 'http://localhost:8000'

SAVE_ORIGINAL=False
DL_ORIGINAL=False

def float_near(a, b, eps="0.001"):
    eps = decimal.Decimal(eps)
    return decimal.Decimal(a).quantize(eps) == decimal.Decimal(b).quantize(eps)

def lists_equal(calc, ref):
    for calc_val, ref_val in zip(calc, ref):
        if isinstance(calc_val, list):
            lists_equal(calc_val, ref_val)
        elif isinstance(calc_val, dict):
            dicts_equal(calc_val, ref_val)
        elif isinstance(calc_val, str):
            assert calc_val == ref_val
        elif calc_val is None or ref_val is None:
            assert bool(calc_val) == bool(ref_val)
        else:
            assert float_near(calc_val, ref_val)

def dicts_equal(calc_dict, ref_dict):
    for k,v in ref_dict.items():
        log.debug("Comparing %s", k)
        if isinstance(v, list):
            lists_equal(calc_dict[k], v)
        elif isinstance(v, dict):
            dicts_equal(calc_dict[k], v)
        elif isinstance(v, str):
            assert calc_dict[k] == v
        elif calc_dict[k] is None or v is None:
            assert bool(calc_dict[k]) == bool(v)
        else:
            assert float_near(calc_dict[k], v)
    return True

with open (os.path.join(os.path.dirname(__file__), 'urls.txt'), 'r') as f:
    urls = [l.strip() for l in f if l.strip()]

session = requests.Session()

@pytest.mark.parametrize('url', urls)
def test_url(url):
    log.debug("Getting %s" % url)
    start_time = time.time()
    if DL_ORIGINAL:
        target = session.get(REF_BASE + url)
        target_time = time.time()
        if SAVE_ORIGINAL:
            with open(os.path.join("tests/originals", url.split('/')[-1]), 'wb') as f:
                f.write(target.text.encode('utf8'))
        target_data = target.json()
    else:
        with open(os.path.join("tests/originals", url.split('/')[-1]), 'r') as f:
            target_data = json.load(f)
        target_time = time.time()
            
    local = requests.get(TEST_BASE + url)
    local_time = time.time()
    log.info("Url: %s Target Time: %4.2f, Local time: %4.2f", url, target_time- start_time, local_time- target_time)

    assert dicts_equal(local.json(), target_data)
