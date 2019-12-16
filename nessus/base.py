import requests
import json
import traceback

from .settings import (NESSUS_BASE,NESSUS_ACCESS_KEY,NESSUS_SECRET_KEY,NESSUS_URL)

NESSUS_HEADERS = {'X-ApiKeys': 'accessKey={}; secretKey={}'.format(NESSUS_ACCESS_KEY, NESSUS_SECRET_KEY), 'Content-Type': 'application/json', 'Accept': 'text/plain'}
NESSUS_REPORT = lambda scan_id: '{}/scans/{}'.format(NESSUS_BASE, scan_id)
NESSUS_VULNS = lambda scan_id, host_id, history_id: '{}/scans/{}/hosts/{}?history_id={}'.format(NESSUS_BASE, scan_id, host_id, history_id)
NESSUS_RESULT_URL = lambda scan_id, host_id, history_id: '{}/#/scans/reports/{}/hosts/{}/vulnerabilities'.format(NESSUS_URL, scan_id, host_id, history_id)

requests.packages.urllib3.disable_warnings()

def get_json(url,filename=None):
    """
    retrive json data from nessus server
    save json data to file if filename is not none ,
    return json data to caller if filename is none
    """
    res = requests.get("{}{}".format(NESSUS_BASE,url), headers=NESSUS_HEADERS, verify=False)
    res.raise_for_status()
    try:
        if filename:
            with open(filename,'w') as f:
                f.write(json.dumps(res.json(),indent="    "))
        else:
            return res.json()
    except Exception as ex:
        traceback.print_exc() 
        raise Exception("Invalid response.{}".format(res.text))

def get_scans(filename=None):
    return get_json("/scans",filename)

def get_active_scans(filename=None):
    return get_json("/scans?folder_id=3",filename)

def get_scan(scanid,filename=None):
    return get_json("/scans/{}".format(scanid),filename)

def get_host_scan(scanid,hostid,filename=None):
    return get_json("/scans/{}/hosts/{}".format(scanid,hostid),filename)

def get_host_report_url(scanid,hostid):
    return "{}/#/scans/reports/{}/hosts/21/vulnerabilities".format(NESSUS_URL,scanid,hostid)
