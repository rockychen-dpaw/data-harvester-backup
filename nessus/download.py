import json
import os
from datetime import datetime

from .base import (get_json,get_active_scans,get_scan,get_host_scan,get_host_report_url)
from . import settings
from utils import JSONEncoder,JSONDecoder


severity_properties = {
    0:"info",
    1:"low",
    2:"medium",
    3:"high",
    4:"critical"
}
offline_severity_properties = {
    0:"offline_info",
    1:"offline_low",
    2:"offline_medium",
    3:"offline_high",
    4:"offline_critical"
}

DOWNLOADED = 1
NO_NEW_SCANS = 2
SCAN_NOT_COMPLETED = 3

def download(filename=None,download_vulnerability_detail=False,active_scans=None,last_scan_time=None):
    """
    retrieve active scan result with json format
    write it to file if filename is not none;otherwise return json object
    return
      if scan result can download, return (DOWNLOADED,filename) if filename is not none; otherwise return(DOWNLOADED, dictionary object)
      if scan result can't download, return (download_status,Reason)
    """
    active_scans = get_active_scans() if active_scans is None else active_scans

    active_scans_list = active_scans.get("scans") or []
    #check whether all scans are completed and also find the latest completed time
    starttime = None
    endtime = None
    for scan in active_scans_list:
        if scan["status"] not in ('completed','aborted'):
            #this scan is not completed
            return (SCAN_NOT_COMPLETED,"The scan(id={},name={}) is not completed".format(scan["id"],scan["name"]))
        if starttime is None:
            starttime = scan['creation_date']
        elif starttime > scan['creation_date']:
            starttime = scan['creation_date']

        if endtime is None:
            endtime = scan['last_modification_date']
        elif endtime < scan['last_modification_date']:
            endtime = scan['last_modification_date']

    starttime = datetime.fromtimestamp(float(starttime),tz=settings.TZ)
    endtime = datetime.fromtimestamp(float(endtime),tz=settings.TZ)

    if last_scan_time and last_scan_time >= endtime:
        #new new scans since last_scan_time
        return (NO_NEW_SCANS,"No new scans since {}".format(last_scan_time))

    result = {
        'scan_starttime' : starttime,
        'scan_endtime' : endtime,
        'hosts':{
        }
    }
    for scan in active_scans_list:
        scan_result = get_scan(scan["id"])
        scan_host_list = scan_result.get('hosts') or []
        for host in scan_host_list:
            scan_host_result = get_host_scan(scan['id'],host['host_id'])
            hostname = scan_host_result["info"]["host-fqdn"] if scan_host_result["info"].get("host-fqdn") else host["hostname"]
            vulnerabilities = []
            host_result = {
                "host_id":host["host_id"],
                "info":host.get("info",0),
                "low":host.get("low",0),
                "medium":host.get("medium",0),
                "high":host.get("high",0),
                "critical":host.get("critical",0),
                #"offline_info":host.get("offline_info",0),
                #"offline_low":host.get("offline_low",0),
                #"offline_medium":host.get("offline_medium",0),
                #"offline_high":host.get("offline_high",0),
                #"offline_critical":host.get("offline_critical",0),
                "severity":host.get("severity",0),
                "score":host.get("score",0),

                "host_info": scan_host_result["info"],
                "scan_id" : scan["id"],
                "scan_name" : scan["name"],
                "report_url" : get_host_report_url(scan["id"],host["host_id"]),
                "vulnerabilities":vulnerabilities

            }
            for vulner in scan_host_result["vulnerabilities"]:
                vulnerabilities.append({
                    "scan_id" :scan["id"],
                    "plugin_id" :vulner.get("plugin_id"),
                    "plugin_name" :vulner.get("plugin_name"),
                    "plugin_family" :vulner.get("plugin_family"),
                    "count" :vulner.get("count",0),
                    "severity" :vulner.get("severity",0),
                    "offline" :vulner.get("offline",False)
                })

            if hostname in result["hosts"]:
                exist_host = result["hosts"][hostname]
                if exist_host.get("other_scan_ids"):
                    exist_host["other_scan_ids"].append(host_result["scan_id"])
                    exist_host["other_scan_names"].append(host_result["scan_name"])
                    exist_host["other_report_urls"].append(host_result["report_url"])
                else:
                    exist_host["other_scan_ids"] = [host_result["scan_id"]]
                    exist_host["other_scan_names"] = [host_result["scan_name"]]
                    exist_host["other_report_urls"] = [host_result["report_url"]]

                print("{} is scaned in multiple scans.({} , {})".format(hostname,exist_host["scan_name"]," , ".join(exist_host.get("other_scan_names"))))
                
                #try to add non duplicate vulnerabilities to host, and also update the severitycount and other properties if required
                for vulner in vulnerabilities:
                    exist = False
                    if vulner["offline"]:
                        #this vulnerability is offline, ignore
                        continue
                    for exist_vulner in exist_host["vulnerabilities"]:
                        if vulner["plugin_id"] == exist_vulner["plugin_id"]:
                            exist = True
                            break
                    if exist:
                        #plugin already executed in other scans, ignore
                        continue
                    #this is a new plugin, add it to vulnerabilities
                    exist_host["vulnerabilities"].append(vulner)

                    #update the severitycount to reflect the added vulnerability
                    """
                    exist = False
                    for severitycount in  exist_host["severitycount"]["item"]:
                        if severitycount["severitylevel"] == vulner["severity"]:
                            severitycount["count"] += 1
                            exist = True
                            break

                    if not exist:
                        #severitycount does not exist, add a new one
                        exist_host["severitycount"]["item"].append({"severitylevel":vulner["severity"],"count":1})
                    """

                    #update the related properties to reflect the added vulnerability
                    if vulner["severity"] in severity_properties:
                        if vulner["offline"]:
                            exist_host[offline_severity_properties[vulner["severity"]]] = exist_host.get(offline_severity_properties[vulner["severity"]],0) + vulner["count"]
                        else:
                            exist_host[severity_properties[vulner["severity"]]] = exist_host.get(severity_properties[vulner["severity"]],0) + vulner["count"]

            else:
                result["hosts"][hostname] = host_result


    if not download_vulnerability_detail:
        for hostname,host_result in result["hosts"].items():
            if "vulnerabilities" in host_result:
                del host_result["vulnerabilities"]

    if filename:
        with open(filename,'w') as f:
            f.write(json.dumps(result,indent="    ",cls=JSONEncoder))
        return (DOWNLOADED,filename)
    else:
        return (DOWNLOADED,result)

def get_group(hostname,host_detail):
    if hostname.endswith('.wa.gov.au'):
        return "webapps"
    else:
        return "hosts"

def get_scantime(scans,scanids):
    starttime=None
    endtime=None
    for scanid in scanids:
        for scan in scans:
            if scanid != scan["id"]:
                continue
            if starttime is None:
                starttime = scan['creation_date']
            elif starttime > scan['creation_date']:
                starttime = scan['creation_date']

            if endtime is None:
                endtime = scan['last_modification_date']
            elif endtime < scan['last_modification_date']:
                endtime = scan['last_modification_date']


    starttime = datetime.fromtimestamp(float(starttime),tz=settings.TZ)
    endtime = datetime.fromtimestamp(float(endtime),tz=settings.TZ)
    return (starttime,endtime)

def get_groupfile(file_folder,filename=None):
    """
    filename can be a string or a pattern which can take timestamp as parameter,
    retuurn a function which take groupname as parameter and return a group file name
    """
    if not filename:
        filename = "scanresut_{}.json"
    filename = os.path.splitext(filename.format(datetime.now().strftime("%Y%m%d%H%M%S")))

    def _func(groupname):
        return os.path.join(file_folder,"{}_{}{}".format(filename[0],groupname,filename[1]))

    return _func

def download_by_group(f_groupfile=None,download_vulnerability_detail=False,last_scan_time=None):
    """
    f_groupfile is a function which take groupname as parameter and return a group filename 
    """
    active_scans = get_active_scans()
    downloaded,result = download(download_vulnerability_detail=download_vulnerability_detail,active_scans=active_scans,last_scan_time=last_scan_time)
    if downloaded != DOWNLOADED:
        #Can't download, return directly.
        return (downloaded,result)

    active_scan_list = active_scans.get("scans") or []
    groups_result = {}
    groups_scanids = {}
    #divide the hosts into different group
    for hostname,host_detail in result["hosts"].items():
        groupname = get_group(hostname,host_detail)
        if groupname not in groups_result:
            groups_scanids[groupname] = set()
            groups_result[groupname] = {
                'hosts':{
                    hostname:host_detail
                }
            }
        else:
            groups_result[groupname]["hosts"][hostname] = host_detail

        #get the scanid list for each group
        groups_scanids[groupname].add(host_detail["scan_id"])
        if "other_scan_ids" in host_detail:
            for scan_id in host_detail["other_scan_ids"]:
                groups_scanids[groupname].add(scan_id)

    #get scan start and end time for each group
    for groupname,group_detail in groups_result.items():
        starttime,endtime = get_scantime(active_scan_list,groups_scanids[groupname])
        group_detail["scan_starttime"] = starttime
        group_detail["scan_endtime"] = endtime


    if f_groupfile:
        groupfiles = {
        }
        for groupname,group_detail in groups_result.items():
            groupfile = f_groupfile(groupname)
            with open(groupfile,'w') as f:
                f.write(json.dumps(group_detail,indent="    ",cls=JSONEncoder))

            groupfiles[groupname] = groupfile

        return (DOWNLOADED,groupfiles)

    else:
        return (DOWNLOADED,groups_result)



