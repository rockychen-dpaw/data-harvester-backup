from datetime import datetime
import traceback

from storage.azure_blob import AzureBlobResource,AzureBlobResourceMetadata

from . import settings
from .download import download_by_group,get_groupfile,DOWNLOADED,NO_NEW_SCANS

def harvest():
    metadata_client = AzureBlobResourceMetadata(
                settings.AZURE_STORAGE_CONNECTION_STRING,
                settings.NESSUS_CONTAINER,
                cache=True)
    nessus_metadata = metadata_client.json
    if nessus_metadata:
        last_scan_time = nessus_metadata.get("scan_endtime")
        new_metadata = dict(nessus_metadata)
    else:
        last_scan_time = None
        new_metadata = {}
    new_metadata["harvest_status"] =  "succeed",
    new_metadata["harvest_starttime"] = datetime.now(tz=settings.TZ)
    try:
        result = download_by_group(last_scan_time=last_scan_time)
    
        if result[0] == DOWNLOADED:
            uploaded_groups = {}
            group_messages = {}
            for groupname,group_detail in result[1].items():
                #calculate the scan starttime and scan endtime
                if "scan_starttime" not in new_metadata:
                    new_metadata["scan_starttime"] = group_detail["scan_starttime"]
                elif new_metadata["scan_starttime"] > group_detail["scan_starttime"]:
                    new_metadata["scan_starttime"] = group_detail["scan_starttime"]
    
                if "scan_endtime" not in new_metadata:
                    new_metadata["scan_endtime"] = group_detail["scan_endtime"]
                elif new_metadata["scan_endtime"] < group_detail["scan_endtime"]:
                    new_metadata["scan_endtime"] = group_detail["scan_endtime"]
    
                storage = AzureBlobResource(
                        groupname,
                        settings.AZURE_STORAGE_CONNECTION_STRING,
                        settings.NESSUS_CONTAINER)
                groupmetadata = storage.resourcemetadata
                if groupmetadata:
                    try:
                        if groupmetadata["current"]["scan_endtime"] >= group_detail["scan_endtime"]:
                            print("No new scans for resource '{}'".format(groupname))
                            group_messages[groupname] = ("No new scans for resource '{}' since {}".format(groupname,groupmetadata["current"]["scan_endtime"]))
                            continue
                    except:
                        pass
                
                metadata = {
                    "scan_starttime":group_detail["scan_starttime"],
                    "scan_endtime":group_detail["scan_endtime"]
                }
    
                groupmetadata = storage.push_json(group_detail,metadata=metadata)
                uploaded_groups[groupname] = groupmetadata
                print("{}={}".format(groupname,groupmetadata))
            #update nessus metadata
            if uploaded_groups:
                if group_messages:
                    new_metadata["harvest_message"] = group_messages
                else:
                    new_metadata["harvest_message"] = "OK"
                new_metadata["harvest_detail"] = uploaded_groups
                return (DOWNLOADED,uploaded_groups)
            else:
                result = (NO_NEW_SCANS,"No new scans since {}".format(last_scan_time))
                new_metadata["harvest_message"] = result[1]
                return result
        else:
            new_metadata["harvest_message"] = result[1]
            return result
    except:
        new_metadata["harvest_status"] = "failed"
        new_metadata["harvest_message"] = traceback.format_exc()
        raise
    finally:
        new_metadata["harvest_endtime"] = datetime.now(tz=settings.TZ)
        metadata_client.update(new_metadata)

            
