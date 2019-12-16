from datetime import datetime

from storage.azure_blob import AzureBlob,AzureBlobMetadata

from . import settings
from .download import download_by_group,get_groupfile,DOWNLOADED,NO_NEW_SCANS

def harvest():
    metadata_storage = AzureBlobMetadata(
                "nessus",
                settings.AZURE_STORAGE_CONNECTION_STRING,
                settings.NESSUS_CONTAINER)
    nessus_metadata = metadata_storage.resourcemetadata
    if nessus_metadata:
        last_scan_time = nessus_metadata["scan_endtime"]
    else:
        last_scan_time = None

    result = download_by_group(last_scan_time=last_scan_time)

    if result[0] == DOWNLOADED:
        new_metadata = {}
        uploaded_groups = {}
        for groupname,group_detail in result[1].items():
            storage = AzureBlob(
                    groupname,
                    settings.AZURE_STORAGE_CONNECTION_STRING,
                    settings.NESSUS_CONTAINER)
            groupmetadata = storage.resourcemetadata
            if groupmetadata:
                try:
                    if groupmetadata["current"]["scan_endtime"] >= group_detail["scan_endtime"]:
                        print("No new scans for resource '{}'".format(groupname))
                        continue
                except:
                    pass
            
            metadata = {
                "scan_starttime":group_detail["scan_starttime"],
                "scan_endtime":group_detail["scan_endtime"]
            }
            if "scan_starttime" not in new_metadata:
                new_metadata["scan_starttime"] = group_detail["scan_starttime"]
            elif new_metadata["scan_starttime"] > group_detail["scan_starttime"]:
                new_metadata["scan_starttime"] = group_detail["scan_starttime"]

            if "scan_endtime" not in new_metadata:
                new_metadata["scan_endtime"] = group_detail["scan_endtime"]
            elif new_metadata["scan_endtime"] < group_detail["scan_endtime"]:
                new_metadata["scan_endtime"] = group_detail["scan_endtime"]

            groupmetadata = storage.push_json(group_detail,metadata=metadata)
            uploaded_groups[groupname] = groupmetadata
            print("{}={}".format(groupname,groupmetadata))
        #update nessus metadata
        if uploaded_groups:
            metadata_storage.update(new_metadata)
            return (DOWNLOADED,uploaded_groups)
        else:
            return (NO_NEW_SCANS,"No new scans since {}".format(last_scan_time))


    else:
        return result
            
