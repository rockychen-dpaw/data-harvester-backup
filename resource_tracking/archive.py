from datetime import datetime,date
import os
import traceback
import logging
import tempfile
from datetime import datetime,timedelta


from utils import timezone,gdal
import utils

from storage.azure_blob import AzureBlobResource,AzureBlobResourceMetadata

from . import settings

logger = logging.getLogger(__name__)

sql = "SELECT a.*,b.deviceid,b.registration FROM tracking_loggedpoint a JOIN tracking_device b ON a.device_id = b.id WHERE a.seen >= '{0}' AND a.seen < '{1}'"
del_sql = "DELETE FROM tracking_loggedpoint WHERE seen >= '{0}' AND seen < '{1}'"
pattern = "%Y-%m-%d %H:%M:%S %Z"
vrt = """
<OGRVRTDataSource>
    <OGRVRTUnionLayer name="loggedpoint">
    {}
    </OGRVRTUnionLayer>
</OGRVRTDataSource>
"""
individual_layer = """
        <OGRVRTLayer name="loggedpoint">
            <SrcDataSource>{}</SrcDataSource>
        </OGRVRTLayer>
"""

def archive_by_date(d,delete_after_archive=False):
    """
    Archive the logged point within the specified date
    """
    archive_group = d.strftime("loggedpoint%Y-%m")
    archive_id= d.strftime("loggedpoint%Y-%m-%d.gpkg")
    start_date = timezone.datetime(d.year,d.month,d.day)
    end_date = start_date + timedelta(days=1)
    return archive(archive_group,archive_id,start_date,end_date,delete_after_archive=delete_after_archive)


def _set_end_datetime(key):
    def _func(metadata):
        metadata[key] = timezone.now()
    return _func

def archive(archive_group,archive_id,start_date,end_date,delete_after_archive=False):
    """
    Archive the resouce tracking history by start_date(inclusive), end_date(exclusive)
    """
    db = settings.DATABASE
    metadata = {"start_archive":timezone.now(),"resource_id":archive_id,"resource_group":archive_group}

    archive_sql = sql.format(start_date.strftime(pattern),end_date.strftime(pattern))
    filename = None
    d_filename = None
    vrt_filename = None
    d_vrt_filename = None
    work_folder = tempfile.mkdtemp(prefix="archive_loggedpoint")
    def set_end_archive(metadata):
        metadata["end_archive"] = timezone.now()
    try:
        logger.debug("Begin to archive loggedpoint, archive_group={},archive_id={},start_date={},end_date={}".format(archive_group,archive_id,start_date,end_date))
        layer_metadata,filename = db.export_spatial_data(archive_sql,file_name=os.path.join(work_folder,"loggedpoint.gpkg"),layer="loggedpoint")
        metadata["file_md5"] = utils.file_md5(filename)
        metadata["layer"] = layer_metadata["layer"]
        metadata["features"] = layer_metadata["features"]
        blob_resource = AzureBlobResource(
                "loggedpoint",
                settings.AZURE_CONNECTION_STRING,
                settings.AZURE_CONTAINER,
                group_resource=True,
                archive=False,
        )
        #upload archive file
        logger.debug("Begin to push loggedpoint archive file to blob storage, archive_group={},archive_id={},start_date={},end_date={}".format(archive_group,archive_id,start_date,end_date))
        import ipdb;ipdb.set_trace()
        resourcemetadata = blob_resource.push_file(filename,metadata=metadata,f_post_push=_set_end_datetime("end_archive"))
        #check whether uploaded succeed or not
        logger.debug("Begin to check whether loggedpoint archive file was pushed to blob storage successfully, archive_group={},archive_id={},start_date={},end_date={}".format(
            archive_group,archive_id,start_date,end_date
        ))
        d_metadata,d_filename = blob_resource.download(archive_id,resource_group=archive_group,filename=os.path.join(work_folder,"loggedpoint_download.gpkg"))
        d_file_md5 = utils.file_md5(d_filename)
        if metadata["file_md5"] == d_file_md5:
            raise Exception("Upload loggedpoint archive file failed.source file's md5={}, uploaded file's md5={}".format(metadata["file_md5"],d_file_md5))

        d_layer_metdata = gdal.get_layers(d_filename)[0]
        if d_layer_metadata["features"] != layer_metadata["features"]:
            raise Exception("Upload loggedpoint archive file failed.source file's features={}, uploaded file's features={}".format(layer_metadata["features"],d_layer_metadata["features"]))
        

        #update vrt file
        logger.debug("Begin to update vrt file to union all spatial files in the same group, archive_group={},archive_id={},start_date={},end_date={}".format(
            archive_group,archive_id,start_date,end_date
        ))
        vrt_id = "{}.vrt".format(archive_group)
        try:
            d_vrt_metadata = next(m for m in resourcemetadata[archive_group] if m["resource_id"] == vrt_id)
        except:
            d_vrt_metadata = None

        vrt_metadata = {"resource_id":vrt_id,"resource_group":archive_group}
        vrt_metadata["features"] = metadata["features"] + (d_vrt_metadata["features"] if d_vrt_metadata else 0)

        groupmetadata = resourcemetadata[archive_group]
        individual_layers = os.linesep.join(individual_layer.format(m["resource_id"] for m in groupmetadata if m["resource_id"] != "{}.vrt".format(archive_group)))
        vrt_data = vrt.format(individual_layers)
        vft_filename = os.path.join(work_folder,"loggedpoint.vrt")
        with open(vrt_filename,"wb") as f:
            f.write(vrt_data)

        vrt_metadata["file_md5"] = utils.file_md5(vrt_filename)

        resourcemetadata = blob_resource.push_file(vrt_filename,metadata=vrt_metadata,f_post_push=_set_end_datetime("updated"))
        #check whether uploaded succeed or not
        logger.debug("Begin to check whether the group vrt file was pused to blob storage successfully, archive_group={},archive_id={},start_date={},end_date={}".format(
            archive_group,archive_id,start_date,end_date
        ))
        d_vrt_metadata,d_vrt_filename = blob_resource.download(vrt_metadata["resource_id"],resource_group=archive_group,filename=os.path.join(work_folder,"loggedpoint_download.vrt"))
        d_vrt_file_md5 = utils.file_md5(d_vrt_filename)
        if vrt_metadata["file_md5"] == d_vrt_file_md5:
            raise Exception("Upload vrt file failed.source file's md5={}, uploaded file's md5={}".format(vrt_metadata["file_md5"],d_vrt_file_md5))

        if delete_after_archive:
            logger.debug("Begin to delete archived data, archive_group={},archive_id={},start_date={},end_date={}".format(
                archive_group,archive_id,start_date,end_date
            ))

            del_sql = del_sql.format(start_date.strftime(pattern),end_date.strftie(pattern))
            deleted_rows = db.update(del_sql)
            logger.debug("Delete {} rows from table tracking_loggedpoint, archive_group={},archive_id={},start_date={},end_date={}".format(
                deleted_rows,archive_group,archive_id,start_date,end_date
            ))

        logger.debug("End to archive loggedpoint, archive_group={},archive_id={},start_date={},end_date={}".format(archive_group,archive_id,start_date,end_date))


    finally:
        #utils.remove_folder(work_folder)
        pass
            



archive_by_date(date(2019,11,16),delete_after_archive=False)
    

            
