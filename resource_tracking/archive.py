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

earliest_archive_date = "SELECT min(seen) FROM tracking_loggedpoint"
missing_device_sql = "INSERT INTO tracking_device (deviceid) SELECT distinct a.deviceid FROM {0} a WHERE NOT EXISTS(SELECT 1 FROM tracking_device b WHERE a.deviceid = b.deviceid)"
restore_with_id_sql = """INSERT INTO tracking_loggedpoint (id,device_id,point,heading,velocity,altitude,seen,message,source_device_type,raw)
    SELECT a.id,b.id,a.point,a.heading,a.velocity,a.altitude,to_timestamp(a.seen),a.message,a.source_device_type,a.raw
    FROM {0} a JOIN tracking_device b on a.deviceid = b.deviceid"""

restore_sql = """INSERT INTO tracking_loggedpoint (device_id,point,heading,velocity,altitude,seen,message,source_device_type,raw)
    SELECT b.id,a.point,a.heading,a.velocity,a.altitude,a.seen,a.message,a.source_device_type,a.raw
    FROM {0} a JOIN tracking_device b on a.deviceid = b.deviceid"""

archive_sql = "SELECT a.id,a.point,a.heading,a.velocity,a.altitude,a.message,a.source_device_type,a.raw,extract(epoch from a.seen)::bigint as seen,b.deviceid,b.registration FROM tracking_loggedpoint a JOIN tracking_device b ON a.device_id = b.id WHERE a.seen >= '{0}' AND a.seen < '{1}'"
del_sql = "DELETE FROM tracking_loggedpoint WHERE seen >= '{0}' AND seen < '{1}'"
datetime_pattern = "%Y-%m-%d %H:%M:%S %Z"
vrt = """<OGRVRTDataSource>
    <OGRVRTUnionLayer name="{}">
    {}
    </OGRVRTUnionLayer>
</OGRVRTDataSource>"""

individual_layer = """        <OGRVRTLayer name="{}">
            <SrcDataSource>{}</SrcDataSource>
        </OGRVRTLayer>"""

get_archive_group = lambda d:d.strftime("loggedpoint%Y-%m")
get_archive_id= lambda d:d.strftime("loggedpoint%Y-%m-%d")


_blob_resource = None
def get_blob_resource():
    global _blob_resource
    if _blob_resource is None:
        _blob_resource = AzureBlobResource(
            "loggedpoint",
            settings.AZURE_CONNECTION_STRING,
            settings.AZURE_CONTAINER,
            group_resource=True,
            archive=False,
        )
    return _blob_resource

def continuous_archive(delete_after_archive=False,check=False,max_archive_days=None):
    db = settings.DATABASE
    earliest_date = db.get(earliest_archive_date)
    now = datetime.now()
    today = now.date()
    if settings.END_WORKING_HOUR is not None and now.hour <= settings.END_WORKING_HOUR:
        if settings.START_WORKING_HOUR is None or now.hour >= settings.START_WORKING_HOUR:
            raise Exception("Please don't run continuous archive in working hour")

    if settings.START_WORKING_HOUR is not None and now.hour >= settings.START_WORKING_HOUR:
        if settings.END_WORKING_HOUR is None or now.hour <= settings.END_WORKING_HOUR:
            raise Exception("Please don't run continuous archive in working hour")

    logger.info("Begin to continuous archiving loggedpoint, earliest archive date={0}, delete_after_archive={1}, check={2}, max_archive_days={3}".format(
        earliest_date,delete_after_archive,check,max_archive_days
    ))

    last_archive_date = today - timedelta(days=settings.LOGGEDPOINT_ACTIVE_DAYS)
    archive_date = earliest_date
    archived_days = 0
    max_archive_days = max_archive_days if max_archive_days > 0 else None
    while archive_date < last_archive_date and (not max_archive_days or archived_days < max_archive_days):
        archive_by_date(archive_date,delete_after_archive=delete_after_archive,check=check)
        archive_date += timedelta(days=1)
        archived_days += 1

def archive_by_month(year,month,delete_after_archive=False,check=False):
    now = datetime.now()
    today = now.date()
    archive_date = date(year,month,1)
    last_archive_date = date(archive_date.year if archive_date.month < 12 else (archive_date.year + 1), (archive_date.month + 1) if archive_date.month < 12 else 1,1)
    if last_archive_date >= today:
        last_archive_date = today
    if archive_date >= today:
        raise Exception("Can only archive the logged points happened before today")

    logger.info("Begin to archive loggedpoint by month, month={0}/{1}, start archive date={2}, end archive date={3}".format(
        year,month,archive_date,last_archive_date
    ))

    while archive_date < last_archive_date:
        archive_by_date(archive_date,delete_after_archive=delete_after_archive,check=check)
        archive_date += timedelta(days=1)

def archive_by_date(d,delete_after_archive=False,check=False):
    """
    Archive the logged point within the specified date
    """
    now = datetime.now()
    today = now.date()
    if d >= today:
        raise Exception("Can only archive the logged points happened before today")
    archive_group = get_archive_group(d)
    archive_id= get_archive_id(d)
    start_date = timezone.datetime(d.year,d.month,d.day)
    end_date = start_date + timedelta(days=1)
    return archive(archive_group,archive_id,start_date,end_date,delete_after_archive=delete_after_archive,check=check)


def _set_end_datetime(key):
    def _func(metadata):
        metadata[key] = timezone.now()
    return _func

def archive(archive_group,archive_id,start_date,end_date,delete_after_archive=False,check=False):
    """
    Archive the resouce tracking history by start_date(inclusive), end_date(exclusive)
    """
    db = settings.DATABASE
    resource_id = "{}.gpkg".format(archive_id)
    metadata = {"start_archive":timezone.now(),"resource_id":resource_id,"resource_group":archive_group}

    filename = None
    vrt_filename = None
    work_folder = tempfile.mkdtemp(prefix="archive_loggedpoint")
    def set_end_archive(metadata):
        metadata["end_archive"] = timezone.now()
    try:
        logger.debug("Begin to archive loggedpoint, archive_group={},archive_id={},start_date={},end_date={}".format(archive_group,archive_id,start_date,end_date))
        sql = archive_sql.format(start_date.strftime(datetime_pattern),end_date.strftime(datetime_pattern))
        export_result = db.export_spatial_data(sql,filename=os.path.join(work_folder,"loggedpoint.gpkg"),layer=archive_id)
        if not export_result:
            logger.debug("No loggedpoints to archive, archive_group={},archive_id={},start_date={},end_date={}".format(archive_group,archive_id,start_date,end_date))
            return

        layer_metadata,filename = export_result
        metadata["file_md5"] = utils.file_md5(filename)
        metadata["layer"] = layer_metadata["layer"]
        metadata["features"] = layer_metadata["features"]
        blob_resource = get_blob_resource()
        #upload archive file
        logger.debug("Begin to push loggedpoint archive file to blob storage, archive_group={},archive_id={},start_date={},end_date={}".format(archive_group,archive_id,start_date,end_date))
        resourcemetadata = blob_resource.push_file(filename,metadata=metadata,f_post_push=_set_end_datetime("end_archive"))
        if check:
            #check whether uploaded succeed or not
            logger.debug("Begin to check whether loggedpoint archive file was pushed to blob storage successfully, archive_group={},archive_id={},start_date={},end_date={}".format(
                archive_group,archive_id,start_date,end_date
            ))
            d_metadata,d_filename = blob_resource.download(resource_id,resource_group=archive_group,filename=os.path.join(work_folder,"loggedpoint_download.gpkg"))
            d_file_md5 = utils.file_md5(d_filename)
            if metadata["file_md5"] != d_file_md5:
                raise Exception("Upload loggedpoint archive file failed.source file's md5={}, uploaded file's md5={}".format(metadata["file_md5"],d_file_md5))

            d_layer_metadata = gdal.get_layers(d_filename)[0]
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
        individual_layers = os.linesep.join(individual_layer.format(os.path.splitext(m["resource_id"])[0],m["resource_id"]) for m in groupmetadata if m["resource_id"] != "{}.vrt".format(archive_group))
        vrt_data = vrt.format(archive_group,individual_layers)
        vrt_filename = os.path.join(work_folder,"loggedpoint.vrt")
        with open(vrt_filename,"w") as f:
            f.write(vrt_data)

        vrt_metadata["file_md5"] = utils.file_md5(vrt_filename)

        resourcemetadata = blob_resource.push_file(vrt_filename,metadata=vrt_metadata,f_post_push=_set_end_datetime("updated"))
        if check:
            #check whether uploaded succeed or not
            logger.debug("Begin to check whether the group vrt file was pused to blob storage successfully, archive_group={},archive_id={},start_date={},end_date={}".format(
                archive_group,archive_id,start_date,end_date
            ))
            d_vrt_metadata,d_vrt_filename = blob_resource.download(vrt_metadata["resource_id"],resource_group=archive_group,filename=os.path.join(work_folder,"loggedpoint_download.vrt"))
            d_vrt_file_md5 = utils.file_md5(d_vrt_filename)
            if vrt_metadata["file_md5"] != d_vrt_file_md5:
                raise Exception("Upload vrt file failed.source file's md5={}, uploaded file's md5={}".format(vrt_metadata["file_md5"],d_vrt_file_md5))

        if delete_after_archive:
            logger.debug("Begin to delete archived data, archive_group={},archive_id={},start_date={},end_date={}".format(
                archive_group,archive_id,start_date,end_date
            ))

            delete_sql = del_sql.format(start_date.strftime(datetime_pattern),end_date.strftime(datetime_pattern))
            deleted_rows = db.update(delete_sql)
            logger.debug("Delete {} rows from table tracking_loggedpoint, archive_group={},archive_id={},start_date={},end_date={}".format(
                deleted_rows,archive_group,archive_id,start_date,end_date
            ))

        logger.debug("End to archive loggedpoint, archive_group={},archive_id={},start_date={},end_date={}".format(archive_group,archive_id,start_date,end_date))


    finally:
        #utils.remove_folder(work_folder)
        pass
            
def restore_by_month(year,month,restore_to_origin_table=False,preserve_id=True):
    d = date(year,month,1)
    archive_group = get_archive_group(d)
    logger.debug("Begin to import archived loggedpoint, archive_group={}".format(archive_group))
    blob_resource = get_blob_resource()
    work_folder = tempfile.mkdtemp(prefix="restore_loggedpoint")
    try:
        metadata,filename = blob_resource.download_group(archive_group,folder=work_folder,overwrite=True)
        imported_table = _restore_data(os.path.join(work_folder,"{}.vrt".format(archive_group)),restore_to_origin_table=restore_to_origin_table,preserve_id=preserve_id)
        logger.debug("End to import archived loggedpoint, archive_group={},imported_table".format(archive_group,imported_table))
    finally:
        utils.remove_folder(work_folder)
        pass


def restore_by_date(d,restore_to_origin_table=False,preserve_id=True):
    archive_group = get_archive_group(d)
    archive_id= get_archive_id(d)
    resource_id = "{}.gpkg".format(archive_id)
    logger.debug("Begin to import archived loggedpoint, archive_group={},archive_id={}".format(archive_group,archive_id))
    blob_resource = get_blob_resource()
    work_folder = tempfile.mkdtemp(prefix="restore_loggedpoint")
    try:
        metadata,filename = blob_resource.download(resource_id,resource_group=archive_group,filename=os.path.join(work_folder,resource_id))
        imported_table =_restore_data(filename,restore_to_origin_table=restore_to_origin_table,preserve_id=preserve_id)
        logger.debug("End to import archived loggedpoint, archive_group={},archive_id={},imported_table={}".format(archive_group,archive_id,imported_table))
    finally:
        utils.remove_folder(work_folder)
        pass

def _restore_data(filename,restore_to_origin_table=False,preserve_id=True):
    db = settings.DATABASE
    imported_table = db.import_spatial_data(filename)

    if restore_to_origin_table:
        #insert the missing device
        logger.debug("Create the missing devices from imported table({0})".format(imported_table))
        sql = missing_device_sql.format(imported_table)
        rows = db.update(sql,autocommit=True)
        if rows :
            logger.debug("Created {2} missing devices from imported table({0})".format(imported_table,rows))
        else:
            logger.debug("All devices referenced from imported table({0}) exist".format(imported_table,rows))

        logger.debug("Restore the logged points from table({0}) to table(tracking_loggedpoint)".format(imported_table))
        if preserve_id:
            sql = restore_with_id_sql
        else:
            sql = restore_sql

        sql = sql.format(imported_table)
        rows = db.update(sql,autocommit=True)
        logger.debug("{1} records are restored from from table({0}) to table(tracking_loggedpoint)".format(imported_table,rows))
        try:
            logger.debug("Try to drop the imported table({0})".format(imported_table))
            rows = db.executeDDL("DROP TABLE \"{}\"".format(imported_table))
            logger.debug("Dropped the imported table({0})".format(imported_table))
        except:
            logger.error("Failed to drop the temporary imported table to table({0}). {1}".format(imported_table,traceback.format_exc()))
            pass
        return "tracking_loggedpoint"

    else:
        return imported_table


#archive_by_date(date(2019,11,16),delete_after_archive=False,check=True)
#restore_by_date(date(2019,11,16))
            
