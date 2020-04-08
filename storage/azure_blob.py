import json
import tempfile
import logging
import os
import shutil

from azure.storage.blob import  BlobServiceClient,BlobClient,BlobType
from azure.core.exceptions import (ResourceNotFoundError,)

from .storage import ResourceStorage
from . import settings
from utils import JSONEncoder,JSONDecoder,timezone

logger = logging.getLogger(__name__)

class AzureBlob(object):
    """
    A blob client to get/update a blob resource
    """
    def __init__(self,blob_path,connection_string,container_name):
        self._blob_client = BlobClient.from_connection_string(connection_string,container_name,blob_path,**settings.AZURE_BLOG_CLIENT_KWARGS)

    def download(self,filename=None,overwrite=False):
        """
        Return the downloaded local resource file
        """
        if filename:
            if os.path.exists(filename):
                if not os.path.isfile(filename):
                    #is a folder
                    raise Exception("The path({}) is not a file.".format(filename))
                elif not overwrite:
                    #already exist and can't overwrite
                    raise Exception("The path({}) already exists".format(filename))
        else:
            with tempfile.NamedTemporaryFile(prefix=self.resourcename) as f:
                filename = f.name

        with open(filename,'wb') as f:
            blob_data = self.get_blob_client(metadata["resource_path"]).download_blob().readinto(f)

        return filename
        

    def update(self,blob_data):
        """
        Update the blob data
        """
        if blob_data is None:
            #delete the blob resource
            self._blob_client.delete_blob(delete_snapshots=True)
        else:
            if not isinstance(blob_data,bytes):
                #blob_data is not byte array, convert it to json string
                raise Exception("Updated data must be bytes type.")
            #self._blob_client.stage_block("main",blob_data)
            #self._blob_client.commit_block_list(["main"])
            self._blob_client.upload_blob(blob_data,overwrite=True,timeout=3600)

class AzureJsonBlob(AzureBlob):
    """
    A blob client to get/update a json blob resource
    """
    @property
    def json(self):
        """
        Return resource data as dict object.
        Return None if resource is not found
        """
        try:
            data = self._blob_client.download_blob().readall()
            return json.loads(data.decode(),cls=JSONDecoder)
        except ResourceNotFoundError as e:
            #blob not found
            return None

    def update(self,blob_data):
        """
        Update the blob data
        """
        blob_data = {} if blob_data is None else blob_data
        if not isinstance(blob_data,bytes):
            #blob_data is not byte array, convert it to json string
            blob_data = json.dumps(blob_data,cls=JSONEncoder).encode()
        super().update(blob_data)

class AzureBlobResourceMetadata(AzureJsonBlob):
    """
    A client to get/create/update a blob resource's metadata
    metadata is a json object.
    """
    def __init__(self,connection_string,container_name,resource_base_path=None,cache=False,metadata_filename=None):
        metadata_filename = metadata_filename or "metadata.json"
        if resource_base_path:
            metadata_file = "{}/{}".format(resource_base_path,metadata_filename)
        else:
            metadata_file = metadata_filename
        logger.debug("container={}, metadata file={}".format(container_name,metadata_file))
        super().__init__(metadata_file,connection_string,container_name)
        self._cache = cache

    @property
    def json(self):
        """
        Return the resource's meta data as dict object.
        Return None if resource's metadata is not found
        """
        if self._cache and hasattr(self,"_json"):
            #json data is already cached
            return self._json

        json_data = super().json

        if self._cache and json_data is not None:
            #cache the json data
            self._json = json_data

        return json_data

    def update(self,metadata):
        if metadata is None:
            metadata = {}
        super().update(metadata)
        if self._cache:
            #cache the result
            self._json = metadata

class AzureBlobResourceClient(AzureBlobResourceMetadata):
    """
    A client to track the non group resource consuming status of a client
    """
    def __init__(self,connection_string,container_name,clientid,resource_base_path=None,cache=False):
        metadata_filename = ".json".format(clientid)
        if resource_base_path:
            client_base_path = "{}/clients".format(resource_base_path)
        else:
            client_base_path = "clients"
        super().__init__(metadata_file,connection_string,container_name,resource_base_path=client_base_path,metadata_filename=metadata_filename,cache=cache)
        self._metadata_client = AzureBlobResourceMetadata(connection_string,container_name,resource_base_path=resource_base_path,cache=False)


    @property
    def status(self):
        """
        Return tuple(True if the latest resource was consumed else False,(latest_resource_id,latest_resource's publish_date),(consumed_resurce_id,consumed_resource's published_date,consumed_date))
        """
        client_metadata = self.json
        resource_metadata = self._metadata_client.json
        if not client_metadata or not client_metadata.get("resource_id"):
            #this client doesn't consume the resource before
            if not resource_metadata or not resource_metadata.get("current",{}).get("resource_id"):
                #not resource was published
                return (True,None,None)
            else:
                #some resource hase been published
                return (False,(resource_metadata.get("current",{}).get("resource_id"),resource_metadata.get("current",{}).get("publish_date")),None)
        elif not resource_metadata or not resource_metadata.get("current",{}).get("resource_id"):
            #no resource was published
            return (True,None,(client_metadata.get("resource_id"),client_metadata.get("publish_date"),client_metadata.get("consume_date")))
        elif client_metadata.get("resource_id") == resource_metadata.get("current",{}).get("resource_id"):
            #the client has consumed the latest resource
            return (
                True,
                (resource_metadata.get("current",{}).get("resource_id"),resource_metadata.get("current",{}).get("publish_date")),
                (client_metadata.get("resource_id"),client_metadata.get("publish_date"),client_metadata.get("consume_date"))
            )
        else:
            return (
                False,
                (resource_metadata.get("current",{}).get("resource_id"),resource_metadata.get("current",{}).get("publish_date")),
                (client_metadata.get("resource_id"),client_metadata.get("publish_date"),client_metadata.get("consume_date"))
            )

    @property
    def isbehind(self):
        """
        Return true if consumed resurce is not the latest resource; otherwise return False
        """
        return not self.status[0]

    def consume(self,callback,isjson=True):
        """
        Return True if some resource has been consumed; otherwise return False
        """
        status = self.status
        if status[0]:
            #the latest resource has been consumed
            return False

        resource_client = AzureBlob(status[1][0],connection_string,container_name)
        if isjson:
            callback(resource_client.json)
        else:
            res_file = resource_client.download()
            try:
                with open(res_file,'rb') as f:
                    callback(f)
            finally:
                #after processing,remove the downloaded local resource file
                os.remove(res_file)
        #update the client consume data
        client_metdata = {
            "resource_id" : status[1][0],
            "publish_date" : status[1][1],
            "consume_date": timezone.now()
        }

        self.update(client_metadata)

        return True


class AzureBlobResourceBase(object):
    """
    A base client to manage a Azure Resourcet
    """
    _f_resourceid = staticmethod(lambda resource_name:"{0}_{1}.json".format(resource_name,timezone.now().strftime("%Y-%m-%d-%H-%M-%S")))
    _f_resourcepath = staticmethod(lambda data_path,resource_group,resourceid:"{0}/{1}/{2}".format(data_path,resource_group,resourceid) if resource_group else "{0}/{1}".format(data_path,resourceid))
    def __init__(self,resource_name,connection_string,container_name,resource_base_path=None,group_resource=False,archive=True,f_resourceid=None):
        self._resource_name = resource_name
        self._resource_base_path = resource_name if resource_base_path is None else resource_base_path
        if self._resource_base_path:
            self._resource_data_path = "{}/data".format(self._resource_base_path)
        else:
            self._resource_data_path = "data"
        self._connection_string = connection_string
        self._container_name = container_name
        self._metadata_client = AzureBlobResourceMetadata(connection_string,container_name,resource_base_path=self._resource_base_path,cache=True)
        self._archive = archive
        self.group_resource = group_resource
        if not f_resourceid:
            self._f_resourceid = staticmethod(f_resourceid)

    def get_blob_client(self,blob_name):
        return BlobClient.from_connection_string(self._connection_string,self._container_name,blob_name,**settings.AZURE_BLOG_CLIENT_KWARGS)

    @property
    def resourcename(self):
        return self._resource_name

    @property
    def resourcemetadata(self):
        """
        Return resource metadata.
        Return None if resource metadata is not found
        """
        return self._metadata_client.json

    def download_group(self,resource_group,folder=None,overwrite=False):
        """
        Only available for group resource
        """
        if not self.group_resource:
            raise Exception("{} is not a group resource.".format(self.resourcename))

        if folder:
            if os.path.exists(folder):
                if not os.path.isdir(folder):
                    #is a folder
                    raise Exception("The path({}) is not a folder.".format(folder))
                elif not overwrite:
                    #already exist and can't overwrite
                    raise Exception("The path({}) already exists".format(folder))
                else:
                    #remove the existing folder
                    shutil.rmtree(folder)

            #create the folder
            os.makedirs(folder)
        else:
            folder = tempfile.mkdtemp(prefix=resource_group)

        resourcemetadata = self.resourcemetadata
        if not resourcemetadata:
            raise ResourceNotFoundError("{} Not Found".format(self.resourcename))

        if not resourcemetadata.get(resource_group):
            raise ResourceNotFoundError("The resource group({}.{}) Not Found".format(self.resourcename,resource_group))

        groupmetadata = resourcemetadata[resource_group]
        for metadata in groupmetadata:
            if self._archive:
                metadata = metadata.get("current")
            if not metadata:
                continue
            if metadata.get("resource_id") and metadata.get("resource_path"):
                with open(os.path.join(folder,metadata["resource_id"]),'wb') as f:
                    self.get_blob_client(metadata["resource_path"]).download_blob().readinto(f)

        return (groupmetadata,folder)

    def download(self,resourceid=None,filename=None,overwrite=False,resource_group=None):
        """
        Download the resource with resourceid, and return the filename 
        remove the existing file or folder if overwrite is True
        """
        if filename:
            if os.path.exists(filename):
                if not os.path.isfile(filename):
                    #is a folder
                    raise Exception("The path({}) is not a file.".format(filename))
                elif not overwrite:
                    #already exist and can't overwrite
                    raise Exception("The path({}) already exists".format(filename))
        
        resourcemetadata = self.resourcemetadata
        if not resourcemetadata:
            raise ResourceNotFoundError("{} Not Found".format(self.resourcename))
        if self.group_resource:
            if resource_group:
                groupmetadata = resourcemetadata.get(resource_group)
                if not groupmetadata:
                    raise ResourceNotFoundError("The resource group({}.{}) Not Found".format(self.resourcename,resource_group))
                if resourceid:
                    try:
                        resourcemetadata = next(m for m in groupmetadata if m["resource_id"] == resourceid)
                    except:
                        raise ResourceNotFoundError("The resource({}.{}.{}) Not Found".format(self.resourcename,resource_group,resourceid))
                else:
                    resourcemetadata = groupmetadata[0]
            else:
                raise Exception("Must provide resource group to get latest resource from group resource({}) ".format(self.resourcename))
    
        if self._archive:
            if not resourcemetadata.get("current") or resourcemetadata["current"].get("resource_path"):
                raise ResourceNotFoundError("Can't find any resource in {}".format(self.resourcename))
            metadata = resourcemetadata["current"]
        else:
            metadata = resourcemetadata


        if not filename:
            with tempfile.NamedTemporaryFile(prefix=resourceid) as f:
                filename = f.name

        with open(filename,'wb') as f:
            self.get_blob_client(metadata["resource_path"]).download_blob().readinto(f)

        return (metadata,filename)


class AzureBlobResource(AzureBlobResourceBase,ResourceStorage):
    """
    A client to upload/download azure resource
    the resource can be a single AzureBlobResource or a group of AzureBlobResource
    if incremental is Ture, this azure resource is uploaded incrementally
    Each AzureResource has a corresponding metadata, the metadata has different structure
    for increnental resource:
        resouce metadata has two status: current status and history status.
    for non increnental resource:
        resouce metadata has a status list
    for grouped resource.
        group resource metadata is a dict between group name and a list of resource metata

    """

    def push_resource(self,data,metadata=None,f_post_push=None,length=None):
        """
        Push the resource to the storage
        f_post_push: a function to call after pushing resource to blob container but before pushing the metadata, has one parameter "metadata"
        Return the new resourcemetadata.
        """
        #populute the latest resource metadata
        if not metadata:
            metadata = {}
        #get the resourceid
        if self.group_resource and not metadata.get("resource_group"):
            raise Exception("Missing resource group in metadata")

        resource_group = metadata["resource_group"]
        resourceid = metadata.get("resource_id") or self._f_resourceid(self._resource_data_path,self._resource_name,resource_group)
        resourcepath = self._f_resourcepath(self._resource_data_path,resource_group,resourceid)
        if not resourceid:
            raise Exception("Missing resource_id in metadata")

        metadata["publish_date"] = timezone.now()
        metadata["resource_id"] = resourceid
        metadata["resource_path"] = resourcepath

        resourcemetadata = self.resourcemetadata
        resource_existed = False

        if resource_group:
            if not resourcemetadata:
                resourcemetadata = {}

            if resource_group in resourcemetadata:
                groupmetadata = resourcemetadata[resource_group]
            else:
                groupmetadata = []
                resourcemetadata[resource_group] = groupmetadata
            #check whether the existing resource exist or not
            index = len(groupmetadata) - 1
            while index >= 0:
                if groupmetadata[index]["resource_id"] == metadata["resource_id"]:
                    break
                else:
                    index -= 1
            if index >= 0:
                #resource already exists
                currentmetadata = groupmetadata[index]
                resource_existed = True
            else:
                currentmetadata = {}
                groupmetadata.append(currentmetadata)
        else:
            if not resourcemetadata:
                resourcemetadata = {}

            currentmetadata = resourcemetadata

        if self._archive:
            if "histories" not in currentmetadata:
                currentmetadata["histories"] = []
            if currentmetadata.get("current"):
                currentmetadata["histories"].insert(0,currentmetadata["current"])

        #push the resource to azure storage
        blob_client = self.get_blob_client(resourcepath)
        blob_client.upload_blob(data,blob_type=BlobType.BlockBlob,overwrite=True,timeout=3600,max_concurrency=5,length=length)
        #update the resource metadata
        if f_post_push:
            f_post_push(metadata)

        if self._archive:
            currentmetadata["current"] = metadata
        else:
            currentmetadata.update(metadata)

        self._metadata_client.update(resourcemetadata)

        return resourcemetadata

        
