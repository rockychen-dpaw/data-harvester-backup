import json
from datetime import datetime

from azure.storage.blob import  BlobServiceClient
from azure.core.exceptions import (ResourceNotFoundError,)

from .storage import ResourceStorage
from . import settings
from utils import JSONEncoder,JSONDecoder

class AzureBlobMetadata(object):
    def __init__(self,resource_name,connect_string,container_name,resource_base_path=None):
        self._resource_name = resource_name
        self._resource_base_path = resource_base_path
        if self._resource_base_path:
            self._resource_metadata_file = "{}/metadata.json".format(self._resource_base_path)
        else:
            self._resource_metadata_file = "metadata.json"

        self._blob_service_client = BlobServiceClient.from_connection_string(connect_string)
        self._container_client = self._blob_service_client.get_container_client(container_name)
        self._metadata_client = self._container_client.get_blob_client(self._resource_metadata_file)

    @property
    def resourcename(self):
        return self._resource_name

    @property
    def resourcemetadata(self):
        """
        Return resource metadata.
        Return None if resource metadata is not found
        """
        try:
            blob_data = self._metadata_client.download_blob().readall()
            metadata = json.loads(blob_data.decode(),cls=JSONDecoder)
        except ResourceNotFoundError as e:
            #metadata not found
            metadata = None
                
        return metadata

    def update(self,metadata):
        metadata["publish_date"] = datetime.now(tz=settings.TZ)
        self._metadata_client.stage_block("main",json.dumps(metadata,cls=JSONEncoder).encode())
        self._metadata_client.commit_block_list(["main"])


class AzureBlobReader(ResourceStorage):
    def __init__(self,resource_name,connect_string,container_name,resource_base_path=None):
        self._resource_name = resource_name
        self._resource_base_path = resource_name if resource_base_path is None else resource_base_path
        if self._resource_base_path:
            self._resource_data_path = "{}/data".format(self._resource_base_path)
            self._resource_metadata_file = "{}/metadata.json".format(self._resource_base_path)
        else:
            self._resource_data_path = "data"
            self._resource_metadata_file = "metadata.json"

        self._blob_service_client = BlobServiceClient.from_connection_string(connect_string)
        self._container_client = self._blob_service_client.get_container_client(container_name)
        self._metadata_client = self._container_client.get_blob_client(self._resource_metadata_file)
        self._event_queue_name = "{}_{}".format(container_name,self._resource_name)

    @property
    def resourcename(self):
        return self._resource_name

    @property
    def resourcemetadata(self):
        """
        Return resource metadata.
        Return None if resource metadata is not found
        """
        if not hasattr(self,"_resourcemetadata"):
            try:
                blob_data = self._metadata_client.download_blob().readall()
                self._resourcemetadata = json.loads(blob_data.decode(),cls=JSONDecoder)
            except ResourceNotFoundError as e:
                #metadata not found
                return None

                
        return self._resourcemetadata

    @resourcemetadata.setter
    def resourcemetadata(self,value):
        self._resourcemetadata = value

    def get_resource(self,resourceid=None):
        """
        Return the resource with resourceid
        if resourceid is None, return the current resource
        """
        if not resourceid:
            metadata = self.resourcemetadata
            if not metadata:
                raise ResourceNotFoundError("{} Not Found".format(self.resourcename))
            resourceid = metadata["resourceid"]

        blob_data = self._metadata_client.download_blob().readall()

        return blob_data

class AzureBlob(AzureBlobReader):

    def __init__(self,resource_name,connect_string,container_name,resource_base_path=None,f_resourceid=None):
        super().__init__(resource_name,connect_string,container_name,resource_base_path)
        self._f_resourceid = f_resourceid or (lambda data_path:"{}/{}_{}.json".format(data_path,self._resource_name,datetime.now().strftime("%Y-%m-%d-%H-%M-%S")))

    def push_resource(self,data,metadata=None):
        """
        Push the resource to the storage
        Return the new resourcemetadata.
        """
        #get the resourceid
        resourceid = self._f_resourceid(self._resource_data_path)
        #populute the latest resource metadata
        if not metadata:
            metadata = {
            }
        metadata["publish_date"] = datetime.now(tz=settings.TZ)
        metadata["resource_id"] = resourceid

        if not self.resourcemetadata:
            self.resourcemetadata = { 
                "histories":[]
            }
        elif "histories" in self.resourcemetadata:
            self.resourcemetadata["histories"].insert(0,self.resourcemetadata["current"])
        else:
            self.resourcemetadata["histories"] = [self.resourcemetadata["current"]]

        self.resourcemetadata["current"] = metadata

        #push the resource to azure storage
        blob_client = self._container_client.get_blob_client(resourceid)
        blob_client.upload_blob(data)
        #update the resource metadata
        #self._metadata_client.upload_blob(json.dumps(self.resourcemetadata,cls=JSONEncoder).encode())
        self._metadata_client.stage_block("main",json.dumps(self.resourcemetadata,cls=JSONEncoder).encode())
        self._metadata_client.commit_block_list(["main"])


        return self.resourcemetadata

        
