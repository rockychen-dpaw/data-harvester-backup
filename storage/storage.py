import json
import tempfile
import os

from utils import JSONEncoder,JSONDecoder

class ResourceStorage(object):
    """
    A interface to list/upload/get a resource from storage container
    """

    @property
    def resourcename(self):
        """
        the resource name.
        """
        raise NotImplementedError("Property 'resourcename' is not implemented.")

    @property
    def resource(self):
        """
        Return (the current resourceid,the current resource)
        Return (The current resurceid,None) if resource with the current resourceid is not found
        Return (None,None) if can't find the current resourceid
        """
        return self.get_resource()

    @property
    def json(self):
        """
        Return (the current resourceid,the current resource as dist object)
        Return (The current resurceid,None) if resource with the current resourceid is not found
        Return (None,None) if can't find the current resourceid
        """
        return self.get_json()


    @property
    def resourcemetadata(self):
        """
        Return resource metadata.
        including 
            current resourse metadata
            resource history list
        """
        raise NotImplementedError("Property 'resourcemetadata' is not implemented.")

    def get_resource(self,resourceid=None):
        """
        Return (resourceid,resource)
        if resourceid is None, return (the current resourceid, the current resource)
        Return (resourceid,None) if resource with resourceid is not found
        Return (None,None) if can't find the current resourceid
        """
        raise NotImplementedError("Method 'get_resource' is not implemented.")

    def get_json(self,resourceid=None):
        """
        Return (resourceid,resource as dict object)
        if resourceid is None, return (the current resourceid, the current resource dict object)
        Return (resourceid,None) if resource with resourceid is not found
        Return (None,None) if can't find the current resourceid
        """
        resourceid,data = self.get_resource(resourceid)
        if data is None:
            return (resourceid,None)
        else:
            return (resourceid,json.loads(data.decode(),cls=JSONDecoder))

    def download(self,resourceid,filename=None):
        """
        Download the resource 
        Return (resourceid,the filename)
        Return (resourceid,None) if resource with resourceid is not found
        Return (None,None) if can;t find the resourceid
        """
        resourceid,data = self.get_resource(resourceid)
        if data is None:
            return (resourceid,None)
        else:
            if not filename:
                filename = os.path.join(tempfile.gettempdir(),resourceid)
            filedir = os.path.split(filename)[0]
            if filedir and not os.path.exists(filedir):
                os.makedirs(filedir)

            with open(filename,'wb') as f:
                f.write(data)

            return (resourceid,filename)

    def push_resource(self,resource,metadata=None):
        """
        Push the resource to the storage
        Return the new resourcemetadata.
        """
        raise NotImplementedError("Method 'push_resource' is not implemented.")

        
    def push_json(self,obj,metadata=None):
        """
        Push the resource to the storage
        Return the new resourcemetadata.
        """
        return self.push_resource(json.dumps(obj,cls=JSONEncoder).encode(),metadata=metadata)

    def push_file(self,filename,metdata=None):
        """
        Push the resource from file to the storage
        Return the new resourcemetadata.
        """
        with open(filename,'rb') as f:
            return self.push_resource(f,metadata=metadata)
