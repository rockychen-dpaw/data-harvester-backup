import traceback
import logging
import subprocess
import tempfile

import psycopg2

from utils import parse_db_connection_string,classproperty,gdal



logger = logging.getLogger(__name__)


class PostgreSQL(object):
    def __init__(self,db_url):
        self._params = parse_db_connection_string(db_url)
        self._connection = None
        self._cursor = None

    def __enter__(self):
        self._connection = psycopg2.connect(host=self._params["host"],port=self._params["port"],dbname=self._params["dbname"],user=self._params["user"],password=self._params["password"])
        self._cursor = self._connection.cursor()
        return self

    def __exit__ (self, type, value, tb):
        if self._cursor:
            try:
                self._cursor.close()
            except:
                logger.error(traceback.format_exc())
            finally:
                self._cursor = None
        if self._connection:
            try:
                self._connection.close()
            except:
                logger.error(traceback.format_exc())
            finally:
                self._connection = None

    def query(self,sql,columns=None):
        """
        Execute select sql and return a list of tuple or a list of dict if columns is not None
        """
        if self._cursor:
            return self._query(sql,columns=columns)
        else:
            with self as db:
                return db._query(sql,columns=columns)
                
    def _query(self,sql,columns=None):
        self._cursor.execute(sql)
        if columns:
            return [dict(zip(columns,row)) for row in self._cursor.fetchall()]
        else:
            return self._cursor.fetchall()

        
    def get(self,sql,columns=None):
        """
        Execute select sql and return a list of data or a dict if columns is not None
        """
        if self._cursor:
            return self._get(sql,columns=columns)
        else:
            with self as db:
                return db._get(sql,columns=columns)
                
    def _get(self,sql,columns=None):
        self._cursor.execute(sql)
        if columns:
            return dict(zip(columns,self._cursor.fetchone()))
        else:
            return self._cursor.fetchone()

    def update(self,sql,commit=True):
        """
        insert/update/delete data
        """
        if self._cursor:
            return self._update(sql,commit=commit)
        else:
            with self as db:
                return db._update(sql,commit=commit)
                
    def _update(self,sql,commit=True):
        self._cursor.execute(sql)
        if commit:
            self._connection.commit()
        return self._cursor.rowcount


    def executeDDL(self,sql):
        """
        execute ddl related statements
        """
        if self._cursor:
            return self._executeDDL(sql)
        else:
            with self as db:
                return db._executeDDL(sql)
                
    def _executeDDL(self,sql):
        self._cursor.execute(sql)
        self._connection.commit()

    def count(self,table):
        """
        Get the number of records in table.
        table can be a table, a view , and even a sql
        """
        count_sql = "select count(1) from ({}) as a".format(table)
        return self.get(count_sql)[0]


    def export_spatial_data(self,sql,file_name=None,file_ext=None,layer=None):
        """
        export spatial table data using gdal
        table can be a table or a view
        check: Check whether the table count is equal with the exported feature count.
        Return (layer metadata ,filename)
        """
        if not file_name and not file_ext:
            raise Exception("Please specify file_name or file_ext to export")

        if file_ext and file_ext[0] != ".":
            file_ext = "." + file_ext

        if not file_name:
            with tempfile.NamedTemporaryFile(prefix=self._params["dbname"],suffix=file_ext,delete=False) as f:
                file_name = f.name

        cmd = """ogr2ogr -overwrite -preserve_fid {0} PG:"host='{2}' {3} dbname='{4}' {5} {6}" {1} -sql "{7}" """.format(
            file_name,
            "-nln {}".format(layer) if layer else "",
            self._params["host"],
            "port={}".format(self._params["port"]) if self._params["port"] else "",
            self._params["dbname"],
            "user='{}'".format(self._params["user"]) if self._params["user"] else "",
            "password='{}'".format(self._params["password"]) if self._params["password"] else "",
            sql

        )
        logger.debug("Export the spatial data. cmd='{}'".format(cmd))
        subprocess.check_call(cmd,shell=True)

        count = self.count(sql)
        layer_metadata = gdal.get_layers(file_name)[0]
        if count == layer_metadata["features"]:
            logger.debug("Succeed to export {1} features to {0}".format(file_name,count))
        else:
            raise Exception("Failed, only {1}/{2} features were exported to {0}".format(file_name,layer_metadata["features"],count))

        return (layer_metadata,file_name)







        
