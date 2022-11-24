import pandas as pd
from exceptions import LoadingDatabaseError
from transversal_classes import DB_Connect
import os
import logging
import threading
#s3 related libraries
import boto3
from botocore.exceptions import ClientError
from boto3.s3.transfer import TransferConfig
import pdb 


class PublishOps():

  def __init__(self, path_store_view, s3_bucket_name, s3_key_name):
    self.roo_logger = logging.getLogger('publish.load_views_to_s3')
    self.db_connection = DB_Connect().get_connection()
    self.schema_name = DB_Connect().get_db_schema_name()
    self.path_store_view = path_store_view
    self.s3_bucket_name = s3_bucket_name
    self.s3_key_name = s3_key_name
    self.s3_client = boto3.client('s3')

  def create_view_file(self, view, year, month):
    try:
      self.roo_logger.info('iniciando ejecución de query creación de vista este proceso puede tardar varios minutos...')
      if view=='SCANTRACK':
        self.roo_logger.info("Ejecutando query: " + ("SELECT * FROM %s.PRICING_SHOPPER_SCANTRACK WHERE GRUPO=%s and ANIO= %s")%(self.schema_name, month, year))
        chunk = pd.read_sql(("SELECT * FROM %s.PRICING_SHOPPER_SCANTRACK WHERE GRUPO=%s and ANIO= %s")%(self.schema_name, month, year),
        con=self.db_connection, chunksize=10000)
        self.roo_logger.info('Finaliza ejecución del query creando archivo de vista')
        nombre_archivo = "VISTA_NIELSEN_SCANTRACK_" + str(year) + ('00'+ str(month))[-2:]+ ".csv"
        self.roo_logger.info('creando archivo de vista %s' % (nombre_archivo))
      else:
        self.roo_logger.info("Ejecutando query: " + ("SELECT * FROM %s.PRICING_SHOPPER_RETAIL WHERE month(PERIODO)=%s and year(PERIODO)= %s")%(self.schema_name, month,year))
        chunk = pd.read_sql(("SELECT * FROM %s.PRICING_SHOPPER_RETAIL WHERE month(PERIODO)=%s and year(PERIODO)= %s")%(self.schema_name, month,year),
        con=self.db_connection, chunksize=10000)
        self.roo_logger.info('Finaliza ejecución del query creando archivo de vista')
        nombre_archivo = "VISTA_NIELSEN_RETAIL_" + str(year) + ('00'+ str(month))[-2:]+ ".csv"
        self.roo_logger.info('creando archivo de vista %s' % (nombre_archivo))

      mode='w'
      header = True
      for data in chunk:
        data.to_csv(self.path_store_view + nombre_archivo, sep=';', index=False, mode=mode, encoding='latin', header=True)
        mode='a'
        header=False
      return (0, 'Ejecución exitosa')
    except Exception as e:
      self.roo_logger.error(str(e))
      return (-1, 'Ejecución fallida' + str(e))
  

  def publish_s3_bucket(self, filename):
    config = TransferConfig(multipart_threshold=1024*25, max_concurrency=10,
                        multipart_chunksize=1024*25, use_threads=True)
    file = self.path_store_view + filename
    key = self.s3_key_name + filename
    
    try:
      self.s3_client.upload_file(file, self.s3_bucket_name, key #,
      #ExtraArgs={ 'ACL': 'public-read', 'ContentType': 'text/csv'},
      #Config = config,
      #Callback= ProgressPercentage(file)
      )
    except ClientError as e:
      self.roo_logger.error("Error durante la publicación de archivo a S3")
      self.roo_logger.error(e)
      return False
    return True


#class inside class adapted from Boto3 documentation
class ProgressPercentage(object):
  def __init__(self, filename):
    self._filename = filename
    self._size = float(os.path.getsize(filename))
    self._seen_so_far = 0
    self._lock = threading.Lock()
    self.roo_logger = logging.getLogger('publish.publish_s3_method') 

  def __call__(self, bytes_amount):
    # To simplify we'll assume this is hooked up
    # to a single filename.
    with self._lock:
      self._seen_so_far += bytes_amount
      percentage = (self._seen_so_far / self._size) * 100
      self.roo_logger.info("%s  %s / %s  (%.2f%%)" % ( self._filename, self._seen_so_far, self._size, percentage))