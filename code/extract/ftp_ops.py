import os
import sys
import exceptions as excp
from ftplib import FTP
import pandas as pd
import reporting
from transversal_classes import DB_Connect
#from transversal_classes import ExecutionStatus
import pysftp

WORKING_DIRECTORY = DB_Connect().get_ftp_config()[3]

"""File that handles FTP connection,  publish and recieving of FTP data, complete filename 
includes relative folder path, user and password previously captured"""

def filePublishToFTP(localpath, filename, host, user, pwd):
  """Function that publishes a file to the FTP returns a boolean which indicates if 
  the file was correctly published"""
  isPublished = False
  cnopts = pysftp.CnOpts()
  cnopts.hostkeys = None
  with pysftp.Connection(host=host, username= user , password=pwd, cnopts=cnopts) as ftp:
    try:
      ftp.cwd(WORKING_DIRECTORY)

      local_file_size = os.stat(localpath + filename).st_size
      files = os.listdir(localpath)           #TODO delete this line

      if not(ftp.exists(filename)):
        file_attr = ftp.put(localpath + filename, filename)
        server_file_size = file_attr.st_size
        # checksum
        if local_file_size != server_file_size:
          reporting.ext_logger.error("the local size (%s) differs from server size (%s) so the file is not correctly uploaded" % 
            (local_file_size, server_file_size))
          raise
        else:
          # ¿does the file need to be erased local? if it's not then delete this line
          reporting.ext_logger.info("file uploaded correctly local(%s) server(%s)... deleting local copy" % 
            (local_file_size, server_file_size))
          os.unlink(localpath + filename)
      else:
        reporting.ext_logger.warning("El Archivo ya existe en el servidor")
    except Exception as e:
      #agregar codigo para escribir html de resumen con la variable output_folder
      reporting.ext_logger.error(str(e))
      isPublished = False
    else:
      isPublished = True
    finally:
      return isPublished


def fileRecieveFromFTP(localpath, host, user,  pwd):
  """Function that recieves files from the server """
  isRecieved = False
  cnopts = pysftp.CnOpts()
  cnopts.hostkeys = None
  exestatus = ExecutionStatus()
  with pysftp.Connection(host=host, username= user , password=pwd, cnopts=cnopts) as ftp:
    try:
      ftp.cwd(WORKING_DIRECTORY)
      files = ftp.listdir()                                     # TODO delete this line
      exestatus.qty_ftp_files = len(files)
      for file_in_server in files:
        if os.path.exists(localpath + file_in_server):
          os.unlink(localpath + file_in_server)
        ftp.get(file_in_server, localpath+file_in_server, preserve_mtime = True)
        server_file_size = ftp.stat(file_in_server).st_size 
        local_file_size = os.stat(localpath + file_in_server).st_size
        if local_file_size != server_file_size:
          reporting.ext_logger.error("The local size (%s) differs from server size (%s) so the file is not correctly downloaded" % 
            (local_file_size, server_file_size))
          raise #on error raise exception captured in this same function
        else:
          # ¿does the file need to be erased local? if it's not then delete this line
          reporting.ext_logger.info("archivo descargado correctamente, borrando versión del servidor...")
          ftp.remove(file_in_server) #borrar el archivo del servidor

    except Exception as e:
      if str(e) == "550 No file found":
        reporting.ext_logger.error("No files in this directory")
      reporting.ext_logger.error(str(e))
      isRecieved = False
    else:
      isRecieved = True
    finally:
      return isRecieved

