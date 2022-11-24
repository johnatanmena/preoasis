import re
import os
import gc #garbage collection 
import json
import math
import shutil
import pkgutil
import exceptions as exc
import pandas as pd
import git
import unidecode as ud
import enrichment.additional_variables as enrich
import extract.ftp_ops as ftp_ops
import extract.input_validations as iv
import extract.read_and_validate_file as extract #FileOpsReadAndValidations
import transform.transform_operations as transform
import transform.transform_controller as tcontroller
import load.load_operation as load
import load.load_param_files_to_db  as loadstats
import reports.report_operation as report
import publish.publish_ops as publish
import mysql.connector
import reporting
import logging
import getpass
from sqlalchemy import create_engine
from datetime import datetime
import util_constant  #strings for mail message
from transversal_classes import ProjectParameters
from transversal_classes import DB_Connect
from transversal_classes import ExecutionStatus
from transversal_classes import MailClass

# TODO separate this function in multiple functions
def read_and_publish_oasis_inputs_and_catalogs():
  params = ProjectParameters()
  input_files = os.listdir(params.CATALOG_FILES)
  files_input  = os.listdir(params.INPUT_FILES)
  catalog_files = []
  for infile in input_files:
    if infile.startswith("OASIS_MST"):
      catalog_files.append(infile)

  roo_logger = logging.getLogger('root.read_publish_nielsen_data')
  roo_logger.info('Borrando archivos de catalogos temporales')
  #before the process delete files on the temp catalog files path
  for tmp_file in os.listdir(params.TEMP_CATALOG_FILES):
    os.unlink(os.path.join(params.TEMP_CATALOG_FILES,tmp_file))
  ite=0
  printProgressBar(ite, len(files_input), "Catalogando:", "Completado", length=60)
  exestatus = ExecutionStatus()
  exestatus.start_process_hour = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
  #implement the continue and status
  for namefile in files_input:
    exestatus.chunk_counter = 0 #change chunk counter status from file
    exestatus.process_filename = namefile
    roo_logger.info('Procesando archivo %s' % namefile)
    chunk = pd.read_csv(params.INPUT_FILES + namefile, sep=';', encoding='latin', chunksize = params.chunksize)
    bitheader = 1
    for data in chunk:
      process_obj = extract.FileOpsReadAndValidations(data, params.INPUT_COLUMNS, params.ADDITIONAL_DCHK_COLUMNS)
      process_obj.process_chunk(params.INPUT_STRING_COLUMNS,exestatus,  pd)
      bitheader = process_obj.cross_catalogs(catalog_files, bitheader, pd, params)
      roo_logger.info('procesando lote numero %s ' % str(exestatus.chunk_counter))
      exestatus.chunk_counter = exestatus.chunk_counter + 1 
      #print(exestatus.chunk_counter)
    #before opening the next file call garbage collection
    ite = ite +1
    printProgressBar(ite, len(files_input), "Catalogando:", "Completado", length=60)
    gc.collect()
  
  temp_catalog_files = os.listdir(params.TEMP_CATALOG_FILES)
  extract.simplify_catalogs(temp_catalog_files, pd, params)
  qty_tmp_cat = len(temp_catalog_files)
  if qty_tmp_cat != 0:
    ans = input("Existen cambios en %d catálogo(s), por favor verificar el(los) archivo(s):\n%s\n¿desea Publicarlos?S/N\n>" % (qty_tmp_cat, str(temp_catalog_files)))
    if ans.upper() == 'S':
      extract.publish_catalogs(params.CATALOG_FILES, params.TEMP_CATALOG_FILES , pd, git, "Nielsen") #change the user
      roo_logger.info("catalogos publicados correctamente")
    else:
      roo_logger.error("Revisar inconsistencias y ejecutar de nuevo")
      print("Revisar inconsistencias y ejecutar de nuevo")
      return
  ans = input("No existen diferencias con los catalogos, ¿Desea publicar hasta el servidor FTP? S/N\n>")
  if ans.upper() == 'S':
    #publicar archivos al FTP capturar datos de acceso al FTP
    mail_obj = MailClass(DB_Connect().get_mail_config()) #objeto de comunicación de correo
    extract.publish_catalogs(params.CATALOG_FILES, params.TEMP_CATALOG_FILES , pd, git, "Nielsen") #change the user
    ftp_data = DB_Connect()
    ftp_data = ftp_data.get_ftp_config()
    roo_logger.info("Publicando archivos procesados a servidor ftp...")
    host = ftp_data[2]
    user = ftp_data[0]
    password = ftp_data[1]
    ite=0
    printProgressBar(ite, len(files_input), "Publicando:", "Completado", length=60)
    for namefile in files_input:
      ispublished = ftp_ops.filePublishToFTP(params.INPUT_FILES, namefile, host, user, password)
      if ispublished:
        roo_logger.info("Archivo %s publicado de manera correcta, se eliminará la versión local" %  namefile)
        exestatus.qty_process_files = exestatus.qty_process_files + 1 
      else:
        roo_logger.error("Error subiendo el archivo %s al servidor FTP" % namefile)
      ite=ite+1
      printProgressBar(ite, len(files_input), "Publicando:", "Completado", length=60)
    exestatus.end_process_hour = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    exestatus.calculate_time_difference_process()
    try:
      mail_obj.send_mail("Publicacion archivos Nielsen", util_constant.get_mail_nls_ok_message(exestatus),
         "INFO", params.LOG_FILES)
    except:
      roo_logger.error("Error enviando correo de confirmación de publicación al FTP")
  else:
    roo_logger.warning("Los archivos no serán publicados al servidor")
  roo_logger.info("El proceso ha finalizado")
  print("El proceso ha finalizado")
  #done!
  
def track_processed_files(processed_path, filename, mode='a'):
  """función que permite llevar trazabilidad de los archivos procesados en la herramienta
  guardando estos nombres en un archivo de texto, con la información delos archivos procesados en toda
  la historia de la herramienta"""
  if os.path.exists(processed_path + 'processed_files.txt' ):
    with open(processed_path + 'processed_files.txt', mode) as f:
      f.write(filename+'\n')
  else:
    logger = logging.getLogger('root.track_processed_files')
    logger.warning('no existe archivo de archivos procesados, se crea uno nuevo ¿es la primera vez que se ejecuta OASIS?')
    with open(processed_path + 'processed_files.txt', 'w') as f:
      f.write(filename+'\n')

#def recieve_files_from_ftp():
  #does not recieve path because it must write to the stage path
  #ftp_data = DB_Connect()
  #ftp_data = ftp_data.get_ftp_config()
  #host = ftp_data[2]
  #user = ftp_data[0]
  #password = ftp_data[1]
  #params = ProjectParameters()
  #relativepath  = params.INPUT_FILES
  #isready = ftp_ops.fileRecieveFromFTP(relativepath, host, user, password)
  #if not(isready):
    #print("ocurrió un error durante la descarga de los archivos... reintentando")
    #isready = ftp_ops.fileRecieveFromFTP(relativepath, host, user, password)
    #if not(isready):
      #print("ocurrió un error durante la descarga de archivos... continuando con los archivos locales")
  #else:
    #print("archivos descargados correctamente")


def recieve_input_files(data_chunk, exestatus, params, namefile, pd):
  roo_logger = logging.getLogger('root.recieve_input_files')
  roo_logger.info('realizando lectura de catálogos disponibles')
  FMT = "%Y-%m-%d %H:%M:%S"
  start_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
  files_in_cat_folder = os.listdir(params.CATALOG_FILES)
  input_files  = os.listdir(params.INPUT_FILES)
  catalog_files = []
  for infile in files_in_cat_folder:
    if infile.startswith("OASIS_MST"):
      catalog_files.append(infile)
  
  process_obj = extract.FileOpsReadAndValidations(data_chunk, params.INPUT_COLUMNS, params.ADDITIONAL_DCHK_COLUMNS)
  roo_logger.info('Estructura de archivo correcta')
  process_obj.process_chunk(params.INPUT_STRING_COLUMNS,exestatus, pd)
  process_obj.assign_gn_value(params.CATALOG_FILES, catalog_files, params.TEMP_INPUT_FILES,
    namefile, exestatus, pd)
  roo_logger.info('Asignación de valores de grupo finalizada')
  end_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
  exestatus.textra = exestatus.textra + ((datetime.strptime(end_time, FMT) - datetime.strptime(start_time, FMT)).seconds / 60)
  if (process_obj.filechunk.shape[0] > params.chunksize): #
    raise exc.InvalidCatalogException("catalogo con valores duplicados")# una vez escrito codigo para validar integridad de catalogos eliminar esta validacion
  return process_obj.filechunk


def recieve_catalogs():
  params = ProjectParameters()
  roo_logger = logging.getLogger('root.recieve_catalogs')
  catalogfiles = os.listdir(params.CATALOG_FILES)
  files_input  = os.listdir(params.INPUT_FILES)
  input_files = []
  try:
    catalog_repo = git.Repo(params.CATALOG_FILES)
    origin = catalog_repo.remote(name="origin")
    origin.pull()
  except Exception as e:
    roo_logger.warning("Error recepcion de catalogos, trabajando con versión local")
  ini_config= DB_Connect()
  roo_logger.info('recibiendo catalogos')
  for infile in catalogfiles:
    if infile.startswith("OASIS_MST"):
      catalog_obj = extract.CatalogsOps(infile, params.CATALOG_FILES, pd)
      try:
        if not(catalog_obj.validate_catalog()):
          roo_logger.warning("WARNING: el catálogo contiene valores nuevos se asignan valores por defecto: " + infile)
          ans = ini_config.get_auto_assign_catalog_mode()
          catalog_obj.process_catalog(ans.upper())
      except exc.InvalidCatalogException as e:
        roo_logger.error(e.message)
        raise e
  #not at the moment just checking it works
  try:
    extract.publish_catalogs(params.CATALOG_FILES, params.TEMP_CATALOG_FILES, pd,  git, "Nutresa")
  except Exception as e:
    roo_logger.error("Error de publicación de catalogos, no se pudo sincronizar con Gitlab")


def assign_master_ids(data_chunk, params, exestatus, db_connection):
  roo_logger = logging.getLogger('root.assign_master_ids')
  filename = exestatus.process_filename
  FMT = "%Y-%m-%d %H:%M:%S"
  start_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
  source = filename.split('_')[1]
  transform_obj = transform.TransformOps(data_chunk, exestatus, db_connection,params, pd)
  roo_logger.info('Asignando Id\'s de base de datos a la data' )
  transform_obj.add_auxiliar_variables(source,  filename, "ETL")
  transform_obj.calculate_range_product_desc()
  roo_logger.info('Finalizada transformación de datos bloque preparado para carga en base de datos' )
  end_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
  exestatus.ttrans = exestatus.ttrans + ((datetime.strptime(end_time, FMT) - datetime.strptime(start_time, FMT)).seconds / 60)
  return transform_obj.chunk_df    

def load_operation(data_chunk, params, exestatus, db_connection, engine):
  roo_logger = logging.getLogger('root.load_operation_function')
  FMT = "%Y-%m-%d %H:%M:%S"
  start_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
  load_obj = load.LoadOps(data_chunk, db_connection, pd, exestatus)
  if exestatus.get_status()['process_filename'].split("_")[1] != "DATACHECK":
    load_obj.load_product_chunk_to_db()
    roo_logger.info('finaliza la carga o actualización de insumos los productos')
  else:
    load_obj.datacheck_product_load_to_db()
    exestatus.qty_row_deleted_on_db =  load_obj.load_to_trx_to_tmp_table(engine)
  load_obj.load_transactions_chunk_to_db(engine)
  roo_logger.info('finaliza la carga de transacciones')
  end_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
  exestatus.tload = exestatus.tload + ((datetime.strptime(end_time, FMT) - datetime.strptime(start_time, FMT)).seconds / 60)

  return load_obj

def delete_file_from_transactional_data(filename, params, exestatus, conn):
  roo_logger = logging.getLogger('root.reprocess_operation')
  load_obj = load.DeleteOps(filename, conn, exestatus, params)
  load_obj.delete_from_transaction()
  roo_logger.info("Borrados registros de la base de datos, iniciando borrado de archivos físicos procesados")
  if filename in os.listdir(params.PROCESSED_FILES):
    os.unlink(params.PROCESSED_FILES + filename)
    roo_logger.info("Borrado archivo local de la carpeta de procesados")
  if check_processed_file(filename, params):
    # debo borrar el nombre de archivo del archivo de procesados
    with open(params.PROCESSED_FILES + 'processed_files.txt', 'r') as f:
      processed_files = [x.strip() for x in f.readlines()]
    processed_files.remove(filename)
    #despues de eliminado el elemento debo eliminarlo del archivo de procesados
    with open(params.PROCESSED_FILES + 'processed_files.txt', 'w') as f:
      for x in processed_files:
        f.write(x+'\n')

    
  roo_logger.info("Proceso completado de manera correcta...")

def create_report(params):
  roo_logger = logging.getLogger('root.create_report')
  try:
    roo_logger.info('generando reporte de ejecución ....')
    report_obj = report.ReportStats(params.STATS_FOLDER, params.LOG_FILES)
    report_obj.execution_report(datetime.now().strftime("%Y-%m-%d"))
    roo_logger.info('reporte finalizado ...')
  except Exception as e:
    roo_logger.error('Ocurrio un error durante la creación del reporte de ejecución, favor contactar un usuario administrador')
    roo_logger.error(str(e))

def check_processed_file(filename, params):
  """Cambiar lógica de validación de archivos procesados de acuerdo al archivo de procesados"""
  logger = logging.getLogger('root.check_processed_file')
  try:
    with open(params.PROCESSED_FILES + 'processed_files.txt', 'r') as f:
      processed_files = [x.strip() for x in f.readlines()]
    return True if filename in processed_files else False
  except Exception as e:
    logger.error(e)
    return False
  #print(os.listdir(params.PROCESSED_FILES))

def resume_execution():
  """function which resumes the execution of previously descarted chunks on files """
  roo_logger = logging.getLogger('root.resume_execution')
  params = ProjectParameters()
  exestatus = ExecutionStatus()
  exestatus.qty_process_files = -1
  exestatus.set_lot_id(params.STATS_FOLDER)
  exestatus.execution_mode = "Reanudar"
  mail_obj = MailClass(DB_Connect().get_mail_config())
  ini_config= DB_Connect()
  conn = ini_config.get_connection()
  engine = ini_config.get_engine()
  filewitherrors = False
  lineswitherrors = []
  with open(params.LOG_FILES + 'status.csv', 'r') as error_file:
    error_file.readline(); #skip first line
    lines = error_file.readlines()
    printProgressBar(0, len(lines), "Progreso:", "Completado", length=60)
    ite=0
    roo_logger.info('Procesando en modo procesar novedades %s líneas del archivo' % len(lines))
    exestatus.qty_process_files = len(lines) 
    for line in lines:
      filename = line.split(';')[0]
      chunk_number = int(line.split(';')[1])
      chunksize = line.split(';')[4]
      exestatus.process_filename = filename
      exestatus.start_hour_file  = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
      counter = 0
      roo_logger.info('Iniciando procesamiento del bloque %s del archivo %s con un tamaño de bloque de %s' % (chunk_number,filename, chunksize))
      chunk = pd.read_csv(params.INPUT_FILES + filename, sep=';', encoding='latin', chunksize = int(chunksize),
      dtype={'BARCODE':str, 'TAG':str})
      data = -1
      for datachunk  in chunk :
        if counter == chunk_number:
          data = datachunk
          break #iterar hasta encontrar el chunk
        counter = counter + 1
      if not(isinstance(data, pd.DataFrame)):
        #TODO write warning logger chunk not found in file
        roo_logger.warning('El bloque no fue encontrado en el archivo verificar que los tamaños de bloques' +
         'sean los mismos que cuando se genero el archivo de status')
        roo_logger.warning('Pasando al siguiente bloque...')
        break
      try:
        data_to = recieve_input_files(data, exestatus, params, filename, pd)
        data_to = assign_master_ids(data_to, params, exestatus, conn)
        #TODO - write enrichment code
        status = load_operation(data_to, params, exestatus, conn, engine)
        roo_logger.info('bloque de novedades procesado de manera correcta, pasando al siguiente bloque')
      except (exc.ExtractException, exc.DuplicateTagsException, exc.AdditionalColumnsException,
        exc.LoadingDatabaseError) as e:
        exestatus.error_chunks_counter = exestatus.error_chunks_counter + 1
        filewitherrors = True
        lineswitherrors = lineswitherrors + [line]
        roo_logger.error(e)
        #TODO write code error handling send mails etc
        continue
      except exc.FileStructureException as e:
        exestatus.error_chunks_counter = math.ceil(exestatus.file_qty_rows/chunksize)
        roo_logger.error(e)
        filewitherrors = True
        lineswitherrors = lineswitherrors + [line]
        break  
      except Exception as e:
        #TODO -handle exception with LOG class
        mail_obj.send_mail("Problema durante resumen", util_constant.get_mail_fail_resume_message(exestatus, str(e)),
         "ERROR", params.LOG_FILES)
        conn.close()
        raise e
      finally:
        ite = ite + 1
        exestatus.qty_archivos_procesados = ite
        printProgressBar(ite, len(lines), "Progreso:", "Completado", length=60)
        


    roo_logger.info('Finalizado procesamiento del archivo de estado verificando errores de ejecución...')
    exestatus.end_hour_file   = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    exestatus.calculate_time_difference_file()
    exestatus.write_file_stats(params.STATS_FOLDER)
  if filewitherrors:
    roo_logger.info('Se encontraron errores en el procesamiento de algunos bloques, generando archivo de estado actualizado')
    with open(params.LOG_FILES + 'status.csv', 'w') as error_file:
      #add title to status file
      title = "archivo;particion;seccion;mensaje;tamano_particion\n"
      error_file.write(title)
      for line in lineswitherrors:
        error_file.write(line)
  else:
    roo_logger.info('Archivo de status procesado de manera correcta, moviendo archivos a carpeta de procesados')
    with open(params.LOG_FILES + 'status.csv', 'r') as error_file:
      error_file.readline(); #skip first line
      lines = error_file.readlines()
      for line in lines:
        filename = line.split(';')[0]
        if check_processed_file(filename, params):
          os.unlink(os.path.join(params.PROCESSED_FILES, filename))
        shutil.move(params.INPUT_FILES + filename, params.PROCESSED_FILES + filename)
        track_processed_files(params.PROCESSED_FILES, filename)
    roo_logger.info('Archivo de status procesado de manera correcta, eliminando versión local')
    os.unlink(os.path.join(params.LOG_FILES, 'status.csv'))
  conn.close()
  return (0, "Ejecución Finalizada")

#@profile(precision=4) #debug memory usage with memory_profiler
def main(params, mail_obj):
  """Proceso de ejecución de carga de archivos Nielsen va desde el listado de archivos de la carpeta de insumos
  hasta el almacenamiento en la base de datos esquema OASIS"""
  roo_logger = logging.getLogger('root.main_load_to_db')
  reporting.roo_logger.info('Crear el estado inicial de ejecución')  
  input_files  = os.listdir(params.INPUT_FILES)
  reporting.roo_logger.info('archivos ' + str(input_files))
  exestatus = ExecutionStatus()
  exestatus.set_chunksize(params.chunksize)
  exestatus.set_lot_id(params.STATS_FOLDER)
  exestatus.qty_process_files = len(input_files)
  ini_config= DB_Connect()
  conn = ini_config.get_connection()
  engine = ini_config.get_engine()
  reporting.roo_logger.info('Objeto de conexión con la base de datos finalizado')
  printProgressBar(0, exestatus.qty_process_files, "Progreso:", "Completado", length=60)
  counter = 0
  for namefile in input_files:
    if check_processed_file(namefile, params):
      roo_logger.warning('Archivo %s ya procesado, se excluye del proceso' % namefile)
      continue
    exestatus.chunk_counter = 0 #change chunk counter status from file
    exestatus.process_filename = namefile
    exestatus.start_hour_file  = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    exestatus.file_qty_rows = exestatus.count_row_on_file(params.INPUT_FILES)
    exestatus.file_size = os.stat(params.INPUT_FILES + namefile).st_size/(1024*1024) #size in megabytes
    filewitherrors = False
    filediscard = False
    roo_logger.info('Procesando el archivo %s' % namefile)
    chunk = pd.read_csv(params.INPUT_FILES + namefile, sep=';', encoding='latin', chunksize = params.chunksize,
      dtype={'BARCODE':str, 'TAG': str})
    #starts TransferObject Logic
    for data in chunk:
      try:
        roo_logger.info('Preparando el bloque %s, para cruzar con catalogos y asignar valor de grupo' % str(exestatus.chunk_counter))
        data_to = recieve_input_files(data, exestatus, params, namefile, pd)
        roo_logger.info('Asignación valor de grupo finalizada, asignando id\'s de base de datos')
        #TODO - write code to save status
        data_to = assign_master_ids(data_to, params, exestatus, conn)
        roo_logger.info('Id\'s asignados de manera correcta, preparar carga de lote a la base de datos')
        #TODO - write enrichment code
        status = load_operation(data_to, params, exestatus, conn, engine)
        roo_logger.info('Finalizada ejecución bloque %s ' % str(exestatus.chunk_counter))
        #borrar variables no usadas
        del status
        del data_to
      except (exc.DuplicateTagsException, exc.AdditionalColumnsException,
        exc.LoadingDatabaseError, exc.DeleteDatabaseError, exc.InvalidCatalogException) as e:
        exestatus.error_chunks_counter = exestatus.error_chunks_counter + 1
        roo_logger.error(e)
        filewitherrors = True
        #TODO write code error handling send mails etc
        continue
      except exc.FileStructureException as e:
        exestatus.error_chunks_counter = math.ceil(exestatus.file_qty_rows/params.chunksize)
        roo_logger.error(e)
        filediscard = True
        break
      except exc.ExtractException as e:
        e.export_dataframe(params.TEMP_CATALOG_FILES)
        roo_logger.error('Error con el formato o validación del archivo')
        filewitherrors = True
        exestatus.error_chunks_counter = exestatus.error_chunks_counter + 1
      except Exception as e:
        roo_logger.critical(e)
        filewitherrors = True
        filediscard = True
        #send email
        status = exestatus.get_status()
        exestatus.qty_discard_files = exestatus.qty_discard_files + 1
        exestatus.end_process_hour  = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        mail_obj.send_mail("Problema de Ejecución", util_constant.get_mail_fail_message(exestatus), "ERROR", params.LOG_FILES)
        exestatus.execution_status = 0
        raise exc.Status(status['process_filename'],  status['chunk_counter'], str(e))
      finally:
        exestatus.chunk_counter = exestatus.chunk_counter+1
    #before opening the next file call garbage collection
    exestatus.end_hour_file   = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    exestatus.calculate_time_difference_file()
    exestatus.write_file_stats(params.STATS_FOLDER)
    exestatus.tload = 0
    exestatus.ttrans= 0
    exestatus.textra= 0
    exestatus.qty_prod_updated_on_db = 0
    if not(filewitherrors) and not(filediscard):
      shutil.move(params.INPUT_FILES + namefile, params.PROCESSED_FILES + namefile)
      track_processed_files(params.PROCESSED_FILES, namefile) #trazability of processed files
    elif filediscard:
      exestatus.qty_discard_files = exestatus.qty_discard_files + 1
    else:
      exestatus.qty_error_files = exestatus.qty_error_files + 1
    counter = counter+1
    exestatus.qty_archivos_procesados = counter
    printProgressBar(counter, exestatus.qty_process_files, "Progreso:", "Completado", length=60)
    gc.collect()

def reprocess_load(params, list_files):
  """Función que se encarga de eliminar los datos transaccionales de los archivos procesados para que puedan ser cargados como nuevos
  se almacena la ejecución en el archivo de estadísticas de ejecución """
  roo_logger = logging.getLogger('root.reprocess_operation')
  reporting.roo_logger.info('Preparando modo de ejecución reproceso')  
  exestatus = ExecutionStatus()
  exestatus.set_chunksize(params.chunksize)
  exestatus.qty_process_files = len(list_files)
  exestatus.execution_mode = "Reprocesar"
  ini_config= DB_Connect()
  mail_obj = MailClass(DB_Connect().get_mail_config())
  conn = ini_config.get_connection()
  engine = ini_config.get_engine()
  for filename in list_files:
    try:
      #eliminar de la base de dato
      exestatus.process_filename = filename
      delete_file_from_transactional_data(filename, params, exestatus, conn)

    except (Exception, exc.DeleteDatabaseError) as e:
      roo_logger.critical(str(e))
      status = exestatus.get_status()
      exestatus.end_process_hour  = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
      mail_obj.send_mail("Error Reproceso", util_constant.get_mail_fail_reprocess_message(exestatus, e)
        , "ERROR", params.LOG_FILES)
      raise Status(status['process_filename'],  status['chunk_counter'], message)
  reporting.roo_logger.info('Proceso de eliminado de la base de datos completado... preparando proceso de carga normal')
  main(params,mail_obj)

#TODO replace de parameters ''  without a default value '' is just for testing
def update_catalog(infile, path_to_catalog, old_val='', new_val='', user="Nutresa"):
  """Funcion que permite actualizar valores de catálogos en caso que se necesite cambiar algunos de los valores
  previamente almacenados en el repositorio  en caso que se desee modificar un valor de grupo esto afecta a la
  base de datos tambien """
  __REGULAR_EXPRESION = r'(?:\s+)?[\[\]&!¡¿?+\\\.()%#"°|;,\-\_\']'
  exestatus = ExecutionStatus()
  exestatus.process_filename = infile
  exestatus.start_hour_file = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
  exestatus.execution_mode_cat = 'Actualizar'
  exestatus.usr = user
  params = ProjectParameters() 
  old_val = ud.unidecode(re.sub(r'\s\s+' ,' ', re.sub(__REGULAR_EXPRESION, ' ', old_val.upper()))).strip()
  new_val = ud.unidecode(re.sub(r'\s\s+' ,' ', re.sub(__REGULAR_EXPRESION, ' ', new_val.upper()))).strip()
  if not old_val or not new_val: #verificar que los strings no pueden ser vacíos
    print("los valores no pueden ser nulos")
    return (-1, "los valores no pueden ser nulos")
  else:
    catalog_obj = extract.CatalogsOps(infile, path_to_catalog, pd)
    exestatus.qty_cat_upd_file = catalog_obj.update_catalog_file(old_val, new_val, git, user)
    if user.upper() == "NUTRESA":
      table = params.CATALOG_DB_MAPPING[catalog_obj.catalog_name] #obtener tabla para realizar update
      column_name = catalog_obj.column_name
      db_col_name = params.CATALOG_COL_DB_MAPPING[column_name]
      ini_config= DB_Connect()
      engine = ini_config.get_engine()
      db_utils = load.DB_Utils(exestatus, engine)
      exestatus.qty_cat_upd_db = db_utils.update_mst_table(table, db_col_name, old_val, new_val)
      if table not in ['MERCADO', 'NIVEL']:
        exestatus.qty_cat_upd_db = exestatus.qty_cat_upd_db + db_utils.debug_mst_table(table) #clean db after process
      elif table == 'MERCADO':
        exestatus.qty_cat_upd_db = exestatus.qty_cat_upd_db + db_utils.debug_market_table(table) #clean db after process
      elif table == 'NIVEL':
        exestatus.qty_cat_upd_db = exestatus.qty_cat_upd_db + db_utils.debug_level_table(table) #clean db after process
      exestatus.qty_cat_del_db = db_utils.exestatus.qty_cat_del_db
  exestatus.val_act = old_val
  exestatus.val_bef = new_val
  exestatus.responsable= DB_Connect().get_execution_mode()
  exestatus.end_hour_file = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
  exestatus.calculate_time_difference_file()
  exestatus.write_catalog_stats(params.STATS_FOLDER)
  return (0, "Ejecución exitosa")


def delete_data_from_catalog(infile, path_to_catalog, old_val, user="Nielsen"):
  """Permite eliminar data de un catálogo ya publicado con algunas condiciones claves:
  - Solo se puede seleccionar valor a eliminar de la columna Nielsen
  - Solo se pueden eliminar registros si el valor Nutresa asociado es nulo O se encuentre repetido en el catalogo"""
  #old_val = input('Escriba el valor a eliminar >')
  __REGULAR_EXPRESION = r'(?:\s+)?[\[\]&!¡¿?+\\\.()%#"°|;,\-\_\']'
  exestatus = ExecutionStatus()
  exestatus.process_filename = infile
  exestatus.start_hour_file = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
  exestatus.execution_mode_cat = 'Borrar Valor'
  exestatus.usr = user
  params = ProjectParameters() 
  old_val = ud.unidecode(re.sub(r'\s\s+' ,' ', re.sub(__REGULAR_EXPRESION, ' ', old_val.upper()))).strip()
  if not old_val: #verificar que los strings no pueden ser vacíos
    print("los valores no pueden ser nulos")
    return (-1, "los valores no pueden ser nulos")
  else:
    catalog_obj = extract.CatalogsOps(infile, path_to_catalog, pd)
    exestatus.qty_cat_del_file = catalog_obj.delete_record_catalog_file(old_val, git, user)
  exestatus.end_hour_file = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
  exestatus.calculate_time_difference_file()
  exestatus.responsable= DB_Connect().get_execution_mode()
  exestatus.val_act = old_val
  exestatus.write_catalog_stats(params.STATS_FOLDER)
  return (0, "Ejecución Finalizada")


def prepare_workspace(path, path_to_catalog, path_to_stats):
  if(os.path.exists(os.path.join(path, 'status.csv'))):
    os.unlink(os.path.join(path, 'status.csv'))#######TODO think more cases
  if (os.path.exists(os.path.join(path_to_stats, 'resumen_validaciones_por_procesos.csv'))):
    os.unlink(os.path.join(path_to_stats, 'resumen_validaciones_por_procesos.csv'))
  if (os.path.exists(os.path.join(path_to_stats, 'estadisticas_proceso_ejecución.csv'))):
    os.unlink(os.path.join(path_to_stats, 'estadisticas_proceso_ejecución.csv'))
  if (os.path.exists(os.path.join(path_to_stats, 'estadisticas_catalogos.csv'))):
    os.unlink(os.path.join(path_to_stats, 'estadisticas_catalogos.csv'))
  if (os.path.exists(os.path.join(path_to_stats, 'estadisticas_archivo_ejecución.csv'))):
    os.unlink(os.path.join(path_to_stats, 'estadisticas_archivo_ejecución.csv'))
  
  #delete reporte_ejecución
  files_on_dir = os.listdir(path)
  for fileondir in files_on_dir:
    if fileondir[-4:] == "html":
      os.unlink(os.path.join(path, fileondir))
  #delete error catalogs
  files_on_dir = os.listdir(path_to_catalog)
  for fileondir in files_on_dir:
    if fileondir[:3] == "ERR":
      os.unlink(os.path.join(path_to_catalog, fileondir))


def refresh_item_volumen_info(item_volumen_name, path_to_item_volumen):
  exestatus = ExecutionStatus()
  roo_logger = logging.getLogger('root.load_item_volumen')
  roo_logger.info("Comienza ejecución de carga de item volumen a base de datos")
  try:
    ini_config= DB_Connect()
    engine = ini_config.get_engine()
    enrich_obj = enrich.EnrichmentOps()
    df = enrich_obj.obtain_additional_var_from_item_volume(path_to_item_volumen, item_volumen_name)
    roo_logger.info("Termina extracción de tamano y unidades por empaque")
    db_utils = load.DB_Utils(exestatus, engine)
    db_utils.load_item_volumen(df)
    roo_logger.info("Termina ejecución de carga a base de datos")
    return (0, "Ejecución exitosa")
  except exc.IVExtractException as iv_error:
    roo_logger.error("Ocurrio un error durante la carga del Item Volumen")
    roo_logger.error(iv_error)
    return (-1, "Ocurrio un error durante la carga del Item Volumen")
  except Exception as e:
    roo_logger.error(str(e))
    return (-1, str(e))

def refresh_dict_info(dict_name, path_to_dict):
  exestatus = ExecutionStatus()
  roo_logger = logging.getLogger('root.load_dict_info')
  roo_logger.info("Comienza ejecución de carga de item volumen a base de datos")
  try:
    ini_config= DB_Connect()
    engine = ini_config.get_engine()
    enrich_obj = enrich.EnrichmentOps()
    df = enrich_obj.read_xml_dictionary(path_to_dict, dict_name)
    roo_logger.info("Termina extracción de información de diccionario")
    tablename = "dict_"+ dict_name.split("_")[1].lower() + "_" + dict_name.split("_")[2].lower()
    db_utils = load.DB_Utils(exestatus, engine)
    db_utils.load_dictionary(df, tablename)
    roo_logger.info("Termina ejecución de carga a base de datos")
    return (0, "Ejecucion Exitosa")
  except exc.XMLExtractException as xml_error:
    roo_logger.error("Ocurrio un error durante la carga del diccionario")
    roo_logger.error(xml_error)
    return (-1, "Ocurrio un error durante la carga del diccionario")
  except Exception as e:
    roo_logger.error("Ocurrio un error durante la carga del diccionario")
    roo_logger.error(str(e))
    return (-1, str(e))


def create_dashboard(date_from, date_to, params):
  exestatus = ExecutionStatus()
  roo_logger = logging.getLogger('root.load_dict_info')
  roo_logger.info("Comienza ejecución de carga de item volumen a base de datos")
  try:
    report_obj = report.ReportStats(params.STATS_FOLDER, params.LOG_FILES)
    print("Process running on 127.0.0.1:8050")
    report_obj.execution_report_dashboard(date_from, date_to)
    roo_logger.info("Termina ejecución de tablero de control")

  except Exception as e:
    roo_logger.error("Ocurrio error durante la creación del dashboard")
    print(e)
    roo_logger.error(str(e))



#function provideb by user Greenstick on stack overflow modified by Carlos Murillo
#on scope of project OASIS 

# Print iterations progress
def printProgressBar (iteration, total, prefix = '', suffix = '', decimals = 1, length = 100, fill = '█', printEnd = "\r"):
    """
    Call in a loop to create terminal progress bar
    @params:
        iteration   - Required  : current iteration (Int)
        total       - Required  : total iterations (Int)
        prefix      - Optional  : prefix string (Str)
        suffix      - Optional  : suffix string (Str)
        decimals    - Optional  : positive number of decimals in percent complete (Int)
        length      - Optional  : character length of bar (Int)
        fill        - Optional  : bar fill character (Str)
        printEnd    - Optional  : end character (e.g. "\r", "\r\n") (Str)
    """
    if total==0:
      total = 1 #avoid division by zero
    percent = ("{0:." + str(decimals) + "f}").format(100 * (iteration / float(total)))
    filledLength = int(length * iteration // total)
    bar = fill * filledLength + '-' * (length - filledLength)
    print('\r%s |%s| %s%% %s' % (prefix, bar, percent, suffix), end = printEnd)
    # Print New Line on Complete
    if iteration == total: 
        print()

def create_datacheck(params, name_new, name_old):
  """Function that creates a datacheck file executed by Nielsen"""
  dt_object = extract.DatacheckOperations(params.TEMP_INPUT_FILES, name_old, name_new, params.INPUT_COLUMNS)
  return dt_object.create_datacheck(params.TEMP_INPUT_FILES)


def load_stats_to_db(stats_folder, mode):
  roo_logger = logging.getLogger('root.load_dict_info')
  roo_logger.info("Comienza ejecución de carga de estadisticas a base de datos")
  try:
    stats_obj = loadstats.StatsOps()
    ini_config= DB_Connect()
    engine = ini_config.get_engine()
    schema = ini_config.get_db_schema_name()
    stats_obj.load_data(stats_folder, engine, schema, mode)
    roo_logger.info("Finaliza ejecución de carga de estadisticas a base de datos")
  except Exception as e:
    roo_logger.error("Ocurrio un error durante la carga de datos a la base de datos")


def load_stats_to_file(stats_folder):
  roo_logger = logging.getLogger('root.load_stats_to_file')
  roo_logger.info("Comienza ejecución de creacion de estadísticas en carpeta local")
  try:
    stats_obj = loadstats.StatsOps()
    ini_config= DB_Connect()
    conn = ini_config.get_connection()
    schema = ini_config.get_db_schema_name()
    stats_obj.move_data_to_csvfile(stats_folder, conn, schema)
    roo_logger.info("Finaliza ejecución de creacion de estadísticas en carpeta local")
  except Exception as e:
    roo_logger.error("Ocurrio error durante la lectura de las tablas de estadísticas")

def publish_views_to_s3(year, month):
  #primera parte del proceso de publicación de archivos a S3 
  roo_logger = logging.getLogger('root.publish_views_to_s3')
  params = ProjectParameters()  
  try:
    publish_obj = publish.PublishOps(params.VIEW_PATH, params.S3_BUCKET_NAME, params.S3_KEY_NAME)
    roo_logger.info("Comienza creación de archivo de vista de Scantrack")
    publish_obj.create_view_file('SCANTRACK', year, month)
    roo_logger.info("Comienza creación de archivo de vista de Retail")
    publish_obj.create_view_file('RETAIL', year, month)
    roo_logger.info("Finaliza ejecución de creación de vistas iniciando publicación a s3")
    
    #para cada vista se debe realizar una publicación en s3 en caso que sea correcta la ejecución
    #se elimina el archivo local
    created = set(os.listdir(params.VIEW_PATH))
    for view in os.listdir(params.VIEW_PATH):
      result = publish_obj.publish_s3_bucket(view)
      if result:
        os.unlink(os.path.join(params.VIEW_PATH, view))

    left = set(os.listdir(params.VIEW_PATH))

    return(0,  "Ejecución Finalizada", list(created.difference(left)))

  except Exception as e:
    roo_logger.error("Error de ejecución de la ejecución de vistas")
    roo_logger.error(str(e))
    return(-1, "Ocurrio un error durante la creación o Publicacion de vistas: %s" % str(e), -1)

def read_and_publish_oasis_inputs_and_catalogs_step_one():
  """# publish nielsen data to ftp in multiple steps  designed to  work with the api class
  # is divided in three functions the first one apply the join with the catalogs and detects new
  # condition returns a json with the response with the new files to analize"""
  params = ProjectParameters()
  input_files = os.listdir(params.CATALOG_FILES)
  files_input  = os.listdir(params.INPUT_FILES)
  catalog_files = []
  for infile in input_files:
    if infile.startswith("OASIS_MST"):
      catalog_files.append(infile)

  roo_logger = logging.getLogger('root.read_publish_nielsen_data_api')
  roo_logger.info('Borrando archivos de catalogos temporales')
  #before the process delete files on the temp catalog files path
  for tmp_file in os.listdir(params.TEMP_CATALOG_FILES):
    os.unlink(os.path.join(params.TEMP_CATALOG_FILES,tmp_file))
  exestatus = ExecutionStatus()
  exestatus.start_process_hour = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
  #implement the continue and status
  for namefile in files_input:
    exestatus.chunk_counter = 0 #change chunk counter status from file
    exestatus.process_filename = namefile
    roo_logger.info('Procesando archivo %s' % namefile)
    chunk = pd.read_csv(params.INPUT_FILES + namefile, sep=';', encoding='latin', chunksize = params.chunksize)
    bitheader = 1
    for data in chunk:
      process_obj = extract.FileOpsReadAndValidations(data, params.INPUT_COLUMNS, params.ADDITIONAL_DCHK_COLUMNS)
      process_obj.process_chunk(params.INPUT_STRING_COLUMNS,exestatus,  pd)
      bitheader = process_obj.cross_catalogs(catalog_files, bitheader, pd, params)
      roo_logger.info('procesando lote numero %s ' % str(exestatus.chunk_counter))
      exestatus.chunk_counter = exestatus.chunk_counter + 1 
      #print(exestatus.chunk_counter)
    #before opening the next file call garbage collection
    gc.collect()
  exestatus.end_process_hour = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
  exestatus.calculate_time_difference_process()
  temp_catalog_files = os.listdir(params.TEMP_CATALOG_FILES)
  extract.simplify_catalogs(temp_catalog_files, pd, params)
  qty_tmp_cat = len(temp_catalog_files)
  return (0, temp_catalog_files, qty_tmp_cat)

def read_and_publish_oasis_inputs_and_catalogs_step_two(ans):
  params = ProjectParameters()
  temp_catalog_files = os.listdir(params.TEMP_CATALOG_FILES)
  qty_tmp_cat = len(temp_catalog_files)
  exestatus = ExecutionStatus()
  roo_logger = logging.getLogger('root.read_publish_nielsen_data_api')
  if qty_tmp_cat != 0:
    if ans.upper() == 'S':
      extract.publish_catalogs(params.CATALOG_FILES, params.TEMP_CATALOG_FILES , pd, git, "Nielsen") #change the user
      roo_logger.info("catalogos publicados correctamente")
    else:
      roo_logger.error("Revisar inconsistencias y ejecutar de nuevo")
      return (-1, "Revisar inconsistencias y ejecutar de nuevo")
  return (0, "catalogos publicados correctamente")

def read_and_publish_oasis_inputs_and_catalogs_step_three(ans):
  params = ProjectParameters()
  exestatus = ExecutionStatus()
  exestatus.start_process_hour = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
  roo_logger = logging.getLogger('root.read_publish_nielsen_data_api')
  files_input  = os.listdir(params.INPUT_FILES)
  if ans.upper() == 'S':
    #publicar archivos al FTP capturar datos de acceso al FTP
    mail_obj = MailClass(DB_Connect().get_mail_config()) #objeto de comunicación de correo
    extract.publish_catalogs(params.CATALOG_FILES, params.TEMP_CATALOG_FILES , pd, git, "Nielsen") #change the user
    ftp_data = DB_Connect()
    ftp_data = ftp_data.get_ftp_config()
    roo_logger.info("Publicando archivos procesados a servidor ftp...")
    host = ftp_data[2]
    user = ftp_data[0]
    password = ftp_data[1]
    for namefile in files_input:
      df = pd.read_csv(params.INPUT_FILES + namefile, sep=";", encoding='latin')
      df['TAMANO'] = df['TAMANO'].apply(lambda x: x.replace(" ", "."))
      df.to_csv(params.INPUT_FILES + namefile, sep=";", encoding="latin", header=True, index=False)
      ispublished = ftp_ops.filePublishToFTP(params.INPUT_FILES, namefile, host, user, password)
      if ispublished:
        roo_logger.info("Archivo %s publicado de manera correcta, se eliminará la versión local" %  namefile)
        exestatus.qty_process_files = exestatus.qty_process_files + 1 
      else:
        roo_logger.error("Error subiendo el archivo %s al servidor FTP" % namefile)
    try:
      roo_logger.info("Enviando correo...")
      mail_obj.send_mail("Publicacion archivos Nielsen", util_constant.get_mail_nls_ok_message(exestatus),
         "INFO", params.LOG_FILES)
    except:
      roo_logger.error("Error enviando correo de confirmación de publicación al FTP")
  else:
    roo_logger.warning("Los archivos no serán publicados al servidor")



  files_output  = os.listdir(params.INPUT_FILES)
  exestatus.end_process_hour = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
  exestatus.calculate_time_difference_process()
  roo_logger.info("El proceso ha finalizado")
  return (0, "El proceso ha finalizado", files_input, files_output)
  #done!

def safely_get_last_repo_version(repo_path):
  config_branch_name = DB_Connect().get_git_branch_name()
  this_repo = git.Repo(repo_path)
  roo_logger = logging.getLogger('root.safely_pull')
  origin = this_repo.remote(name='origin')

  modifiedfiles = [i.a_path for i in this_repo.index.diff(None)]
  #verify that i can safely pull data from remote
  if len(this_repo.untracked_files) == 0 and len(modifiedfiles) == 1 and modifiedfiles[0] == 'stage_area/catalogos':
    roo_logger.info('Se deben publicar cambios en el repositorio antes de continuar... Publicando cambios')
    this_repo.git.add(modifiedfiles, update=True)
    this_repo.index.commit("Publicando actualizaciones de catálogos en repositorio principal actualizado por programa de ejecución OASIS")
    origin.push()
  elif len(this_repo.untracked_files) == 0 and len(modifiedfiles) == 0:
    roo_logger.info('Repositorio actualizado con la versión de Gitlab, actualizando versión Local')
  else:
    roo_logger.error('El repositorio contiene información no esperada no se puede ejecutar el proceso de manera automática, verificar usando \'git status\'')
    return -1

  roo_logger.info('Repositorio preparado para recibir datos de Gitlab iniciando el proceso de actualización del repositorio')
  commits_behind = this_repo.iter_commits('%s..origin/%s' % (config_branch_name.lower(),config_branch_name.lower()))
  count_behind = sum(1 for c in commits_behind)
  if count_behind != 0:
    origin.pull() # get the last version of the repo 

    #despues de actualizar el repositorio  se deben actualizar de nuevo los catalogos para que el roceso pueda obtener un git pull sin inconvenientes
    modifiedfiles = [i.a_path for i in this_repo.index.diff(None)]
    #verify that i can safely pull data from remote
    if len(this_repo.untracked_files) == 0 and len(modifiedfiles) == 1 and modifiedfiles[0] == 'stage_area/catalogos':
      roo_logger.info('Se deben publicar cambios en el repositorio antes de continuar... Publicando cambios')
      this_repo.git.add(modifiedfiles, update=True)
      this_repo.index.commit("Publicando actualizaciones de catálogos en repositorio principal actualizado por programa de ejecución OASIS")
      origin.push()
    elif len(this_repo.untracked_files) == 0 and len(modifiedfiles) == 0:
      roo_logger.info('Repositorio actualizado con la versión de Gitlab, actualizando versión Local')
    else:
      roo_logger.error('El repositorio contiene información no esperada no se puede ejecutar el proceso de manera automática, verificar usando \'git status\'')
      return -1
  else:
    roo_logger.info('Repositorio se encuentra actualizado no se realizaron acciones adicionales')
  
  return 0


def safely_set_last_repo_version(repo_path):
  config_branch_name = DB_Connect().get_git_branch_name()
  this_repo = git.Repo(repo_path)
  roo_logger = logging.getLogger('root.safely_push')
  origin = this_repo.remote(name='origin')
  commits_ahead = this_repo.iter_commits('origin/%s..%s' % (config_branch_name.lower(),config_branch_name.lower()))
  count_ahead = sum(1 for c in commits_ahead)
  roo_logger.info('publicando cambios despues de la ejecución de OASIS')

  modifiedfiles = [i.a_path for i in this_repo.index.diff(None)]

  if len(this_repo.untracked_files) == 0 and len(modifiedfiles) == 1 and modifiedfiles[0] == 'stage_area/catalogos':
    roo_logger.info('Se deben publicar cambios en el repositorio antes de continuar... Publicando cambios')
    this_repo.git.add(modifiedfiles, update=True)
    this_repo.index.commit("Publicando actualizaciones de catálogos en repositorio principal actualizado por programa de ejecución OASIS")
    origin.push()
  elif len(this_repo.untracked_files) == 0 and len(modifiedfiles) == 0 and count_ahead >= 0:
    roo_logger.info('Repositorio local preparado para publicar, actualizando versión de Gitlab')
    origin.push()
  else:
    roo_logger.error('El repositorio contiene información no esperada no se puede ejecutar el proceso de manera automática, verificar usando \'git status\'')
    return -1

  return 0


def change_tag_process(df, params):
  code = iv.validate_tag_file(df)
  if code == 0:
    tc_obj = tcontroller.TransformController()
    exestatus = ExecutionStatus()
    engine = DB_Connect().get_engine()
    code, df_tags =  tc_obj.call_stored_procedure(df, exestatus, engine)
    df_tags.to_csv(params.TEMP_INPUT_FILES + 'tags_output.csv', header=True, sep=';', encoding='latin')
    return (code, "Ejecución correcta, archivo generado en carpeta de archivos temporales")

  else:
    return (-1, 'Revisar archivo inconsistencias')
    

def remove_tag_process(df, params):
  code = iv.validate_tag_remove_file(df)
  if code == 0:
    tc_obj = tcontroller.TransformController()
    exestatus = ExecutionStatus()
    engine = DB_Connect().get_engine()
    code, df_tags =  tc_obj.call_remove_stored_procedure(df, exestatus, engine)
    df_tags.to_csv(params.TEMP_INPUT_FILES + 'tags_output.csv', header=True, sep=';', encoding='latin')
    return (code, "Ejecución correcta, archivo generado en carpeta de archivos temporales")    
  else:
    return (-1, 'Revisar archivo inconsistencias')


