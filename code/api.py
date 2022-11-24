from flask import Flask
from flask_restful import Resource, Api, reqparse
import werkzeug
from flask_cors import CORS 
import os
import pandas as pd
import utils
import reporting
import logging
import extract.input_validations as iv
import exceptions as exc
from datetime import datetime
import util_constant  #strings for mail message
from transversal_classes import ProjectParameters
from transversal_classes import DB_Connect
from transversal_classes import ExecutionStatus
from transversal_classes import MailClass
from utils_other_sources import LoaderOtherGeographies, CleanerOtherGeo
import dateutil.relativedelta
import pdb

app = Flask(__name__)
CORS(app)
api = Api(app)


class NielsenFTPPublish(Resource):

  def get(self):
    try:
      roo_logger = logging.getLogger('')
      roo_logger.info('Comienza lectura de parámetros archivo JSON')
      params = ProjectParameters()
      exe_mode = DB_Connect().get_execution_mode()
      mail_obj = MailClass(DB_Connect().get_mail_config())
      roo_logger.info('Se define el modo de ejecución con usuario '+ exe_mode)
      exestatus = ExecutionStatus()
      iv.validate_input(params.INPUT_FILES, params.OUTPUT_GRAPHS_FOLDER, params.STATS_FOLDER, params.INPUT_STRING_COLUMNS, 0, exe_mode)
      cod_status, data, qty = utils.read_and_publish_oasis_inputs_and_catalogs_step_one() #Nielsen
      roo_logger.info('Ejecución finalizada')
      #create response
      json_response = [{'cod_status': cod_status,
      'message_response': 'Ejecución finalizada',
      'tmp_files': data,
      'qty_files': qty}]

      return json_response
    except Exception as e:
      #create response
      json_response = [{'cod_status': -1,
      'message_response': str(e),
      'tmp_files': [],
      'qty_files': 0}]

      return json_response

class NielsenRefreshCatalogs(Resource):
  def get(self, ans):
    try:
      cod_status, message_response = utils.read_and_publish_oasis_inputs_and_catalogs_step_two(ans)
      json_response = {'cod_status': cod_status, 'message_response': message_response}
      return json_response
    except Exception as e:
      json_response = {'cod_status': -1, 'message_response': str(e)}
      return json_response

class  NielsenPublishInfoFTP(Resource):
  def get(self, ans):
    try:
      cod_status, message_response, ini_files, end_files = utils.read_and_publish_oasis_inputs_and_catalogs_step_three(ans)
      qty_published =len(ini_files)-len(end_files)
      json_response = {'cod_status': cod_status, 'message_response': message_response, 'ini_files':ini_files,'qty_published':qty_published}
      return json_response
    except Exception as e:
      json_response = {'cod_status': -1, 'message_response': str(e), 'ini_files':[], 'qty_published':0}
      return json_response

class NielsenChangeCatalogValue(Resource):
  def get(self,infile, old_val, new_val, column='Nielsen'):
    try:
      params = ProjectParameters()
      user = column
      path_to_catalog = params.CATALOG_FILES
      cod_status, message_response = utils.update_catalog(infile, path_to_catalog, old_val, new_val, user)
      json_response = {'cod_status': cod_status, 'message_response': message_response}
      return json_response
    except Exception as e:
      json_response = {'cod_status': -1, 'message_response': str(e)}
      return json_response

class NielsenCreateDatacheck(Resource):
  def get(self,old_file, new_file):
    try:
      params = ProjectParameters()
      cod_status, message_response = utils.create_datacheck(params, new_file, old_file)
      json_response = {'cod_status': cod_status, 'message_response': message_response}
      return json_response
    except Exception as e:
      json_response = {'cod_status': -1, 'message_response': str(e)}
      return json_response

class NutresaLoadNielsentoDB(Resource):
  def get(self):
    roo_logger = logging.getLogger('')
    roo_logger.info('Comienza lectura de parámetros archivo JSON')
    params = ProjectParameters()
    exe_mode = DB_Connect().get_execution_mode()
    mail_obj = MailClass(DB_Connect().get_mail_config())
    roo_logger.info('Se define el modo de ejecucion con usuario '+ exe_mode)
    exestatus = ExecutionStatus()
    exestatus.reset_execution_status()
    exestatus.execution_status = 1
    roo_logger.info('Registrar tiempo de inicio de ejecución')
    exestatus.start_process_hour = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    utils.prepare_workspace(params.LOG_FILES, params.CATALOG_FILES, params.STATS_FOLDER)  # ¿when to do this?
    utils.load_stats_to_file(params.STATS_FOLDER)
    roo_logger.info('Carga de estadísticas de la base de datos completa')
    #utils.recieve_files_from_ftp() #just for testing uncomment when done #not uncomment done with Sterling
    lot_id = exestatus.get_lot_id(params.STATS_FOLDER)
    try:
      iv.validate_input(params.INPUT_FILES, params.OUTPUT_GRAPHS_FOLDER, params.STATS_FOLDER, params.INPUT_STRING_COLUMNS,lot_id , exe_mode)
    except Exception as e:
      roo_logger.error("Ocurrio un error de ejecución durante la  validación del insumo prosiguiendo con la carga")
      roo_logger.error(e)
    try:
      utils.recieve_catalogs() #Nutresa
    except exc.InvalidCatalogException as e:
      roo_logger.error("Ocurrio un error durante la recepción de catálogos")
      roo_logger.error(e.message)
      json_response = {'cod_status': -1, 'message_response': e.message}
      return json_response
    utils.main(params, mail_obj)
    utils.create_report(params)
    exestatus.end_proccess_hour = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    exestatus.calculate_time_difference_process()
    exestatus.write_process_stats(params.STATS_FOLDER)
    try:
      mail_obj.send_mail("Ejecución Finalizada", util_constant.get_mail_nls_ok_message(exestatus), "INFO", params.LOG_FILES)
    except:
      roo_logger.error("No se pudo enviar el correo de ejecución de finalización, revisar parámetros y puertos de la herramienta")
    exestatus.execution_status = 0
    utils.load_stats_to_db(params.STATS_FOLDER, 'replace')
    roo_logger.info('Registrar tiempo de finalización de ejecución')
    json_response = {'cod_status': 0, 'message_response': "Proceso de carga completado"}
    return json_response

class NutresaReprocessLoad(Resource):
  def get(self):
    try:
      exestatus = ExecutionStatus()
      exestatus.reset_execution_status()
      exestatus.execution_status = 1
      params = ProjectParameters()
      list_files = os.listdir(params.INPUT_FILES)
      utils.recieve_catalogs() #Nutresa, recibe catalogos antes de empezar la ejecución en nutresa
      cod_status, message_response = utils.reprocess_load(params, list_files)
      json_response = {'cod_status': cod_status, 'message_response': message_response}
      exestatus.execution_status = 0
      return json_response
    except Exception as e:
      json_response = {'cod_status': -1, 'message_response': str(e)}
      exestatus.execution_status = 0
      return json_response

class NutresaProcessNovelty(Resource):
  def get(self):
    try:
      exestatus = ExecutionStatus()
      exestatus.reset_execution_status()
      exestatus.execution_status = 1
      utils.recieve_catalogs() #Nutresa, recibe catalogos antes de empezar la ejecución en nutresa
      cod_status, message_response = utils.resume_execution()
      json_response = {'cod_status': cod_status, 'message_response': message_response}
      exestatus.execution_status = 0
      return json_response
    except Exception as e:
      exestatus.execution_status = 0
      json_response = {'cod_status': -1, 'message_response': str(e)}
      return json_response

class NutresaDeleteDataCatalog(Resource):
  def get(self, catalog_name, old_val, exe_mode):
    try:
      params = ProjectParameters()
      cod_status, message_response = utils.delete_data_from_catalog(catalog_name, params.CATALOG_FILES, old_val, exe_mode)
      json_response = {'cod_status': cod_status, 'message_response': message_response}
      return json_response
    except Exception as e:
      json_response = {'cod_status': -1, 'message_response': str(e)}
      return json_response

class NutresaRefreshIV(Resource):
  def get(self, path_to_iv):
    try:
      params = ProjectParameters()
      cod_status, message_response = utils.refresh_item_volumen_info(path_to_iv, params.TEMP_INPUT_FILES)
      json_response = {'cod_status': cod_status, 'message_response': message_response}
      return json_response
    except Exception as e:
      json_response = {'cod_status': -1, 'message_response': str(e)}
      return json_response

class NutresaRefreshDict(Resource):
  def get(self, path_to_dict):
    try:
      params = ProjectParameters()
      cod_status, message_response = utils.refresh_dict_info(path_to_dict, params.TEMP_INPUT_FILES)
      json_response = {'cod_status': cod_status, 'message_response': message_response}
      return json_response
    except Exception as e:
      json_response = {'cod_status': -1, 'message_response': str(e)}
      return json_response

class LogReader(Resource):
  def get(self):
    try:
      params = ProjectParameters()
      with open(params.LOG_FILES+'execution_info.log', 'r') as f:
        contents = f.read()
      #type(contents)
      return  contents
    except FileNotFoundError as e:
      return "Archivo de log no existente"
    except Exception as e:
      return e

class StatusReader(Resource):
  def get(self):
    try:
      exestatus = ExecutionStatus();
      params = ProjectParameters();
      json_response = {
        'cod_status':0,
        'actual_file': exestatus.process_filename,
        'qty_process_files': exestatus.qty_process_files,
        'qty_error_files': exestatus.qty_error_files,
        'qty_discard_files': exestatus.qty_discard_files,
        'qty_lot_errors': exestatus.error_chunks_counter,
        'lot_size': exestatus.chunksize,
        'qty_pending_files': len(os.listdir(params.INPUT_FILES)),
        'qty_archivos_procesados': exestatus.qty_archivos_procesados,
        'qty_rows_act_file': exestatus.file_qty_rows,
        'actual_chunk': exestatus.chunk_counter,
        'initial_file_hour': exestatus.start_hour_file,
        'end_file_hour': exestatus.end_hour_file,
        'end_process_hour': exestatus.end_proccess_hour,
        'start_process_hour':exestatus.start_process_hour
      }
      return json_response
    except Exception as e:
      json_response = {
        'cod_status':-1,
        'message_response': str(e)
      }
      return json_response

class CreateDashboard(Resource):
  def get(self,datefrom, dateto):
    try:
      params = ProjectParameters()
      utils.create_dashboard(datefrom, dateto, params)
      return {'cod_status': 0}
    except Exception as e:
      return {'cod_status': -1, 'message_response': str(e)}

class PublishViewsToS3(Resource):
  def get(self, anio=0, mes=0):
    try:
      if anio==0 or mes==0:
        #calcular fecha
        d1 = datetime.date.today()
        d2 = d1 - dateutil.relativedelta.relativedelta(months=1)
        anio = d2.year
        mes = d2.month
      cod_status, message_response, view_published = utils.publish_views_to_s3(anio, mes)
      json_response = {
        'cod_status':cod_status,
        'message_response': message_response,
        'view_published' : view_published
      }
      return json_response

    except Exception as e:
      return {'cod_status': -1, 'message_response': str(e), 'view_published':'Revisar log ejecucion'}

class NutresaChangeTag(Resource):
  def post(self):
    try:
      params = ProjectParameters()
      parse = reqparse.RequestParser()
      parse.add_argument('file_tag', type=werkzeug.datastructures.FileStorage, location='files')

      args = parse.parse_args()
      tag_file = args['file_tag']
      tag_file.save(params.TEMP_INPUT_FILES + 'tmp_tag.xlsx')

      df = pd.read_excel(params.TEMP_INPUT_FILES + 'tmp_tag.xlsx', engine='openpyxl')
      cod_status, df_response = utils.change_tag_process(df, params)
      return {'cod_status': cod_status, 'message_response': df_response}
    except Exception as e:
      return {'cod_status': -1, 'message_response': str(e)}

class NutresaRemoveTag(Resource):
  def post(self):
    try:
      params = ProjectParameters()
      parse = reqparse.RequestParser()
      parse.add_argument('file_tag', type=werkzeug.datastructures.FileStorage, location='files')

      args = parse.parse_args()
      tag_file = args['file_tag']
      tag_file.save(params.TEMP_INPUT_FILES + 'tmp_tag.xlsx')

      df = pd.read_excel(params.TEMP_INPUT_FILES + 'tmp_tag.xlsx', engine='openpyxl')
      cod_status, df_response = utils.remove_tag_process(df, params)
      return {'cod_status': cod_status, 'message_response': df_response}
    except Exception as e:
      return {'cod_status': -1, 'message_response': str(e)}

class RunningStatus(Resource):
  def get(self):
    try:
      exestatus = ExecutionStatus();
      json_response = {
        'cod_status'        : 0,
        'execution_status'  : exestatus.execution_status
      }
      return json_response
    except Exception as e:
      return {'cod_status': -1, 'message_response': str(e)}

class StorecheckTransformRawData(Resource):
  def get(self):
    try:
      roo_logger = logging.getLogger('')
      roo_logger.info('Comienza lectura de parámetros archivo JSON')
      params = ProjectParameters()
      params_storecheck = params.getothergeoparameters()
      input_files = os.listdir(params_storecheck['path_to_storecheck_input_files'])
      proc_files = []
      exestatus = ExecutionStatus()
      for f in input_files:
        category = f.split('_')[3].upper().strip()
        ishistoric = True if f.split('_')[2].upper().strip() in ['HISTORICO', 'HISTÓRICO'] else False
        if ishistoric:
          periodo = "HISTORICO"
        else:
          periodo = f.split('_')[4]
        prefix_file = "DN_PANAMA_3_"
        new_filename = prefix_file + category + "_" + periodo + ".xlsx"
        exestatus.process_filename = new_filename

        roo_logger.info(f'parametros recibidos: {ishistoric}, {category}, {f}')
        cleaner = CleanerOtherGeo(params)
        cleaner.move_raw_input_to_temp_input(f, new_filename, 
          'path_to_storecheck_input_files', 'path_to_storecheck_processed_files')

        complete_filename = params.TEMP_INPUT_FILES + prefix_file + category + "_" + periodo + ".xlsx"
        roo_logger.info(f'creado archivo en la carpeta temporal: {complete_filename}')
        
        roo_logger.info(f'Inicia proceso de validación del archivo')
        loader = LoaderOtherGeographies(3, complete_filename, params)
        df = loader.validate_raw_input_file(category)
        qty, df, tmp = loader.transform_raw_input_to_oasis_input(df,new_filename)
        if qty != 0:
          roo_logger.info(f'archivo temporal creado {complete_filename.replace("xlsx", "csv")}, pendiente de asignar valores temporales')
        else:
          roo_logger.info(f"Ejecución completada, archivo \"{complete_filename}\" creado y validado en carpeta temporal")
        loader.export_oasis_file(df, complete_filename.replace('xlsx', 'csv'))
        proc_files.append(new_filename)

      #termina la lectura y validación de archivo diseñando respuesta
      qty, tmp = len(os.listdir(params.TEMP_CATALOG_FILES)), os.listdir(params.TEMP_CATALOG_FILES)
      nl = '\n'

      if qty == 0:
        json_response = {
          'cod_status'        : 0,
          'message_response'  : f"Archivos validados en carpeta temporal los archivos: {nl.join(proc_files)} ",
          'catalogos'         : []
        }
      else: 
        json_response = {
          'cod_status'        : 1,
          'message_response'  : f"""Ejecución completada, los archivos: {nl.join(proc_files)} {nl} han sido validado pero existen 
          diferencias con catalogos publicados, hay {qty} archivos con diferencias.""",
          'catalogos'         : tmp
        }
      return json_response
    except Exception as e:
      roo_logger.error(str(e))
      return {'cod_status': -1, 'message_response': str(e)}

class StorecheckAssignTemporaryValues(Resource):
  def get(self):
    try:
      roo_logger = logging.getLogger('')
      roo_logger.info('Comienza lectura de parámetros archivo JSON')
      params = ProjectParameters()
      
      tmp_raw_files = os.listdir(params.TEMP_INPUT_FILES)
      prefix_file = "DN_PANAMA_3_"
      for tmp_raw_file in tmp_raw_files:
        if tmp_raw_file.startswith(prefix_file) and tmp_raw_file.endswith('.csv'):
          roo_logger.info(f'realizando lectura de archivo: {tmp_raw_file}')
          DICT_RAW_DATA_TYPES = {'Rango':str, 'Subtipo':str, 'Tipo':str, 'Categoria_':str, 
          'Product_id':str, 'Upc':str}
          df = pd.read_csv(params.TEMP_INPUT_FILES + tmp_raw_file, sep=";", encoding='latin', dtype=DICT_RAW_DATA_TYPES)

          roo_logger.info(f'asignando valores temporales a archivo: {tmp_raw_file}')
          complete_filename = params.TEMP_INPUT_FILES + tmp_raw_file
          loader = LoaderOtherGeographies(3, complete_filename, params)
          df = loader.assign_temp_files_values_to_df(df)
          loader.export_oasis_file(df, params.TEMP_INPUT_FILES + tmp_raw_file)
          roo_logger.info(f'creado archivo: {tmp_raw_file}')
      #borrar catalogos temporales y mover datos insumo 3 a la carpeta de input files
      cleaner = CleanerOtherGeo(params)
      roo_logger.info(f'borrando catalogos temporales')
      cleaner.remove_tmp_catalogs()
      roo_logger.info(f'moviendo y borrando archivo de insumo temporal a la carpeta insumo de oasis')
      cleaner.move_temp_input_to_oasis_input(prefix_file)

      json_response = {
          'cod_status':   0,
          'message_response': "Archivos cargados en la carpeta de insumo para ser procesados en OASIS"
      }
      return json_response
    except Exception as e:
      roo_logger.error(str(e))
      return {'cod_status': -1, 'message_response': str(e)}

class ItemVolumenPanamaTransformRawData(Resource):
  def get(self):
    try:
      roo_logger = logging.getLogger('')
      roo_logger.info('Comienza lectura de parámetros archivo JSON')
      params = ProjectParameters()
      params_iv = params.getothergeoparameters()
      input_files = os.listdir(params_iv['path_to_iv_panama_input_files'])
      proc_files = []
      exestatus = ExecutionStatus()
      prefix_file = "NIELSEN_PANAMA_4_"
      for f in input_files:
        category = f.split('_')[3].upper() # TODO CHANGE THIS 
        ishistoric = True if f.split('_')[2].upper().strip() in ['HISTORICO', 'HISTÓRICO'] else False #TODO CHANGE THIS
        if ishistoric:
          periodo = "HISTORICO"
        else:
          periodo = f.split('_')[4]
        new_filename = prefix_file + category + "_" + periodo + ".xlsx"
        exestatus.process_filename = new_filename

        roo_logger.info(f'parametros recibidos: {ishistoric}, {category}, {f}')
        cleaner = CleanerOtherGeo(params)
        cleaner.move_raw_input_to_temp_input(f, new_filename,
          'path_to_iv_panama_input_files','path_to_iv_panama_processed_files')
        complete_filename = params.TEMP_INPUT_FILES + prefix_file + category + "_" + periodo + ".xlsx"
        roo_logger.info(f'creado archivo en la carpeta temporal: {complete_filename}')

        roo_logger.info(f'Inicia proceso de validación del archivo')
        loader = LoaderOtherGeographies(4, complete_filename, params)
        df = loader.validate_raw_input_file(category)
        qty, df, tmp = loader.transform_raw_input_to_oasis_input(df,new_filename)
        if qty != 0:
          roo_logger.info(f'archivo temporal creado {complete_filename.replace("xlsx", "csv")}, pendiente de asignar valores temporales')
        else:
          roo_logger.info(f"Ejecución completada, archivo \"{complete_filename}\" creado y validado en carpeta temporal")
        roo_logger.info(f"Ejecución completada, archivo \"{complete_filename}\" creado y validado en carpeta temporal")
        loader.export_oasis_file(df, complete_filename.replace('xlsx', 'csv'))
        proc_files.append(new_filename)
        #termina la lectura y validación de archivo diseñando respuesta

      qty, tmp = len(os.listdir(params.TEMP_CATALOG_FILES)), os.listdir(params.TEMP_CATALOG_FILES)
      nl = '\n'
      if qty == 0:
        json_response = {
          'cod_status'        : 0,
          'message_response'  : f"Archivos validados en carpeta temporal los archivos: {nl.join(proc_files)} ",
          'catalogos'         : []
        }
      else: 
        json_response = {
          'cod_status'        : 1,
          'message_response'  : f"""Ejecución completada, los archivos: {nl.join(proc_files)} {nl} han sido validado pero existen 
          diferencias con catalogos publicados, hay {qty} archivos con diferencias.""",
          'catalogos'         : tmp
        }  
      return json_response
    except Exception as e: #mala practica se debe capturar sección específica
      roo_logger.error(str(e))
      return {'cod_status': -1, 'message_response': str(e), 'catalogos': []}



class ItemVolumenAssignTemporaryValues(Resource):
  def get(self):
    try:
      roo_logger = logging.getLogger('')
      roo_logger.info('Comienza lectura de parámetros archivo JSON')
      params = ProjectParameters()
      
      tmp_raw_files = os.listdir(params.TEMP_INPUT_FILES)
      prefix_file = "NIELSEN_PANAMA_4_"
      for tmp_raw_file in tmp_raw_files:
        if tmp_raw_file.startswith(prefix_file) and tmp_raw_file.endswith('.csv'):
          roo_logger.info(f'realizando lectura de archivo: {tmp_raw_file}')
          DICT_RAW_DATA_TYPES = {'Rango':str, 'Subtipo':str, 'Tipo':str, 'Categoria_':str, 
          'Product_id':str, 'Upc':str}
          df = pd.read_csv(params.TEMP_INPUT_FILES + tmp_raw_file, sep=";", encoding='latin', dtype=DICT_RAW_DATA_TYPES)

          roo_logger.info(f'asignando valores temporales a archivo: {tmp_raw_file}')
          complete_filename = params.TEMP_INPUT_FILES + tmp_raw_file
          loader = LoaderOtherGeographies(4, complete_filename, params)
          df = loader.assign_temp_files_values_to_df(df)
          loader.export_oasis_file(df, params.TEMP_INPUT_FILES + tmp_raw_file)
          roo_logger.info(f'creado archivo: {tmp_raw_file}')
      #borrar catalogos temporales y mover datos insumo 3 a la carpeta de input files
      cleaner = CleanerOtherGeo(params)
      roo_logger.info(f'borrando catalogos temporales')
      cleaner.remove_tmp_catalogs()
      roo_logger.info(f'moviendo y borrando archivo de insumo temporal a la carpeta insumo de oasis')
      cleaner.move_temp_input_to_oasis_input(prefix_file)

      json_response = {
          'cod_status':   0,
          'message_response': "Archivos cargados en la carpeta de insumo para ser procesados en OASIS"
      }
      return json_response
    except Exception as e:
      roo_logger.error(str(e))
      return {'cod_status': -1, 'message_response': str(e)}



## adding resources to api,  all the resources are get implemented at the moment
api.add_resource(NielsenFTPPublish, '/load_nls_val')
api.add_resource(NielsenRefreshCatalogs, '/load_nls_cat/ans/<ans>')
api.add_resource(NielsenPublishInfoFTP, '/load_nls_ftp/ans/<ans>')
api.add_resource(NielsenChangeCatalogValue, '/update_catalog/infile/<infile>/old_val/<old_val>/new_val/<new_val>', '/update_catalog/infile/<infile>/old_val/<old_val>/new_val/<new_val>/column/<column>')
api.add_resource(NielsenCreateDatacheck, '/create_datacheck/old_file/<old_file>/new_file/<new_file>')
api.add_resource(NutresaLoadNielsentoDB, '/load_nut_to_db')
api.add_resource(NutresaReprocessLoad, '/nut_reprocess_load')
api.add_resource(NutresaProcessNovelty,'/nut_process_novelty')
api.add_resource(NutresaDeleteDataCatalog, '/nut_delete_data_catalog/catalog_name/<catalog_name>/old_val/<old_val>/exe_mode/<exe_mode>')
api.add_resource(NutresaRefreshIV, '/nut_refresh_iv/path_to_iv/<path_to_iv>')
api.add_resource(NutresaRefreshDict, '/nut_refresh_dict/path_to_dict/<path_to_dict>')
api.add_resource(LogReader, '/get_execution_log')
api.add_resource(StatusReader, '/get_execution_status')
api.add_resource(CreateDashboard, '/create_dashboard/datefrom/<datefrom>/dateto/<dateto>')
api.add_resource(PublishViewsToS3, '/publish_views_to_s3/anio/<anio>/mes/<mes>', '/publish_views_to_s3')
api.add_resource(NutresaChangeTag, '/change_tag_process')
api.add_resource(NutresaRemoveTag, '/remove_tag_process')
api.add_resource(RunningStatus, '/get_running_status')
api.add_resource(StorecheckTransformRawData, '/storecheck_raw_transform')
api.add_resource(StorecheckAssignTemporaryValues, '/storecheck_assign_temporary_values')
api.add_resource(ItemVolumenPanamaTransformRawData, '/item_volumen_panama_raw_transform')
api.add_resource(ItemVolumenAssignTemporaryValues, '/nielsenpan_assign_temporary_values')

app.run(host='0.0.0.0', port=8000, debug=True)