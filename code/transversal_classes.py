import os
import json
import configparser
import mysql.connector
import pdb
from sqlalchemy import create_engine
from datetime import datetime
import pandas as pd
#libraries needed to send emails
import email
import smtplib
import unidecode as ud
from email import encoders
from string import Template
from datetime import datetime
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email.utils import COMMASPACE, formatdate
from email.mime.multipart import MIMEMultipart

# TODO: study metaclasses in Python
class Singleton (type):
  _instances = {}
  def __call__(cls, *args, **kwargs):
    if cls not in cls._instances:
      cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
    return cls._instances[cls]

class ProjectParameters(metaclass=Singleton):
  """Clase que lee y almacena el archivo de configuración del proyecto uso transversal a la 
  aplicación """
  __instance = None
  __JSON_PATH = '../parametros_stats/config.json'
  config_filename = ""
  PROJECT_PATH= ''
  INPUT_FILES = ''
  CATALOG_FILES = ''
  TEMP_INPUT_FILES = ''
  TEMP_CATALOG_FILES = ''
  LOG_FILES = ''
  INPUT_COLUMNS = []
  INPUT_STRING_COLUMNS =[]
  CATALOG_DB_MAPPING = ''
  COLUMN_DB_MAPPING = ''
  CATALOG_COL_DB_MAPPING = ''
  STATS_FOLDER = ''
  PROCESSED_FILES = ''
  OUTPUT_GRAPHS_FOLDER = ''
  VIEW_PATH = ''
  chunksize = 0


  """this class is in charge of create and process all variable files"""
  def __init__(self):
    self.config_filename = self.__JSON_PATH
    self.readparameters()


  def getconfig(self):
    try:
      with open(self.config_filename, encoding='utf-8') as json_config_file:
        config = json.load(json_config_file)
    except Exception as e:
      print("reading test config file...")
      with open('../tests/unit/fixtures/config.json', encoding='utf-8') as json_test_config_file:
        config = json.load(json_test_config_file)
    return config

  def readparameters(self):
    config = self.getconfig()
    self.PROJECT_PATH = config['project']['project_folder']
    self.INPUT_FILES = config['project']['input_files']
    self.CATALOG_FILES = config['project']['catalog_files']
    self.INPUT_COLUMNS = config['project']['input_file_columns']
    self.TEMP_INPUT_FILES = config['project']['tmp_input_files']
    self.TEMP_CATALOG_FILES = config['project']['temp_catalog_files']
    self.INPUT_STRING_COLUMNS = config['project']['input_master_columns']
    self.OUTPUT_GRAPHS_FOLDER = config['project']['project_graph_folder']
    self.STATS_FOLDER = config['project']['stats_folder']
    self.PROCESSED_FILES = config['project']['processed_files']
    self.LOG_FILES = config['project']['log_files']
    self.chunksize = config['project']['chunksize']
    self.COLUMN_DB_MAPPING = config['db_mapping']['column_to_table']
    self.CATALOG_DB_MAPPING= config['db_mapping']['catalog_to_table']
    self.CATALOG_COL_DB_MAPPING=config['db_mapping']['catalog_column_to_db_column']
    self.ADDITIONAL_DCHK_COLUMNS= config['project']['datacheck_additional_columns']
    self.VIEW_PATH = config['project']['path_to_views']
    self.S3_BUCKET_NAME = config['project']['s3_bucket_name']
    self.S3_KEY_NAME = config['project']['s3_bucket_key_view']

  def getconsoledebuglevel(self):
    config = self.getconfig()
    return config['project']['console_log_level']

  def getfiledebuglevel(self):
    config = self.getconfig()
    return config['project']['file_log_level']

  def getothergeoparameters(self):
    """retorna TODOS los parametros adicionales para el proyecto otras geografias"""
    config = self.getconfig()
    return config['project_another_sources'] 

class DB_Connect(metaclass = Singleton):
  """Clase encargada de leer parámetros de configuración y conexiones entre ellas base de datos"""
  __INI_PATH = '../parametros_stats/config.ini'
  def __init__(self):
    config = configparser.ConfigParser(allow_no_value=True)
    try:
      config.read(self.__INI_PATH) 
    except FileNotFoundError as e:
      config.read('../tests/unit/fixtures/config.ini')
    self.dbconfig   = config['database']
    self.exe_mode   = config['execution_mode']
    self.gitconfig  = config['git']
    self.ftpconfig  = config['ftp']
    self.mailconfig = config['mail']

  def get_engine(self, region = 'col'):
    if region == 'col':
      engine = create_engine(self.dbconfig['conn_engine'] ,pool_recycle=3600,pool_size=6, echo=False)
    return engine

  def get_connection(self, region='col'):
    conn = mysql.connector.connect(user=self.dbconfig['username'], 
      password=self.dbconfig['password'], 
      database=self.dbconfig['dbname'], 
      port=self.dbconfig['port'], 
      host=self.dbconfig['servername'])
    return conn

  def get_db_schema_name(self, id_fuente=1):
    if id_fuente== 1 or id_fuente == 2:
      schema_name = self.dbconfig['dbname'].upper() #on linux systems sql is case sensitive
    elif id_fuente == 3 or id_fuente == 4:
      schema_name = self.dbconfig['dbinternacional'].upper() #on linux systems sql is case sensitive
    return schema_name

  def get_ftp_config(self):
    return (self.ftpconfig['user'], self.ftpconfig['pass'], self.ftpconfig['host'], self.ftpconfig['work_path'])

  def get_execution_mode(self):
    return self.exe_mode['modo_ejecucion']

  def get_auto_assign_catalog_mode(self):
    return self.exe_mode['auto_catalog']

  def get_mail_config(self):
    return self.mailconfig

  def get_git_branch_name(self):
    return self.gitconfig['branch'].lower()

class ExecutionStatus(metaclass = Singleton):
  """ Clase encargada de almacenar el estado de ejecución en todo momento
  de manera estática para que esté disponible en todas las secciones del código
  y funcione de manera transversal utiliza el patron Singleton"""
  __instance = None
  chunksize = 0
  chunk_counter = 0
  process_filename = ''
  phase = ''
  start_hour_file = 0
  end_hour_file = 0
  start_process_hour = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
  end_proccess_hour = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
  process_duration = 0
  file_duration = 0
  file_qty_rows = 0
  file_size = 0
  error_chunks_counter = 0
  qty_process_files = 0
  qty_ftp_files = 0
  qty_error_files = 0
  qty_discard_files = 0
  qty_archivos_procesados = 0 #almacena la cantidad de archivos procesados durante la ejecución
  qty_prod_updated_on_db = 0 # cantidad de productos actualizado en la base de datos
  qty_row_deleted_on_db = 0
  qty_cat_upd_file = 0  # cantidad registros actualizados en catalogo
  qty_cat_upd_db = 0    # cantidad registros actualizados en base de datos derivados del cambio en el catalogo
  qty_cat_del_file = 0  # cantidad registros borrados en catalogo
  qty_cat_del_db = 0    # cantidad registros borrados en base de datos derivados del cambio en el catalogo
  lot_id = 0
  tload = 0   #tiempo de carga minutos redondeado a dos decimales
  ttrans = 0  #tiempo de transformacion minutos redondeado a dos decimales
  textra = 0  #tiempo de extraccion minutos redondeado a dos decimales
  treport= 0  #tiempo de reporte minutos redondeado a dos decimales
  usr = ''
  val_act ='' #valor actual
  val_bef ='' #valor anterior
  responsable=''

  execution_mode = 'Normal' # Modo de ejecución normal, reanudar ejecución o actualizar información
  execution_mode_cat = ''
  execution_status = 0 # 0 = no está en ejecución, 1 = se encuentra en ejecución 

  def reset_execution_status(self):
    """función encargada de reiniciar los atributos de la clase porque esta no tiene un init()"""
    self.chunksize = 0
    self.chunk_counter = 0
    self.process_filename = ''
    self.phase = ''
    self.start_hour_file = 0
    self.end_hour_file = 0
    self.start_process_hour = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    self.end_proccess_hour = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    self.process_duration = 0
    self.file_duration = 0
    self.file_qty_rows = 0
    self.file_size = 0
    self.error_chunks_counter = 0
    self.qty_process_files = 0
    self.qty_ftp_files = 0
    self.qty_error_files = 0
    self.qty_discard_files = 0
    self.qty_archivos_procesados = 0 #almacena la cantidad de archivos procesados durante la ejecución
    self.qty_prod_updated_on_db = 0 # cantidad de productos actualizado en la base de datos
    self.qty_row_deleted_on_db = 0
    self.qty_cat_upd_file = 0  # cantidad registros actualizados en catalogo
    self.qty_cat_upd_db = 0    # cantidad registros actualizados en base de datos derivados del cambio en el catalogo
    self.qty_cat_del_file = 0  # cantidad registros borrados en catalogo
    self.qty_cat_del_db = 0    # cantidad registros borrados en base de datos derivados del cambio en el catalogo
    self.lot_id = 0
    self.tload = 0   #tiempo de carga minutos redondeado a dos decimales
    self.ttrans = 0  #tiempo de transformacion minutos redondeado a dos decimales
    self.textra = 0  #tiempo de extraccion minutos redondeado a dos decimales
    self.treport= 0  #tiempo de reporte minutos redondeado a dos decimales
    self.usr = ''
    self.val_act ='' #valor actual
    self.val_bef ='' #valor anterior
    self.responsable=''

    self.execution_mode = 'Normal' # Modo de ejecución normal, reanudar ejecución o actualizar información
    self.execution_mode_cat = ''
    self.execution_status = 0 # 0 = no está en ejecución, 1 = se encuentra en ejecución 


  def get_status(self):
    return {"chunksize": self.chunksize, "process_filename": self.process_filename,
     "chunk_counter":self.chunk_counter}

  def set_chunksize(self, chunksize):
    self.chunksize = chunksize

  def calculate_time_difference_file(self):
    FMT = "%Y-%m-%d %H:%M:%S"
    self.file_duration =  datetime.strptime(self.end_hour_file, FMT) - datetime.strptime(self.start_hour_file, FMT)
    self.file_duration = self.file_duration.seconds / 60 #duracion en minutos

  def calculate_time_difference_process(self):
    FMT = "%Y-%m-%d %H:%M:%S"
    self.process_duration =  datetime.strptime(self.end_proccess_hour, FMT) - datetime.strptime(self.start_process_hour, FMT)
    self.process_duration = self.process_duration.seconds / 60 #duracion en minutos

  def set_lot_id(self, stats_folder):
    if os.path.exists(stats_folder + 'estadisticas_proceso_ejecución.csv'):
      self.lot_id = str(self.count_row_on_file(stats_folder, 'estadisticas_proceso_ejecución.csv' ))

  def write_file_stats(self, stats_folder):
    elements = [str(self.lot_id),self.process_filename, str(self.start_hour_file), str(self.end_hour_file), str(round(self.file_duration,2)),
      str(self.chunksize), str(self.chunk_counter), str(self.error_chunks_counter), str(self.file_qty_rows), str(round(self.file_size,2)),
      self.execution_mode, str(round(self.textra,2)), str(round(self.ttrans,2)), str(round(self.tload,2)), str(round(self.treport,2)), str(self.qty_prod_updated_on_db)]
    string = ";".join(elements) + '\n'
    if os.path.exists(stats_folder + 'estadisticas_archivo_ejecución.csv'):
      with open(stats_folder + 'estadisticas_archivo_ejecución.csv', 'a') as stats_file:
        stats_file.write(string)
    else:
      with open(stats_folder + 'estadisticas_archivo_ejecución.csv', 'w') as stats_file:
        stats_file.write("id_proceso;nombre_archivo;fecha_inicio;fecha_fin;duracion_en_minutos;tamano_particion;cantidad_part_procesada;cantidad_part_descartadas;" + 
          "cantidad_filas;tamano_archivo;modo_ejecucion;tiempo_extraccion;tiempo_transformacion;tiempo_carga;tiempo_reporte;prod_actualizados\n")
        stats_file.write(string)

  def write_process_stats(self, stats_folder):
    elements = [str(self.lot_id), str(self.start_process_hour), str(self.end_proccess_hour), str(self.process_duration),
      str(self.qty_ftp_files), str(self.qty_process_files),str(self.qty_error_files), str(self.qty_discard_files), self.execution_mode]
    string = ";".join(elements) + '\n' 
    if os.path.exists(stats_folder + 'estadisticas_proceso_ejecución.csv'):
      with open(stats_folder + 'estadisticas_proceso_ejecución.csv', 'a') as stats_file:
        stats_file.write(string)
    else:
      with open(stats_folder + 'estadisticas_proceso_ejecución.csv', 'w') as stats_file:
        stats_file.write("id_lote;fecha_inicio;fecha_fin;duracion_en_minutos;archivos_ftp;archivos_locales;cantidad_archivos_error;archivos_descartados;modo_ejecucion\n")
        stats_file.write(string)

  def write_catalog_stats(self, stats_folder):
    elements = [str(self.process_filename),  str(self.start_hour_file), str(self.end_hour_file), str(round(self.file_duration,2)),str(self.qty_cat_upd_file),
      str(self.qty_cat_upd_db),str(self.qty_cat_del_file),str(self.qty_cat_del_db), self.execution_mode_cat, self.usr, self.val_act, self.val_bef,self.responsable]
    string = ";".join(elements) + '\n'
    if os.path.exists(stats_folder + 'estadisticas_catalogos.csv'):
      with open(stats_folder + 'estadisticas_catalogos.csv', 'a') as stats_file:
        stats_file.write(string)
    else:
      with open(stats_folder + 'estadisticas_catalogos.csv', 'w') as stats_file:
        stats_file.write("nombre_catalogo;fecha_inicio;fecha_fin;duracion;reg_actualizados_archivo;reg_actualizados_db;reg_borrado_archivo;reg_borrado_db;accion;usuario;valor_actualizado;val_borrado;modo_ejecucion\n")
        stats_file.write(string)

  def get_lot_id(self, stats_folder):
    """Función encargada de obtener el id de proceso actual para uso en la logica del programa"""
    lot_id = 0
    if os.path.exists(stats_folder + 'estadisticas_proceso_ejecución.csv'):
      lot_id = str(self.count_row_on_file(stats_folder, 'estadisticas_proceso_ejecución.csv' ))
    return lot_id


  def blocks(self, files, size=65536):
    """Helper function that reads a chunk of a file and returns it"""
    while True:
      b = files.read(size)
      if not b: break
      yield b

  def count_row_on_file(self, file_folder, process_filename=None):
    if process_filename is None:
      process_filename = self.process_filename
    with open(file_folder + process_filename, "r",encoding="utf-8",errors='ignore') as f:
      return sum(bl.count("\n") for bl in self.blocks(f))


class MailClass():
  """this class implements the operation to send emails reporting execution status and logs"""
  def __init__(self, mail_config):
    self.smpt = mail_config['smtp_server']
    self.port = mail_config['smtp_port']
    self.usr = mail_config['sender_user']
    self.pwd = mail_config['mail_pwd']
    self.adm_usr = mail_config['reciev_useradm'].split(";")
    self.op_usr = mail_config['reciev_userop'].split(";")
    self.nls_usr = mail_config['reciev_usernls'].split(";")
  
  def  send_mail(self, subject, template_message, exe_level, attach_folder_path):
    with smtplib.SMTP(host = self.smpt, port=self.port) as s:
      try:
        s.starttls()
        s.login(self.usr, self.pwd)
      except Exception as e:
        #Nutresa's smtp server doesn'trequire authentication then this could change
        pass

      try:
        msg = MIMEMultipart()
        message = template_message
        msg['From']=self.usr
        #TODO sanitize sent users To and from
        if exe_level=="INFO":
          msg['To']=",".join(self.op_usr + self.nls_usr)
          msg['CC']=str(self.adm_usr)
        elif exe_level in ["ERROR", "CRITICAL"]:
          msg['To']=",".join(self.adm_usr)
        msg['Subject'] = subject

        for info in os.listdir(attach_folder_path):
          pathtofile = attach_folder_path + info
          part = MIMEBase('application', "octet-stream")
          with open(pathtofile,"rb") as myfile:
            part.set_payload(myfile.read())
            encoders.encode_base64(part)
            part.add_header('Content-Disposition', 'attachment; filename="'+ os.path.basename(ud.unidecode(pathtofile)) +'"')
            msg.attach(part)
        msg.attach(MIMEText(message))
        s.send_message(msg)
      except Exception as e:
        print("Ocurrio  un error durante el envio de correo revisar conexión a internet y servidor SMTP")
        print(e)


class ProductInfo(metaclass = Singleton):
  """this class is in charge of adquiring all product info on one place so it keeps all products on memory and does not have to ask
   each chunk iteration for all products on products tables it implements read tables on create that returns a dataframe"""
  __df_product_sku = pd.DataFrame()
  __df_grouped_product = pd.DataFrame()
  __flag_need_refresh_sku=True
  __flag_need_refresh_grouped=True
  __last_id_source = -1

  def get_sku_product(self):
    return self.__df_product_sku

  def get_grouped_product(self):
    return self.__df_grouped_product

  def set_flag_need_refresh_sku(self, value):
    self.__flag_need_refresh_sku = value

  def get_flag_need_refresh_sku(self):
    return self.__flag_need_refresh_sku

  def set_flag_need_refresh_grouped(self, value):
    self.__flag_need_refresh_grouped = value

  def get_flag_need_refresh_grouped(self):
    return self.__flag_need_refresh_grouped

  def update_sku_product(self, db_connection, schema_name, id_source):
    self.__last_id_source = id_source
    self.__df_product_sku = pd.read_sql(sql="select * from %s.producto_sku where activo=TRUE" % schema_name, con=db_connection);

  def update_grouped_product(self, db_connection, schema_name, id_source):
    self.__last_id_source = id_source
    self.__df_grouped_product = pd.read_sql(sql="select * from %s.producto_agrupado where activo=TRUE" % schema_name, con=db_connection);

  def get_last_id_source(self):
    return self.__last_id_source