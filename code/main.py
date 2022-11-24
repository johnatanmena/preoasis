  # -*- coding: utf-8 -*-
from __future__ import print_function, unicode_literals
from pyfiglet import Figlet
from pprint import pprint
from PyInquirer import style_from_dict, prompt, Token,Separator
from PyInquirer import Validator, ValidationError
import os
import pandas as pd
import utils
import reporting
import logging
import extract.input_validations as iv
from datetime import datetime
import util_constant  #strings for mail message
from transversal_classes import ProjectParameters
from transversal_classes import DB_Connect
from transversal_classes import ExecutionStatus
from transversal_classes import MailClass
from exceptions import InvalidCatalogException
import pdb

#file which implements the user interaction with te script in a more 
#friendly way
f = Figlet(font = 'slant')
print(f.renderText("OASIS 2.0"))
print("""Bienvenido a OASIS, programa de carga de datos a repositorio central.""")

#############################################################
###########################CREATE STYLES ####################
#############################################################

custom_style_2 = style_from_dict({
    Token.Separator: '#6C6C6C',
    Token.QuestionMark: '#FF9D00 bold',
    Token.Selected: '#673AB7 bold',  # default
    Token.Pointer: '#FF9D00 bold',
    Token.Instruction: '',  # default
    Token.Answer: '#2196f3 bold',
    Token.Question: '',
})


def nut_load_nielsen_data_to_db():
  roo_logger = logging.getLogger('')
  roo_logger.info('Comienza lectura de parámetros archivo JSON')
  params = ProjectParameters()
  exe_mode = DB_Connect().get_execution_mode()
  mail_obj = MailClass(DB_Connect().get_mail_config())
  roo_logger.info('Se define el modo de ejecucion con usuario '+ exe_mode)
  exestatus = ExecutionStatus()
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
  except InvalidCatalogException as e:
    roo_logger.error("Ocurrio un error durante la recepción de catálogos")
    roo_logger.error(e.message)
    return # error en la ejecución
  utils.main(params, mail_obj)
  utils.create_report(params)
  exestatus.end_proccess_hour = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
  exestatus.calculate_time_difference_process()
  exestatus.write_process_stats(params.STATS_FOLDER)
  try:
    mail_obj.send_mail("Ejecución Finalizada", util_constant.get_mail_nls_ok_message(exestatus), "INFO", params.LOG_FILES)
  except:
    roo_logger.error("No se pudo enviar el correo de ejecución de finalización, revisar parámetros y puertos de la herramienta")
  utils.load_stats_to_db(params.STATS_FOLDER, 'replace')
  roo_logger.info('Registrar tiempo de finalización de ejecución')

def nls_publish_nielsen_data():
  roo_logger = logging.getLogger('')
  roo_logger.info('Comienza lectura de parámetros archivo JSON')
  params = ProjectParameters()
  exe_mode = DB_Connect().get_execution_mode()
  mail_obj = MailClass(DB_Connect().get_mail_config())
  roo_logger.info('Se define el modo de ejecucion con usuario '+ exe_mode)
  exestatus = ExecutionStatus()
  iv.validate_input(params.INPUT_FILES, params.OUTPUT_GRAPHS_FOLDER, params.STATS_FOLDER, params.INPUT_STRING_COLUMNS, 0, exe_mode)
  utils.read_and_publish_oasis_inputs_and_catalogs() #Nielsen
  roo_logger.info('Ejecución finalizada')

####################################################################################################################
###################################### funciones auxiliares del menu ###############################################
####################################################################################################################

class CatalogNameValidator(Validator):
  def validate(self, document):
    params = ProjectParameters()
    if document.text.upper() not in [x.upper() for x in os.listdir(params.CATALOG_FILES)]:
      raise ValidationError(
        message = 'el catalogo "%s" no existe en la carpeta de catalogos "%s"' % (document.text, params.CATALOG_FILES),
        cursor_position = len(document.text)
      )

class GrupoNielsenValidator(Validator):
  def validate(self, document):
    params = ProjectParameters()
    if document.text.upper() not in ["NIELSEN", "NUTRESA"]:
      raise ValidationError(
        message = 'El valor debe ser Nielsen o Nutresa',
        cursor_position = len(document.text)
      )

class ItemVolumenValidator(Validator):
  def validate(self, document):
    params = ProjectParameters()
    if document.text.upper() not in [x.upper() for x in os.listdir(params.TEMP_INPUT_FILES)]:
      raise ValidationError(
        message = 'El archivo de Item Volumen: "%s" no se encuentra en la carpeta temporal "%s"' % (document.text, params.TEMP_INPUT_FILES),
        cursor_position= len(document.text)
      )
    else:
      df_dict = pd.read_excel(params.TEMP_INPUT_FILES+ document.text, sheet_name=None, header=2)
      cols = ['GALLETAS', 'CAFE MOLIDO', 'CAFE SOLUBLE', 'HELADOS', 'PASTAS', 'MODIFICADORES LECHE', 'CHOCOLATINAS',
      'MANI', 'CARNES FRIAS', 'CHOCOLATE MESA', 'CARNICOS CONSERVA', 'VEGETALES CONSERVA', 'CEREALES BARRA'] #hojas quemadas revisar el tema 
      for sheet_name, sheet in df_dict.items():
        if sheet_name.upper() not in cols:
          raise ValidationError(
            message = 'El archivo no cuenta con las hojas necesarias para realizar el análisis (%s)' %(sheet_name),
            cursor_position= len(document.text)
          )
        if len(set(['TAG', 'DESC', 'TAMANOS']).difference(set(sheet.columns))) != 0 and \
        len(set(['PESO']).difference(set(sheet.columns))) != 0 and \
        len(set(['CONTENIDO']).difference(set(sheet.columns))) != 0 and \
        len(set(['PESO TOTAL', 'DESCRIPCION']).difference(set(sheet.columns))) != 0 and\
        len(set(['TAMANO']).difference(set(sheet.columns))) != 0:
          raise ValidationError(
            message = 'la hoja %s no cuenta con las columnas necesarias para el análisis' %(sheet_name),
            cursor_position= len(document.text)
          )


class DateFormatValidator(Validator):
  def validate(self, document):
    try:
      datetime.strptime(document.text,"%d-%m-%Y")
    except ValueError as err:
      raise ValidationError(
          message = "La fecha ingresada no cumple con los requisitos de formato",
          cursor_position = len(document.text)
      )

class StatsValidator(Validator):
  """docstring for StatsValidator"""
  def validate(self, document):
    if document.text.upper() not in ['S', 'A']:
      raise ValidationError(
        message=' Los valores válidos son sobreescribir (S) o anexar (A)',
        cursor_position=len(document.text)
      )

class DictionaryValidator(Validator):
  def validate( self, document):
    params = ProjectParameters()
    if document.text.upper() not in [x.upper() for x in os.listdir(params.TEMP_INPUT_FILES)]:
      raise ValidationError(
        message = 'El archivo de Diccionario: "%s" no se encuentra en la carpeta temporal "%s"' % (document.text, params.TEMP_INPUT_FILES),
        cursor_position= len(document.text)
      )

class YearValidator(Validator):
  def validate( self, document):
    if not(document.text.isnumeric() and int(document.text)>=1800 and int(document.text)<=2100):
      raise ValidationError(
        message = 'Año no válido para generar vista',
        cursor_position= len(document.text)
      )

class MonthValidator(Validator):
  def validate( self, document):
    if not(document.text.isnumeric() and int(document.text)>=1 and int(document.text)<=12):
      raise ValidationError(
        message = 'Mes no válido para generar vista introduzca valores entre 1 y 12',
        cursor_position= len(document.text)
      )  

def showConfirmMessage(answer):
  if answer['principal'] == 'Salir':
    return False
  elif 'more_options' in answer.keys():
    if answer['more_options'] == 'Reprocesar carga':
      return True
  else:
    return False
  return False


####################################################################################################################
################################################ preguntas #########################################################
####################################################################################################################
questions_nielsen = [
  {
    'type': 'list',
    'name': 'principal',
    'message': '¿Que accion desea realizar?',
    'choices': [
        'Cargar Datos Nielsen A FTP',
        'Actualizar catálogos',
        {
          'name': 'Mas Opciones',
          'disabled': 'De momento no disponible'
        },
        'Crear Datacheck',
        Separator(),
        'Salir',
    ]
  },
  {
    'type': 'input',
    'name': 'old_val',
    'message':'Nombre de la base antigua "OLD" (incluir extension ".csv")',
    'when':lambda answer: answer['principal']=='Crear Datacheck',
    'validate': DictionaryValidator
  },
  {
    'type': 'input',
    'name': 'new_val',
    'message':'Nombre de la nueva base "NEW" (incluir extension ".csv")',
    'when':lambda answer: answer['principal']=='Crear Datacheck',
    'validate': DictionaryValidator
  }

]

#opciones de preguntas
questions = [
  {
    'type': 'list',
    'name': 'principal',
    'message': '¿Que accion desea realizar?',
    'choices': [
        'Cargar Datos Nielsen A Base de Datos',
        'Actualizar catálogos',
        'Generar reporte de ejecucion',
        {
          'name': 'Mas Opciones',
        },
        'Opciones estadisticas',
        Separator(),
        'Salir',
    ]
  },
  {
    'type': 'input',
    'name': 'old_val',
    'message':'Ingrese valor a modificar',
    'when':lambda answer: answer['principal']=='Actualizar catálogos'
  },
  {
    'type': 'input',
    'name': 'new_val',
    'message':'Ingrese El nuevo valor',
    'when':lambda answer: answer['principal']=='Actualizar catálogos'
  },
  {
    'type': 'input',
    'name': 'rep_date_from',
    'message': 'Introduzca fecha inicial desde(dd-mm-yyyy):',
    'when': lambda answer: answer['principal']=='Generar reporte de ejecucion',
    'validate': DateFormatValidator
  },
  {
    'type': 'input',
    'name': 'rep_date_to',
    'message': 'Introduzca fecha final hasta(dd-mm-yyyy):',
    'when': lambda answer: answer['principal']=='Generar reporte de ejecucion',
    'validate': DateFormatValidator
  },
  {
    'type': 'input',
    'name': 'col_update',
    'message':'Columna Actualizar (Nielsen/Nutresa)',
    'when':lambda answer: answer['principal']=='Actualizar catálogos',
    'validate': GrupoNielsenValidator
  },
  {
    'type': 'input',
    'name': 'catalog_name',
    'message':'Nombre del catálogo a actualizar (incluir extension ".csv")',
    'when':lambda answer: answer['principal']=='Actualizar catálogos',
    'validate': CatalogNameValidator
  },
  {
    'type': 'list',
    'name': 'more_options',
    'message': 'Mas opciones...',
    'choices':[
      'Reprocesar carga',
      'Procesar novedades',
      {'name':'Borrar valor',
      #'disabled':'Actualmente en pruebas'
      },
      'Publicar vista Nielsen',
      'Actualizar información del Item Volumen',
      'Leer diccionarios de datos (archivos .xml)',
      'Volver menu anterior'
    ],
    'when': lambda answer: answer['principal']=='Mas Opciones'
  },
  {
      'type':'list',
      'name':'stats',
      'message': 'Opciones de estadisticas',
      'choices':[
        'Cargar archivos de estadisticas a base de datos',
        'Leer base de datos a archivos de estadisticas',
        'Volver Menu Anterior'
      ],
      'when': lambda answer: answer['principal']=='Opciones estadisticas'
  },
  {
    'type': 'confirm',
    'name': 'reload',
    'message': '¿Desea iniciar este proceso?(eliminará información transaccional del archivo de la base de datos)',
    'when': lambda answer: showConfirmMessage(answer)
  }
]

del_questions = [
  {
    'type': 'input',
    'name': 'del_from_catalog_name',
    'message':'Nombre del catálogo a modificar (incluir extension ".csv")',
    'validate': CatalogNameValidator
  },
  {
    'type': 'input',
    'name': 'del_old_val',
    'message':'Ingrese valor a borrar',
  }

]

stats_questions =  [
  {
    'type':'input',
    'name':'load_stats_mode',
    'message': '¿Desea sobreescribir o anexar los datos de estadisticas en la base de datos?(S / A)',
    'validate': StatsValidator
  }
]

refresh_item_volumen_questions = [
  {
    'type': 'input',
    'name': 'item_volumen',
    'message':'Nombre del archivo de Item Volumen (Incluir Extensión)',
    'validate': ItemVolumenValidator
  },
  {
    'type': 'confirm',
    'name': 'item_confirm',
    'message': 'Desea cargar la información de este Item Volumen? se borrara la información anterior.'
  }
]

refresh_dictionary_questions = [
  {
    'type': 'input',
    'name': 'dictionary',
    'message':'Nombre del archivo de diccionarios(.xml)',
    'validate': DictionaryValidator
  },
  {
    'type': 'confirm',
    'name': 'dict_confirm',
    'message': 'Desea cargar la información de este diccionario? se borrara la información anterior.'
  }
]

publish_views_questions = [
  {
    'type': 'input',
    'name': 'year_view',
    'message':'Introduzca año a generar, en formato numérico (yyyy)',
    'validate': YearValidator
  },
  {
    'type': 'input',
    'name': 'month_view',
    'message':'Introduzca mes a generar, en formato numérico (1 - 12)',
    'validate': MonthValidator
  }
]

del_confirm_questions = [
  {
    'type': 'confirm',
    'name':'del_confirm',
    'message':"mensaje confirmacion defecto"
  }

]


####################################################################################################################
######################################### logica funcional del menu  ###############################################
####################################################################################################################
#print("------------- preparando ambiente de ejecucion -----------------")
#ret = utils.safely_get_last_repo_version('..')
#if ret != 0:
#  print("El repositorio no se pudo actualizar de manera automatica: ejecutar un \'git status\' y realizar las operaciones necesarias en git para actualizarlo")
#  exit()

exe_mode = DB_Connect().get_execution_mode()
if exe_mode == 'Nielsen':
  answers = prompt(questions_nielsen, style=custom_style_2)
elif exe_mode=='Nutresa':
  answers = prompt(questions, style=custom_style_2)



action = answers['principal']
params = ProjectParameters()
list_files = os.listdir(params.INPUT_FILES)
while action!='Salir':
  #pprint(answers)
  if action == 'Cargar Datos Nielsen A Base de Datos':
    #print("Proceso de carga")
    nut_load_nielsen_data_to_db()
  elif action == 'Actualizar catálogos':
    old_val = answers['old_val']
    new_val = answers['new_val']
    infile  = answers['catalog_name']
    user =    answers['col_update']
    path_to_catalog = params.CATALOG_FILES
    #print("Se actualiza catálogo", old_val, new_val, infile)
    utils.update_catalog(infile, path_to_catalog, old_val, new_val, user)
  elif action == 'Cargar Datos Nielsen A FTP':
    nls_publish_nielsen_data()
    #print("Se publica información de catálogos")
  elif action == 'Generar reporte de ejecucion':
    date_from = answers['rep_date_from']
    date_to = answers['rep_date_to']
    print("generando reporte de ejecucion entre: %s - %s" % (date_from, date_to))
    utils.create_dashboard(date_from, date_to, params)
  elif action == 'Crear Datacheck':
    name_old = answers['old_val']
    name_new = answers['new_val']
    print('Generando Datacheck...')
    utils.create_datacheck(params, name_new, name_old)
    print('Ejecución finalizada...')
  elif action == 'Mas Opciones':
    if answers['more_options'] == 'Reprocesar carga' and answers['reload']:
      print("Reprocesando carga...")
      utils.reprocess_load(params, list_files)
    elif answers['more_options'] == 'Procesar novedades':
      print("Procesando novedades...")
      utils.resume_execution()
    elif answers['more_options'] == 'Publicar vista Nielsen':
      view_answer = prompt(publish_views_questions, style=custom_style_2)
      print("Publicando vistas a S3")
      utils.publish_views_to_s3(view_answer['year_view'], view_answer['month_view'])
    elif answers['more_options'] == 'Volver menu anterior':
      pass
    elif answers['more_options'] == 'Borrar valor':
      del_answers = prompt(del_questions, style=custom_style_2)
      del_confirm_questions[0]['message'] = '¿Desea eliminar el valor "%s" del catalogo "%s"?' % (del_answers['del_old_val'], del_answers['del_from_catalog_name'])
      del_confirm = prompt(del_confirm_questions, style=custom_style_2)
      if del_confirm['del_confirm']:
        print('')
        utils.delete_data_from_catalog(del_answers['del_from_catalog_name'], params.CATALOG_FILES, del_answers['del_old_val'], exe_mode)
    if answers['more_options'] == 'Actualizar información del Item Volumen':
      item_answers = prompt(refresh_item_volumen_questions, style=custom_style_2)
      if item_answers['item_confirm']:
        print('modificando la información del Item Volumen')
        utils.refresh_item_volumen_info(item_answers['item_volumen'], params.TEMP_INPUT_FILES)
    if answers['more_options'] == 'Leer diccionarios de datos (archivos .xml)':
      item_answers = prompt(refresh_dictionary_questions, style=custom_style_2)
      if item_answers['dict_confirm']:
        print('modificando la información del Diccionario de %s' % item_answers['dictionary'])
        utils.refresh_dict_info(item_answers['dictionary'], params.TEMP_INPUT_FILES)
  elif action == 'Opciones estadisticas':
    if answers['stats'] == 'Cargar archivos de estadisticas a base de datos':
      stats_answers = prompt(stats_questions,style=custom_style_2)
      if stats_answers['load_stats_mode'] == 'A':
        utils.load_stats_to_db(params.STATS_FOLDER, 'replace')
      elif stats_answers['load_stats_mode'] == 'S':
        #TODO ADD CONFIRMATION MESSAGE
        utils.load_stats_to_db(params.STATS_FOLDER, 'replace')
    elif answers['stats'] == 'Leer base de datos a archivos de estadisticas':
      utils.load_stats_to_file(params.STATS_FOLDER)
      print("lectura")
      pass
    elif answers['stats'] == 'Volver Menu Anterior':
      pass

  if exe_mode == 'Nielsen':
    answers = prompt(questions_nielsen, style=custom_style_2)
  elif exe_mode=='Nutresa':
    answers = prompt(questions, style=custom_style_2)
  action = answers['principal']
  pass

#print("------------- actualizando versión server -----------------")
#ret = utils.safely_set_last_repo_version('..')
#if  ret != 0:
#  print("validar estado del repositorio no pudo ser actualizado")

