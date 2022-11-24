from string import Template
from datetime import datetime

mail_text_ok_status = Template("""
  Buen@s $jornada

  El proceso de carga de insumos Nielsen terminó de manera correcta, se adjuntan archivos de control de ejecución.
  Adicionalmente las siguientes estadísticas:

  Cantidad de Archivos procesados: $processed_files
  Cantidad de Archivos con errores: $qty_error_files
  Hora inicio de ejecución: $start_hour
  Hora fin de ejecución: $end_hour

  Gracias por la atención prestada

  PD: En caso de inquietud, favor no responda este correo, contacte al administrador del sistema

""")

mail_text_error_status = Template("""
  Buen@s $jornada

  Ocurrió un error crítico durante la ejecución del proceso de carga de archivos Nielsen a la base de datos,
  revisar archivos adjuntos, para realizar trazabilidad del error y relanzar el proceso (resumir o recargar)

  Gracias por la atención prestada

  """)

mail_text_resume_error_status = Template("""
  Buen@s $jornada

  Se generó un error durante el proceso de resumir ejecución de archivos Nielsen, revisar archivos adjuntos
  para trazabilidad del error. Se adjunta error

  $error

  Gracias por la atención prestada

  Este correo se generó de forma automática. Favor no responder

  """)

mail_text_reprocess_error_status = Template("""
  Buen@s $jornada

  El proceso de recarga de insumos finalizó con un error crítico Se adjunta error

  $error

  Gracias por la atención prestada

  Este correo se generó de forma automática. Favor no responder
  """)

mail_text_nls_ok_status = Template("""
  Buen@s $jornada

  El proceso de publicación de archivos al servidor FTP fue realizado con éxito, se adjuntan archivos de control de ejecución.
  Adicionalmente las siguientes estadísticas:

  Cantidad de Archivos publicados: $processed_files
  Hora inicio de ejecución: $start_hour
  Hora fin de ejecución: $end_hour

  Gracias por la atención prestada

  Feliz $jornada

  PD: En caso de inquietud, favor no responda este correo, contacte al administrador del sistema


  """)

def get_mail_ok_message(exestatus):
  FMT = "%Y-%m-%d %H:%M:%S"
  end_hour = datetime.strptime(exestatus.end_proccess_hour, FMT).hour
  if end_hour < 12:
    jornada = "días"
  elif end_hour > 12 and end_hour < 19:
    jornada = "tardes"
  else:
    jornada = "noches"

  procesados = exestatus.qty_process_files + exestatus.qty_ftp_files
  error = exestatus.qty_error_files
  start_date = exestatus.start_process_hour
  end_date = exestatus.end_proccess_hour
  return mail_text_ok_status.substitute(jornada=jornada, processed_files=procesados, qty_error_files=error,
    start_hour = start_date, end_hour = end_date)

def get_mail_nls_ok_message(exestatus):
  FMT = "%Y-%m-%d %H:%M:%S"
  end_hour = datetime.strptime(exestatus.end_proccess_hour, FMT).hour
  if end_hour < 12:
    jornada = "días"
  elif end_hour > 12 and end_hour < 19:
    jornada = "tardes"
  else:
    jornada = "noches"

  procesados = exestatus.qty_process_files + exestatus.qty_ftp_files
  start_date = exestatus.start_process_hour
  end_date = exestatus.end_proccess_hour
  return mail_text_nls_ok_status.substitute(jornada=jornada, processed_files=procesados,
    start_hour = start_date, end_hour = end_date)

def get_mail_fail_message(exestatus):
  FMT = "%Y-%m-%d %H:%M:%S"
  end_hour = datetime.strptime(exestatus.end_proccess_hour, FMT).hour
  if end_hour < 12:
    jornada = "días"
  elif end_hour > 12 and end_hour < 19:
    jornada = "tardes"
  else:
    jornada = "noches"

  return mail_text_error_status.substitute(jornada = jornada)  

def get_mail_fail_resume_message(exestatus, exeception_message):
  FMT = "%Y-%m-%d %H:%M:%S"
  end_hour = datetime.now().hour
  if end_hour < 12:
    jornada = "días"
  elif end_hour > 12 and end_hour < 19:
    jornada = "tardes"
  else:
    jornada = "noches"

  return mail_text_resume_error_status.substitute(jornada=jornada, error= exeception_message)

def get_mail_fail_reprocess_message(exestatus, exeception_message):
  FMT = "%Y-%m-%d %H:%M:%S"
  end_hour = datetime.now().hour
  if end_hour < 12:
    jornada = "días"
  elif end_hour > 12 and end_hour < 19:
    jornada = "tardes"
  else:
    jornada = "noches"

  return mail_text_reprocess_error_status.substitute(jornada=jornada, error= exeception_message)
