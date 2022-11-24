import pandas as pd
from exceptions import LoadingDatabaseError
import logging

class StatsOps():
  roo_logger = None

  def __init__(self):
    self.roo_logger = logging.getLogger('load.load_stats')
    pass

  def load_data(self, path_folder, engine,schema_name, mode='replace'):

    mode = mode.lower()
    statsfiles =['estadisticas_archivo_ejecución.csv', 'estadisticas_proceso_ejecución.csv', 'estadisticas_catalogos.csv',
    'resumen_validaciones_por_procesos.csv']
    dfs = []
    for stats in statsfiles:
      try:
        df = pd.read_csv(path_folder+stats, sep=';', encoding="latin") 
        dfs.append((df, stats))
      except Exception as e:
        self.roo_logger.error(e)
        pass

    self.roo_logger.info('carga iniciada')

    with engine.begin() as conn:
      for df, filename in dfs:
        table_name = ''
        if filename == 'estadisticas_archivo_ejecución.csv':
          table_name ='file_stats'
        elif filename == 'estadisticas_proceso_ejecución.csv':
          table_name = 'process_stats'
        elif filename == 'estadisticas_catalogos.csv':
          table_name = 'catalog_stats'
        elif filename == 'resumen_validaciones_por_procesos.csv':
          table_name = 'validaciones_stats'
        else:
          continue

        df.to_sql(table_name, schema=schema_name, con=conn, if_exists=mode, index=False)

    self.roo_logger.info('carga finalizada')

  def move_data_to_csvfile(self, path_folder, conn, schema_name):
    dbtables =['file_stats', 'process_stats', 'catalog_stats', 'validaciones_stats']
  
    for table_name in dbtables:
      filename = ''
      if table_name == 'file_stats':
        filename = 'estadisticas_archivo_ejecución.csv'
      elif table_name == 'process_stats':
        filename = 'estadisticas_proceso_ejecución.csv'
      elif table_name == 'catalog_stats':
        filename = 'estadisticas_catalogos.csv'
      elif table_name == 'validaciones_stats':
        filename = 'resumen_validaciones_por_procesos.csv'
      else:
        continue
      self.roo_logger.info("lectura archivo")
      try:
        df = pd.read_sql("select * from %s.%s" % (schema_name, table_name), conn)
        df.to_csv(path_folder+filename, index=False, sep=";", encoding="latin", header=True)
        self.roo_logger.info("finaliza lectura archivo de estadisticas %s" % filename)
      except Exception as e:
        self.roo_logger.error(schema_name)
        self.roo_logger.error(e)
        continue



