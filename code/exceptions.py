from fuzzywuzzy import process
import fuzzywuzzy as fuzz
import os.path as path
from transversal_classes import ProjectParameters 


def fuzzyDistanceSearchKey(strSearched, listPossibleString):
  """función que permite identificar dada una cadena y una lista la cadena mas parecida y su puntuación"""
  highestMatched = process.extractOne(strSearched, listPossibleString) #, scorer = "fuzz.partial_ratio") #se comenta para verificar cual da mejor resultado
  return highestMatched

#TODO add aditional parameter seccion to identify in wich section the error was caused
class Status(Exception):
  """Clase de tipo Excepcion que se encarga de almacenar el estado de ejecución en un archivo en caso de error
  el archivo se encontrará en la carpeta de logs con el nombre de status.csv, el cual contiene el ultimo estado conocido
  en el cual el proceso genera un error"""
  def __init__(self, filename, chunk_counter, message):
    super(Status, self).__init__()
    params = ProjectParameters()
    if path.exists(params.LOG_FILES + 'status.csv'):
      with open(params.LOG_FILES + 'status.csv', 'a') as error_file:
        error_file.write(filename + ';' + str(chunk_counter) + ';' + 'por definir' + ';' + message+
          ';'+ str(params.chunksize) + '\n')  
    else:
      with open(params.LOG_FILES + 'status.csv', 'w') as error_file:
        error_file.write('archivo;particion;seccion;mensaje;tamano_particion\n')
        error_file.write(filename + ';' + str(chunk_counter) + ';' + 'por definir' + ';' + message+
          ';'+ str(params.chunksize) + '\n')



class ExtractException(Status):
  error_df = None
  filename = None
  column_name = None
  """Excepción lanzada en la fase de extracción del proceso ETL
  debe escribir el status en un archivo de configuración"""
  def __init__(self, message, chunk_counter, filename, dataframe, listPossibleString, catalog_name):
    super(ExtractException, self).__init__(filename, chunk_counter, message)
    #buscar sugerencias de posibles resultados
    dataframe['SUGERENCIAS'] = dataframe.iloc[:,0].apply(lambda x: fuzzyDistanceSearchKey(x, listPossibleString))
    #then separate the sugerencias field in multiple columns
    column_list = ["Sugerencia", "PorcentajeCoincidencia"]
    for n, col in enumerate(column_list):
      dataframe[col] = dataframe['SUGERENCIAS'].apply(lambda x : x[n])
    dataframe = dataframe.drop('SUGERENCIAS', axis=1)
    self.error_df = dataframe
    self.filename = filename
    self.column_name = catalog_name

  """funcion para exportar el dataframe de error """
  def export_dataframe(self, path_to_df):
    catalog_name = "ERR_CAT_OASIS_" + self.column_name + ".csv"
    self.error_df.to_csv(path_to_df + catalog_name, sep=';', encoding='latin', index=False)

  def __str__(self):
    return """Error porque no todas las claves del campo analizado cruzaron con el archivo de catálogos
    correspondiente a la columna, revisar los archivos de catalogos o verificar que la versión de
    los catalogos en el repositorio de Git se encuentre actualizada"""

class FileStructureException(Status):
  """FileStructureException, recieves the cols_a parameter wich indicates files that should be present on the
  file but aren't and files on cols_b files tha are present but shouldn't"""
  def __init__(self, filename, chunk_counter, message, cols_a, cols_b):
    super(FileStructureException, self).__init__(filename, chunk_counter, message)
    self.cols_a = cols_a
    self.cols_b = cols_b

  def __str__(self):
    return """Error causado porque las columnas no coinciden con las esperadas en el proceso, faltan columnas
    esperadas en el insumo (%s) y columnas que presentes en el insumo que no son esperadas (%s)  """ \
    % (self.cols_a, self.cols_b)

class InvalidCatalogException(Exception):
  """docstring for InvalidCatalogException"""
  def __init__(self, message):
    super(InvalidCatalogException, self).__init__()
    self.message = message
    self.__str__()

  def __str__(self):
    return """Ocurrió un error en la validación del catálogo que contiene valores duplicados, eliminar estos
    casos del archivo para poder continuar la ejecución"""

class DuplicateTagsException(Status):
  """Duplicate Tags Exception is an exception that is launched when are duplicate tags on the database and
  creates double transaction records"""
  def __init__(self, filename, chunk_counter, message):
    super(DuplicateTagsException, self).__init__(filename, chunk_counter, message)

  def __str__(self):
    return """Error durante la asignación de ID's de la base de datos ocurre porque existen elementos duplicados
    en algunas tablas maestras."""  
  
class AdditionalColumnsException(Status):
  """AdditionalColumnsException is launched when there are more columns than espected on the transform op"""
  def __init__(self, filename, chunk_counter, message):
    super(AdditionalColumnsException, self).__init__(filename, chunk_counter, message)
    self.message = message

  def __str__(self):
    return """Existen columnas adicionales, fruto del cruce de la información con la base de datos verificar
    que el ID de las tablas maestrass se encuentre en la  primera columna"""

class LoadingDatabaseError(Status):
  """This exception is launched when an illegal query is performed on the database """
  def __init__(self, filename, chunk_counter, message):
    super(LoadingDatabaseError, self).__init__(filename, chunk_counter, message)
    self.message = message

  def __str__(self):
    return """Ocurrio un error de un dato inválido en este bloque revisar los datos o en caso de ser necesario
    solicitar al administrador de base de datos realizar las modificaciones pertinentes """

class DeleteDatabaseError(Status):
  """This Exception is launched on the delete class"""
  def __init__(self, filename, chunk_counter, message):
    super(DeleteDatabaseError, self).__init__(filename, chunk_counter, message)
    self.message = message
    self.filename = filename

  def __str__(self):
    return """Ocurrió un error de borrado de las transacciones del archivo: %s """ % self.filename
    

class XMLExtractException(Exception):
  """docstring for XMLExtractException"""
  def __init__(self, message, filename):
    super(XMLExtractException, self).__init__()
    self.message = message
    self.filename = filename
  
  def __str__(self):
    return "Error durante la extracción de información del diccionario %s. Detalle del error:\n%s"  % (
     self.filename, self.message )


class IVExtractException(Exception):
  """docstring for IVExtractException"""
  def __init__(self, message, filename):
    super(IVExtractException, self).__init__()
    self.message = message
    self.filename = filename
  
  def __str__(self):
    return "Error durante la extracción de información del item volumen %s. Detalle del error:\n%s"  % (
     self.filename, self.message )

class InvalidRawFileException(Exception):
  """Excepción que se lanza cuando hay un error con algun archivo inicial de nuevas fuentes
  puede tener diferentes motivos para mayor detalle revisar documentación"""
  def __init__(self, message, filename):
    super(InvalidRawFileException, self).__init__()
    self.message = message
    self.filename = filename

  def __str__(self):
    return f"Error en el archivo {self.filename}, el detalle es:\n {self.message}"

class InvalidRangeFileException(Exception):
  """Excepción que se lanza cuando hay un error en el archivo de insumo de estructura"""
  def __init__(self, message, filename):
    super(InvalidRangeFileException, self).__init__()
    self.message = message
    self.filename = filename

  def __str__(self):
    return f"Archivo de estructuras no válido {self.filename}, {self.message}"