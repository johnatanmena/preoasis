
import unidecode as ud
import re
import os
import sys
from exceptions import ExtractException
from exceptions import FileStructureException
from exceptions import InvalidCatalogException
from fuzzywuzzy import process
from fuzzywuzzy import fuzz
import reporting
import datacompy
import pandas as pd
from transversal_classes import DB_Connect
# import numpy as np #no necesario de momento porque no se realizan operaciones sobre catalogos en muchos registros

class FileOpsReadAndValidations():
  filecolumns=[]
  filecolumncount = 0
  __REGULAR_EXPRESION = r'(?:\s+)?[\[\]&!¡¿?+\\\.()%#"°|;,\-\_\']'
  __REGULAR_EXPRESION_SKIP_RANGE = r'(?:\s+)?[\[\]&!¡¿?+\\()%#"°|;,\_\']' #caracteres necesarios para el punto

  def __init__(self, filechunk, filecolumns, dchkcolumns):
    self.filechunk = filechunk
    self.filecolumns = filecolumns
    self.filecolumncount = len(filecolumns)
    self.dchkcolumns = dchkcolumns
    self.filechunkcolumns = filechunk.columns

  def __checkchunkstructure(self, chunkcolumns, exestatus):
    if set(self.filecolumns) != set(self.filechunkcolumns) and set(self.filecolumns + self.dchkcolumns) != set(self.filechunkcolumns):
      cols_a = set(self.filecolumns).difference(set(self.filechunkcolumns))
      cols_b = set(self.filechunkcolumns).difference(set(self.filecolumns))
      message = "Error en la estructura del archivo verificar columnas"
      status = exestatus.get_status()
      reporting.ext_logger.error(message)
      raise FileStructureException(status['process_filename'],  status['chunk_counter'], message,
      cols_a, cols_b)#launch custom exception

  def __homologatedata(self, dataFrame, arrayfields, pd, exestatus, skipfields=['RANGO']): #TODO ADD skip fields to config json and recieve as parameter
    for field in arrayfields:
      filename = exestatus.process_filename
      id_source = int(filename.split("_")[2])

      if (id_source > 2 ) and field in skipfields:
        apply_expr = self.__REGULAR_EXPRESION_SKIP_RANGE
      else:
        apply_expr = self.__REGULAR_EXPRESION

      dataFrame[field] = dataFrame[field].apply(
        # the andpersand symbol is special and must be eliminated
        lambda x:
          x if pd.isnull(x) else ud.unidecode(re.sub(r'\s\s+' ,' ',re.sub(apply_expr, ' ',
          str(x).strip().upper())))
        ).str.strip()
        
    self.filechunk = dataFrame

  def __get_suggestions_for_new_records(self,strSearched, listPossibleString):
    """función que permite identificar dada una cadena y una lista la cadena mas parecida y su puntuación"""
    suggestion1 = process.extractOne(strSearched, listPossibleString, scorer=fuzz.token_set_ratio)
    suggestion2 = process.extractOne(strSearched, listPossibleString, scorer=fuzz.partial_ratio)
    suggestion3 = process.extractOne(strSearched, listPossibleString, scorer=fuzz.token_sort_ratio)
    suggestion4 = process.extractOne(strSearched, listPossibleString, scorer=fuzz.ratio)
    highestMatched = [strSearched, '', suggestion1, suggestion2, suggestion3, suggestion4]
    return highestMatched

  def cross_catalogs(self, catalog_path, bitheader, pd, params):
    """Función que verifica las categorias existentes en un archivo y las compara con los catálogos
    existentes en GIT en caso de haber diferencias crea un archivo temporal que debe ser validado
    por el usuario"""
    data = self.filechunk
    for catalogname in catalog_path:
      df_catalog = pd.read_csv(params.CATALOG_FILES + catalogname, sep=';', encoding='latin')
      column_name = catalogname.split('_')[2].replace('.csv', '')
      df_catalog[column_name] = df_catalog[column_name].apply(
        # is ok a burned regular expression because i dont use range here
        lambda x: 
          ud.unidecode(re.sub(r'\s\s+' ,' ', re.sub(r'(?:\s+)?[\[\]&!¡¿?+\\()%#"°|;,\-\_\']', ' ',
          str(x).upper())))
        ).str.strip()
      df_catalog[column_name+'_GN'].fillna("OASIS_DUMMY_VALUE", inplace=True)
      dataFrame = pd.merge(data, df_catalog, how="left", on=column_name)
      nulldf = dataFrame[dataFrame[column_name+"_GN"].isnull()]
      if nulldf.shape[0] != 0:
        #create output file 
        nulldf = nulldf[[column_name, column_name+"_GN"]].drop_duplicates()
        listcandidates = df_catalog[column_name].tolist()
        nulldf['sugerencias'] = nulldf[column_name].apply(lambda x: self.__get_suggestions_for_new_records(x, listcandidates))
        nulldf[[column_name, column_name+'_GN', 'sug_token_set', 'sug_partial', 'sug_token_sort', 'sug_ratio']] = \
          pd.DataFrame(nulldf.sugerencias.values.tolist(), index= nulldf.index)
        nulldf.drop('sugerencias', axis=1, inplace=True)
        if os.path.exists(params.TEMP_CATALOG_FILES+"OASIS_TMP_MST_"+column_name+".csv"):
          bitheader = 0
        else : 
          bitheader = 1
        nulldf.to_csv(params.TEMP_CATALOG_FILES+"OASIS_TMP_MST_"+column_name+".csv", mode="a+", sep=";", encoding="latin",
          index=False, header=bitheader)
        

        #reporting.ext_logger.info("the output dataframe for the file %s and catalog %s, on the chunk %d is : %d" % (namefile, catalogname, chunk_counter, nulldf.shape[0]))
    return bitheader
  #read the input files and create the catalogs files.
  def process_chunk(self, chunkdata_homologate_columns, exestatus, pd):
    self.__checkchunkstructure(self.filecolumns, exestatus)
    self.__homologatedata(self.filechunk, chunkdata_homologate_columns, pd,exestatus)
    self.filechunk['PRODUCTO'].replace('"','', inplace=True)
    reporting.ext_logger.info('Validación de partición completada, cumple con requisitos de forma')
    reporting.ext_logger.info('Se  aplica transformación de campos de texto clave')

  #assign a gn value to a chunk TODO verify catalog before joining
  def assign_gn_value(self, catalog_path, catalog_files, tmp_filepath, filename, exestatus, pd):
    for catalogname in catalog_files:
      column_name = catalogname.split('_')[2].replace('.csv', '')
      catalog_obj = CatalogsOps(catalogname, catalog_path, pd)
      df_catalog = catalog_obj.catalog_df
      chunkdata = pd.merge(self.filechunk, df_catalog, how='left', on=column_name)
      if chunkdata[chunkdata[column_name+"_GN"].isnull()].shape[0] != 0:
        df = chunkdata.loc[chunkdata[column_name+"_GN"].isnull(),[column_name, column_name+"_GN"]]
        df = df.drop_duplicates(subset=[column_name, column_name+"_GN"])
        listcandidates = df_catalog[column_name].drop_duplicates().tolist()
        status_dict = exestatus.get_status()
        message = u"columna '%s' inválida  revisar catálogos" % column_name
        reporting.ext_logger.error(message)
        raise ExtractException(message, status_dict['chunk_counter'], status_dict['process_filename'], 
          df, listcandidates, column_name) # lanza excepción relacionada con la extracción de datos        
      else:
        chunkdata = chunkdata.drop(columns=[column_name], axis=1)
        chunkdata = chunkdata.rename(columns={column_name+"_GN": column_name})
        reporting.ext_logger.debug('finaliza la asignación de valor de catálogo Nutresa en: ' + catalogname)
        self.filechunk =chunkdata




class CatalogsOps():
  """Clase que representa a los catálogos definidos en el repositorio de catálogos """
  __REGULAR_EXPRESION = r'(?:\s+)?[\[\]&!¡¿?+\\\.()%#"°|;,\-\_\']'
  def __init__(self, catalog_name, catalog_path, pd):
    self.catalog_name = catalog_name.upper().replace('.CSV', '.csv')
    self.catalog_df = pd.read_csv(catalog_path + self.catalog_name,  sep=";")
    self.column_name = self.catalog_name.split('_')[2].replace('.csv', '')
    self.catalog_path = catalog_path
    self.pd = pd
    self.catalog_df = self.__homologatedata(self.catalog_df, self.catalog_df.columns)

  def __homologatedata(self, dataFrame, arrayfields):
    for field in arrayfields:
      dataFrame[field] = dataFrame[field].apply(
        # the andpersand symbol is special and must be eliminated
        lambda x:
          x if self.pd.isnull(x) else ud.unidecode(re.sub(r'\s\s+' ,' ',re.sub(self.__REGULAR_EXPRESION, ' ',
          str(x).strip().upper())))
        )
    return dataFrame


  def validate_catalog(self):
    qty_new_cat = self.catalog_df[self.catalog_df[self.column_name+"_GN"].isnull()].shape[0]
    duplicated_df = self.catalog_df[self.catalog_df.duplicated([self.column_name])]
    is_valid = True
    if duplicated_df.shape[0] != 0:
      message = "el catálogo %s contiene valores duplicados en la columna %s no puede ser evaluado" % \
        (self.catalog_name, self.column_name)
      reporting.ext_logger.info(message) #llamar al log de ejecución
      raise InvalidCatalogException(message)
    elif qty_new_cat != 0 :
      reporting.ext_logger.info("el catálogo %s contiene %d valor(es) sin catalogar revisar y ejecutar nuevamente" % 
        (self.catalog_name, qty_new_cat))
      is_valid = False
    return is_valid


  #this is not effiecient because it tries to download all catalogs files always
  def publish_recieved_catalog(self, catalog_path, git, user="Nutresa"):
    catalog_repo = git.Repo(catalog_path)
    catalog_repo = set_active_branch(catalog_repo)
    origin = catalog_repo.remote(name="origin")
    origin.pull()
    catalog_repo.git.add([self.catalog_name], update=True)
    catalog_repo.index.commit("Catalogos actualizado por %s" % user)
    origin.push()
  
  def process_catalog(self, process_default):
    if process_default=='S':
      self.catalog_df.loc[self.catalog_df[self.column_name + '_GN'].isnull(), self.column_name+'_GN'] = self.__homologatedata(
        self.catalog_df[self.catalog_df[self.column_name + '_GN'].isnull()], [self.column_name]).loc[:,self.column_name]
    else:
      values = self.catalog_df[self.catalog_df[self.column_name + '_GN'].isnull()][self.column_name].to_list()
      for cat_value in values:
        ans = input("Escriba valor grupo para '%s' y finalice con la tecla Enter\n>" % (cat_value))
        ans = ud.unidecode(re.sub(r'\s\s+' ,' ', re.sub(self.__REGULAR_EXPRESION, ' ', ans.upper()))).str.strip()
        self.catalog_df.loc[self.catalog_df[self.column_name] == cat_value, [self.column_name + '_GN']] = ans

    self.catalog_df.to_csv(self.catalog_path + self.catalog_name, sep=';', header=True, encoding='latin', index=False)

  def update_catalog_file(self, old_val, new_val, git, usuario="Nutresa"):
    try:
      catalog_repo = git.Repo(self.catalog_path)
      catalog_repo = set_active_branch(catalog_repo)
      origin = catalog_repo.remote(name="origin")
      origin.pull()
      column_name = self.column_name + "_GN" if usuario.upper() == 'NUTRESA' else  self.column_name
      qty_found = self.catalog_df.loc[self.catalog_df[column_name] == old_val, column_name].shape[0]

      self.catalog_df.loc[self.catalog_df[column_name] == old_val, column_name] = new_val
      if usuario.upper() == 'NUTRESA': 
        self.catalog_df.drop_duplicates([column_name,self.column_name], inplace=True)
      else:
        self.catalog_df.drop_duplicates(column_name, inplace=True)

      self.catalog_df.to_csv(self.catalog_path + self.catalog_name, sep=';', header=True, encoding='latin', index=False)
      catalog_repo.git.add([self.catalog_name], update=True)
      catalog_repo.index.commit("Catalogos actualizado por %s" % usuario)
      origin.push()
    except Exception as e:
      reporting.ext_logger.error(e)
    return qty_found

  def delete_record_catalog_file(self, old_val, git, usuario="Nielsen"):
    """Funcion que elimina un registro de un catalogo publicado solo de la columna nutresa"""
    try:
      catalog_repo = git.Repo(self.catalog_path)
      catalog_repo = set_active_branch(catalog_repo)
      origin = catalog_repo.remote(name="origin")
      origin.pull()
      duplicates = self.catalog_df[self.catalog_df.duplicated(subset = [self.column_name+'_GN'], keep = False)]
      duplicates = duplicates.loc[duplicates[self.column_name] == old_val , self.column_name]
      if duplicates.shape[0] == 1:
        self.catalog_df = self.catalog_df[self.catalog_df[self.column_name] != old_val].copy()
        self.catalog_df.to_csv(self.catalog_path + self.catalog_name, sep=';', header=True, encoding='latin', index=False)
      else:
        reporting.ext_logger.info("No se puede eliminar el registro {} o no fue encontrado".format(old_val))

      catalog_repo.git.add([self.catalog_name], update=True)
      catalog_repo.index.commit("Catalogos actualizado por %s" % usuario)
      origin.push()
    except Exception as e:
      reporting.ext_logger.error("No se pudo completar la operacion:")
      reporting.ext_logger.error(e)
    return duplicates.shape[0] if duplicates.shape[0]==1 else 0 


def publish_catalogs(catalog_path, temp_catalog_path, pd, git, user='Nutresa'):
  try:
    catalog_repo = git.Repo(catalog_path)
    catalog_repo = set_active_branch(catalog_repo)
    origin = catalog_repo.remote(name='origin')
    origin.pull()
    for tmp_catalog in os.listdir(temp_catalog_path):
      df_oasis = pd.read_csv(temp_catalog_path + tmp_catalog, sep=';', encoding='latin')
      new_name = tmp_catalog.replace("_TMP", "")
      df_oasis.iloc[: , [0,1]].to_csv(catalog_path + new_name, sep=';', header=False, mode='a', encoding='latin', index=False)
      os.remove(temp_catalog_path + tmp_catalog)
    #publish on the repository 
    catalog_repo.git.add(update=True)
    catalog_repo.index.commit("Catalogos actualizados por: "+ user)
    origin.push()
  except:
    reporting.ext_logger.error("error durante la publicación del catálogo --- intentar publicacion manual")

def simplify_catalogs(temp_catalog_files, pd, params):
  for tmpcatalogname in temp_catalog_files:
    df_catalog = pd.read_csv(params.TEMP_CATALOG_FILES + tmpcatalogname, sep=';', encoding='latin')
    column_name = tmpcatalogname.split('_')[3].replace('.csv', '')
    df_catalog = df_catalog.drop_duplicates(subset=[column_name])
    df_catalog.to_csv(params.TEMP_CATALOG_FILES + tmpcatalogname, sep=';', encoding='latin', index=False, mode="w")

def set_active_branch(repo):
  branch = repo.active_branch.name
  config_branch_name = DB_Connect().get_git_branch_name()
  if branch != config_branch_name and config_branch_name in ['develop','master']:
    try:
      repo.git.checkout(config_branch_name)
    except Exception as e:
      reporting.ext_logger.error("No se puede cambiar de branch a " + config_branch_name)
      reporting.ext_logger.error(e)
      raise e
  return repo

######## Clase solo necesaria para opreaciones Nielsen
class DatacheckOperations():
  def __init__(self, path_to_dtchck, old_file, new_file, filecolumns):
      self.rep_old2 = old_file.upper().replace('CSV', 'csv')
      self.rep_new  = new_file.upper().replace('CSV', 'csv')
      self.filecolumns = filecolumns
      self.path_to_dtchck = path_to_dtchck
      

  def create_datacheck(self, write_path):
      REP_OLD2 = pd.read_csv(self.path_to_dtchck + self.rep_old2, encoding='latin-1', sep=';')
      REP_OLD2 = REP_OLD2.iloc[:, list(range(0,68))]
      REP_OLD2['INDICE_ORIGINAL'] = REP_OLD2.index
      REP_NEW = pd.read_csv(self.path_to_dtchck + self.rep_new, encoding='latin-1', sep=';')
      REP_NEW = REP_NEW.iloc[:, list(range(0,68))]
      dc = datacompy.Compare(REP_NEW, REP_OLD2, join_columns=tuple(self.filecolumns), abs_tol=0, #('MERCADO','PERIODO', 'TAG', 'MARCA')
                  rel_tol=0,
                  df1_name='new',
                  df2_name='old')
      dc.matches(ignore_extra_columns=False)
   
      out_df_1 = dc.df1_unq_rows.copy()
      out_df_1['NOMBRE_ARCHIVO'] = self.rep_old2
      out_df_1['NUMERO_LINEA'] = out_df_1.index  + 1 
      out_df_1['ACCION'] = 'IN'
      out_df_1.columns = [x.upper() for x in out_df_1.columns]

      out_df_2= dc.df2_unq_rows.copy()
      out_df_2['NOMBRE_ARCHIVO'] = self.rep_new
      out_df_2['NUMERO_LINEA'] = out_df_2['indice_original']  + 1
      out_df_2['ACCION'] = 'OUT'
      out_df_2.drop(columns=['indice_original'], axis=1, inplace=True)
      out_df_2.columns = [x.upper() for x in out_df_2.columns]

      out_name = self.rep_new.replace(self.rep_new.split('_')[1], 'DATACHECK')
      
      pd.concat([out_df_1, out_df_2]).to_csv(write_path + out_name, index=False, sep=';', encoding="latin")
      reporting.ext_logger.info("Archivo %s, creado con éxito" % out_name)
      return (0, "Archivo %s, creado con éxito" % out_name)
   
