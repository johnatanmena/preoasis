import sys
import os
import transform.constant as constant
import re
from datetime import datetime
from exceptions import DuplicateTagsException
from exceptions import AdditionalColumnsException
from exceptions import LoadingDatabaseError
from transversal_classes import ProjectParameters
from transversal_classes import ProductInfo
import reporting
from transversal_classes import DB_Connect

class TransformOps():
  __column_table_dict = {}
  ID_FABRICANTE_PL  = 2  # DB: FAB SUPERMERCADOS
  ID_FABRICANTE_SUP = 2  # DB: FAB SUPERMERCADOS
  ID_NIVEL_TAMANO   = 2  # DB: TAMANO
  ID_NIVEL_RANGO    = 3  # DB: RANGO
  ID_NIVEL_INTEGRAL = 4  # DB: INTEGRALNOINTEGRAL
  ID_CAT_CAFMOLIDO  = 1  # DB: CAFE MOLIDO
  ID_CAT_CAFSOLUBLE = 2  # DB: CAFE SOLUBLE
  ID_CAT_CARNESFRIAS= 3  # DB: CARNES FRIAS
  ID_CAT_SALCHICHAS = 4  # DB: SALCHICHAS Y CARNICOS EN CONSERVA
  ID_CAT_CEREALES   = 5  # DB: CEREALES EN BARRA
  ID_CAT_CHOCOMESA  = 6  # DB: CHOCOLATE DE MESA
  ID_CAT_CHOCOLATINAS=7  # DB: CHOCOLATINAS
  ID_CAT_GALLETAS   = 8  # DB: GALLETAS 
  ID_CAT_HELADOS    = 9  # DB: HELADOS
  ID_CAT_MODLECHE   = 10 # DB: MODIFICADORES DE LECHE
  ID_CAT_PASTAS     = 11 # DB: PASTAS
  ID_CAT_VEGCONSERVA= 12 # DB: VEGETALES EN CONSERVA
  ID_CAT_PASABOCAS  = 13 # DB: NUECES
  ID_CAT_PASACONGEL = 14 # DB: PASABOCAS CONGELADOS
  ID_CAT_PASTASCONG = 15 # DB: PASTAS REFRIGERADAS
  ID_CAT_PIZZASREF  = 16 # DB: PIZZAS REFRIGERADAS
  ID_CAT_PLATOSCONG = 17 # DB: PLATOS CONGELADOS
  ID_CAT_POLLOPRECO = 18 # DB: POLLO PRECOCIDO


  def __init__(self, chunk,exestatus, db_connection, params, pd):
    self.chunk_df = chunk.copy()
    self.db_connection = db_connection
    self.pd = pd
    self.exestatus = exestatus 
    self.filename = exestatus.process_filename
    self.id_source = int(self.exestatus.process_filename.split("_")[2])
    self.schema_name = DB_Connect().get_db_schema_name(self.id_source)
    self.__column_table_dict = params.COLUMN_DB_MAPPING
    batch_number = exestatus.chunksize * exestatus.chunk_counter
    self.chunk_df['TMP_AUX'] = 1
    self.chunk_df['NUMERO_LINEA_ARCHIVO'] = self.chunk_df['TMP_AUX'].cumsum() + batch_number
    self.file_location_s3 = params.S3_BUCKET_NAME+"/"+ params.S3_KEY_NAME.replace("pricing_shopper_views", "input_nielsen_files")
    #Asignar los id's a los datos de cada chunk
    self.item = self.__leer_nivel_item()
    self.assign_id_to_chunk()

  def __create_conditions_scantrack(self):
    # crear condiciones especiales para el proceso de scantrack
    self.condition_pl = (
      (self.chunk_df['FABRICANTE'] == self.ID_FABRICANTE_PL) & 
        (
          ((self.chunk_df['NIVEL']== self.ID_NIVEL_TAMANO) & (
                      self.chunk_df['CATEGORIA'].isin([self.ID_CAT_CAFSOLUBLE, self.ID_CAT_MODLECHE, self.ID_CAT_PASACONGEL, self.ID_CAT_PIZZASREF, self.ID_CAT_PLATOSCONG,
                      self.ID_CAT_GALLETAS, self.ID_CAT_CHOCOLATINAS, self.ID_CAT_PASABOCAS, self.ID_CAT_CARNESFRIAS, self.ID_CAT_CEREALES, self.ID_CAT_POLLOPRECO , self.ID_CAT_PASTASCONG])
                    ))
          |
          ((self.chunk_df['NIVEL']== self.ID_NIVEL_RANGO) & (
                      self.chunk_df['CATEGORIA'].isin([self.ID_CAT_CAFMOLIDO, self.ID_CAT_SALCHICHAS, self.ID_CAT_VEGCONSERVA,
                      self.ID_CAT_CHOCOMESA])
                    ))
          |
          ((self.chunk_df['NIVEL']== self.ID_NIVEL_INTEGRAL) & (
                      self.chunk_df['CATEGORIA'].isin([self.ID_CAT_PASTAS])
                    ))
        )
      )
    # esta condición se puede simplificar de acuerdo a la definición que 
    # el ID de PL y fabricante SUP pasa a ser el mismo
    self.condition_sup = (
      (self.chunk_df['FABRICANTE'] == self.ID_FABRICANTE_SUP) & 
        (
          ((self.chunk_df['NIVEL'] == self.ID_NIVEL_TAMANO) & (
            self.chunk_df['CATEGORIA'] == self.ID_CAT_HELADOS
          ))
        )
    )


  def __leer_nivel_item(self):
    #schema_name = DB_Connect().get_db_schema_name()
    query= "SELECT ID_NIVEL FROM %s.nivel WHERE NIVEL ='ITEM' " % (self.schema_name)
    cursor = self.db_connection.cursor()
    cursor.execute(query)
    nivel_item = cursor.fetchone()
    cursor.close()
    return nivel_item[0]

  def __insert_new_category_values(self, value, table_name, column_name):
    query = "INSERT INTO %s.%s (%s) VALUES ('%s')" % (self.schema_name, table_name, column_name, value)
    cursor = self.db_connection.cursor()
    try:
      cursor.execute(query)
      self.db_connection.commit()
      reporting.trn_logger.debug("%s, Agregado a la tabla: %s" %(value, table_name))
    except Exception as e:
      message = 'Error de inserción en la base de datos de %s - %s' % (value, table_name)
      status = self.exestatus.get_status()
      print(query)
      error_obj = LoadingDatabaseError(status['process_filename'],  status['chunk_counter'], message)
      reporting.trn_logger.error(error_obj)
      raise error_obj
    finally:
      cursor.close()

  def __insert_new_market_values(self, value, table_name):
    query = "INSERT INTO %s.%s (%s, %s) VALUES ('%s', '%s')" % (self.schema_name, 'MERCADO', 'NOMBRE_MERCADO', 'MERCADO_DL', value, value)
    cursor = self.db_connection.cursor()
    try:
      cursor.execute(query)
      self.db_connection.commit()
      reporting.trn_logger.debug("%s, Agregado a la tabla: %s" %(value, table_name))
    except Exception as e:
      message = 'Error de inserción en la base de datos de %s - %s' % (value, table_name)
      status = self.exestatus.get_status()
      print(query)
      error_obj = LoadingDatabaseError(status['process_filename'],  status['chunk_counter'], message)
      reporting.trn_logger.error(error_obj)
      raise error_obj
    finally:
      cursor.close()

  def __identify_sku_new_products(self):
    #seleccionar todos los productos activos en la base de datos 
    #products_df = self.pd.read_sql(sql="select * from producto_sku where activo=TRUE", con=self.db_connection)
    if ProductInfo().get_flag_need_refresh_sku() or self.id_source != ProductInfo().get_last_id_source() :
      ProductInfo().update_sku_product(self.db_connection, self.schema_name, self.id_source)
      reporting.trn_logger.info("nuevos productos se actualiza la info de proucto en memoria con info de la base de datos")
      ProductInfo().set_flag_need_refresh_sku(False)
      reporting.trn_logger.debug("Existen nuevos productos sku en el bloque: "+ str(self.exestatus.chunk_counter))
    products_df = ProductInfo().get_sku_product()
    #para la prueba inicial se realiza sobre el código Tag
    if self.id_source == 1 or self.id_source >=3: #Nielsen colombia retail o fuentes 3 y 4
      sku_df = self.chunk_df[self.chunk_df['NIVEL'] == self.item]
    else:
      sku_df = self.chunk_df[((self.chunk_df['NIVEL'] == self.item) | (self.condition_pl) | (self.condition_sup))] #buscar el nivel 'ITEM' en la base de datos
    sku_df = self.pd.merge(sku_df, products_df, how='left', left_on='TAG', right_on='COD_TAG', suffixes=('', '_TMP'))
    sku_df['ITEM_OR_GROUPED'] = 'I'
    if sku_df.loc[~sku_df['ACTIVO'].isnull(), :].shape[0] != 0: 
      sku_df.loc[~sku_df['ACTIVO'].isnull(),'NUEVO_PRODUCTO'] = 'F'
      reporting.trn_logger.debug("no existen productos sku nuevos")
    if sku_df.loc[sku_df['ACTIVO'].isnull(), : ].shape[0] != 0:
      sku_df.loc[sku_df['ACTIVO'].isnull(), 'NUEVO_PRODUCTO'] = 'T'
      ProductInfo().set_flag_need_refresh_sku(True)#en la siguiente iteración se deben cargar los nuevos productos en memoria
    
    if 'NUEVO_PRODUCTO' not in sku_df.columns:
      sku_df['NUEVO_PRODUCTO'] = ''
    #renombrar campos con respecto a los de la base de datos
    for file_col_name, db_col_name in constant.dict_file_column_table_column_item.items():
      if file_col_name == db_col_name:
        #TODO define wich fields are the corrects to define an update on the database
        #sku_df.loc[~sku_df['ACTIVO'].isnull() & sku_df[file_col_name] != sku_df[db_col_name+"_TMP"],'NUEVO_PRODUCTO'] = 'F'
        sku_df[db_col_name+"_TMP"] = sku_df[file_col_name]
        sku_df.drop(columns=[file_col_name], axis=1, inplace=True)
        sku_df.rename(columns={db_col_name+'_TMP': db_col_name},  inplace=True)
      else:
        #sku_df.loc[~sku_df['ACTIVO'].isnull() & sku_df[file_col_name] != sku_df[db_col_name],'NUEVO_PRODUCTO'] = 'F'
        sku_df[db_col_name] = sku_df[file_col_name]
        sku_df.drop(columns=[file_col_name], axis=1, inplace=True)
    return sku_df


  def __identify_grouped_new_products(self):
    #group_products_df = self.pd.read_sql(sql="select * from producto_agrupado where activo=TRUE",
    #  con=self.db_connection)
    if ProductInfo().get_flag_need_refresh_grouped() or self.id_source != ProductInfo().get_last_id_source():
      ProductInfo().update_grouped_product(self.db_connection, self.schema_name, self.id_source)
      reporting.trn_logger.info("nuevos productos se actualizan los producto en memoria con info de la base de datos")
      ProductInfo().set_flag_need_refresh_grouped(False)
      reporting.trn_logger.debug("Existen nuevos productos agrupados en el bloque: "+ str(self.exestatus.chunk_counter))
    

    group_products_df = ProductInfo().get_grouped_product()
    if self.id_source == 1 or self.id_source >=3: #Nielsen colombia retail o fuentes 3 y 4
      grouped_df = self.chunk_df[self.chunk_df['NIVEL']!= self.item]
    else:
      grouped_df = self.chunk_df[~((self.chunk_df['NIVEL'] == self.item) | (self.condition_pl) | (self.condition_sup))] #buscar el nivel 'ITEM' en la base de datos
    grouped_df = self.pd.merge(grouped_df, group_products_df, how='left', left_on='TAG', right_on='COD_TAG',
      #cambia un poco la lógica porque tiene varios niveles agrupados y el tag no es único
      suffixes=('', '_TMP'))
    grouped_df['ITEM_OR_GROUPED'] = 'G'

    if grouped_df.loc[~grouped_df['ACTIVO'].isnull(),:].shape[0] != 0:
      grouped_df.loc[~grouped_df['ACTIVO'].isnull(),'NUEVO_PRODUCTO'] = 'F'
      reporting.trn_logger.debug("no existen productos agrupados nuevos")
    if grouped_df.loc[grouped_df['ACTIVO'].isnull(), :].shape[0] != 0:
      grouped_df.loc[grouped_df['ACTIVO'].isnull(), 'NUEVO_PRODUCTO'] = 'T'
      ProductInfo().set_flag_need_refresh_grouped(True)

    if 'NUEVO_PRODUCTO' not in grouped_df.columns:
      grouped_df['NUEVO_PRODUCTO'] = ''
    for file_col_name, db_col_name in constant.dict_file_column_table_column_grouped.items():
      #this line checks if there is a change on the data to assign a flag that tells the load operation that the product must be
      #updated or if it has to be equal
      if file_col_name == db_col_name:
        grouped_df[db_col_name+"_TMP"] = grouped_df[file_col_name]
        grouped_df.drop(columns=[file_col_name], axis=1, inplace=True)
        grouped_df.rename(columns={db_col_name+'_TMP': db_col_name},  inplace=True)
      else:
        grouped_df[db_col_name] = grouped_df[file_col_name]
        grouped_df.drop(columns=[file_col_name], axis=1, inplace=True)
    return grouped_df

  def __obtain_id(self, col_name, table_name):
    """funcion que obtiene los ids relacionados a una columna del archivo de insumo"""
    mst_table_df = self.pd.read_sql(sql = "select * from %s.%s" % (self.schema_name, table_name), con=self.db_connection)
    mst_table_col_name = str(mst_table_df.columns[1])
    mst_table_drop_col = mst_table_df.columns.to_list()[1:]
    aux_df = self.pd.merge(self.chunk_df, mst_table_df, how='left', left_on = col_name, 
      right_on=mst_table_col_name)
    null_df = aux_df[aux_df[mst_table_df.columns[0]].isnull()]
    if null_df.shape[0] != 0:
      null_df = null_df.drop_duplicates(subset=[col_name])
      #si esto ocurre es porque existe una categoría nueva y debe ser insertada en la base de datos
      #¿esto se debe hacer en este modulo o en el de carga?
      null_df[col_name].apply(lambda x: self.__insert_new_category_values(x, table_name, mst_table_df.columns[1]))
      mst_table_df = self.pd.read_sql(sql = "select * from %s.%s" % (self.schema_name, table_name), con=self.db_connection)
      aux_df = self.pd.merge(self.chunk_df, mst_table_df, how='left', left_on = col_name, 
        right_on=mst_table_col_name)
    self.chunk_df = aux_df
    self.chunk_df.rename(columns={mst_table_df.columns[0]: "tmpoasis_"+col_name}, inplace=True)
    self.chunk_df.drop(columns=mst_table_drop_col+[col_name], axis=1, inplace=True)
    self.chunk_df.rename(columns={"tmpoasis_"+col_name: col_name},  inplace=True)


  def __obtain_id_market(self):
    """Función que asigna el id al mercado es necesario hacerlo en una función aparte debido a que el proceso de
    inserción de un nuevo mercado es distinto"""
    mst_table_df = self.pd.read_sql(sql="SELECT * FROM {}.MERCADO".format(self.schema_name), con=self.db_connection)
    aux_df = self.pd.merge(self.chunk_df, mst_table_df, how='left', left_on=constant.mercado_col_name_on_file,
     right_on=constant.mercado_col_name_on_db)
    null_df = aux_df[aux_df[constant.mercado_id_col_name_on_db].isnull()]
    if null_df.shape[0] != 0:
      null_df = null_df.drop_duplicates(subset=[constant.mercado_col_name_on_file])
      null_df[constant.mercado_col_name_on_file].apply(lambda x: self.__insert_new_market_values(x, "MERCADO"))
      mst_table_df = self.pd.read_sql(sql="SELECT * FROM {}.MERCADO".format(self.schema_name), con=self.db_connection)
      aux_df = self.pd.merge(self.chunk_df, mst_table_df, how='left', left_on=constant.mercado_col_name_on_file,
        right_on=constant.mercado_col_name_on_db)
    self.chunk_df = aux_df
    self.chunk_df.rename(columns={constant.mercado_id_col_name_on_db: "tmpoasis_" + constant.mercado_col_name_on_file}, inplace=True)
    self.chunk_df.drop(columns=[constant.mercado_col_name_on_file, constant.mercado_col_name_on_db, "MERCADO_DL"],
      axis=1, inplace=True)
    self.chunk_df.rename(columns={"tmpoasis_"+ constant.mercado_col_name_on_file: constant.mercado_col_name_on_file}, inplace=True)


  def assign_id_to_chunk(self):
    """Función que asigna un id para cada uno de los campos del diccionario que relaciona columnas del archivo de insumo con
    las columnas de las tablas de la base de datos. cuando un id no existe lo agrega en la base de datos"""
    for col_name, table_name in self.__column_table_dict.items():
      self.__obtain_id(col_name,  table_name)
      reporting.trn_logger.debug("Asignacion de Id  para el campo %s completado." % (col_name))
    self.__obtain_id_market()
    if self.id_source <= 2:
      self.__create_conditions_scantrack() #solo aplica para nielsen colombia ids de fuente 1 y 2
    reporting.trn_logger.info("Completada asignación de Id para mercado")
    sku_df = self.__identify_sku_new_products()
    grouped_df = self.__identify_grouped_new_products()
    #print(sku_df.shape, grouped_df.shape, self.chunk_df.shape)
    #validar si tienen la suma de la cantidad de filas debe ser igual a la cantidad de filas inicial  
    if sku_df.shape[0] + grouped_df.shape[0] != self.chunk_df.shape[0]:
      #error de ejecución
      status = self.exestatus.get_status()
      message = "Existen tags duplicados en la base de datos"
      reporting.trn_logger.error(message)
      raise DuplicateTagsException(status['process_filename'],  status['chunk_counter'], message) #something, esto significa que algun tag esta duplicado
    elif  sku_df.shape[1] != grouped_df.shape[1]:
      #error de ejecución 
      status = self.exestatus.get_status()
      message = "Hay cantidad diferentes de columnas en sku y grupo"
      reporting.trn_logger.error(message)
      raise AdditionalColumnsException(status['process_filename'],  status['chunk_counter'], message) #esto significa que hay columnas de mas en uno de los dos dataframes
    else:
      self.chunk_df = self.pd.concat([sku_df, grouped_df], sort=False)


  #todo get the source id from the filename 
  #modify the line number on file
  def add_auxiliar_variables(self, source, filename, user):
    #function that adds info variables to the process variables
    self.chunk_df['ID_FUENTE'] = int(filename.split("_")[2])
    if source == 'DATACHECK':
      self.chunk_df['FILENAME'] = self.chunk_df['NOMBRE_ARCHIVO']
      self.chunk_df['NUMERO_LINEA_ARCHIVO'] = self.chunk_df['NUMERO_LINEA']
      self.chunk_df.drop(columns=['NOMBRE_ARCHIVO', 'NUMERO_LINEA'], axis=1, inplace=True)
    else:
      self.chunk_df['FILENAME'] = filename
    self.chunk_df['USUARIO'] = user
    self.chunk_df['FECHA_PROCESAMIENTO'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    self.chunk_df['UBICACION_ARCHIVO'] = self.file_location_s3
    reporting.trn_logger.info("creación de variables adicionales finalizada") 
  
  def __generate_range_size_fields(self, row):
    #TODO generate on error conditions and verify range cases
    expr = r'\d+(?:\.\d+)?'
    match_obj = re.match(expr, row['RANGO_SINPROC'])
    if match_obj is not None:
      pos_size = match_obj.group()
      if pos_size is None or pos_size=='':
        match_obj = re.match(expr, row['RANGO_SINPROC'])
        pos_size = match_obj.group()
      else:
        row['RANGO_MIN'] = pos_size
        #obtener el tamaño máximo
        if len(re.findall(expr, row['RANGO_SINPROC'])) > 1: 
          pos_size = re.findall(expr, row['RANGO_SINPROC'])[1]
          row['RANGO_MAX'] =pos_size #revisar este caso en específico
        else:
          row['RANGO_MAX'] = -1
    #obtener tamaño
    match_obj = re.match(expr, str(row['TAMANO']))
    if match_obj is not None:
      pos_size = match_obj.group()
      if pos_size == None or pos_size == '':
        row['TAMANO_SINPROC'] = row['TAMANO']
        row['TAMANO'] = -1
      else: 
        row['TAMANO_SINPROC'] = row['TAMANO']
        row['TAMANO'] = pos_size
    else:
      row['TAMANO_SINPROC'] = row['TAMANO']
      row['TAMANO'] = -1
    return row

  def calculate_range_product_desc (self):
    #obtener el rango mínimo
    self.chunk_df = self.chunk_df.apply(lambda row: self.__calculate_product_desc(row), axis=1)
    self.chunk_df = self.chunk_df.apply(lambda row: self.__generate_range_size_fields(row), axis=1)
    self.chunk_df.fillna(value={"DUP_ID_UNIDAD_MEDIDA":-1, 'RANGO_MIN':0, 'RANGO_MAX':0}, inplace=True)
    reporting.trn_logger.info("Se genera codigo de producto, calculo de rangos y rellenado de nulos ")

  #generar codigo de descripcion del producto
  def __calculate_product_desc(self, row):
    row['PRODUCTO_DESC'] =( ("0000"+str(row['DUP_ID_NIVEL']))[-2:] + 
        ("0000"+str(row['DUP_ID_MARCA']))[-4:] + 
        ("0000"+str(row['DUP_ID_CONSISTENCIA']))[-2:] + 
        ("0000"+str(row['DUP_ID_NIVEL_AZUCAR']))[-1:] + 
        ("0000"+str(row['DUP_ID_FABRICANTE']))[-3:] + 
        ("0000"+str(row['DUP_ID_EMPAQUE']))[-2:] + 
        ("0000"+str(row['DUP_ID_PRESENTACION']))[-3:] + 
        ("0000"+str(row['DUP_ID_CATEGORIA']))[-2:] + 
        ("0000"+str(row['DUP_ID_SEGMENTO']))[-3:] + 
        ("0000"+str(row['DUP_ID_SUBMARCA']))[-3:] + 
        #("0000"+str(row['DUP_ID_UNIDAD_MEDIDA']))[:-2] + #pendiente de agregar a la data
        ("0000"+str(row['DUP_ID_VARIEDAD']))[-2:] + 
        ("0000"+str(row['DUP_ID_TIPO']))[-3:] + 
        ("0000"+str(row['DUP_ID_TIPOCARNE']))[-2:] + 
        ("0000"+str(row['DUP_ID_SUBTIPO']))[-3:] + 
        ("0000"+str(row['DUP_ID_SABOR']))[-3:] +
        ("0000"+str(row['DUP_ID_TIPOSABOR']))[-3:] 
        )
    return row
