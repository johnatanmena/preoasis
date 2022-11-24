import sys
import os
import mysql.connector as myconnlib
import reporting
from transversal_classes import DB_Connect
from exceptions import LoadingDatabaseError
from exceptions import DeleteDatabaseError
import pandas as pd
import sqlalchemy

"""
La generación de queries dinamica es un inconveniente grande si no se conoce la estructura de las tablas de la base de datos, y en
este caso la base de datos aun se encuentra en una fase de pruebas, se sabe que estos queries se pueden actualizar y mejorar el proceso mucho en el futuro
pero queda pendiente para una nueva versión de la aplicación
"""

item_columns = ["COD_EAN", "COD_TAG", "DUP_PRODUCTO", "PRODUCTO_DESC", "DUP_ID_MARCA", "DUP_ID_CONSISTENCIA", "DUP_ID_NIVEL_AZUCAR",
"DUP_ID_FABRICANTE", "DUP_ID_EMPAQUE", "DUP_INTEGRAL_NO_INTEGRAL", "DUP_OFERTA_PROMOCIONAL", "DUP_IMPORTADO", "DUP_ID_PRESENTACION",
"DUP_ID_CATEGORIA", "DUP_ID_SEGMENTO", "DUP_ID_SUBMARCA", "DUP_ID_UNIDAD_MEDIDA", "DUP_ID_VARIEDAD", "DUP_ID_TIPO", "DUP_ID_TIPOCARNE", "DUP_ID_SUBTIPO",
"DUP_ID_SABOR", "RANGO_MIN", "RANGO_MAX", "TAMANO", "TAMANO_SINPROC", "RANGO_SINPROC", "DUP_ID_NIVEL", "NUEVO_PRODUCTO", "COD_PRODUCTO", "DUP_ID_TIPOSABOR"]

grouped_columns = ["COD_TAG", "DUP_PRODUCTO", "PRODUCTO_DESC", "DUP_ID_MARCA", "DUP_ID_CONSISTENCIA", "DUP_ID_NIVEL_AZUCAR",
"DUP_ID_FABRICANTE", "DUP_ID_EMPAQUE", "DUP_INTEGRAL_NO_INTEGRAL", "DUP_OFERTA_PROMOCIONAL", "DUP_IMPORTADO", "DUP_ID_PRESENTACION",
"DUP_ID_CATEGORIA", "DUP_ID_SEGMENTO", "DUP_ID_SUBMARCA", "DUP_ID_UNIDAD_MEDIDA", "DUP_ID_VARIEDAD", "DUP_ID_TIPO", "DUP_ID_TIPOCARNE", "DUP_ID_SUBTIPO",
"DUP_ID_SABOR", "RANGO_MIN", "RANGO_MAX", "TAMANO", "TAMANO_SINPROC", "RANGO_SINPROC", "DUP_ID_NIVEL", "NUEVO_PRODUCTO", "COD_PRODUCTO", "DUP_ID_TIPOSABOR"]


insert_query = """
INSERT INTO %s.%s (%s, %s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s, %s,
%s, %s, %s, %s, %s, %s, %s, %s) VALUES ("%s", "%s", "%s", "%s", %s, %s, %s,%s, %s, "%s", "%s", "%s", %s, %s,%s, %s, %s,
 %s, %s, %s, %s,%s, %s, %s, %s, "%s", "%s", %s, %s);
"""

insert_grouped_query = """
INSERT INTO %s.%s (%s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s, %s,
%s, %s, %s, %s, %s, %s, %s, %s) VALUES ("%s", "%s", "%s", %s, %s, %s,%s, %s, "%s", "%s", "%s", %s, %s,%s, %s, %s,
 %s, %s, %s, %s,%s, %s, %s, %s, "%s", "%s", %s, %s);
"""

number_columns = ['COMPRAS_PROMEDIO_POR_TIENDA_KILOS',
       'INVENTARIO_PROMEDIO_POR_TIENDA_KILOS', 'COMPRAS_TOTALES_KILOS_000',
       'INVENTARIO_ACTIVO_KILOS_000', 'INVENTARIO_TOTAL_KILOS_000',
       'DIST_MANEJANTES_POND', 'DIST_MANEJANTES_NUM',
       'DIST_AGOTADOS_NUM', 'DIST_TIENDAS_COMPRADORAS_NUM',
       'DIST_TIENDAS_COMPRADORAS_POND', 'DIST_AGOTADOS_POND',
       'COMPRAS_DIRECTAS_KILOS_000', 'DIST_MATERIAL_POP_NUM',
       'DIST_MATERIAL_POP_POND', 'DIST_EXHIBI_ESPECIALES_NUM',
       'DIST_EXHIBI_ESPECIALES_POND', 'DIST_OFERTAS_NUM',
       'DIST_OFERTAS_POND', 'DIST_ACTIVIDAD_PROMOCIONAL_NUM',
       'DIST_ACTIVIDAD_PROMOCIONAL_POND', 'DIST_EXHIBICIONES_NUM_MAX',
       'DIST_EXHIBICIONES_POND_MAX', 'DIST_EXHIBICIONES_POND_PROM',
       'DIST_NUM_AGOTADOS_PROM', 'DIST_NUM_MANEJANTE_MAX',
       'DIST_NUM_MANEJANTE_PROM','DIST_NUM_TIENDAS_COMPRANDO_MAX', 'DIST_NUM_TIENDAS_VENDIENDO_MAX',
       'DIST_OFERTAS_NUM_MAX', 'DIST_OFERTAS_NUM_PROM',
       'DIST_OFERTAS_POND_MAX', 'DIST_OFERTAS_POND_PROM',
       'DIST_POND_AGOTADOS_PROM', 'DIST_POND_MANEJANTE_MAX',
       'DIST_POND_MANEJANTE_PROM', 'DIST_POND_TIENDAS_COMPRANDO_MAX', 'DIST_POND_TIENDAS_VENDIENDO_MAX']

update_query = """
UPDATE %s.%s SET COD_EAN='%s', COD_TAG='%s', DUP_PRODUCTO="%s", PRODUCTO_DESC='%s', DUP_ID_MARCA=%s,
DUP_ID_CONSISTENCIA = %s, DUP_ID_NIVEL_AZUCAR=%s, DUP_ID_FABRICANTE = %s, DUP_ID_EMPAQUE= %s, DUP_INTEGRAL_NO_INTEGRAL='%s',
DUP_OFERTA_PROMOCIONAL = '%s', DUP_IMPORTADO='%s', DUP_ID_PRESENTACION= %s, DUP_ID_CATEGORIA=%s, DUP_ID_SEGMENTO=%s,
DUP_ID_SUBMARCA = %s, DUP_ID_UNIDAD_MEDIDA=%s, DUP_ID_VARIEDAD=%s, DUP_ID_TIPO=%s, DUP_ID_TIPOCARNE=%s, DUP_ID_SUBTIPO=%s,
DUP_ID_SABOR=%s,RANGO_MIN=%s, RANGO_MAX=%s, TAMANO=%s, TAMANO_SINPROC='%s', RANGO_SINPROC='%s', DUP_ID_NIVEL=%s, DUP_ID_TIPOSABOR=%s
 WHERE COD_PRODUCTO = '%s';
"""

update_datacheck_query = """
UPDATE %s.%s SET COD_EAN='%s', COD_TAG='%s', DUP_PRODUCTO="%s", PRODUCTO_DESC='%s', DUP_ID_MARCA=%s,
DUP_ID_CONSISTENCIA = %s, DUP_ID_NIVEL_AZUCAR=%s, DUP_ID_FABRICANTE = %s, DUP_ID_EMPAQUE= %s, DUP_INTEGRAL_NO_INTEGRAL='%s',
DUP_OFERTA_PROMOCIONAL = '%s', DUP_IMPORTADO='%s', DUP_ID_PRESENTACION= %s, DUP_ID_CATEGORIA=%s, DUP_ID_SEGMENTO=%s,
DUP_ID_SUBMARCA = %s, DUP_ID_UNIDAD_MEDIDA=%s, DUP_ID_VARIEDAD=%s, DUP_ID_TIPO=%s, DUP_ID_TIPOCARNE=%s, DUP_ID_SUBTIPO=%s,
DUP_ID_SABOR=%s,RANGO_MIN=%s, RANGO_MAX=%s, TAMANO=%s, TAMANO_SINPROC='%s', RANGO_SINPROC='%s', DUP_ID_NIVEL=%s, DUP_ID_TIPOSABOR=%s
OBSERVACION='%s', ACTIVO=1
 WHERE COD_PRODUCTO = '%s';
"""


update_grouped_query = """
UPDATE %s.%s SET COD_TAG='%s', DUP_PRODUCTO="%s", PRODUCTO_DESC='%s', DUP_ID_MARCA=%s,
DUP_ID_CONSISTENCIA = %s, DUP_ID_NIVEL_AZUCAR=%s, DUP_ID_FABRICANTE = %s, DUP_ID_EMPAQUE= %s, DUP_INTEGRAL_NO_INTEGRAL='%s',
DUP_OFERTA_PROMOCIONAL = '%s', DUP_IMPORTADO='%s', DUP_ID_PRESENTACION= %s, DUP_ID_CATEGORIA=%s, DUP_ID_SEGMENTO=%s,
DUP_ID_SUBMARCA = %s, DUP_ID_UNIDAD_MEDIDA=%s, DUP_ID_VARIEDAD=%s, DUP_ID_TIPO=%s, DUP_ID_TIPOCARNE=%s, DUP_ID_SUBTIPO=%s,
DUP_ID_SABOR=%s,RANGO_MIN=%s, RANGO_MAX=%s, TAMANO=%s, TAMANO_SINPROC='%s', RANGO_SINPROC='%s', DUP_ID_NIVEL=%s, DUP_ID_TIPOSABOR=%s
 WHERE COD_PRODUCTO = '%s';
"""

update_grouped_datacheck_query = """
UPDATE %s.%s SET COD_TAG='%s', DUP_PRODUCTO="%s", PRODUCTO_DESC='%s', DUP_ID_MARCA=%s,
DUP_ID_CONSISTENCIA = %s, DUP_ID_NIVEL_AZUCAR=%s, DUP_ID_FABRICANTE = %s, DUP_ID_EMPAQUE= %s, DUP_INTEGRAL_NO_INTEGRAL='%s',
DUP_OFERTA_PROMOCIONAL = '%s', DUP_IMPORTADO='%s', DUP_ID_PRESENTACION= %s, DUP_ID_CATEGORIA=%s, DUP_ID_SEGMENTO=%s,
DUP_ID_SUBMARCA = %s, DUP_ID_UNIDAD_MEDIDA=%s, DUP_ID_VARIEDAD=%s, DUP_ID_TIPO=%s, DUP_ID_TIPOCARNE=%s, DUP_ID_SUBTIPO=%s,
DUP_ID_SABOR=%s,RANGO_MIN=%s, RANGO_MAX=%s, TAMANO=%s, TAMANO_SINPROC='%s', RANGO_SINPROC='%s', DUP_ID_NIVEL=%s, DUP_ID_TIPOSABOR=%s, OBSERVACION='%s', ACTIVO=1
 WHERE COD_PRODUCTO = '%s';
"""

class LoadOps():

  def __init__(self, chunk_df, db_connection, pd, exestatus ):
    self.chunk_df = chunk_df
    self.db_connection = db_connection
    self.pd = pd
    self.exestatus = exestatus
    self.id_source = int(self.exestatus.process_filename.split("_")[2])
    self.chunk_df.fillna('', inplace=True)
    self.schema_name = DB_Connect().get_db_schema_name(self.id_source)
    self.__prepare_chunk_df()


  def __prepare_chunk_df(self):
    self.sku_df = self.chunk_df[self.chunk_df['ITEM_OR_GROUPED'] == 'I'].copy()
    self.grp_df = self.chunk_df[self.chunk_df['ITEM_OR_GROUPED'] == 'G'].copy()
    reporting.loa_logger.info("Separando los lotes en información agrupada y sku")
    if self.exestatus.get_status()['process_filename'].split("_")[1] != "DATACHECK":
      self.sku_df = self.sku_df[item_columns]
      self.sku_df.drop_duplicates(subset = ['COD_TAG'], inplace=True)
      self.grp_df = self.grp_df[grouped_columns]
      self.grp_df.drop_duplicates(subset = ['COD_TAG'], inplace=True)
    else:
      self.sku_df = self.sku_df[item_columns+['ACCION']]
      self.sku_df.drop_duplicates(subset = ['COD_TAG'], inplace=True)
      self.grp_df = self.grp_df[grouped_columns+['ACCION']]
      self.grp_df.drop_duplicates(subset = ['COD_TAG'], inplace=True)


  #TODO renombrar esta función
  def load_product_chunk_to_db(self):
    flag = False
    if self.exestatus.execution_mode == 'Reanudar':
      flag = True
    if self.sku_df[self.sku_df['NUEVO_PRODUCTO']=='T'].shape[0]!=0:
      self.sku_df.loc[self.sku_df['NUEVO_PRODUCTO']=='T', 'ID_DB'] = self.sku_df[self.sku_df['NUEVO_PRODUCTO']=='T'].apply(lambda row: self.load_sku_product_to_db(row, 'producto_sku'),  axis=1)
    if self.sku_df[self.sku_df['NUEVO_PRODUCTO']=='F'].shape[0]!=0:
      self.sku_df.loc[self.sku_df['NUEVO_PRODUCTO']=='F', 'ID_DB'] = self.sku_df[self.sku_df['NUEVO_PRODUCTO']=='F'].apply(lambda row: self.update_sku_product_to_db(row, 'producto_sku', flag),axis=1)
    reporting.loa_logger.info('Cantidad de productos nuevos %s, cantidad de productos actualizados: %s' %
    ( self.sku_df[self.sku_df['NUEVO_PRODUCTO']=='T'].shape[0], 
      self.sku_df[self.sku_df['NUEVO_PRODUCTO']=='F'].shape[0]))
    #same process to grp_df
    if self.grp_df[self.grp_df['NUEVO_PRODUCTO']=='T'].shape[0]!=0:
      self.grp_df.loc[self.grp_df['NUEVO_PRODUCTO']=='T', 'ID_DB'] = self.grp_df[self.grp_df['NUEVO_PRODUCTO']=='T'].apply(lambda row: self.load_sku_product_to_db(row, 'producto_agrupado'),  axis=1)
    if self.grp_df[self.grp_df['NUEVO_PRODUCTO']=='F'].shape[0]!=0:
      self.grp_df.loc[self.grp_df['NUEVO_PRODUCTO']=='F', 'ID_DB'] = self.grp_df[self.grp_df['NUEVO_PRODUCTO']=='F'].apply(lambda row: self.update_sku_product_to_db(row, ' producto_agrupado', flag),axis=1)
    reporting.loa_logger.info('Cantidad de productos agrupados nuevos %s, cantidad de productos actualizados: %s' %
    (self.grp_df[self.grp_df['NUEVO_PRODUCTO']=='T'].shape[0], 
      self.grp_df[self.grp_df['NUEVO_PRODUCTO']=='F'].shape[0]))
    if flag:
      self.exestatus.qty_prod_updated_on_db = self.exestatus.qty_prod_updated_on_db + self.grp_df[self.grp_df['NUEVO_PRODUCTO']=='F'].shape[0] + self.sku_df[self.sku_df['NUEVO_PRODUCTO']=='F'].shape[0]

    if self.sku_df.shape[0] !=0 :
      sku_df_map = self.sku_df.loc[:, ['COD_TAG', 'ID_DB']]
    else:
      sku_df_map = pd.DataFrame(columns=['COD_TAG', 'ID_DB'])
    if self.grp_df.shape[0] !=0 :
      grp_df_map = self.grp_df.loc[:, ['COD_TAG', 'ID_DB']]
    else:
      grp_df_map = pd.DataFrame(columns=['COD_TAG', 'ID_DB'])

    #finally remove the colummns from the data chunk
    tags_df = self.pd.concat([sku_df_map, grp_df_map])
    self.chunk_df = self.pd.merge(self.chunk_df, tags_df, how='left', on='COD_TAG')
    self.chunk_df.drop(columns=item_columns, axis=1, inplace=True)

  def datacheck_product_load_to_db(self):
    #pdb.set_trace() #debug function delete when done
    if not self.sku_df.empty:
      size_of_new_datacheck_products = self.sku_df[(self.sku_df['ACCION']=="IN") & (self.sku_df['NUEVO_PRODUCTO']=="T")].shape[0]
      size_of_upd_datacheck_products = self.sku_df[(self.sku_df['ACCION']=="IN") & (self.sku_df['NUEVO_PRODUCTO']=="F")].shape[0]
      self.exestatus.qty_prod_updated_on_db = self.exestatus.qty_prod_updated_on_db + size_of_upd_datacheck_products
      if size_of_new_datacheck_products != 0:
        self.sku_df[(self.sku_df['ACCION']=='IN') & (self.sku_df['NUEVO_PRODUCTO']=='T'), 'ID_DB'] = \
        self.sku_df.loc[(self.sku_df['ACCION']=='IN') & (self.sku_df['NUEVO_PRODUCTO']=='T'),:].apply(
          lambda row:self.load_sku_product_to_db(row, 'producto_sku'), axis = 1)
      if size_of_upd_datacheck_products != 0:
        self.sku_df[(self.sku_df['ACCION']=='IN') & (self.sku_df['NUEVO_PRODUCTO']=='F'), 'ID_DB'] = \
        self.sku_df.loc[(self.sku_df['ACCION']=='IN') & (self.sku_df['NUEVO_PRODUCTO']=='F'),:].apply(
          lambda row:self.update_sku_product_to_db(row, 'producto_sku', True), axis = 1)
    #same process to grp_df
    if not self.grp_df.empty:
      size_of_new_datacheck_products = self.grp_df[(self.grp_df['ACCION']=='IN') & (self.grp_df['NUEVO_PRODUCTO']=='T')].shape[0]
      size_of_upd_datacheck_products = self.grp_df[(self.grp_df['ACCION']=='IN') & (self.grp_df['NUEVO_PRODUCTO']=='F')].shape[0]
      self.exestatus.qty_prod_updated_on_db = self.exestatus.qty_prod_updated_on_db + size_of_upd_datacheck_products
      if size_of_new_datacheck_products != 0:
        mask = []
        self.grp_df.loc[(self.grp_df['ACCION']=='IN') & (self.grp_df['NUEVO_PRODUCTO']=='T'), 'ID_DB'] = \
        self.grp_df[(self.grp_df['ACCION']=='IN') & (self.grp_df['NUEVO_PRODUCTO']=='T')].apply(
          lambda row:self.load_sku_product_to_db(row, 'producto_agrupado'), axis = 1)
      if size_of_upd_datacheck_products != 0:
        #pdb.set_trace()
        self.grp_df.loc[(self.grp_df['ACCION']=='IN') & (self.grp_df['NUEVO_PRODUCTO']=='F'), 'ID_DB'] = \
        self.grp_df[(self.grp_df['ACCION']=='IN') & (self.grp_df['NUEVO_PRODUCTO']=='F')].apply(
          lambda row:self.update_sku_product_to_db(row, 'producto_agrupado', True), axis = 1)
      
    #after map each tag with the product ID from de database
    if self.sku_df.shape[0] !=0 :
      sku_df_map = self.sku_df.loc[:, ['COD_TAG', 'ID_DB']]
    else:
      sku_df_map = pd.DataFrame(columns=['COD_TAG', 'ID_DB'], type=str)
    if self.grp_df.shape[0] !=0 :
      grp_df_map = self.grp_df.loc[:, ['COD_TAG', 'ID_DB']]
    else:
      grp_df_map = pd.DataFrame(columns=['COD_TAG', 'ID_DB'], type=str)

    tags_df = self.pd.concat([sku_df_map, grp_df_map])
    self.chunk_df = self.pd.merge(self.chunk_df, tags_df, how='left', on='COD_TAG')
    self.chunk_df.drop(columns=item_columns, axis=1, inplace=True)

  def load_to_trx_to_tmp_table(self, engine):
    #pdb.set_trace()
    tmp_table = self.chunk_df.loc[self.chunk_df['ACCION'] == 'OUT' , ['FILENAME', 'NUMERO_LINEA_ARCHIVO']]
    deleted_records = 0
    if not tmp_table.empty:
      with engine.begin() as conn:
        try:
          reporting.loa_logger.info("Creando tabla tmp_out_table en la base de datos")
          tmp_table.to_sql('tmp_out_table', schema=self.schema_name, con=conn, if_exists='replace')
          reporting.loa_logger.info("Cración de tabla temporal finalizada")
          #call function on the database to delete records with the infoormation in this table
        except  sqlalchemy.exc.DatabaseError as dberror:
          status = self.exestatus.get_status()
          message = "Error durante el borrado de transacciones en la Base de datos: " + str(dberror.orig)
          raise LoadingDatabaseError(status['process_filename'],  status['chunk_counter'], message)
        else:
          #self.chunk_df['PERIODO'] = "10-05-2010" # TEMPORAL PARA HACER PRUEBAS EN EL INSUMO
          self.chunk_df = self.chunk_df.loc[self.chunk_df['ACCION'] == 'IN']
          self.chunk_df.drop(columns=['ACCION'], axis=1, inplace=True)
      try:
        cursor = self.db_connection.cursor()
        reporting.loa_logger.info("""DELETE %s.TRANSACCIONAL FROM %s.TRANSACCIONAL INNER JOIN 
        %s.TMP_OUT_TABLE ON transaccional.NOMBRE_ARCHIVO = TMP_OUT_TABLE.filename AND
        transaccional.NUMERO_LINEA_ARCHIVO = TMP_OUT_TABLE.NUMERO_LINEA_ARCHIVO""" 
        % (self.schema_name, self.schema_name, self.schema_name))
        reporting.loa_logger.info("""DELETE %s.transaccional_agrupado FROM %s.transaccional_agrupado INNER JOIN 
        %s.TMP_OUT_TABLE ON transaccional_agrupado.NOMBRE_ARCHIVO = TMP_OUT_TABLE.FILENAME AND
        transaccional_agrupado.NUMERO_LINEA_ARCHIVO = TMP_OUT_TABLE.NUMERO_LINEA_ARCHIVO""" 
        % (self.schema_name, self.schema_name, self.schema_name))
        cursor.execute("""DELETE %s.TRANSACCIONAL FROM %s.TRANSACCIONAL INNER JOIN 
        %s.TMP_OUT_TABLE ON transaccional.NOMBRE_ARCHIVO = TMP_OUT_TABLE.filename AND
        transaccional.NUMERO_LINEA_ARCHIVO = TMP_OUT_TABLE.NUMERO_LINEA_ARCHIVO""" 
        % (self.schema_name, self.schema_name, self.schema_name))
        deleted_records = cursor.rowcount
        cursor.execute("""DELETE %s.transaccional_agrupado FROM %s.transaccional_agrupado INNER JOIN 
        %s.TMP_OUT_TABLE ON transaccional_agrupado.NOMBRE_ARCHIVO = TMP_OUT_TABLE.FILENAME AND
        transaccional_agrupado.NUMERO_LINEA_ARCHIVO = TMP_OUT_TABLE.NUMERO_LINEA_ARCHIVO""" 
        % (self.schema_name, self.schema_name, self.schema_name))
        deleted_records = deleted_records+ cursor.rowcount
      except Exception as e:
        reporting.loa_logger.error("Ocurrio un error durante el proceso de DATACHECK")
        raise e
      else:
        reporting.loa_logger.info("Realizando operación de actualización de datacheck en la base de datos")
        self.db_connection.commit()
        pass
      finally:
        cursor.close()
    return deleted_records



  #TODO add column usuario, fechaprocesamiento to db
  #NOMBRE_ARCHIVO aumentar longitud de almacenamiento
  #FECHA_CREACION en ambas tablas debe tener la fecha actual en el momento de creacion
  def load_transactions_chunk_to_db(self, engine):
    self.chunk_df.drop(columns=['FECHA_CREACION', 'FECHA_ACTUALIZACION', 'FECHA_DESACTIVACION', 'FECHA_REPROCESO',
      'OBSERVACION', 'ID', 'TMP_AUX', 'ACTIVO', 'FECHA_PROCESAMIENTO'], axis = 1, inplace=True)
    self.chunk_df.rename(columns={'ID_DB':'ID_PRODUCTO', 'MERCADO':'ID_MERCADO', 'FILENAME':'NOMBRE_ARCHIVO'}, inplace=True)
    self.chunk_df['PERIODO'] = self.pd.to_datetime(self.chunk_df['PERIODO'], format='%d-%m-%Y')
    self.chunk_df.loc[:, number_columns] = self.chunk_df[number_columns].replace('', '0').astype('float32')
    self.sku_trx_df = self.chunk_df.loc[self.chunk_df['ITEM_OR_GROUPED'] == 'I', :].copy()
    self.grp_trx_df = self.chunk_df.loc[self.chunk_df['ITEM_OR_GROUPED'] == 'G', :].copy()
    self.sku_trx_df.drop(columns=['ITEM_OR_GROUPED'], axis = 1, inplace=True)
    self.grp_trx_df.drop(columns=['ITEM_OR_GROUPED'], axis = 1, inplace=True)
    with engine.begin() as conn:
      try:
        self.sku_trx_df.to_sql('transaccional', schema=self.schema_name,con=conn,  if_exists = 'append', index=False)
        reporting.loa_logger.info('Finaliza carga de datos transaccionales de la partición')
        self.grp_trx_df.to_sql('transaccional_agrupado', schema=self.schema_name,con=conn,  if_exists = 'append', index=False ) #en caso de error verificar el chunksize
        reporting.loa_logger.info('Finaliza carga de datos transaccionales agrupados de la partición')
      except sqlalchemy.exc.DatabaseError as dberror:
        status = self.exestatus.get_status()
        message = "Error de carga a la base de datos: " + str(dberror.orig)
        #TODO verificar la mejor manera de obtener el error code de la base de datos
        raise LoadingDatabaseError(status['process_filename'],  status['chunk_counter'], message)


  #TODO renombrar esta función y revisar una mejor manera de crear el query
  def load_sku_product_to_db(self, row, table_name):
    """function that inserts a row of data into the product sku table"""
    if table_name == 'producto_sku':
      query = insert_query % (self.schema_name, table_name, "COD_EAN", "COD_TAG", "DUP_PRODUCTO", "PRODUCTO_DESC", "DUP_ID_MARCA", "DUP_ID_CONSISTENCIA", "DUP_ID_NIVEL_AZUCAR",
      "DUP_ID_FABRICANTE", "DUP_ID_EMPAQUE", "DUP_INTEGRAL_NO_INTEGRAL", "DUP_OFERTA_PROMOCIONAL", "DUP_IMPORTADO", "DUP_ID_PRESENTACION",
      "DUP_ID_CATEGORIA", "DUP_ID_SEGMENTO", "DUP_ID_SUBMARCA", "DUP_ID_UNIDAD_MEDIDA", "DUP_ID_VARIEDAD", "DUP_ID_TIPO", "DUP_ID_TIPOCARNE", "DUP_ID_SUBTIPO",
      "DUP_ID_SABOR", "RANGO_MIN", "RANGO_MAX", "TAMANO", "TAMANO_SINPROC", "RANGO_SINPROC", "DUP_ID_NIVEL", "DUP_ID_TIPOSABOR",
      #row values  
      row["COD_EAN"], row["COD_TAG"], row["DUP_PRODUCTO"].replace('"', ""), row["PRODUCTO_DESC"], row["DUP_ID_MARCA"], row["DUP_ID_CONSISTENCIA"], row["DUP_ID_NIVEL_AZUCAR"],
      row["DUP_ID_FABRICANTE"], row["DUP_ID_EMPAQUE"], row["DUP_INTEGRAL_NO_INTEGRAL"], row["DUP_OFERTA_PROMOCIONAL"], row["DUP_IMPORTADO"], row["DUP_ID_PRESENTACION"],
      row["DUP_ID_CATEGORIA"], row["DUP_ID_SEGMENTO"], row["DUP_ID_SUBMARCA"], row["DUP_ID_UNIDAD_MEDIDA"], row["DUP_ID_VARIEDAD"], row["DUP_ID_TIPO"], row["DUP_ID_TIPOCARNE"], row["DUP_ID_SUBTIPO"],
      row["DUP_ID_SABOR"], row["RANGO_MIN"], row["RANGO_MAX"], row["TAMANO"], row["TAMANO_SINPROC"], row["RANGO_SINPROC"], row["DUP_ID_NIVEL"], row["DUP_ID_TIPOSABOR"])
    else:
      query = insert_grouped_query % (self.schema_name, table_name, "COD_TAG", "DUP_PRODUCTO", "PRODUCTO_DESC", "DUP_ID_MARCA", "DUP_ID_CONSISTENCIA", "DUP_ID_NIVEL_AZUCAR",
      "DUP_ID_FABRICANTE", "DUP_ID_EMPAQUE", "DUP_INTEGRAL_NO_INTEGRAL", "DUP_OFERTA_PROMOCIONAL", "DUP_IMPORTADO", "DUP_ID_PRESENTACION",
      "DUP_ID_CATEGORIA", "DUP_ID_SEGMENTO", "DUP_ID_SUBMARCA", "DUP_ID_UNIDAD_MEDIDA", "DUP_ID_VARIEDAD", "DUP_ID_TIPO", "DUP_ID_TIPOCARNE", "DUP_ID_SUBTIPO",
      "DUP_ID_SABOR", "RANGO_MIN", "RANGO_MAX", "TAMANO", "TAMANO_SINPROC", "RANGO_SINPROC", "DUP_ID_NIVEL", "DUP_ID_TIPOSABOR",
      #row values  
      row["COD_TAG"], row["DUP_PRODUCTO"].replace('"', ""), row["PRODUCTO_DESC"], row["DUP_ID_MARCA"], row["DUP_ID_CONSISTENCIA"], row["DUP_ID_NIVEL_AZUCAR"],
      row["DUP_ID_FABRICANTE"], row["DUP_ID_EMPAQUE"], row["DUP_INTEGRAL_NO_INTEGRAL"], row["DUP_OFERTA_PROMOCIONAL"], row["DUP_IMPORTADO"], row["DUP_ID_PRESENTACION"],
      row["DUP_ID_CATEGORIA"], row["DUP_ID_SEGMENTO"], row["DUP_ID_SUBMARCA"], row["DUP_ID_UNIDAD_MEDIDA"], row["DUP_ID_VARIEDAD"], row["DUP_ID_TIPO"], row["DUP_ID_TIPOCARNE"], row["DUP_ID_SUBTIPO"],
      row["DUP_ID_SABOR"], row["RANGO_MIN"], row["RANGO_MAX"], row["TAMANO"], row["TAMANO_SINPROC"], row["RANGO_SINPROC"], row["DUP_ID_NIVEL"],row["DUP_ID_TIPOSABOR"])

    cursor = self.db_connection.cursor()

    try:
      cursor.execute(query)
      self.db_connection.commit()
      cursor.execute("SELECT COD_PRODUCTO FROM %s.%s WHERE ID=%s" % (self.schema_name, table_name, cursor.lastrowid))
      return cursor.fetchone()[0]
    except myconnlib.errors.IntegrityError as e:
      reporting.loa_logger.warning("valor duplicado por optimización apply no error real revisar como manejar esta excepcion")
      reporting.loa_logger.error(query)
      reporting.loa_logger.error(row)
      reporting.loa_logger.error("SELECT COD_PRODUCTO FROM %s.%s WHERE ID=%s" % (self.schema_name, table_name, cursor.lastrowid))
    except Exception as e:
      reporting.loa_logger.info(query)
      reporting.loa_logger.error(e)
      pass
    finally:
      cursor.close()
    return "ERROR"


  def update_sku_product_to_db(self, row, table_name, update_to_db=False):
    if table_name == 'producto_sku':
      if self.exestatus.get_status()['process_filename'].split("_")[1] != "DATACHECK":
        query = update_query %(self.schema_name, table_name,
        row["COD_EAN"], row["COD_TAG"], row["DUP_PRODUCTO"], row["PRODUCTO_DESC"], row["DUP_ID_MARCA"], row["DUP_ID_CONSISTENCIA"], row["DUP_ID_NIVEL_AZUCAR"],
        row["DUP_ID_FABRICANTE"], row["DUP_ID_EMPAQUE"], row["DUP_INTEGRAL_NO_INTEGRAL"], row["DUP_OFERTA_PROMOCIONAL"], row["DUP_IMPORTADO"], row["DUP_ID_PRESENTACION"],
        row["DUP_ID_CATEGORIA"], row["DUP_ID_SEGMENTO"], row["DUP_ID_SUBMARCA"], row["DUP_ID_UNIDAD_MEDIDA"], row["DUP_ID_VARIEDAD"], row["DUP_ID_TIPO"], row["DUP_ID_TIPOCARNE"], row["DUP_ID_SUBTIPO"],
        row["DUP_ID_SABOR"], row["RANGO_MIN"], row["RANGO_MAX"], row["TAMANO"], row["TAMANO_SINPROC"], row["RANGO_SINPROC"], row["DUP_ID_NIVEL"], row['DUP_ID_TIPOSABOR'],
        row['COD_PRODUCTO'])
      else:
        OBSERVACION = "Modificado por archivo %s" % self.exestatus.get_status()['process_filename']
        query = update_datacheck_query %(self.schema_name, table_name,
        row["COD_EAN"], row["COD_TAG"], row["DUP_PRODUCTO"], row["PRODUCTO_DESC"], row["DUP_ID_MARCA"], row["DUP_ID_CONSISTENCIA"], row["DUP_ID_NIVEL_AZUCAR"],
        row["DUP_ID_FABRICANTE"], row["DUP_ID_EMPAQUE"], row["DUP_INTEGRAL_NO_INTEGRAL"], row["DUP_OFERTA_PROMOCIONAL"], row["DUP_IMPORTADO"], row["DUP_ID_PRESENTACION"],
        row["DUP_ID_CATEGORIA"], row["DUP_ID_SEGMENTO"], row["DUP_ID_SUBMARCA"], row["DUP_ID_UNIDAD_MEDIDA"], row["DUP_ID_VARIEDAD"], row["DUP_ID_TIPO"], row["DUP_ID_TIPOCARNE"], row["DUP_ID_SUBTIPO"],
        row["DUP_ID_SABOR"], row["RANGO_MIN"], row["RANGO_MAX"], row["TAMANO"], row["TAMANO_SINPROC"], row["RANGO_SINPROC"], row["DUP_ID_NIVEL"], row['DUP_ID_TIPOSABOR'],
        OBSERVACION, row['COD_PRODUCTO'])
        row['ID_DB'] = row['COD_PRODUCTO']
    else:
      #pdb.set_trace()
      if self.exestatus.get_status()['process_filename'].split("_")[1] != "DATACHECK":
        query = update_grouped_query %( self.schema_name, table_name,
        row["COD_TAG"], row["DUP_PRODUCTO"], row["PRODUCTO_DESC"], row["DUP_ID_MARCA"], row["DUP_ID_CONSISTENCIA"], row["DUP_ID_NIVEL_AZUCAR"],
        row["DUP_ID_FABRICANTE"], row["DUP_ID_EMPAQUE"], row["DUP_INTEGRAL_NO_INTEGRAL"], row["DUP_OFERTA_PROMOCIONAL"], row["DUP_IMPORTADO"], row["DUP_ID_PRESENTACION"],
        row["DUP_ID_CATEGORIA"], row["DUP_ID_SEGMENTO"], row["DUP_ID_SUBMARCA"], row["DUP_ID_UNIDAD_MEDIDA"], row["DUP_ID_VARIEDAD"], row["DUP_ID_TIPO"], row["DUP_ID_TIPOCARNE"], row["DUP_ID_SUBTIPO"],
        row["DUP_ID_SABOR"], row["RANGO_MIN"], row["RANGO_MAX"], row["TAMANO"], row["TAMANO_SINPROC"], row["RANGO_SINPROC"], row["DUP_ID_NIVEL"], row['DUP_ID_TIPOSABOR'],
        row['COD_PRODUCTO'])
      else:
        OBSERVACION = "Modificado por archivo %s" % self.exestatus.get_status()['process_filename']
        query = update_grouped_datacheck_query %(self.schema_name, table_name,
        row["COD_TAG"], row["DUP_PRODUCTO"], row["PRODUCTO_DESC"], row["DUP_ID_MARCA"], row["DUP_ID_CONSISTENCIA"], row["DUP_ID_NIVEL_AZUCAR"],
        row["DUP_ID_FABRICANTE"], row["DUP_ID_EMPAQUE"], row["DUP_INTEGRAL_NO_INTEGRAL"], row["DUP_OFERTA_PROMOCIONAL"], row["DUP_IMPORTADO"], row["DUP_ID_PRESENTACION"],
        row["DUP_ID_CATEGORIA"], row["DUP_ID_SEGMENTO"], row["DUP_ID_SUBMARCA"], row["DUP_ID_UNIDAD_MEDIDA"], row["DUP_ID_VARIEDAD"], row["DUP_ID_TIPO"], row["DUP_ID_TIPOCARNE"], row["DUP_ID_SUBTIPO"],
        row["DUP_ID_SABOR"], row["RANGO_MIN"], row["RANGO_MAX"], row["TAMANO"], row["TAMANO_SINPROC"], row["RANGO_SINPROC"], row["DUP_ID_NIVEL"], row['DUP_ID_TIPOSABOR'],
        OBSERVACION, row['COD_PRODUCTO'])
        #row['ID_DB'] = row['COD_PRODUCTO']
    if update_to_db:
      cursor = self.db_connection.cursor()
      try:
        cursor.execute(query)
        self.db_connection.commit()
        return row['COD_PRODUCTO']
      except Exception as e:
        reporting.loa_logger.error(e)
        pass
      finally:
        cursor.close()
      return row['ID_DB']
    else:
      return row['COD_PRODUCTO']


class DeleteOps(object):
  """Clase que permite el borrado de regesistrso de la base de datos"""
  def __init__(self, filename, conn, exestatus, params):
    self.filename= filename
    self.id_source = int(filename.split("_")[2])
    self.conn = conn
    self.exestatus = exestatus
    self.params = params
    self.schema_name = DB_Connect().get_db_schema_name(self.id_source)

  def delete_from_transaction(self):
    query = "DELETE FROM %s.transaccional WHERE NOMBRE_ARCHIVO = '%s'" % (self.schema_name, self.filename)
    query2 = "DELETE FROM %s.transaccional_agrupado WHERE NOMBRE_ARCHIVO = '%s'" % (self.schema_name, self.filename)
    cursor = self.conn.cursor()
    try:
      cursor.execute(query)
      qty_trn = cursor.rowcount
      cursor.execute(query2)
      qty_grp = cursor.rowcount
      self.conn.commit()
      self.exestatus.qty_cat_del_db = qty_trn + qty_grp
      reporting.loa_logger.info('Se eliminan %d registros de la tabla transaccional y %d de la transaccional agrupada' % (qty_trn, qty_grp))
    except Exception as e:
      status = self.exestatus.get_status()
      message = "Error durante borrando en la base de datos: " + str(e)
      raise DeleteDatabaseError(status['process_filename'],  status['chunk_counter'], message)
    finally:
      cursor.close()





class DB_Utils(object):
  """DB_Utils: Operaciones y funciones de gestion a la base de datos actualizaciones
  depuracion del proceso cada uno de estas funciones generan insumos que son utilizados
  para generar los indicadores de ejecución"""
  def __init__(self, exestatus, engine, id_source = 1):
    self.exestatus = exestatus
    self.engine = engine
    self.schema_name = DB_Connect().get_db_schema_name(id_source)
    self.conn = DB_Connect().get_connection()
  
  def load_item_volumen(self, dataframe):
    reporting.loa_logger.info("Cargando información de Item Volumen a base de datos")
    engine = self.engine
    with engine.begin() as conn:
      dataframe.to_sql('item_volumen', schema=self.schema_name,con=conn,  if_exists = 'replace', index=False)
    reporting.loa_logger.info("Finaliza carga de información de Item Volumen a base de datos: %s" % str(dataframe.shape[0]) )

  def load_dictionary(self, dataframe, table_name):
    reporting.loa_logger.info("Cargando información de diccionario a base de datos")
    engine = self.engine
    with engine.begin() as conn:
      dataframe.to_sql(table_name, schema=self.schema_name,con=conn,  if_exists = 'replace', index=False)
    reporting.loa_logger.info("Finaliza carga de información de diccionario %s a base de datos: %s" % (table_name, str(dataframe.shape[0]) ))


  def update_mst_table(self, table_name,column_name, old_value, new_value):
    """when a value in a catalog is updated this function is called to update that record on the database"""

    ini_config= DB_Connect()
    conn = ini_config.get_connection()

    cursor = conn.cursor()
    try:
      query = "UPDATE %s.%s SET %s = '%s' WHERE %s='%s'" % (self.schema_name, table_name, column_name, new_value, column_name,
        old_value)
      reporting.loa_logger.info(query)
      cursor.execute(query)
      conn.commit()
      reporting.loa_logger.info(cursor.rowcount)
      return cursor.rowcount
    except Exception as e:
      reporting.loa_logger.error(e)
      return -1
    finally:
      cursor.close()

  def __process_change_on_mst_table(self, tblMstName, id_original, id_toupdate,col_to_update, col_name, conn):
    """this function takes a pair of masters id's and it processes the changes on the master table and 
    product table"""
    cursor = conn.cursor()
    try:
      if tblMstName not in ['MERCADO', 'NIVEL']:
        cursor.execute("UPDATE {}.producto_sku SET {} = {} WHERE {} = {} AND ACTIVO=1".format(self.schema_name, col_to_update, id_original, col_to_update, id_toupdate))
        conn.commit()
        self.exestatus.qty_cat_upd_db = cursor.rowcount
        cursor.execute("UPDATE {}.producto_agrupado SET {} = {} WHERE {} = {} AND ACTIVO=1".format(self.schema_name, col_to_update, id_original, col_to_update, id_toupdate))
        conn.commit()
        self.exestatus.qty_cat_upd_db = self.exestatus.qty_cat_upd_db + cursor.rowcount
      elif tblMstName == 'MERCADO':
        cursor.execute("UPDATE {}.transaccional SET {} = {} WHERE {} = {}".format(self.schema_name, col_to_update, id_original, col_to_update, id_toupdate))
        conn.commit()
        self.exestatus.qty_cat_upd_db = cursor.rowcount
        cursor.execute("UPDATE {}.transaccional_agrupado SET {} = {} WHERE {} = {}".format(self.schema_name, col_to_update, id_original, col_to_update, id_toupdate))
        conn.commit()
        self.exestatus.qty_cat_upd_db = self.exestatus.qty_cat_upd_db + cursor.rowcount
      else:
        cursor.execute("UPDATE {}.producto_agrupado SET {} = {} WHERE {} = {}".format(self.schema_name, col_to_update, id_original, col_to_update, id_toupdate))
        conn.commit()
        self.exestatus.qty_cat_upd_db = cursor.rowcount

      cursor.execute("DELETE FROM {} WHERE {} = {}".format(tblMstName, col_name, id_toupdate))
      self.exestatus.qty_cat_del_db = cursor.rowcount
      conn.commit()
      #TODO log qty updated records
    except Exception as e:
      raise e
    finally:
      cursor.close()

  def __process_change_product_table(self, tblProdName, tblTransactionName, id_original, id_toupdate, conn):
    cursor = conn.cursor()
    try:
      cursor.execute("UPDATE {}.{} SET COD_PRODUCTO = {} WHERE COD_PRODUCTO = {}".format(self.schema_name, tblTransactionName, id_original, id_toupdate))
      conn.commit()
      self.exestatus.qty_cat_upd_db = self.exestatus.qty_cat_upd_db + cursor.rowcount 
      cursor.execute("UPDATE {}.{} SET ACTIVO = 0 SET OBSERVACION = 'Producto duplicado con {}' WHERE COD_PRODUCTO = {}".format(self.schema_name, tblProdName,id_original, id_toupdate))
      conn.commit()
      self.exestatus.qty_cat_del_db = self.exestatus.qty_cat_del_db + cursor.rowcount
    except Exception as e:
      raise e
    finally:
      pass


  def debug_mst_table(self, table_name):
    """Function which eliminates duplicate records on masters tables and updates the id's pointing to 
    them on the product tables and finally transaction tables if required 
    TODO change methodology for mercado and nivel tables 
    """
    error_code = 0
    ini_config= DB_Connect()
    conn = ini_config.get_connection()
    df = pd.read_sql("SELECT * FROM %s.%s" % (self.schema_name, table_name) , conn)

    col_subset = [col for col in df.columns if col.lower().startswith("dup")]
    col_prod_name = table_name.replace('MST', 'DUP_ID').replace('mst', 'DUP_ID')
    df_duplicates =df[df.duplicated(subset = col_subset, keep=False)].copy() #registros duplicados
    df_duplicates.sort_values(by= df_duplicates.columns[0], inplace = True) #se orfanizan los datos de acuerdo a los valores duplicados

    df_groups = df[df.duplicated(subset=col_subset, keep='first')].copy()

    df_toupdate = pd.merge(df_duplicates, df_groups, on=col_subset, how='inner',  suffixes=('_filas', '_grupo'))
    col_name_row   = [col for col in df_toupdate if col.lower().startswith("id") and col.endswith("_filas")][0] #selecciona el nombre de columna de id en filas
    col_name_group = [col for col in df_toupdate if col.lower().startswith("id") and col.endswith("_grupo")][0] #selecciona el nombre de columna de id en grupos

    #selecciona los id's diferentes pues estos indican los que se deben actualizar en la tabla producto
    df_toupdate = df_toupdate[df_toupdate[col_name_row] != df_toupdate[col_name_group]] #registros que se deben modificarr
    #en este caso si el apply se realiza dos veces no sería importante porque las operaciones son update y delete
    try:
      MASTER_TABLE = table_name
      df_toupdate.apply(lambda x: self.__process_change_on_mst_table(MASTER_TABLE, x[col_name_row], x[col_name_group],col_prod_name, col_name_row.replace('_filas', ''), conn), axis=1)
      self.debug_prod_table()
      self.debug_prod_group_table()
    except Exception as e:
      #TODO log e
      print(e)
      error_code = -1
      return error_code
    finally:
      conn.close() #cerrar conexión
    
    return error_code


  def debug_market_table(self):
    error_code = 0
    ini_config= DB_Connect()
    conn = ini_config.get_connection()
    df = pd.read_sql("SELECT * FROM %s.mercado" % (self.schema_name), conn)

    col_subset = ['NOMBRE_MERCADO', 'MERCADO_DL']
    col_tran_name = 'ID_MERCADO'
    df_duplicates =df[df.duplicated(subset = col_subset, keep=False)].copy() #registros duplicados
    df_duplicates.sort_values(by= df_duplicates.columns[0], inplace = True) #se orfanizan los datos de acuerdo a los valores duplicados
    df_groups = df[df.duplicated(subset=col_subset, keep='first')].copy()

    df_toupdate = pd.merge(df_duplicates, df_groups, on=col_subset, how='inner',  suffixes=('_filas', '_grupo'))
    col_name_row   = 'ID_MERCADO_filas' #selecciona el nombre de columna de id en filas
    col_name_group = 'ID_MERCADO_grupo' #selecciona el nombre de columna de id en grupos

    #selecciona los id's diferentes pues estos indican los que se deben actualizar en la tabla producto
    df_toupdate = df_toupdate[df_toupdate[col_name_row] != df_toupdate[col_name_group]] #registros que se deben modificarr
    #en este caso si el apply se realiza dos veces no sería importante porque las operaciones son update y delete
    try:
      df_toupdate.apply(lambda x: self.__process_change_on_mst_table('MERCADO', x[col_name_row], x[col_name_group],col_tran_name, 'ID_MERCADO', conn), axis=1)
    except Exception as e:
      #TODO log e
      print(e)
      error_code = -1
      return error_code
    finally:
      conn.close() #cerrar conexión
    return error_code

  def debug_level_table(self):
    error_code = 0
    ini_config= DB_Connect()
    conn = ini_config.get_connection()
    df = pd.read_sql("SELECT * FROM %s.nivel" % (self.schema_name), conn)

    col_subset = ['NIVEL', 'NIVEL_LARGO']
    col_tran_name = 'ID_NIVEL'
    df_duplicates =df[df.duplicated(subset = col_subset, keep=False)].copy() #registros duplicados
    df_duplicates.sort_values(by= df_duplicates.columns[0], inplace = True) #se orfanizan los datos de acuerdo a los valores duplicados
    df_groups = df[df.duplicated(subset=col_subset, keep='first')].copy()

    df_toupdate = pd.merge(df_duplicates, df_groups, on=col_subset, how='inner',  suffixes=('_filas', '_grupo'))
    col_name_row   = 'ID_NIVEL_filas' #selecciona el nombre de columna de id en filas
    col_name_group = 'ID_NIVEL_grupo' #selecciona el nombre de columna de id en grupos

    #selecciona los id's diferentes pues estos indican los que se deben actualizar en la tabla producto
    df_toupdate = df_toupdate[df_toupdate[col_name_row] != df_toupdate[col_name_group]] #registros que se deben modificarr
    #en este caso si el apply se realiza dos veces no sería importante porque las operaciones son update y delete
    try:
      df_toupdate.apply(lambda x: self.__process_change_on_mst_table('NIVEL', x[col_name_row], x[col_name_group],col_tran_name, 'ID_NIVEL', conn), axis=1)
      self.debug_prod_group_table()
    except Exception as e:
      #TODO log e
      print(e)
      error_code = -1
    finally:
      conn.close() #cerrar conexión
    return error_code    
    

  def debug_prod_table(self):
    """Eliminates duplicated records on the producto_sku table and updates these registers on the transactional
    table"""
    ini_config= DB_Connect()
    conn = ini_config.get_connection()

    # TODO revisar esta metodología cuando existan muchos productos
    df = pd.read_sql("SELECT * FROM %s.producto_sku WHERE ACTIVO=1" % (self.schema_name) , conn) 

    col_subset = [col for col in df.columns[2:]]
    df_duplicates =df[df.duplicated(subset = col_subset, keep=False)].copy()
    df_duplicates.sort_values(by= df_duplicates.columns[0], inplace = True)

    df_groups = df[df.duplicated(subset=col_subset, keep='first')].copy()
    df_toupdate = pd.merge(df_duplicates, df_groups, on=col_subset, how='inner',  suffixes=('_filas', '_grupo'))
    col_name_row = [col for col in df_toupdate if col.lower().startswith("id") and col.endswith("_filas")][0]
    col_name_group= [col for col in df_toupdate if col.lower().startswith("id") and col.endswith("_grupo")][0]

    df_toupdate = df_toupdate[df_toupdate[col_name_row] != df_toupdate[col_name_group]]
    df_toupdate.apply(lambda x: updateProducto("PRODUCTO_SKU", "TRANSACCIONAL", x[col_name_row], x[col_name_group], conn), axis=1)


  def debug_prod_group_table(self):
    """"""
    ini_config= DB_Connect()
    conn = ini_config.get_connection()

    # TODO revisar esta metodología cuando existan muchos productos
    df = pd.read_sql("SELECT * FROM %s.producto_agrupado WHERE ACTIVO=1" % (self.schema_name), conn) 

    col_subset = [col for col in df.columns[2:]]
    df_duplicates =df[df.duplicated(subset = col_subset, keep=False)].copy()
    df_duplicates.sort_values(by= df_duplicates.columns[0], inplace = True)

    df_groups = df[df.duplicated(subset=col_subset, keep='first')].copy()
    df_toupdate = pd.merge(df_duplicates, df_groups, on=col_subset, how='inner',  suffixes=('_filas', '_grupo'))
    col_name_row = [col for col in df_toupdate if col.lower().startswith("id") and col.endswith("_filas")][0]
    col_name_group= [col for col in df_toupdate if col.lower().startswith("id") and col.endswith("_grupo")][0]

    df_toupdate = df_toupdate[df_toupdate[col_name_row] != df_toupdate[col_name_group]]
    df_toupdate.apply(lambda x: updateProducto("producto_agrupado", "transaccional_agrupado", x[col_name_row], x[col_name_group], conn), axis=1)

  def change_tag_products(self, old_tag, new_tag):
    """Función que permite llamar el procedimiento almacenado para cambiar el valor de un tag en la tabla de productos
    tanto agrupado como sku. 
    TODO: PARAMETER CONN TO SELECT SCHEMA NAME"""  
    try:
      cursor = self.conn.cursor()

      args = [old_tag, new_tag, -99]

      result = cursor.callproc('change_tag_procedure', args)

      return result[2]
    except Exception as e:
      return -1
    finally:
      cursor.close()

  def remove_tag_products(self, old_tag):
    """Función que permite llamar el procedimiento almacenado para eliminar un tag de la base de datos"""  
    try:
      cursor = self.conn.cursor()

      args = [old_tag, -99]

      result = cursor.callproc('remove_tag_procedure', args)

      return result[1]
    except Exception as e:
      return -1
    finally:
      cursor.close()
      
      

    