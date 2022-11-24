import pandas as pd
import reporting
from load.load_operation import DB_Utils


class TransformController():

  def call_stored_procedure(self, df_tags, exestatus, engine):

    reporting.trn_logger.debug('Llamando procedimiento almacenado por cada fila')
    utl_obj = DB_Utils(exestatus, engine)
    ret_code = 0
    df_tags['RET_CODE'] = df_tags.apply(lambda x : 
      utl_obj.change_tag_products(x['TAG ANTERIOR'], x['TAG ACTUAL']), axis=1)
    if df_tags.loc[df_tags['RET_CODE']!=0, :].shape[0] != 0:
      reporting.trn_logger.warning('algunos tags no pudieron ser actualizados')
      ret_code = -1

    reporting.trn_logger.debug('Retornando valores')

    return (ret_code, df_tags)


  def call_remove_stored_procedure(self, df_tags, exestatus, engine):

    reporting.trn_logger.debug('Llamando procedimiento almacenado por cada fila')
    utl_obj = DB_Utils(exestatus, engine)
    ret_code = 0
    df_tags['RET_CODE'] = df_tags.apply(lambda x : 
      utl_obj.remove_tag_products(x['TAG']), axis=1)
    if df_tags.loc[df_tags['RET_CODE']!=0, :].shape[0] != 0:
      reporting.trn_logger.warning('algunos tags no pudieron ser actualizados')
      ret_code = -1

    reporting.trn_logger.debug('Retornando valores')

    return (ret_code, df_tags)




