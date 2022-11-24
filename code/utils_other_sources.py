

import pandas as pd
import numpy as np
import unidecode as ud
import shutil
import re
import logging
from transversal_classes import ProjectParameters, ExecutionStatus
from extract.read_and_validate_file import FileOpsReadAndValidations, simplify_catalogs
from extract.other_geo_validation import FileOpsOtherGeo, FileOpsOtherGeoTransformer, FileOpsIVOtherGeo
from enrichment.additional_variables import EnrichmentOpsOtherGeo
import os
import pdb

class LoaderOtherGeographies(object):
	"""clase que contiene la lógica de carga de otras geografías adminsistra el funcionamiento de la
	   carga desde la validación hasta el proceso de carga"""


	def __init__(self, id_source:int, complete_filename:str, params:ProjectParameters):
		self.id_source = id_source
		self.complete_filename = complete_filename
		self.params = params
		self.log_obj = logging.getLogger('loader_other_geo')

	def validate_raw_input_file(self, category):
		"""Dependiendo de la fuente se selecciona el método de validación."""
		if self.id_source == 3:
			self.log_obj.info("Validando archivo crudo de storecheck")
			params = self.params.getothergeoparameters()
			file_other_geo_obj = FileOpsOtherGeo(self.complete_filename, params)
			file_other_geo_obj.prepare_raw_input() #TODO add log messages
			file_other_geo_obj.create_additional_cols(category) #TODO add log messages
			file_other_geo_obj.create_derivated_cols()#TODO add log messages
			file_other_geo_obj.calculate_range_col()
			file_other_geo_obj.do_final_validations()
			df = file_other_geo_obj.df_input.copy()
			self.log_obj.info("Calculando peso y unidades por empaque de artículos que no lo tienen")
			enrichment_other_geo_obj = EnrichmentOpsOtherGeo(df)
			df = enrichment_other_geo_obj.obtain_size_and_uxe()
			del file_other_geo_obj #libero memoria del proceso
			self.log_obj.info("Finaliza validación archivo storecheck")
		elif self.id_source == 4:
			self.log_obj.info("validando archivo crudo de item volumen")
			params = self.params.getothergeoparameters()
			file_other_iv_geo_obj = FileOpsIVOtherGeo(self.complete_filename, "WSP_Sheet1" , params)
			file_other_iv_geo_obj.prepare_raw_input(params['iv_start_column'], params['iv_end_column'])
			file_other_iv_geo_obj.create_oasis_df()
			df = file_other_iv_geo_obj.give_oasis_format(category, params['iv_start_column'], params['iv_period_column'])

			self.log_obj.info("Calculando peso y unidades por empaque de artículos que no lo tienen")
			enrichment_other_geo_obj = EnrichmentOpsOtherGeo(df)
			df = enrichment_other_geo_obj.obtain_size_and_uxe_font_4()
			del file_other_iv_geo_obj

			self.log_obj.info("Finaliza validación de Item Volumen")
		return df

	def cross_with_catalogs(self, df):
		if self.id_source in [3,4] :
			input_files = os.listdir(self.params.CATALOG_FILES)
			catalog_files = []
			for infile in input_files:
				if infile.startswith("OASIS_MST"):
					catalog_files.append(infile)

			exestatus = ExecutionStatus()
			process_obj = FileOpsReadAndValidations(df, self.params.INPUT_COLUMNS, self.params.ADDITIONAL_DCHK_COLUMNS)
			process_obj.process_chunk(self.params.INPUT_STRING_COLUMNS, exestatus,  pd)
			bitheader = process_obj.cross_catalogs(catalog_files, 1, pd, self.params) # bit header is always true
			temp_catalog_files = os.listdir(self.params.TEMP_CATALOG_FILES)
			simplify_catalogs(temp_catalog_files, pd, self.params)
			return (len(temp_catalog_files), temp_catalog_files)

	def assign_temp_files_values_to_df(self, df):
		if self.id_source == 3:
			self.log_obj.info("Asignando valores temporales... ")
			for tmp_catalog in os.listdir(self.params.TEMP_CATALOG_FILES):
				self.log_obj.info(f"Iniciando proceso con catalogo temporal: {tmp_catalog}")
				df_oasis = pd.read_csv(self.params.TEMP_CATALOG_FILES + tmp_catalog, sep=';', encoding='latin', usecols=[0,1])
				col = tmp_catalog.replace('.csv','').split('_')[3]
				df = df.merge(df_oasis, how='left', on=col, copy=True)
				df.loc[~df[col+'_GN'].isna(), col] = df.loc[~df[col+'_GN'].isna(), col+'_GN']
				df.drop(col+'_GN',axis=1, inplace=True)
				self.log_obj.info(f"Reemplazo de valor en columna {col} completado")

		return df

	def transform_raw_input_to_oasis_input(self, df, filename):
		"""Dependiendo de la fuente transforma un dataframe de entrada en los valores OASIS para carga"""
		params = self.params.getothergeoparameters()
		file_transform_geo_obj = FileOpsOtherGeoTransformer(params, df)
		if self.id_source == 3:
			self.log_obj.info("Transformando información storecheck en información oasis")
			df_updated = file_transform_geo_obj.create_or_update_description_file(df)
			df_updated = file_transform_geo_obj.group_same_date_trxs(df_updated)
			qty_tmp_files, tmp_files_arr = self.cross_with_catalogs(df_updated)
			
		elif self.id_source == 4:
			self.log_obj.info("Transformando informaciónitem volumen panama en formato oasis ")
			df_updated = file_transform_geo_obj.group_same_date_trxs(df, params['iv_start_column'], "BARCODE",
				params['new_dist_columns'][0], params['new_dist_columns'][-1])
			qty_tmp_files, tmp_files_arr = self.cross_with_catalogs(df_updated)

		if qty_tmp_files != 0:
			self.log_obj.warning("diferencias con catálogos requiere información adicional")
		#return value the same in both cases
		return (qty_tmp_files, df_updated, tmp_files_arr)

	def export_oasis_file(self, df, complete_filename):
		output_filename = complete_filename #TODO implement a funtcion that transform de original filename
		str_cols = df.select_dtypes(exclude=[np.number, 'datetime64']).columns
		for col in str_cols: #self.params['input_oasis_file_columns']:
			self.log_obj.info(f"aplicando decodificación a columna: {col}")
			df[col] = df[col].apply(ud.unidecode)

		# into oasis_filename
		df.to_csv(output_filename, encoding="latin", decimal=".", sep=";", index=False, header=True)
		return True


class CleanerOtherGeo(object):

	def __init__(self, params):
		self.log_obj = logging.getLogger('cleaner_other_geo')
		self.params = params

	def remove_tmp_catalogs(self):
		self.log_obj.info(f"Borrando catalogos temporales")
		for tmp in os.listdir(self.params.TEMP_CATALOG_FILES):
			os.unlink(self.params.TEMP_CATALOG_FILES + tmp)
		self.log_obj.info(f"finaliza borrado de catalogo temporal")

	def move_temp_input_to_oasis_input(self, prefix_filename):
		"""cuando termina el proceso de pre oasis en la carpeta temporal copia los archivos procesados 
		en la carpeta de input files de OASIS para iniciar el proceso de carga normal"""
		for raw_tmp_file in os.listdir(self.params.TEMP_INPUT_FILES):
			if raw_tmp_file.startswith(prefix_filename):
				if raw_tmp_file.endswith('.csv'):
					self.log_obj.info(f"copiando archivo{raw_tmp_file} desde'{self.params.TEMP_INPUT_FILES}' a '{self.params.INPUT_FILES}'")
					shutil.copy(self.params.TEMP_INPUT_FILES + raw_tmp_file, self.params.INPUT_FILES + raw_tmp_file)
				self.log_obj.info(f"Borrando archivo: {self.params.TEMP_INPUT_FILES + raw_tmp_file}")
				os.unlink(self.params.TEMP_INPUT_FILES + raw_tmp_file)
				self.log_obj.info(f"finaliza proceso {raw_tmp_file}")

	def move_raw_input_to_temp_input(self, raw_filename, rename_filename, path_to_input, path_to_processed):
		"""Mueve una copia del archivo de entrada de storecheck crudo a la carpeta temporal y otra a la carpeta
		de procesados para no perder trazabilidad del archivo original"""
		params_other_geo = self.params.getothergeoparameters()
		compl_original_filename = params_other_geo[path_to_input] + raw_filename
		compl_new_filename = self.params.TEMP_INPUT_FILES + rename_filename
		self.log_obj.info(f"moviendo archivo {raw_filename}, a {self.params.TEMP_INPUT_FILES}")
		shutil.copy(compl_original_filename, compl_new_filename)
		self.log_obj.info(f"moviendo archivo {raw_filename}, a {params_other_geo[path_to_processed] + raw_filename}")
		shutil.copy(compl_original_filename, params_other_geo[path_to_processed] + raw_filename)
		os.unlink(compl_original_filename)

