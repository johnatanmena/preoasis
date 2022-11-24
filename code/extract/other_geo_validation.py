import pandas as pd
import numpy as np
import unidecode as ud
import logging
import re, os, math, locale
from datetime import datetime as dt
from exceptions import InvalidRawFileException, InvalidRangeFileException
import pdb

locale.setlocale(locale.LC_ALL,'') #configurar locale a español

class FileOpsOtherGeo(object):
	"""Case encargada de realizar las operaciones de validación implementa los métodos de validación
	de la fuente de datos """
	__DICT_RAW_DATA_TYPES = {'Rango':str, 'Subtipo':str, 'Tipo':str, 'Categoria_':str, 
		'Product_id':str, 'Upc':str}

	def __init__(self,project_filename:str, params:dict):
		self.project_filename = project_filename
		self.params = params
		self.log_obj = logging.getLogger('file_ops_prepare_raw')
		self.log_obj.info("leyendo archivo de Excel")
		self.df_input = pd.read_excel(project_filename, sheet_name="Hoja 1", engine="openpyxl", 
			dtype=self.__DICT_RAW_DATA_TYPES)
		self.rows, self.columns = self.df_input.shape
		self.log_obj.info("finaliza ejecución del init")

	def __read_ranges_from_file(self, sheet_name, str_cols, pattern_number, new_colnames=['Segmento', 'Rango']):
		"""Función encargada de retornar un dataframe listo para cruzar y obtener el rango"""
		file_structure_name = self.params['path_to_additional_files'] + self.params['name_structure_file']
		df_ranges = pd.read_excel(file_structure_name, sheet_name=sheet_name, engine="openpyxl", skiprows=2, usecols=str_cols,
			header=None, names=new_colnames)
		rows, cols = df_ranges.shape

		tmp_ranges = df_ranges['Rango'].str.extractall(pattern_number).reset_index().pivot(index='level_0',columns='match',
		  values='min')\
			.stack().str.replace(',','.').unstack().astype(float)

		df_ranges = pd.merge(df_ranges, tmp_ranges, left_index=True, right_index=True)
		df_ranges = df_ranges.fillna(9999)

		if df_ranges.shape[1] != 4:
			message = f"Cantidad de columnas creadas inválidas en los rangos de la hoja {sheet_name}"
			raise InvalidRangeFileException(message, file_structure_name)
		
		return df_ranges #finaliza función

	def search_range_from_df(self, segmento:str, valor:float, df, size:bool) -> str:
		"""función que recibe un valor y retorna el rango en valor de string asociado a ese valor"""
		if math.isnan(valor):
			return "SA"
		elif math.isinf(valor) or valor > 9999:
			valor = 9998

		if size:
			valor = int(valor)
		else:
			valor = round(valor,2)

		range = df.loc[(df['Segmento'] == segmento) & 
			(valor >= df[0]) & 
			(valor <= df[1]), 'Rango'].values[0]

		return range 


	def prepare_raw_input(self):
		"""Validaciones a la fuente"""
		self.log_obj.debug(f"""Cantidad columnas entrada {self.df_input.shape[1]}, 
		cantidad esperada: {len(self.params['input_source_file_columns'])}""")
		if self.df_input.shape[1] != len(self.params['input_source_file_columns']):
			self.log_obj.error(f"""error de formato""")
			message = """Cantidad de columnas en el archivo de entrada diferente a la esperada 
			(verificar caracteres especiales en los nombres de columnas)"""
			raise InvalidRawFileException(message, self.project_filename)
		elif set(self.params['input_source_file_columns']) != set(self.df_input.columns):
			cols_a = set(self.params['input_source_file_columns']).difference(set(self.df_input.columns))
			cols_b = set(self.df_input.columns).difference(set(self.params['input_source_file_columns']))
			message = f"Diferentes columnas esperadas en el archivo de entrada: {str(cols_a)} //\n comparado con :{str(cols_b)}"
			raise InvalidRawFileException(message, self.project_filename)
		else:
			# dataframe válido en estructura preparar campos nulos.
			# rellena valores nulos con valores oasis por defecto
			self.log_obj.info("Rellenando valores por defecto de OASIS")
			self.df_input = self.df_input.replace('-', np.nan)
			self.df_input.loc[:, self.params['market_columns']] = \
				self.df_input.loc[:, self.params['market_columns']].fillna("SA") #avoid error on market columns
			self.df_input.loc[:, self.df_input.select_dtypes(exclude=[np.number]).columns] = \
				self.df_input.select_dtypes(exclude=[np.number]).fillna("S/A")
			self.df_input.loc[:, self.df_input.select_dtypes(include=[np.number]).columns] = \
				self.df_input.select_dtypes(include=[np.number]).fillna(-1)
		return

	def create_additional_cols(self, category):
		"""Función que crea las columnas por defecto del archivo y renombra las ya existentes
		TODO: add log messages"""
		self.log_obj.info("Creando columnas de texto por defecto")
		self.df_input.rename(columns = self.params['input_rename_columns'], inplace = True) #renombrar
		for col_name in self.params['input_default_str_columns']: #columnas de texto
			if col_name == "CATEGORIA":
				self.df_input[col_name] = category ######## importante este valor se debe extraer del archivo de insumo se debe crear función
			elif col_name == "NIVEL":
				self.df_input[col_name] = "ITEM"
			elif col_name  in ['VARIEDAD', 'SABOR']:
				self.df_input[col_name] = self.df_input['TIPO']
			else:
				self.df_input[col_name] = "S/A"
		self.log_obj.info("Creando columnas de numericas por defecto")
		for col_name in self.params['input_default_num_columns']:
			self.df_input[col_name] = - 1
		return

	def create_derivated_cols(self):
		"""Función encargada del calculo de las columnas derivadas de información existente"""
		self.log_obj.info("Creando columnas derivadas para calculos de rango")
		self.df_input['PERIODO'] = pd.to_datetime(self.df_input['Ordered date'],format='%Y%m').dt.strftime("%d-%m-%Y")
		self.df_input['MERCADO'] = self.df_input['País'] + "/" + self.df_input['Provincia_Departamento'] \
		 + "/SUPERMERCADOS CADENAS/" + self.df_input['Formato'] + "/" + self.df_input['Cadena_'] + "/" + self.df_input['Cadena']
		self.df_input['Valor'] = pd.to_numeric(self.df_input['TAMANO'], errors='coerce') #columnas necesaria para formulas
		self.df_input['VENTAS_EN_VOLUMEN_KILOS_000'] = (self.df_input['Valor'] * self.df_input['Unidades'])/1000
		self.df_input.loc[self.df_input['VENTAS_EN_VOLUMEN_KILOS_000'].isnull(), 'VENTAS_EN_VOLUMEN_KILOS_000'] = -1
		self.df_input['Precio Multipack'] = (self.df_input['VENTAS_EN_VALOR_000000'] / 
			self.df_input['VENTAS_EN_VOLUMEN_KILOS_000'])*(self.df_input['Valor']/1000)
		self.df_input['Precio Individual'] = ( (self.df_input['VENTAS_EN_VALOR_000000'] /
		 self.df_input['VENTAS_EN_VOLUMEN_KILOS_000'])*(self.df_input['Valor']/1000))\
		 / self.df_input['DIST_POND_TIENDAS_VENDIENDO_MAX'] # (( a / b * c/1000) /d )
		self.df_input['Tamano Multipack'] = self.df_input['Valor']
		self.df_input['Tamano Individual'] = self.df_input['Valor'] / self.df_input['DIST_POND_TIENDAS_VENDIENDO_MAX']
		self.df_input.loc[self.df_input['Tamano Individual'] <= 0, ['Tamano Individual']] = self.df_input['Valor']
		self.df_input.loc[(self.df_input['DIST_POND_TIENDAS_VENDIENDO_MAX'] == 0), ['Tamano Individual']] = 9998
		self.df_input.loc[(self.df_input['Precio Multipack'] <=0) | (self.df_input['DIST_POND_TIENDAS_VENDIENDO_MAX'] == 0), 
		       ['Precio Individual', 'Precio Multipack']] = np.nan
		self.df_input.loc[(self.df_input['Precio Individual'] <=0) | (self.df_input['DIST_POND_TIENDAS_VENDIENDO_MAX'] == 0), 
		       ['Precio Individual', 'Precio Multipack']] = np.nan

		return

	def calculate_range_col(self):
		"""Función encargada de calcular la columna rango en la fuente de storecheck"""
		self.log_obj.info("Calculando rango 1 ...")
		df_ranges = self.__read_ranges_from_file("Rangos Tamaño", "A,B", r"(?P<min>\d+)")
		self.df_input['Rango1'] = self.df_input.apply(lambda row : self.search_range_from_df(row['SEGMENTO'], 
			row['Tamano Individual'], df_ranges, True), axis=1)
		self.log_obj.info("Calculando rango 2 ...")
		df_ranges = self.__read_ranges_from_file("Rangos Tamaño", "D,E", r"(?P<min>\d+)")
		self.df_input['Rango2'] = self.df_input.apply(lambda row : self.search_range_from_df(row['SEGMENTO'], 
			row['Tamano Multipack'], df_ranges, True), axis=1)
		self.log_obj.info("Calculando rango 3 ...")
		df_ranges = self.__read_ranges_from_file("Rangos Precio", "A,B", r"(?P<min>\d+(?:,\d+)?)")
		self.df_input['Rango3'] = self.df_input.apply(lambda row : self.search_range_from_df(row['SEGMENTO'], 
			row['Precio Individual'], df_ranges, True), axis=1)
		self.log_obj.info("Calculando rango 4 ...")
		df_ranges = self.__read_ranges_from_file("Rangos Precio", "D,E", r"(?P<min>\d+(?:,\d+)?)")
		self.df_input['Rango4'] = self.df_input.apply(lambda row : self.search_range_from_df(row['SEGMENTO'], 
			row['Precio Multipack'], df_ranges, True), axis=1)
		self.log_obj.info("Calculando rango Final")
		self.df_input['RANGO'] = self.df_input['Rango1'] + "/" + self.df_input['Rango2'] + "/" + self.df_input['Rango3']\
			+ "/" + self.df_input['Rango4']

	def do_final_validations(self):
		"""Función encargada de validar que todos los valores finales se encuentren de la manera esperada para pasar 
		los valores a formato OASIS hasta el momento se realizan cambios a la estructura del archivo pero no a 
		la forma final"""
		self.log_obj.info("Realizando validaciones finales ....")
		self.df_input = self.df_input.loc[:, self.params['input_oasis_file_columns']]
		if self.rows != self.df_input.shape[0]:
			message = "Error de clave duplicada en el proceso de preparación de datos"
			raise InvalidRawFileException(message, self.project_filename)
		if self.df_input.isnull().values.any():
			message = "Valores nulos en el dataframe validar el proceso"
			raise InvalidRawFileException(message, self.project_filename)
		self.log_obj.info("Preparación de datos crudos para storecheck finalizada con éxito")
		return


class FileOpsOtherGeoTransformer(object):
	"""Esta clase se encarga de encapsular la lógica de preparación de datos del archivo de insumo de storecheck al
	 formato OASIS, actualiza la información de la descripción de tags antes de la publicación de esta manera se puede 
	 tener trazabilidad del archivo antes de la publicación en el servidor FTP"""

	def __init__(self, params, df):
		self.params = params
		self.rows, self.cols = df.shape
		self.log_obj = logging.getLogger('file_ops_transform_raw')
		self.log_obj.info("Creado objeto de transformación")

	def search_dups_on_data (self, df, key_cols = ["TAG","PERIODO","MERCADO", "PRODUCTO"]):
		"""Dada una lista de columnas especiales define cuales son las transacciones duplicadas en la data
		retorna un booleano de acuerdo a si existe o no duplicados en la data"""
		trx = df.duplicated(subset=key_cols)
		tmp = df.loc[trx, :]
		if tmp.shape[0] != 0:
			return True
		else:
			return False

	def validate_duplicated_tags (self, df, key_cols=["TAG", "PRODUCTO"]):
		"""Genera archivo que contiene tags que pueden ser duplicados en el proceso"""
		self.log_obj.debug(f"Buscando duplicados en la data por {key_cols}")
		tag = df.drop_duplicates(subset=["TAG", "PRODUCTO"])
		tags_duplicados = tag.loc[tag.duplicated(subset=["TAG"]), "TAG"]
		self.log_obj.debug(f"encontrado(s) {tag.loc[tag['TAG'].isin(tags_duplicados), :].shape[0]}, en la data, generando archivo")
		tag.loc[tag['TAG'].isin(tags_duplicados), :]\
		.sort_values(by= key_cols).to_csv( self.params['path_to_additional_files'] + "TAGS_DUPLICADOS.csv", encoding="latin"
			, decimal=".", sep=";", index=False, header=True)
		return tags_duplicados

	def create_or_update_description_file(self, df, key_cols =["TAG", "PRODUCTO"], 
			description_filename="descripciones_tags.csv"):
		"""Función que genera un archivo que se utiliza para obtener una única descripción de producto"""
		complete_filename = self.params['path_to_additional_files'] + description_filename
		self.log_obj.info("calculando tags duplicados")
		tags_duplicados = self.validate_duplicated_tags(df, key_cols)
		tag_description_df = df.loc[df['TAG'].isin(tags_duplicados), :].groupby(by=key_cols, as_index=False)\
			['VENTAS_EN_VALOR_000000'].sum().groupby(by=['TAG'], as_index=False).max()
		tag_no_duplicated = df.loc[~df['TAG'].isin(tags_duplicados), key_cols].drop_duplicates()
		self.log_obj.info("unificando tags duplicados y no duplicados")
		tag_description_df = pd.concat([tag_description_df, tag_no_duplicated])#unificar todos los tags

		self.log_obj.info("creando archivo de descripciones")
		if not(os.path.exists(complete_filename)):
			tag_description_df.loc[:, key_cols].to_csv(complete_filename,	header=True, sep=";", index=False)

		df_description = pd.read_csv(complete_filename, sep=';', dtype={'TAG':object})
		col_order = df.columns #orden de columnas original

		df_merged = df.merge(df_description, how='left', on='TAG', suffixes=('','_file'), indicator= True)
		self.log_obj.info("actualizando archivo de descripciones")
		new_descriptions = df_merged.loc[df_merged['_merge'] == 'left_only', key_cols].drop_duplicates(subset=['TAG'])
		self.log_obj.info("escribiendo nuevas descripciones al archivo")
		new_descriptions.to_csv(complete_filename, header=False, sep=';', index=False, mode='a')

		self.log_obj.info("actualizando valores de descripciones en el dataframe original")
		df_description = pd.read_csv(complete_filename, sep=';', dtype={'TAG': object})
		df_merged = df.merge(df_description, how='left', on='TAG', suffixes=('','_file'), indicator= True)
		df_merged.drop(columns=['PRODUCTO', '_merge'], inplace=True)
		df_merged.rename(columns={"PRODUCTO_file": "PRODUCTO"}, inplace=True)
		df_merged = df_merged.loc[:,col_order]

		return df_merged

	def group_same_date_trxs(self, df, from_col="MERCADO", until_col= "BARCODE", 
		num_from_col = "DIST_TIENDAS_VENDEDORAS_POND", num_to_col="DIST_POND_TIENDAS_VENDIENDO_MAX"):
		"""Función encargada de agrupar todas las transacciones que ocurren en la misma fecha sumando los valores por defecto"""
		self.log_obj.info("unificando transacciones en la misma fecha")
		start  = df.columns.values.tolist().index(from_col)
		end = df.columns.values.tolist().index(until_col)
		df = df.groupby(by=df.columns.values.tolist()[start:end+1], as_index=False).sum()

		self.log_obj.info("valores por defecto para distribuciones")
		start  = df.columns.values.tolist().index(num_from_col)
		end = df.columns.values.tolist().index(num_to_col)
		df.loc[:, df.columns.values.tolist()[start:end]] = -1 # notice +1 not in the clausule this is ok

		return df

class FileOpsIVOtherGeo(object):
	"""Clase encargada de realizar las operaciones de validación de un Item Volumen, también implementa los métodos de validación
	de la fuente"""
	def __init__(self, project_filename, sheet_name, params):
		self.project_filename = project_filename
		self.params = params
		self.log_obj = logging.getLogger('file_ops_prepare_raw_item_volumen')
		self.log_obj.info("leyendo archivo de Excel de Item Volumen")
		self.df_input = pd.read_excel(project_filename, sheet_name=sheet_name, engine="openpyxl")
		self.rows, self.columns = self.df_input.shape
		self.df_data = self.df_values = None
		self.log_obj.info("finaliza ejecución del init")

	def __validate_input(self, end_index):
		for col in self.params['input_item_volumen_necesary_columns']:
			if col not in self.df_input.columns.to_list():
				message = f"La columna '{col}' no se encuentra en el archivo de entrada"
				raise InvalidRawFileException(message, self.project_filename)
		for col in self.df_input.columns.to_list()[end_index:]:
			try:
				dt.strptime(col, '%B %Y') #intentar conversión a fecha
			except ValueError as e:
				message = f"La columna '{col}' no tiene un formato de fecha válido, corregir nombre mes completo: MES AÑO - %B %Y"
				raise InvalidRawFileException(message, self.project_filename)
		self.log_obj.info("archivo con un formato válido")


	def prepare_raw_input(self, start_col = 'MERCADO', end_col='TAG'):
		self.log_obj.debug(f"""Iniciando transformación del archivo...""")
		self.log_obj.debug(f"""Cantidad de filas inicial {self.rows} y columnas inicial {self.columns}""")
		self.df_input = self.df_input[~self.df_input['TAG'].isnull()] #eliminar registros sin tag
		self.df_input['VARIABLE'].fillna(method='ffill', inplace=True)
		start_index = self.df_input.columns.to_list().index(start_col)
		end_index	  = self.df_input.columns.to_list().index(end_col) + 1
		self.df_input.dropna(how='all', subset= self.df_input.columns.to_list()[end_index:], inplace=True)
		self.log_obj.debug(f"""Se eliminan {self.rows - self.df_input.shape[0]} registros con solo NA's""")
		self.rows, self.columns = self.df_input.shape
		self.df_input = self.df_input.loc[self.df_input.loc[:, self.df_input.columns.to_list()[end_index:]].sum(axis=1) != 0, :] 
		self.log_obj.debug(f"""Se eliminan {self.rows - self.df_input.shape[0]} registros cuya suma es cero""")
		self.rows, self.columns = self.df_input.shape
		self.df_input.reset_index(inplace=True)
		start_index = self.df_input.columns.to_list().index("index")
		end_index = self.df_input.columns.to_list().index(end_col)+1
		self.df_input.drop(self.df_input.filter(regex=r'^(?:YTD)|(?:RY)').columns, axis=1, inplace=True)
		# sumo uno a la cantidad de columnas para ignorar el index
		self.log_obj.debug(f"""Se eliminan {self.columns+1 - self.df_input.shape[1]} columnas que se descartan del proceso""") 
		self.__validate_input(end_index)
		self.df_data = self.df_input.loc[:, self.df_input.columns[start_index:end_index]]
		self.df_values = self.df_input.loc[:, ['index'] + self.df_input.columns.to_list()[end_index:]]
		self.df_values = self.df_values.melt(id_vars=['index'], var_name='PERIODO')
		self.log_obj.debug(f"""Creado dataframe de datos y de valores de fechas... creando un dataframe de oasis archivo validado""")
		

	def create_oasis_df(self):
		self.log_obj.debug(f"Generando información de Oasis dataframe")
		df_data, df_values = self.df_data, self.df_values
		df_joined = pd.merge(df_data, df_values, how='inner', on=['index'])
		self.log_obj.debug(f"nuevo dataframe bruto {df_joined.shape}. Empezando depuracion")
		cols, unwanted = (df_joined.columns.to_list(), ['VARIABLE', 'value', 'index'])
		for col in unwanted:
			cols.remove(col)
		df_prepared = df_joined.pivot(index=cols, columns='VARIABLE', values='value').reset_index()
		self.log_obj.debug(f"seteando nuevo dataframe como el de clase")
		self.df_input = df_prepared.copy()
		self.rows, self.columns = self.df_input.shape
		self.log_obj.debug(f"finaliza preparacion de datos...")

	def give_oasis_format(self, category, start_col='MERCADO', end_col='PERIODO'):
		self.log_obj.debug(f"""Iniciando formato de archivo...""")
		self.log_obj.debug(f"""Cantidad de filas inicial {self.rows}, columnas iniciales {self.columns}""")
		start_index = self.df_input.columns.to_list().index(start_col)
		end_index = self.df_input.columns.to_list().index(end_col)+1
		self.df_input.loc[:, self.df_input.columns[start_index: end_index]] = self.df_input.loc[:, self.df_input.columns[start_index: end_index]].fillna("S/A")
		self.df_input.loc[:, self.df_input.columns[end_index :]] = self.df_input.loc[:, self.df_input.columns[end_index:]].fillna(-1)
		self.df_input = self.df_input.loc[self.df_input.loc[:, self.df_input.columns.to_list()[end_index:]].sum(axis=1) != -2, :].copy()
		self.log_obj.debug(f"""Se eliminan {self.rows - self.df_input.shape[0]} registros con solo -1 en la fila""")

		for col in self.params['new_iv_columns']:
			if col == 'CATEGORIA':
				self.df_input[col] = category

			elif col in ['SEGMENTO', 'SUB-SEGMENTO-1']:
				self.df_input['SEGMENTO'] = self.df_input['SUB-SEGMENTO-1']

			elif col == 'EMPAQUE':
				self.df_input['EMPAQUE 104'] = self.df_input['EMPAQUE 104']

			else:
				self.df_input[col] = "S/A"

		for col in self.params['new_dist_columns']:
			self.df_input[col] = -1
		self.df_input.rename(columns=self.params['input_item_volumen_rename_columns'], inplace=True)
		self.log_obj.debug(f"""creación y renombrado columnas finalizada , generando columnas PERIODO en formato fecha""")
		self.df_input['PERIODO'] = self.df_input['PERIODO'].apply(lambda x: dt.strptime(x, '%B %Y'))
		self.df_input['PERIODO'] = self.df_input['PERIODO'].dt.strftime("%d-%m-%Y")
		self.df_input = self.df_input.loc[:, self.params['input_oasis_file_columns']]
		return self.df_input





