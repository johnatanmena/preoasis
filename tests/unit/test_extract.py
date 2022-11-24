import unittest

import pandas as pd
from pandas._testing import assert_frame_equal, assert_series_equal, assert_index_equal
import extract.input_validations as iv
import extract.read_and_validate_file as rv
from transversal_classes import ExecutionStatus
from transversal_classes import ProjectParameters
from exceptions import InvalidCatalogException
from exceptions import ExtractException
from exceptions import FileStructureException
import reporting
import pdb
import os

class TestExtract(unittest.TestCase):

  @classmethod
  def setUpClass(cls):
    cls.path_fixtures = '../tests/unit/fixtures/'
    cls.df_oasis_input = pd.read_csv(cls.path_fixtures + 'source_country_5_category_month.csv', sep=';', encoding='latin',
      dtype={'BARCODE':str})
    cls.additional_datacheck_columns= ["NOMBRE_ARCHIVO", "NUMERO_LINEA", "ACCION"]
    cls.exestatus = ExecutionStatus()
    cls.params = ProjectParameters()
    cls.exestatus.set_chunksize(cls.params.chunksize)
    cls.exestatus.set_lot_id(cls.params.STATS_FOLDER)
    cls.exestatus.qty_process_files = 1
    cls.exestatus.process_filename = 'source_country_5_category_month.csv'

  def setUp(self):
    self.addTypeEqualityFunc(pd.DataFrame, self.assertDataframeEqual)
    self.addTypeEqualityFunc(pd.Index, self.assertIndexEqual)
    self.addTypeEqualityFunc(pd.Series, self.assertSeriesEqual)
    reporting.ext_logger.propagate = False

  def assertDataframeEqual(self, a, b, msg):
    try:
      assert_frame_equal(a, b, check_dtype=False)
    except AssertionError as e:
      raise self.failureException(msg) from e

  def assertSeriesEqual(self, a, b, msg):
    try:
      assert_series_equal(a, b, check_dtype=False)
    except AssertionError as e:
      raise self.failureException(msg) from e

  def assertIndexEqual(self, a, b, msg):
    try:
      assert_index_equal(a, b)
    except AssertionError as e:
      raise self.failureException(msg) from e

  def test_validate_size_structure(self):
    param_list = [
      ('120GR', "Cumple estructura"),
      ('S/A', "Cumple estructura"),
      (' 120GR', "No Cumple estructura"),
      ('', "No Cumple estructura"),
      ('VEINTEGR', "No Cumple estructura")
    ]
    for par1, out in param_list:
      with self.subTest():
        self.assertEqual(iv.check_column_structure(par1), out)


  def test_verify_date(self):
    param_list = [
      ('2020-01-12', False),
      ('2020-Ene-12', False),
      ('2020/01/01', False),
      ('20200112', False),
      ('S/A', False),
      ('20209999', False),
      ('12-01-2020', True),
      ('2-1-2020', True),
      ('2-01-2020', True),
    ]
    for par1, out in param_list:
      with self.subTest():
        self.assertEqual(iv.verifyDate(par1), out)

  def test_validate_sell_value_sell_volumen(self):
    param_list = [
      (100, 0, False),
      (0, 0, True),
      (-1, 0, True),
      (100, 10, True),
      (100, -1, False),
      (None, None, False),
      ('0', 'S/A', False),
    ]

    for par1, par2, out in param_list:
      with self.subTest():
        df = pd.DataFrame(data=[[par1, par2]], columns=['VENTAS_EN_VALOR_000000', 'VENTAS_EN_VOLUMEN_KILOS_000'])
        ret = iv.validate_sell_value_sell_volumen(df.iloc[0, :])
        self.assertEqual(ret , out)

  def test_check_categorical_values(self):
    param_list = [
      ('AL HUEVO/NO INTEGRAL','REGULAR', 'IMPORTADO', True),
      ('AL HUEVO/NO INTEGRAL','OFERTADO', 'IMPORTADO', True),
      ('AL HUEVO/NO INTEGRAL','REGULAR', 'S/A', True),
      ('S/A','S/A', 'S/A', True),
      (None,0, 0, False),
      ('OFERTADO','REGULAR', 'IMPORTADO', False),
      ('AL','CORRIENTE/INTEGRAL', 'nacional', False),
    ]

    for par1, par2, par3, out in param_list:
      with self.subTest():
        df = pd.DataFrame(data=[[par1, par2, par3]], columns=['INTEGRALNOINTEGRAL', 'OFERTAPROMOCIONAL', 'IMPORTADO'])
        ret = iv.check_categorical_values(df.iloc[0, :])
        self.assertEqual(ret , out) 

  def test_file_validation(self):
    file_validation_obj = rv.FileOpsReadAndValidations(self.df_oasis_input, self.df_oasis_input.columns,
      self.additional_datacheck_columns)
    file_validation_obj.process_chunk(self.params.INPUT_STRING_COLUMNS,self.exestatus, pd)
    with self.subTest():
      self.assertEqual(file_validation_obj.filechunk.shape, self.df_oasis_input.shape)
    with self.subTest():
      self.assertEqual(file_validation_obj.filechunk.columns, self.df_oasis_input.columns)

  def test_assign_gn_value(self):
    file_validation_obj = rv.FileOpsReadAndValidations(self.df_oasis_input, self.df_oasis_input.columns,
      self.additional_datacheck_columns)
    catalog_path = self.path_fixtures + 'catalog/'
    files_in_cat_folder = os.listdir(catalog_path)
    input_files  = [self.path_fixtures + 'test_oasis_input_file.csv']
    catalog_files = []
    for infile in files_in_cat_folder:
      if infile.startswith("OASIS_MST"):
        catalog_files.append(infile)
    file_validation_obj.assign_gn_value(catalog_path, catalog_files, self.path_fixtures, 
      'test_oasis_input_file.csv', self.exestatus, pd)
    with self.subTest():
      self.assertEqual(file_validation_obj.filechunk.shape, self.df_oasis_input.shape)
    with self.subTest():
      self.assertEqual(file_validation_obj.filechunk.columns, 
        self.df_oasis_input.loc[:,file_validation_obj.filechunk.columns].columns)
    with self.subTest():
      file_validation_obj_err = rv.FileOpsReadAndValidations(self.df_oasis_input.copy(), self.df_oasis_input.columns,
      self.additional_datacheck_columns)
      file_validation_obj_err.filechunk.iloc[0, 6] = "VALOR NO EN CATALOGO"
      with self.assertRaises(ExtractException):
        file_validation_obj_err.assign_gn_value(catalog_path, catalog_files, self.path_fixtures, 
        'source_country_5_category_month.csv', self.exestatus, pd)

  def test_assign_gn_value_datacheck(self):
    dtchck = self.df_oasis_input.copy()
    dtchck['NOMBRE_ARCHIVO'] = "MINOMBREARCHIVO"
    dtchck['NUMERO_LINEA'] = 1
    dtchck['ACCION'] = "OUT"
    file_validation_obj = rv.FileOpsReadAndValidations(dtchck, self.df_oasis_input.columns,
      self.additional_datacheck_columns)
    catalog_path = self.path_fixtures + 'catalog/'
    files_in_cat_folder = os.listdir(catalog_path)
    input_files  = [self.path_fixtures + 'source_country_5_category_month.csv']
    catalog_files = []
    for infile in files_in_cat_folder:
      if infile.startswith("OASIS_MST"):
        catalog_files.append(infile)
    file_validation_obj.assign_gn_value(catalog_path, catalog_files, self.path_fixtures, 
      'source_country_5_category_month.csv', self.exestatus, pd)
    with self.subTest():
      self.assertEqual(file_validation_obj.filechunk.shape, dtchck.shape)
    with self.subTest():
      self.assertEqual(file_validation_obj.filechunk.columns, 
        dtchck.loc[:,file_validation_obj.filechunk.columns].columns)
    with self.subTest():
      file_validation_obj_err = rv.FileOpsReadAndValidations(dtchck.copy(), self.df_oasis_input.columns,
      self.additional_datacheck_columns)
      file_validation_obj_err.filechunk.iloc[0, 6] = "VALOR NO EN CATALOGO"
      with self.assertRaises(ExtractException):
        file_validation_obj_err.assign_gn_value(catalog_path, catalog_files, self.path_fixtures, 
        'source_country_5_category_month.csv', self.exestatus, pd)




  def tearDown(self):
    reporting.ext_logger.propagate = True  
