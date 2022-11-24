import unittest

import pandas as pd
from pandas._testing import assert_frame_equal, assert_series_equal, assert_index_equal
import transform.transform_operations as transform
import transform.transform_controller as tcontroller
from transversal_classes import ExecutionStatus
from transversal_classes import ProjectParameters
from exceptions import InvalidCatalogException
from exceptions import ExtractException
from exceptions import FileStructureException
import reporting
import pdb
import os


"""clase encargada de testear las actividades en transform pero exige mucho trabajo crear una base de datos de pruebas por lo tanto queda
pendiente crear una base de datos espejo de OASIS por ejemplo oasis_develop donde se puedan realizar las pruebas, 
algo similar para la clsae de test de encargada de la fase de carga pendiente realizar las pruebas"""


class TestTransform(unittest.TestCase):
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