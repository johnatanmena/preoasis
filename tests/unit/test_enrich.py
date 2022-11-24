import unittest

import pandas as pd
from pandas._testing import assert_frame_equal, assert_series_equal
import enrichment.additional_variables as enrich
import reporting
import pdb


class TestEnrich(unittest.TestCase):

  @classmethod
  def setUpClass(cls):
    cls.path_fixtures = '../tests/unit/fixtures/'
    cls.file_name_input  = 'Item_Volumen_in.xlsx'
    cls.file_name_output = 'Item_Volumen_out.xlsx'
    cls.file_name_prepared = 'Item_Volumen_prepared_in.xlsx'
    cls.file_name_xml = 'Retail_Galletas_Noel_Colombia.xml'
    cls.df_output = pd.read_excel(cls.path_fixtures+cls.file_name_output, sheet_name='Sheet1', dtype=object)
    reporting.enr_logger.propagate = False

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



  def setUp(self):
    self.addTypeEqualityFunc(pd.DataFrame, self.assertDataframeEqual)
    self.addTypeEqualityFunc(pd.Series,  self.assertSeriesEqual)
    reporting.enr_logger.propagate = False


  def test_item_volumen(self):
    """
      checks whether the first operation filter on the class encrich filters onty
      de selected cases
    """
    enrich_obj = enrich.EnrichmentOps()
    input_df = enrich_obj.obtain_additional_var_from_item_volume(self.path_fixtures, self.file_name_input)
    self.assertEqual(input_df, self.df_output)

  def test_xml_filter(self):
    enrich_obj = enrich.EnrichmentOps()
    df_input = enrich_obj.read_xml_dictionary(self.path_fixtures, self.file_name_xml)
    df_output = pd.read_excel(self.path_fixtures + 'out_dict_gall.xlsx', dtype=object)
    df_output.fillna('', inplace=True)
    self.assertEqual(df_input, df_output)



  def tearDown(self):
    reporting.enr_logger.propagate = True