from flask import Flask, render_template, request, redirect, url_for, flash, session 
import os   ## trabajar con directorios y carpetas del sistema operativo
import time
import pandas as pd
import unidecode as ud
import re
import pdb
import json

import config.conexion as conexion ## Importar función de conexión a mysql
from config.config import DevelopmentConfig 

#import controlador.controlador_seguridad as c_seg
#import modelo.modelo_seguridad as m_seguridad
 

app = Flask(__name__)
app.config.from_object(DevelopmentConfig)

# conexión mysql
#mysql = conexion.con_mysql(app)

@app.route('/')
def index():
    return render_template('index.html')


@app.route('/v_nutresa',methods=['POST','GET'])
def v_nutresa():
    return render_template('v_nutresa.html')

@app.route('/v_nielsen',methods=['POST','GET'])
def v_nielsen():
    return render_template('v_nielsen.html')    

@app.route('/v_storecheck',methods=['POST','GET'])
def v_storecheck():
    return render_template('v_storecheck.html')    

@app.route('/v_nielsen_panama', methods=['POST', 'GET'])
def v_nielsen_panama():
    return render_template('v_nielsen_panama.html')


@app.route('/v_storecheck/transform', methods=['GET', 'POST'])
def show_form_transform_raw_data():
    config_filename = '../parametros_stats/config.json'
    with open(config_filename, encoding='utf-8') as json_config_file:
        config = json.load(json_config_file)
    
    return render_template('forms/form_transform_raw_data.html',
     expected_columns = config['project_another_sources']['input_source_file_columns'],
     folder_input_storecheck = config['project_another_sources']['path_to_storecheck_input_files'],
     folder_processed_storecheck = config['project_another_sources']['path_to_storecheck_processed_files'])

@app.route('/v_storecheck/assignvalue', methods=['GET', 'POST'])
@app.route('/v_storecheck/assignvalue/<category>', methods=['GET', 'POST'])
def show_form_assign_default_value(category=None):
    path_to_tmp_catalog = "../stage_area/temp_files/catalogs/"
    categories = os.listdir(path_to_tmp_catalog)
    clean_categories = [ cat.split("_")[3].replace(".csv", "")  for cat in categories]
    if len(clean_categories) > 0:
        if category:
            index_cat = clean_categories.index(category)
            clean_categories.remove(category)
            clean_categories = [category] + clean_categories #add element at the beginning
        else:
            index_cat = 0
        filename = categories[index_cat]
        df = pd.read_csv(path_to_tmp_catalog + filename, sep=";", encoding="latin")
        df["Indice"] = df.index
        df['NUEVO_VALOR'] = ""

        render_obj = render_template('forms/form_assign_default_values.html', categories = clean_categories,
            column_names = df.columns.values, row_data = list(df.values.tolist()), zip=zip)
    else:
        render_obj = render_template('forms/form_assign_default_values.html', categories = clean_categories,
            column_names = [], row_data = [], zip=zip)

    return render_obj

@app.route('/v_storecheck/update_value/<category>/<index>/<new_val>', methods=['GET'])
def update_value(category, index, new_val):
    if new_val:
        REGULAR_EXPRESION = r'(?:\s+)?[\[\]&!¡¿?+\\\.()%#"°|;,\-\_\']'
        new_val = ud.unidecode(re.sub(r'\s\s+' ,' ',re.sub(REGULAR_EXPRESION, ' ',
                      str(new_val).strip().upper())))
    else:
        new_val= ""
    path_to_tmp_catalog = "../stage_area/temp_files/catalogs/"
    categories = os.listdir(path_to_tmp_catalog)
    clean_categories = [ cat.split("_")[3].replace(".csv", "")  for cat in categories]
    index_cat = clean_categories.index(category)
    filename = categories[index_cat]
    df = pd.read_csv(path_to_tmp_catalog + filename, sep=";", encoding="latin")
    df.iloc[int(index),1]=new_val
    df.to_csv(path_to_tmp_catalog + filename, sep=";", encoding="latin",header=True,  index=False)
    return "0"

@app.route('/v_nielsen_panama/transform', methods=['GET', 'POST'])
def show_form_transform_item_volumen():
    config_filename = '../parametros_stats/config.json'
    with open(config_filename, encoding='utf-8') as json_config_file:
        config = json.load(json_config_file)
    
    render_obj = render_template('forms/form_transform_iv_panama.html', 
        folder_input_item_volumen = config['project_another_sources']['path_to_iv_panama_input_files'], 
        folder_processed_item_volumen = config['project_another_sources']['path_to_iv_panama_processed_files'])
    return render_obj

@app.route('/v_nielsen_panama/form_assign_iv_panama', methods=['GET', 'POST'])
@app.route('/v_nielsen_panama/form_assign_iv_panama/<category>', methods=['GET', 'POST'])
def show_form_assign_default_value_panama(category=None):
    path_to_tmp_catalog = "../stage_area/temp_files/catalogs/"
    categories = os.listdir(path_to_tmp_catalog)
    clean_categories = [ cat.split("_")[3].replace(".csv", "")  for cat in categories]
    if len(clean_categories) > 0:
        if category:
            index_cat = clean_categories.index(category)
            clean_categories.remove(category)
            clean_categories = [category] + clean_categories #add element at the beginning
        else:
            index_cat = 0
        filename = categories[index_cat]
        df = pd.read_csv(path_to_tmp_catalog + filename, sep=";", encoding="latin")
        df["Indice"] = df.index
        df['NUEVO_VALOR'] = ""

        render_obj = render_template('forms/form_assign_default_values_panama.html', categories = clean_categories,
            column_names = df.columns.values, row_data = list(df.values.tolist()), zip=zip)
    else:
        render_obj = render_template('forms/form_assign_default_values_panama.html', categories = clean_categories,
            column_names = [], row_data = [], zip=zip)

    return render_obj

@app.route('/v_nielsen_panama/update_value/<category>/<index>/<new_val>', methods=['GET'])
def update_value_panama_value(category, index, new_val):
    if new_val:
        REGULAR_EXPRESION = r'(?:\s+)?[\[\]&!¡¿?+\\\.()%#"°|;,\-\_\']'
        new_val = ud.unidecode(re.sub(r'\s\s+' ,' ',re.sub(REGULAR_EXPRESION, ' ',
                      str(new_val).strip().upper())))
    else:
        new_val= ""
    path_to_tmp_catalog = "../stage_area/temp_files/catalogs/"
    categories = os.listdir(path_to_tmp_catalog)
    clean_categories = [ cat.split("_")[3].replace(".csv", "")  for cat in categories]
    index_cat = clean_categories.index(category)
    filename = categories[index_cat]
    df = pd.read_csv(path_to_tmp_catalog + filename, sep=";", encoding="latin")
    df.iloc[int(index),1]=new_val
    df.to_csv(path_to_tmp_catalog + filename, sep=";", encoding="latin",header=True,  index=False)
    return "0"


if __name__ == "__main__":
  app.run(host='0.0.0.0', port=5000, debug=True)
  #mail.init_app(app)