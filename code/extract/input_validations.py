import pandas as pd
import numpy as np
import os
import glob
import json
from pandas.io.json import json_normalize
import chart_studio.plotly as py
import plotly.graph_objs as go
from plotly.subplots import make_subplots
from plotly.offline import download_plotlyjs, init_notebook_mode, plot, iplot  
import plotly.tools as tls
import re
import math
import reporting
from datetime import datetime
from transversal_classes import ExecutionStatus


#verifica cuando un registro  de tamaño cumple con la estructura
#necesaria para ser evaluado
def check_column_structure(value):
    m = re.match(r"^\d*GR$|S/A", value)
    if m:
        return ("Cumple estructura")
    else:
        return ("No Cumple estructura")

#function that checks the barcode provided with the digit checksum return false whether the digit is not equal to provided or th checksum
#can't be reproduced for the code supplied
def verifyCheckSum( code ):
    try:
        given_checksum = code[-1]
        code = code[:-1] #to verify the code is complete
        digits = [*map(int, reversed(code))]
        even, odd = digits[0::2], digits[1::2]
        number = sum(odd) + sum(even) * 3
        calculated_checksum = (10 - number) % 10
        return int(given_checksum) == int(calculated_checksum)
    except:
        return False

def verifyDate(value):
    try:
        datetime.strptime(value,"%d-%m-%Y")
        return True
    except:
        return False
#function that validates a row VENTAS VALOR field if its major than zero then VENTAS_VOLUMEN
#if that doesn't happen it means that the input file has invalid records that MUST be evaluated again
def validate_sell_value_sell_volumen(row):
    try:
        if row['VENTAS_EN_VALOR_000000'] > 0 and row['VENTAS_EN_VOLUMEN_KILOS_000']<=0:
            return False
        else:
            return True
    except:
        return False

#function that validates that values are in the spected values on the database for example:
# INTEGRALNOINTEGRAL field expected values are ['AL HUEVO/NO INTEGRAL', 'CORRIENTE/INTEGRAL', 'CORRIENTE/NO INTEGRAL', 'TODOS/NO INTEGRAL', "S/A"]
# OFERTAPROMOCIONAL field  expected values are ['REGULAR', 'OFERTADO', 'S/A']
# IMPORTADO NO IMPORTADO field expected values are ['IMPORTADO', 'NACIONAL', 'S/A']
def check_categorical_values(row):
    try:
        if row['INTEGRALNOINTEGRAL'] not in ['AL HUEVO/NO INTEGRAL', 'AL HUEVO/INTEGRAL', 'CORRIENTE/INTEGRAL', 'CORRIENTE/NO INTEGRAL', 'TODOS/NO INTEGRAL', "S/A"] or \
            row['OFERTAPROMOCIONAL'] not in ['REGULAR', 'OFERTADO', 'NO PROMOCION', 'PROMOCION', 'S/A'] or \
            row['IMPORTADO'] not in ['IMPORTADO', 'NACIONAL', 'S/A']:

            return False
        else:
            return True
    except:
        return False

def validate_input(input_files, output_folder,output_folder_stats, list_master_columns, id_proceso, exe_mode):
    df_config = pd.DataFrame(list_master_columns)
    df_config.rename(columns={0: "NIVEL"}, inplace=True)
    df_config = df_config.append(pd.DataFrame([['TOTAL']], columns=['NIVEL']), ignore_index=True)
    list_input_files = os.listdir(input_files)
    Nulos_No_Prom = []
    Estructura = []
    Rango = []
    Coincidencia_cat = []
    Nombre_Arch = []
    Barcode = []
    Fecha =  []
    val_vtas_valor = []
    values_in_category = []
    exestatus = ExecutionStatus()

    for filename in list_input_files:
        reporting.ext_logger.info("iniciando validaciones de archivo: %s" % filename)
        chunk = pd.read_csv(input_files + filename, sep = ';',  encoding='latin', dtype={'BARCODE':str}, chunksize = 100000) #bloques de 100000 filas
        
        # Cantidad de filas de cada archivo
        Registros_Totales = exestatus.count_row_on_file(input_files, filename)
        #listas auxiliares
        Reg_Cump_Nivel_list = []
        nulos_list          = []
        rango_list          = []
        val_tamano_list     = []
        val_barcode_list    = []
        val_date_list       = []
        val_vtas_vol_list   = []
        val_cat_values_list = []

        for df in chunk:
            #here my logic
            # Validación campos de los catálogos en cada uno de los archivos
            reporting.ext_logger.debug("Validando columna de catalogos de archivo: %s" % filename) #too verbose set on debug this later
            Grupo_Nivel= df.reset_index().groupby(['NIVEL'],as_index=False ).count()
            Grupo_Nivel = Grupo_Nivel['NIVEL'] 
            df_Nivel = pd.DataFrame(Grupo_Nivel)

            Val_Nivel = df_Nivel.merge(df_config, how='outer', indicator='Validación')
            Cumplen_Nivel = Val_Nivel[Val_Nivel['Validación'] =='both']
            Cumplen_Nivel = pd.DataFrame(Cumplen_Nivel)
            Lista_Nivel = Cumplen_Nivel['NIVEL'].tolist()
            Reg_Cump_Nivel = Cumplen_Nivel.shape[0]
            Reg_Cump_Nivel_list.append(Reg_Cump_Nivel) #agregar a la lista

            # Validación de valores nulos en las columnas de distribución, VENTAS_EN_VALOR_000000, VENTAS_EN_VOLUMEN_KILOS_000 y los 
            # campos de los catálogos
            reporting.ext_logger.debug("Validanción de campos de distribucion: %s" % filename)
            data_dist_cols = [x for x in df.filter(like='DIST').columns] # muy ineficiente
            data_ventas = ['VENTAS_EN_VALOR_000000','VENTAS_EN_VOLUMEN_KILOS_000']
            data_nivel = [x for x in df.filter(Lista_Nivel).columns]
            df_data_nivel = df.loc[:, data_dist_cols + data_ventas + data_nivel ]
            nulos = df_data_nivel.isna().sum().sum() # cantidad de elementos nulos
            nulos_list.append(nulos)

            ### Validación cumplimiento del rango 0%-100% en las columnas distribución
            reporting.ext_logger.debug("Validación de rangos de distribuciones: %s" % filename)
            data_dist = df.filter(like='DIST')
            rango = data_dist[(data_dist > 100).any(1) | (data_dist < -1).any(1) ].shape[0]
            rango_list.append(rango)

            ## Validación columna tamaño
            reporting.ext_logger.debug("Validación de columna tamaño: %s" % filename)
            df['Validacion_tamano'] = df['TAMANO'].apply(lambda x:check_column_structure(x))
            val_tamano = np.sum(df['Validacion_tamano'] == 'Cumple estructura')
            val_tamano_list.append(val_tamano)

            #validacion de barcode
            reporting.ext_logger.debug("Validación de barcode: %s" % filename)
            df['Validacion Barcode'] = df['BARCODE'].apply(lambda x: verifyCheckSum(x))
            val_barcode = df.loc[df['Validacion Barcode']==True, : ].shape[0]
            val_barcode_list.append(val_barcode)

            #validacion fecha
            reporting.ext_logger.debug("Validación de formato de fecha: %s" % filename)
            df['Validacion Fecha'] = df['PERIODO'].apply(lambda x: verifyDate(x))
            val_date = df.loc[df['Validacion Fecha']==True, : ].shape[0]
            val_date_list.append(val_date)

            #validacion vtasvalor y vtasvolumen
            reporting.ext_logger.debug("Validación de ventas valor y ventas volumen: %s" % filename)
            df['Validacion Vtas']  = df.apply(lambda x: validate_sell_value_sell_volumen(x), axis=1)
            val_vtas_vol = df.loc[df['Validacion Vtas']==True, : ].shape[0]
            val_vtas_vol_list.append(val_vtas_vol)

            #validacion valores categóricos
            reporting.ext_logger.debug("Validación de valores categoricos: %s" % filename)
            df['Validacion Cat']  = df.apply(lambda x: check_categorical_values(x), axis=1)
            val_cat_values = df.loc[df['Validacion Cat']==True, : ].shape[0]
            val_cat_values_list.append(val_cat_values)





        # Calcular porcentajes de los archivos seleccionados
        reporting.ext_logger.info("Calculando porcentajes del archivo: %s" % filename)
        Porcentaje_coincidencia = 100 -  round((sum(Reg_Cump_Nivel_list) / Registros_Totales) * 100) 
        Porcentaje_nulos = 100 - round((sum(nulos_list) / (Registros_Totales * len(data_dist_cols + data_ventas + data_nivel))) * 100)
        Porcenta_cumplen_rango = 100 - round((sum(rango_list) / Registros_Totales)*100)
        Porcenta_cumplen_est = round((sum(val_tamano_list) / Registros_Totales)*100)
        Porcenta_cumplen_bar = round(sum(val_barcode_list)/ Registros_Totales*100)
        Porcenta_cumplen_date = round(sum(val_date_list)/ Registros_Totales*100)
        Porcenta_cumplen_vta = round(sum(val_vtas_vol_list)/ Registros_Totales*100)
        Porcenta_cumplen_cat = round(sum(val_cat_values_list)/ Registros_Totales*100)

        #agregar elementos calculados a columnas
        reporting.ext_logger.info("Creando columnas del archivo: %s" % filename)
        Coincidencia_cat.append(Porcentaje_coincidencia)
        Nombre_Arch.append(filename)
        Nulos_No_Prom.append(Porcentaje_nulos)
        Rango.append(Porcenta_cumplen_rango)
        Estructura.append(Porcenta_cumplen_est)
        Barcode.append(Porcenta_cumplen_bar)
        Fecha.append(Porcenta_cumplen_date)
        val_vtas_valor.append(Porcenta_cumplen_vta)
        values_in_category.append(Porcenta_cumplen_cat)

        #crear dataframe de salida
        reporting.ext_logger.info("Agregando columnas a la tabla insumo del reporte: %s" % filename)
        df_Resumen = pd.DataFrame(Nombre_Arch, columns =['Nombre Archivo'])
        df_Resumen['% Coincidencia columna vs CATALOGOS'] = Coincidencia_cat
        df_Resumen['% Cumplimiento Promedio No Nulos'] = Nulos_No_Prom
        df_Resumen['% Cumplen intervalo (0%-100%) col-DIST'] = Rango
        df_Resumen['% Cumplen estructura columna TAMANO'] = Estructura        
        df_Resumen['% Cumplen Estructura Barcode'] = Barcode
        df_Resumen['% Cumplen Estructura Fecha'] = Fecha
        df_Resumen['% Validacion vtas valor vtas volumen'] = val_vtas_valor
        df_Resumen['% Validacion valores categóricos'] = values_in_category

        # Resumen
        reporting.ext_logger.info("Generando columna resumen: %s" % filename)
        all_columns = list(df_Resumen) 
        df_Resumen[all_columns] = df_Resumen[all_columns].astype(str)
        
        df_Resumen.iloc[:, df_Resumen.columns != 'Nombre Archivo'] =  df_Resumen.iloc[:, df_Resumen.columns != 'Nombre Archivo'].astype(str) + '%' 
        
        if df_Resumen['% Coincidencia columna vs CATALOGOS'].all() =='100%' and df_Resumen['% Cumplimiento Promedio No Nulos'].all()=='100%' \
            and df_Resumen['% Cumplen intervalo (0%-100%) col-DIST'].all()=='100.0%'  and df_Resumen['% Cumplen estructura columna TAMANO']=='100.0%' \
            and df_Resumen['% Cumplen Estructura Fecha']=='100%' and df_Resumen['% Validacion vtas valor vtas volumen'] =='100%' \
            and df_Resumen['% Validacion valores categóricos'] == '100%':
            df_Resumen['Validación'] = 'Cumple'
        else:
            df_Resumen['Validación'] = 'No Cumple'

    #agregar codigo para escribir html de resumen con la variable output_folder
    reporting.ext_logger.info("Generando archivo de salida con tabla resumen del proceso realizado")
    if len(list_input_files) != 0:
        fecha_proceso = datetime.now().strftime("%Y-%m-%d")
        df_Resumen['Fecha Proceso'] = fecha_proceso
        fig = make_subplots(rows=1, cols=1,
                specs=[[{"type": "table"}]],)

        fig.add_trace(
            go.Table(
                columnwidth = [70,20,20,20,20,20,20],
                header=dict(
                    values=df_Resumen.columns,
                    font = dict(color = 'white', size = 12),
                    line = dict(color = '#506784'),
                    fill = dict(color = 'grey'),
                    align="center"
                ),
                cells=dict(
                    values=[df_Resumen[k].tolist() for k in df_Resumen.columns[0:]],
                    line = dict(color = '#506784'),
                    fill = dict(color = 'white'),
                    font = dict(color = '#506784'),
                    align = "center")
            ),
            row=1, col=1
        )

        fig.update_layout(template="seaborn", title_text="RESUMEN VALIDACIONES ARCHIVOS",xaxis={'categoryorder':'category ascending'} ,showlegend=False)
        fig.write_html(output_folder +'Archivo_Resumen_'+ fecha_proceso +'.html', auto_open=False) #escritrura de archivo resumen 
        if exe_mode == 'Nutresa' and len(list_input_files) !=0: #already checked
            df_Resumen['id_proceso'] = id_proceso
            df_Resumen.rename({'% Coincidencia columna vs CATALOGOS': 'VAL01', '% Cumplimiento Promedio No Nulos': 'VAL02',
                '% Cumplen intervalo (0%-100%) col-DIST':'VAL03', '% Cumplen estructura columna TAMANO':'VAL04', '% Cumplen Estructura Fecha':'VAL05',
                 '% Validacion vtas valor vtas volumen':'VAL06', 'Validación':'VAL00', 'Nombre Archivo':'FILENAME', 'id_proceso':'PROCESS_ID',
                  'Fecha Proceso': 'DATE_PROCESS', '% Cumplen Estructura Barcode': 'VAL07', '% Validacion valores categóricos':'VAL08'},axis=1, inplace=True)
            col_order = ['PROCESS_ID', 'FILENAME', 'VAL00',  'VAL01', 'VAL02', 'VAL03', 'VAL04','VAL05', 'VAL06', 'VAL07','VAL08', 'DATE_PROCESS']
            df_Resumen = df_Resumen.loc[:, col_order]
            df_Resumen.to_csv(output_folder_stats + "resumen_validaciones_por_procesos.csv", mode="w", sep=";", encoding="latin",
                index=False, header=True)

#function that validates the input format of the tag file
#returns 0: on valid file
#   -1: on not all columns
#   -2: on not valid tag anterior
#   -3: on not valid tag actual
def validate_tag_file(df):
    reporting.ext_logger.info("iniciando validaciones de archivo")
    if set(df.columns.tolist()) == set(['TAG ANTERIOR', 'TAG ACTUAL', 'DESCRIPCION', 'LEVEL']):
        if df.loc[df['TAG ANTERIOR'].str.len() < 10, :].shape[0] != 0:
            reporting.ext_logger.warning("tags invalidos")
            return -2
        elif df.loc[df['TAG ACTUAL'].str.len() < 10, :].shape[0] != 0:
            reporting.ext_logger.warning("tags invalidos")
            return -3
        return 0
    else:
        reporting.ext_logger.warning("columnas no validas")
        return -1

def validate_tag_remove_file(df):
    reporting.ext_logger.info("iniciando validaciones de archivo")
    if set(df.columns.tolist()) == set(['TAG', 'DESCRIPCION']):
        if df.loc[df['TAG'].str.len() < 10, :].shape[0] != 0:
            reporting.ext_logger.warning("tags invalidos")
            return -2
        return 0
    else:
        reporting.ext_logger.warning("columnas no validas")
        return -1