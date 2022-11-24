import sys
import os
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objs as go
import plotly.tools as tls
from datetime import datetime, timedelta
from plotly.subplots import make_subplots
from plotly.offline import download_plotlyjs, init_notebook_mode, plot, iplot
import dash
import dash_core_components as dcc
import dash_html_components as html
#import dash_bootstrap_components as dbc
from dash.dependencies import Input, Output
import warnings
warnings.filterwarnings("ignore")
from flask import request
import reports.create_layout_1 as lay1
import reports.create_layout_2 as lay2
import reports.create_layout_3 as lay3
import reports.create_layout_4 as lay4
import reporting
import pdb
#import reports.create_layout_5 as lay5
import base64
from bs4 import BeautifulSoup

class ReportStats():
  """docstring for ReportStats"""
  def __init__(self, complete_path, output_path):
    self.file_stats_path = complete_path #TODO function to filter last three months
    self.complete_filename = self.file_stats_path + 'estadisticas_archivo_ejecución.csv'
    self.process_filename = self.file_stats_path + 'estadisticas_proceso_ejecución.csv'
    self.glossary = self.file_stats_path + 'Glosario.csv'
    self.data1 = pd.read_csv(self.complete_filename, sep =';',encoding='latin', parse_dates=['fecha_inicio','fecha_fin'], infer_datetime_format=True,chunksize=200)
    self.data2 = pd.read_csv (self.process_filename, sep =';',encoding='latin', parse_dates=['fecha_inicio','fecha_fin'], infer_datetime_format=True)
    self.data3 = pd.read_csv(self.glossary, sep=',', encoding='latin')
    self.output_path = output_path
    #print()    #self.tidy_stats_data()

  def create_html_from_folder(self):
        svg_files = os.listdir(self.output_path)
        svg_files = [x for x in svg_files if x[-4:]=='.svg']
        stringhtml = """
        <html>
        <head>
        <title>Reporte de ejecucion</title>
        <link rel="stylesheet" href="https://stackpath.bootstrapcdn.com/bootstrap/4.4.1/css/bootstrap.min.css" ></link>
        <script src="https://code.jquery.com/jquery-3.2.1.slim.min.js" ></script>
        <script src="https://cdnjs.cloudflare.com/ajax/libs/popper.js/1.12.9/umd/popper.min.js"></script>
        <script src="https://maxcdn.bootstrapcdn.com/bootstrap/4.0.0/js/bootstrap.min.js"></script>
        </head>
        <body>
        <div class='container-fluid'>
        """
        with open(self.output_path + 'reporte_ejecucion.html', 'w') as htmlf:
            htmlf.write(stringhtml)
            counter = 0
            for graph in svg_files:
                if counter ==0:
                    htmlf.write("<div class='row'>")
                with open(self.output_path + graph, 'rb') as graph_data:
                    encoded_image = base64.b64encode(graph_data.read()).decode("utf-8")
                    htmlf.write("<div class='col-md-6'>")
                    htmlf.write("<img src='data:image/svg+xml;base64,%s' />" % (encoded_image))
                    htmlf.write("</div>")
                counter = counter + 1
                if counter == 2:
                    htmlf.write("</div>")
                    counter=0

                os.unlink(self.output_path + graph)
            htmlf.write("</div></body></html>")

        return

  def execution_report(self, start_date, end_date = None, filename = None):
      if end_date is None:
        end_date=0
      reporting.rep_logger.info("Creando reporte de ejecución")
      reporting.rep_logger.debug("Seleccionando rango de fechas")
      data2 = pd.read_csv (self.process_filename, sep =';',encoding='latin', parse_dates=['fecha_inicio','fecha_fin'], infer_datetime_format=True)
      data1 = pd.read_csv(self.complete_filename, sep =';',encoding='latin', parse_dates=['fecha_inicio','fecha_fin'], infer_datetime_format=True,chunksize=200)
      ## Creación columna Fecha_Ini_Aux (es igual a la fecha_inicio sin la parte de la hora)
      data2["Fecha_Ini_Aux"] = pd.to_datetime(data2["fecha_inicio"]).dt.date
      data2['Fecha_Ini_Aux'] = pd.to_datetime(data2['Fecha_Ini_Aux'])

      ## Filtro de fecha seleccionado en la base de datos de ejecución proceso
      if start_date != 0 and end_date == 0:
          data2 = data2[data2['Fecha_Ini_Aux'].astype(str).str[:10] == start_date]
      else:
          mask = (data2['Fecha_Ini_Aux'] >= start_date) & (data2['Fecha_Ini_Aux'] <= end_date)
          data2 = data2.loc[mask]

      ## Cálculo de archivos procesados, descartados y con errror en la ejecución
      data2['Total_Archivos'] = data2['archivos_ftp'] + data2['archivos_locales']
      data2['Archivos_procesados'] = data2['Total_Archivos'] - (
              data2['cantidad_archivos_error'] + data2['archivos_descartados'])

      ## Filtro de fecha en la base de datos ejecucción archivos
      data = pd.DataFrame()
      for chunk in data1:
          chunk["Fecha_Ini_Aux"] = pd.to_datetime(chunk["fecha_inicio"]).dt.date
          chunk['Fecha_Ini_Aux'] = pd.to_datetime(chunk['Fecha_Ini_Aux'])

          if start_date != 0 and end_date == 0:
              chunk = chunk[chunk['Fecha_Ini_Aux'].astype(str).str[:10] == start_date]
          else:
              mask = (chunk['Fecha_Ini_Aux'] >= start_date) & (chunk['Fecha_Ini_Aux'] <= end_date)
              chunk = chunk.loc[mask]

          data = pd.concat([data, chunk])

      ## Filtrar última ejecución (fecha más reciente)
      fec_ult_eje = data['Fecha_Ini_Aux'].max()
      data_ult_eje = data[data['Fecha_Ini_Aux'] == fec_ult_eje]

      ## Se extraen de la columna nombre_archivo las variables (Fuente, Región, Canal, Categoría y fecha)
      new = data_ult_eje["nombre_archivo"].str.split('_', n=5, expand=True)
      data_ult_eje["Fuente"] = new[0]
      data_ult_eje["Región"] = new[1]
      data_ult_eje["Canal"] = new[2]
      data_ult_eje["Categoría"] = new[3]
      data_ult_eje["Fecha"] = new[4]

      ## Cálculo de los tiempos de ejecución por fase en horas
      data_ult_eje['tiempo_extraccion_Min'] = data_ult_eje['tiempo_extraccion'] / 60
      data_ult_eje['tiempo_transformacion_Min'] = data_ult_eje['tiempo_transformacion'] / 60
      data_ult_eje['tiempo_carga_Min'] = data_ult_eje['tiempo_carga'] / 60

      ## Creación dataframe tiempos de ejecución por fase en horas
      Min_Ext = data_ult_eje['tiempo_extraccion_Min'].sum()
      Min_Trns = data_ult_eje['tiempo_transformacion_Min'].sum()
      Min_Carga = data_ult_eje['tiempo_carga_Min'].sum()
      Min_Fase = pd.DataFrame({"Extracción": Min_Ext, "Transformación": Min_Trns, "Carga": Min_Carga}, index=[0])
      Min_Fase = Min_Fase.T
      Min_Fase['Row_1'] = Min_Fase.index
      Min_Fase.rename(columns={'Row_1': "Fase", 0: 'Duración_Minutos'}, inplace=True)
      cols = Min_Fase.columns.tolist()
      cols = cols[-1:] + cols[:-1]
      Min_Fase = Min_Fase[cols]
      Min_Fase = Min_Fase.reset_index(drop=True)
      list_of_names_1 = Min_Fase['Fase'].to_list()
      list_of_values_2 = Min_Fase['Duración_Minutos'].to_list()

      ## Gráfico tiempos de ejecución por fase en horas
      reporting.rep_logger.debug("Creando gráfico tiempo de ejecución en horas")
      figur = go.Figure(data=[go.Pie(labels=list_of_names_1, values=list_of_values_2, hole=.4,
                                     text=[round(val, 2) for val in list_of_values_2], textposition='inside',
                                     marker_colors=['rgb(5,255,255)', 'rgb(41,24,107)', 'rgb(117,127,221)'])])
      figur.update_layout(title_text="% de duración (horas) <br>por Fase de Ejecución", template="seaborn")
      figur.write_image(self.output_path +'Duracion_Fase.svg',  format="svg", scale=1)
      reporting.rep_logger.debug("Gráfico finalizado de manera correcta")
      total_h = float(round(Min_Fase['Duración_Minutos'].sum(), 2))

      ## Cálculo categorías y tiempos por fase en horas según canal

      ## Filtro Canal Retail y Scantrack
      reporting.rep_logger.debug("Creando gráfica de Retail y scantrack discriminada")## Cálculo Total Horas
      Retail = data_ult_eje[data_ult_eje['Canal'] == '1']
      Scantrack = data_ult_eje[data_ult_eje['Canal'] == '2']
      tam_Ret = len(Retail)

      if tam_Ret != 0:
          ## Cálculos Canal Retail
          Min_Extraccion_Retail = Retail.groupby(['Categoría'])['tiempo_extraccion_Min'].sum().to_frame().reset_index()
          Min_Transcion_Retail = Retail.groupby(['Categoría'])[
              'tiempo_transformacion_Min'].sum().to_frame().reset_index()
          Min_Carga_Retail = Retail.groupby(['Categoría'])['tiempo_carga_Min'].sum().to_frame().reset_index()

          join1_Retail = pd.merge(Min_Extraccion_Retail, Min_Transcion_Retail, on='Categoría')
          join2_Retail = pd.merge(join1_Retail, Min_Carga_Retail, on='Categoría')

          join2_Retail['Suma_Retail'] = join2_Retail.sum(axis=1)
          join2_Retail['Extracción_Retail'] = round(
              (join2_Retail['tiempo_extraccion_Min'] / join2_Retail['Suma_Retail']) * 100)
          join2_Retail['Transformación_Retail'] = round(
              (join2_Retail['tiempo_transformacion_Min'] / join2_Retail['Suma_Retail']) * 100)
          join2_Retail['Carga_Retail'] = round((join2_Retail['tiempo_carga_Min'] / join2_Retail['Suma_Retail']) * 100)
          join2_Retail['Extracción_Retail'].astype('int')
          join2_Retail['sum'] = join2_Retail['Extracción_Retail'] + join2_Retail['Transformación_Retail'] + \
                                join2_Retail['Carga_Retail']
          join2_Retail.loc[join2_Retail['sum'] != 100, 'Extracción_Retail'] = join2_Retail.loc[join2_Retail[
                                                                                                   'sum'] != 100, 'Extracción_Retail'] + 1

          x1_data_Retail = join2_Retail['Extracción_Retail'].to_list()
          x2_data_Retail = join2_Retail['Transformación_Retail'].to_list()
          x3_data_Retail = join2_Retail['Carga_Retail'].to_list()
          x_data_T_Retail = [x1_data_Retail, x2_data_Retail, x3_data_Retail]
          values_Retail = np.asarray(x_data_T_Retail).T.tolist()

          ##Gráfica Canal Retail
          top_labels_Retail = ['EXTRACCIÓN', 'TRANSFORMACIÓN', 'CARGA']

          colors_Retail = ['rgba(38, 24, 74, 0.8)', 'rgba(71, 58, 131, 0.8)',
                           'rgba(122, 120, 168, 0.8)']

          y_data_Retail = join2_Retail['Categoría'].to_list()

          x_data_Retail = values_Retail

          grafica_Retail = go.Figure()

          for i in range(0, len(x_data_Retail[0])):
              for xd_Retail, yd_Retail in zip(x_data_Retail, y_data_Retail):
                  grafica_Retail.add_trace(go.Bar(
                      x=[xd_Retail[i]], y=[yd_Retail],
                      orientation='h',
                      marker=dict(
                          color=colors_Retail[i],
                          line=dict(color='rgb(248, 248, 249)', width=1)
                      )
                  ))

          grafica_Retail.update_layout(
              xaxis=dict(
                  showgrid=False,
                  showline=False,
                  showticklabels=False,
                  zeroline=False,
                  domain=[0.15, 1]
              ),
              yaxis=dict(
                  showgrid=False,
                  showline=False,
                  showticklabels=False,
                  zeroline=False,
              ),
              barmode='stack',
              paper_bgcolor='rgb(248, 248, 255)',
              plot_bgcolor='rgb(248, 248, 255)',
              margin=dict(l=120, r=10, t=140, b=80),
              showlegend=False,
          )

          annotations = []

          for yd_Retail, xd_Retail in zip(y_data_Retail, x_data_Retail):
              # labeling the y-axis
              annotations.append(dict(xref='paper', yref='y',
                                      x=0.14, y=yd_Retail,
                                      xanchor='right',
                                      text=str(yd_Retail),
                                      font=dict(family='Arial', size=12,
                                                color='rgb(67, 67, 67)'),
                                      showarrow=False, align='right'))
              # labeling the first percentage of each bar (x_axis)
              annotations.append(dict(xref='x', yref='y',
                                      x=xd_Retail[0] / 2, y=yd_Retail,
                                      text=str(xd_Retail[0]) + '%',
                                      font=dict(family='Arial', size=12,
                                                color='rgb(248, 248, 255)'),
                                      showarrow=False))
              # labeling the first Likert scale (on the top)
              if yd_Retail == y_data_Retail[-1]:
                  annotations.append(dict(xref='x', yref='paper',
                                          x=xd_Retail[0] / 2, y=1.1,
                                          text=top_labels_Retail[0],
                                          font=dict(family='Arial', size=12,
                                                    color='rgb(67, 67, 67)'),
                                          showarrow=False))
              space = xd_Retail[0]
              for i in range(1, len(xd_Retail)):
                  # labeling the rest of percentages for each bar (x_axis)
                  annotations.append(dict(xref='x', yref='y',
                                          x=space + (xd_Retail[i] / 2), y=yd_Retail,
                                          text=str(xd_Retail[i]) + '%',
                                          font=dict(family='Arial', size=12,
                                                    color='rgb(248, 248, 255)'),
                                          showarrow=False))
                  # labeling the Likert scale
                  if yd_Retail == y_data_Retail[-1]:
                      annotations.append(dict(xref='x', yref='paper',
                                              x=space + (xd_Retail[i] / 2), y=1.1,
                                              text=top_labels_Retail[i],
                                              font=dict(family='Arial', size=12,
                                                        color='rgb(67, 67, 67)'),
                                              showarrow=False))
                  space += xd_Retail[i]

          grafica_Retail.update_layout(annotations=annotations, template="seaborn",
                                       title_text='DURACIÓN DE PROCESAMIENTO (HORAS) DE CADA CATEGORÍA PARA EL CANAL RETAIL<br>SEGÚN LA FASE DE EJECUCIÓN',
                                       height=600, yaxis_tickangle=-50,
                                       font=dict(family="Calibri",
                                                 size=12,
                                                 color="#7a4b4b"))

          grafica_Retail.write_image(self.output_path + 'grafica_Retail.svg',  format="svg", scale=1)
          Cat_Retail = join2_Retail['Categoría'].nunique()
          reporting.rep_logger.debug("Finaliza gráfica retail")

      else:
          Cat_Retail = 0
          reporting.rep_logger.debug("No hay datos que cumplan esta condición")


      tam_Scan = len(Scantrack)

      if tam_Scan != 0:

          ## Cálculos Canal Scantrack
          Min_Extraccion_Scantrack = Scantrack.groupby(['Categoría'])[
              'tiempo_extraccion_Min'].sum().to_frame().reset_index()
          Min_Transcion_Scantrack = Scantrack.groupby(['Categoría'])[
              'tiempo_transformacion_Min'].sum().to_frame().reset_index()
          Min_Carga_Scantrack = Scantrack.groupby(['Categoría'])['tiempo_carga_Min'].sum().to_frame().reset_index()

          join1_Scantrack = pd.merge(Min_Extraccion_Scantrack, Min_Transcion_Scantrack, on='Categoría')
          join2_Scantrack = pd.merge(join1_Scantrack, Min_Carga_Scantrack, on='Categoría')

          join2_Scantrack['Suma_Scantrack'] = join2_Scantrack.sum(axis=1)
          join2_Scantrack['Extracción_Scantrack'] = round(
              (join2_Scantrack['tiempo_extraccion_Min'] / join2_Scantrack['Suma_Scantrack']) * 100)
          join2_Scantrack['Transformación_Scantrack'] = round(
              (join2_Scantrack['tiempo_transformacion_Min'] / join2_Scantrack['Suma_Scantrack']) * 100)
          join2_Scantrack['Carga_Scantrack'] = round(
              (join2_Scantrack['tiempo_carga_Min'] / join2_Scantrack['Suma_Scantrack']) * 100)
          join2_Scantrack['Extracción_Scantrack'].astype('int')

          x1_Scantrack_Scantrack = join2_Scantrack['Extracción_Scantrack'].to_list()
          x2_Scantrack_Scantrack = join2_Scantrack['Transformación_Scantrack'].to_list()
          x3_Scantrack_Scantrack = join2_Scantrack['Carga_Scantrack'].to_list()
          x_Scantrack_T_Scantrack = [x1_Scantrack_Scantrack, x2_Scantrack_Scantrack, x3_Scantrack_Scantrack]
          values_Scantrack = np.asarray(x_Scantrack_T_Scantrack).T.tolist()

          ## Gráficas Canal Scantrack
          top_labels_Scantrack = ['EXTRACCIÓN', 'TRANSFORMACIÓN', 'CARGA']

          colors_Scantrack = ['rgba(38, 24, 74, 0.8)', 'rgba(71, 58, 131, 0.8)',
                              'rgba(122, 120, 168, 0.8)']

          y_Scantrack_Scantrack = join2_Scantrack['Categoría'].to_list()

          x_Scantrack_Scantrack = values_Scantrack

          grafica_Scantrack = go.Figure()

          for i in range(0, len(x_Scantrack_Scantrack[0])):
              for xd_Scantrack, yd_Scantrack in zip(x_Scantrack_Scantrack, y_Scantrack_Scantrack):
                  grafica_Scantrack.add_trace(go.Bar(
                      x=[xd_Scantrack[i]], y=[yd_Scantrack],
                      orientation='h',
                      marker=dict(
                          color=colors_Scantrack[i],
                          line=dict(color='rgb(248, 248, 249)', width=1)
                      )
                  ))

          grafica_Scantrack.update_layout(
              xaxis=dict(
                  showgrid=False,
                  showline=False,
                  showticklabels=False,
                  zeroline=False,
                  domain=[0.15, 1]
              ),
              yaxis=dict(
                  showgrid=False,
                  showline=False,
                  showticklabels=False,
                  zeroline=False,
              ),
              barmode='stack',
              paper_bgcolor='rgb(248, 248, 255)',
              plot_bgcolor='rgb(248, 248, 255)',
              margin=dict(l=120, r=10, t=140, b=80),
              showlegend=False,
          )

          annotations = []

          for yd_Scantrack, xd_Scantrack in zip(y_Scantrack_Scantrack, x_Scantrack_Scantrack):
              # labeling the y-axis
              annotations.append(dict(xref='paper', yref='y',
                                      x=0.14, y=yd_Scantrack,
                                      xanchor='right',
                                      text=str(yd_Scantrack),
                                      font=dict(family='Arial', size=12,
                                                color='rgb(67, 67, 67)'),
                                      showarrow=False, align='right'))
              # labeling the first percentage of each bar (x_axis)
              annotations.append(dict(xref='x', yref='y',
                                      x=xd_Scantrack[0] / 2, y=yd_Scantrack,
                                      text=str(xd_Scantrack[0]) + '%',
                                      font=dict(family='Arial', size=12,
                                                color='rgb(248, 248, 255)'),
                                      showarrow=False))
              # labeling the first Likert scale (on the top)
              if yd_Scantrack == y_Scantrack_Scantrack[-1]:
                  annotations.append(dict(xref='x', yref='paper',
                                          x=xd_Scantrack[0] / 2, y=1.1,
                                          text=top_labels_Scantrack[0],
                                          font=dict(family='Arial', size=12,
                                                    color='rgb(67, 67, 67)'),
                                          showarrow=False))
              space = xd_Scantrack[0]
              for i in range(1, len(xd_Scantrack)):
                  # labeling the rest of percentages for each bar (x_axis)
                  annotations.append(dict(xref='x', yref='y',
                                          x=space + (xd_Scantrack[i] / 2), y=yd_Scantrack,
                                          text=str(xd_Scantrack[i]) + '%',
                                          font=dict(family='Arial', size=12,
                                                    color='rgb(248, 248, 255)'),
                                          showarrow=False))
                  # labeling the Likert scale
                  if yd_Scantrack == y_Scantrack_Scantrack[-1]:
                      annotations.append(dict(xref='x', yref='paper',
                                              x=space + (xd_Scantrack[i] / 2), y=1.1,
                                              text=top_labels_Scantrack[i],
                                              font=dict(family='Arial', size=12,
                                                        color='rgb(67, 67, 67)'),
                                              showarrow=False))
                  space += xd_Scantrack[i]

          grafica_Scantrack.update_layout(annotations=annotations, template="seaborn",
                                          title_text='DURACIÓN DE PROCESAMIENTO (HORAS) DE CADA CATEGORÍA PARA EL CANAL SCANTRACK<br>SEGÚN LA FASE DE EJECUCIÓN',
                                          height=600, yaxis_tickangle=-50,
                                          font=dict(family="Calibri",
                                                    size=12,
                                                    color="#7a4b4b"))
          # grafica_Scantrack.show()

          grafica_Scantrack.write_image(self.output_path +'grafica_Scantrack.svg',  format="svg", scale=1)
          ## Total categorías canal Scantrack
          Cat_Scantrack = join2_Scantrack['Categoría'].nunique()
          reporting.rep_logger.debug("Finaliza gráfica scantrack")
      else:
          Cat_Scantrack = 0
          reporting.rep_logger.debug("No hay datos que cumplan esta condición")


      # Cáculos por canal
      Canal_C = data_ult_eje.groupby(['Canal'])['duracion_en_minutos'].sum().to_frame().reset_index()
      Canal_C['Duración_Horas'] = Canal_C['duracion_en_minutos'] / 60
      Canal_C.loc[Canal_C['Canal'] == '1', 'Canal'] = 'Retail'
      Canal_C.loc[Canal_C['Canal'] == '2', 'Canal'] = 'Scantrack'
      # Total Horas Canal Retail
      if tam_Ret != 0:
        T_R = round(Canal_C.loc[Canal_C['Canal']=='Retail', 'Duración_Horas'].values[0], 2)
      else:
        T_R = 0

      if tam_Scan !=0:
        T_S = round(Canal_C.loc[Canal_C['Canal']=='Scantrack', 'Duración_Horas'].values[0], 2)
      else:
        T_S = 0

      # Total Horas Canal Scantrack (Validar cuando no haya retail)

      #T_S = round(Canal_C['Duración_Horas'].iloc[0], 2)

      ## Gráficos Totalizadores
      reporting.rep_logger.debug("Generando gráfica de totales")## Cálculo Total Horas
      fig_ToT = make_subplots(
          rows=2, cols=3,
          specs=[[{"type": "domain"}, {"type": "domain"}, {"type": "domain"}],[{"type": "domain"}, {"type": "domain"}, {"type": "domain"}]])

      # Gráfico Totalizados de Horas
      fig_ToT.add_trace(go.Indicator(
          mode="number",
          value=total_h,
          title={
              "text": "<span style='font-size:0.9em;color:green'>TOTAL<br>HORAS<br><span style='font-size:0.8em;color:gray'>Total de horas</span><br><span style='font-size:0.8em;color:gray'>procesadas</span>"}),
          row=1, col=1)

      # Gráfico Totalizador categorías canal Retail
      fig_ToT.add_trace(go.Indicator(
          mode="number",
          value=Cat_Retail,
          title={
              "text": "<span style='font-size:0.9em;color:green'>CATEGORÍAS<br>RETAIL<br><span style='font-size:0.8em;color:gray'>Total de Categorías</span><br><span style='font-size:0.8em;color:gray'>Canal Retail</span>"}),
          row=1, col=2)

      # Gráfico Totalizador categorías canal Scantrack
      fig_ToT.add_trace(go.Indicator(
          mode="number",
          value=Cat_Scantrack,
          title={
              "text": "<span style='font-size:0.9em;color:green'>CATEGORÍAS<br>SCANTRACK<br><span style='font-size:0.8em;color:gray'>Total de Categoría</span><br><span style='font-size:0.8em;color:gray'>Canal Scantrack</span>"}),
          # delta={'reference': Cat_Sca, 'relative': True, 'increasing': {'color': "yellow"},'decreasing': {'color': "yellow"}}),
          row=1, col=3)

      # Totalizador Horas Canal Retail
      fig_ToT.add_trace(go.Indicator(
          mode="number",
          value=T_R,
          title={
              "text": "<span style='font-size:0.9em;color:green'>TOTAL<br>HORAS RETAIL<br><span style='font-size:0.8em;color:gray'>Total de horas</span><br><span style='font-size:0.8em;color:gray'>procesadas</span>"}),
          # delta={'reference': Hor_Ret, 'relative': True, 'increasing': {'color': "red"}, 'decreasing': {'color': "green"}}),
          row=2, col=1)

      # Totalizador Horas Canal Scantrank
      fig_ToT.add_trace(go.Indicator(
          mode="number",
          value=T_S,
          title={
              "text": "<span style='font-size:0.9em;color:green'>TOTAL<br>HORAS SCANTRACK<br></span><span style='font-size:0.8em;color:gray'>Total de horas</span><br><span style='font-size:0.8em;color:gray'>procesadas</span>"}),
          row=2, col=3)

      fig_ToT.write_image(self.output_path +'Totalizadores.svg',  format="svg", scale=0.9)
      reporting.rep_logger.debug("Finaliza gráfica de totalizadores")

      ## Cálculo particione procesadas y descartadas
      PartP_Modo = data_ult_eje.groupby(['modo_ejecucion'])['cantidad_part_procesada'].sum().to_frame().reset_index()
      PartD_Modo = data_ult_eje.groupby(['modo_ejecucion'])['cantidad_part_descartadas'].sum().to_frame().reset_index()
      Part_Modo = pd.merge(PartP_Modo, PartD_Modo, on='modo_ejecucion')

      Cat_Proc = data_ult_eje.groupby(['Categoría'])['cantidad_part_procesada'].sum().to_frame().reset_index()
      Cat_Desc = data_ult_eje.groupby(['Categoría'])['cantidad_part_descartadas'].sum().to_frame().reset_index()
      Cat_Part = pd.merge(Cat_Proc, Cat_Desc, on='Categoría')

      Particiones_Procesadas = data_ult_eje['cantidad_part_procesada'].sum()
      Particiones_Descartadas = data_ult_eje['cantidad_part_descartadas'].sum()

      label_part = ['Particiones Procesadas', 'Particiones Descartadas']
      values_part = [Particiones_Procesadas, Particiones_Descartadas]

      # Gráfico particiones procesadas vs descartadas
      reporting.rep_logger.debug("Generando gráfico de totales vs descartados")## Cálculo Total Horas
      fig1 = make_subplots(
          rows=2, cols=2,
          specs=[[{"rowspan": 2}, {"type": "domain"}],
                 [None, {}, ]],
          subplot_titles=("# de particiones procesadas y descartadas<br>según la categoría",
                          "Cantidad y porcentaje de particiones<br> procesadas y descartadas",
                          "Número de particiones según<br>el modo de ejecución"))

      fig1.add_trace(go.Pie(labels=label_part,
                            values=values_part, hole=.7, text=[round(val, 2) for val in values_part],
                            textposition='inside',
                            marker_colors=['rgb(0,68,27)', 'rgb(255,102,102)']), row=1, col=2)

      fig1.add_trace(
          go.Bar(y=Cat_Part['Categoría'], x=Cat_Part['cantidad_part_procesada'], name='Particiones Procesadas',
                 orientation='h', showlegend=False,
                 marker=dict(color='rgb(0,68,27)')), row=1, col=1)
      fig1.add_trace(
          go.Bar(y=Cat_Part['Categoría'], x=Cat_Part['cantidad_part_descartadas'], name='Particiones Descartadas',
                 orientation='h', showlegend=False,
                 marker=dict(color='rgb(255,102,102)')), row=1, col=1)
      fig1.add_trace(
          go.Bar(name="Particiones Procesadas", x=Part_Modo["modo_ejecucion"], y=Part_Modo["cantidad_part_procesada"],
                 marker_color='rgb(0,68,27)', showlegend=False,
                 offsetgroup=0), row=2, col=2)
      fig1.add_trace(
          go.Bar(name="Particiones Descartadas", x=Part_Modo["modo_ejecucion"],
                 y=Part_Modo["cantidad_part_descartadas"],
                 marker_color='rgb(255,102,102)', showlegend=False,
                 offsetgroup=1), row=2, col=2)
      fig1.update_layout(template="seaborn", barmode='stack',
                         height=600,width=900,xaxis_tickangle=45,
                         font=dict(family="Calibri", size=18, color="#7a4b4b"))

      fig1.write_image(self.output_path +'Procesados_Descartados.svg',  format="svg", scale=1)
      reporting.rep_logger.debug("Finaliza gráfica de totales versus descartados")
      ## Cálculos para box_plot duración en minutos
      df_descrp = data_ult_eje.describe()
      df_max = df_descrp.iloc[[1, 3, 7]]
      df_descrp2 = df_descrp.iloc[[1, 3, 4, 5, 6, 7]]

      ## Gráfico box_plot duración en minutos
      reporting.rep_logger.debug("Generar Box plot de duración")## Cálculo Total Horas
      fig_box = go.Figure(
          data=[go.Box(y=df_descrp2["duracion_en_minutos"], name='Duración Minutos', marker_color='#3D9970')])
      fig_box.update_layout(title_text="Distribución duración de procesamiento en minutos", template="seaborn")
      fig_box.update_yaxes(title_text="Minutos")
      fig_box.write_image(self.output_path +'Box_plot_duracion.svg',  format="svg", scale=1)

      reporting.rep_logger.debug("Finaliza box plot de duración")## Cálculo Total Horas
      ## Cálculo tamaño promedio de partición
      prom_tam_part = data_ult_eje['tamano_particion'].mean()

      ## Cálculo total bytes transportados
      Total_Bytes = data_ult_eje['tamano_archivo'].sum()
      Total_Bytes = int(Total_Bytes)

      ## Gráficos Totalizadores promedio_particiones analizadas y bytes transportados
      fig_ToT_PB = make_subplots(
          rows=1, cols=2,
          specs=[[{"type": "domain"}, {"type": "domain"}]])

      fig_ToT_PB.add_trace(go.Indicator(
          mode="number",
          value=Total_Bytes,
          title={
              "text": "TOTAL<br>BYTES<br><span style='font-size:0.8em;color:gray'>Cantidad Total de Bytes</span><br><span style='font-size:0.8em;color:gray'>Transportados</span>"}),
          ##delta={'reference': Bytes_Tot, 'relative': True, 'increasing': {'color': "green"},'decreasing': {'color': "red"}}),
          row=1, col=1
      )

      fig_ToT_PB.add_trace(go.Indicator(
          mode="number",
          value=prom_tam_part,
          title={
              "text": "CANTIDAD DE <br>PARTICIONES PROMEDIO<br><span style='font-size:0.8em;color:gray'>Promedio de</span><br><span style='font-size:0.8em;color:gray'>Particiones Analizadas</span>"}),
          # delta={'reference': Bytes_Tot, 'relative': True, 'increasing': {'color': "green"},'decreasing': {'color': "red"}}),
          row=1, col=2
      )

      fig_ToT_PB.write_image(self.output_path +'Tot_Part_Bytes.svg',  format="svg", scale=1)
      reporting.rep_logger.debug("Finaliza creación transporte en bytes")## Cálculo Total Horas
      ## Filtrar última ejecución base de datos proceso_ejecución (fecha más reciente)
      fec_ult_eje_2 = data2['Fecha_Ini_Aux'].max()
      data_ult_eje_2 = data2[data2['Fecha_Ini_Aux'] == fec_ult_eje]

      Total_Arch = data_ult_eje_2['Total_Archivos'].sum()
      Arch_Error = data_ult_eje_2['cantidad_archivos_error'].sum()
      Arch_Descar = data_ult_eje_2['archivos_descartados'].sum()
      Arch_Proces = Total_Arch - (Arch_Error + Arch_Descar)

      valuesA = [Arch_Proces, Arch_Error, Arch_Descar]
      lablesA = ['Archivos Procesados', 'Archivos con Error', 'Archivos Descartados']

      Figura_Arc = go.Figure(data=[
          go.Pie(labels=lablesA, values=valuesA, hole=.7, text=[round(val, 2) for val in valuesA],
                 textposition='inside',
                 marker_colors=['rgb(5,255,255)', 'rgb(41,24,107)', 'rgb(117,127,221)'])])
      Figura_Arc.update_layout(title_text="Distribución porcentual de archivos<br>según su estado", template="seaborn")
      Figura_Arc.write_image(self.output_path +'Cantidad_Arch.svg',  format="svg", scale=1)
      reporting.rep_logger.info("Discriminación de archivos procesados, descartados y con error")
      reporting.rep_logger.info('Las gráficas se han guardado exitosamente')
      self.create_html_from_folder()
      reporting.rep_logger.info("Archivo HTML generado con éxito")
      return True


  def execution_report_dashboard(self, start_date, end_date=None):
    data1 = self.data1
    data2 = self.data2
    external_stylesheets = ["https://stackpath.bootstrapcdn.com/bootstrap/4.4.1/css/bootstrap.min.css"]
    external_scripts = ["https://code.jquery.com/jquery-3.2.1.slim.min.js", "https://cdnjs.cloudflare.com/ajax/libs/popper.js/1.12.9/umd/popper.min.js",
    "https://maxcdn.bootstrapcdn.com/bootstrap/4.0.0/js/bootstrap.min.js"]
    app = dash.Dash(__name__,external_scripts=external_scripts, external_stylesheets=external_stylesheets)
    app.config.suppress_callback_exceptions = True
    

    fig_glossary = make_subplots(rows=1, cols=1,
                        specs=[[{"type": "table"}]], )

    fig_glossary.add_trace(
        go.Table(
            columnwidth=[30, 70],
            header=dict(
                values=["CONCEPTO", "DEFINICIÓN"],
                font=dict(color='white', size=12),
                line=dict(color='#506784'),
                fill=dict(color='grey'), 
                align="center"
            ),
            cells=dict(
                values=[self.data3[k].tolist() for k in self.data3.columns[0:]],
                line=dict(color='#506784'),
                fill=dict(color='white'),
                font=dict(color='#506784'),
                align="center")
        ),
        row=1, col=1
    )


    app.layout = html.Div([
      dcc.Location(id='url', refresh=False),
      html.Div(id='page-content',className="container-xl", style={'max-width':'1640px'})
    ])
    
    image_filename = r"./reports/logo-oasis-negro.png"
    with open(image_filename, 'rb') as image_file:
      encoded_image = base64.b64encode(image_file.read()).decode("utf-8")

    image_IND001 = r"./reports/IND001.png"
    with open(image_IND001, 'rb') as IND001:
        encoded_image1 = base64.b64encode(IND001.read()).decode("utf-8")

    image_IND002 = r"./reports/IND002.png"
    with open(image_IND002, 'rb') as IND002:
        encoded_image2 = base64.b64encode(IND002.read()).decode("utf-8")

    image_IND003 = r"./reports/IND003.png"
    with open(image_IND003, 'rb') as IND003:
        encoded_image3 = base64.b64encode(IND003.read()).decode("utf-8")

    index_page = html.Div([
        html.Nav([
          html.A(children=[html.Img(src='data:image/png;base64,%s' % (encoded_image)  , width= '136px', height='56px')], className='navbar-brand mb-0 h1'),
          html.Div(children=[
            html.Ul(children=[
              html.Li(html.A('Indicadores: IND001',title = 'Trazabilidad Tiempos de Ejecución', href='/page-1', className="nav-link"), className="nav-item"),
              html.Li(html.A('Indicadores: IND002',title = '% Registros procesados vs. descartados', href='/page-2', className="nav-link"), className="nav-item"),
              html.Li(html.A('Indicadores: IND003',title = 'Trazabilidad de archivos', href='/page-3', className="nav-link"), className="nav-item"),
              #html.Li(html.A('Indicadores: IND004',title = 'Indicadores Generales', href='/page-4', className="nav-link"), className="nav-item"),
              html.Li(html.A('Indicadores: IND004',title = 'Última Ejecución', href='/page-4', className="nav-link"), className="nav-item"),
              html.Li(html.A('Cerrar', href='/shutdown', className="nav-link"), className="nav-item")], className="navbar-nav mr-auto")],  
            className="collapse navbar-collapse")
        ], className="navbar navbar-expand-lg navbar-light", style={'background-color': '#d8de71'}),
       #carousel
        html.Div(children=[
            html.Ol(children=[
                html.Li(className='active', **{'data-taget':'#carouselInd', 'data-slide-to':'0'}),
                html.Li(**{'data-taget':'#carouselInd', 'data-slide-to':'1'}),
                html.Li(**{'data-taget':'#carouselInd', 'data-slide-to':'2'})
            ], className='carousel-indicators'),
            html.Div(children=[
                html.Div([
                    html.Img(src='data:image/png;base64,%s' % (encoded_image1)  , width= '220px', height='800px', className='d-block w-100')
                ], className='carousel-item active'),
                html.Div([
                    html.Img(src='data:image/png;base64,%s' % (encoded_image2)  , width= '220px', height='800px', className='d-block w-100')
                ], className='carousel-item'),
                html.Div([
                    html.Img(src='data:image/png;base64,%s' % (encoded_image3)  , width= '220px', height='800px', className='d-block w-100')
                ], className='carousel-item'),

            ], className='carousel-inner'),
            html.A(children=[
                html.Span(className='carousel-control-prev-icon',**{'aria-hidden':'true'}),
                html.Span('Anterior', className='sr-only')
            ], className='carousel-control-prev', href="#carouselInd", **{'role':'button', 'data-slide':'prev'}),
            html.A(children=[
                html.Span(className='carousel-control-next-icon',**{'aria-hidden':'true'}),
                html.Span('Siguiente', className='sr-only')
            ], className='carousel-control-next', href="#carouselInd", **{'role':'button', 'data-slide':'next'}),
        ], className='carousel slide carousel-fade',id="carouselInd", **{'data-ride':'carousel'}),
        html.Br(),
        #texto explicativo
        html.H1("""Glosario"""),
        html.P("En la siguiente tabla se muestra la información de términos que se muestran en las gráficas", className='Lead'),
      html.Div(children=[
        html.Div([ dcc.Graph(id='figure4', figure=fig_glossary),]),
      ], className="row"),
    ])

    d1 = self.data1
    d2 = pd.read_csv(self.complete_filename, sep =';',encoding='latin', parse_dates=['fecha_inicio', 'fecha_fin'], infer_datetime_format=True,chunksize=200)
    d3 = pd.read_csv(self.complete_filename, sep=';', encoding='latin', parse_dates=['fecha_inicio', 'fecha_fin'],infer_datetime_format=True, chunksize=200)
    d4 = pd.read_csv(self.complete_filename, sep=';', encoding='latin', parse_dates=['fecha_inicio', 'fecha_fin'],infer_datetime_format=True, chunksize=200)
    d5 = pd.read_csv(self.complete_filename, sep=';', encoding='latin', parse_dates=['fecha_inicio', 'fecha_fin'],infer_datetime_format=True, chunksize=200)
    page_1_layout = lay1.create_layout_ind0001(d1, start_date, end_date)
    page_2_layout = lay2.create_layout_ind0002(d2, start_date, end_date)
    
    page_3_layout = lay3.create_layout_ind0003(data2,d4 ,start_date, end_date)
    #page_4_layout = lay4.create_layout_ind0004(d3, start_date, end_date)
    
    page_4_layout = lay4.create_layout_ind0004(data2,d5 ,start_date, end_date)
    if page_1_layout is None:
        return print("No hay datos que cumplan esta condición 1")
    if page_2_layout is None:
        return print("No hay datos que cumplan esta condición 2")
    if page_3_layout is None:
        return print("No hay datos que cumplan esta condición 3")
    if page_4_layout is None:
        return print("No hay datos que cumplan esta condición 4")

    # Update the index
    @app.callback(dash.dependencies.Output('page-content', 'children'),
                  [dash.dependencies.Input('url', 'pathname')])
    def display_page(pathname):
      if pathname == '/page-1':
        return page_1_layout
      elif pathname == '/page-2':
        return page_2_layout
      elif pathname == '/page-3':
        return page_3_layout
      elif pathname == '/page-4':
        return page_4_layout
      #elif pathname == '/page-5':
        #return page_5_layout
      elif pathname == '/shutdown':
        shutdown()
      else:
        return index_page
      # You could also return a 404 "URL not found" page here

    def shutdown():
      func = request.environ.get('werkzeug.server.shutdown')
      if func is None:
        raise RuntimeError('Not running with the Werkzeug Server')
      func()

    #if __name__ == '__main__':
    app.logger.disabled=True
    app.run_server(debug=False)

    #return app





