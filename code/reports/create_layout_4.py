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
from dash.dependencies import Input, Output
import base64
import pdb

def create_layout_ind0004(data2, data1, start_date, end_date):

  ## Definición Valores primera ejecución
  # Total Horas
  Horas_ToT = 1.66
  # Total Categorías Retail
  Cat_Ret = 13
  # Total Categorías Scantrack
  Cat_Sca = 19
  # Horas Retail
  Hor_Ret = 0.59
  # Horas Scantrack
  Hor_Sca = 1.08

  ## Definición Valores primera ejecución
  # Total Particiones
  Partic_Total = 337
  # Total Registros
  Regi_Total = 3370000
  # Total Registros Retail
  Regi_Total_R = 1220000
  # Total Archivos Scantrack
  Regi_Total_S = 2150000

  ## Definición Valores primera ejecución
  # Total Archivos
  Arch_Tot = 32
  # Bytes Transportados
  Bytes_Tot = int(1175)
  # Total Archivos Retail
  Arc_R_T = 13
  # Total Archivos Scantrack
  Arc_S_T = 19

  data2["Fecha_Ini_Aux"] = pd.to_datetime(data2["fecha_inicio"]).dt.date
  data2['Fecha_Ini_Aux'] = pd.to_datetime(data2['Fecha_Ini_Aux'])

  if start_date != 0 and end_date == 0:
    data2 = data2[data2['Fecha_Ini_Aux'].astype(str).str[:10] == start_date]
  else:
    mask = (data2['Fecha_Ini_Aux'] >=  datetime.strptime(start_date, "%d-%m-%Y")) & (data2['Fecha_Ini_Aux'] <=  datetime.strptime(end_date, "%d-%m-%Y"))
    data2 = data2.loc[mask]

  if data2.shape[0] == 0:
      return None
  
  data2['Total_Archivos'] = data2['archivos_ftp'] + data2['archivos_locales']
  data2['Archivos_procesados'] = data2['Total_Archivos'] - (
          data2['cantidad_archivos_error'] + data2['archivos_descartados'])

  data = pd.DataFrame()
  for chunk in data1:
    chunk["Fecha_Ini_Aux"] = pd.to_datetime(chunk["fecha_inicio"]).dt.date
    chunk['Fecha_Ini_Aux'] = pd.to_datetime(chunk['Fecha_Ini_Aux'])

    if start_date != 0 and end_date == 0:
      chunk = chunk[chunk['Fecha_Ini_Aux'].astype(str).str[:10] == start_date]
    else:
      mask = (chunk['Fecha_Ini_Aux'] >=  datetime.strptime(start_date, "%d-%m-%Y")) & (chunk['Fecha_Ini_Aux'] <=  datetime.strptime(end_date, "%d-%m-%Y"))
      chunk = chunk.loc[mask]

    data = pd.concat([data, chunk])
      
  if data.shape[0] == 0:
      return None

   ## Filtrar última ejecución (fecha más reciente)
  fec_ult_eje = data['Fecha_Ini_Aux'].max()
  data_ult_eje = data[data['Fecha_Ini_Aux'] == fec_ult_eje]

  new = data_ult_eje["nombre_archivo"].str.split('_', n=5, expand=True)
  data_ult_eje["Fuente"] = new[0]
  data_ult_eje["Región"] = new[1]
  data_ult_eje["Canal"] = new[2]
  data_ult_eje["Categoría"] = new[3]
  data_ult_eje["Fecha"] = new[4]

  data_ult_eje['tiempo_extraccion_Min'] = data_ult_eje['tiempo_extraccion'] / 60
  data_ult_eje['tiempo_transformacion_Min'] = data_ult_eje['tiempo_transformacion'] / 60
  data_ult_eje['tiempo_carga_Min'] = data_ult_eje['tiempo_carga'] / 60

  # calculo de variables
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
  list_of_names_4 = Min_Fase['Fase'].to_list()
  list_of_values_5 = Min_Fase['Duración_Minutos'].to_list()

  Min_Extraccion = data_ult_eje.groupby(['modo_ejecucion'])['tiempo_extraccion_Min'].sum().to_frame().reset_index()
  Min_Transcion = data_ult_eje.groupby(['modo_ejecucion'])['tiempo_transformacion_Min'].sum().to_frame().reset_index()
  Min_Carga = data_ult_eje.groupby(['modo_ejecucion'])['tiempo_carga_Min'].sum().to_frame().reset_index()

  join1 = pd.merge(Min_Extraccion, Min_Transcion, on='modo_ejecucion')
  join2 = pd.merge(join1, Min_Carga, on='modo_ejecucion')

  list_of_names = join2['modo_ejecucion'].to_list()
  list_of_values = join2['tiempo_extraccion_Min'].to_list()

  list_of_names_1 = join2['modo_ejecucion'].to_list()
  list_of_values_1 = join2['tiempo_transformacion_Min'].to_list()

  list_of_names_2 = join2['modo_ejecucion'].to_list()
  list_of_values_2 = join2['tiempo_carga_Min'].to_list()
  
  Figura_Fases = go.Figure(data=[go.Pie(labels=list_of_names_4, values=list_of_values_5, hole=.7, text=[round(val, 2) for val in list_of_values_5],textposition='inside',
                                                                marker_colors=['rgb(5,255,255)', 'rgb(41,24,107)', 'rgb(117,127,221)'])])
  
  ## Gráfica 1
  fig_carga = make_subplots(rows=1, cols=3, specs=[[{"type": "domain"}, {"type": "domain"},{"type": "domain"}]],
                          subplot_titles=("Extracción",
                                          "Transformación",
                                          "Carga"))
  fig_carga.add_trace(go.Pie(labels=list_of_names, values=list_of_values, hole=.7, text=[round(val, 2) for val in list_of_values], textposition='inside',
                          marker_colors=['rgb(211,238,128)', 'rgb(12,192,170)', 'rgb(77,194,84)']), row=1, col=1)
  fig_carga.add_trace(go.Pie(labels=list_of_names_1, values=list_of_values_1, hole=.7, text=[round(val, 2) for val in list_of_values_1], textposition='inside',
                          marker_colors=['rgb(211,238,128)', 'rgb(12,192,170)', 'rgb(77,194,84)']), row=1, col=2)
  fig_carga.add_trace(go.Pie(labels=list_of_names_2, values=list_of_values_2, hole=.7, text=[round(val, 2) for val in list_of_values_2], textposition='inside',
                          marker_colors=['rgb(211,238,128)', 'rgb(12,192,170)', 'rgb(77,194,84)']), row=1, col=3)
  
  ## Gráfico 2
  ## Filtro Canales Retail = 1 y Scantrack = 2
  Retail = data_ult_eje[data_ult_eje['Canal'] == '1']
  Scantrack = data_ult_eje[data_ult_eje['Canal'] == '2']
  tam_Ret = len(Retail)

  if tam_Ret != 0:
      ## Cálculos Canal Retail
      Min_Extraccion_Retail = Retail.groupby(['Categoría'])['tiempo_extraccion_Min'].sum().to_frame().reset_index()
      Min_Transcion_Retail = Retail.groupby(['Categoría'])['tiempo_transformacion_Min'].sum().to_frame().reset_index()
      Min_Carga_Retail = Retail.groupby(['Categoría'])['tiempo_carga_Min'].sum().to_frame().reset_index()

      join1_Retail = pd.merge(Min_Extraccion_Retail, Min_Transcion_Retail, on='Categoría')
      join2_Retail = pd.merge(join1_Retail, Min_Carga_Retail, on='Categoría')

      join2_Retail['Suma_Retail'] = join2_Retail.sum(axis=1)
      join2_Retail['Extracción_Retail'] = round(
          (join2_Retail['tiempo_extraccion_Min'] / join2_Retail['Suma_Retail']) * 100)
      join2_Retail['Transformación_Retail'] = round(
          (join2_Retail['tiempo_transformacion_Min'] / join2_Retail['Suma_Retail']) * 100)
      join2_Retail['Carga_Retail'] = round((join2_Retail['tiempo_carga_Min'] / join2_Retail['Suma_Retail']) * 100)
      join2_Retail['Extracción_Retail'].fillna(0, inplace=True)
      join2_Retail['Extracción_Retail'].astype('int')

      join2_Retail['sum'] = join2_Retail['Extracción_Retail'] + join2_Retail['Transformación_Retail'] + join2_Retail['Carga_Retail']
      join2_Retail.loc[join2_Retail['sum'] != 100, 'Extracción_Retail'] = join2_Retail.loc[join2_Retail['sum'] != 100, 'Extracción_Retail'] + 1
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
      Cat_Retail = join2_Retail['Categoría'].nunique()
  else:
      Cat_Retail = 0
      print("No hay datos que cumplan esta condición")

  tam_Scan = len(Scantrack)
  
  ## Cálculos Canal Scantrack

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
      join2_Scantrack['Extracción_Scantrack'].fillna(0, inplace=True)
      join2_Scantrack['Extracción_Scantrack'].astype('int')

      join2_Scantrack['sum'] = join2_Scantrack['Extracción_Scantrack'] + join2_Scantrack['Transformación_Scantrack'] + join2_Scantrack['Carga_Scantrack']
      join2_Scantrack.loc[join2_Scantrack['sum'] != 100, 'Extracción_Scantrack'] = join2_Scantrack.loc[join2_Scantrack['sum'] != 100, 'Extracción_Scantrack'] + 1
      join2_Scantrack.loc[join2_Scantrack['sum'] > 100, 'Extracción_Scantrack'] = join2_Scantrack.loc[join2_Scantrack['sum'] > 100, 'Extracción_Scantrack'] - 2                                                                                                       
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

      ## Total categorías canal Scantrack
      Cat_Scantrack = join2_Scantrack['Categoría'].nunique()

  else:
      Cat_Scantrack = 0
      print("No hay datos que cumplan esta condición")

  # calculo de variables
  Min_Cat_Ext1 = data_ult_eje.groupby(['tiempo_extraccion_Min', 'Categoría']).sum().reset_index()
  Min_Cat_Ext1['Fase'] = 'Extracción'
  Min_Cat_Ext1['Cantidad Horas'] = Min_Cat_Ext1['tiempo_extraccion_Min']

  Min_Cat_Transcion1 = data_ult_eje.groupby(['tiempo_transformacion_Min', 'Categoría']).sum().reset_index()
  Min_Cat_Transcion1['Fase'] = 'Transformación'
  Min_Cat_Transcion1['Cantidad Horas'] = Min_Cat_Transcion1['tiempo_transformacion_Min']

  Min_Cat_Carga1 = data_ult_eje.groupby(['tiempo_carga_Min', 'Categoría']).sum().reset_index()
  Min_Cat_Carga1['Fase'] = 'Carga'
  Min_Cat_Carga1['Cantidad Horas'] = Min_Cat_Carga1['tiempo_carga_Min']

  join = pd.concat([Min_Cat_Ext1, Min_Cat_Transcion1, Min_Cat_Carga1])

  fig5 = px.bar(join, x="Fase", y='Cantidad Horas', color='Categoría',
                color_discrete_sequence=px.colors.sequential.Viridis)

  ## Cálculo Total Horas
  total_h = float(round(Min_Fase['Duración_Minutos'].sum(), 2))

  # Cáculos por canal
  Canal_C = data_ult_eje.groupby(['Canal'])['duracion_en_minutos'].sum().to_frame().reset_index()
  Canal_C['Duración_Horas'] = Canal_C['duracion_en_minutos'] / 60
  Canal_C.loc[Canal_C['Canal'] == '1', 'Canal'] = 'Retail'
  Canal_C.loc[Canal_C['Canal'] == '2', 'Canal'] = 'Scantrack'
  
  # Total Horas Canal Retail
  if tam_Ret != 0:
      T_R = round(Canal_C.loc[Canal_C['Canal'] == 'Retail', 'Duración_Horas'].values[0], 2)
  else:
      T_R = 0

  # Total Horas Canal Scantrack
  if tam_Scan != 0:
      T_S = round(Canal_C.loc[Canal_C['Canal'] == 'Scantrack', 'Duración_Horas'].values[0], 2)
  else:
      T_S = 0

  ## Gráficos Totalizadores
  fig_ToT = make_subplots(
      rows=1, cols=5,
      specs=[[{"type": "domain"}, {"type": "domain"}, {"type": "domain"}, {"type": "domain"}, {"type": "domain"}]])

  # Gráfico Totalizados de Horas
  fig_ToT.add_trace(go.Indicator(
      mode="number+delta",
      value=total_h,
      title={
          "text": "<span style='font-size:0.9em;color:green'>TOTAL<br>HORAS<br><span style='font-size:0.8em;color:gray'>Total de horas</span><br><span style='font-size:0.8em;color:gray'>procesadas</span>"},
      delta={'reference': Horas_ToT, 'relative': True, 'increasing': {'color': "red"},
             'decreasing': {'color': "green"}}),
      row=1, col=1)

  # Gráfico Totalizador categorías canal Retail
  fig_ToT.add_trace(go.Indicator(
      mode="number+delta",
      value=Cat_Retail,
      title={
          "text": "<span style='font-size:0.9em;color:green'>CATEGORÍAS<br>RETAIL<br><span style='font-size:0.8em;color:gray'>Total de Categorías</span><br><span style='font-size:0.8em;color:gray'>Canal Retail</span>"},
      delta={'reference': Cat_Ret, 'relative': True, 'increasing': {'color': "yellow"},
             'decreasing': {'color': "yellow"}}),
      row=1, col=2)
  
  # Gráfico Totalizador categorías canal Scantrack
  fig_ToT.add_trace(go.Indicator(
      mode="number+delta",
      value=Cat_Scantrack,
      title={
          "text": "<span style='font-size:0.9em;color:green'>CATEGORÍAS<br>SCANTRACK<br><span style='font-size:0.8em;color:gray'>Total de Categoría</span><br><span style='font-size:0.8em;color:gray'>Canal Scantrack</span>"},
      delta={'reference': Cat_Sca, 'relative': True, 'increasing': {'color': "yellow"},
             'decreasing': {'color': "yellow"}}),
      row=1, col=3)

  # Totalizador Horas Canal Retail
  fig_ToT.add_trace(go.Indicator(
      mode="number+delta",
      value=T_R,
      title={
          "text": "<span style='font-size:0.9em;color:green'>TOTAL<br>HORAS RETAIL<br><span style='font-size:0.8em;color:gray'>Total de horas</span><br><span style='font-size:0.8em;color:gray'>procesadas</span>"},
      delta={'reference': Hor_Ret, 'relative': True, 'increasing': {'color': "red"}, 'decreasing': {'color': "green"}}),
      row=1, col=4)

  # Totalizador Horas Canal Scantrank
  fig_ToT.add_trace(go.Indicator(
      mode="number+delta",
      value=T_S,
      title={
          "text": "<span style='font-size:0.9em;color:green'>TOTAL<br>HORAS SCANTRACK<br><span style='font-size:0.8em;color:gray'>Total de horas</span><br><span style='font-size:0.8em;color:gray'>procesadas</span>"},
      delta={'reference': Hor_Sca, 'relative': True, 'increasing': {'color': "red"}, 'decreasing': {'color': "green"}}),
      row=1, col=5)
  
  ## FAMILIA DE INDICADORES 2 %REGISTROS PROCESADOS VS DESCARTADOS

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

  # crear grfica 1
  fig1 = make_subplots(
      rows=2, cols=2,
      specs=[[{"rowspan": 2}, {"type": "domain"}],
             [None, {}, ]],
      subplot_titles=("# de particiones procesadas y descartadas<br>según la categoría",
                      "Cantidad y porcentaje de particiones<br> procesadas y descartadas",
                      "Número de particiones según<br>el modo de ejecución"))

  fig1.add_trace(go.Pie(labels=label_part,
                        values=values_part, hole=.7, text= [round(val,2)  for val in values_part], textposition='inside',
                        marker_colors=['rgb(0,68,27)', 'rgb(255,102,102)']), row=1, col=2)

  fig1.add_trace(go.Bar(y=Cat_Part['Categoría'], x=Cat_Part['cantidad_part_procesada'], name='Particiones Procesadas',
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
      go.Bar(name="Particiones Descartadas", x=Part_Modo["modo_ejecucion"], y=Part_Modo["cantidad_part_descartadas"],
             marker_color='rgb(255,102,102)', showlegend=False,
             offsetgroup=1), row=2, col=2)
  fig1.update_layout(template="seaborn", barmode='stack',
                     height=600, xaxis_tickangle=45,
                     font=dict(family="Calibri", size=18, color="#7a4b4b"))

  # creacion de datos segunda grafica

  Cat_Proc1 = data_ult_eje.groupby(['cantidad_part_procesada', 'Categoría']).sum().reset_index()
  Cat_Proc1['Tipo Partición'] = 'Particiones Procesadas'
  Cat_Proc1['Cantidad Particiones'] = Cat_Proc1['cantidad_part_procesada']
  Cat_Desc1 = data_ult_eje.groupby(['cantidad_part_descartadas', 'Categoría']).sum().reset_index()
  Cat_Desc1['Tipo Partición'] = 'Particiones Descartadas'
  Cat_Desc1['Cantidad Particiones'] = Cat_Desc1['cantidad_part_descartadas']
  join_DP = pd.concat([Cat_Proc1, Cat_Desc1])

  fig_6 = px.bar(join_DP, y="Tipo Partición", x="Cantidad Particiones",
                 orientation='h', color='Categoría', color_discrete_sequence=px.colors.sequential.Viridis)

  # datos tercera grafica
  data_ult_eje['Filas_Proces'] = data_ult_eje['tamano_particion'] * data_ult_eje['cantidad_part_procesada']
  data_ult_eje['Filas_Desc'] = data_ult_eje['tamano_particion'] * data_ult_eje['cantidad_part_descartadas']

  Fil_Proc = data_ult_eje.groupby(['Fecha_Ini_Aux'])['Filas_Proces'].sum().to_frame().reset_index()
  Fil_No_Proc = data_ult_eje.groupby(['Fecha_Ini_Aux'])['Filas_Desc'].sum().to_frame().reset_index()
  join_Fil = pd.merge(Fil_Proc, Fil_No_Proc, on='Fecha_Ini_Aux')
  Archivos_Fecha = data_ult_eje.groupby('Fecha_Ini_Aux')['nombre_archivo'].count().to_frame().reset_index()


  ## Cálculo Total Particiones
  Part_Total = Particiones_Procesadas + Particiones_Descartadas

  ## Cálculo Total Registros
  data_ult_eje['Total_reg'] = data_ult_eje['Filas_Proces'] + data_ult_eje['Filas_Desc']
  Total_Reg = data_ult_eje['Total_reg'].sum()

  ## Cálculo Total Registros Canal Retail

  Retail = data_ult_eje[data_ult_eje['Canal'] == '1']
  Scantrack = data_ult_eje[data_ult_eje['Canal'] == '2']

  Reg_Retail = Retail['Total_reg'].sum()
  Reg_Scantrack = Scantrack['Total_reg'].sum()

  ## Gráfico Totalizadores
  fig_ToT_Reg = make_subplots(
      rows=1, cols=4,
      specs=[[{"type": "domain"}, {"type": "domain"}, {"type": "domain"}, {"type": "domain"}]])

  # Gráfico Totalizados de Particiones
  fig_ToT_Reg.add_trace(go.Indicator(
      mode="number+delta",
      value=Part_Total,
      title={
          "text": "TOTAL<br>PARTICIONES<br><span style='font-size:0.8em;color:gray'>Total Particiones</span><br><span style='font-size:0.8em;color:gray'>Analizadas</span>"},
      delta={'reference': Partic_Total, 'relative': True, 'increasing': {'color': "green"},
             'decreasing': {'color': "red"}}),
      row=1, col=1)

  # Gráfico Totalizador Registros
  fig_ToT_Reg.add_trace(go.Indicator(
      mode="number+delta",
      value=Total_Reg,
      title={
          "text": "TOTAL<br>REGISTROS<br><span style='font-size:0.8em;color:gray'>Total Registros</span><br><span style='font-size:0.8em;color:gray'>Analizados</span>"},
      delta={'reference': Regi_Total, 'relative': True, 'increasing': {'color': "green"},
             'decreasing': {'color': "red"}}),
      row=1, col=2)

  # Gráfico Totalizador registros Retail
  fig_ToT_Reg.add_trace(go.Indicator(
      mode="number+delta",
      value=Reg_Retail,
      title={
          "text": "TOTAL<br>REGISTROS RETAIL<br><span style='font-size:0.8em;color:gray'>Total Registros</span><br><span style='font-size:0.8em;color:gray'>Canal Retail</span>"},
      delta={'reference': Regi_Total_R, 'relative': True, 'increasing': {'color': "green"},
             'decreasing': {'color': "red"}}),
      row=1, col=3)

  # Totalizador registros Scantrack
  fig_ToT_Reg.add_trace(go.Indicator(
      mode="number+delta",
      value=Reg_Scantrack,
      title={
          "text": "TOTAL<br>REGISTROS SCANTRACK<br><span style='font-size:0.8em;color:gray'>Total Registros</span><br><span style='font-size:0.8em;color:gray'>Canal Scantrack</span>"},
      delta={'reference': Regi_Total_S, 'relative': True, 'increasing': {'color': "green"},
             'decreasing': {'color': "red"}}),
      row=1, col=4)

  
  ## GRAFICO FAMILIA 3 TRAZABILIDAD ARCHIVOS

  ## Filtrar última ejecución (fecha más reciente)
  fec_ult_eje_2 = data2['Fecha_Ini_Aux'].max()
  data_ult_eje_2 = data2[data2['Fecha_Ini_Aux'] == fec_ult_eje]

  Total_Arch = data_ult_eje_2['Total_Archivos'].sum()
  Arch_Error = data_ult_eje_2['cantidad_archivos_error'].sum()
  Arch_Descar = data_ult_eje_2['archivos_descartados'].sum()
  Arch_Proces = Total_Arch - (Arch_Error + Arch_Descar)

  valuesA = [Arch_Proces, Arch_Error, Arch_Descar]
  lablesA = ['Archivos Procesados', 'Archivos con Error', 'Archivos Descartados']

  Figura_Arc = go.Figure(data=[go.Pie(labels=lablesA, values=valuesA, hole=.7, text=[round(val, 2) for val in valuesA],textposition='inside',
                        marker_colors=['rgb(5,255,255)', 'rgb(41,24,107)', 'rgb(117,127,221)'])])
  Figura_Arc.update_layout(title_text="Distribución porcentual de archivos<br>según su estado", template="seaborn")


  Total_local = data_ult_eje_2['archivos_locales'].sum()
  Total_FTP = data_ult_eje_2['archivos_ftp'].sum()

  labelsFL = ['Archivos Locales', 'Archivos FTP']
  valuesFL = [Total_local, Total_FTP]

  Modo_Arch = data_ult_eje_2.groupby(['modo_ejecucion'])['archivos_locales', 'archivos_ftp'].sum().reset_index()


  fig2 = make_subplots(
      rows=1, cols=2,
      specs=[[{"type": "domain"}, {"type": "xy"}]],
      subplot_titles=("% Archivos Locales VS Archivos FTP",
                      "Archivos FTP - Locales por Modo de Ejecución"))

  fig2.add_trace(go.Pie(labels=labelsFL, values=valuesFL, text= [round(val,2)  for val in valuesFL], textposition='inside',marker_colors=['rgb(5,255,255)', 'rgb(41,24,107)']), row=1, col=1)

  fig2.add_trace(go.Bar(
      y=Modo_Arch['modo_ejecucion'],
      x=Modo_Arch["archivos_ftp"],
      name='archivos_ftp',
      marker_color='rgb(41,24,107)',
      orientation='h',
      offsetgroup=0,
      showlegend=False,
  ), row=1, col=2)

  fig2.add_trace(go.Bar(
      y=Modo_Arch['modo_ejecucion'],
      x=Modo_Arch["archivos_locales"],
      name='archivos_locales',
      marker_color='rgb(5,255,255)',
      orientation='h',
      offsetgroup=1,
      showlegend=False,
      base=Modo_Arch['modo_ejecucion']
  ), row=1, col=2)

  # Cálculo Archivos por Canal
  Canal_A = data_ult_eje.groupby(['Canal'])['nombre_archivo'].count().to_frame().reset_index()
  Canal_A.loc[Canal_A['Canal'] == '1', 'Canal'] = 'Retail'
  Canal_A.loc[Canal_A['Canal'] == '2', 'Canal'] = 'Scantrack'

  # Total Arhivos Canal Retail
  #A_R = Canal_A['nombre_archivo'].iloc[0]
  A_R = 0

  # Total Arhivos Canal Scantrack
  A_S = Canal_A['nombre_archivo'].iloc[0]

  ## Total de bytes transportados
  Total_Bytes = data_ult_eje['tamano_archivo'].sum()
  Total_Bytes = int(Total_Bytes)

  Arch_Canal = data_ult_eje.groupby(['Categoría', 'Canal'])['nombre_archivo'].count().to_frame().reset_index()
  Arch_Canal.rename(columns={'nombre_archivo': 'Cantidad Archivos'}, inplace=True)
  Arch_Canal.Canal = Arch_Canal.Canal.replace({"1": "Retail", "2": "Sacantrack"})

  figur55 = px.bar(Arch_Canal, y='Categoría', x='Cantidad Archivos', color='Canal', orientation='h',
                   title='Cantidad de Archivos por Categoría y Canal')

  ## Filtro Canales Retail = 1 y Scantrack = 2
  Retail = data_ult_eje[data_ult_eje['Canal'] == '1']
  Scantrack = data_ult_eje[data_ult_eje['Canal'] == '2']

  tam_Ret1 = len(Retail)

  if tam_Ret1 != 0:
      # Cáculos bytes transportados por categoría y canal
      Tam_Arch_Retail = Retail.groupby(['Categoría'])['tamano_archivo'].sum().to_frame().reset_index()
      ## Gráfica bytes transportados por categoría y Canal Retail
      figur3 = px.bar(Tam_Arch_Retail, y='Categoría', x='tamano_archivo', orientation='h',title='Cantidad de bytes transportados <br>Canal Retail')
  else:
      print("No hay datos que cumplan esta condición")

  tam_Scan1 = len(Scantrack)

  if tam_Scan1 != 0:
      # Cáculos bytes transportados por categoría y canal
      Tam_Arch_Scantrack = Scantrack.groupby(['Categoría'])['tamano_archivo'].sum().to_frame().reset_index()
      ## Gráfica bytes transportados por categoría y Canal Retail
      figur4 = px.bar(Tam_Arch_Scantrack, y='Categoría', x='tamano_archivo', orientation='h',title='Cantidad de bytes transportados<br>Canal Scantrack')
  else:
      print("No hay datos que cumplan esta condición")

  ## cantidad de Archivos según el modo de ejecución
  Arc_Modo = data_ult_eje.groupby(['modo_ejecucion'])['nombre_archivo'].count().to_frame().reset_index()
  labels_M = Arc_Modo['modo_ejecucion'].to_list()
  values_M = Arc_Modo['nombre_archivo'].to_list()

  figur_M = go.Figure(data=[go.Pie(labels=labels_M, values=values_M, hole=.7, text=[round(val, 2) for val in values_M], textposition='inside', marker_colors=['rgb(0,68,27)', 'rgb(255,102,102)'])])
  figur_M.update_layout(title_text="Cantidad de archivos<br>según modo de ejecución", template="seaborn")

  ## Gráficos Indicadores

  fig_ToT_A = make_subplots(
      rows=1, cols=4,
      specs=[[{"type": "domain"}, {"type": "domain"}, {"type": "domain"}, {"type": "domain"}]])

  # Gráfico Totalizados de Archivos
  fig_ToT_A.add_trace(go.Indicator(
      mode="number+delta",
      value=Total_Arch,
      title={
          "text": "TOTAL<br>ARCHIVOS<br><span style='font-size:0.8em;color:gray'>Cantidad Total de Archivos</span><br><span style='font-size:0.8em;color:gray'>Analizados</span>"},
      delta={'reference': Arch_Tot, 'relative': True, 'increasing': {'color': "green"},
             'decreasing': {'color': "red"}}), row=1, col=1)

  fig_ToT_A.add_trace(go.Indicator(
      mode="number+delta",
      value=Total_Bytes,
      title={
          "text": "TOTAL<br>BYTES<br><span style='font-size:0.8em;color:gray'>Cantidad Total de Bytes</span><br><span style='font-size:0.8em;color:gray'>Transportados</span>"},
      delta={'reference': Bytes_Tot, 'relative': True, 'increasing': {'color': "green"},
             'decreasing': {'color': "red"}}), row=1, col=2)

  fig_ToT_A.add_trace(go.Indicator(
      mode="number+delta",
      value=A_R,
      title={
          "text": "TOTAL<br>ARCHIVOS RETAIL<br><span style='font-size:0.8em;color:gray'>Cantidad Total de Archivos</span><br><span style='font-size:0.8em;color:gray'>Analizados</span>"},
      delta={'reference': Arc_R_T, 'relative': True, 'increasing': {'color': "red"}, 'decreasing': {'color': "green"}}),
      row=1, col=3)

  fig_ToT_A.add_trace(go.Indicator(
      mode="number+delta",
      value=A_S,
      title={
          "text": "TOTAL<br>ARCHIVOS SCANTRACK<br><span style='font-size:0.8em;color:gray'>Cantidad Total de Archivos</span><br><span style='font-size:0.8em;color:gray'>Analizados</span>"},
      delta={'reference': Arc_S_T, 'relative': True, 'increasing': {'color': "red"}, 'decreasing': {'color': "green"}}),
      row=1, col=4)

  image_filename = r"./reports/logo-oasis-negro.png"
  with open(image_filename, 'rb') as image_file:
    encoded_image = base64.b64encode(image_file.read()).decode("utf-8")

  classname = 'col-md-12'
  if tam_Ret != 0 and  tam_Scan != 0:
      classname = 'col-md-6'

  if tam_Ret != 0:
      div_R = html.Div([dcc.Graph(id='figure6', figure=grafica_Retail)], className = classname)
  else:
      div_R = html.Div(hidden = True)

  if tam_Scan != 0:
      div_S = html.Div([dcc.Graph(id='figure6', figure=grafica_Scantrack)], className = classname)
  else:
      div_S = html.Div(hidden = True)

###############################################
  classname = 'col-md-12'
  if tam_Ret1 != 0 and tam_Scan1 != 0:
      classname = 'col-md-6'

  if tam_Ret != 0:
      div_Ret = html.Div([dcc.Graph(id='figure6', figure=figur3)], className=classname)
  else:
      div_Ret = html.Div(hidden=True)

  if tam_Scan != 0:
      div_Sca = html.Div([dcc.Graph(id='figure6', figure=figur4)], className=classname)
  else:
      div_Sca = html.Div(hidden=True)

  page_4_layout = html.Div([
    html.Nav([
      html.A(children=[html.Img(src='data:image/png;base64,%s' % (encoded_image)  , width= '136px', height='56px')], className='navbar-brand mb-0 h1'),
      html.Div(children=[
        html.Ul(children=[
          html.Li(html.A('Indicadores: IND001',title = 'Trazabilidad Tiempos de Ejecución', href='/page-1', className="nav-link"), className="nav-item"),
          html.Li(html.A('Indicadores: IND002',title = '% Registros procesados vs. descartados', href='/page-2', className="nav-link"), className="nav-item"),
          html.Li(html.A('Indicadores: IND003',title = 'Trazabilidad de archivos', href='/page-3', className="nav-link"), className="nav-item"),
          #html.Li(html.A('Indicadores: IND004', title='Indicadores Generales', href='/page-4', className="nav-link"),className="nav-item"),
          html.Li(html.A('Indicadores: IND004', title='Última Ejecución', href='/page-4', className="nav-link"),className="nav-item"),
          html.Li(html.A('Cerrar', href='/shutdown', className="nav-link"), className="nav-item")], className="navbar-nav mr-auto")],  
        className="collapse navbar-collapse")
    ], className="navbar navbar-expand-lg navbar-light", style={'background-color': '#d8de71'}),
    html.Div(children=[
        html.Div(children=[
            html.H1('''Familia Indicadores: IND004 ''',className='display-3', style={'display':'inline'}),
            html.Br(),
            html.H1('REPORTE EJECUCIÓN ACTUAL', className='display-4',style={'font-size':'2.5em'}),
            html.P('Indicadores para el periodo:  %s' % (end_date), className='lead')
          ], className='col')
      ], className="row"),
    html.Br(),

    html.Hr(),
    html.H2('IND001 TRAZABILIDAD TIEMPOS - FASES DE EJECUCIÓN', className='display-5', style={'font-size': '1.5em'}),
    html.Hr(),
    html.Div([html.Div([dcc.Graph(id='figure2', figure=fig_ToT)], className='col-md-12')], className='row'),
    html.Hr(),
    html.H2('TIEMPO PROCESAMIENTO EN HORAS POR FASE Y MODO DE EJECUCIÓN', className='display-4' ,style={'font-size':'1.5em'}),
    html.Div([
    html.Div([dcc.Graph(id='figure4', figure=Figura_Fases)], className='col-md-5'),
    html.Div([dcc.Graph(id='figure5', figure=fig_carga)], className='col-md-7')
    ], className='row'),

    html.Hr(),
    html.H2('TIEMPO PROCESAMIEaTO EN HORAS POR CATEGORIA Y CANAL(Retail y Scantrack)', className='display-4' ,style={'font-size':'1.5em'}),
    html.Div(children=[
      div_R,
      div_S
    ], className='row'),

    html.Hr(),
    html.H2('TIEMPO PROCESAMIENTO EN HORAS POR FASE DE EJECUCIÓN SEGÚN LA CATEGORÍA', className='display-4' ,style={'font-size':'1.5em'}),
    html.Div(children=[
      html.Div([dcc.Graph(id='Gráfico_4', figure=fig5)], className='col-md-12')
    ], className='row'),

    html.Hr(),
    html.H2('IND0002 REGISTROS PROCESADOS VS DESCARTADOS', className='display-5' ,style={'font-size':'1.5em'}),
    html.Hr(),
    html.Div(children=[
      html.Div([dcc.Graph(id='Gráfico_4', figure=fig_ToT_Reg)], className='col-md-12')
    ], className='row'),


    html.Hr(),
    html.H2('CANTIDAD PARTICIONES PROCESADAS VS DESCARTADAS', className='display-4' ,style={'font-size':'1.5em'}),
    html.Div([html.Div([dcc.Graph(id='Gráfico_8', figure=fig1)], className='col-md-12')], className='row'),
    html.Hr(),
    html.H2('CANTIDAD DE PARTICIONES PROCESADAS Y DESCARTADAS SEGÚN LA CATEGORÍA', className='display-4', style={'font-size': '1.5em'}),
    html.Div([html.Div([dcc.Graph(id='Gráfico_6', figure=fig_6)], className='col-md-12')], className='row'),

    html.Hr(),
    html.H2('IND003 TRAZABILIDAD ARCHIVOS', className='display-5' ,style={'font-size':'1.5em'}),
    html.Hr(),
    html.Div(children=[
        html.Div([dcc.Graph(id='Gráfico_4', figure=fig_ToT_A)], className='col-md-12')
    ], className='row'),

    html.Hr(),
    html.H2('CANTIDAD PARTICIONES PROCESADAS VS DESCARTADAS', className='display-4', style={'font-size': '1.5em'}),
    html.Br(),
    html.Div([html.Div(children=[dcc.Graph(id='Gráfico_12', figure=Figura_Arc)], className='col-md-6'),
              html.Div(children=[dcc.Graph(id='Gráfico_13', figure=figur_M)], className='col-md-6')
      ], className='row'),

    #html.Div([html.Div([dcc.Graph(id='Gráfico_10', figure=fig2)], className='col-md-12')], className='row'), #las graficas de ftp ya no son necesarias
    #html.Div([html.Div([dcc.Graph(id='Gráfico_11', figure=figur55)], className='col-md-12')], className='row'), #grafica no necesaria en el momento
    html.Hr(),
    html.H2('CANTIDAD DE BYTES TRANSPORTADOS POR CATEGORÍA Y CANAL', className='display-4',style={'font-size': '1.5em'}),
    html.Div(children=[
        div_Ret,
        div_Sca
        ], className='row'),

    html.Div(
        children=[dcc.Link('Pagina principal', href='/', className='btn btn-lg btn-primary')], className="row"
    )
   ])

  return page_4_layout

