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


def create_layout_ind0005(data1, start_date, end_date):

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

  #crear datos de gráfica 1
  new = data["nombre_archivo"].str.split('_', n=5, expand=True)
  data["Fuente"] = new[0]
  data["Región"] = new[1]
  data["Canal"] = new[2]
  data["Categoría"] = new[3]
  data["Fecha"] = new[4]

  Dur_Modo = data.groupby(['modo_ejecucion'])['duracion_en_minutos'].sum().to_frame().reset_index()
  Dur_Modo['Duración_Horas'] = Dur_Modo['duracion_en_minutos'] / 60
  modo = Dur_Modo['modo_ejecucion'].to_list()
  Durac = Dur_Modo['Duración_Horas'].to_list()

  ## Gráfica tiempos por modo de ejecución

  figur1 = go.Figure(data=[go.Pie(labels=modo, values=Durac, textposition='inside', hole=.7,
                                  marker_colors=['rgb(82,82,82)', 'rgb(217,217,217)'])])
  figur1.update_layout(template="seaborn", title_text="Duración de procesamiento<br> (horas) por Modo de Ejecución",
                       font=dict(family="Calibri",
                                 color="#7a4b4b"))

  # Registros procesados y descartados
  data['Filas_Proces'] = data['tamano_particion'] * data['cantidad_part_procesada']
  data['Filas_Desc'] = data['tamano_particion'] * data['cantidad_part_descartadas']
  Registros_Procesados = data['Filas_Proces'].sum()
  Registros_Descartados = data['Filas_Desc'].sum()
  RegLab = ['Reg. Proc.', 'Reg. Desc']
  RegDat = [Registros_Procesados, Registros_Descartados]



  # Gráfico Procesados Descartados
  figur2 = go.Figure(data=[go.Pie(labels=RegLab,
                                  values=RegDat, textposition='inside',
                                  hole=.7, marker_colors=['rgb(42,35,160)', 'rgb(149,207,216)'])])
  figur2.update_layout(template="seaborn", title_text="Cantidad de Registros Procesados<br> Vs Descartados",
                       font=dict(family="Calibri",
                                 color="#7a4b4b"))

  ## Cáculos horas de procesamiento por canal
  Canal_C = data.groupby(['Canal'])['duracion_en_minutos'].sum().to_frame().reset_index()
  Canal_C['Duración_Horas'] = Canal_C['duracion_en_minutos'] / 60
  Canal_C.loc[Canal_C['Canal'] == '1', 'Canal'] = 'Retail'
  Canal_C.loc[Canal_C['Canal'] == '2', 'Canal'] = 'Scantrack'
  CanalC = Canal_C['Canal'].to_list()
  DuracC = Canal_C['Duración_Horas'].to_list()

  ## Gráfico horas de procesamiento por canal
  figur4 = go.Figure(data=[go.Pie(labels=CanalC, values=DuracC, textposition='inside', hole=.7,
                                  marker_colors=['rgb(0,69,41)', 'rgb(217,240,163)'])])
  figur4.update_layout(template="seaborn", title_text="Duración de procesamiento<br> (horas) por Canal",
                       font=dict(family="Calibri",
                                 color="#7a4b4b"))

  # Cáculos bytes transportados por categoría
  Tam_Arch = data.groupby(['Categoría'])['tamano_archivo'].sum().to_frame().reset_index()

  ## Gráfica bytes transportados por categoría
  figur3 = px.bar(Tam_Arch, y='Categoría', x='tamano_archivo', orientation='h',
                  title='Cantidad de bytes transportados por Categoría')

  ## Total de bytes transportados
  Total_bytes = Tam_Arch['tamano_archivo'].sum()

  ## Totalizador de bytes transportados
  figur7 = go.Figure(go.Indicator(
    mode="number",
    value=Total_bytes,
    title={
      "text": "TOTAL BYTES<br><span style='font-size:0.8em;color:gray'>Cantidad total de</span><br><span style='font-size:0.8em;color:gray'>Bytes Transportados</span>"}))

  ## Total Horas Procesadas
  duracion_total_horas = Canal_C['Duración_Horas'].sum()

  ## Totalizador Horas Procesadas
  figur8 = go.Figure(go.Indicator(
    mode="number",
    value=duracion_total_horas,
    title={
      "text": "TOTAL HORAS<br><span style='font-size:0.8em;color:gray'>Cantidad total de horas</span><br><span style='font-size:0.8em;color:gray'>de Procesamiento</span>"}))

  ## Cálculo Total Registros
  data['Total_reg'] = data['Filas_Proces'] + data['Filas_Desc']
  Total_Reg = data['Total_reg'].sum()

  
  ## Totalizador Registros
  figur9 = go.Figure(go.Indicator(
    mode="number",
    value=Total_Reg,
    title={
      "text": "TOTAL REGISTROS<br><span style='font-size:0.8em;color:gray'>Cantidad total de Registros</span><br><span style='font-size:0.8em;color:gray'>(Procesados-Descartados)</span>"}))


  ## Total Archivos
  ToTal_Arch = data['nombre_archivo'].count()

  ## Totalizador Archivos
  figur10 = go.Figure(go.Indicator(
    mode="number",
    value=ToTal_Arch,
    title={
      "text": "TOTAL ARCHIVOS<br><span style='font-size:0.8em;color:gray'>Cantidad total de Archivos</span><br><span style='font-size:0.8em;color:gray'>(Procesados-Descartados-Error)</span>"}))
    # delta = {'reference': 400, 'relative': True},
    # domain={'x': [0.6, 1], 'y': [0, 1]}))

  ## Filtrar última ejecución (fecha más reciente)
  fec_ult_eje = data['Fecha_Ini_Aux'].max()
  data_ult_eje = data[data['Fecha_Ini_Aux'] == fec_ult_eje]

  ## Cáculos horas de procesamiento por canal (Última Ejecución)
  Canal_C_UltE = data_ult_eje.groupby(['Canal'])['duracion_en_minutos'].sum().to_frame().reset_index()
  Canal_C_UltE['Duración_Horas'] = Canal_C_UltE['duracion_en_minutos'] / 60

  ## Cálculos Total Horas (última ejecución)
  dur_total_hor_UltE = Canal_C_UltE['Duración_Horas'].sum()

  ## Total Archivos (última ejecución)
  ToTal_Arch_UltE = data_ult_eje['nombre_archivo'].count()

  # Cáculos bytes transportados por categoría (última ejecución)
  Tam_Arch_UltE = data_ult_eje.groupby(['Categoría'])['tamano_archivo'].sum().to_frame().reset_index()

  ## Total de bytes transportados (última ejecución)
  Total_bytes_UltE = Tam_Arch_UltE['tamano_archivo'].sum()

  ## Total Registros (última ejecución)
  Total_Registros_UltE = data_ult_eje['cantidad_filas'].sum()

  ## Gráfica Totalizadores Última Ejecución
  fig_UEje = make_subplots(
    rows=1, cols=4,
    specs=[[{"type": "domain"}, {"type": "domain"}, {"type": "domain"}, {"type": "domain"}]])

  fig_UEje.add_trace(go.Indicator(
    mode="number",
    value=dur_total_hor_UltE,
    title={
      "text": "TOTAL HORAS<br><span style='font-size:0.8em;color:gray'>Cantidad total de horas</span><br><span style='font-size:0.8em;color:gray'>de Procesamiento</span>"}),
    row=1, col=1)

  fig_UEje.add_trace(go.Indicator(
    mode="number",
    value=ToTal_Arch_UltE,
    title={
      "text": "TOTAL ARCHIVOS<br><span style='font-size:0.8em;color:gray'>Cantidad total de Archivos</span><br><span style='font-size:0.8em;color:gray'>(Procesados-Descartados-Error)</span>"}),
    row=1, col=2)
  # delta = {'reference': 400, 'relative': True},
  # domain={'x': [0.6, 1], 'y': [0, 1]}))

  fig_UEje.add_trace(go.Indicator(
    mode="number",
    value=Total_bytes_UltE,
    title={
      "text": "TOTAL BYTES<br><span style='font-size:0.8em;color:gray'>Cantidad Total de </span><br><span style='font-size:0.8em;color:gray'>Bytes Transportados</span>"}),
    row=1, col=3)

  fig_UEje.add_trace(go.Indicator(
    mode="number",
    value=Total_Registros_UltE,
    title={
      "text": "TOTAL REGISTROS<br><span style='font-size:0.8em;color:gray'>Cantidad total de Registros</span><br><span style='font-size:0.8em;color:gray'>(Procesados-Descartados)</span>"}),
    row=1, col=4)

  fig_UEje.update_layout(title_text="DURACIÓN DE PROCESAMIENTO (HORAS) POR FASE DE EJECUCIÓN", width=1000, height=500)

  image_filename = r"./reports/logo-oasis-negro.png"
  with open(image_filename, 'rb') as image_file:
    encoded_image = base64.b64encode(image_file.read()).decode("utf-8")
    
  page_5_layout = html.Div([
      html.Nav([
        html.A(children=[html.Img(src='data:image/png;base64,%s' % (encoded_image)  , width= '136px', height='56px')], className='navbar-brand mb-0 h1'),
        html.Div(children=[
          html.Ul(children=[
            html.Li(html.A('Indicadores: IND001', title='Trazabilidad Tiempos de Ejecución', href='/page-1',className="nav-link"), className="nav-item"),
            html.Li(html.A('Indicadores: IND002', title='% Registros procesados vs. descartados', href='/page-2',className="nav-link"), className="nav-item"),
            html.Li(html.A('Indicadores: IND003', title='Trazabilidad de archivos', href='/page-3',className="nav-link"), className="nav-item"),
            #html.Li(html.A('Indicadores: IND004', title='Indicadores Generales', href='/page-4',className="nav-link"), className="nav-item"),
            html.Li(html.A('Indicadores: IND004', title='Última Ejecución', href='/page-4', className="nav-link"),className="nav-item"),
            html.Li(html.A('Cerrar', href='/shutdown', className="nav-link"), className="nav-item")],className="navbar-nav mr-auto")],
          className="collapse navbar-collapse")], className="navbar navbar-expand-lg navbar-light", style={'background-color': '#d8de71'}),
  html.Div(
  [
   html.Div([dcc.Graph(id='Graf6', figure=figur8)], className='col-md-3'),
   html.Div([dcc.Graph(id='Graf7', figure=figur10)], className='col-md-3'),
   html.Div([dcc.Graph(id='Graf8', figure=figur7)], className='col-md-3'),
   html.Div([dcc.Graph(id='Graf9', figure=figur9)], className='col-md-3'),
  ], className='row'),
  

  html.Div([dcc.Graph(id='Graf10', figure=fig_UEje)], className='row'),
  html.Div([
    html.Div([dcc.Graph(id='Graf1', figure=figur1), ], className='col-md-4'),
    html.Div([dcc.Graph(id='Graf2', figure=figur2)], className='col-md-4'),
    html.Div([dcc.Graph(id='Graf3', figure=figur4)], className='col-md-4'),
  ], className='row'),
  
  html.Div([html.Div([dcc.Graph(id='Graf4', figure=figur3)], className='col-md-12')], className='row'),
  
  html.Div(
    children=[dcc.Link('Pagina principal', href='/', className='btn btn-lg btn-primary')], className="row"
  )

  ])


  return page_5_layout