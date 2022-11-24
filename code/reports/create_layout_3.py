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
import base64
from dash.dependencies import Input, Output
import pdb

def create_layout_ind0003(data2,data1, start_date, end_date):

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

  data2.loc[:,'Total_Archivos'] = data2['archivos_ftp'] + data2['archivos_locales']
  data2.loc[:,'Archivos_procesados'] = data2['Total_Archivos'] - (
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
      return

  new = data["nombre_archivo"].str.split('_', n=5, expand=True)
  data["Fuente"] = new[0]
  data["Región"] = new[1]
  data["Canal"] = new[2]
  data["Categoría"] = new[3]
  data["Fecha"] = new[4]

  Ar_proc = data2.groupby('Fecha_Ini_Aux')['Archivos_procesados'].sum().to_frame().reset_index()
  Ar_error = data2.groupby('Fecha_Ini_Aux')['cantidad_archivos_error'].sum().to_frame().reset_index()
  Ar_dect = data2.groupby('Fecha_Ini_Aux')['archivos_descartados'].sum().to_frame().reset_index()
  join_Ar = pd.merge(Ar_proc, Ar_error, on='Fecha_Ini_Aux')
  join_Ar2 = pd.merge(join_Ar, Ar_dect, on='Fecha_Ini_Aux')

  #creacion de variables grafica 1
  Total_Arch = data2['Total_Archivos'].sum()
  Arch_Error = data2['cantidad_archivos_error'].sum()
  Arch_Descar = data2['archivos_descartados'].sum()
  Arch_Proces = Total_Arch - (Arch_Error + Arch_Descar)

  valuesA = [Arch_Proces, Arch_Error, Arch_Descar]
  lablesA = ['Archivos Procesados', 'Archivos con Error', 'Archivos Descartados']
  plot2 = make_subplots(rows=1, cols=2,specs=[[{"type": "domain"}, {"type": "xy"}]], subplot_titles=("Distribución porcentual<br>de archivos según su estado",
    "Cantidad de archivos ejecutados<br>por fecha según el estado"))

  plot2.add_trace(go.Pie(labels=lablesA, values=valuesA, text= [round(val,2)  for val in valuesA], textposition='inside', hole=.7, marker_colors=
    ['rgb(211,238,128)', 'rgb(12,192,170)', 'rgb(77,194,84)']), row=1, col=1)

  plot2.add_trace(go.Bar(x=join_Ar2["Fecha_Ini_Aux"], y=join_Ar2["Archivos_procesados"],name='Archivos Procesados', offsetgroup=0,
    marker_color='rgb(211,238,128)',showlegend=False), row=1, col=2)

  plot2.add_trace(go.Bar(x=join_Ar2["Fecha_Ini_Aux"],y=join_Ar2['cantidad_archivos_error'],name='Archivos con error', offsetgroup=0,
    marker_color='rgb(12,192,170)', showlegend=False), row=1, col=2)

  plot2.add_trace(go.Bar(x=join_Ar2["Fecha_Ini_Aux"], y=join_Ar2["archivos_descartados"],name='archivos Descartados', offsetgroup=0, marker_color='rgb(77,194,84)',
    showlegend=False), row=1, col=2)

  plot2.update_yaxes(title_text="Horas", row=1, col=1)

  plot2.update_layout(template="seaborn", height=500,  xaxis_tickangle=45)


  #creacion datos figura2

  Arc_FTP = data2.groupby('Fecha_Ini_Aux')['archivos_ftp'].sum().to_frame().reset_index()
  Arc_Loc = data2.groupby('Fecha_Ini_Aux')['archivos_locales'].sum().to_frame().reset_index()
  join1_df = pd.merge(Arc_FTP, Arc_Loc, on='Fecha_Ini_Aux')

  Total_local = data2['archivos_locales'].sum()
  Total_FTP = data2['archivos_ftp'].sum()

  labelsFL = ['Archivos Locales', 'Archivos FTP']
  valuesFL = [Total_local, Total_FTP]

  Modo_Arch = data2.loc[:,:].groupby(['modo_ejecucion'])['archivos_locales', 'archivos_ftp'].sum().reset_index().copy()

  # Cálculo Archivos por Canal
  Canal_A = data.groupby(['Canal'])['nombre_archivo'].count().to_frame().reset_index()
  Canal_A.loc[Canal_A['Canal'] == '1', 'Canal'] = 'Retail'
  Canal_A.loc[Canal_A['Canal'] == '2', 'Canal'] = 'Scantrack'

  # Total Arhivos Canal Retail
  A_R = Canal_A.loc[Canal_A['Canal']=='Retail','nombre_archivo']
  if A_R.shape[0] == 0:
    A_R = 0
  else:
    A_R = A_R.iloc[0]

  # Total Arhivos Canal Scantrack
  A_S =  Canal_A.loc[Canal_A['Canal']=='Scantrack','nombre_archivo']
  if A_S.shape[0] == 0:
    A_S = 0
  else:
    A_S = A_S.iloc[0]
  ## Total de bytes transportados
  Total_Bytes = data['tamano_archivo'].sum()
  Total_Bytes = int(Total_Bytes)

  ## Gráficos Indicadores

  fig_ToT_A = make_subplots(
    rows=1, cols=4,
    specs=[[{"type": "domain"}, {"type": "domain"}, {"type": "domain"}, {"type": "domain"}]])
  # Gráfico Totalizados de Archivos
  fig_ToT_A.add_trace(go.Indicator(
    mode="number",
    value=Total_Arch,
    title={
      "text": "TOTAL<br>ARCHIVOS<br><span style='font-size:0.8em;color:gray'>Cantidad Total de Archivos</span><br><span style='font-size:0.8em;color:gray'>Analizados</span>"}),
    #delta = {'reference': Arch_Tot, 'relative': True, 'increasing': {'color': "green"}, 'decreasing' :{'color': "red"}}),
    row=1, col=1)

  fig_ToT_A.add_trace(go.Indicator(
    mode="number",
    value=Total_Bytes,
    title={
      "text": "TOTAL<br>BYTES<br><span style='font-size:0.8em;color:gray'>Cantidad Total de Bytes</span><br><span style='font-size:0.8em;color:gray'>Transportados</span>"}),
    #delta = {'reference': Bytes_Tot, 'relative': True, 'increasing': {'color': "green"}, 'decreasing' :{'color': "red"}}),
    row=1, col=2)

  fig_ToT_A.add_trace(go.Indicator(
    mode="number",
    value=A_R,
    title={
      "text": "TOTAL<br>ARCHIVOS RETAIL<br><span style='font-size:0.8em;color:gray'>Cantidad Total de Archivos</span><br><span style='font-size:0.8em;color:gray'>Analizados</span>"}),
    #delta = {'reference': Arc_R_T, 'relative': True, 'increasing': {'color': "red"}, 'decreasing' :{'color': "green"}}),
    row=1, col=3)

  fig_ToT_A.add_trace(go.Indicator(
    mode="number",
    value=A_S,
    title={
      "text": "TOTAL<br>ARCHIVOS SCANTRACK<br><span style='font-size:0.8em;color:gray'>Cantidad Total de Archivos</span><br><span style='font-size:0.8em;color:gray'>Analizados</span>"}),
    #delta = {'reference': Arc_S_T, 'relative': True,'increasing': {'color': "red"}, 'decreasing' :{'color': "green"}}),
    row=1, col=4)

  ## Filtro Canales Retail = 1 y Scantrack = 2
  Retail = data[data['Canal'] == '1']
  Scantrack = data[data['Canal'] == '2']

  # Cáculos bytes transportados por categoría y canal
  Tam_Arch_Retail = Retail.groupby(['Categoría'])['tamano_archivo'].sum().to_frame().reset_index()

  ## Gráfica bytes transportados por categoría y Canal Retail
  figur3 = px.bar(Tam_Arch_Retail, y='Categoría', x='tamano_archivo', orientation='h',title='Cantidad de bytes transportados <br>Canal Retail')

  # Cáculos bytes transportados por categoría y canal
  Tam_Arch_Scantrack = Scantrack.groupby(['Categoría'])['tamano_archivo'].sum().to_frame().reset_index()

  ## Gráfica bytes transportados por categoría y Canal Retail
  figur4 = px.bar(Tam_Arch_Scantrack, y='Categoría', x='tamano_archivo', orientation='h', title='Cantidad de bytes transportados<br>Canal Scantrack')

  ## cantidad de Archivos según el modo de ejecución
  Arc_Modo = data2.groupby(['modo_ejecucion'])['Total_Archivos'].sum().to_frame().reset_index()
  labels_M = Arc_Modo['modo_ejecucion'].to_list()
  values_M = Arc_Modo['Total_Archivos'].to_list()

  figur_M = go.Figure(data=[go.Pie(labels=labels_M, values=values_M,  hole=.7,text= [round(val,2)  for val in values_M], textposition='inside',marker_colors=['rgb(0,68,27)', 'rgb(255,102,102)'])])

  ## No es necesario realizar las gráficas de archivos FTP y Locales
  #fig2 = make_subplots(
    #rows=2, cols=2,
    #specs=[[{"rowspan": 2}, {"type": "domain"}],
           #[None, {}, ]],
    #subplot_titles=("Cantidad de archivos locales<br>y FTP por fecha",
                    #"Distribución porcentual<br>de archivos locales y FTP",
                    #"Número de archivos<br>locales y FTP según el modo de ejecución"))

  #fig2.add_trace(go.Bar( x=join1_df["Fecha_Ini_Aux"], y=join1_df["archivos_ftp"],
    #name='archivos_ftp',
    #marker_color='rgb(41,24,107)',
    #offsetgroup=0,
    #showlegend=False),
    #row=1, col=1)

  #fig2.add_trace(go.Bar(
    #x=join1_df["Fecha_Ini_Aux"],
    #y=join1_df['archivos_locales'],
    #name='archivos_locales',
    #marker_color='rgb(5,255,255)',
    #offsetgroup=1,
    #showlegend=False),
    #row=1, col=1)

  #fig2.add_trace(go.Bar(
    #y=Modo_Arch['modo_ejecucion'],
    #x=Modo_Arch["archivos_ftp"],
    #name='archivos_ftp',
    #marker_color='rgb(41,24,107)',
    #orientation='h',
    #offsetgroup=0,
    #showlegend=False,
  #), row=2, col=2)

  #fig2.add_trace(go.Bar(
    #y=Modo_Arch['modo_ejecucion'],
    #x=Modo_Arch["archivos_locales"],
    #name='archivos_locales',
    #marker_color='rgb(5,255,255)',
    #orientation='h',
    #offsetgroup=1,
    #showlegend=False,
    #base=Modo_Arch['modo_ejecucion']
  #), row=2, col=2)

  #fig2.add_trace(
    #go.Pie(labels=labelsFL, values=valuesFL, hole=.7, text= [round(val,2)  for val in valuesFL], textposition='inside',marker_colors=['rgb(5,255,255)', 'rgb(41,24,107)']), row=1,col=2)

  #fig2.update_layout(template="seaborn", xaxis_tickangle=45,
                       #height=600)


  image_filename = r"./reports/logo-oasis-negro.png"
  with open(image_filename, 'rb') as image_file:
    encoded_image = base64.b64encode(image_file.read()).decode("utf-8")
    
  page_3_layout = html.Div([
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
    html.Div(children=[
      html.Div(children=[
        html.H1('Familia Indicadores: IND003', className='display-3',style={'display':'inline'}),
        html.Br(),
        html.H1('TRAZABILIDAD ARCHIVOS', className='display-4', style={'font-size':'2.5em'}),
        html.P('Indicadores para el periodo: %s / %s' % (start_date, end_date), className='lead')
        ]
      ,className='col')], className='row'),

    html.Div(children=[
        html.Div(
          dcc.Graph(id='Gráfico_10', figure=fig_ToT_A), className='col'
        )
      ], className='row'),

    html.Hr(),
    html.H2('ARCHIVOS SEGÚN SU ESTADO',  className='display-4' ,style={'font-size':'1.5em'}),
    html.Br(),
    html.Div(children=[
      html.Div(children=[dcc.Graph(id='Gráfico_8', figure=plot2)], className='col')
    ], className='row'),

    #html.Hr(),
    #html.H2('ARCHIVOS LOCALES VS ARCHIVOS FTP',  className='display-4' ,style={'font-size':'1.5em'}),
    #html.Br(),
    #html.Div([
      #html.Div(children=[dcc.Graph(id='Gráfico_9', figure=fig2)], className='col')
    #], className='row'),

    html.Hr(),
    html.H2('CANTIDAD DE BYTES TRANSPORTADOS POR CATEGORÍA Y CANAL',  className='display-4' ,style={'font-size':'1.5em'}),
    html.Br(),
    html.Div([
    html.Div(children=[dcc.Graph(id='Gráfico_12', figure=figur3)], className='col-md-6'),
    html.Div(children=[dcc.Graph(id='Gráfico_13', figure=figur4)], className='col-md-6')
    ], className='row'),

    html.Hr(),
    html.H2('CANTIDAD DE ARCHIVOS SEGÚN EL MODO DE EJECUCIÓN',  className='display-4' ,style={'font-size':'1.5em'}),
    html.Br(),
    html.Div([
      html.Div(children=[dcc.Graph(id='Gráfico_11', figure=figur_M)], className='col')
    ], className='row'),

    html.Div(
        children=[dcc.Link('Go back to home', href='/', className='btn btn-lg btn-primary')], className="row"
    )

  ])

  return page_3_layout