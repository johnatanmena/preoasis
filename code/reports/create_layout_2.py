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


def create_layout_ind0002(data1, start_date, end_date):
    
  ## Definición Valores primera ejecución
  # Total Particiones
  Partic_Total = 337
  # Total Registros
  Regi_Total = 3370000
  # Total Registros Retail
  Regi_Total_R = 1220000
  # Total Archivos Scantrack
  Regi_Total_S = 2150000


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

  #crear datos de gráfica 1
  new = data["nombre_archivo"].str.split('_', n=5, expand=True)
  data["Fuente"] = new[0]
  data["Región"] = new[1]
  data["Canal"] = new[2]
  data["Categoría"] = new[3]
  data["Fecha"] = new[4]

  PartP_Modo = data.groupby(['modo_ejecucion'])['cantidad_part_procesada'].sum().to_frame().reset_index()
  PartD_Modo = data.groupby(['modo_ejecucion'])['cantidad_part_descartadas'].sum().to_frame().reset_index()
  Part_Modo = pd.merge(PartP_Modo, PartD_Modo, on='modo_ejecucion')

  Cat_Proc = data.groupby(['Categoría'])['cantidad_part_procesada'].sum().to_frame().reset_index()
  Cat_Desc = data.groupby(['Categoría'])['cantidad_part_descartadas'].sum().to_frame().reset_index()
  Cat_Part = pd.merge(Cat_Proc, Cat_Desc, on='Categoría')

  Particiones_Procesadas = data['cantidad_part_procesada'].sum()
  Particiones_Descartadas = data['cantidad_part_descartadas'].sum()

  label_part = ['Particiones Procesadas', 'Particiones Descartadas']
  values_part = [Particiones_Procesadas, Particiones_Descartadas]

  #crear grfica 1
  fig1 = make_subplots(
      rows=2, cols=2,
      specs=[[{"rowspan": 2}, {"type": "domain"}],
             [None, {}, ]],
      subplot_titles=("# de particiones procesadas y descartadas<br>según la categoría",
                      "Cantidad y porcentaje<br>de particiones procesadas y descartadas",
                      "Número de particiones<br>según el modo de ejecución"))

  fig1.add_trace(go.Pie(labels=label_part,
                        values=values_part, hole=.7, text= [round(val,2)  for val in values_part], textposition='inside',
                        marker_colors=['rgb(0,68,27)', 'rgb(255,102,102)']), row=1, col=2)

  fig1.add_trace(go.Bar( y=Cat_Part['Categoría'], x=Cat_Part['cantidad_part_procesada'], name='Particiones Procesadas',    orientation='h',  showlegend=False,
    marker=dict(color='rgb(0,68,27)')), row=1, col=1)
  fig1.add_trace(go.Bar( y=Cat_Part['Categoría'], x=Cat_Part['cantidad_part_descartadas'], name='Particiones Descartadas', orientation='h',  showlegend=False,
    marker=dict(color='rgb(255,102,102)')),row=1, col=1)
  fig1.add_trace(go.Bar( name="Particiones Procesadas", x=Part_Modo["modo_ejecucion"], y=Part_Modo["cantidad_part_procesada"], marker_color='rgb(0,68,27)', showlegend=False,
    offsetgroup=0),row=2, col=2)
  fig1.add_trace(go.Bar( name="Particiones Descartadas", x=Part_Modo["modo_ejecucion"],y=Part_Modo["cantidad_part_descartadas"],marker_color='rgb(255,102,102)', showlegend=False,
    offsetgroup=1),row=2, col=2)
  fig1.update_layout(template="seaborn", barmode='stack',
                     height=600, xaxis_tickangle=45,
                     font=dict(family="Calibri", size=18, color="#7a4b4b"))


  #creacion de datos segunda grafica

  ## Gráfica cantidad de particiones por Canal

  ## Filtro Canal Retail y Scantrack
  Retail = data[data['Canal'] == '1']
  Scantrack = data[data['Canal'] == '2']
  tam_Ret = len(Retail)

  if tam_Ret != 0:
      # Cáculo cantidad de particiones por canal Retail
      Cat_Proc1 = Retail.groupby('Categoría')['cantidad_part_procesada'].sum().reset_index()
      Cat_Proc1['Tipo Partición'] = 'Particiones Procesadas'
      Cat_Proc1['Cantidad Particiones'] = Cat_Proc1['cantidad_part_procesada']
      Cat_Desc1 = Retail.groupby('Categoría')['cantidad_part_descartadas'].sum().reset_index()
      Cat_Desc1['Tipo Partición'] = 'Particiones Descartadas'
      Cat_Desc1['Cantidad Particiones'] = Cat_Desc1['cantidad_part_descartadas']
      join_DP_R = pd.concat([Cat_Proc1, Cat_Desc1])

      # Gráfico cantidad de particiones por canal Retail
      fig_6 = px.bar(join_DP_R, y="Tipo Partición", x="Cantidad Particiones", orientation='h', color='Categoría',title = 'Cantidad de particiones<br> Retail',color_discrete_sequence=px.colors.sequential.Viridis)
  else:
        print("No hay datos que cumplan esta condición")

  tam_Scan = len(Scantrack)

  if tam_Scan != 0:
      # Cáculo cantidad de particiones por canal Scantrack
      Cat_Proc2 = Scantrack.groupby('Categoría')['cantidad_part_procesada'].sum().reset_index()
      Cat_Proc2['Tipo Partición'] = 'Particiones Procesadas'
      Cat_Proc2['Cantidad Particiones'] = Cat_Proc2['cantidad_part_procesada']
      Cat_Desc2 = Scantrack.groupby('Categoría')['cantidad_part_descartadas'].sum().reset_index()
      Cat_Desc2['Tipo Partición'] = 'Particiones Descartadas'
      Cat_Desc2['Cantidad Particiones'] = Cat_Desc2['cantidad_part_descartadas']
      join_DP_S = pd.concat([Cat_Proc2, Cat_Desc2])

      # Gráfico cantidad de particiones por canal Scantrack
      fig_7 = px.bar(join_DP_S, y="Tipo Partición", x="Cantidad Particiones", orientation='h', color='Categoría', title = 'Cantidad de particiones<br> Scantrack',color_discrete_sequence=px.colors.sequential.Viridis)

  else:
      print("No hay datos que cumplan esta condición")

  #datos tercera grafica
  data['Filas_Proces'] = data['tamano_particion'] * data['cantidad_part_procesada']
  data['Filas_Desc'] = data['tamano_particion'] * data['cantidad_part_descartadas']

  Fil_Proc = data.groupby('Fecha_Ini_Aux')['Filas_Proces'].sum().to_frame().reset_index()
  Fil_No_Proc = data.groupby('Fecha_Ini_Aux')['Filas_Desc'].sum().to_frame().reset_index()
  join_Fil = pd.merge(Fil_Proc, Fil_No_Proc, on='Fecha_Ini_Aux')
  Archivos_Fecha = data.groupby('Fecha_Ini_Aux')['nombre_archivo'].count().to_frame().reset_index()


  #creacion tercera grafica
  plot_name = make_subplots(specs=[[{"secondary_y": True}]])

  plot_name.add_trace(go.Bar(x=join_Fil["Fecha_Ini_Aux"],y=join_Fil["Filas_Proces"], name='Registros Procesados', offsetgroup=1, marker_color='rgb(77,194,84)'))

  plot_name.add_trace(go.Bar(x=join_Fil["Fecha_Ini_Aux"],y=join_Fil["Filas_Desc"],name='Registros Descartados', offsetgroup=1,base=join_Fil["Filas_Proces"],marker_color='#d90d39'))

  plot_name.add_trace(go.Scatter(x=Archivos_Fecha["Fecha_Ini_Aux"],y=Archivos_Fecha["nombre_archivo"], name='Cantidad de Archivos', marker_color='rgb(12,192,170)'),
    secondary_y=True)

  plot_name.update_layout(yaxis_title="Cantidad de Registros",
    height=500,showlegend=True,template="seaborn")
  plot_name.update_yaxes(title_text="Cantidad de Archivos", secondary_y=True)

  ## Cálculo Total Particiones
  Part_Total = Particiones_Procesadas + Particiones_Descartadas

  ## Cálculo Total Registros
  data['Total_reg'] = data['Filas_Proces'] + data['Filas_Desc']
  Total_Reg = data['Total_reg'].sum()

  ## Cálculo Total Registros Canal Retail

  Retail = data[data['Canal'] == '1']
  Scantrack = data[data['Canal'] == '2']

  Reg_Retail = Retail['Total_reg'].sum()
  Reg_Scantrack = Scantrack['Total_reg'].sum()

  ## Gráfico Totalizadores
  fig_ToT_Reg = make_subplots(
      rows=1, cols=4,
      specs=[[{"type": "domain"}, {"type": "domain"}, {"type": "domain"}, {"type": "domain"}]])

  # Gráfico Totalizados de Particiones
  fig_ToT_Reg.add_trace(go.Indicator(
      mode="number",
      value=Part_Total,
      title={
          "text": "TOTAL<br>PARTICIONES<br><span style='font-size:0.8em;color:gray'>Total Particiones</span><br><span style='font-size:0.8em;color:gray'>Analizadas</span>"}),
      #delta = {'reference': Partic_Total, 'relative': True, 'increasing': {'color': "green"}, 'decreasing' :{'color': "red"}}),
      row=1, col=1)

  # Gráfico Totalizador Registros
  fig_ToT_Reg.add_trace(go.Indicator(
      mode="number",
      value=Total_Reg,
      title={
          "text": "TOTAL<br>REGISTROS<br><span style='font-size:0.8em;color:gray'>Total Registros</span><br><span style='font-size:0.8em;color:gray'>Analizados</span>"}),
      #delta = {'reference': Regi_Total, 'relative': True, 'increasing': {'color': "green"}, 'decreasing' :{'color': "red"}}),
      row=1, col=2)

  # Gráfico Totalizador registros Retail
  fig_ToT_Reg.add_trace(go.Indicator(
      mode="number",
      value=Reg_Retail,
      title={
          "text": "TOTAL<br>REGISTROS RETAIL<br><span style='font-size:0.8em;color:gray'>Total Registros</span><br><span style='font-size:0.8em;color:gray'>Canal Retail</span>"}),
      #delta = {'reference': Regi_Total_R, 'relative': True, 'increasing': {'color': "green"}, 'decreasing' :{'color': "red"}}),
      row=1, col=3)

  # Totalizador registros Scantrack
  fig_ToT_Reg.add_trace(go.Indicator(
      mode="number",
      value=Reg_Scantrack,
      title={
          "text": "TOTAL<br>REGISTROS SCANTRACK<br><span style='font-size:0.8em;color:gray'>Total Registros</span><br><span style='font-size:0.8em;color:gray'>Canal Scantrack</span>"}),
      #delta = {'reference': Regi_Total_S, 'relative': True, 'increasing': {'color': "green"}, 'decreasing' :{'color': "red"}}),
      row=1, col=4)

  image_filename = r"./reports/logo-oasis-negro.png"
  with open(image_filename, 'rb') as image_file:
    encoded_image = base64.b64encode(image_file.read()).decode("utf-8")

  classname = 'col-md-12'
  if tam_Ret != 0 and tam_Scan != 0:
      classname = 'col-md-6'

  if tam_Ret != 0:
      div_R = html.Div([dcc.Graph(id='figure6', figure=fig_6)], className=classname)
  else:
      div_R = html.Div(hidden=True)

  if tam_Scan != 0:
      div_S = html.Div([dcc.Graph(id='figure6', figure=fig_7)], className=classname)
  else:
      div_S = html.Div(hidden=True)
  
  page_2_layout = html.Div([

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
          html.H1('Familia Indicadores: IND0002', className='display-3',style={'display':'inline'}),
          html.Br(),
          html.H1('REGISTROS PROCESADOS VS DESCARTADOS', className='display-4',style={'font-size':'2.5em'}),
          html.P('Indicadores para el periodo: %s / %s' % (start_date, end_date), className='lead')
        ], className='col')
      ], className="row"),

      html.Div(children=[
        html.Div(
          dcc.Graph(id='Gráfico_4', figure=fig_ToT_Reg), className='col'
        )
      ], className='row'),

    html.Hr(),
    html.H2('PARTICIONES PROCESADAS VS DESCARTADAS', className='display-4' ,style={'font-size':'1.5em'}),
    html.Br(),

      html.Div(children=[
        html.Div(
            dcc.Graph(id='Gráfico_8', figure=fig1), className='col'
        )
    ], className='row'),
      html.Hr(),
      html.H2('CANTIDAD DE PARTICIONES POR CATEGORÍA Y FUENTE', className='display-4',
              style={'font-size': '1.5em'}),
      html.Div(children=[
          div_R,
          div_S
      ], className="row"),

      html.Hr(),
      html.H2('RELACIÓN ENTRE LA CANTIDAD DE REGISTROS PROCESADOS Y DESCARTADOS CON EL NÚMERO DE ARCHIVOS', className='display-5'),
      html.Br(),
      html.Div([
          html.Div([
              dcc.Graph(id='Gráfico_5', figure=plot_name)
          ], className='col')
      ], className="row"),



      html.Div(
        children=[dcc.Link('Go back to home', href='/', className='btn btn-lg btn-primary')], className="row"
      )

  ])


  return page_2_layout