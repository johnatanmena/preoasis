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

def create_layout_ind0001(data1, start_date, end_date):

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

    if data.shape[0]== 0:
        return None

    data['tiempo_extraccion_Min'] = data['tiempo_extraccion'] / 60
    data['tiempo_transformacion_Min'] = data['tiempo_transformacion'] / 60
    data['tiempo_carga_Min'] = data['tiempo_carga'] / 60
    
     
    #calculo de variables
    Min_Ext = data['tiempo_extraccion_Min'].sum()
    Min_Trns = data['tiempo_transformacion_Min'].sum()
    Min_Carga = data['tiempo_carga_Min'].sum()
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
    Ext_Fecha = data.groupby('Fecha_Ini_Aux')['tiempo_extraccion_Min'].sum().to_frame().reset_index()
    Trns_Fecha = data.groupby('Fecha_Ini_Aux')['tiempo_transformacion_Min'].sum().to_frame().reset_index()
    Carga_Fecha = data.groupby('Fecha_Ini_Aux')['tiempo_carga_Min'].sum().to_frame().reset_index()
    join3 = pd.merge(Ext_Fecha, Trns_Fecha, on='Fecha_Ini_Aux')
    join4 = pd.merge(join3, Carga_Fecha, on='Fecha_Ini_Aux')

    # Add traces
    Figura1 = make_subplots(rows=1, cols=2,specs=[[{"type": "domain"}, {"type": "xy"}]],
      subplot_titles=("% de Duración (horas) por Fase de Ejecución","Duración (horas) por Fase de Ejecución y Fecha"))

    Figura1.add_trace(go.Pie(labels=list_of_names_1, values=list_of_values_2, text= [round(val,2)  for val in list_of_values_2],
                             marker_colors=['rgb(5,255,255)', 'rgb(41,24,107)', 'rgb(117,127,221)']), row=1, col=1)

    Figura1.add_trace(go.Bar(x=join4['Fecha_Ini_Aux'], y=join4['tiempo_extraccion_Min'],
                                 #mode='lines',
                                 name='Extracción',
                                 showlegend=False,
                                 marker_color='rgb(5,255,255)'), row=1, col=2)
    Figura1.add_trace(go.Bar(x=join4['Fecha_Ini_Aux'], y=join4['tiempo_transformacion_Min'],
                                 #mode='lines',
                                 name='Transformación',
                                 showlegend=False,
                                 marker_color='rgb(41,24,107)'), row=1, col=2)
    Figura1.add_trace(go.Bar(x=join4['Fecha_Ini_Aux'], y=join4['tiempo_carga_Min'],
                                 #mode='lines',
                                 name='Carga',
                                 showlegend=False,
                                 marker_color='rgb(117,127,221)'), row=1, col=2)

    Figura1.update_yaxes(title_text="Horas", row=1, col=2)
    Figura1.update_xaxes(title_text="Fecha", row=1, col=2)

    Figura1.update_layout(template="seaborn",
                          height=500, xaxis_tickangle=45,barmode='stack',
                          font=dict(family="Calibri",
                                    size=18,
                                    color="#7a4b4b"))

    Min_Extraccion = data.groupby(['modo_ejecucion'])['tiempo_extraccion_Min'].sum().to_frame().reset_index()
    Min_Transcion = data.groupby(['modo_ejecucion'])['tiempo_transformacion_Min'].sum().to_frame().reset_index()
    Min_Carga = data.groupby(['modo_ejecucion'])['tiempo_carga_Min'].sum().to_frame().reset_index()

    join1 = pd.merge(Min_Extraccion, Min_Transcion, on='modo_ejecucion')
    join2 = pd.merge(join1, Min_Carga, on='modo_ejecucion')

    list_of_names = join2['modo_ejecucion'].to_list()
    list_of_values = join2['tiempo_extraccion_Min'].to_list()

    list_of_names_1 = join2['modo_ejecucion'].to_list()
    list_of_values_1 = join2['tiempo_transformacion_Min'].to_list()

    list_of_names_2 = join2['modo_ejecucion'].to_list()
    list_of_values_2 = join2['tiempo_carga_Min'].to_list()

    Min_Ext_Modo = data.groupby(['Fecha_Ini_Aux', 'modo_ejecucion'])[
      'tiempo_extraccion_Min'].sum().to_frame().reset_index()
    Min_Trns_Modo = data.groupby(['Fecha_Ini_Aux', 'modo_ejecucion'])[
      'tiempo_transformacion_Min'].sum().to_frame().reset_index()
    Min_Carga_Modo = data.groupby(['Fecha_Ini_Aux', 'modo_ejecucion'])[
      'tiempo_carga_Min'].sum().to_frame().reset_index()  

    Normal_Carg = Min_Carga_Modo[Min_Carga_Modo['modo_ejecucion'] == 'Normal']
    Rep_Carg = Min_Carga_Modo[Min_Carga_Modo['modo_ejecucion'] == 'Reproceso']
    Rev_Carg = Min_Carga_Modo[Min_Carga_Modo['modo_ejecucion'] == 'Revisión']

    Normal_Ext = Min_Ext_Modo[Min_Ext_Modo['modo_ejecucion'] == 'Normal']
    Rep_Ext = Min_Ext_Modo[Min_Ext_Modo['modo_ejecucion'] == 'Reproceso']
    Rev_Ext = Min_Ext_Modo[Min_Ext_Modo['modo_ejecucion'] == 'Revisión']
     
    Normal_Trns = Min_Trns_Modo[Min_Trns_Modo['modo_ejecucion'] == 'Normal']
    Rep_Trns = Min_Trns_Modo[Min_Trns_Modo['modo_ejecucion'] == 'Reproceso']
    Rev_Trns = Min_Trns_Modo[Min_Trns_Modo['modo_ejecucion'] == 'Revisión']

    # crear grafico2 figura 1
    figura = make_subplots(
      rows=2, cols=3,
      specs=[[{"type": "domain"}, {"type": "domain"}, {"type": "domain"}],
             [{"type": "xy"}, {"type": "xy"}, {"type": "xy"}]],
      subplot_titles=("Extracción",
                      "Transformación",
                      "Carga"),
      vertical_spacing=0.1)

    figura.add_trace(go.Pie(labels=list_of_names, values=list_of_values, hole=.7, text= [round(val,2)  for val in list_of_values], textposition='inside',
                            marker_colors=['rgb(211,238,128)', 'rgb(12,192,170)', 'rgb(77,194,84)']), row=1, col=1)
    figura.add_trace(go.Pie(labels=list_of_names_1, values=list_of_values_1, hole=.7, text= [round(val,2)  for val in list_of_values_1], textposition='inside',
                            marker_colors=['rgb(211,238,128)', 'rgb(12,192,170)', 'rgb(77,194,84)']), row=1, col=2)
    figura.add_trace(go.Pie(labels=list_of_names_2, values=list_of_values_2, hole=.7, text= [round(val,2)  for val in list_of_values_2], textposition='inside',
                            marker_colors=['rgb(211,238,128)', 'rgb(12,192,170)', 'rgb(77,194,84)']), row=1, col=3)

    figura.add_trace(go.Bar(x=Normal_Carg["Fecha_Ini_Aux"], y=Normal_Carg["tiempo_carga_Min"], name='Normal', marker_color='rgb(211,238,128)',
      showlegend=False, offsetgroup=0), row=2, col=1)
    figura.add_trace(go.Bar(x=Normal_Carg["Fecha_Ini_Aux"], y=Rep_Carg['tiempo_carga_Min'], name='Reproceso', marker_color='rgb(12,192,170)',
      showlegend=False, offsetgroup=0), row=2, col=1)
    figura.add_trace(go.Bar(x=Normal_Carg["Fecha_Ini_Aux"], y=Rev_Carg["tiempo_carga_Min"], name='Revisión', marker_color='rgb(77,194,84)', 
      showlegend=False, offsetgroup=0), row=2, col=1)

    figura.add_trace(go.Bar( x=Normal_Ext["Fecha_Ini_Aux"], y=Normal_Ext["tiempo_extraccion_Min"], name='Normal',marker_color='rgb(211,238,128)',
      showlegend=False, offsetgroup=0 ), row=2, col=2)
    figura.add_trace(go.Bar( x=Normal_Ext["Fecha_Ini_Aux"], y=Rep_Ext['tiempo_extraccion_Min'], name='Reproceso',marker_color='rgb(12,192,170)',
      showlegend=False, offsetgroup=0), row=2, col=2)
    figura.add_trace(go.Bar(x=Normal_Ext["Fecha_Ini_Aux"], y=Rev_Ext["tiempo_extraccion_Min"], name='Revisión', marker_color='rgb(77,194,84)',
      showlegend=False, offsetgroup=0), row=2, col=2)


    figura.add_trace(go.Bar(x=Normal_Trns["Fecha_Ini_Aux"],y=Normal_Trns["tiempo_transformacion_Min"],name='Normal',marker_color='rgb(211,238,128)',
      showlegend=False, offsetgroup=0), row=2, col=3)
    figura.add_trace(go.Bar(x=Normal_Trns["Fecha_Ini_Aux"],y=Rep_Trns['tiempo_transformacion_Min'],name='Reproceso',marker_color='rgb(12,192,170)',
      showlegend=False, offsetgroup=0), row=2, col=3)
    figura.add_trace(go.Bar(x=Normal_Trns["Fecha_Ini_Aux"],y=Rev_Trns["tiempo_transformacion_Min"],name='Revisión', marker_color='rgb(77,194,84)',
      showlegend=False, offsetgroup=0), row=2, col=3)

    figura.update_yaxes(title_text="Horas", row=2, col=1)

    figura.update_layout(template="seaborn",
                         height=740,
                         xaxis_tickangle=45,
                         font=dict(family="Calibri",
                                   size=18,
                                   color="#7a4b4b"))

    #calculo de variables grafica 2
    new = data["nombre_archivo"].str.split('_', n=5, expand=True)
    data["Fuente"] = new[2]
    data["Categoría"] = new[3]
    data["Canal"] = new[2]
     
    ## Filtro Canales Retail = 1 y Scantrack = 2
    Retail = data[data['Canal'] == '1']
    Scantrack = data[data['Canal'] == '2']

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
                                        showarrow=False, align='right'))

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
                                     title_text='RETAIL: TIEMPO DE PROCESAMIENTO POR FASE DE EJECUCIÓN',
                                     height=600, yaxis_tickangle=-50,
                                     font=dict(family="Calibri",
                                               size=12,
                                               color="#7a4b4b"))
        Cat_Retail = join2_Retail['Categoría'].nunique()
    else:
        Cat_Retail = 0
        print("No hay datos que cumplan esta condición 1 Ret")

    tam_Scan = len(Scantrack)

    if tam_Scan != 0:

        ## Cálculos Canal Scantrack
        Min_Extraccion_Scantrack = Scantrack.groupby(['Categoría'])['tiempo_extraccion_Min'].sum().to_frame().reset_index()
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
        #fixed length 99
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
                                        title_text='SCANTRACK: TIEMPO DE PROCESAMIENTO POR FASE DE EJECUCIÓN',
                                        height=600, yaxis_tickangle=-50,
                                        font=dict(family="Calibri",
                                                  size=12,
                                                  color="#7a4b4b"))
        Cat_Scantrack = join2_Scantrack['Categoría'].nunique()
    else:
        Cat_Scantrack = 0
        print("No hay datos que cumplan esta condición 1 Sca")

    #calculo de variables
    Min_Cat_Ext1 = data.groupby(['tiempo_extraccion_Min', 'Categoría']).sum().reset_index()
    Min_Cat_Ext1['Fase'] = 'Extracción'
    Min_Cat_Ext1['Cantidad Horas'] = Min_Cat_Ext1['tiempo_extraccion_Min']

    Min_Cat_Transcion1 = data.groupby(['tiempo_transformacion_Min', 'Categoría']).sum().reset_index()
    Min_Cat_Transcion1['Fase'] = 'Transformación'
    Min_Cat_Transcion1['Cantidad Horas'] = Min_Cat_Transcion1['tiempo_transformacion_Min']

    Min_Cat_Carga1 = data.groupby(['tiempo_carga_Min', 'Categoría']).sum().reset_index()
    Min_Cat_Carga1['Fase'] = 'Carga'
    Min_Cat_Carga1['Cantidad Horas'] = Min_Cat_Carga1['tiempo_carga_Min']

    join = pd.concat([Min_Cat_Ext1, Min_Cat_Transcion1, Min_Cat_Carga1])

    fig5 = px.bar(join, x="Fase", y='Cantidad Horas', color='Categoría',
      color_discrete_sequence=px.colors.sequential.Viridis)

    ## Filtrar última ejecución (fecha más reciente)
    fec_ult_eje = data['Fecha_Ini_Aux'].max()
    data_ult_eje = data[data['Fecha_Ini_Aux'] == fec_ult_eje]

    ## Cálculo Total Horas
    total_h = float(round(Min_Fase['Duración_Minutos'].sum(), 2))

    # Cáculos por canal
    Canal_C = data.groupby(['Canal'])['duracion_en_minutos'].sum().to_frame().reset_index()
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
        mode="number",
        value=total_h,
        title={
            "text": "<span style='font-size:0.9em;color:green'>TOTAL<br>HORAS<br><span style='font-size:0.8em;color:gray'>Total de horas</span><br><span style='font-size:0.8em;color:gray'>procesadas</span>"}),
        #delta = {'reference': Horas_ToT, 'relative': True, 'increasing': {'color': "red"}, 'decreasing' :{'color': "green"} }),
        row=1, col=1)

    # Gráfico Totalizador categorías canal Retail
    fig_ToT.add_trace(go.Indicator(
        mode="number",
        value=Cat_Retail,
        title={
            "text": "<span style='font-size:0.9em;color:green'>CATEGORÍAS<br>RETAIL<br><span style='font-size:0.8em;color:gray'>Total de Categorías</span><br><span style='font-size:0.8em;color:gray'>Canal Retail</span>"}),
        #delta = {'reference': Cat_Ret, 'relative': True, 'increasing': {'color': "yellow"}, 'decreasing' :{'color': "yellow"}}),
        row=1, col=2)

    # Gráfico Totalizador categorías canal Scantrack
    fig_ToT.add_trace(go.Indicator(
        mode="number",
        value=Cat_Scantrack,
        title={
            "text": "<span style='font-size:0.9em;color:green'>CATEGORÍAS<br>SCANTRACK<br><span style='font-size:0.8em;color:gray'>Total de Categoría</span><br><span style='font-size:0.8em;color:gray'>Canal Scantrack</span>"}),
        #delta = {'reference': Cat_Sca, 'relative': True, 'increasing': {'color': "yellow"}, 'decreasing' :{'color': "yellow"}}),
        row=1, col=3)

    # Totalizador Horas Canal Retail
    fig_ToT.add_trace(go.Indicator(
        mode="number",
        value=T_R,
        title={
            "text": "<span style='font-size:0.9em;color:green'>TOTAL<br>HORAS RETAIL<br><span style='font-size:0.8em;color:gray'>Total de horas</span><br><span style='font-size:0.8em;color:gray'>procesadas</span>"}),
        #delta = {'reference': Hor_Ret, 'relative': True, 'increasing': {'color': "red"}, 'decreasing' :{'color': "green"}}),
        row=1, col=4)

    # Totalizador Horas Canal Scantrank
    fig_ToT.add_trace(go.Indicator(
        mode="number",
        value=T_S,
        title={
            "text": "<span style='font-size:0.9em;color:green'>TOTAL<br>HORAS SCANTRACK<br><span style='font-size:0.8em;color:gray'>Total de horas</span><br><span style='font-size:0.8em;color:gray'>procesadas</span>"}),
        #delta = {'reference': Hor_Sca, 'relative': True, 'increasing': {'color': "red"}, 'decreasing' :{'color': "green"}}),
        row=1, col=5)
    
    image_filename = r"./reports/logo-oasis-negro.png"
    with open(image_filename, 'rb') as image_file:
      encoded_image = base64.b64encode(image_file.read()).decode("utf-8")

    classname = 'col-md-12'
    if tam_Ret != 0 and tam_Scan != 0:
        classname = 'col-md-6'

    if tam_Ret != 0:
        div_R = html.Div([dcc.Graph(id='figure6', figure=grafica_Retail)], className=classname)
    else:
        div_R = html.Div(hidden=True)

    if tam_Scan != 0:
        div_S = html.Div([dcc.Graph(id='figure6', figure=grafica_Scantrack)], className=classname)
    else:
        div_S = html.Div(hidden=True)
    
    page_1_layout = html.Div([
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
            html.H1('''Familia Indicadores: IND001 ''',className='display-3', style={'display':'inline'}),
            html.Br(),
            html.H1('TRAZABILIDAD TIEMPOS - FASES DE EJECUCIÓN', className='display-4',style={'font-size':'2.5em'}),
            html.P('Indicadores para el periodo: %s / %s' % (start_date, end_date), className='lead')
          ], className='col')
      ], className="row"),
      html.Br(),
      html.Div( children=[
          html.Div([dcc.Graph(id='figure2', figure=fig_ToT)]),
        html.H2('DURACIÓN DE PROCESAMIENTO (HORAS) POR FASE DE EJECUCIÓN',  className='display-4' ,style={'font-size':'1.5em'}),
        html.Div([dcc.Graph(id='figure3', figure=Figura1)]),
        ]
      ),

      html.Hr(),
      html.H2('DURACIÓN DE PROCESAMIENTO (HORAS) POR FASE Y MODO DE EJECUCIÓN',  className='display-4' ,style={'font-size':'1.5em'}),
      html.Div(
        html.Div([ dcc.Graph(id='Gráfico_2', figure=figura)], className="col"),
      className="row"),
      html.Br(),
      html.Hr(),
      html.Br(),
      html.H2('TIEMPO DE PROCESAMIENTO POR FUENTE (Retail y Scantrack)', className='display-4' ,style={'font-size':'1.5em'}),
      html.Div(children=[
          div_R,
          div_S
      ], className="row"),
      html.Br(),
      html.Hr(),
      html.Br(),

      html.H2('DURACIÓN DE PROCESAMIENTO POR FASE DE EJECUCION',  className='display-4' ,style={'font-size':'1.5em'}),

      html.Div(
        children=[html.Div([dcc.Graph(id='Gráfico_4', figure=fig5)], className='col')], className="row"),
      html.Div(
        children=[dcc.Link('Go back to home', href='/', className='btn btn-lg btn-primary')], className="row"
      )
    ])


    return page_1_layout