import numpy as np
import pandas as pd
import re
import reporting
from exceptions import XMLExtractException, IVExtractException
from lxml import etree


class EnrichmentOps():
    """Clase encargada de obtener variables adicionales de multiples insumos y aplicar reglas para
    almacenar datos en base de datos"""
    __REGULAR_EXPRESION_SIZE = r'((?:\d+(?:\.\d+)?)(?=\s*(?=G(?!RAN)|KG|LT)))'
    __REGULAR_EXPRESION_KG = r'(\d+(?:\.\d+)?)(?=\s*(?=KG))'
    __REGULAR_EXPRESION_LT = r'(\d+(?:\.\d+)?)(?=\s*(?=LT))'
    __REGULAR_EXPRESION_UXE = r'(?:(?:\s|\w|\()X)(?:\s)?(\d+)(?:(?=\s|]|\)|E|\s\d+))'
    __REGULAR_EXPRESION_RANGOS = r'((?:\d+(?:\.\d+)?))'
    __REGULAR_EXPRESION_RANGE_IN_DESC = r'(\d+(?:\.\d+)?) A (\d+(?:\.\d+)?)'

    def __init__(self):
        pass

    def __first_filter(self, row, cols_cal, cols_original):
        # verificar que el tamaño calculado sea igual al derivado
        row['resultado'] = 0
        row['estado'] = 'sin clasificar'
        match_obj = re.findall(self.__REGULAR_EXPRESION_LT, row['DESC'])
        if len(match_obj) > 0:
            for col in cols_cal:
                if row[col] == -99 or col == cols_cal[-1]:
                    row[col] = float(match_obj[0]) * 950  # Litros a gramos aproximados(de manera muy general)
                    break
        # tiene mayor importancia el peso se deja despues de litros por este motivo
        match_obj = re.findall(self.__REGULAR_EXPRESION_KG, row['DESC'])
        if len(match_obj) > 0:
            for col in cols_cal:
                if row[col] == -99 or col == cols_cal[-1]:
                    row[col] = float(match_obj[0]) * 1000  # kilos a gramos
                    break

        if row['0_tamano'] == row['0_tamano_original']:
            row['resultado'] = row['0_tamano']
            row['estado'] = 'OK'
        elif float(row['0_tamano']) >= float(row['0_tamano_original']) and float(row['0_tamano']) <= float(
                row['1_tamano_original']):
            row['resultado'] = row['0_tamano']
            row['estado'] = 'OK'
        else:
            for col in cols_cal:
                if float(row[col]) == float(row['0_tamano_original']) and row['0_tamano_original'] != -99:
                    row['resultado'] = row[col]
                    row['estado'] = 'Revisar'
                    break
                elif float(row[col]) >= float(row['0_tamano_original']) and float(row[col]) <= float(
                        row['1_tamano_original']):
                    row['resultado'] = row[col]
                    row['estado'] = 'Revisar'
                    break

        if row['resultado'] == -99:
            row['resultado'] = 0
            row['estado'] = 'sin clasificar'
        return row

    def __second_filter(self, row, cols_cal, cols_original):
        match_obj = re.findall(self.__REGULAR_EXPRESION_RANGE_IN_DESC, row['DESC'])
        if row['0_tamano_original'] == -99:
            row['resultado'] = row['0_tamano']
            row['estado'] = 'Asignar tamano descripcion'
        elif row['0_tamano'] == -99:
            row['resultado'] = 0
            row['estado'] = 'No se puede asignar tamano'
        elif "O MAS" in row['TAMANOS'] or "MAS DE" in row['TAMANOS']:
            for col in cols_cal:
                if float(row[col]) >= float(row['0_tamano_original']):
                    row['resultado'] = row[col]
                    row['estado'] = 'rango sin limite superior'
                    break
        elif len(match_obj) == 1:
            tmp_match = match_obj[0][1]
            if len(match_obj[0][1]) == 5 and "." in match_obj[0][1]:
                tmp_match = tmp_match.replace('.', '')
            if float(row['0_tamano_original']) >= float(match_obj[0][0]) and \
                    float(row['0_tamano_original']) <= float(tmp_match):
                row['resultado'] = row['0_tamano_original']
                row['estado'] = 'tamano reportado coincide con el rango de descripcion'

        return row

    def __third_filter(self, row, cols_cal, cols_uxe):
        for colsize in cols_cal:
            found = False
            for col in cols_uxe:
                if row[col] != -99:
                    cal_size = int(float(row[colsize]) / int(row[col]))
                    if abs(cal_size - float(row['0_tamano_original'])) <= 2:
                        row['resultado'] = cal_size
                        row['estado'] = 'tamano asignado dividiendo tamano entre unidades por empaque'
                        found = True
                        break
                    elif cal_size >= float(row['0_tamano_original']) and cal_size <= float(row['1_tamano_original']):
                        row['resultado'] = cal_size
                        row['estado'] = 'tamano asignado porque el tamano calculado se encuentra en el rango reportado'
                        found = True
                        break
            if found:
                break
        # casos de decimales
        for colsize in cols_cal:
            if abs(float(row[colsize]) - float(row['0_tamano_original'])) < 0.9:
                row['resultado'] = row[colsize]
                row['estado'] = 'asignado por ajuste decimal'
                break

        # casos de miles
        for colsize in cols_cal:
            if type(row[colsize]) is str:
                if len(row[colsize]) == 5 and "." in row[colsize]:
                    row[colsize] = row[colsize].replace('.', '')
            if row[colsize] == row['0_tamano_original']:
                row['resultado'] = row[colsize]
                row['estado'] = 'asignando modificando unidad de miles del insumo'
                break
            elif float(row[colsize]) >= float(row['0_tamano_original']) and float(row[colsize]) <= float(
                    row['1_tamano_original']):
                row['resultado'] = row[colsize]
                row['estado'] = 'modificado unidad de miles y se ajusta al rango'
                break

        return row

    def rename_used_sheet_columns(self, sheet_name, df):
        """función que renombra las columnas del Item Volumen de acuerdo a un condición"""
        sheet_name = sheet_name.upper()
        if sheet_name in ['GALLETAS', 'PASTAS', 'CAFE MOLIDO', 'CAFE SOLUBLE', 'MODIFICADORES LECHE',
                          'CHOCOLATE MESA']:
            pass
        elif sheet_name == 'CARNICOS CONSERVA':
            df.rename({'PESO': 'TAMANOS'}, axis='columns', inplace=True)
        elif sheet_name == 'VEGETALES CONSERVA':
            df.rename({'CONTENIDO': 'TAMANOS'}, axis='columns', inplace=True)
        elif sheet_name in ['CHOCOLATINAS', 'MANI']:
            df.rename({'TAMANO': 'TAMANOS'}, axis='columns', inplace=True)
        elif sheet_name in ['CARNES FRIAS']:
            try:
                df.loc[df['TAMANO'].isin(['OTROS']), 'TAMANO'] = df.loc[df['TAMANO'] in ['OTROS'], 'RANGOS']
            except Exception as e:
                pass
            df.rename({'TAMANO': 'TAMANOS'}, axis='columns', inplace=True)
        elif sheet_name in ['CEREALES BARRA']:
            df.rename({'TAMANOS': 'TAMANOS_OR'}, axis='columns', inplace=True)
            df.rename({'PESO TOTAL': 'TAMANOS', 'DESCRIPCION': 'DESC'}, axis='columns', inplace=True)
        elif sheet_name == 'HELADOS':
            try:
                df.loc[df['PESO'].isin(['OTROS']), 'PESO'] = df.loc[df['PESO'] in ['OTROS'], 'TAMANOS']
            except Exception as e:
                pass
            df.rename({'TAMANO': 'TAMANOS'}, axis='columns', inplace=True)
        return df

    def obtain_additional_var_from_item_volume(self, path_to_item_volumen, item_volumen_name):
        df_dict = pd.read_excel(path_to_item_volumen + item_volumen_name, sheet_name=None, header=2)
        full_table = pd.DataFrame()
        reporting.enr_logger.info("abriendo archivo de Item Volumen: %s" % item_volumen_name)
        for name, sheet in df_dict.items():
            sheet.columns = sheet.columns.str.strip()
            sheet['CATEGORIA'] = name.upper()
            sheet = self.rename_used_sheet_columns(name, sheet)
            reporting.enr_logger.info("procesando hoja: %s..." % name)
            # procesar item volumen
            sheet = sheet.loc[:, ['TAG', 'DESC', 'TAMANOS']]
            df = sheet['DESC'].str.extractall(self.__REGULAR_EXPRESION_SIZE)
            df = df.reset_index()
            df = df.pivot(index='level_0', columns='match', values=0)
            df.columns = [str(col) + '_tamano' for col in df.columns]
            df = sheet.join(df)
            # pasar tamano y valores a numerico cruzar con sheet y comparar resultados
            df_size_ori = sheet['TAMANOS'].str.extractall(self.__REGULAR_EXPRESION_RANGOS)
            df_size_ori = df_size_ori.reset_index()
            df_size_ori = df_size_ori.pivot(index='level_0', columns='match', values=0)
            df_size_ori.columns = [str(col) + '_tamano_original' for col in df_size_ori.columns]
            df = df.join(df_size_ori)
            del df_size_ori

            # calculo de unidades por empaque
            df_uxe = sheet['DESC'].str.extractall(self.__REGULAR_EXPRESION_UXE)
            df_uxe = df_uxe.reset_index()
            df_uxe = df_uxe.pivot(index='level_0', columns='match', values=0)
            df_uxe.columns = [str(col) + '_uxe' for col in df_uxe.columns]
            df = df.join(df_uxe)
            del df_uxe
            reporting.enr_logger.info("consolidando hoja %s..." % name)
            df['Hoja'] = name.upper()
            full_table = full_table.append(df)
        full_table.reset_index(inplace=True, drop=True)
        reporting.enr_logger.info(
            "finalizado procesamiento de todas las hojas preparando la asignación de valores definitivos")
        cols_cal_size = [col for col in full_table.columns if col.endswith('_tamano')]
        cols_ori_size = [col for col in full_table.columns if col.endswith('_tamano_original')]
        cols_cal_uxe = [col for col in full_table.columns if col.endswith('_uxe')]
        if '1_tamano_original' not in full_table.columns:
            full_table['1_tamano_original'] = np.nan
        full_table = full_table.fillna(-99)
        try:
            reporting.enr_logger.info("Catalogando usando el primer filtro...")
            full_table = full_table.apply(lambda x: self.__first_filter(x, cols_cal_size, cols_ori_size), axis=1)
            if full_table.loc[full_table['estado'] == 'sin clasificar', :].shape[0] != 0:
                reporting.enr_logger.info("Catalogando usando el segundo filtro...")
                full_table.loc[full_table['estado'] == 'sin clasificar', :] = full_table.loc[
                                                                              full_table['estado'] == 'sin clasificar',
                                                                              :].apply(
                    lambda x: self.__second_filter(x, cols_cal_size, cols_ori_size), axis=1)
            if full_table.loc[full_table['estado'] == 'sin clasificar', :].shape[0] != 0:
                reporting.enr_logger.info("Catalogando usando el tercer filtro...")
                full_table.loc[full_table['estado'] == 'sin clasificar', :] = full_table.loc[
                                                                              full_table['estado'] == 'sin clasificar',
                                                                              :].apply(
                    lambda x: self.__third_filter(x, cols_cal_size, cols_cal_uxe), axis=1)
            reporting.enr_logger.info("Rellenando valores por defecto...")
            full_table[full_table == -99] = np.nan
        except Exception as iv:
            raise IVExtractException(str(iv), item_volumen_name)
        # ya no es necesario escribir un archivo de Excel solo necesario durante la prueba
        # full_table.to_excel('../stage_area/temp_files/temp_input_files/out_size_uxe.xlsx', index=False)
        reporting.enr_logger.info("Ejecución finalizada...")
        return full_table;

    def read_xml_dictionary(self, path_to_xml, xml_name):
        data = path_to_xml + xml_name
        try:
            reporting.enr_logger.info("Parseando documento: %s" % xml_name)
            tree = etree.parse(data)
            req_elems = tree.xpath('//wsp_xml_root/Groups/Groups/Group/RequestElements')
            req_elem = tree.xpath('//wsp_xml_root/Groups/Groups/Group/RequestElements/RequestElement')
            group_elem = tree.xpath('//wsp_xml_root/Groups/Groups/Group')
        except Exception as e:
            raise XMLExtractException("Error parsing xml file", xml_name)
        counter = 0
        child_counter = 0
        df_data = []
        try:
            reporting.enr_logger.info("Generando tabla del documento")
            for elem in req_elems:
                grp_tag, grp_name = group_elem[counter].attrib['Tag'], group_elem[counter].attrib['Name']
                elem_attr = elem.attrib['Count']
                for i in range(0, int(elem_attr)):
                    label = req_elem[child_counter].attrib['Label']
                    value = req_elem[child_counter].attrib['Value']
                    child_counter = child_counter + 1
                    df_data.append([counter, grp_tag, grp_name, elem_attr, label, value, str(counter) + "_" + str(i),
                                    child_counter])
                counter = counter + 1
        except Exception as e:
            raise XMLExtractException(str(e), xml_name)
        reporting.enr_logger.info("Retornar datos parseados de xml")
        df = pd.DataFrame(data=df_data, columns=['Id_Grupo', 'tag_grupo', 'nombre_grupo', 'cantidad_elementos_grupo',
                                                 'label_elemento', 'nombre_elemento', 'id_elemento', 'elemento_numero'])
        # df.to_excel(path_to_xml + 'out_dict_gall.xlsx', index=False)
        return df


class EnrichmentOpsOtherGeo(object):
    """Clase encargada de obtener variables adicionales de multiples insumos y aplicar reglas para
    almacenar datos en base de datos"""
    REGULAR_EXPRESION_SIZE = r'(\d+(?:\.\d+)?)(?=(?=G(?!RAN)| G|g|KG|LT|ML|OZ|LB))'
    REGULAR_EXPRESION_KG = r'(\d+(?:\.\d+)?)(?=\s*(?=KG))'
    REGULAR_EXPRESION_GR = r'(\d+(?:\.\d+)?)(?=\s*(?=GR))'
    REGULAR_EXPRESION_LT = r'(\d+(?:\.\d+)?)(?=\s*(?=LT))'
    REGULAR_EXPRESION_OZ = r'(\d+(?:\.\d+)?)(?=\s*(?=OZ|Oz| Oz|oz| ONZ))'
    REGULAR_EXPRESION_LB = r'(\d+(?:\.\d+)?)(?=\s*(?=LB))'
    REGULAR_EXPRESION_ML = r'(\d+(?:\.\d+)?)(?=\s*(?=ML))'
    REGULAR_EXPRESION_UXE = r'(\d+(?:\.\d+)?)(?: UND|UND|X| X|U| U|PZ| PZ)'
    REGULAR_EXPRESION_RANGOS = r'(\d+(?:\.\d+)?)(?=(?=G(?!RAN)))'
    REGULAR_EXPRESION_RANGE_IN_DESC = r'(\d+(?:\.\d+)?) A (\d+(?:\.\d+)?)'

    def __init__(self, df):
        """Recibe descripciones y codigos de tag del archivo de insumo de acuerdo a este"""
        self.df = df
        self.sheet = self.df.loc[:, ['TAG', 'PRODUCTO', 'TAMANO', 'DIST_POND_TIENDAS_VENDIENDO_MAX']]
        self.set_match_from_expr()

    def set_match_from_expr(self) -> None:
        reg_dict = {
            'REGULAR_EXPRESION_GR': (self.REGULAR_EXPRESION_GR, 1),
            'REGULAR_EXPRESION_': (self.REGULAR_EXPRESION_ML, 0.5),
            'REGULAR_EXPRESION_LT': (self.REGULAR_EXPRESION_LT, 600),  # conversion rate
            'REGULAR_EXPRESION_MLKG': (self.REGULAR_EXPRESION_KG, 1000),
            'REGULAR_EXPRESION_OZ': (self.REGULAR_EXPRESION_OZ, 28.35),
            'REGULAR_EXPRESION_LB': (self.REGULAR_EXPRESION_LB, 453.6),
            'REGULAR_EXPRESION_RANGOS': self.REGULAR_EXPRESION_RANGOS,
            'REGULAR_EXPRESION_SIZE': self.REGULAR_EXPRESION_SIZE,
            'REGULAR_EXPRESION_UXE': self.REGULAR_EXPRESION_UXE
        }
        tmp = self.sheet

        reporting.enr_logger.info(f"Calculando variable peso desde la descripción")
        filter_cols = []
        for name, regexp in reg_dict.items():
            suffix = name.split('_')[2]
            reporting.enr_logger.debug(f"Obteniendo variables adicionales de {suffix}")
            if suffix == 'SIZE':
                tmp = tmp['PRODUCTO'].str.extractall(regexp)
                tmp.reset_index(inplace=True)
                if tmp.empty:
                    tmp = self.sheet
                    reporting.enr_logger.debug(f"No se encontraron resultados en {suffix}")
                    continue
                tmp = tmp.pivot(index='level_0', columns='match', values=0)
                tmp.columns = [str(col) + '_tamano' for col in tmp.columns]

                tmp = tmp.fillna(0)
                tmp = tmp[:].astype('float64')
                tmp['max'] = 0.0

                tmp['max'] = tmp.max(axis=1, numeric_only=True)

                if '2_tamano' in tmp.columns:
                    tmp.loc[tmp['2_tamano'] != 0, 'max'] = 0

                tmp['0_tamano'] = tmp['max']
                tmp['0_tamano'] = tmp['0_tamano'].astype('float64')

                [filter_cols.append(col) for col in tmp.columns]


            elif suffix == 'RANGOS':
                tmp = tmp['TAMANO'].str.extractall(regexp)
                tmp.reset_index(inplace=True)
                if tmp.empty:
                    reporting.enr_logger.debug(f"No se encontraron resultados en {suffix}")
                    tmp = self.sheet
                    continue
                tmp = tmp.pivot(index='level_0', columns='match', values=0)
                tmp.columns = [str(col) + '_tamano_original' for col in tmp.columns]
                [filter_cols.append(col) for col in tmp.columns]

            elif suffix == 'UXE':
                tmp = tmp['PRODUCTO'].str.extractall(regexp)
                tmp.reset_index(inplace=True)
                if tmp.empty:
                    reporting.enr_logger.debug(f"No se encontraron resultados en {suffix}")
                    tmp = self.sheet
                    continue
                tmp = tmp.pivot(index='level_0', columns='match', values=0)
                tmp.columns = [str(col) + '_uxe' for col in tmp.columns]

                tmp = tmp.fillna(-1)
                tmp = tmp[:].astype('float64')

                tmp['UXE'] = 1.0

                tmp.loc[tmp['0_uxe'] > 1, 'UXE'] = tmp['0_uxe']

                if 'DIST_POND_TIENDAS_VENDIENDO_MAX' in tmp.columns:
                    tmp.loc[tmp['DIST_POND_TIENDAS_VENDIENDO_MAX'] > 1, 'UXE'] = tmp['DIST_POND_TIENDAS_VENDIENDO_MAX']

                if '1_uxe' in tmp.columns:
                    tmp.loc[tmp['1_uxe'] > -1, 'UXE'] = 1

                tmp = tmp.fillna(1)
                tmp['UXE'] = tmp['UXE'].astype('int64')
                tmp['0_uxe'] = tmp['UXE']

                [filter_cols.append(col) for col in tmp.columns]

            else:
                tmp = tmp['PRODUCTO'].str.extractall(regexp[0])
                tmp.reset_index(inplace=True)
                if tmp.empty:
                    reporting.enr_logger.debug(f"No se encontraron resultados en {suffix}")
                    tmp = self.sheet
                    continue
                tmp = tmp.pivot(index='level_0', columns='match', values=0)
                tmp.columns = [f"{str(col)}_{suffix.lower()}" for col in tmp.columns]

            self.sheet = self.sheet.join(tmp)
            tmp = self.sheet

            if suffix not in ['SIZE', 'RANGOS', 'UXE']:
                col = '0_' + suffix.lower()
                tmp[col] = pd.to_numeric(tmp[col], downcast='float', errors='coerce')
                tmp[col] = tmp[col].fillna(0)

                tmp[suffix + "_CALCULADO"] = tmp[col] * regexp[1]  # conversion rate
                filter_cols.append(suffix + "_CALCULADO")

        for name, regexp in reg_dict.items():
            suffix = name.split('_')[2]
            if suffix not in ['SIZE', 'RANGOS', 'UXE', 'GR'] and suffix + "_CALCULADO" in tmp.columns:
                tmp.loc[tmp[suffix + "_CALCULADO"] > 0, '0_tamano'] = tmp[suffix + "_CALCULADO"]

        cols = ['TAG', 'PRODUCTO', 'TAMANO']
        cols.extend(filter_cols)
        reporting.enr_logger.info(f"Reescribiendo dataframe de clase")

        self.sheet = tmp.loc[:, cols]

    def obtain_size_and_uxe(self) -> pd.DataFrame:
        reporting.enr_logger.info(f"eliminando tags duplicados")
        tmp = self.sheet.drop_duplicates(subset=['TAG']).copy()

        tmp = tmp.loc[:, ['TAG', '0_tamano', '0_uxe']]  # peso y unidades por empaque
        tmp = self.df.merge(tmp, how='left', on='TAG', validate='many_to_one')
        reporting.enr_logger.info(f"asignando valores de tamano y unidades por empaque")

        # TAMANO
        tmp.loc[tmp['TAMANO'] == 'OTROS', 'TAMANO'] = 0
        tmp['TAMANO'] = pd.to_numeric(tmp['TAMANO'], errors='coerce')
        tmp['TAMANO'] = tmp['TAMANO'].astype('float64')
        tmp['0_tamano'] = tmp['0_tamano'].astype('float64')

        tmp.loc[tmp['TAMANO'] <= 1, 'TAMANO'] = tmp['0_tamano']

        tmp['TAMANO'] = tmp['TAMANO'].fillna(0)

        # UXE
        tmp['DIST_POND_TIENDAS_VENDIENDO_MAX'] = tmp['0_uxe']
        tmp['DIST_POND_TIENDAS_VENDIENDO_MAX'] = tmp['DIST_POND_TIENDAS_VENDIENDO_MAX'].fillna(1)
        tmp.loc[tmp['DIST_POND_TIENDAS_VENDIENDO_MAX'] <= 0, 'DIST_POND_TIENDAS_VENDIENDO_MAX'] = 1

        tmp.drop(labels=['0_tamano', '0_uxe'], axis=1, inplace=True)

        # check whether the input dataframe is equal to te output dataframe
        if tmp.shape != self.df.shape:
            # raise error
            reporting.enr_logger.error(f"El dataframe de salida {tmp.shape}, no cumple condiciones {self.df.shape}")
            # TODO raise error

        tmp = tmp.loc[:, self.df.columns].copy()
        return tmp

    def obtain_size_and_uxe_font_4(self) -> pd.DataFrame:
        reporting.enr_logger.info(f"eliminando tags duplicados")
        tmp = self.sheet.drop_duplicates(subset=['TAG']).copy()

        tmp = tmp.loc[:, ['TAG', '0_tamano', '0_uxe']]  # peso y unidades por empaque
        tmp = self.df.merge(tmp, how='left', on='TAG', validate='many_to_one')

        reporting.enr_logger.info(f"asignando valoresde tamano y unidades por empaque")

        # TAMANO
        tmp.loc[tmp['TAMANO'] == 'S/A', 'TAMANO'] = tmp.loc[tmp['TAMANO'] == 'S/A', '0_tamano']
        tmp['TAMANO'] = tmp['TAMANO'].fillna(0)
        tmp.loc[tmp['TAMANO'] == 'OTROS', 'TAMANO'] = 0

        # UXE
        tmp['DIST_POND_TIENDAS_VENDIENDO_MAX'] = tmp['0_uxe']
        tmp['DIST_POND_TIENDAS_VENDIENDO_MAX'] = tmp['DIST_POND_TIENDAS_VENDIENDO_MAX'].fillna(1)
        tmp.loc[tmp['DIST_POND_TIENDAS_VENDIENDO_MAX'] <= 0, 'DIST_POND_TIENDAS_VENDIENDO_MAX'] = 1
        tmp.drop(labels=['0_tamano', '0_uxe'], axis=1, inplace=True)

        # check whether the input dataframe is equal to te output dataframe
        if tmp.shape != self.df.shape:
            # raise error
            reporting.enr_logger.error(f"El dataframe de salida {tmp.shape}, no cumple condiciones {self.df.shape}")
            # TODO raise error

        tmp = tmp.loc[:, self.df.columns].copy()
        return tmp
