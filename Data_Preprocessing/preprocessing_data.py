import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from config import dfs_config as config
from config.dfs_config import create_data_model
from config.colon_config import colon_configuration_apply
import sys
sys.path.append('C:/Users/pablo/Desktop/INVERBIS/2024/Ribera_Salud/Exploratory_Data_Analysis')
fecha_actual = datetime.now()
fecha_actual_str = fecha_actual.strftime("%Y-%m-%d %H:%M:%S")
version = 'V0_1'
version_out = 'V1_5'
TIME_BEFORE_EXTRACTION = 6*30*24*3600     #! MONTH TO SECONDS
generate = False
preprocess = True


def charge_data(): 
    path_gen = "/home/pol/Escritorio/INVERBIS/2024/CancerColon/"
    path_1 =  "Data/raw_data/VDR.xlsx"
    path_2 =  "Data/raw_data/Copia de Términos agrupados.xlsx"
    path_3 =  "Data/raw_data/VDR_DatosLaboratorio_Seudonimizados_v.01.00.xlsx"
    path_4 =  "Data/raw_data/VDR_DatosNoEstructurados_Seudonimizados.xlsx"

    colon = sheet_reader(pd.read_excel(path_1, sheet_name=None))
    termAgrupados = sheet_reader(pd.read_excel(path_2, sheet_name=None))
    laboratorio = sheet_reader(pd.read_excel(path_3, sheet_name=None))
    colon_noEstructurados = sheet_reader(pd.read_excel(path_4, sheet_name=None))

    #CREO DICC
    df_names = {'colon': colon, 'termAgrupados': termAgrupados, 'laboratorio': laboratorio, 'colon_noEstructurados': colon_noEstructurados}
    #Información dataframes
    # for df_name, df in zip(df_names.keys() , df_names.values()):
    #     print(df_info(df_name, df, atrib_name = 'NASI seudonimizado', only_basics= True))
    return df_names


def sheet_reader(df):
    df_sheet = {}
    for name, df in df.items():
        df_sheet[name] = df
    return df_sheet

def concat_df(dataframes):
    # Concatena los dataframes a lo largo de las filas (axis=0)
    df_unido = pd.concat(dataframes, ignore_index=True)
    return df_unido

def merge_df(df1, df2):
     #Le añado la tabla de mantenimiento de protocolo mediante merge
    df_unido = pd.merge(left=df1, right=df2, left_on='PREOPCodPRotocolo', right_on='IdProtocolo', how='left')
    return df_unido



def self_loops_fix(df):
    #Ordenar el DataFrame por 'ID' y 'FECHA'
    df = df.sort_values(by=['NASI seudonimizado', 'FECHA'])
    #Eliminar las filas duplicadas excepto la primera aparición
    df = df.drop_duplicates(subset=['NASI seudonimizado','Actividad'], keep='first')
    # Asegúrate de resetear el índice si es necesario
    df = df.reset_index(drop=True)
    return df

def self_loops(df):
    #Convertir la columna 'FECHA' a formato datetime
    df['FECHA'] = pd.to_datetime(df['FECHA'], format= 'mixed')
    #Ordenar el DataFrame por 'ID' y 'FECHA'
    df = df.sort_values(by=['NASI seudonimizado', 'FECHA'])
    #Eliminar las filas duplicadas excepto la primera aparición
    df = df.drop_duplicates(subset=['NASI seudonimizado','Actividad', 'FECHA'], keep='first')
    # Asegúrate de resetear el índice si es necesario
    df = df.reset_index(drop=True)
    return df


def delete_nan(df):
    # Eliminar filas con valores nulos o en blanco en 'mi_columna'
    df = df.dropna(subset=['NASI seudonimizado', 'Actividad','FECHA','FECHA_FIN'])
    return df


def anonymize_atributes(df, attri, generic_name = 'Value'):
    #! crear diccionario sustitucion
    dict_attri = {}
    values = df[attri].dropna().unique().tolist()
    
    for index, value in enumerate(values):
        dict_attri[value] = f'{generic_name}_{index+1}'
    print(dict_attri)
    #! Sustituir en df con diccionario
    df[attri] = df[attri].replace(dict_attri)
    return df


def no_colonoscopy(df):
    filtro_sin = ~df['Actividade CAPNOR'].isin(['COLONOSCOPIA', 'COLONOSCOPIA VIA RAPIDA', 'COLONOSCOPIA CON BIOPSIA'])
    filtro_con = df['Actividade CAPNOR'].isin(['COLONOSCOPIA', 'COLONOSCOPIA VIA RAPIDA', 'COLONOSCOPIA CON BIOPSIA'])
    # print(df.shape)
    # print(df['NASI seudonimizado'].nunique())
    colon_colonoscopia = df[filtro_con]
    # print(colon_colonoscopia.shape)
    # print(colon_colonoscopia['NASI seudonimizado'].nunique())
    lista_validos = colon_colonoscopia['NASI seudonimizado'].unique().tolist()

    colon_sin_colonoscopia = df[filtro_sin]
    # print(colon_sin_colonoscopia.shape)
    # print(colon_sin_colonoscopia['NASI seudonimizado'].nunique())
    return colon[colon['NASI seudonimizado'].isin(lista_validos)]



def fix_dates(df, start_year=None, end_year=None):
    df['FECHA'] = pd.to_datetime(df['FECHA'], format = 'mixed')
    df['FECHA_FIN'] = pd.to_datetime(df['FECHA_FIN'], format= 'mixed')
    if start_year != None:
        df['FECHA'] = df['FECHA'].apply(lambda x: x.strftime('%d-%m-%Y %H:%M:%S'))
    if end_year != None:
        df['FECHA_FIN'] = df['FECHA_FIN'].apply(lambda x: x.strftime('%d-%m-%Y %H:%M:%S'))
    df['FECHA'] = pd.to_datetime(df['FECHA'], format = '%d-%m-%Y %H:%M:%S')
    df['FECHA_FIN'] = pd.to_datetime(df['FECHA_FIN'], format= '%d-%m-%Y %H:%M:%S')
    if start_year != None and end_year != None:
        df = df.query(f" FECHA >= '01-01-{start_year} 00:00:00' & FECHA < '01-01-{end_year} 00:00:00'")
    
    df = df.sort_values(by = 'FECHA')
    return df

def filter_dataApa(trace, value):
    trace['FECHA'] = pd.to_datetime(trace['FECHA'], format = '%d-%m-%Y %H:%M:%S')
    # Fecha límite para el filtrado, también como datetime
    limit_date = pd.to_datetime(value, format= '%d-%m-%Y %H:%M:%S')

    limit_date_minus_10s = limit_date - pd.Timedelta(seconds=TIME_BEFORE_EXTRACTION)
    trace_filtered = trace[trace['FECHA'] >= limit_date_minus_10s] 
    return trace_filtered


def crear_dead_bolean(trace):
    trace['Defuncion'] = 'Defuncion'
    return trace


def modify_dead_date(trace):
    max_date = trace['FECHA'].max()
    new_date = max_date + timedelta(days=30)
    new_date = pd.to_datetime(new_date, format= '%d-%m-%Y %H:%M:%S')
    # Cambiar las fechas asociadas a 'Defunción'
    trace.loc[trace['Actividad'] == 'Defunción', ['FECHA', 'FECHA_FIN']] = new_date
    return trace

def filter_extraction(df):

    # df['FECHA'] = pd.to_datetime(df['FECHA'], format= '%d-%m-%Y %H:%M:%S')
    # df['FECHA_FIN'] = pd.to_datetime(df['FECHA_FIN'], format= '%d-%m-%Y %H:%M:%S')
    df['FECHA'] = pd.to_datetime(df['FECHA'], format='%Y-%m-%d %H:%M:%S')
    df['FECHA_FIN'] = pd.to_datetime(df['FECHA_FIN'], format='%Y-%m-%d %H:%M:%S')
    grouper = df.groupby('NASI seudonimizado')

    dates, traces = [], []
    dates_dict = {}
    for name, trace in grouper:
        trace['Defuncion'] = 'Sin Defuncion'
        if 'Colonoscopia diagnóstica' in trace['Actividad'].unique().tolist():
            df = trace.query(" Actividad == 'Colonoscopia diagnóstica'")
            value = df['FECHA'].values[0]
            dates.append(value)
            dates_dict[name] = value
            
            #!boleano defunciones
            if 'Defunción' in trace['Actividad'].unique().tolist():
                trace = crear_dead_bolean(trace)
                trace = modify_dead_date(trace)
                
            traces.append(filter_dataApa(trace, value))

    return pd.concat(traces)


def boolean_important_drugs(df):
    important_drugs = ['LAXANTES OSMÓTICOS','LAXANTES FORMADORES DE VOLUMEN', 'ENEMAS']
    # Crear una nueva columna booleana y establecerla en False por defecto
    df['Medicamento importante'] = 'NO Administración de Farmacos importantes (Enemas y Laxantes)'
    # Marcar como True si alguno de los medicamentos importantes está presente para cada ID único
    for group_id, group_data in df.groupby('NASI seudonimizado'):
        if any(group_data['Subg. químico terapéutico ATC disp'].isin(important_drugs)):
            df.loc[group_data.index, 'Medicamento importante'] = 'Administración de Farmacos importantes (Enemas y Laxantes)'
    return df


def filter_important_drugs(df):
    important_drugs = ['VITAMINA D Y ANÁLOGOS', 'MULTIVITAMÍNICOS CON MINERALES','VITAMINA B12 (CIANOCOBALAMINA Y ANÁLOGOS)','VITAMINA B1 SOLA', 'VITAMINA K', 'COMBINACIONES DE VITAMINAS']
    datas = []
    for group_id, group_data in df.groupby('NASI seudonimizado'):
        if group_data['Subg. químico terapéutico ATC disp'].isin(important_drugs).any():
            # Eliminar las filas donde la actividad sea 'Dispensación medicamento ATC'
            group_data = group_data[group_data['Actividad'] != 'Dispensación medicamento ATC']
        datas.append(group_data)
    df_fix = pd.concat(datas)

    return df_fix



def filter_patients_with_colonoscy(df):
    important = ['COLONOSCOPIA', 'COLONOSCOPIA VIA RAPIDA']
    datas = []
    for group_id, group_data in df.groupby('NASI seudonimizado'):
        if ~group_data['Subg. químico terapéutico ATC disp'].isin(important).any():
            group_data = group_data[group_data['Actividad'] != 'Dispensación medicamento ATC']
        datas.append(group_data)
    df_fix = pd.concat(datas)

    return df_fix



def drugs_info(df):
    # #! Función que conteara el numero de veces que va a buscar medicamentos, y cuanto le cuesta en total estos medicamentos
    grouper_2 = colon_filtered.groupby('NASI seudonimizado')

    colon_filtered['Nº de compras farmaceuticas'] = 0
    colon_filtered['Coste total medicamentos'] = 0
    traces = []
    for name, trace in grouper_2: # Prezo PVP nomen disp
        cont = 0 
        try:
            cont = trace['Actividad'].value_counts()['Dispensación medicamento ATC']
            trace['Nº de compras farmaceuticas'] = cont
            trace['Coste total medicamentos'] = trace['Prezo PVP nomen disp'].sum()
        except KeyError:
            trace['Nº de compras farmaceuticas'] = 0
            trace['Coste total medicamentos'] = 0
        traces.append(trace)
    
    return pd.concat(traces)


def tc_abdominal(df):
    return df[df['Actividade CAPNOR'].str.contains('TC') & df['Actividade CAPNOR'].str.contains('ABDOM')]['Actividade CAPNOR'].unique().tolist()


def surname_Appointments(df, attrib_name):
    traces = []
    ids_colonoscopy = []
    df['VDR'] = 'Sin Via rapida'
    df['Colonoscopia'] = 'Evento NO presente en la traza'
    df['Consultas Digestivo/Medicina interna'] = 'Evento NO presente en la traza'
    df['Consultas Cirugía General/Oncología'] = 'Evento NO presente en la traza'
    df['Realización TC abdominales'] = 'Evento NO presente en la traza'
    lista_colono = ['COLONOSCOPIA', 'COLONOSCOPIA VIA RAPIDA']
    lista_TCAbdominal = tc_abdominal(colon)
    grouper_3 = df.groupby('NASI seudonimizado')

    for name, trace in grouper_3:
        for index in range(len(trace['Actividad'])):

            if trace['Actividad'].values[index] == attrib_name:
                if trace['Actividade CAPNOR'].values[index] in lista_colono: 
                    trace['Actividad'].values[index] = trace['Actividade CAPNOR'].values[index]
                    trace['Colonoscopia'] = 'Evento presente en la traza'
                    if trace['Tipo vía rápida'].values[index] == "VIA RAPIDA COLON              ":
                        trace['VDR'] = 'Con Via rapida'

                elif trace['Actividade CAPNOR'].values[index] == 'CONSULTA DE ENFERMIDADE':
                    if trace['GNA'].values[index] == 'DIXESTIVO':
                        trace['Actividad'].values[index] = 'Cita en Digestivo'
                        trace['Consultas Digestivo/Medicina interna'] = 'Evento presente en la traza'

                    elif trace['GNA'].values[index] == 'MEDICINA INTERNA':
                        trace['Actividad'].values[index] = 'Cita en Medicina interna'
                        trace['Consultas Digestivo/Medicina interna'] = 'Evento presente en la traza'
                
                        
                    elif trace['GNA'].values[index] == 'ONCOLOXIA MEDICA':
                        trace['Actividad'].values[index] = 'Cita en Oncología'
                        trace['Consultas Cirugía General/Oncología'] = 'Evento presente en la traza'

                    elif trace['GNA'].values[index] == 'CIRURXIA XERAL E DIXESTIVA':
                        trace['Actividad'].values[index] = 'Cita en Cirugía General'
                        trace['Consultas Cirugía General/Oncología'] = 'Evento presente en la traza'


                elif trace['Actividade CAPNOR'].values[index] in lista_TCAbdominal: 
                    trace['Actividad'].values[index] = 'Cita TAC Abdominal'
                    trace['Realización TC abdominales'] = 'Evento presente en la traza'
           
                            
        traces.append(trace)
    traces_fix = pd.concat(traces)
    #eliminar actividad cita normal
    return traces_fix.drop(traces_fix.loc[traces_fix['Actividad'] == attrib_name].index)


def waiting_list_duration(df, enter_list, exit_list):
     # Asegurarse de que 'FECHA' esté en formato datetime y ordenar
    df['FECHA'] = pd.to_datetime(df['FECHA'], format= '%d-%m-%Y %H:%M:%S')
    df['FECHA_FIN'] = pd.to_datetime(df['FECHA_FIN'], format= '%d-%m-%Y %H:%M:%S')
    df = df.sort_values(by='FECHA')
    # Agrupar por 'CodPaciente'
    grouped = df.groupby('NASI seudonimizado')
    # Crear una columna con valores por defecto False
    df['Duracion Lista Espera'] = 0
    traces = []
    for name, group in grouped:
        # Verificar si ambas actividades están presentes
        if enter_list in group['Actividad'].values and exit_list in group['Actividad'].values:
            # Obtener las fechas de las actividades
            fecha_admin_medicamentos = group[group['Actividad'] == enter_list]['FECHA'].iloc[0]
            fecha_cirugia = group[group['Actividad'] == exit_list]['FECHA'].iloc[0]
            
            # Calcular la diferencia de tiempo en días
            diff_hours = abs((fecha_cirugia - fecha_admin_medicamentos).total_seconds()) / 3600 / 24
            group['Duracion Lista Espera'] = diff_hours
        traces.append(group)
            
    return pd.concat(traces)


def conservate_activity(df, conservate_list):
    # Filtrar el DataFrame para conservar solo las filas con las actividades especificadas
    df = df[df['Actividad'].isin(conservate_list)]
    return df


def df_info(df):
    print(df['NASI seudonimizado'].nunique())
    print(df['Actividad'].unique())
    print(df.columns)


#! filtrar medico familia atencion primario importantes
def filter_doctor_visit(df):
    # Lista de valores a verificar en el campo 'Acto'
    lista = ['DEMANDA', 'PROGRAMADA', 'TRATAMENTO SUCESIVO','ALTA DE TRATAMENTO']
    # Filtramos el DataFrame para eliminar las filas que cumplan las condiciones dadas
    return df[~((df['Actividad'] == 'Visita médico familia') & (~df['Acto'].isin(lista)))]


#! filtrar cirugias importantes
def filter_surgery_typology(df):
    # Lista de valores a verificar en el campo 'Acto'
    lista = ['CIRURXIA XERAL E DIXESTIVA']
    # Filtramos el DataFrame para eliminar las filas que cumplan las condiciones dadas
    return df[~((df['Actividad'] == 'Cirugia') & (~df['GNA'].isin(lista)))]

def create_initial_activity(df):
    lista = ['Visita médico familia','Cita en Medicina interna', 'Cita en Oncología', 'Cita en Cirugía General', 'Cita en Digestivo']
    # Agrupar por el identificador de traza
    groupon = df.groupby('NASI seudonimizado')

    traces = []
    # Iterar sobre cada grupo
    for name, trace in groupon:

        # Crear una máscara booleana para identificar las filas donde 'Actividad' es 'biopsia'
        mask_biopsia = trace['Actividad'] == 'Colonoscopia diagnóstica'
        # Crear una máscara booleana para identificar las filas donde 'Actividad' está en la lista
        mask_lista = trace['Actividad'].isin(lista)
        # Crear una máscara booleana que marque las filas que deben eliminarse
        mask_eliminar = mask_biopsia.cumsum() == 0
        # Aplicar la máscara para eliminar las filas anteriores a 'biopsia' excepto las que están en la lista
        trace = trace[~(mask_eliminar & ~mask_lista)]
        trace = trace.reset_index(drop=True)

        primer_indice = trace.index.to_list()[0]
        print(primer_indice)
        biopsia_indice = trace[trace['Actividad'] == 'Colonoscopia diagnóstica'].index.min()
        print(biopsia_indice)

        if (biopsia_indice - primer_indice) > 1:
            print('resta', biopsia_indice)
            trace = trace.iloc[biopsia_indice-1:]
        traces.append(trace)

    return pd.concat(traces)  


def surgery_activities(df, attrib_name):
    traces = []
    grouper_3 = df.groupby('NASI seudonimizado')

    for name, trace in grouper_3:
        for index in range(len(trace['Actividad'])):

            if trace['Actividad'].values[index] == attrib_name:
                if trace['Tipo intervención'].values[index] == 'PROGRAMADA': 
                    trace['Actividad'].values[index] = "Cirugia Programada"
                elif trace['Tipo intervención'].values[index] == 'URXENTE': 
                    trace['Actividad'].values[index] = "Cirugia Urgente"
                                
        traces.append(trace)
    traces_fix = pd.concat(traces)
    #renombre cirugia URXENTE
    traces_fix = traces_fix.rename(columns={"Cirugia PROGRAMADA": "Cirugia Programada", "Cirugia URXENTE": "Cirugia Urgente"})
    #eliminar actividad cita normal
    return traces_fix.drop(traces_fix.loc[traces_fix['Actividad'] == attrib_name].index)

# Función para convertir a float si es necesario
def convertir_a_float(valor):
    if isinstance(valor, np.int64):
        return int(valor)
    else:
        return valor  # Si ya es float, lo dejamos como está
    

def conditions_surgery_quimio(cirugia_index, quimioterapia_index, lenght):
    # Comprobar las condiciones posibles
    if np.isnan(cirugia_index) and not np.isnan(quimioterapia_index):
        print('El primer valor es nulo y el segundo no')
        indice_cirugia_quimioterapia = quimioterapia_index
    elif not np.isnan(cirugia_index) and np.isnan(quimioterapia_index):
        print('El primer valor no es nulo y el segundo sí')
        indice_cirugia_quimioterapia = cirugia_index
    elif not np.isnan(cirugia_index) and not np.isnan(quimioterapia_index):
        print('Ambos valores no son nulos')
        # Mantener la actividad que ocurra primero
        indice_cirugia_quimioterapia = min(cirugia_index, quimioterapia_index)
    elif np.isnan(cirugia_index) and np.isnan(quimioterapia_index):
        print('Ambos valores son nulos')
        indice_cirugia_quimioterapia = lenght
    
    return indice_cirugia_quimioterapia

def appointment_post_biopsia(df):
    # Agrupar por el identificador de traza
    groupon = df.groupby('NASI seudonimizado')

    df["num_citas_med_interna (Colono-Cirugia/quimio)"] = 0
    df["num_citas_oncologia (Colono-Cirugia/quimio)"] = 0
    df["num_citas_med_cir_general (Colono-Cirugia/quimio)"] = 0
    df["num_citas_digestivo (Colono-Cirugia/quimio)"] = 0
    df["num_tac_abdominal (Colono-Cirugia/quimio)"] = 0
    df["num_colonoscopia (Colono-Cirugia/quimio)"] = 0
    df["num_colonoscopia_VDR (Colono-Cirugia/quimio)"] = 0
    df["num_PAC (Colono-Cirugia/quimio)"] = 0

    df["num_citas_med_interna (Cirugia/quimio-final)"] = 0
    df["num_citas_oncologia (Cirugia/quimio-final)"] = 0
    df["num_citas_med_cir_general (Cirugia/quimio-final)"] = 0
    df["num_citas_digestivo (Cirugia/quimio-final)"] = 0
    df["num_tac_abdominal (Cirugia/quimio-final)"] = 0
    df["num_colonoscopia (Cirugia/quimio-final)"] = 0
    df["num_PAC (Cirugia/quimio-final)"] = 0

    traces = []
    # Iterar sobre cada grupo
    for name, trace in groupon:
        print(name)
        trace = trace.reset_index(drop=True)
        # Obtener el índice de la biopsia diagnóstica
        biopsia_indice = trace[trace['Actividad'] == 'Colonoscopia diagnóstica'].index.min()

        # Obtener el índice de la primera ocurrencia de 'Cirugía' y 'Quimioterapia' después de la biopsia
        cirugia_index = trace[trace.index > biopsia_indice][trace['Actividad'] == 'Cirugia'].index.min()
        quimioterapia_index = trace[trace.index > biopsia_indice][trace['Actividad'] == 'Quimioterapia'].index.min()
        cirugia_index = convertir_a_float(cirugia_index)
        quimioterapia_index = convertir_a_float(quimioterapia_index)
        print(cirugia_index)
        print(quimioterapia_index)
        #logica entre cirugia y quimio
        indice_cirugia_quimioterapia = conditions_surgery_quimio(cirugia_index, quimioterapia_index, len(trace))
        print(biopsia_indice,indice_cirugia_quimioterapia)

        #Contadores entre biosia y cirugia/quimio
        rango_indices = trace.index[biopsia_indice:indice_cirugia_quimioterapia]  # Puedes ajustar el rango según tu necesidad

        trace["num_citas_med_interna (Colono-Cirugia/quimio)"] = trace.loc[rango_indices, 'Actividad'].eq('Cita en Medicina interna').sum()
        trace["num_citas_oncologia (Colono-Cirugia/quimio)"] = trace.loc[rango_indices, 'Actividad'].eq('Cita en Oncología').sum()
        trace["num_citas_med_cir_general (Colono-Cirugia/quimio)"] = trace.loc[rango_indices, 'Actividad'].eq('Cita en Cirugía General').sum()
        trace["num_citas_digestivo (Colono-Cirugia/quimio)"] = trace.loc[rango_indices, 'Actividad'].eq('Cita en Digestivo').sum()
        trace["num_tac_abdominal (Colono-Cirugia/quimio)"] = trace.loc[rango_indices, 'Actividad'].eq('Cita TAC Abdominal').sum()
        trace["num_colonoscopia (Colono-Cirugia/quimio)"] = trace.loc[rango_indices, 'Actividad'].eq('COLONOSCOPIA').sum()
        trace["num_colonoscopia_VDR (Colono-Cirugia/quimio)"] = trace.loc[rango_indices, 'Actividad'].eq('COLONOSCOPIA VIA RAPIDA').sum()
        trace["num_PAC (Colono-Cirugia/quimio)"] = trace.loc[rango_indices, 'Actividad'].eq('Asistencia a punto de Atención Continuada').sum()

        #Contadores entre cirugia/quimio y el final
        rango_indices = trace.index[indice_cirugia_quimioterapia:]  # Puedes ajustar el rango según tu necesidad

        trace["num_citas_med_interna (Cirugia/quimio-final)"] = trace.loc[rango_indices, 'Actividad'].eq('Cita en Medicina interna').sum()
        trace["num_citas_oncologia (Cirugia/quimio-final)"] = trace.loc[rango_indices, 'Actividad'].eq('Cita en Oncología').sum()
        trace["num_citas_med_cir_general (Cirugia/quimio-final)"] = trace.loc[rango_indices, 'Actividad'].eq('Cita en Cirugía General').sum()
        trace["num_citas_digestivo (Cirugia/quimio-final)"] = trace.loc[rango_indices, 'Actividad'].eq('Cita en Digestivo').sum()
        trace["num_tac_abdominal (Cirugia/quimio-final)"] = trace.loc[rango_indices, 'Actividad'].eq('Cita TAC Abdominal').sum()
        trace["num_colonoscopia (Cirugia/quimio-final)"] = trace.loc[rango_indices, 'Actividad'].eq('COLONOSCOPIA').sum()
        trace["num_colonoscopia_VDR (Cirugia/quimio-final)"] = trace.loc[rango_indices, 'Actividad'].eq('COLONOSCOPIA VIA RAPIDA').sum()
        trace["num_PAC (Cirugia/quimio-final)"] = trace.loc[rango_indices, 'Actividad'].eq('Asistencia a punto de Atención Continuada').sum()

        interna = trace[trace.index > indice_cirugia_quimioterapia][trace['Actividad'] == 'Cita en Medicina interna'].index
        Oncologia = trace[trace.index > indice_cirugia_quimioterapia][trace['Actividad'] == 'Cita en Oncología'].index
        general = trace[trace.index > indice_cirugia_quimioterapia][trace['Actividad'] == 'Cita en Cirugía General'].index
        digestivo = trace[trace.index > indice_cirugia_quimioterapia][trace['Actividad'] == 'Cita en Digestivo'].index
        medico = trace[trace.index > biopsia_indice][trace['Actividad'] == 'Visita médico familia'].index

        # cirugia_index = convertir_a_float(cirugia_index)
        # quimioterapia_index = convertir_a_float(quimioterapia_index)
        list_to_lists = []
        list_to_lists.extend(interna)
        list_to_lists.extend(Oncologia)
        list_to_lists.extend(general)
        list_to_lists.extend(digestivo)
        list_to_lists.extend(medico)

        trace = trace.drop(list_to_lists)
        traces.append(trace)

    return pd.concat(traces)


def only_first_quimio(df):
    groupon = df.groupby('NASI seudonimizado')
    traces = []
    # Iterar sobre cada grupo
    for name, trace in groupon:
        
        trace = trace.reset_index(drop=True)
        indices_quimioterapia = trace.loc[trace['Actividad'] == 'Quimioterapia'].index
        if not indices_quimioterapia.empty:
            print(name)
            delete_index = indices_quimioterapia[1:]
            print(delete_index)
            trace = trace.drop(delete_index)
        traces.append(trace)
    return pd.concat(traces)


def change_dead_dates(df):
    #cargar df muertes
    path = "Data/raw_data/Subestudio estadística mortalidad VDR CCR (Inverbis).xlsx"
    defunciones = pd.read_excel(path)
    #crear diccionario cruce
    defunciones_dict = defunciones[['NASI seudonimizado','Data defunción 01/01/AAAA' ]]
    defunciones_dict['Data defunción 01/01/AAAA'] = pd.to_datetime(defunciones_dict['Data defunción 01/01/AAAA'], format= '%d-%m-%Y %H:%M:%S')
    defunciones_dict_clear = defunciones_dict.dropna(subset=['Data defunción 01/01/AAAA'])
    defunciones_dict_clear['Data defunción 01/01/AAAA'] = defunciones_dict_clear['Data defunción 01/01/AAAA']
    fecha_muerte = defunciones_dict_clear['Data defunción 01/01/AAAA'].tolist()
    id = defunciones_dict_clear['NASI seudonimizado'].tolist()
    dict_change = dict(zip(id, fecha_muerte))
    # Aplicar la traducción utilizando el método replace de pandas
    df['Actividad'] = df['Actividad'].replace(dict_change) 
    df.loc[df['Actividad'] == 'Defunción', 'FECHA'] = df.loc[df['Actividad'] == 'Defunción', 'NASI seudonimizado'].map(dict_change)
    df.loc[df['Actividad'] == 'Defunción', 'FECHA_FIN'] = df.loc[df['Actividad'] == 'Defunción', 'NASI seudonimizado'].map(dict_change)
    # colon['FECHA'] = pd.to_datetime(colon['FECHA'], format= '%d-%m-%Y %H:%M:%S')
    # colon['FECHA_FIN'] = pd.to_datetime(colon['FECHA_FIN'], format= '%d-%m-%Y %H:%M:%S')
    return df


def counters_pre_biopsia(df):
    # Agrupar por el identificador de traza
    groupon = df.groupby('NASI seudonimizado')
    df["NUM_LAXANTES"] = 0

    traces = []
    # Iterar sobre cada grupo
    for name, trace in groupon:
        print(name)
    
        # Convertir la columna FECHA a tipo datetime
        trace['FECHA'] = pd.to_datetime(trace['FECHA'], format= '%d-%m-%Y %H:%M:%S')
        # Encontrar la fecha de la actividad 'Biopsia'
        fecha_biopsia = trace.loc[trace['Actividad'] == 'Colonoscopia diagnóstica', 'FECHA'].iloc[0]
        # Restar 6 meses a la fecha de la biopsia
        fecha_limite = fecha_biopsia - pd.DateOffset(months=6)
        # Filtrar los registros dentro del rango de tiempo
        registros_filtrados = trace[(trace['FECHA'] >= fecha_limite) & (trace['FECHA'] <= fecha_biopsia)]
        # Contar el número de veces que aparece 'laxante' en el campo 'medicamentos'
        num_laxante_formador = registros_filtrados['Subg. químico terapéutico ATC disp'].value_counts().get('LAXANTES FORMADORES DE VOLUMEN', 0)
        num_laxante_contacto = registros_filtrados['Subg. químico terapéutico ATC disp'].value_counts().get('LAXANTES DE CONTACTO', 0)
        num_laxante_osmotico = registros_filtrados['Subg. químico terapéutico ATC disp'].value_counts().get('LAXANTES OSMÓTICOS', 0)
        print(f"""Número de veces que aparece 'laxante' desde al menos 6 meses hacia atrás desde la actividad 'Biopsia':\n
              {num_laxante_formador}
              {num_laxante_contacto}
              {num_laxante_osmotico}""")
        print(f"Numero de laxantes totales {num_laxante_formador + num_laxante_contacto + num_laxante_osmotico}")
        trace["NUM_LAXANTES"] = num_laxante_formador + num_laxante_contacto + num_laxante_osmotico
        traces.append(trace)

    return pd.concat(traces)


def counters_quimio(df):
    # Agrupar por el identificador de traza
    groupon = df.groupby('NASI seudonimizado')
    df["NUM_QUIMIOS"] = 0

    traces = []
    # Iterar sobre cada grupo
    for name, trace in groupon:
        print(name)
    
        # Convertir la columna FECHA a tipo datetime
        trace['FECHA'] = pd.to_datetime(trace['FECHA'], format= '%d-%m-%Y %H:%M:%S')
        # Encontrar la fecha de la actividad 'Biopsia'
        fecha_biopsia = trace.loc[trace['Actividad'] == 'Colonoscopia diagnóstica', 'FECHA'].iloc[0]
        # Filtrar los registros dentro del rango de tiempo
        registros_filtrados = trace[trace['FECHA'] >= fecha_biopsia]
        # Contar el número de veces que aparece 'laxante' en el campo 'medicamentos'
        num_quimios = registros_filtrados['Actividad'].value_counts().get('Quimioterapia', 0)
        # num_laxante_contacto = registros_filtrados['Subg. químico terapéutico ATC disp'].value_counts().get('LAXANTES DE CONTACTO', 0)
        # num_laxante_osmotico = registros_filtrados['Subg. químico terapéutico ATC disp'].value_counts().get('LAXANTES OSMÓTICOS', 0)
        print(f"Número de veces que aparece quimioterapia desde la actividad 'Biopsia':{num_quimios}")
        trace["NUM_QUIMIOS"] = num_quimios
        traces.append(trace)

    return pd.concat(traces)


def save_semiprocessed_df(df):
    df.to_csv("Data/processed_data/CancerColon_semi"+str(version_out)+".csv", date_format="%d-%m-%Y %H:%M:%S", index=False,  encoding="UTF-8", sep = ';')


def save_processed_df(df):
    df.to_csv("Data/processed_data/CancerColon_filtered"+str(version_out)+".csv", date_format="%d-%m-%Y %H:%M:%S", index=False,  encoding="UTF-8", sep = ';')
    print(df.columns)
    #Crear version basica
    df_2 = df[['NASI seudonimizado', 'FECHA', 'FECHA_FIN','Actividad']]
    #Guardar version basica 
    df_2.to_csv("Data/processed_data/CancerColon_basico_"+str(version)+".csv", date_format="%d-%m-%Y %H:%M:%S", index=False,  encoding="UTF-8", sep = ';')


if __name__ == "__main__":
    #!Genero dataset raw
    if generate:
        df_names = charge_data()
        colon = df_names['colon']
        dataframes = config.create_data_model(colon)
        colon = concat_df(dataframes)
        colon.to_csv("Data/processed_data/CancerColon_raw.csv", index=False, date_format="%d-%m-%Y %H:%M:%S", encoding="UTF-8", sep = ';')
        print(colon['Actividad'].unique())
    #! Cargo dataset raw
    else:
        path_gen = "/home/pol/Escritorio/INVERBIS/2024/CancerColon/"
        path_1 =  "Data/processed_data/CancerColon_raw.csv"
        colon = pd.read_csv(path_1, sep= ';')
        print(colon.shape)
    
    #! Preprocesado
    if preprocess:
        #Filtra trazas con colonoscopia
        #colon = no_colonoscopy(colon)
        #! Anonimizar valores  
        colon = anonymize_atributes(colon,
                                    'Hospital',
                                    'Hospital' )
        colon = anonymize_atributes(colon,
                                    'PAC',
                                    'PAC' )
        #! Aplicar configuracion
        colon = colon_configuration_apply(colon)
        
        #! Arreglar fechas
        colon = fix_dates(colon,
                          start_year= 2012,
                          end_year = 2020)
        #! Arreglar fechas de defunción con fechas reales
        colon = change_dead_dates(colon)

        #! Eliminar nan
        colon = delete_nan(colon)

        #! Aqui irá lo de los conteos
        #save_semiprocessed_df(colon)
        colon = counters_pre_biopsia(colon) 
        colon = counters_quimio(colon) 

        #! Filtrar por encima de la fecha de extracción y arreglar la actividad de Defunción
        colon_filtered = filter_extraction(colon)
       
        #!Pone apellidos a citas de tipo colonoscopia y crea booleanos para identificar ids con colonoscopia y con VDR, tambien creo columnas de contadores posteriores a biopsia
        colon_filtered = surname_Appointments(colon_filtered,
                                              'Cita')
        save_semiprocessed_df(colon_filtered)
        #!Creo columna con duracion lista de espera y lo elimino como actividad
        colon_filtered = waiting_list_duration(colon_filtered,
                                      'Entrada Lista de Espera',
                                      'Salida Lista de Espera' )
        
        #! Elimino aquellas actividades de visita medico que no son relevantes
        colon_filtered = filter_doctor_visit(colon_filtered)

        #! Elimino aquellas actividades de cirugia que no son de tipo XERAL E DIXESTIVO
        colon_filtered = filter_surgery_typology(colon_filtered)

        #! Eliminar autobucles
        colon_filtered = self_loops(colon_filtered)
        
        #!Tratamiento con los medicamentos
        colon_filtered = boolean_important_drugs(colon_filtered)
        #colon_filtered = filter_important_drugs(colon_filtered)
        colon_filtered = drugs_info(colon_filtered)

        #! Crear actividad inicial
        colon_filtered = create_initial_activity(colon_filtered)

        #! Filtro los 4 tipos de citas importantes que hay detras de la actividad de cirugia/quimioterapia
        #! y tambien citas medicas solo anterioires a la biopsia y contadores entre biopsia y cirugia/quimioterapia y posterior a cirugia/quimioterapia
        colon_filtered = appointment_post_biopsia(colon_filtered) 

        #! Elimino todas las quimios duplicadas posterioires a la primera quimio (primer ciclo de quimio)
        colon_filtered = only_first_quimio(colon_filtered)

        #! Creo dos actividades para los tipos de cirugia, una programada y otra urgente (apellidos)
        colon_filtered = surgery_activities(colon_filtered, 'Cirugia')
        
        #!Eliminar '\n' y '\r' en valores y cabeceras
        colon_filtered = colon_filtered.replace({r'\r': '', r'\n': ''}, regex=True)
        #save_semiprocessed_df(colon_filtered)
        #!Antes de guardarlo tengo que eliminar ciertas actividades
        conservate_list = [ 
            'Cita en Medicina interna',
            'Colonoscopia diagnóstica',
            'Ingreso hospitalario',
            'Cirugia Programada',
            'Alta Hospitalizacion',
            'Visita médico familia',
            'Defunción',
            'Cita en Cirugía General',
            'Quimioterapia',
            'Cita en Digestivo',
            'Cirugia Urgente',
            'Cita en Oncología',
            ]
 
        colon_filtered = conservate_activity(colon_filtered, conservate_list)

        #! Arreglar fechas de defunción con fechas reales
        colon_filtered = change_dead_dates(colon_filtered)

        #!  Visualizar info y dimension df final
        df_info(colon_filtered)

        #! Guardar version completa y basica
        save_processed_df(colon_filtered)
        
        print('\nDataset procesado guardado\n')

        grouponi = colon_filtered.groupby('NASI seudonimizado')

        for name, trace in grouponi:
            print(name)
            print(trace['Actividad'].values.tolist())
            print('\n')



     