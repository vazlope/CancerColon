import pandas as pd
from datetime import datetime, timedelta
from config import dfs_config as config
from config.dfs_config import create_data_model
from config.colon_config import colon_configuration_apply
import sys
sys.path.append('C:/Users/pablo/Desktop/INVERBIS/2024/Ribera_Salud/Exploratory_Data_Analysis')
fecha_actual = datetime.now()
fecha_actual_str = fecha_actual.strftime("%Y-%m-%d %H:%M:%S")
version = 'V0_1'
version_out = 'V0_9'
TIME_BEFORE_EXTRACTION = 5*24*3600     #! MONTH TO SECONDS
generate = False
preprocess = True


def charge_data(): 
    path_gen = "/home/pol/Escritorio/INVERBIS/2024/CancerColon/"
    path_1 = path_gen + "Data/raw_data/VDR.xlsx"
    path_2 = path_gen + "Data/raw_data/Copia de Términos agrupados.xlsx"
    path_3 = path_gen + "Data/raw_data/VDR_DatosLaboratorio_Seudonimizados_v.01.00.xlsx"
    path_4 = path_gen + "Data/raw_data/VDR_DatosNoEstructurados_Seudonimizados.xlsx"

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
    trace['Defuncion'] = True
    return trace


def modify_dead_date(trace):
    max_date = trace['FECHA'].max()
    new_date = max_date + timedelta(days=30)
    new_date = pd.to_datetime(new_date, format= '%d-%m-%Y %H:%M:%S')
    # Cambiar las fechas asociadas a 'Defunción'
    trace.loc[trace['Actividad'] == 'Defunción', ['FECHA', 'FECHA_FIN']] = new_date
    return trace

def filter_extraction(df):

    df['FECHA'] = pd.to_datetime(df['FECHA'], format= '%d-%m-%Y %H:%M:%S')
    df['FECHA_FIN'] = pd.to_datetime(df['FECHA_FIN'], format= '%d-%m-%Y %H:%M:%S')
    grouper = df.groupby('NASI seudonimizado')

    dates, traces = [], []
    dates_dict = {}
    for name, trace in grouper:
        trace['Defuncion'] = False
        if 'Prueba laboratorio (APA)' in trace['Actividad'].unique().tolist():
            df = trace.query(" Actividad == 'Prueba laboratorio (APA)'")
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
    df['Medicamento importante'] = False
    # Marcar como True si alguno de los medicamentos importantes está presente para cada ID único
    for group_id, group_data in df.groupby('NASI seudonimizado'):
        if any(group_data['Subg. químico terapéutico ATC disp'].isin(important_drugs)):
            df.loc[group_data.index, 'Medicamento importante'] = True
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
    df['VDR'] = False
    df['Colonoscopia'] = False
    df['Consultas Digestivo/Medicina interna'] = False
    df['Consultas Cirugía General/Oncología'] = False
    df['Realización TC abdominales'] = False
    lista_colono = ['COLONOSCOPIA', 'COLONOSCOPIA VIA RAPIDA']
    lista_TCAbdominal = tc_abdominal(colon)
    grouper_3 = df.groupby('NASI seudonimizado')

    for name, trace in grouper_3:
        for index in range(len(trace['Actividad'])):

            if trace['Actividad'].values[index] == attrib_name:
                if trace['Actividade CAPNOR'].values[index] in lista_colono: 
                    trace['Actividad'].values[index] = trace['Actividade CAPNOR'].values[index]
                    trace['Colonoscopia'] = True
                    if trace['Tipo vía rápida'].values[index] == "VIA RAPIDA COLON              ":
                        trace['VDR'] = True

                elif trace['Actividade CAPNOR'].values[index] == 'CONSULTA DE ENFERMIDADE':
                    if trace['GNA'].values[index] == 'DIXESTIVO' or trace['GNA'].values[index] == 'MEDICINA INTERNA':
                        trace['Actividad'].values[index] = 'Consultas en Digestivo/Medicina interna'
                        trace['Consultas Digestivo/Medicina interna'] = True
                        
                    elif trace['GNA'].values[index] == 'ONCOLOXIA MEDICA' or trace['GNA'].values[index] == 'CIRURXIA XERAL E DIXESTIVA':
                        trace['Actividad'].values[index] = 'Consultas en Cirugía General/Oncología'
                        trace['Consultas Cirugía General/Oncología'] = True


                elif trace['Actividade CAPNOR'].values[index] in lista_TCAbdominal: 
                    trace['Actividad'].values[index] = 'Realización de TC abdominales'
                    trace['Realización TC abdominales'] = True
           
                            
        traces.append(trace)
    traces_fix = pd.concat(traces)
    #eliminar actividad cita normal
    return traces_fix.drop(traces_fix.loc[traces_fix['Actividad'] == attrib_name].index)


def save_processed_df(df):
    df.to_csv(path_gen+"Data/processed_data/CancerColon_filtered"+str(version_out)+".csv", date_format="%d-%m-%Y %H:%M:%S", index=False,  encoding="UTF-8", sep = ';')
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
        path_1 = path_gen + "Data/processed_data/CancerColon_raw.csv"
        colon = pd.read_csv(path_1, sep= ';')
        print(colon.shape)
    
    #! Preprocesado
    if preprocess:
        #Filtra trazas con colonoscopia
        #colon = no_colonoscopy(colon)
        #Anonimizar valores  
        colon = anonymize_atributes(colon,'Hospital', 'Hospital' )
        colon = anonymize_atributes(colon,'PAC', 'PAC' )
        #Aplicar configuracion
    
        colon = colon_configuration_apply(colon)

        
        print(colon['Actividad'].unique())
        print(colon.columns)
        
        #Arreglar fechas
        colon = fix_dates(colon, start_year= 2012, end_year = 2020)
        #Eliminar nan
        colon = delete_nan(colon)
        
        #Filtrar por encima de la fecha de extracción y arreglar la actividad de Defunción
        colon_filtered = filter_extraction(colon)
        print(colon_filtered['Actividad'].unique())
        print(colon_filtered.columns)
        #Pone apellidos a citas de tipo colonoscopia y crea booleanos para identificar ids con colonoscopia y con VDR
        colon_filtered = surname_Appointments(colon_filtered, 'Cita')
        print(colon_filtered['Actividad'].unique())
        print(colon_filtered.columns)
        #Eliminar autobucles
        colon_filtered = self_loops(colon_filtered)
        print(colon_filtered['NASI seudonimizado'].nunique())
        colon_filtered = boolean_important_drugs(colon_filtered)
        #colon_filtered = filter_important_drugs(colon_filtered)
        print(colon_filtered['NASI seudonimizado'].nunique())
        colon_filtered = drugs_info(colon_filtered)
        
        #Eliminar '\n' y '\r' en valores y cabeceras
        colon = colon.replace({r'\r': '', r'\n': ''}, regex=True)
        print(colon_filtered['NASI seudonimizado'].nunique())

  
        #Guardar version completa y basica
        save_processed_df(colon_filtered)
        
        print('\nDataset procesado guardado\n')



     