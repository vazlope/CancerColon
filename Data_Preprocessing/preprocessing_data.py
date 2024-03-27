import pandas as pd
from datetime import datetime
from config import dfs_config as config
from config.dfs_config import create_data_model
from config.colon_config import colon_configuration_apply
import sys
sys.path.append('C:/Users/pablo/Desktop/INVERBIS/2024/Ribera_Salud/Exploratory_Data_Analysis')
fecha_actual = datetime.now()
fecha_actual_str = fecha_actual.strftime("%Y-%m-%d %H:%M:%S")
version = 'V0_1'
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

def operating_room_fix(df):
    #Aqui deberia hace la correcion para los datos de quirofano
    #Saco primero los ids de trazas que contienen la actividad de quirofano
    trazas_con_quirofano = df.query("Actividad == 'Programacion Quirofano'")['NASI seudonimizado'].tolist()
    print(len(trazas_con_quirofano))
    #recorro traza a traza y añado a cada traza los valores de Room y duration 
    df['Reserva_quirofano'] = False
    df['Varios_quirofano'] = False
    df_s = []
    cont = 0
    group = df.groupby('NASI seudonimizado') 
    for nombre_traza, traza in group: # Para cada traza
        if nombre_traza in trazas_con_quirofano:
            traza['Duracion_AdmMed_Cirugia'] = traza['Duration'].sum()
            traza['Reserva_quirofano'] = True
            cont +=1
            if len(traza['Room'].unique()) ==2:
                traza['Habitacion Quirofano'] = traza['Room'].unique()[1]
                traza['Varios_quirofano'] = False
            elif len(traza['Room'].unique()) >2:
                print(f"{str(traza['Room'].unique()[1])} y {str(traza['Room'].unique()[2])}")
                traza['Habitacion Quirofano'] = f"{str(traza['Room'].unique()[1])} y {str(traza['Room'].unique()[2])}"
                traza['Varios_quirofano'] = True
        df_s.append(traza)
    df = pd.concat(df_s)
    print(cont)
    #borrar columnas Room y Duration
    df.drop(columns=['Room', 'Duration'], axis = 0, inplace= True)
    df.groupby('Reserva_quirofano')['CodPaciente'].nunique()
    return df

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

if __name__ == "__main__":
    #!Genero dataset raw
    if generate:
        df_names = charge_data()
        colon = df_names['colon']
        dataframes = config.create_data_model(colon)
        colon = concat_df(dataframes)
        colon.to_csv("Data/processed_data/CancerColon_raw.csv", index=False, date_format="%d-%m-%Y %H:%M:%S", encoding="UTF-8", sep = ';')
    # # ribera_pre = merge_df(ribera1, dfs['Mantenimiento protocolos'])
    # # ribera_pos = operating_room_fix(ribera_pre)
    #! Cargo dataset raw
    else:
        path_gen = "/home/pol/Escritorio/INVERBIS/2024/CancerColon/"
        path_1 = path_gen + "Data/processed_data/CancerColon_raw.csv"
        colon = pd.read_csv(path_1, sep= ';')
        print(colon.shape)
    
    #! Preprocesado
    if preprocess:
        print(colon['Actividad'].unique())
        print(colon.columns)
        #Anonimizar valores  
        colon = anonymize_atributes(colon,'Hospital', 'Hospital' )
        colon = anonymize_atributes(colon,'PAC', 'PAC' )
        #Aplicar configuracion
        colon = colon_configuration_apply(colon)
        #Eliminar autobucles
        colon = self_loops(colon)
        #Arreglar fechas
        colon = fix_dates(colon, start_year= 2012, end_year = 2020)
        #Eliminar nan
        colon = delete_nan(colon)
        #Eliminar '\n' y '\r' en valores y cabeceras
        colon = colon.replace({r'\r': '', r'\n': ''}, regex=True)
        #Guardar version completa
        colon.to_csv("Data/processed_data/CancerColon_"+str(version)+".csv", date_format="%d-%m-%Y %H:%M:%S", index=False,  encoding="UTF-8", sep = ';')
        print(colon.columns)
        #Crear version basica
        colon_basic = colon[['NASI seudonimizado', 'FECHA', 'FECHA_FIN','Actividad']]
        #Guardar version basica 
        colon_basic.to_csv("Data/processed_data/CancerColon_basico_"+str(version)+".csv", date_format="%d-%m-%Y %H:%M:%S", index=False,  encoding="UTF-8", sep = ';')
        print('\nDataset procesado guardado\n')