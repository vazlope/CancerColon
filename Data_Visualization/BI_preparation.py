import pandas as pd
from datetime import datetime
from Utils import utils_ribera as utils
fecha_actual = datetime.now()
import numpy as np
version = 'V3_ConProtocoloyMedic'
path_minado = "Data/mined_data/trace_activities.csv"
path_orig = "Data/processed_data/RiberaSalud_"+str(version)+".csv"

def charge_df():
    df = pd.read_csv(path_minado, sep = ',')
    print(df.shape)
    print(df.columns)
    return df

def separate_columns(df):
    columnas_dejar, columnas_separar = utils.columnas_separables(df)
    #Ribera atributos separables
    ribera_atributos = df[columnas_separar]
    last_column = ribera_atributos.columns[-1]
    other_columns = ribera_atributos.columns[:-1]
    new_columns = [last_column] + list(other_columns)
    ribera_atributos = ribera_atributos[new_columns]
    ribera_atributos_fix = utils.oneId_to_oneValue(ribera_atributos)
    print(ribera_atributos_fix.columns)
    print(ribera_atributos_fix.shape)
    ribera_atributos_fix.to_csv("Salida_script/RiberaSalud_atributosBI.csv", index=False, date_format="%d/%m/%Y %H:%M:%S", encoding="UTF-8", sep = ';')
  

def keep_columns(df):
    columnas_dejar, columnas_separar = utils.columnas_separables(df)
    #Ribera atributos dejables
    ribera_atributos_dejables = df[columnas_dejar]
    last_column = ribera_atributos_dejables.columns[-1]
    other_columns = ribera_atributos_dejables.columns[:-1]
    new_columns = [last_column] + list(other_columns)
    ribera_atributos_dejables = ribera_atributos_dejables[new_columns]
    print(ribera_atributos_dejables.columns)
    print(ribera_atributos_dejables.shape)
    ribera_atributos_dejables.to_csv("Salida_script/RiberaSalud_atributos_dejablesBI.csv", index=False, date_format="%d/%m/%Y %H:%M:%S", encoding="UTF-8", sep = ';')


def operating_room_crossing_table(df):
    #Generar tabla de cruce con valor de duracion en el quirofano
    ribera_dur = df[['traceId', 'activity', 'start', 'end', 'DuracionAdmMedCirugia']]
    ribera_dur['Incumplimiento_SLA'] = False
    ribera_dur.sort_values(by= 'end')
    ribera_dur['end'] = ribera_dur['end'].apply(pd.to_datetime)
    grouped1 = ribera_dur.groupby('traceId')
    trazas = []
    for name, traza in grouped1:
        traza['end'] = pd.to_datetime(traza['end'])
        traza['start'] = pd.to_datetime(traza['start'])
        entrada = traza[traza['activity'] == 'Entrada en quirofano']
        incision = traza[traza['activity'] == 'Cirugia']
        salida = traza[traza['activity'] == 'Salida de quirofano']
        duration = (salida['end'].values[0]) - (entrada['end'].values[0])
        dur_min = (duration / np.timedelta64(1, 's'))/60
        traza['duracion_quirofano'] = dur_min   
        #calculo diferencia duracion KPI real
        duration_cirugia_limpieza = (salida['end'].values[0]) - (incision['start'].values[0])
        duration_cirugia_limpieza_min = (duration_cirugia_limpieza / np.timedelta64(1, 's'))/60
        traza['duration_cirugia_limpieza'] = duration_cirugia_limpieza_min   
        trazas.append(traza)  
    ribera_dur = pd.concat(trazas).sort_values(by= 'start')
    ribera_dur.drop('activity', axis= 1, inplace= True)
    ribera_dur.rename(columns= {'DuracionAdmMedCirugia':'Duracion_quirofano_KPI'}, inplace=True)
    trazas = []
    for name, traza in ribera_dur.groupby('traceId'):
        traza.drop_duplicates(subset='duracion_quirofano', inplace= True)
        traza['activity'] = 'Quirofano'
        dif =  traza['Duracion_quirofano_KPI'].values[0] - traza['duration_cirugia_limpieza'].values[0] 
        traza['Diferencia_quirofano'] = dif
        if dif < 0:
            traza['Incumplimiento_SLA'] = True
        trazas.append(traza)
    ribera_dur = pd.concat(trazas)
    #Guardar csv
    print(ribera_dur.head(10))
    #print(ribera_dur.shape)
    ribera_dur.to_csv("Salida_script/RiberaSalud_duracionQuirofano.csv", index=False, date_format="%d/%m/%Y %H:%M:%S", encoding="UTF-8", sep = ';')

if __name__ == "__main__":

    dfs = charge_df()