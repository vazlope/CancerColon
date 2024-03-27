import pandas as pd
from datetime import datetime

def df_info(df_name, df, atrib_name, only_basics = False):
    print(f'\n- {df_name}:')

    try:
        if only_basics:
            print(f'    {df_name} -> {df.shape}')
            print(f'    Trace_ID unique count -> {df[atrib_name].nunique()}')
        else:
            print(f'\nTrace_ID unique count -> {df[atrib_name].nunique()}\n')
            print(f'\nINFO -> {df.info()}')
            print(f'\nStatistical Info -> {df.describe()}')
    except KeyError:
        print(f'    WARNING! -> El df {df_name} no tiene una columna llamada {atrib_name}')


def IDs_comunes(df1, df2, atrib_name1, atrib_name2):
    # Encontrar los valores únicos en cada columna
    valores_columna1 = set(df1[atrib_name1])
    valores_columna2 = set(df2[atrib_name2])
    # Encontrar la intersección de los conjuntos
    valores_comunes = valores_columna1.intersection(valores_columna2)
    # Contar cuántos valores son comunes
    cantidad_valores_comunes = len(valores_comunes)
    print("Cantidad de valores comunes:", cantidad_valores_comunes)
    print("Valores comunes:", valores_comunes)

def excels_to_csv(url_excel, url_csv):
    #PRIMERO PASAR EL NUEVO EXCEL A CSV
    # Leer el archivo Excel y cargarlo en un DataFrame de Pandas
    df_excel = pd.read_excel(url_excel)
    # Guardar el DataFrame como un archivo CSV con separador ';'
    df_excel.to_csv(url_csv, sep=';', index=False)
    df_csv = pd.read_csv(url_excel, sep = ';')
    print('\nGUARDADO CORRECTAMENTE\n')
    return df_csv


def oneId_to_oneValue(df):
    # Creamos un DataFrame vacío donde almacenaremos los resultados
    resultados = pd.DataFrame()
    grouper = df.groupby('traceId')
    for group_name, group_df in grouper:
        row = {}
        row['traceId'] = group_name
        for col in group_df.columns[1:]:  # Excluimos la columna 'ID' de la iteración
            row[col] = group_df[col].dropna().iloc[0] if not group_df[col].isnull().all() else None
        resultados = resultados.append(row, ignore_index=True)
    return resultados

def oneId_to_UniqueValues(df):
    # Creamos un DataFrame vacío donde almacenaremos los resultados
    resultados = pd.DataFrame()
    grouper = df.groupby('traceId')
    for group_name, group_df in grouper:
        row = {}
        row['traceId'] = group_name
        for col in group_df.columns[1:]:  # Excluimos la columna 'ID' de la iteración
            row[col] = group_df[col].dropna().iloc[0] if not group_df[col].isnull().all() else None
        resultados = resultados.append(row, ignore_index=True)
    return resultados

def columnas_separables(df): 
    var_sep = []
    var_dejar = []
    for columna in df.columns.to_list()[5:]:
        cont_unico, cont_multiple = 0,0
        # Filtramos los valores nan o vacíos
        df_filtered = df.dropna(subset=[columna])
        filtro = df_filtered.groupby('traceId')[columna].nunique().to_dict()
        for ids in filtro.keys():
            if filtro[ids] > 1:
                cont_multiple +=1
            else:
                cont_unico +=1
        if cont_multiple ==  0:
            var_sep.append(columna)
        else:
            var_dejar.append(columna)
    
    var_sep.append('traceId')
    var_dejar.append('traceId')
    return var_dejar, var_sep

#utils ribera


    