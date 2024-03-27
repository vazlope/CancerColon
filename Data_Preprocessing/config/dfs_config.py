import pandas as pd

def create_mining_df(df, columnas_fecha, name):
    
    group_d = df.groupby('NASI seudonimizado') # Agrupamos por el identificador de traza
    # Crea una lista para almacenar las filas duplicadas
    filas_duplicadas = []
    for nombre_traza, traza in group_d: # Para cada traza
        for _, fila in traza.iterrows():
                # Para cada fecha, crea una nueva fila duplicada con la fecha en la columna 'FECHA'
                for columna_fecha in columnas_fecha:
                    nueva_fila = fila.copy()
                    
                    if columna_fecha == str('DATA ENTRADA') or columna_fecha == str('DATA SALE'):
                        nueva_fila['FECHA'] = fila['DATA ENTRADA']
                        nueva_fila['FECHA_FIN'] = fila['DATA SALE']
                    else:
                        nueva_fila['FECHA'] = fila[columna_fecha]
                        nueva_fila['FECHA_FIN'] = fila[columna_fecha]
                    if name == 'quirofano':
                        nombre_actividad = str('programacion Quirofano')
                    else:
                        nombre_actividad = str(columna_fecha)
                    nueva_fila['Actividad'] = nombre_actividad.replace('Fecha', '')
                    filas_duplicadas.append(nueva_fila)
    
    # Crea un nuevo dataframe con las filas duplicadas
    df = pd.DataFrame(filas_duplicadas)
    # Ordena el dataframe resultante por 'ID' y 'FECHA'
    df = df.sort_values(by=['NASI seudonimizado', 'FECHA'])
    # Restablece los índices del dataframe resultante
    df = df.reset_index(drop=True)
    return df

def create_data_model(df_names):
    for name, df in zip(df_names.keys(),df_names.values()):
        columnas_fecha = []

        if name == 'Gasto farmacéutico':   
            print(name) 
            columnas_fecha.append('Data dispensación')
            #df[columnas_fecha] = df[columnas_fecha].apply(pd.to_datetime(infer_datetime_format=True)) 
            medicamentos = create_mining_df(df, columnas_fecha, name)
        
        elif name == 'Urgencias hospitalarias':   
            print(name) 
            columnas_fecha.append('Data atención')
            #df[columnas_fecha] = df[columnas_fecha].apply(pd.to_datetime(infer_datetime_format=True)) 
            urgencias = create_mining_df(df, columnas_fecha, name)
        
        elif name == 'PAC':   
            print(name) 
            columnas_fecha.append('Día asistencia')
            #df[columnas_fecha] = df[columnas_fecha].apply(pd.to_datetime(infer_datetime_format=True)) 
            pac = create_mining_df(df, columnas_fecha, name)
        
        elif name == 'Visitas médico familia':   
            print(name) 
            columnas_fecha.append('Data consulta')
            #df[columnas_fecha] = df[columnas_fecha].apply(pd.to_datetime(infer_datetime_format=True)) 
            visitas = create_mining_df(df, columnas_fecha, name)
        
        elif name == 'Episodios y condicionantes':   
            print(name) 
            columnas_fecha.append('Data inicio episodio')
            #df[columnas_fecha] = df[columnas_fecha].apply(pd.to_datetime(infer_datetime_format=True)) 
            episodio = create_mining_df(df, columnas_fecha, name)
        
        elif name == 'Hospitalización':   
            print(name) 
            columnas_fecha.append('Data ingreso')
            columnas_fecha.append('Data de alta')
            #df[columnas_fecha] = df[columnas_fecha].apply(pd.to_datetime(infer_datetime_format=True)) 

            hospitalización = create_mining_df(df, columnas_fecha, name)

        elif name == 'Consultas y pruebas at. especia':
            print(name) 
            columnas_fecha.append('Data prescrición') 
            columnas_fecha.append('Data cita')
            columnas_fecha.append('Día da semana da cita')
            #df[columnas_fecha] = df[columnas_fecha].apply(pd.to_datetime(infer_datetime_format=True)) 
      
            consultas = create_mining_df(df, columnas_fecha, name)
        
        elif name == 'CS, Médico de familia y Fecha d':   
            print(name) 
            columnas_fecha.append('Ano')
            #df[columnas_fecha] = df[columnas_fecha].apply(pd.to_datetime(infer_datetime_format=True)) 
            medico_familia = create_mining_df(df, columnas_fecha, name)

        elif name == 'LEQ':
            print(name) 
            columnas_fecha.append('Data inclusión') 
            columnas_fecha.append('Data remate')
            #df[columnas_fecha] = df[columnas_fecha].apply(pd.to_datetime(infer_datetime_format=True)) 
      
            leq = create_mining_df(df, columnas_fecha, name)
        
        elif name == 'Cirugía':   
            print(name) 
            columnas_fecha.append('Data intervención')
            #df[columnas_fecha] = df[columnas_fecha].apply(pd.to_datetime(infer_datetime_format=True)) 
            cirugia = create_mining_df(df, columnas_fecha, name)
        
        elif name == 'Farmacia onlolóxica':   
            print(name) 
            columnas_fecha.append('Data dispensación')
            #df[columnas_fecha] = df[columnas_fecha].apply(pd.to_datetime(infer_datetime_format=True)) 
            farmacia = create_mining_df(df, columnas_fecha, name)
        
        elif name == 'Mortalidad':   
            print(name) 
            columnas_fecha.append('Data defunción 01/01/AAAA')
            #df[columnas_fecha] = df[columnas_fecha].apply(pd.to_datetime(infer_datetime_format=True)) 
            mortalidad = create_mining_df(df, columnas_fecha, name)
        
        elif name == 'APA':
            print(name) 
            columnas_fecha.append('DATA EXTRACCIÓN') 
            columnas_fecha.append('DATA ENTRADA')
            columnas_fecha.append('DATA SALE')
            #df[columnas_fecha] = df[columnas_fecha].apply(pd.to_datetime(infer_datetime_format=True)) 
      
            apa = create_mining_df(df, columnas_fecha, name)
    dataframes = [medicamentos, urgencias, pac, visitas, episodio, hospitalización, consultas, medico_familia, leq, cirugia, farmacia, mortalidad, apa]
    return dataframes

        

        