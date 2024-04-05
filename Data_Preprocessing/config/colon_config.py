import pandas as pd

def chosen_atributtes(df):
    lista_defi = [
        'NASI seudonimizado',
        'Subg. químico terapéutico ATC disp',
        'Principio activo ATC disp',
        'Prezo PVP nomen disp',
        'FECHA',
        'FECHA_FIN',
        'Actividad',
        'Duracion Lista Espera',
        'Hospital',
        'Motivo de asistencia',
        'Motivo saída',
        'PAC',
        'Lugar asistencia',
        'Motivo asistencia',
        'Acto',
        'Cód. CIAP 2',
        'Desc. CIAP 2',
        'Orixe',
        # 'Lista diagnósticos',
        # 'Lista procedementos',
        'Prioridade',
        'GNA',
        'Actividade CAPNOR',
        'Código Prestación',
        'Prestación',
        'Tipo vía rápida',
        'Detalle tipo actividade',
        'Tipo programación',
        #'Cód. Centro seudonimizado',
        'Cnp med. Seudonimizado',
        'Sexo',
        'Cola',
        'Tipo intervención',
        'Orixe PM/Diagnóstico',
        'PM/Diagnóstico',
        #'Cód. cat. CIE',
        'Categoría CIE',
        'Subcategoría CIE',
        'CENTRO',
        'SERVICIO',
        'MEDICO',
        #'TIPO MUESTRA',
        'MUESTRA',
        'DESCRIPCION',
        #'SNOMED',
        'DATA EXTRACCIÓN'   ] 
    
    print(len(lista_defi))
    df = df.reindex(columns= lista_defi)
    df = df.dropna(axis=1, how='all')
    df_fix = df
    return df_fix

def chosen_activities(df):
    # Lista de actividades a conservar 
    actividades_conservar = [ 
        'Dispensación medicamento ATC' , #! AÑADIR SUS ATRIBUTOS DE ALGUNA MANERA
        'Urgencias Hospitalarias' ,
        'Asistencia a punto de Atención Continuada' ,
        'Visita médico familia' ,
        #'Inicio episodio' , #! No tener en cuenta
        'Ingreso hospitalario' , #? crear actividad doble
        'Alta Hospitalizacion' ,
        #'Cita prescripcion' , #! incluir citas de caracter de Colón y booleano de VDR
        'Cita' ,
        #'Año Nacimiento Paciente' , #? atributo
        'Entrada Lista de Espera' , 
        'Salida Lista de Espera' ,
        'Cirugia' ,
        'Quimioterapia' , #! Entender que es (preguntarle a Ismael)
        'Defunción' ,
        'Biopsia diagnóstica' , #! crear actividad doble
  
    ]
    print('\nEsto es la parte de eliminar actividades')
    # Filtrar el DataFrame para conservar solo las filas con las actividades especificadas
    df = df[df['Actividad'].isin(actividades_conservar)]
    #! Por ahora vamos a dejar todos los atributos
    df = chosen_atributtes(df)
    return df



def activity_traduction(df):
    diccionario_actividades = {
        'Data dispensación' :'Dispensación medicamento ATC' ,
        'Data atención' :'Urgencias Hospitalarias' ,
        'Día asistencia':'Asistencia a punto de Atención Continuada' ,
        'Data consulta':'Visita médico familia' ,
        'Data inicio episodio':'Inicio episodio' ,
        'Data ingreso' :'Ingreso hospitalario' , #? crear actividad doble
        'Data de alta' :'Alta Hospitalizacion' ,
        'Data prescrición' :'Cita prescripcion' ,
        'Data cita' :'Cita' ,
        'Día da semana da cita':'Día semana' , #! eliminar
        'Ano':'Año Nacimiento Paciente' , #? atributo
        'Data inclusión' :'Entrada Lista de Espera' , 
        'Data remate' :'Salida Lista de Espera' ,
        'Data intervención' :'Cirugia' ,
        'Dispensación farmacia onlolóxica' :'Quimioterapia' ,
        'Data defunción 01/01/AAAA' :'Defunción' ,
        'DATA EXTRACCIÓN' :'APA eliminar' , #!Eliminar
        'DATA ENTRADA' :'Biopsia diagnóstica' , #! crear actividad doble
        'DATA SALE': 'Emisión informe Laboratorio'
    }
    len(diccionario_actividades)

    # Aplicar la traducción utilizando el método replace de pandas
    df['Actividad'] = df['Actividad'].replace(diccionario_actividades)
    print('\nEsto es la parte de renombrar actividades')
    print(df['Actividad'].unique())
    #! Por ahora vamos a dejar todas las actividades
    df = chosen_activities(df)
    return df

def colon_configuration_apply(df):
    df_fix = activity_traduction(df)
    return df_fix













