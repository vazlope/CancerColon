import pandas as pd
from datetime import datetime
#from DataScience.Utils import utils_ribera as utils
version = 'V3_ConProtocoloyMedic'
path_minado = "Data/mined_data/trace_activities.csv"
path_orig = "Data/processed_data/RiberaSalud_"+str(version)+".csv"

def get_df(path):
    df = pd.read_csv(path, sep = ';')
    posology = pd.read_csv('Data/raw_data/Pantalla_Mantenimiento_Protocolos_V1.csv', sep = ';')
    return df, posology

def posology_analysis(df):
    cond1 = df['Actividad'] == 'Administración de Medicamentos'
    cond2 = df['CodPaciente'] == 'ZMMG590947108001'

    df_filter = df.loc[cond2]
    df_filter = df_filter[['CodPaciente', 'Actividad','FECHA', 'FECHA_FIN','PREOPCodPRotocolo', 'ADCodAdministrado', 'POSIdEspecialidad', 'POSDescMedicamento']]
    print(df_filter.head(100))

def compliance_posology(df, posology):
    grouped = df.groupby('CodPaciente')
    trazas_cumplen = []
    trazas_no_cumplen = []
    trazas_medicamentos = []
    medicamentos_cumplen_uno = 0
    medicamentos_cumplen_multiple = 0
    posologia_cumplen_uno = 0
    posologia_cumplen_multiple = 0
    posologia_general_uno = 0
    posologia_general_multiple = 0
    no_cumplen_uno = 0
    no_cumplen_multiple = 0
    chavon = 0
    protocolos_dicc = {}
    for name, traza in grouped: #para cada traza
        #De los que tienen administracion de medicamentos:
        condi_1 = 'Administración de Medicamentos' in traza['Actividad'].tolist()
        condi_2 = 'Cirugia' in traza['Actividad'].tolist()
        if condi_1 and condi_2:
            trazas_medicamentos.append(name)
            id_protoco = int(traza['PREOPCodPRotocolo'].dropna().unique().tolist()[0])
            posology_trace = posology[posology['IdProtocolo'] == id_protoco]
            #print('\n',name)
            #print('Antes filtrado',posology_trace[['IdProtocolo', 'POSDiaInicial', 'POSIdEspecialidad', 'POSDescMedicamento']])
            cond1 = "POSDiaInicial == 0"
            cond2 = "POSEliminado == 0"
            posology_trace = posology_trace.query(cond1 and cond2)
            #print('Despues filtrado',posology_trace[['IdProtocolo', 'POSDiaInicial', 'POSIdEspecialidad', 'POSDescMedicamento']])
            posology_trace = posology_trace['POSIdEspecialidad'].dropna().unique().tolist()
            posologia = set(posology_trace)
            medicament_trace = traza['ADCodAdministrado'].dropna().unique().tolist()
            medicament_trace = list(map(int, medicament_trace))
            administrado = set(medicament_trace)
            
            # Encontrar los elementos que están en una lista pero no en la otra
            valores_no_comunes = administrado.symmetric_difference(posologia)
            # Si hay valores no comunes, imprimirlos
            if  valores_no_comunes: 
                trazas_no_cumplen.append(name)
                # print('\nNocumple:')
                # print('ID traza: ',name)
                # print('id protocolo: ', id_protoco)
                # print('Valores no comunes',valores_no_comunes)
                # print('\n')
                if len(posology_trace) <=1:
                    no_cumplen_uno +=1
                else:
                    no_cumplen_multiple +=1
            else:
                trazas_cumplen.append(name)
                # print('\ncumple:')
                # print('ID traza: ',name)
                # print('id protocolo: ', id_protoco)
                # print('medicamentos protocolo: ', posology_trace)
                # print('medicamentos administrados: ', medicament_trace)
                if len(medicament_trace) <=1:
                    medicamentos_cumplen_uno +=1
                else:
                    medicamentos_cumplen_multiple +=1 
                    chavon = name
                if len(posology_trace) <=1:
                    posologia_cumplen_uno +=1
                else:
                    posologia_cumplen_multiple +=1
                print('\n')
            
            if len(posology_trace) <=1:
                posologia_general_uno +=1
            else:
                posologia_general_multiple +=1


    print(f'\nPacientes con administración de medicamentos: {len(trazas_medicamentos)}')        
    print(f'Pacientes que cumplen: {len(trazas_cumplen)}')
    print(f'Pacientes que no cumplen: {len(trazas_no_cumplen)}')
    print('\n')
    print(f"Pacientes con protocolo asociado: {df['CodPaciente'].nunique()}")
    print(f'Pacientes con protocolo que su medicamento estipulado era solo uno: {posologia_general_uno}')
    print(f'Pacientes con protocolo que su medicamento estipulado era más de uno: {posologia_general_multiple}')
    print('\n')
    num_protocolos = df['PREOPCodPRotocolo'].dropna().nunique()
    protocolos = df['PREOPCodPRotocolo'].dropna().unique()
    print(f'Numero de protocolos totales asociados a los pacientes de que disponemos -> {num_protocolos} protocolos distintos:\n{protocolos}\n')
    for protocolo in protocolos:
        posologia_comprobacion = posology.loc[posology['IdProtocolo'] == protocolo]
        cond1 = "POSDiaInicial == 0"
        cond2 = "POSEliminado == 0"
        posologia_comprobacion = posologia_comprobacion.query(cond1 and cond2)
        protocolos_dicc[protocolo] = posologia_comprobacion['POSIdEspecialidad'].dropna().unique()
        
    for i,j in zip(protocolos_dicc.keys(), protocolos_dicc.values()):
        print(f'El procolo {i} tiene {j} medicamentos distintos')
    print('\n')
    print(f'Pacientes con medicamentos que CUMPLEN y su medicamento administrado era solo uno: {medicamentos_cumplen_uno}')
    print(f'Pacientes con protocolo que CUMPLEN y su medicamento administrado era solo uno: {posologia_cumplen_uno}')
    print(f'Pacientes con protocolo que NO CUMPLEN y su medicamento administrado era solo uno: {no_cumplen_uno}')
    print('\n')
    print(f'Pacientes con medicamentos que CUMPLEN y su medicamento administrado era más de uno: {medicamentos_cumplen_multiple} y su ID es {chavon}')
    print(f'Pacientes con protocolo que CUMPLEN y su medicamento administrado era más de uno: {posologia_cumplen_multiple}')
    print(f'Pacientes con protocolo que NO CUMPLEN y su medicamento administrado era más de uno: {no_cumplen_multiple}\n')

    return trazas_cumplen, trazas_no_cumplen, trazas_medicamentos


if __name__ == "__main__":
    df, posology = get_df(path_orig)
    trazas_cumplen, trazas_no_cumplen, trazas_medicamentos = compliance_posology(df, posology)
    #print(compliance_protocol_df.groupby('Medicamento_administrado')['CodPaciente'].nunique())