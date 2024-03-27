import matplotlib.pyplot as plt

def plot_posology_compliance():
    # Datos
    pacientes_admin_medicamentos = 199
    pacientes_cumplen = 97
    pacientes_no_cumplen = 102
    # Calculamos los porcentajes
    porcentaje_cumplen = (pacientes_cumplen / pacientes_admin_medicamentos) * 100
    porcentaje_no_cumplen = (pacientes_no_cumplen / pacientes_admin_medicamentos) * 100
    # Etiquetas y valores para la gráfica
    etiquetas = ['Cumplen\n({})'.format(pacientes_cumplen), 'No Cumplen\n({})'.format(pacientes_no_cumplen)]
    valores = [porcentaje_cumplen, porcentaje_no_cumplen]
    # Colores para las secciones de la gráfica
    colores = ['#66c2a5', '#fc8d62']
    # Crear la gráfica de torta
    plt.figure(figsize=(10, 6))
    plt.pie(valores, labels=etiquetas, autopct='%1.1f%%', colors=colores, startangle=140)
    plt.title('Porcentaje de Pacientes que Cumplen y No Cumplen con la Administración de Medicamentos')
    plt.axis('equal')  # Equal aspect ratio ensures that pie is drawn as a circle.
    plt.show()

def plot_num_medicament():
    pacientes_protocolo_asociado = 199
    pacientes_un_medicamento = 186
    pacientes_mas_de_un_medicamento = 13
    # Calculamos los porcentajes
    porcentaje_un_medicamento = (pacientes_un_medicamento / pacientes_protocolo_asociado) * 100
    porcentaje_mas_de_un_medicamento = (pacientes_mas_de_un_medicamento / pacientes_protocolo_asociado) * 100
    # Etiquetas y valores para la gráfica
    etiquetas = ['Un Medicamento\n({})'.format(pacientes_un_medicamento), 'Más de un Medicamento\n({})'.format(pacientes_mas_de_un_medicamento)]
    valores = [porcentaje_un_medicamento, porcentaje_mas_de_un_medicamento]
    # Colores para las secciones de la gráfica
    colores = ['#66c2a5', '#fc8d62']
    # Crear la gráfica de torta
    plt.figure(figsize=(10, 6))
    plt.pie(valores, labels=etiquetas, autopct='%1.1f%%', colors=colores, startangle=140)
    plt.title('Porcentaje de Pacientes con Protocolo Asociado y admistrado según Cantidad de Medicamentos Estipulados')
    plt.axis('equal')  # Equal aspect ratio ensures that pie is drawn as a circle.
    plt.show()


def bar_chart_protocolos():
    # Datos
    protocolos = ['170001.0', '170002.0', '160008.0', '160006.0', '160007.0', '160005.0', '160013.0', '160009.0', '160010.0']
    num_medicamentos = [5, 6, 1, 1, 1, 1, 1, 2, 1]
    # Ordenar protocolos y num_medicamentos de mayor a menor según num_medicamentos
    protocolos_ordenados, num_medicamentos_ordenados = zip(*sorted(zip(protocolos, num_medicamentos), key=lambda x: x[1], reverse=True))
    # Crear gráfico de barras
    plt.figure(figsize=(10, 6))
    bars = plt.bar(protocolos_ordenados, num_medicamentos_ordenados, color='skyblue')
    # Añadir etiquetas y título
    plt.xlabel('Protocolo')
    plt.ylabel('Número de Medicamentos Unicos')
    plt.title('Número de Medicamentos Unicos por Protocolo (Ordenados)')
    plt.xticks(rotation=45)  # Rotar etiquetas del eje x para mejor visualización
    # Añadir valores en cada barra
    for bar in bars:
        height = bar.get_height()
        plt.text(bar.get_x() + bar.get_width()/2, height, '%d' % int(height), ha='center', va='bottom')
    # Mostrar gráfico
    plt.tight_layout()  # Ajustar diseño para evitar recorte de etiquetas
    plt.show()

def bar_chart_1med():
    # Datos
    categorias = ['Pacientes con protocolo y una medicacion',
                'Pacientes con un medicamento que CUMPLEN',
                'Pacientes con un medicamento que NO CUMPLEN protocolo']
    num_pacientes = [186, 97, 89]
    # Crear gráfico de barras
    plt.figure(figsize=(12,6))
    bars = plt.bar(categorias, num_pacientes, color='skyblue')
    # Añadir etiquetas y título
    plt.xlabel('Categorías')
    plt.ylabel('Número de Pacientes')
    plt.title('Número de Pacientes según Categoría')
    plt.xticks(rotation=45)  # Rotar etiquetas del eje x para mejor visualización
    # Añadir valores en cada barra
    for bar in bars:
        height = bar.get_height()
        plt.text(bar.get_x() + bar.get_width()/2, height, '%d' % int(height), ha='center', va='bottom')
    # Mostrar gráfico
    plt.tight_layout()  # Ajustar diseño para evitar recorte de etiquetas
    plt.show()

import matplotlib.pyplot as plt

def bar_chart_more_than_1_med():
    # Datos
    categorias = ['Pacientes con protocolo que su medicamento estipulado era más de uno',
                  'Pacientes con protocolo que NO CUMPLEN y su medicamento administrado era más de uno']
    num_pacientes = [31, 31]
    # Crear gráfico de barras
    plt.figure(figsize=(8,6))
    bars = plt.bar(categorias, num_pacientes, color='skyblue')
    # Añadir etiquetas y título
    plt.xlabel('Categorías')
    plt.ylabel('Número de Pacientes')
    plt.title('Número de Pacientes con Más de una Medicación según Categoría')
    plt.xticks(rotation=45)  # Rotar etiquetas del eje x para mejor visualización
    # Añadir valores en cada barra
    for bar in bars:
        height = bar.get_height()
        plt.text(bar.get_x() + bar.get_width()/2, height, '%d' % int(height), ha='center', va='bottom')
    # Mostrar gráfico
    plt.tight_layout()  # Ajustar diseño para evitar recorte de etiquetas
    plt.show()


plot_posology_compliance()
plot_num_medicament()
bar_chart_protocolos()
bar_chart_1med()
#bar_chart_more_than_1_med()

