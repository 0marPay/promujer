import pandas as pd 
import numpy as np
import os

def filtrar_registros(df, columna, valores, df_problemas, folder_path):
    # Filtrar los registros donde la columna coincida con los valores especificados
    filtro = df[columna].isin(valores)
    df_malo = df[filtro]
    # Obtener los registros que no cumplen el filtro
    df_bueno = df[~filtro]
    
    if not df_malo.empty:
        df_problemas = df_problemas.append(df_malo[columnas], ignore_index=True)
        file_malo = folder_path + '/' + "malos_registros.csv"
        df_problemas.to_csv(file_malo, index=False)
    
    return df_bueno, df_problemas

folder_path = "carga_datos"
files = [f for f in os.listdir(folder_path) if f.endswith('.csv')]
files.remove("malos_registros.csv")
files.remove("total_registros.csv")
set_programa = set()
# DataFrame vacío con las columnas especificadas
columnas = ['Nombre', 'Correo', 'Telefono', 'DNI', 'Programa']
df_problemas = pd.DataFrame(columns=columnas)
columnas_nuevas = columnas + ['Programa_id']
df_total = pd.DataFrame(columns=columnas_nuevas)
for file in files:
    print("Processing...")
    file = folder_path + '/' + file
    print(file)
    df = pd.read_csv(file, encoding='latin-1')
    
    df, df_problemas = filtrar_registros(df, 'Programa', ['Mejora y fortalece', '0','Entrena'], df_problemas, folder_path)
    df, df_problemas = filtrar_registros(df, 'Correo', [np.nan], df_problemas, folder_path)
    
    # Obtener los tipos de datos únicos en la columna "Programa"
    tipos_datos = df['Programa'].unique().tolist()
    set_programa.update(tipos_datos)
    
    programa_dict = {
        'Expando':3, 
        'Idea':5, 
        'Mejoro':4, 
        'Mejora':4, 
        'Fortalece':2, 
        'FORTALECE':2, 
        'Fortalezco':2, 
        'REASIGNADA IDEA':5, 
        'MEJORA':4, 
        'Ideo':5, 
        'Fortalece II':2,  
        'mejora':4
    }
    '''
    2	Fortalezco mi negocio
    3	Expando mi negocio
    4	Mejoro mi negocio
    5	Ideo
    '''
    df['Programa_id'] = df['Programa'].map(programa_dict).fillna(np.nan)
    print("filas:", df.shape[0])
    print(df.head(5))
    print()
    
    df_total = df_total.append(df[columnas_nuevas], ignore_index=True)
    

print("filas totales:", df_total.shape[0])
file_total = folder_path + '/' + "total_registros.csv"
df_total.to_csv(file_total, index=False)

    
    