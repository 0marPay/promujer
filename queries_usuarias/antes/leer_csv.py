import os
import pandas as pd


def limpiar_df(df):
    df_sorted = df.apply(lambda x: x.isna().sum(), axis=1).sort_values().index
    grouped = df.loc[df_sorted].groupby("correo")
    df_final = grouped.first().reset_index()
    return df_final

# Ruta de la carpeta que contiene los archivos CSV
ruta_carpeta = "queries_usuarias"

# Obtener la lista de archivos CSV en la carpeta
archivos_csv = [archivo for archivo in os.listdir(ruta_carpeta) if archivo.endswith('_2.csv')]

# Crear una lista para almacenar los DataFrames
dataframes = []
# Leer cada archivo CSV y agregarlo a la lista de DataFrames
for archivo_csv in archivos_csv:
    ruta_archivo = os.path.join(ruta_carpeta, archivo_csv)
    df = pd.read_csv(ruta_archivo)
    print(archivo_csv)
    print(df.head(10))
    print("---------------------------------"*3)
    dataframes.append(df)

# Concatenar los DataFrames en uno solo
df = pd.concat(dataframes, ignore_index=True)
df = limpiar_df(df)
df['telefono'] = df['telefono'].replace('', None)
df['telefono'] = df['telefono'].replace('None', pd.NA)
df['telefono'] = pd.to_numeric(df['telefono'], errors='coerce').astype(pd.Int64Dtype())
# Guardar el DataFrame en un nuevo archivo CSV
archivo_csv = ruta_carpeta + "/" + "archivo_final.csv"
print(archivo_csv)
print(df.head(10))
print("---------------------------------"*3)
df.to_csv(archivo_csv, index=False)
