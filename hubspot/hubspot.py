import pandas as pd
import mysql.connector
import re
import unicodedata
import json 
import numpy as np
import math

# configuracin de datos db
max_caracters = 64
ruta_archivo = "errores.txt"

def establecer_conexion(database):
    connection = mysql.connector.connect(
        host='20.10.9.227',
        user='emprende_dev',
        password='3mpr3nd3Pr0Muj3r.2023',
        port=3306,
        database=database
    )
    return connection

def insert(connection, columnas, data, name_tabla, sort_by = 'Record ID', bunch_size = 100, previous_index=0):
    cursor = connection.cursor()
    # data = data.sort_values(sort_by)
    start = 0
    data = data.iloc[start:data.shape[0]]
    print("inserting...")
    for i in range(math.ceil(data.shape[0]/bunch_size)):
        i = i * bunch_size
        f = i + bunch_size
        if f > data.shape[0]:
            f = data.shape[0]
        data_i = data.iloc[i:f]
        
        insert_query = "INSERT INTO " 
        insert_query += f"{name_tabla} ("
        for key in columnas:
            insert_query += key + ", "
        insert_query = insert_query[:-2]
        insert_query += ") VALUES "    
        insert_query_full = str(insert_query)
        for _, row in data_i.iterrows():
            insert_query_full += obtener_buena_fila(row)
            start += 1
        
        insert_query_full = insert_query_full[:-2] + ";"
        try:
            cursor.execute(insert_query_full)
            connection.commit()
            print(previous_index+i,previous_index+f)
        except Exception as e: 
            try:
                print("Error: " + str(e))
                guardar_texto_en_txt("ERROR: " + str(e), ruta_archivo)
                if "at row " in str(e):
                    e = str(e)
                    e = e[e.index("at row ") + len("at row "):len(e)]
                    e = int(e) + previous_index
                    print("Error en fila: " + str(i + e))
                    e = e-1
                    problema = data_i.iloc[e]
                    guardar_texto_en_txt("QUERY: " + insert_query + obtener_buena_fila(problema)[:-2] + ";", ruta_archivo) 
                    _id = problema[sort_by]
                    data_i = data_i.query(f"{sort_by} != '{_id}'")
                    # print(str(tuple(data_i.iloc[e])))
                    # data_i = data_i[data_i.index != e]
                    # print(str(tuple(data_i.iloc[e])))
                    insert(connection, columnas, data_i, name_tabla, sort_by, int(bunch_size/1), previous_index=i)
            except Exception as e: 
                print("Error: " + str(e))

def get_data_for_create(data):
    
    columnas = data.columns.tolist()
    
    ccolumnas = clean_columnas(columnas)
    good_columnas = keeping_good(ccolumnas)
    
    print(json.dumps(good_columnas))
          
    return good_columnas

def create(connection, columnas, name_tabla):
    is_created = True
    cursor = connection.cursor()
    
    # Crear la sentencia CREATE TABLE
    create_table_query = f"CREATE TABLE {name_tabla} ("

    for i, columna in enumerate(columnas):
        
        primary_key = ""
        if i == 0:
            primary_key = " PRIMARY KEY"
            
        create_table_query += f"{columna} VARCHAR(100){primary_key}, "

    create_table_query = create_table_query.rstrip(", ") + ")"

    try:
        # Ejecutar la sentencia CREATE TABLE
        cursor.execute(create_table_query)
    except Exception as e:
        is_created = False
        print ("Error: " + str(e))
    
    connection.commit()
    return is_created

def create2(connection, columnas, name_tabla):
    
    cursor = connection.cursor()
    is_created = True
    # Crear la sentencia CREATE TABLE
    create_table_query = f"CREATE TABLE {name_tabla} ("

    for k in columnas.keys():
        
        v = columnas[k]
        columna = k
        data_type = v["type"]
        i = v["index"]
        
        primary_key = ""
        if i == 0:
            primary_key = " PRIMARY KEY"
            
        create_table_query += f"{columna} {data_type}{primary_key}, "

    create_table_query = create_table_query.rstrip(", ") + ")"

    try:
        # Ejecutar la sentencia CREATE TABLE
        cursor.execute(create_table_query)
    except Exception as e:
        is_created = False
        print ("Error al crear tabla: " + str(e))
        
    connection.commit()
    return is_created

def keeping_good(columns):
    good = ["RecordId"]
    good += ["Correo", "Celular", "Telefono", "Curp", "Numero"]
    good += ["Email", "Cell", "Telephone", "Dni", "Number"]
    good += ["Pais", "Nombre", "Apellido","Nacimiento"]
    good += ["Contry", "Name", "Lastname", "Surname", "Birthdate"]
    good += ["Rut", "Rfc"] # RUT es el documento de identidad nacional en chile
    
    diccionario = {e:i for i,e in enumerate(columns) if any(g in e for g in good)}
    
    # Eliminar los pares llave/valor con valor mayor o igual a 64
    diccionario = {k: v for k, v in diccionario.items() if len(k) < 50}

    # Identificar valores que ya aparecen anteriormente al convertirlos a minúsculas
    
    valores = list(diccionario.keys())  
    valores_lower = [valor.lower() for valor in valores]
    valores = set([valores[i] for i in range(len(valores)) if valores_lower.count(valores_lower[i]) <= 1])
    
    return {k:diccionario[k] for k in valores}

def remove_symbols(text):
    # Eliminar símbolos y caracteres especiales excepto espacios
    cleaned_text = re.sub(r'[^\w\s]', '', text)
    return cleaned_text

def remove_accents(text):
    # Remover acentos y diacríticos
    normalized_text = unicodedata.normalize('NFKD', text)
    cleaned_text = ''.join(c for c in normalized_text if not unicodedata.combining(c))
    return cleaned_text

def eliminar_numeros(text):
    return re.sub(r'\d+', '', text)

def camel_case(text):
    # Eliminar símbolos y convertir a formato CamelCase
    cleaned_text = remove_symbols(text)
    cleaned_text = remove_accents(cleaned_text)
    words = cleaned_text.split()
    camel_case_text = ''.join(word.title() for word in words)
    return camel_case_text

def clean_colum_name(text):
    text = eliminar_numeros(text)
    return camel_case(text)[0:max_caracters]
    
def clean_columnas(columnas):
    clean_columnas = []
    repetitions = {}
    for columna in columnas:
        clean_columna = clean_colum_name(columna)
        if clean_columna in clean_columnas:
            
            if clean_columna in repetitions:
                repetitions[clean_columna] = repetitions[clean_columna] + 1
                 
            else:
                repetitions[clean_columna] = 2
            
            i = repetitions[clean_columna]
                
            clean_columna = clean_columna[:-1] + str(i) if len(clean_columna) == 64 else clean_columna + str(i)
        
        clean_columnas.append(clean_columna)

    return clean_columnas

def obtener_longitud_maxima(df):
    # Obtener la longitud de los caracteres en cada columna del DataFrame
    longitudes = df.applymap(lambda x: len(str(x)))

    # Encontrar el valor máximo de todas las longitudes
    longitud_maxima = longitudes.max().max()

    return longitud_maxima

def fix_dates(df, indice_columna):
    # Obtener lista de nombres de columnas
    nombres_columnas = df.columns.tolist()

    # Obtener nombre de columna por índice
    nombre_columna = nombres_columnas[indice_columna]
    
    # Longitud máxima esperada para la columna
    longitud_maxima = 10

    # Reemplazar los valores que exceden la longitud máxima por NaN
    df[nombre_columna] = np.where(df[nombre_columna].str.len() > longitud_maxima, np.nan, df[nombre_columna])

    return df

def get_data_from_sql(connection, table_name):
    # Sentencia SQL para obtener todos los registros de la tabla
    sql_query = f"SELECT * FROM {table_name}"
    # Cargar los registros en un DataFrame
    df = pd.read_sql(sql_query, connection)
    
    return df

def rellenar_nulos(df, col1, col2):
    # Verificar si la columna "Nombre" tiene valores nulos
    nulos = df[col1].isnull()
    
    # Copiar valores de la columna "Nombre2" en la columna "Nombre" donde sea necesario
    df.loc[nulos, col1] = df.loc[nulos, col2]
    
    return df

def borrar_columnas_nan(df):
    df = df.dropna(axis=1, how='all')
    return df

def ordenar_columnas(df, columna):
    columnas_nombre = [col for col in df.columns if columna in col]
    columnas_nulos = df[columnas_nombre].isnull().sum()
    columnas_ordenadas = columnas_nulos.sort_values().index.tolist()
    
    # Renombrar las columnas según el orden y el formato deseado
    nuevos_nombres = {}
    for i, col in enumerate(columnas_ordenadas):
        i = "" if i == 0 else i
        nuevo_nombre = f"{columna}{i}" if columna in col else col
        nuevos_nombres[col] = nuevo_nombre
    
    df = df.rename(columns=nuevos_nombres)
    df = df.reindex(columns=columnas_ordenadas + [col for col in df.columns if col not in columnas_ordenadas])
    df = borrar_columnas_nan(df)
    return df

def guardar_texto_en_txt(texto, ruta_archivo):
    with open(ruta_archivo, 'a', encoding='utf-8') as archivo:
        archivo.write(texto + '\n')

def to_uderscore(nombre, added):
    max_len = 64 - len(added)
    nombre = re.sub(r'\.csv$', '', nombre)
    nombre = re.sub(r'\s|-', '_', nombre)
    nombre = nombre[0:max_len] if len(nombre) > max_len else nombre
    return nombre

def delete_table(table_name, connection):
    cursor = connection.cursor()
    sql_query = f"DROP TABLE {table_name}"
    try:
        cursor.execute(sql_query)
        print("La tabla se eliminó correctamente.")
    except mysql.connector.Error as error:
        print("Error al eliminar la tabla:", error)
    finally:
        cursor.close()

def actualizar_registros(connection, tabla, cantidad):
    # Crear un objeto cursor
    cursor = connection.cursor()
    columna = "RecordId"

    try:
        # Sentencia SQL para obtener todos los registros de la tabla
        sql_query = f"SELECT * FROM {tabla}"
        
        # Cargar los registros en un DataFrame
        registro = pd.read_sql(sql_query, connection)
        
        # Actualizar los valores en la columna "RecordId"
        registro[columna] = registro[columna] + cantidad
        
        # Generar la sentencia SQL de actualización en lote
        sql_update = f"UPDATE {tabla} SET {columna} = %s WHERE {columna} = %s"
        
        # Obtener los valores actualizados y los valores originales como tuplas
        valores_actualizados = tuple(registro[columna])
        valores_originales = tuple(registro.index)
        
        # Ejecutar la sentencia SQL de actualización en lote
        cursor.executemany(sql_update, zip(valores_actualizados, valores_originales))
        
        # Confirmar los cambios en la base de datos
        connection.commit()
        
        print("Los registros se actualizaron correctamente.")
    except mysql.connector.Error as error:
        print("Error al actualizar los registros:", error)
    finally:
        # Cerrar el cursor
        cursor.close()

def get_error_columns(row):
    if row.isna().any():
        error_columns = df.columns[row.isna()].tolist()
    else:
        error_columns = []
    return error_columns

def obtener_buena_fila(row):
    row = str(tuple(row))
    row = row.replace("'nan'", "NULL")
    row = row.replace(", nan)", ", NULL)")
    row = row.replace(", nan,", ", NULL,")
    row = row.replace(", nan,", ", NULL,")
    row = row.replace("'None'", "NULL")
    row = row.replace("'NaT'", "NULL")
    return row + ", "