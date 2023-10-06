from datetime import datetime
import pandas as pd 
import mysql.connector
import math

ruta_archivo = "errores.txt"

def establecer_conexion(database):
    connection = mysql.connector.connect(
        host='emprendesrv2.mariadb.database.azure.com',
        user='emprendeuser@emprendesrv2',
        password='@5THrrE5z9Z7bJ',
        port=3306,
        database=database
    )
    return connection

def actualizar_registros(connection, tabla, df):
    # Crear cursor para ejecutar consultas SQL
    cursor = connection.cursor()

    # Obtener los datos de usuario_id y membresia_id desde el DataFrame df
    registros = []
    for index, row in df.iterrows():
        usuario_id = row['id']
        membresia_id = row['Programa_id']
        registros.append((usuario_id, membresia_id))

    # Verificar si los registros existen en la tabla
    consulta = f"SELECT usuario_id FROM {tabla} WHERE usuario_id IN (%s)"
    placeholders = ', '.join(['%s'] * len(registros))
    consulta = consulta % placeholders
    cursor.execute(consulta, [r[0] for r in registros])
    registros_existentes = cursor.fetchall()
    print(len(registros_existentes))

    # Actualizar los registros existentes
    for registro in registros_existentes:
        usuario_id = registro[0]
        membresia_id = next(r[1] for r in registros if r[0] == usuario_id)
        fecha_modificacion = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        consulta = f"UPDATE {tabla} SET membresia_id = %s, fechaModificacion = %s WHERE usuario_id = %s"
        cursor.execute(consulta, (membresia_id, fecha_modificacion, usuario_id))
        connection.commit()
        print(f"Registro {usuario_id} actualizado exitosamente.")

    # Obtener los registros que no existen en la tabla
    registros_nuevos = [r for r in registros if r[0] not in [reg[0] for reg in registros_existentes]]

    # Generar los nuevos registros
    fecha_alta = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    consulta = f"INSERT INTO {tabla} (usuario_id, membresia_id, fechaAlta) VALUES (%s, %s, %s)"
    cursor.executemany(consulta, [(r[0], r[1], fecha_alta) for r in registros_nuevos])
    connection.commit()
    print(f"Se generaron {len(registros_nuevos)} nuevos registros.")

    # Cerrar cursor
    cursor.close()

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

def insert(connection, columnas, data, name_tabla, bunch_size = 100):
    cursor = connection.cursor()
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
        for _, row in data_i.iterrows():
            row = str(tuple(row))
            row = row.replace("'nan'", "NULL")
            row = row.replace(", nan)", ", NULL)")
            row = row.replace(", nan,", ", NULL,")
            row = row.replace(", nan,", ", NULL,")
            row = row.replace("'None'", "NULL")
            row = row.replace("'NaT'", "NULL")
            insert_query += row + ", "
            start += 1
        insert_query = insert_query[:-2]
        insert_query += ";"
        try:
            cursor.execute(insert_query)
            connection.commit()
            print(i,f)
        except Exception as e:
            print("Error: " + str(e))
            guardar_texto_en_txt("ERROR: " + str(e), ruta_archivo) 
            if "at row " in str(e):
                e = str(e)
                e = e[e.index("at row ") + len("at row "):len(e)]
                e = int(e)
                print("Error en fila: " + str(i + e))
            guardar_texto_en_txt("ERROR: " + str(e), ruta_archivo) 
            guardar_texto_en_txt("QUERY: " + insert_query, ruta_archivo) 

def guardar_texto_en_txt(texto, ruta_archivo):
    with open(ruta_archivo, 'a', encoding='utf-8') as archivo:
        archivo.write(texto + '\n')

connection = establecer_conexion("emprende_db")

folder_path = "queries_usuarias"
file = "archivo_final.csv"
print(file)
file = folder_path + '/' + file
data = pd.read_csv(file, encoding='latin-1')
# ind_1 = 24300
# ind_2 = ind_1 + 100
# data = data.loc[ind_1:ind_2]

print(data.head(5)); print()

table_name = "bases_para_15"
columnas = ["correo","dni","nombre","apellido","pais","telefono"]
create(connection, columnas, table_name)
insert(connection, columnas, data, table_name)

connection.close()


