from datetime import datetime
import pandas as pd 
import mysql.connector
from carga_datos import filtrar_registros 

def establecer_conexion(database):
    connection = mysql.connector.connect(
        host='emprendesrv2.mariadb.database.azure.com',
        user='emprendeuser@emprendesrv2',
        password='@5THrrE5z9Z7bJ',
        port=3306,
        database=database
    )
    return connection

def get_data_from_sql(connection, table_name):
    # Sentencia SQL para obtener todos los registros de la tabla
    sql_query = f"SELECT * FROM {table_name}"
    # Cargar los registros en un DataFrame
    df = pd.read_sql(sql_query, connection)
    
    return df

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
    # for registro in registros_existentes:
    #     usuario_id = registro[0]
    #     membresia_id = next(r[1] for r in registros if r[0] == usuario_id)
    #     fecha_modificacion = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    #     consulta = f"UPDATE {tabla} SET membresia_id = %s, fechaModificacion = %s WHERE usuario_id = %s"
    #     cursor.execute(consulta, (membresia_id, fecha_modificacion, usuario_id))
    #     connection.commit()
    #     print(f"Registro {usuario_id} actualizado exitosamente.")

    # Obtener los registros que no existen en la tabla
    # registros_nuevos = [r for r in registros if r[0] not in [reg[0] for reg in registros_existentes]]

    # # Generar los nuevos registros
    # fecha_alta = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    # consulta = f"INSERT INTO {tabla} (usuario_id, membresia_id, fechaAlta) VALUES (%s, %s, %s)"
    # cursor.executemany(consulta, [(r[0], r[1], fecha_alta) for r in registros_nuevos])
    # connection.commit()
    # print(f"Se generaron {len(registros_nuevos)} nuevos registros.")

    # Cerrar cursor
    cursor.close()

# connection = establecer_conexion("emprende_db")

folder_path = "carga_datos"
file = "total_registros.csv"
print("Registros")
file = folder_path + '/' + file
data = pd.read_csv(file, encoding='latin-1')
filtrar_registros(data, "Programa", [], df_problemas, folder_path)
print(data.head(5)); print()



# Supongamos que tienes el DataFrame df_nuevo con los datos de usuario_id y membresia_id
actualizar_registros(connection, tabla="usuario_membresia", df=nuevo_df)

# connection.close()


