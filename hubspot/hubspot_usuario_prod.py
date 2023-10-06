import pandas as pd
import mysql.connector
from hubspot import insert, get_data_from_sql, establecer_conexion, to_uderscore, create2, delete_table
import numpy as np
from unidecode import unidecode


def establecer_conexion_prod():
    connection = mysql.connector.connect(
        host='emprendesrv2.mariadb.database.azure.com',
        user='emprendeuser@emprendesrv2',
        password='@5THrrE5z9Z7bJ',
        port=3306,
        database="emprende_db"
    )
    return connection

def get_error_columns(row):
    if row.isna().any():
        error_columns = df.columns[row.isna()].tolist()
    else:
        error_columns = []
    return error_columns

if __name__ == "__main__":
    
    # Establecer la conexión a la base de datos
    connection = establecer_conexion(database="emprende_db_testing")
    connection_prod = establecer_conexion_prod()
    
    try:    
        # Creamos la tabla en prod   
        columns = {
            "RecordId":{"index":0,"type":"INT"},
            "Nombres":{"index":1,"type":"VARCHAR(100)"},
            "Apellidos":{"index":2,"type":"VARCHAR(100)"},
            "Pais":{"index":3,"type":"VARCHAR(20)"},
            "FechaDeNacimiento":{"index":4,"type":"DATE"},
            "CorreoElectronico":{"index":5,"type":"VARCHAR(100)"},
            "Celular":{"index":6,"type":"VARCHAR(20)"},
            "NombrePrimeros2":{"index":7,"type":"VARCHAR(20)"},
            "ApellidoPrimeros2":{"index":8,"type":"VARCHAR(20)"},
            "ApellidoUltimos2":{"index":9,"type":"VARCHAR(20)"},
            "FechaNacimientoDDMMYY":{"index":10,"type":"VARCHAR(10)"},
            "IdEmprende":{"index":11,"type":"VARCHAR(30)"},
            "Status":{"index":12,"type":"VARCHAR(100)"},
            "Error":{"index":13,"type":"VARCHAR(250)"},
            "Origen":{"index":14,"type":"VARCHAR(250)"}
        }
        delete_table("usuario_hubspot", connection_prod)
        create2(connection_prod, columns, "usuario_hubspot")
        cursor = connection_prod.cursor()
        cursor.execute("ALTER TABLE usuario_hubspot MODIFY COLUMN Nombres VARCHAR(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;")
        cursor.execute("ALTER TABLE usuario_hubspot MODIFY COLUMN Apellidos VARCHAR(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;")
        connection.commit()
        
        # obtenemos la info de la base de datos anterior
        data = get_data_from_sql(connection, table_name="usuario_hubspot")
        data = data.astype(str)
        
        # guardamos los nuevos datos en la nueva base de datos de prod
        insert(connection_prod, columns, data, name_tabla = "usuario_hubspot", sort_by='RecordId', bunch_size=10000)
            
    except Exception as e:
        print("Error:", e)
        
    # Cerrar la conexión
    connection.close()











