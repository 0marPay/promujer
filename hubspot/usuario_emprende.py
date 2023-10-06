import pandas as pd
import mysql.connector
from hubspot import insert, get_data_from_sql, delete_table, create2
import numpy as np
from unidecode import unidecode

def establecer_conexion(database):
    connection = mysql.connector.connect(
        host='emprendesrv2.mariadb.database.azure.com',
        user='emprendeuser@emprendesrv2',
        password='@5THrrE5z9Z7bJ',
        port=3306,
        database=database
    )
    return connection

def get_error_columns(row, columns_to_check):
    error_columns = []
    for column in columns_to_check:
        if row[column] is None or pd.isna(row[column]) or row[column] == "":
            error_columns.append(column)
    return error_columns

def get_primeros_dos(x):
    if pd.notna(x) and len(x) >= 2:
        return unidecode(x).upper()[:2]
    else:
        return None

def get_ultimos_dos(x):
    if pd.notna(x) and len(x) >= 2:
        return unidecode(x).upper()[-2:]
    else:
        return None

if __name__ == "__main__":
    
    
    # Establecer la conexión a la base de datos
    connection = establecer_conexion(database="emprende_db")
            
    try:        
        columns = {
            "Id":{"index":0,"type":"INT"},
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
            "IdEmprende":{"index":11,"type":"VARCHAR(20)"},
            "Codigo":{"index":12,"type":"VARCHAR(100)"},
            "Tag":{"index":13,"type":"VARCHAR(500)"},
            "Status":{"index":14,"type":"VARCHAR(100)"},
            "Error":{"index":15,"type":"VARCHAR(250)"}
        }
        
        delete_table("usuario_emprende", connection)
        create2(connection, columns, "usuario_emprende")
        
        df = get_data_from_sql(connection, table_name="usuario_emprende_previo")
        df = df.astype(str)

        nombre_primeros_dos = df['nombre'].apply(get_primeros_dos)
        apellido_primeros_dos = df['apellido'].apply(get_primeros_dos)
        apellido_ultimos_dos = df['apellido'].apply(get_ultimos_dos)
        
        # Convertir la columna de fechas al formato datetime
        df['FechaDeNacimiento'] = pd.to_datetime(df['fechaNacimiento'], format='%Y-%m-%d', errors='coerce')
        # Crear una nueva columna 'NuevaColumna' con la combinación de los valores
        
        df["Id"] = df["id"]
        df["Nombres"] = df["nombre"]
        df["Apellidos"] = df["apellido"]
        df["Pais"] = df["nacionalidad"]
        df["CorreoElectronico"] = df["mail"]
        df["Celular"] = df["celular"]
        df["Codigo"] = df["codigo"]
        df["Tag"] = df["tag"]
        df['NombrePrimeros2'] = nombre_primeros_dos
        df['ApellidoPrimeros2'] = apellido_primeros_dos
        df['ApellidoUltimos2'] = apellido_ultimos_dos
        df['FechaNacimientoDDMMYY'] = df['FechaDeNacimiento'].dt.strftime('%d%m%y')
        df["IdEmprende"] = df['Pais'] + df['NombrePrimeros2'] + df['FechaNacimientoDDMMYY'] + df['ApellidoPrimeros2'] + df['ApellidoUltimos2']   
        df['Status'] = np.where(df['IdEmprende'].isnull(), 'ERROR', 'NUEVO')
        df['Error'] = df.apply(get_error_columns, axis=1, columns_to_check=["Nombres", "Apellidos", "Pais", "FechaDeNacimiento", "NombrePrimeros2", "ApellidoPrimeros2", "ApellidoUltimos2"])

        columns_names = list(columns.keys())
        df = df.astype(str)
        data = df.loc[:, columns_names]
        print(data.head(5))
        data.reset_index(drop=True, inplace=True)
        insert(connection, columns, data, name_tabla = "usuario_emprende", sort_by='Id', bunch_size=10000)
            
    except Exception as e:
        print("Error:", e)
        
    # Cerrar la conexión
    connection.close()











