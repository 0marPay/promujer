import pandas as pd
import mysql.connector
from hubspot import insert, get_data_from_sql, establecer_conexion, to_uderscore, create2, delete_table
import numpy as np
from unidecode import unidecode


def get_error_columns(row):
    if row.isna().any():
        error_columns = df.columns[row.isna()].tolist()
    else:
        error_columns = []
    return error_columns

if __name__ == "__main__":
    
    # Establecer la conexión a la base de datos
    connection = establecer_conexion(database="emprende_db_testing")
    
    try:       
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
        delete_table("usuario_hubspot", connection)
        create2(connection, columns, "usuario_hubspot")
        df = get_data_from_sql(connection, table_name="usuario_hubspot_raw")
        df = df.astype(str)
        cursor = connection.cursor()
        cursor.execute("ALTER TABLE usuario_hubspot MODIFY COLUMN Nombres VARCHAR(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;")
        cursor.execute("ALTER TABLE usuario_hubspot MODIFY COLUMN Apellidos VARCHAR(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;")
        connection.commit()
        
        # Validación 1: Cambiar registros con Status "CREADO" a "NUEVO"
        df.loc[df['Status'] == 'CREADO', 'Status'] = 'NUEVO'

        # Validación 2: Eliminar registros con todos los valores en Nombres, Apellidos, CorreoElectronico y FechaDeNacimiento como nulos
        df = df.dropna(subset=['Nombres', 'Apellidos', 'CorreoElectronico', 'FechaDeNacimiento'], how='all')

        # Validación 3: Cambiar el Status a "ERROR" para registros donde las primeras dos letras de Nombres no son letras
        df.loc[~df['Nombres'].str[:2].str.isalpha(), 'Status'] = 'ERROR'

        # Validación 4: Cambiar el Status a "ERROR" para registros donde las primeras dos letras de ApellidoPrimeros2 no son letras
        df.loc[~df['ApellidoPrimeros2'].str[:2].str.isalpha(), 'Status'] = 'ERROR'

        # Validación 5: Cambiar el Status a "ERROR" para registros donde las últimas dos letras de ApellidoPrimeros2 no son letras
        df.loc[~df['ApellidoPrimeros2'].str[-2:].str.isalpha(), 'Status'] = 'ERROR'

        # Crear una nueva columna 'NuevaColumna' con la combinación de los valores
        paises = {
            'Argentina': 'ARG',
            'argentina': 'ARG',
            'Bolivia': 'BOL',
            'bolivia': 'BOL',
            'Brasil': 'BRA',
            'brasil': 'BRA',
            'Chile': 'CHL',
            'chile': 'CHL',
            'Colombia': 'COL',
            'colombia': 'COL',
            'Costa Rica': 'CRI',
            'costa rica': 'CRI',
            'Cuba': 'CUB',
            'cuba': 'CUB',
            'Ecuador': 'ECU',
            'ecuador': 'ECU',
            'El Salvador': 'SLV',
            'el salvador': 'SLV',
            'Guatemala': 'GTM',
            'guatemala': 'GTM',
            'Honduras': 'HND',
            'honduras': 'HND',
            'México': 'MEX',
            'mexico': 'MEX',
            'Nicaragua': 'NIC',
            'nicaragua': 'NIC',
            'Panamá': 'PAN',
            'panama': 'PAN',
            'Paraguay': 'PRY',
            'paraguay': 'PRY',
            'Perú': 'PER',
            'peru': 'PER',
            'República Dominicana': 'DOM',
            'republica dominicana': 'DOM',
            'Uruguay': 'URY',
            'uruguay': 'URY',
            'Venezuela': 'VEN',
            'venezuela': 'VEN',
            'Antigua y Barbuda': 'ATG',
            'antigua y barbuda': 'ATG',
            'Aruba': 'ABW',
            'aruba': 'ABW',
            'Guayana': 'GUY',
            'guayana': 'GUY',
            'Puerto Rico': 'PRI',
            'puerto rico': 'PRI'
        }
        
        df['CodigoPais'] = df['Pais'].map(paises).fillna(np.nan)
        df["IdEmprende"] = df['CodigoPais'] + df['NombrePrimeros2'] + df['FechaNacimientoDDMMYY'] + df['ApellidoPrimeros2'] + df['ApellidoUltimos2']   
        # Verificar si alguno de los valores en las columnas es NaN
        df['Status'] = np.where(df['IdEmprende'].isnull(), 'ERROR', 'NUEVO')
        df['Error'] = df.apply(get_error_columns, axis=1)

        columns_names = list(columns.keys())
        df = df.astype(str)
        data = df.loc[:, columns_names]
        print(data.head(5))
        data.reset_index(drop=True, inplace=True)
        insert(connection, columns, data, name_tabla = "usuario_hubspot", sort_by='RecordId', bunch_size=10000)
            
    except Exception as e:
        print("Error:", e)
        
    # Cerrar la conexión
    connection.close()











