import pandas as pd
import mysql.connector
from hubspot import insert, get_data_from_sql, establecer_conexion, to_uderscore, delete_table, create2
import numpy as np
from unidecode import unidecode


files=[ "Form Inscripcion 4taC - Fortalece Bolivia.csv",
        "Form Inscripcion 4taC - Fortalece Bs.Aires.csv",
        "Form Inscripcion 4taC - Fortalece Centroamerica.csv",
        "Form Inscripcion 4taC - Fortalece Chile.csv",
        "Form Inscripcion 4taC - Fortalece MP MX.csv",
        "Form Inscripcion 4taC - Fortalece SudAmerica.csv",
        "Form Inscripcion 4taC - Fortalece surestemx.csv",
        "Form Inscripcion 4taC - Mejora Bolivia.csv",
        "Form Inscripcion 4taC - Mejora Bs.Aires.csv",
        "Form Inscripcion 4taC - Mejora Centroamerica.csv",
        "Form Inscripcion 4taC - Mejora Chile.csv",
        "Form Inscripcion 4taC - Mejora MP MX.csv",
        "Form Inscripcion 4taC - Mejora SudAmerica.csv",
        "Form Inscripcion 4taC - Mejora surestemx.csv",
        "Formulario Comienza tu Negocio (1).csv",
        "Formulario inscripcion 2022 - Emprende (3ra cohorte) (23 Mayo) (1).csv",
        "Formulario inscripcion 2022 - Emprende PM y Mercado Pago.csv",
        "Formulario inscripcion 2022 - Emprende y MP (3ra cohorte) (1).csv",
        "Formulario inscripcion 2022- Emprende  (3ra cohorte) (1).csv",
        "Formulario Mejora tu negocio (1).csv",
        "Postulantes a la 2ra cohorte (convocatoria abierta).csv",
        "hubspot-crm-exports-emprende-pro-mujer-2023-06-01.csv" ]

if __name__ == "__main__":
    
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
    connection_usuario = establecer_conexion(database="emprende_db_testing")
    delete_table("usuario_hubspot", connection_usuario)
    create2(connection_usuario, columns, "usuario_hubspot")
    
    for file in files:
        # Establecer la conexión a la base de datos
        connection = establecer_conexion(database="hubpspot_db")
        
        print(file)
        file_clean = to_uderscore(file, added="hubspot_clean ()")
        
        try:        
        
            df = get_data_from_sql(connection, table_name=f"`hubspot_clean ({file_clean})`")
            df = df.astype(str)
            # Tomar los primeros dos valores de la columna 
            nombre_primeros_dos = df['Nombre'].apply(lambda x: unidecode(x).upper().split()[0][:2] if x != "None" else None)
            # Tomar los últimos dos valores de la columna 
            apellido_primeros_dos = df['Apellido'].apply(lambda x: unidecode(x).upper().split()[0][:2] if x != "None" else None)
            # Tomar los últimos dos valores de la columna 
            apellido_ultimos_dos = df['Apellido'].apply(lambda x: unidecode(x).upper().split()[0][-2:] if x != "None" else None)
            # Convertir la columna de fechas al formato datetime
            df['FechaDeNacimiento'] = pd.to_datetime(df['FechaDeNacimiento'], format='%Y-%m-%d', errors='coerce')
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
            
            df["Nombres"] = df["Nombre"]
            df["Apellidos"] = df["Apellido"]
            df["CorreoElectronico"] = df["Correo"]
            df["Celular"] = df["Telefono"]
            df['NombrePrimeros2'] = nombre_primeros_dos
            df['ApellidoPrimeros2'] = apellido_primeros_dos
            df['ApellidoUltimos2'] = apellido_ultimos_dos
            df['FechaNacimientoDDMMYY'] = df['FechaDeNacimiento'].dt.strftime('%d%m%y')
            df['CodigoPais'] = df['Pais'].map(paises).fillna(np.nan)
            df["IdEmprende"] = df['CodigoPais'] + df['NombrePrimeros2'] + df['FechaNacimientoDDMMYY'] + df['ApellidoPrimeros2'] + df['ApellidoUltimos2']   
            # Verificar si alguno de los valores en las columnas es NaN
            df['Status'] = np.where(df['IdEmprende'].isnull(), 'ERROR', 'CREADO')
            df['Origen'] = file
            df['Error'] = df[['Nombres', 'Apellidos', 'FechaDeNacimiento', 'Pais']].apply(lambda row: row.index[row.isna()].tolist(), axis=1)

            columns_names = list(columns.keys())
            df = df.astype(str)
            data = df.loc[:, columns_names]
            print(data.head(5))
            data.reset_index(drop=True, inplace=True)
            insert(connection_usuario, columns, data, name_tabla = "usuario_hubspot", sort_by='RecordId', bunch_size=10000)
                
        except Exception as e:
            print("Error:", e)
            
        # Cerrar la conexión
        connection.close()
    connection_usuario.close()











