import pandas as pd
import mysql.connector
from hubspot import insert, get_data_from_sql, to_uderscore, create2, delete_table
import numpy as np
from unidecode import unidecode

def establecer_conexion_emprende_db():
    connection = mysql.connector.connect(
        host='emprendesrv2.mariadb.database.azure.com',
        user='emprendeuser@emprendesrv2',
        password='@5THrrE5z9Z7bJ',
        port=3306,
        database="emprende_db"
    )
    return connection

connection_emprende_db = establecer_conexion_emprende_db()
emprende_db = connection_emprende_db.cursor()

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

usuarios_emprende_completo = get_data_from_sql(connection_emprende_db, "usuario_hubspot")
usuarios_emprende_completo = usuarios_emprende_completo[usuarios_emprende_completo['Status'] != 'NUEVO']
usuarios_emprende_completo = usuarios_emprende_completo[usuarios_emprende_completo['Status'] != 'ERROR']
usuarios_emprende_completo = usuarios_emprende_completo.astype(str)

delete_table("usuarios_emprende_completo", connection_emprende_db)
create2(connection_emprende_db, columns, "usuarios_emprende_completo")
cursor = connection_emprende_db.cursor()
cursor.execute("ALTER TABLE usuarios_emprende_completo MODIFY COLUMN Nombres VARCHAR(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;")
cursor.execute("ALTER TABLE usuarios_emprende_completo MODIFY COLUMN Apellidos VARCHAR(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;")
connection_emprende_db.commit()
insert(connection_emprende_db, columns, usuarios_emprende_completo, name_tabla = "usuarios_emprende_completo", sort_by='RecordId', bunch_size=10000)

# Guardar cambios y cerrar la conexión
connection_emprende_db.close()
