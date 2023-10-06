import pandas as pd
import mysql.connector
from hubspot import insert, get_data_from_sql, to_uderscore, create2, delete_table
import numpy as np
from unidecode import unidecode

pd.set_option('mode.chained_assignment', None)

def establecer_conexion_emprende_db():
    connection = mysql.connector.connect(
        host='emprendesrv2.mariadb.database.azure.com',
        user='emprendeuser@emprendesrv2',
        password='@5THrrE5z9Z7bJ',
        port=3306,
        database="emprende_db"
    )
    return connection

def establecer_conexion_emprende_db_testing():
    connection = mysql.connector.connect(
        host='20.10.9.227',
        user='emprende_dev',
        password='3mpr3nd3Pr0Muj3r.2023',
        port=3306,
        database="emprende_db_testing"
    )
    return connection

connection_emprende_db = establecer_conexion_emprende_db()

emprende_db = connection_emprende_db.cursor()

usuarios_emprende = get_data_from_sql(connection_emprende_db, "usuario_emprende")

###########################################
# Recolectar correos electrónicos de usuario_emprende
correos_emprende = "','".join(usuarios_emprende['CorreoElectronico'].tolist())
# Consultar en usuario_hubspot utilizando la cláusula IN
query = f"SELECT * FROM usuario_hubspot WHERE CorreoElectronico IN ('{correos_emprende}')"
usuarios_hubspot = pd.read_sql(query, con=connection_emprende_db)
print(usuarios_hubspot.shape[0])
# Iterar sobre los resultados y actualizar los campos según sea necesario
# for indice, usuario_emprende in usuarios_emprende.iterrows():
#     correo_emprende = usuario_emprende['CorreoElectronico']
#################################