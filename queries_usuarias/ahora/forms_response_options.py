import pandas as pd
import os
import json
import mysql.connector
from hubspot import insert, get_data_from_sql, to_uderscore, create2, delete_table
import jmespath

# Nombre del archivo JSON que deseas abrir
files_1 = [file for file in os.listdir() if file.startswith("formulario") and file.endswith(".json")]
files_2 = [file for file in os.listdir() if file.startswith("linea") and file.endswith(".json")]
files = files_1 + files_2

# Definir un DataFrame vacío inicialmente
to_insert = pd.DataFrame(columns=[
    "id",
    "responseLabel",
    "responseValue",
    "lockEdit",
    "lockDelete",
    "questionId"
])

# Función para cargar y procesar el archivo JSON
def procesar_archivo_json(nombre_archivo):
    try:
        # Abrir el archivo JSON
        with open(nombre_archivo, "r", encoding="utf-8") as archivo:
            # Cargar el contenido del archivo JSON
            return json.load(archivo)
        
    except FileNotFoundError:
        print(f"El archivo {nombre_archivo} no fue encontrado.")
        return None
    except Exception as e:
        print(f"Ocurrió un error al procesar el archivo: {str(e)}")
        return None

def get_count_from_table(connection, table):
    # Sentencia SQL para obtener todos los registros de la tabla
    sql_query = f"select max(id) from {table};"
    # Cargar los registros en un DataFrame
    df = pd.read_sql(sql_query, connection)
    return df.iloc[0, 0]


def establecer_conexion_emprende_db():
    connection = mysql.connector.connect(
        host='emprendesrv2.mariadb.database.azure.com',
        user='emprendeuser@emprendesrv2',
        password='@3mpr3nd32O2E.08',
        port=3306,
        database="emprende_db"
    )
    return connection

connection_emprende_db = establecer_conexion_emprende_db()
emprende_db = connection_emprende_db.cursor()

optionId = 1
optionId = get_count_from_table(connection_emprende_db, "forms_response_options") + 1
for i, file in enumerate(files):

    print(f"in file {file}")
    data = procesar_archivo_json(file)
    data = data["form"]["sections"]
    

    for e, section in enumerate(data):
        for a, question in enumerate(section["questions"]):
        # Extraer los valores del JSON
            questionId = question["questionId"]    
            
            answer = question["answerOptions"][0]
            
            if answer["answerType"] == "selectInputRadioButtons" or answer["answerType"] == "selectInputCheckboxes":
                options = "options"
                continue
            elif answer["answerType"] == "moneyCurrencyInput":
                options = "optionsCurrency"
                continue
            elif answer["answerType"] == "selectInput":
                options = "optionItems"
                continue
            elif answer["answerType"] == "inputText":
                options = ""
            else:
                continue
            
            optionsList = answer["answerAdicionalProperties"][options] if options else [{'value': '', 'label': ''}]
            
            for optionValue in optionsList:
            
                responseValue = optionValue["value"]
                responseLabel = optionValue["label"]
                lockEdit = 1
                lockDelete = 1
                
                # Crear un DataFrame
                df = pd.DataFrame({
                    "id": [optionId],
                    "responseLabel": [responseLabel],
                    "responseValue": [responseValue],
                    "lockEdit": [lockEdit],
                    "lockDelete": [lockDelete],
                    "questionId": [questionId]
                })

                to_insert = pd.concat([to_insert, df], ignore_index=True)
                
                optionId += 1
                
                # Mostrar el DataFrame resultante
                vertical_df = df.transpose()
                print(vertical_df)
                print()
        print()
    print()
print()
    
print(to_insert.head(20))

columns = {
    "id": "INT(11)",
    "responseLabel": "text",
    "responseValue": "text",
    "lockEdit": "tinyint(1)",
    "lockDelete": "tinyint(1)",
    "questionId": "INT(11)"
}
# eliminar mayor a 1780


insert(connection_emprende_db, columns, to_insert, name_tabla = "forms_response_options", sort_by='id', bunch_size=100)
