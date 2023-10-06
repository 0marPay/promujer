import pandas as pd
import os
import json
import mysql.connector
from hubspot import insert

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

# Nombre del archivo JSON que deseas abrir
files_1 = [file for file in os.listdir() if file.startswith("formulario") and file.endswith(".json")]
files_2 = [file for file in os.listdir() if file.startswith("linea") and file.endswith(".json")]
files = files_1 + files_2

# Definir un DataFrame vacío inicialmente
to_insert = pd.DataFrame(columns=[
    "id",
    "text",
    "description",
    "instructions",
    "order",
    "mandatory",
    "answerType",
    "multipleChoice",
    "lockEdit",
    "lockDelete",
    "hideConditions",
    "sectionId"
])

# Función para cargar y procesar el archivo JSON con múltiples secciones
def procesar_archivo_json(nombre_archivo, id):
    try:
        # Abrir el archivo JSON
        with open(nombre_archivo, "r", encoding="utf-8") as archivo:
            # Cargar el contenido del archivo JSON
            data = json.load(archivo)
        
        return data
        
    except FileNotFoundError:
        print(f"El archivo {nombre_archivo} no fue encontrado.")
        return None
    except Exception as e:
        print(f"Ocurrió un error al procesar el archivo: {str(e)}")
        return None

for i, file in enumerate(files):

    # Actualizar el archivo JSON con el ID
    data = procesar_archivo_json(file, i).get("form", {})
    form_id = data["formId"]

    for section in data.get("sections", []):
        # print(section)
        section_id = section["sectionId"]
        
        for question in section.get("questions", []):
            
            questionId = question.get("questionId")
            questionText = question.get("questionText", "")
            questionDescription = question.get("questionDescription", "")
            instructions = question.get("textInstructions", "")
            orden = question.get("orden")
            questionType = question.get("questionType", "")
            mandatory = 1 if question.get("mandatory") else 0
            answerOptions = question.get("answerOptions", [])
            
            lockEdit = 1
            lockDelete = 1
            hideConditions = "{}"
            
            answerType = ""
            multipleChoice = 0
            
            if answerOptions:
                answer = answerOptions[0]  # Tomar la primera respuesta, puedes ajustarlo según tus necesidades
                answerType = answer.get("answerType", "")
                answerAdicionalProperties = answer.get("answerAdicionalProperties", {})
                multipleChoice = 1 if answerAdicionalProperties.get("multipleChoice", "") else 0
            
                
            # Crear un DataFrame para cada sección y agregarlo al DataFrame principal
            section_df = pd.DataFrame({
                "id": [questionId],
                "text": [questionText],
                "description": [questionDescription],
                "instructions": [instructions],
                "order": [orden],
                "mandatory": [mandatory],
                "answerType": [answerType],
                "multipleChoice": [multipleChoice],
                "lockEdit": [lockEdit],
                "lockDelete": [lockDelete],
                "hideConditions": [hideConditions],
                "sectionId": [section_id]
            })

            to_insert = pd.concat([to_insert, section_df], ignore_index=True)
            
            # Mostrar el DataFrame resultante
            vertical_df = section_df.transpose()  # También puedes usar section_df.T
            print(vertical_df); print()
    
print(to_insert.head(20))

columns = {
    "id":{"index":0,"type":"INT(11)"},
    "text":{"index":1,"type":"text"},
    "description":{"index":3,"type":"text"},
    "instructions":{"index":0,"type":"text"},
    "`order`":{"index":1,"type":"INT(11)"},
    "mandatory":{"index":3,"type":"tinyint(1)"},
    "answerType":{"index":1,"type":"VARCHAR(100)"},
    "multipleChoice":{"index":0,"type":"tinyint(1)"},
    "lockEdit":{"index":3,"type":"tinyint(1)"},
    "lockDelete":{"index":0,"type":"tinyint(1)"},
    "hideConditions":{"index":1,"type":"text"},
    "sectionId":{"index":2,"type":"INT(11)"}
}

    
insert(connection_emprende_db, columns, to_insert, name_tabla = "forms_question", sort_by='id', bunch_size=10)


connection_emprende_db.close()