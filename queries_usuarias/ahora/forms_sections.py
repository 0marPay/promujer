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
    "type",
    "name",
    "description",
    "instructionsText",
    "orderNum",
    "formId",
    "samePage",
    "showSectionName",
    "showInstructionsText",
    "lockPreviouslyAnsweredSection",
    "lockEdit",
    "lockDelete",
    "hideConditions"
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
        # Extraer los valores de cada sección
        section_id = section.get("sectionId", "")
        section_name = section.get("sectionName", "")
        section_description = section.get("sectionDescription", "")
        instructions_text = section.get("textInstructions", "")
        order_num = section.get("orden")
        same_page = section.get("displaySection", {}).get("breakSectionPages", False)
        show_section_name = section.get("displaySection", {}).get("showName", True)
        show_instructions_text = section.get("displaySection", {}).get("showInstructions", False)
        lock_previously_answered_section = section.get("displaySection", {}).get("lockPreviouslyAnsweredSection", False)
        lock_edit = 1  # Aquí debes definir tu lógica para lockEdit
        lock_delete = 1  # Aquí debes definir tu lógica para lockDelete
        hide_conditions = "{}"  # Aquí debes definir tu lógica para hideConditions

        # Crear un DataFrame para cada sección y agregarlo al DataFrame principal
        section_df = pd.DataFrame({
            "id": [section_id],
            "type": [""],
            "name": [section_name],
            "description": [section_description],
            "instructionsText": [instructions_text],
            "orderNum": [order_num],
            "formId": [form_id],
            "samePage": [1 if same_page else 0],
            "showSectionName": [1 if show_section_name else 0],
            "showInstructionsText": [1 if show_instructions_text else 0],
            "lockPreviouslyAnsweredSection": [1 if lock_previously_answered_section else 0],
            "lockEdit": [lock_edit],
            "lockDelete": [lock_delete],
            "hideConditions": [hide_conditions]
        })

        to_insert = pd.concat([to_insert, section_df], ignore_index=True)
    
    # Mostrar el DataFrame resultante
        vertical_df = section_df.transpose()  # También puedes usar section_df.T
        print(vertical_df); print()
    
print(to_insert.head(20))

columns = {
    "id":{"index":0,"type":"INT(11)"},
    "type":{"index":1,"type":"VARCHAR(255)"},
    "name":{"index":2,"type":"VARCHAR(255)"},
    "description":{"index":3,"type":"text"},
    "instructionsText":{"index":0,"type":"text"},
    "orderNum":{"index":1,"type":"INT(11)"},
    "formId":{"index":2,"type":"INT(11)"},
    "samePage":{"index":3,"type":"tinyint(1)"},
    "showSectionName":{"index":0,"type":"tinyint(1)"},
    "showInstructionsText":{"index":1,"type":"tinyint(1)"},
    "lockPreviouslyAnsweredSection":{"index":2,"type":"tinyint(1)"},
    "lockEdit":{"index":3,"type":"tinyint(1)"},
    "lockDelete":{"index":0,"type":"tinyint(1)"},
    "hideConditions":{"index":1,"type":"text"}
}
    
insert(connection_emprende_db, columns, to_insert, name_tabla = "forms_section", sort_by='id', bunch_size=1)


connection_emprende_db.close()