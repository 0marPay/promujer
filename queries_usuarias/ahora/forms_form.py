import pandas as pd
import os
import json
import mysql.connector
from hubspot import insert, get_data_from_sql, to_uderscore, create2, delete_table

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
    "presentationText",
    "instructionsText",
    "privacyText",
    "gratitudeText",
    "contactText"
])

# Función para cargar y procesar el archivo JSON con JMESPath
def procesar_archivo_json(nombre_archivo, id):
    try:
        # Abrir el archivo JSON
        with open(nombre_archivo, "r", encoding="utf-8") as archivo:
            # Cargar el contenido del archivo JSON
            data = json.load(archivo)
        
        # Actualizar el valor de formId con el ID
        data["form"]["formId"] = str(id)

        # Guardar el archivo JSON actualizado
        # with open(nombre_archivo, "w", encoding="utf-8") as archivo:
        #     json.dump(data, archivo, ensure_ascii=False, indent=2)
        
        return data
        
    except FileNotFoundError:
        print(f"El archivo {nombre_archivo} no fue encontrado.")
        return None
    except Exception as e:
        print(f"Ocurrió un error al procesar el archivo: {str(e)}")
        return None

for i, file in enumerate(files):
    i += 1
    # Actualizar el archivo JSON con el ID
    data = procesar_archivo_json(file, i)

    # Extraer los valores del JSON
    id = data["form"]["formId"]
    type = data["form"]["formType"]
    name = data["form"]["formName"]
    description = data["form"]["formDescription"]
    presentationText = data["form"]["textPresentation"]
    instructionsText = data["form"]["textInstructions"]
    privacyText = data["form"]["textPrivacy"]
    gratitudeText = data["form"]["textThanksCompleting"]
    contactText = data["form"]["textContact"]

    # Crear un DataFrame
    df = pd.DataFrame({
        "id": [id],
        "type": [type],
        "name": [name],
        "description": [description],
        "presentationText": [presentationText],
        "instructionsText": [instructionsText],
        "privacyText": [privacyText],
        "gratitudeText": [gratitudeText],
        "contactText": [contactText]
    })

    to_insert = pd.concat([to_insert, df], ignore_index=True)
    
    # Mostrar el DataFrame resultante
    vertical_df = df.transpose()  # También puedes usar df.T
    print(vertical_df); print()
    

print(to_insert.head(20))

columns = {
    "id": "INT",
    "type": "VARCHAR(255)",
    "name": "VARCHAR(255)",
    "description": "text",
    "presentationText": "text",
    "instructionsText": "text",
    "privacyText": "text",
    "gratitudeText": "text",
    "contactText": "text"
}

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

    
insert(connection_emprende_db, columns, to_insert, name_tabla = "forms_form", sort_by='id', bunch_size=1)
