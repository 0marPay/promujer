import pandas as pd
import mysql.connector
from hubspot import insert, get_data_from_sql, to_uderscore, create2, delete_table
import numpy as np
from unidecode import unidecode
import json
import os
import jmespath

# Nombre del archivo JSON que deseas abrir
files_1 = [file for file in os.listdir() if file.startswith("formulario") and file.endswith(".json")]
files_2 = [file for file in os.listdir() if file.startswith("linea") and file.endswith(".json")]
files = files_1 + files_2

# Función para cargar y procesar el archivo JSON con JMESPath
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

def establecer_conexion_emprende_db():
    connection = mysql.connector.connect(
        host='emprendesrv2.mariadb.database.azure.com',
        user='emprendeuser@emprendesrv2',
        password='@3mpr3nd32O2E.08',
        port=3306,
        database="emprende_db"
    )
    return connection
# TODO: improve filtering
def get_data_from_sql(connection, max_id):
    # Sentencia SQL para obtener todos los registros de la tabla
    max_id = max_id - 1
    sql_query = f"""SELECT *
        FROM forms_form_response
        where id > {max_id};"""
    # Cargar los registros en un DataFrame
    df = pd.read_sql(sql_query, connection)
    
    return df

def get_count_from_table(connection, table):
    # Sentencia SQL para obtener todos los registros de la tabla
    sql_query = f"select max(id) from {table};"
    # Cargar los registros en un DataFrame
    df = pd.read_sql(sql_query, connection)
    return df.iloc[0, 0]

def get_true_answersId(data):
    consulta_jmespath = "[?value].answersId"
    return jmespath.search(consulta_jmespath, data)
    
def get_label(question, options_type, answer):
    new_answer = []
    for item in answer:
        consulta_jmespath = f"{options_type}[?value=='{item}'] | [0]"
        new_answer.append(jmespath.search(consulta_jmespath, question))
    return new_answer

def get_question_id(data, section_order, question_order):
    consulta_jmespath = f"form.sections[?orden == to_number('{section_order}')] | [0] | questions[?orden == to_number('{question_order}')] | [0] | questionId"
    consulta_jmespath = f"form.sections[{section_order}] | questions[{question_order}] | questionId"
    
    return jmespath.search(consulta_jmespath, data)

# Definir un DataFrame vacío inicialmente
to_insert = pd.DataFrame(columns=[
    "id",
    'responseValue',
    'responseId', 
    'questionId'
])

connection_emprende_db = establecer_conexion_emprende_db()
emprende_db = connection_emprende_db.cursor()

registros = get_data_from_sql(connection_emprende_db, 982)

map_files = {procesar_archivo_json(file)["form"]["formId"]: file for file in files}

id = 1
for indice, registro in registros.iterrows():
    
    respuesta_json = json.loads(registro["responseCallback"])
    # print(registro["formId"])
    file_name = map_files[str(registro["formId"])]
    # print(file_name)
    with open(file_name, "r", encoding="utf-8") as archivo:
        form_json = json.load(archivo)
    
    for i, section_pregunta in enumerate(respuesta_json["sections"]):
        
        section_order = section_pregunta["sectionId"]
        
        for e, question in enumerate(section_pregunta["answers"]):
            
            question_order = question["questionId"]
            answer = question["values"]
            
            if type(answer) == list:
                if len(answer) == 1 and type(answer[0]) == str:
                    answer = answer[0]
                else:
                    answer = get_true_answersId(answer)
                    answer = answer[0] if len(answer) == 1 else answer
                if type(answer) == list and len(answer) == 0:
                    continue 
            elif "income" in answer:
                answer = json.dumps(answer)
            else:
                raise ValueError("Invalid answer")
            
            question_id = get_question_id(form_json, i, e)
                
            if question_id is None or answer is None:
                continue

            nueva_entrada = {'id': id, 
                            'responseValue': answer,
                            'responseId': registro["id"], 
                            'questionId': question_id}
    
            nueva_entrada = pd.DataFrame.from_dict(nueva_entrada, orient='index').T
            # print(nueva_entrada.T)
            to_insert = pd.concat([to_insert, nueva_entrada], ignore_index=True)
            
            id += 1
            
        # print()


print(to_insert.head(20))
        
columns = {
    "id":{"index":0,"type":"INT(11)"},
    "responseValue":{"index":1,"type":"text"},
    "responseId":{"index":2,"type":"INT(11)"},
    "questionId":{"index":3,"type":"INT(11)"},
}

print("insertando...")
max_id = int(get_count_from_table(connection_emprende_db, "forms_question_response"))
print("max_id", max_id)
to_insert["id"] += max_id + 1
insert(connection_emprende_db, columns, to_insert, name_tabla = "forms_question_response", sort_by='id', bunch_size=100)

# Guardar cambios y cerrar la conexión
connection_emprende_db.close()

    