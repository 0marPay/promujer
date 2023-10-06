import pandas as pd
import mysql.connector
from hubspot import insert, get_data_from_sql, to_uderscore, create2, delete_table
import numpy as np
from unidecode import unidecode
import json
import os
import jmespath

files = [file[0:(len(file)-len(".json"))] for file in os.listdir() if file.startswith("T") and file.endswith(".json")]
# print(files)

def establecer_conexion_emprende_db():
    connection = mysql.connector.connect(
        host='emprendesrv2.mariadb.database.azure.com',
        user='emprendeuser@emprendesrv2',
        password='@3mpr3nd32O2E.08',
        port=3306,
        database="emprende_db"
    )
    return connection

def get_data_from_sql(connection):
    # Sentencia SQL para obtener todos los registros de la tabla
    sql_query = """SELECT ufr.*, ue.idEmprende
        FROM usuario_formulario_respuestas ufr
        INNER JOIN usuario u ON ufr.usuario_id = u.codigo
        INNER JOIN usuario_emprende ue ON u.codigo = ue.Codigo;"""
    # Cargar los registros en un DataFrame
    df = pd.read_sql(sql_query, connection)
    
    return df

def get_true_answersId(data):
    consulta_jmespath = "[?value].answersId"
    return jmespath.search(consulta_jmespath, data)
    
def get_label(question, options_type, answer):
    new_answer = []
    for item in answer:
        consulta_jmespath = f"{options_type}[?value=='{item}'] | [0]"
        new_answer.append(jmespath.search(consulta_jmespath, question))
    return new_answer

connection_emprende_db = establecer_conexion_emprende_db()
emprende_db = connection_emprende_db.cursor()

usuarios_emprende = get_data_from_sql(connection_emprende_db)
usuarios_emprende = usuarios_emprende[["idEmprende", "membresia_id", "tipo_formulario", "json_respuestas"]]

not_found = 0
to_insert = pd.DataFrame({'id': [],
                          'idEmprende': [],
                          'formId': [],
                          'responseCallback': []})

for indice, usuario_emprende in usuarios_emprende.iterrows():
    
    respuesta_json = json.loads(usuario_emprende["json_respuestas"])
    if "sections" not in respuesta_json:
        continue
    
    code = "T" 
    if "entrada" in usuario_emprende["tipo_formulario"]:
        code += "f"
    elif "base" in usuario_emprende["tipo_formulario"]:
        code += "b"
    elif "salida" in usuario_emprende["tipo_formulario"]:
        code += "s"
    
    code += "-M"+ str(usuario_emprende["membresia_id"])
    
    code = code + "-S" + str(len(respuesta_json["sections"]))
    for i, secc in enumerate(respuesta_json["sections"]):
        code = code + "-" + str(len(secc["answers"]))
    
    respuesta_json["code"] = code
    
    id = 1
    if code in files:
        with open(code+".json", "r", encoding="utf-8") as archivo:
            # Cargar el contenido del archivo JSON
            form_json = json.load(archivo)["form"]
        
        for i, section_pregunta in enumerate(form_json):
            print(section_pregunta["sectionId"], section_pregunta["sectionName"])
            section_answer = respuesta_json["sections"][i]
            for e, question in enumerate(section_pregunta["questions"]):
                answer = section_answer["answers"][e]["values"]
                print(f"{i+1}.{e+1}", question["questionText"])
                if question["answerType"] == "selectInputRadioButtons":
                    answer = get_true_answersId(answer)
                    options_type = "radioButtonsOptions"
                    answer = get_label(question, options_type, answer)
                elif question["answerType"] == "moneyCurrencyInput" or question["answerType"] == "inputText":
                    "ignore"
                else:
                    "ignore"
                
                print(answer)
                print()

                form_id = code + f"-[{i+1}.{e+1}]"
                nueva_entrada = {'id': id, 
                                 'idEmprende': usuario_emprende["idEmprende"],
                                'formId': form_id, 
                                'responseCallback': answer[0] if len(answer)==1 else answer}
                nueva_entrada = pd.DataFrame.from_dict(nueva_entrada, orient='index').T

                to_insert = pd.concat([to_insert, nueva_entrada], ignore_index=True)
                id += 1

                
            print()
    else:
        not_found += 1

print("total", indice+1)
print("not_found", not_found)

print(to_insert.head(21))
        

columns = {
    "RecordId":{"index":0,"type":"INT"},
    "Nombres":{"index":1,"type":"VARCHAR(100)"},
    "Apellidos":{"index":2,"type":"VARCHAR(100)"},
    "Pais":{"index":3,"type":"VARCHAR(20)"},
}
    
insert(connection_emprende_db, columns, to_insert, name_tabla = "usuarios_emprende_completo", sort_by='id', bunch_size=10)

# Guardar cambios y cerrar la conexión
connection_emprende_db.close()
