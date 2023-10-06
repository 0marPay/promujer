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
    sql_query = """SELECT ufr.*, u.id_emprende
        FROM usuario_formulario_respuestas ufr
        INNER JOIN usuario u ON ufr.usuario_id = u.codigo
        WHERE ufr.status is NULL;"""
    # Cargar los registros en un DataFrame
    df = pd.read_sql(sql_query, connection)
    
    return df

def get_count_from_table(connection, table):
    # Sentencia SQL para obtener todos los registros de la tabla
    sql_query = f"select count(*) from {table};"
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

def actualizar_no_encontrado(registro, nuevo_status):
     # Consulta SQL para actualizar el registro
    membresia_id = registro["membresia_id"]
    usuario_id = registro["usuario_id"]
    tipo_formulario = registro["tipo_formulario"]
    json_respuestas = registro["json_respuestas"]
    
    update_sql = """
        UPDATE usuario_formulario_respuestas
        SET status = %s
        WHERE membresia_id = %s AND usuario_id = %s AND tipo_formulario = %s AND json_respuestas = %s
    """

    # Ejecutar la consulta SQL
    emprende_db.execute(update_sql, (nuevo_status, membresia_id, usuario_id, tipo_formulario, json_respuestas))

    # Confirmar la actualización
    connection_emprende_db.commit()


connection_emprende_db = establecer_conexion_emprende_db()
emprende_db = connection_emprende_db.cursor()

usuarios_emprende = get_data_from_sql(connection_emprende_db)
usuarios_emprende = usuarios_emprende[["id_emprende", "usuario_id","membresia_id", "tipo_formulario", "json_respuestas"]]

not_found = 0
to_insert = pd.DataFrame({'id': [],
                          'id_emprende': [],
                          'formId': [],
                          'responseCallback': []})

id = 1
for indice, usuario_emprende in usuarios_emprende.iterrows():
    
    if usuario_emprende["id_emprende"] is None:
        print(usuario_emprende.T); print()
        actualizar_no_encontrado(usuario_emprende, "SIN ID EMPRENDE")
        continue
    
    respuesta_json = json.loads(usuario_emprende["json_respuestas"])
    if "sections" not in respuesta_json:
        print(usuario_emprende.T); print()
        actualizar_no_encontrado(usuario_emprende, "NO CUMPLE FORMATO")
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
    
    if code in files:
        with open(code+".json", "r", encoding="utf-8") as archivo:
            # Cargar el contenido del archivo JSON
            form_json = json.load(archivo)
        
        nueva_entrada = {'id': id, 
                        'id_emprende': usuario_emprende["id_emprende"],
                        'formId': form_json["formId"], 
                        'responseCallback': usuario_emprende["json_respuestas"]}
        nueva_entrada = pd.DataFrame.from_dict(nueva_entrada, orient='index').T

        to_insert = pd.concat([to_insert, nueva_entrada], ignore_index=True)
        id += 1
        print(nueva_entrada.T); print()
        
    else:
        print(usuario_emprende.T); print()
        actualizar_no_encontrado(usuario_emprende, "FORMULARIO NO ENCONTRADO")
        not_found += 1

print("total", indice+1)
print("not_found", not_found)

print(to_insert.head(21))

columns = {
    "id":{"index":0,"type":"INT(11)"},
    "idEmprende":{"index":1,"type":"VARCHAR(100)"},
    "formId":{"index":2,"type":"INT(11)"},
    "responseCallback":{"index":3,"type":"longtext"},
}

max_id = int(get_count_from_table(connection_emprende_db, "forms_form_response"))
print("max_id:", max_id)
to_insert["id"] += max_id
insert(connection_emprende_db, columns, to_insert, name_tabla = "forms_form_response", sort_by='id', bunch_size=50)

# Guardar cambios y cerrar la conexión
connection_emprende_db.close()
