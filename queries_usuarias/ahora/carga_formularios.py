import pandas as pd
import mysql.connector
import json
import os
import jmespath
import math


max_caracters = 64
ruta_archivo = "errores.txt"

def obtener_buena_fila(row):
    row = str(tuple(row))
    row = row.replace("'nan'", "NULL")
    row = row.replace(", nan)", ", NULL)")
    row = row.replace(", nan,", ", NULL,")
    row = row.replace(", nan,", ", NULL,")
    row = row.replace("'None'", "NULL")
    row = row.replace("'NaT'", "NULL")
    return row + ", "

def guardar_texto_en_txt(texto, ruta_archivo):
    with open(ruta_archivo, 'a', encoding='utf-8') as archivo:
        archivo.write(texto + '\n')
        
def insert(connection, columnas, data, name_tabla, sort_by = 'Record ID', bunch_size = 100, previous_index=0):
    cursor = connection.cursor()
    start = 0
    data = data.iloc[start:data.shape[0]]
    print("inserting...")
    for i in range(math.ceil(data.shape[0]/bunch_size)):
        i = i * bunch_size
        f = i + bunch_size
        if f > data.shape[0]:
            f = data.shape[0]
        data_i = data.iloc[i:f]
        
        insert_query = "INSERT INTO " 
        insert_query += f"{name_tabla} ("
        for key in columnas:
            insert_query += key + ", "
        insert_query = insert_query[:-2]
        insert_query += ") VALUES "    
        insert_query_full = str(insert_query)
        for _, row in data_i.iterrows():
            insert_query_full += obtener_buena_fila(row)
            start += 1
        
        insert_query_full = insert_query_full[:-2] + ";"
        
        print(previous_index+i,previous_index+f)
        
        try:
            cursor.execute(insert_query_full)
            connection.commit()
        except Exception as e: 
            try:
                print("Error: " + str(e))
                print("Query: " + insert_query_full)
                guardar_texto_en_txt("ERROR: " + str(e), ruta_archivo)
                if "at row " in str(e):
                    e = str(e)
                    e = e[e.index("at row ") + len("at row "):len(e)]
                    e = int(e) + previous_index
                    print("Error en fila: " + str(i + e))
                    e = e-1
                    problema = data_i.iloc[e]
                    guardar_texto_en_txt("QUERY: " + insert_query + obtener_buena_fila(problema)[:-2] + ";", ruta_archivo) 
                    _id = problema[sort_by]
                    data_i = data_i.query(f"{sort_by} != '{_id}'")
                    insert(connection, columnas, data_i, name_tabla, sort_by, int(bunch_size/1), previous_index=i)
            except Exception as e: 
                print("Error: " + str(e))

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

def get_data_from_usuario_formulario_respuestas(connection):
    # Sentencia SQL para obtener todos los registros de la tabla
    sql_query = """SELECT ufr.*, u.id_emprende
        FROM usuario_formulario_respuestas ufr
        INNER JOIN usuario u ON ufr.usuario_id = u.codigo
        WHERE ufr.status is NULL;"""
    # Cargar los registros en un DataFrame
    df = pd.read_sql(sql_query, connection)
    
    return df

def update_nulls_form_forms_response(connection):
    # Sentencia SQL para obtener todos los registros de la tabla
    sql_query = """UPDATE usuario_formulario_respuestas ufr
        SET ufr.status = 'TRANSFERIDO'
        WHERE ufr.status is NULL;"""
    # Cargar los registros en un DataFrame
    df = pd.read_sql(sql_query, connection)
    
    return df

def get_data_from_forms_form_response(connection, max_id):
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

def get_question_id(data, section_order, question_order):
    consulta_jmespath = f"form.sections[?orden == to_number('{section_order}')] | [0] | questions[?orden == to_number('{question_order}')] | [0] | questionId"
    consulta_jmespath = f"form.sections[{section_order}] | questions[{question_order}] | questionId"
    
    return jmespath.search(consulta_jmespath, data)

def update_form_forms_response():
    
    global connection_emprende_db
    
    files = [file[0:(len(file)-len(".json"))] for file in os.listdir() if file.startswith("T") and file.endswith(".json")]

    usuarios_emprende = get_data_from_usuario_formulario_respuestas(connection_emprende_db)
    usuarios_emprende = usuarios_emprende[["id_emprende", "usuario_id","membresia_id", "tipo_formulario", "json_respuestas"]]

    not_found = 0
    to_insert = pd.DataFrame({'id': [],
                            'id_emprende': [],
                            'formId': [],
                            'responseCallback': []})

    id_ = 1
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
            
            nueva_entrada = {'id': id_, 
                            'id_emprende': usuario_emprende["id_emprende"],
                            'formId': form_json["formId"], 
                            'responseCallback': usuario_emprende["json_respuestas"]}
            nueva_entrada = pd.DataFrame.from_dict(nueva_entrada, orient='index').T

            to_insert = pd.concat([to_insert, nueva_entrada], ignore_index=True)
            id_ += 1
            print(nueva_entrada.T); print()
            
        else:
            print(usuario_emprende.T); print()
            actualizar_no_encontrado(usuario_emprende, "FORMULARIO NO ENCONTRADO")
            not_found += 1

    try:
        indice
    except Exception:
        indice = -1
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
    update_nulls_form_forms_response(connection_emprende_db)
    return max_id + 1

def update_form_question_reponse(max_id):
    
    global connection_emprende_db
    
    # Definir un DataFrame vacío inicialmente
    to_insert = pd.DataFrame(columns=[
        "id",
        'responseValue',
        'responseId', 
        'questionId'
    ])

    registros = get_data_from_forms_form_response(connection_emprende_db, max_id)

    # Nombre del archivo JSON que deseas abrir
    files_1 = [file for file in os.listdir() if file.startswith("formulario") and file.endswith(".json")]
    files_2 = [file for file in os.listdir() if file.startswith("linea") and file.endswith(".json")]
    files = files_1 + files_2
    map_files = {procesar_archivo_json(file)["form"]["formId"]: file for file in files}

    id_ = 1
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

                nueva_entrada = {'id': id_, 
                                'responseValue': answer,
                                'responseId': registro["id"], 
                                'questionId': question_id}
        
                nueva_entrada = pd.DataFrame.from_dict(nueva_entrada, orient='index').T
                # print(nueva_entrada.T)
                to_insert = pd.concat([to_insert, nueva_entrada], ignore_index=True)
                
                id_ += 1
                
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
    
connection_emprende_db = establecer_conexion_emprende_db()
emprende_db = connection_emprende_db.cursor()
    
if __name__ == '__main__':
    try:
        max_id = update_form_forms_response()
        update_form_question_reponse(max_id)
    except Exception as e:
        print("Error updating data", e)

connection_emprende_db.close()
    
    
    
    