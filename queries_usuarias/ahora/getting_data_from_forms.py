import json
import jmespath
import os

# Nombre del archivo JSON que deseas abrir
files_1 = [file for file in os.listdir() if file.startswith("formulario") and file.endswith(".json")]
files_2 = [file for file in os.listdir() if file.startswith("linea") and file.endswith(".json")]
files = files_1 + files_2
codes = []
repetidos = []

# Función para cargar y procesar el archivo JSON con JMESPath
def procesar_archivo_json(nombre_archivo, consulta_jmespath):
    try:
        # Abrir el archivo JSON
        with open(nombre_archivo, "r", encoding="utf-8") as archivo:
            # Cargar el contenido del archivo JSON
            datos_json = json.load(archivo)

            # Aplicar la consulta JMESPath
            resultado = jmespath.search(consulta_jmespath, datos_json)

            return resultado
    except FileNotFoundError:
        print(f"El archivo {nombre_archivo} no fue encontrado.")
        return None
    except Exception as e:
        print(f"Ocurrió un error al procesar el archivo: {str(e)}")
        return None

# Consulta JMESPath de ejemplo
consulta = """{formId: form.formId, campaignId: campaignId, campaignName:campaignName, form:form.sections[].{sectionId: sectionId, sectionName: sectionName, questions:questions[].{questionId:questionId, questionText:questionText, mandatory: mandatory, answerType:answerOptions[0].answerType, radioButtonsOptions:answerOptions[0].answerAdicionalProperties.options, selectInputOptions:answerOptions[0].answerAdicionalProperties.optionItems}}}"""

for file in files:
    
    code = "T" 
    if "formulario" in file:
        code += "f"
    elif "base" in file:
        code += "b"
    elif "salida" in file:
        code += "s"
    
    resultado_consulta = procesar_archivo_json(file, consulta)
    
    campaña = resultado_consulta["campaignName"]
    print()
    code += "-M"
    if "inscripción" in campaña or "inscripcion" in campaña:
        code += "i"
    elif "Fortalece" in campaña or "fortalece" in campaña:
        code += "2"
    elif "Mejora" in campaña or "mejora" in campaña:
        code += "4"
    elif "Comienzo" in campaña:
        code += "6"
    elif "Expando" in campaña:
        code += "3"
    elif "Ideo" in campaña:
        code += "5"
    elif "Todas Digitales" in campaña:
        if "Basico" in campaña:
            code += "7"
        elif "Intermedio" in campaña:
            code += "8"
        elif "Avanzado" in campaña:
            code += "9"
        else:
            code += "1"
    elif "f" in code and "seguimiento" in campaña:
        code += "s"
    else:
        print("Unknown")
        code = campaña
        
    code = code + "-S" + str(len(resultado_consulta["form"]))
    
    print("TOTAL CONSULTAS", len(resultado_consulta["form"]))
    for i, secc in enumerate(resultado_consulta["form"]):
        print(f"TOTAL PREGUNTAS [{i}]", len(secc["questions"]))
        code = code + "-" + str(len(secc["questions"]))
    
    if code in codes:
        print("repetido")
        repetidos.append(file)
    else:
        codes.append(code)
    print("code:", code)
    resultado_consulta["code"] = code
    
    if resultado_consulta is not None:
        # Crear el nombre del archivo de salida
        nombre_archivo_salida = code + ".json"
        print(nombre_archivo_salida) 
        print()
        # Guardar el resultado en el archivo de salida
        with open(nombre_archivo_salida, "w", encoding="utf-8") as archivo_salida:
            # Serializar el resultado a JSON y escribirlo en el archivo
            json.dump(resultado_consulta, archivo_salida, ensure_ascii=False)

print(repetidos)