import os
import json

files = [file for file in os.listdir() if file.endswith(".json") and not file.startswith("T")]

# Procesar cada archivo JSON
for filepath in files:
    
    with open(filepath, "r", encoding="utf-8") as json_file:
        data = json.load(json_file)

    # Procesar secciones y asignar nuevos section_ids
    sections = data["form"]["sections"]
    for i, section in enumerate(sections):
        
        for e, question in enumerate(section["questions"]):
            
            original_question_id = question["questionId"]
        
            # Generar un nuevo question_id único y universal
            new_question_id = question["questionId"]
                        
            if len(section["questions"][e]["answerOptions"]) > 1:
                 raise ValueError("Hay dos answers")
            
            section["questions"][e]["answerOptions"][0]["answerId"] = new_question_id
        
        data["form"]["sections"][i] = section

    # Guardar el archivo JSON actualizado con los nuevos ids
    with open(filepath, "w", encoding="utf-8") as json_file:
        json.dump(data, json_file, ensure_ascii=False, indent=2)

