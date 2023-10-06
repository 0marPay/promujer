import os
import json

files = [file for file in os.listdir() if file.endswith(".json")]

new_section_id = 1
new_question_id = 1

# Procesar cada archivo JSON
for filepath in files:
    
    with open(filepath, "r", encoding="utf-8") as json_file:
        data = json.load(json_file)

    # Procesar secciones y asignar nuevos section_ids
    sections = data["form"]["sections"]
    for i, section in enumerate(sections):
        
        original_section_id = section["sectionId"]
        
        # Generar un nuevo section_id único y universal
        section["sectionId"] = str(new_section_id)
        new_section_id = new_section_id + 1

        # Procesar preguntas dentro de la sección y asignar nuevos question_ids
        questions = section["questions"]
        
        for e, question in enumerate(questions):
            
            original_question_id = question["questionId"]
        
            # Generar un nuevo question_id único y universal
            question["questionId"] = str(new_question_id)
            section["questions"][e] = question
            new_question_id = new_question_id + 1
        
        data["form"]["sections"][i] = section

    # Guardar el archivo JSON actualizado con los nuevos ids
    with open(filepath, "w", encoding="utf-8") as json_file:
        json.dump(data, json_file, ensure_ascii=False, indent=2)

