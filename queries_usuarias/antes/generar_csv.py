import pandas as pd

def remove_from_list(main_list, elements_to_remove):
    return [element for element in main_list if element not in elements_to_remove]

carpeta = "queries_usuarias/"
nombre_archivo = carpeta + 'Bases para 1.5 (1).xlsx'
xl = pd.ExcelFile(nombre_archivo)
hojas = xl.sheet_names
hojas = remove_from_list(hojas, ['Equipo Emprende', 'Base data', 'Hoja2', 'cod.pais', 'Usuarios Prueba'])

for hoja in hojas:
    df = pd.read_excel(nombre_archivo, sheet_name=hoja)
    print(hoja)
    print(df.head(10))
    print("---------------------------------"*3)

    # Guardar DataFrame en un archivo CSV
    nombre_archivo_csv = carpeta + hoja + "_1.csv"
    df.to_csv(nombre_archivo_csv, index=False)
