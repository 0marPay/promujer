import os
import pandas as pd

def xls_to_csv(input_file):
    output_file = input_file.replace('xlsx', 'csv')
    data_frame = pd.read_excel(input_file)  # Lee el archivo .xls
    data_frame.to_csv(output_file, index=False)  # Convierte y guarda como .csv

folder_path = "hubspot"
# Obtener todos los archivos de la carpeta que terminen en .xls
files = [f for f in os.listdir(folder_path) if f.endswith('.xlsx')]
files = ["C:/Users/olopezric/Documents/PROMUJER/carga_datos/Bases.xlsx"]
# Convertir cada archivo a .csv
for file in files:
    xls_file = os.path.join(folder_path, file)
    xls_to_csv(xls_file)
    
print("terminado")
