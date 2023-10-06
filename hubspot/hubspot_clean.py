import pandas as pd
from hubspot import create2, insert, get_data_from_sql, rellenar_nulos, establecer_conexion, borrar_columnas_nan, ordenar_columnas, delete_table, to_uderscore
from hubspot_data import fixed_file as file

if __name__ == "__main__":
    
    # files =["Form Inscripcion 4taC - Fortalece Bolivia.csv",
    #         "Form Inscripcion 4taC - Fortalece Bs.Aires.csv",
    #         "Form Inscripcion 4taC - Fortalece Centroamerica.csv",
    #         "Form Inscripcion 4taC - Fortalece Chile.csv",
    #         "Form Inscripcion 4taC - Fortalece MP MX.csv",
    #         "Form Inscripcion 4taC - Fortalece SudAmerica.csv",
    #         "Form Inscripcion 4taC - Fortalece surestemx.csv",
    #         "Form Inscripcion 4taC - Mejora Bolivia.csv",
    #         "Form Inscripcion 4taC - Mejora Bs.Aires.csv",
    #         "Form Inscripcion 4taC - Mejora Centroamerica.csv",
    #         "Form Inscripcion 4taC - Mejora Chile.csv",
    #         "Form Inscripcion 4taC - Mejora MP MX.csv",
    #         "Form Inscripcion 4taC - Mejora SudAmerica.csv",
    #         "Form Inscripcion 4taC - Mejora surestemx.csv",
    #         "Formulario Comienza tu Negocio (1).csv",
    files =["Formulario inscripcion 2022 - Emprende (3ra cohorte) (23 Mayo) (1).csv",
            "Formulario inscripcion 2022 - Emprende PM y Mercado Pago.csv",
            "Formulario inscripcion 2022 - Emprende y MP (3ra cohorte) (1).csv",
            "Formulario inscripcion 2022- Emprende  (3ra cohorte) (1).csv",
            "Formulario Mejora tu negocio (1).csv",
            "Postulantes a la 2ra cohorte (convocatoria abierta).csv"]

    files = ["hubspot-crm-exports-emprende-pro-mujer-2023-06-01.csv"]

    for file in files:
        # Establecer la conexión a la base de datos
        connection = establecer_conexion(database="hubpspot_db")
        
        file_clean = to_uderscore(file, added="hubspot_clean ()")
        file_data = to_uderscore(file, added="hubspot_data ()")
        
        try:        
            columns = {"RecordId":{"index":0,"type":"INT"},"Nombre":{"index":1,"type":"VARCHAR(100)"},"Apellido":{"index":2,"type":"VARCHAR(100)"},"Pais":{"index":3,"type":"VARCHAR(20)"},"FechaDeNacimiento":{"index":4,"type":"DATE"},"Correo":{"index":5,"type":"VARCHAR(100)"},"Telefono":{"index":6,"type":"VARCHAR(20)"},"Dni":{"index":8,"type":"VARCHAR(100)"}}
            delete_table(f"`hubspot_clean ({file_clean})`", connection)
            create2(connection, columns, name_tabla = f"`hubspot_clean ({file_clean})`")        
                
            df = get_data_from_sql(connection, table_name=f"`hubspot_data ({file_data})`")
            
            cols = ["Nombre", "Apellido", "Pais", "FechaDeNacimiento", "Correo", "Telefono", "Dni"]
            for col in cols:
                df = ordenar_columnas(df, col)
            
            for col in cols:
                same_name = [c for c in df.columns if col in c]
                same_name.remove(col)
                if len(same_name) > 0:
                    for name in same_name:
                        df = rellenar_nulos(df, col, name)
            try:
                df["FechaDeNacimiento"] = pd.to_datetime(df["FechaDeNacimiento"], format='%d/%m/%y').dt.strftime('%Y-%m-%d')
            except Exception as e:
                print("ERROR AL INTENTAR CAMBIAR FECHA:",str(e))
            columns_names = list(columns.keys())
            df = df.astype(str)
            data = df.loc[:, columns_names]
            data = data.reindex(columns=columns_names)
            data.reset_index(drop=True, inplace=True)
            insert(connection, columns, data, name_tabla = f"`hubspot_clean ({file_clean})`", sort_by='RecordId')
                
        except Exception as e:
            print("Error:", e)
            
        # Cerrar la conexión
        connection.close()
    





