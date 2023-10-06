import pandas as pd
import mysql.connector
from hubspot import insert, get_data_from_sql, to_uderscore, create2, delete_table
import numpy as np
from unidecode import unidecode

def establecer_conexion_emprende_db():
    connection = mysql.connector.connect(
        host='emprendesrv2.mariadb.database.azure.com',
        user='emprendeuser@emprendesrv2',
        password='@5THrrE5z9Z7bJ',
        port=3306,
        database="emprende_db"
    )
    return connection

connection_emprende_db = establecer_conexion_emprende_db()
connection_emprende_db = establecer_conexion_emprende_db()

emprende_db = connection_emprende_db.cursor()
emprende_db = connection_emprende_db.cursor()

usuarios_emprende = get_data_from_sql(connection_emprende_db, "usuario_emprende")
# usuarios_hubspot_temp = get_data_from_sql(connection_emprende_db, "usuario_hubspot")
usuarios_emprende = usuarios_emprende[usuarios_emprende['Status'] == 'ERROR']
usuarios_emprende = usuarios_emprende[usuarios_emprende['NombrePrimeros2'].notna()]
usuarios_emprende = usuarios_emprende[usuarios_emprende['ApellidoPrimeros2'].notna()]
usuarios_emprende = usuarios_emprende[usuarios_emprende['ApellidoUltimos2'].notna()]

print(usuarios_emprende.head(10))

usuarios_emprende_completo = pd.DataFrame(columns=usuarios_emprende.columns.tolist())
full_len = usuarios_emprende.shape[0]
i = 1
for indice, usuario_emprende in usuarios_emprende.iterrows():
    try: 
        correo_emprende = usuario_emprende['CorreoElectronico']
        correo_emprende = correo_emprende.replace("'", "")
        
        print()
        print(f"{i}/{full_len} - {round(100*i/full_len,2)}% - {correo_emprende}")
        
        usuarios_hubspot = pd.read_sql(f"SELECT * FROM usuario_hubspot WHERE CorreoElectronico = '{correo_emprende}'", con=connection_emprende_db)

        # Caso a: Si coincide más de uno, marcar los registros como REPETIDO
        if usuarios_hubspot.shape[0] > 1:
                    
            emprende_db.execute(f"UPDATE usuario_emprende SET Status = 'REPETIDO' WHERE CorreoElectronico = '{correo_emprende}'")
            emprende_db.execute(f"UPDATE usuario_hubspot SET Status = 'REPETIDO' WHERE CorreoElectronico = '{correo_emprende}'")    
            
            connection_emprende_db.commit()
            connection_emprende_db.commit()

        # Caso b: Si coincide solo uno, verificar los campos del ID Emprende
        elif usuarios_hubspot.shape[0] == 1:
            
            print(usuario_emprende)
            
            usuario_hubspot = usuarios_hubspot.iloc[0]
                            
            nombre_primeros_dos_emprede = usuario_emprende['NombrePrimeros2']
            apellido_primeros_dos_emprende = usuario_emprende["ApellidoPrimeros2"]
            apellido_ultimos_dos_emprende = usuario_emprende["ApellidoUltimos2"]
            
            nombre_primeros_dos_hubspot = usuario_hubspot['NombrePrimeros2']
            apellido_primeros_dos_hubspot = usuario_hubspot["ApellidoPrimeros2"]
            apellido_ultimos_dos_hubspot = usuario_hubspot["ApellidoUltimos2"]
            
            id_emprende_emprende_new = (nombre_primeros_dos_emprede, apellido_primeros_dos_emprende, apellido_ultimos_dos_emprende)
            id_emprende_hubspot_new = (nombre_primeros_dos_hubspot, apellido_primeros_dos_hubspot, apellido_ultimos_dos_hubspot)

            id_emprende_emprende = usuario_emprende['IdEmprende']
            id_emprende_hubspot = usuario_hubspot['IdEmprende']

            # Verificar si coinciden todos los campos del ID Emprende
            if (id_emprende_emprende == id_emprende_hubspot and id_emprende_emprende != None) :
                
                # Guardar registro en usuario_emprende_completados y actualizar estatus en los 3 registros
                usuario_emprende["Status"] = "TERMINADO"
                usuarios_emprende_completo = usuarios_emprende_completo.append(usuario_emprende, ignore_index=True)
                
                emprende_db.execute(f"UPDATE usuario_emprende SET Status = 'TERMINADO' WHERE CorreoElectronico = '{correo_emprende}'")
                emprende_db.execute(f"UPDATE usuario_hubspot SET Status = 'TERMINADO' WHERE CorreoElectronico = '{correo_emprende}'")
                            
                connection_emprende_db.commit()
                connection_emprende_db.commit()
                
            # Verificar si coinciden todos los campos del ID Emprende
            elif (id_emprende_emprende_new == id_emprende_hubspot_new) :
                
                status = 'TERMINADO_COINCIDE_NOMBRE'
                print(status)
                # Guardar registro en usuario_emprende_completados y actualizar estatus en los 3 registros
                usuario_emprende["Status"] = "TERMINADO_COINCIDE_NOMBRE"
                usuarios_emprende_completo = usuarios_emprende_completo.append(usuario_emprende, ignore_index=True)
                
                emprende_db.execute(f"UPDATE usuario_emprende SET Status = 'TERMINADO_COINCIDE_NOMBRE' WHERE CorreoElectronico = '{correo_emprende}'")
                emprende_db.execute(f"UPDATE usuario_hubspot SET Status = 'TERMINADO_COINCIDE_NOMBRE' WHERE CorreoElectronico = '{correo_emprende}'")
                            
                connection_emprende_db.commit()
                connection_emprende_db.commit()
                
            else:
                campos_no_coinciden = 0
                if nombre_primeros_dos_emprede != nombre_primeros_dos_hubspot:
                    campos_no_coinciden += 1      
                if apellido_primeros_dos_emprende != apellido_primeros_dos_hubspot:
                    campos_no_coinciden += 1
                if apellido_ultimos_dos_emprende != apellido_ultimos_dos_hubspot:
                    campos_no_coinciden += 1
                
                status = "NO_COINCIDE_" + str(campos_no_coinciden)
                print(status)
                usuario_emprende["Status"] = status
                usuarios_emprende_completo = usuarios_emprende_completo.append(usuario_emprende, ignore_index=True)
                
                emprende_db.execute(f"UPDATE usuario_emprende SET Status = '{status}' WHERE CorreoElectronico = '{correo_emprende}'")
                emprende_db.execute(f"UPDATE usuario_hubspot SET Status = '{status}' WHERE CorreoElectronico = '{correo_emprende}'")
                            
                connection_emprende_db.commit()
                connection_emprende_db.commit()
    
        # caso c: no coincide nada 
        else:      
            emprende_db.execute(f"UPDATE usuario_emprende SET Status = 'SIN_COINCIDENCIAS' WHERE CorreoElectronico = '{correo_emprende}'")
            # emprende_db.execute(f"UPDATE usuario_hubspot SET Status = 'SIN_COINCIDENCIAS' WHERE CorreoElectronico = '{correo_emprende}'")    
            
            connection_emprende_db.commit()
            # connection_emprende_db.commit()
            
    except Exception as e:
        print("Error ", e)
    i = i + 1

columns = {
    "RecordId":{"index":0,"type":"INT"},
    "Nombres":{"index":1,"type":"VARCHAR(100)"},
    "Apellidos":{"index":2,"type":"VARCHAR(100)"},
    "Pais":{"index":3,"type":"VARCHAR(20)"},
    "FechaDeNacimiento":{"index":4,"type":"DATE"},
    "CorreoElectronico":{"index":5,"type":"VARCHAR(100)"},
    "Celular":{"index":6,"type":"VARCHAR(20)"},
    "NombrePrimeros2":{"index":7,"type":"VARCHAR(20)"},
    "ApellidoPrimeros2":{"index":8,"type":"VARCHAR(20)"},
    "ApellidoUltimos2":{"index":9,"type":"VARCHAR(20)"},
    "FechaNacimientoDDMMYY":{"index":10,"type":"VARCHAR(10)"},
    "IdEmprende":{"index":11,"type":"VARCHAR(30)"},
    "Status":{"index":12,"type":"VARCHAR(100)"},
    "Error":{"index":13,"type":"VARCHAR(250)"},
    "Origen":{"index":14,"type":"VARCHAR(250)"}
}
# delete_table("usuario_hubspot_completo", connection_emprende_db)
# create2(connection_emprende_db, columns, "usuario_hubspot_completo")
insert(connection_emprende_db, columns, usuarios_emprende_completo, name_tabla = "usuarios_emprende_completo", sort_by='RecordId', bunch_size=1000)

# Guardar cambios y cerrar la conexión
connection_emprende_db.close()
connection_emprende_db.close()
