from hubspot import actualizar_registros, establecer_conexion

connection = establecer_conexion(database="hubpspot_db")
table = "`hubspot_clean (Form Inscripcion 4taC - Fortalece Bs.Aires.csv)`"
actualizar_registros(connection, table, 108)
