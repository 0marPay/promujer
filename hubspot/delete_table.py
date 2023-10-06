import mysql.connector
from hubspot import create2, establecer_conexion

connection = establecer_conexion(database='emprende_db_testing')

def delete_table_(table_name, connection):
    cursor = connection.cursor()
    delete_query = f"DELETE FROM {table_name} WHERE RecordId > 90000"
    cursor.execute(delete_query)
    connection.commit()
    cursor.close()

def create_table(table_name, columnas, connection):
    create2(connection, columnas, name_tabla=table_name)

if __name__ == '__main__':
    table_name = "`hubspot_clean (Form Inscripcion 4taC - Fortalece Bs.Aires.csv)`"
    table_name = "`hubspot_data (Form Inscripcion 4taC - Fortalece Bs.Aires.csv)`"
    table_name = "usuario_hubspot"
    delete_table_(table_name, connection)
    connection.close()

