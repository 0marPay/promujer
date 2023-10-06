import pandas as pd

# DataFrame original 1
data_1 = {
    "id": [1, 2, 3, 4],
    "status": ["Creado", "Proceso", "Creado", "Proceso"],
    "id emprende": [101, 102, 103, 104],
    "correo": ["correo1@example.com", "correo2@example.com", "correo3@example.com", "correo4@example.com"]
}

df_1 = pd.DataFrame(data_1)

# DataFrame original 2
data_2 = {
    "id": [5, 6, 7, 8],
    "status": ["Creado", "Proceso", "Creado", "Proceso"],
    "id emprende": [101, 106, 107, 104],
    "correo": ["correo1@example.com", "correo6@example.com", "correo7@example.com", "correo4@example.com"]
}

df_2 = pd.DataFrame(data_2)

# Paso 1: Filtrar los DataFrames originales por "Creado" en la columna "status"
df_1_creado = df_1[df_1["status"] == "Creado"]
df_2_creado = df_2[df_2["status"] == "Creado"]

# Paso 2: Fusionar los DataFrames filtrados por las columnas "id emprende" y "correo"
df_fusion = pd.merge(df_1_creado, df_2_creado, on=["id emprende", "correo"])

# Paso 3: Crear un nuevo DataFrame con las filas que coinciden y cambiar el valor de "status" en los DataFrames originales
df_encontrado = df_fusion.copy()
df_1.loc[df_1["id"].isin(df_encontrado["id_x"]), "status"] = "Encontrado"
df_2.loc[df_2["id"].isin(df_encontrado["id_y"]), "status"] = "Encontrado"

# Imprimir los resultados
print("DataFrame original 1:")
print(df_1)

print("\nDataFrame original 2:")
print(df_2)

print("\nDataFrame encontrado:")
print(df_encontrado)
