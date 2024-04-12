import pandas as pd
import numpy as np

# Definicon de Rutas y nombre de archvios
input_path = 'C:\\Users\\Jesus\\Documents\\proyectos\\Parket-to-Json\\input\\'
output_path = 'C:\\Users\\Jesus\\Documents\\proyectos\\Parket-to-Json\\output\\'
input_file = 'business.parquet'
output_file = 'business.json'

archivo_json = open (output_path + output_file,'w')

def convertir_clave_valor(tupla):
    clave, valor = tupla
    if type(valor) in  (np.int64,np.float64):
        return f"'{clave}':{valor}"
    else :
        return f"'{clave}':'{valor}'"

# Lee el archvio parquet y lo convierte en un DataFrame
df = pd.read_parquet(input_path + input_file)
columnas = df.columns

# Construccion de una lista de colecciones a partid del DataFrame
colecciones = list()
for fila in range(len(df)):
    colecciones.append([convertir_clave_valor((columnas[columna], df.iloc[fila,columna])) for columna in range(len(columnas))])

# Construccion del archivo json
archivo_json.write("[  \r")
for indice, coleccion in enumerate(colecciones):
    archivo_json.write("  {\r")
    for columna in range(len(coleccion)):
        if columna == len(coleccion) - 1:
            archivo_json.write("   " + coleccion[columna] + "\r")
        else :
            archivo_json.write("   " + coleccion[columna] + ",\r")
    if indice == len(colecciones) - 1:
        archivo_json.write("  }\r")
    else :
        archivo_json.write("  },\r")
archivo_json.write("]  \r")
archivo_json.close()