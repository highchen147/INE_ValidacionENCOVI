import pandas as pd
power_resumen = pd.read_excel("Mariajose/Validaciones_18-09-2023-13-06-27/InconsistenciasPowerBi_18-9-2023.xlsx")

import os, shutil

# Especifica la ruta de la carpeta que deseas explorar
ruta_carpeta = "Mariajose"

# Inicializa una lista para almacenar los nombres de los archivos
nombres_archivos = []

# Itera sobre los archivos en la carpeta
for elemento in os.listdir(ruta_carpeta):
    ruta_elemento = os.path.join(ruta_carpeta, elemento)
    # Verifica si el elemento es una carpeta y está vacía
    vacios_de_elemento = 0
    if os.path.isdir(ruta_elemento) and os.listdir(ruta_elemento):
        exceles = []
        exceles = list(os.listdir(ruta_elemento))
        for excel in exceles:
            if excel[-4:] == "xlsx":
                tamano = pd.read_excel(os.path.join(ruta_elemento, excel)).shape[0]
                if tamano == 0:
                    vacios_de_elemento += 1
    if vacios_de_elemento >= 35:
        print("la carpeta " + elemento + " tiene solo exceles vacíos")
        print(ruta_elemento)
        
        shutil.rmtree(ruta_elemento)
    # print(vacios_de_elemento)

# Imprime la lista de nombres de archivos
# print(nombres_archivos)
# a = pd.DataFrame(columns=["a","b"])

# a.shape[0] == 0