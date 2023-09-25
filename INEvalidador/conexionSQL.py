from sqlalchemy import create_engine, text
import pandas as pd
import os
from datetime import datetime

from .utils import columnas_a_mayuscula, condicion_a_variables

class baseSQL:
    def __init__(self, descargar: bool=True):
        if descargar:
            # Parámetros de conexión
            usuario = 'rrcastillo'
            contraseña = 'Rcastillo2023'
            host = '20.10.8.4'
            puerto = '3308'
            # Crear la conexión de SQLAlchemy
            engine = create_engine(f'mysql+mysqlconnector://{usuario}:{contraseña}@{host}:{puerto}/encabih')
            self.__conexion = engine.connect()
            self.extraer_base()
        # Diccionario para almacenar los nombres de los archivos y las columnas
        self.base_df = {}   # Diccionario que asocia nombre de df con el df
        self.base_col = {}  # Diciconario que asocia variable con el nombre del df

        # Recorre todos los archivos en el directorio especificado
        for archivo in os.listdir('db'):
            if archivo.endswith('.feather'):  # Verifica si el archivo es un archivo Feather
                ruta_completa = os.path.join('db', archivo)
                # Lee el archivo Feather
                df = pd.read_feather(ruta_completa)
                df = columnas_a_mayuscula(df)
                # Agrega el nombre del archivo y las columnas al diccionario
                nombre_df = archivo.replace('.feather', '')
                self.base_df[nombre_df] = df
                # Agregar columnas a base_col
                columnas = df.columns.tolist()
                try:
                    columnas.remove('LEVEL-1-ID')
                except:
                    pass
                try:
                    columnas.remove('INDEX')
                except:
                    pass
                for col in columnas:
                    self.base_col[col] = nombre_df

    def df_equals_in_list(self,target_df, df_list):
        for df in df_list:
            if df.equals(target_df):
                return True
        return False

    def df_para_condicion(self, condicion: str, fecha_inicio, fecha_final):
        # PR, tomar primera ronda
        variables = condicion_a_variables(condicion)

        df_a_unir = list(set([self.base_col.get(var) for var in variables]))
        # tipo = df_a_unir[0][-2:] # devuelve SR o PR
        
        df_a_unir = [self.base_df.get(archivo) for archivo in df_a_unir] 

        df_base = self.base_df.get(f'level-1')
        for df in df_a_unir:
            df = df.drop('INDEX', axis=1)
            if "OCC" in df.columns.tolist():
                df = df.drop('OCC', axis=1)
            df_base = pd.merge(df_base, df, on='LEVEL-1-ID', how='inner')

        df_cases = self.base_df.get(f'cases')
        df_base = pd.merge(df_base, df_cases, left_on='CASE-ID', right_on='ID', how='inner')
        # Puede que el nombre de la variable DELETED cambie para la base de ENCABIH
        df_base = df_base.query('DELETED == 0')

        # Agregar dataframe con la caratula
        caratula_df = pd.read_feather('db/caratula.feather')
        caratula_df = columnas_a_mayuscula(caratula_df)
        # Agregar dataframe con las fechas
        visitas_df = pd.read_feather("db/visitas.feather")
        visitas_df = columnas_a_mayuscula(visitas_df)
        # Agregar dataframe con control_entrevista
        control_df = pd.read_feather("db/control_entrevista.feather")
        control_df = columnas_a_mayuscula(control_df)
        # Unir a base raiz
        df_base = pd.merge(df_base, visitas_df, on="LEVEL-1-ID", how="inner")
        df_base = df_base.drop("INDEX", axis=1)
        df_base = pd.merge(df_base, control_df, on="LEVEL-1-ID", how="inner")
        df_base = df_base.drop("INDEX", axis=1)
        df_base = pd.merge(df_base, caratula_df, on='LEVEL-1-ID', how='inner')  # Unión por 'LEVEL-1-ID'
        
        # Si tipo es "SR", agregamos el dataframe "estado_de_boleta_SR.feather"
        # if tipo == 'SR':
        #     # Agregar dataframe estado boleta
        #     estado_boleta_df = pd.read_feather('db/estado_de_boleta_SR.feather')
        #     estado_boleta_df = columnas_a_mayuscula(estado_boleta_df)
        #     # Agregar dataframe de control de tiempo
        #     tiempo_sr_df = pd.read_feather("db/control_tiempo_SR.feather")
        #     tiempo_sr_df = columnas_a_mayuscula(tiempo_sr_df)
        #     # Unir a base raiz
        #     df_base = pd.merge(df_base, tiempo_sr_df, on="LEVEL-1-ID", how="inner")
        #     df_base = df_base.drop("INDEX",axis=1)
        #     df_base = pd.merge(df_base, estado_boleta_df, on='LEVEL-1-ID', how='inner')  # Unión por 'LEVEL-1-ID'

        # Validar solo las encuestas terminadas
        if "P01D08" in df_base.columns and "P01D07" in df_base.columns: 
            df_base = df_base[df_base["P01D08"] == 1]
        # if "ESTADO_SR" in df_base.columns:
        #     df_base = df_base[df_base["ESTADO_SR"] == 1]
        if "P01D07" in df_base.columns and "P01D08" not in df_base.columns: 
            df_base = df_base[df_base["P01D07"] == 1] 
        # Se eliminó el filtro de estado_pr

        # Agregar código CP = 0 para las validaciones de hogares
        if "CP" not in df_base.columns:
            if "CP_ELEGIDA" in df_base.columns:
                df_base["CP"] = df_base["CP_ELEGIDA"]
            else:
                df_base["CP"] = 0

        # Agregar filtrado por fecha tomando el capítulo 1 como inicio de la encuesta
        # if "FECHA_INICIO_BOLETA" in df_base.columns: # CAMBIAR NOMBRE DE VARIABLE DE FECHA DE INICIO DEL CAPÍTULO
        
        # Agregar columna con la fecha ya calculada de las variables P01D04A/B/C
        df_base["P01D04A"] = df_base["P01D04A"].astype("Int32").astype(str) # Día
        df_base["P01D04B"] = df_base["P01D04B"].astype("Int32").astype(str) # Mes
        df_base["P01D04C"] = df_base["P01D04C"].astype("Int32").astype(str) # Año
        df_base["FECHA_INICIO_BOLETA"] = df_base["P01D04A"] + "-" + df_base["P01D04B"] + "-" + df_base["P01D04C"]
        df_base["FECHA_INICIO_BOLETA"] = pd.to_datetime(df_base["FECHA_INICIO_BOLETA"], format="%d-%m-%Y")

        df_base = df_base[(df_base["FECHA_INICIO_BOLETA"] >= fecha_inicio) & (df_base["FECHA_INICIO_BOLETA"] <= fecha_final)]
        # if "FECHA_INICIO_CAPXIIIA" in df_base.columns:
        #     df_base["FECHA_INICIO_CAPXIIIA"] = pd.to_datetime(df_base["FECHA_INICIO_CAPXIIIA"])
        #     df_base = df_base[(df_base["FECHA_INICIO_CAPXIIIA"] >= fecha_inicio) & (df_base["FECHA_INICIO_CAPXIIIA"] <= fecha_final)]

        for columna in df_base.columns:
            if columna[-2:] == "_y":
                df_base.drop(columns=columna, inplace=True)
            if columna[-2:] == "_x":
                df_base = df_base.rename(columns={columna : columna[0:-2]})

        return df_base


    def info_tablas(self):
            conexion = self.__conexion 

            resultado = conexion.execute(text("SHOW TABLES"))
            tablas = [row[0] for row in resultado]

            i = 0
            for tabla_nombre in tablas:
                try:
                    # Usar text en las consultas
                    filas = conexion.execute(text(f"SELECT COUNT(*) FROM `{tabla_nombre}`")).fetchone()[0]
                    columnas = conexion.execute(text(f"SELECT COUNT(*) FROM information_schema.columns WHERE table_name = '{tabla_nombre}'")).fetchone()[0]

                    i += 1
                    print(f"> {tabla_nombre}({i})\n   Filas: {filas} - Columnas: {columnas}")
                except Exception as e:
                    print(f'> Error "{e}" al obtener la forma de la tabla {tabla_nombre}')

    def tablas_a_feather(self, dir_salida: str = 'output'):
        conexion = self.__conexion

        resultado = conexion.execute(text("SHOW TABLES"))
        tablas = [row[0] for row in resultado]

        # Convertir cada tabla en un DataFrame y exportarlo en formato feather
        for tabla_nombre in tablas:
            try:
                # Formatear el nombre de la tabla correctamente con comillas invertidas
                tabla_con_comillas = f'`{tabla_nombre}`'
                df = pd.read_sql(text(f'SELECT * FROM {tabla_con_comillas}'), con=conexion)


                # Crear el directorio de salida si no existe 
                if not os.path.exists(dir_salida):
                    os.makedirs(dir_salida)
                df.reset_index(inplace=True)
                # Exportar el DataFrame en formato feather
                tabla_nombre = f"{tabla_nombre}"
                df.to_feather(os.path.join(dir_salida, f'{tabla_nombre}.feather'))

            except Exception as e:
                print(f'> Error al convertir la tabla {tabla_nombre} en un DataFrame y exportarlo: {str(e)}')
        
    def extraer_base(self):
        self.tablas_a_feather('db')
        # self.tablas_a_feather('SR', 'db')
