import os
import re
import pickle
import pandas as pd
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaFileUpload
from datetime import datetime

def subir_a_drive(path):
    dia = datetime.now().day
    mes = datetime.now().month
    año = datetime.now().year

    SCOPES = ['https://www.googleapis.com/auth/drive']

    # Autenticación
    creds = None
    if os.path.exists('token.pickle'):
        with open('token.pickle', 'rb') as token:
            creds = pickle.load(token)

    if not creds or not creds.valid:
        flow = InstalledAppFlow.from_client_secrets_file('creds2.json', SCOPES)
        creds = flow.run_local_server(port=0)
        with open('token.pickle', 'wb') as token:
            pickle.dump(creds, token)

    service = build('drive', 'v3', credentials=creds)

    # Función para subir un archivo a una carpeta específica
    def upload_to_folder(folder_id, filename):
        df = pd.read_excel(filename)
        if df.empty:
            # print(f"El archivo {filename} está vacío y no se subirá.")
            return

        media = MediaFileUpload(filename)
        request = service.files().create(media_body=media, body={
            'name': os.path.basename(filename),
            'parents': [folder_id]
        })

        try:
            file = request.execute()
            print(f'Archivo {filename} subido correctamente con ID: {file.get("id")}')
        except HttpError as error:
            print(f'Ha ocurrido un error al subir el archivo {filename}: {str(error)}')

    # Lista de archivos sin ordenar
    files = [os.path.join(path, f) for f in os.listdir(path) if os.path.isfile(os.path.join(path, f))]

    # Ordenar la lista de archivos
    files = list(sorted(files, key=lambda x: int(re.search(r'GRUPO(\d+)', x).group(1)) if re.search(r'GRUPO(\d+)', x) else float('inf')))

    folder_ids = ["1GpxN_g67E0Knv7ZLn7kPRbi2UmfUVvf2", "17wcfDgZ845YaHOxUVAXmWUmuczq488BF", 
                  "1cGCdFA3Z3KhlWptDAM7sPZWdiNooNM1b", "11_vpDaoRpqhQa_DSwpYb3ZbG9xQzhmEi",
                  "1hFuQ_Ku6etXD0AqcXJGvJ0EHWtv5-a0_", "1ZO6nCuqoq_svJM6nwCEmV5o4qs45V4v_",
                  "1IuJ3eyumCsNCMXfwDCQc-X0CoiN0mtxR", "1A6pPWlukIKm4t51qFVVNBF2C-ILeOW-g",
                  "1LuuoRaJUXCBuknLwCG5RZ_OLdvWeH6vT", "18jiONAOXChea3ZQ-YZ88jdeOUaEWu2Ha",
                  "1Yw3akBEgwaJQLm4HFKs25Jpw4mtNF8Ba", "12LgkIqzf5ekODHf5V2VerpFUHFVBtRDk",
                  "11tAwIphvrrrNt-ta22edAQJIEjYBPrSF", "1n2yVmQ2764Ve1sDGkJRQdjps_r2W7o7W",
                  "1TPoUxqFlRn_cS549o55mrGU7Pgm9FfSD", "1qjwISv5o4ziIXDicwaj2u9yj3516qy2X",
                  "1uymvnmhMbNW-1Jnts2DZiEGxyUe7lpW2", "1yOlFTJFX49jTxoE7rwUPtzF0_O2hT981",
                  "1prwl8vWDMLxHL9K5A7GS28xrYsKwUkoc", "1xfDbm9yBX5AJHkNVyoHgR20cU4rSylJl",
                  "16zF8jOB_UEDzrGkN0S0gDfYzh0My42da", "1AdaH2pslrrpYuwgRPNHO7mKXoxm2-3i4",
                  "1vnV-ZG8wV2rsqhchdL-smq40Umou2kOi", "1FgEQUJqGMji-ZgZC2iJRt_tSIAIXeIc9",
                  "1PPtX91YC8-WjaVnYwjCXtH5hOj8VEC0T", "1446aFRdTnCenRGTZ8esk_lKDUxPX0vrr",
                  "1k2Zi8lb0PZhSYnzYkTHeYWNplAk2H-H3", "19NU24P_peExuvFQOQ7TkpP2tvJiUPuPp",
                  "1PHa2uLyxx4kWef6SRv3d3LBBwNiX6PmC", "1EXOrQt22liQhi7aKbiO7Si_VJ2jx9M35",
                  "1bl8QuCiGstj2pp94bb8Dlt80raY2LdMz", "1GYD1TRsPAeNQAheFx1KXJSPT5SQ6wVeS",
                  "1h00hc4CIXp6CojAckJn23MKUrtVX5xZx", "1DwFSvYGWfZCc53iPCFe-oFpcXnVaJ5GP",
                  "1P4e7uriJASRfE3BKasUYA-iTb4rbgSp-"]
    

    for file, folder_id in zip(files, folder_ids):
        upload_to_folder(folder_id, file)