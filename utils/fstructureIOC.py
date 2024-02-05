import pyarrow.parquet as pq
import pandas as pd
import boto3
import io
from datetime import datetime

s3_client = boto3.client('s3')

def get_header():
  return pd.DataFrame({"resultado" : [f"C|{datetime.now().strftime('%Y%m%d')}|{'1'.zfill(9)}"]})

def get_footer(num):
  return pd.DataFrame({"resultado" : [f"R|{str(num).zfill(10)}"]})

def generate_files(l_dic_config, domain):
   for l_sistema_origen in l_dic_config[domain]['path_file_txt']:
    
    # LISTA DE OBJETOS DE LA CARPETA ESPECIFICADA
    L_LIST_GROUP = s3_client.list_objects_v2(Bucket=l_dic_config['GENERAL']['bucket']['artifact'], Prefix=l_dic_config[domain]['path_origen'])
    
    #DECLARA DATAFRAME
    L_DF_GRUPO_FINAL = pd.DataFrame(columns=['resultado'])
    
    #ITERAR LA LISTA CON LOS ARCHIVOS DEL GRUPO
    for l_obj in L_LIST_GROUP['Contents']:
        # OBTENER EL NOMBRE DEL ARCHIVO
        L_FILE_NAME = l_obj['Key']
        
        #ADMITIR ARCHIVOS PARQUET
        if L_FILE_NAME.endswith('.parquet'):

            # LEER EL ARCHIVO S3 A PARQUET 
            L_OBJECT_S3 = s3_client.get_object(Bucket=l_dic_config['GENERAL']['bucket']['artifact'], Key=L_FILE_NAME)
            L_PARQUET_DATA = io.BytesIO(L_OBJECT_S3['Body'].read())
         
            # LEER EL ARCHIVO PARQUET A UN DATAFRAME
            L_PQ_FILE = pq.read_table(L_PARQUET_DATA)
            L_DF_GRUPO = L_PQ_FILE.to_pandas()
            print(len(L_DF_GRUPO))
        
            # FILTRAR POR SISTEMA ORIGEN ENTRANTE 
            L_DF_GRUPO = L_DF_GRUPO[L_DF_GRUPO['kgiorigm'] == str(l_sistema_origen['sistema_origen'])]

            # FORMAR ESTRUCTURA CON | DE SEPARADOR
            L_DF_GRUPO['resultado'] = L_DF_GRUPO.apply(lambda row: '|'.join(map(str, row)), axis=1)
        
            # AGREGAR LOS DATOS AL DATAFRAME FINAL
            L_DF_GRUPO_FINAL = pd.concat([L_DF_GRUPO_FINAL, L_DF_GRUPO[['resultado']]], axis=0)

    L_DF_GRUPO_FINAL.reset_index(drop=True, inplace=True)

    #CONTAR LOS REGISTROS POR ITERACIÓN
    L_RECORD_REGISTER = len(L_DF_GRUPO_FINAL)
    
    #CABECERA
    L_CABECERA = get_header()

    #FOOTER
    L_FOOTER = get_footer(L_RECORD_REGISTER)
    
    #UNION Y ORDENAMIENTO DE TODA LA ESTRUCTURA
    dataframe_group = pd.concat([L_CABECERA, L_DF_GRUPO_FINAL ,L_FOOTER], axis=0)
        
    #Trasformar a bit escrito en formato txt
    file = convert_to_txt(dataframe_group)
        
    # Escribir y guardar el objeto Parquet en S3
    bucket_config = l_dic_config['GENERAL']['bucket']['dimensiones']
    path_config = l_sistema_origen['path']

    put_to_s3(bucket_config, path_config, file)

    # limpiar objetos temporales de los S3
    #delete(l_dic_config['GENERAL']['bucket']['artifact'],l_dic_config[domain]['path_origen'])

def convert_to_txt(dataframe_group):
  #Trasformar a bit escrito en formato txt
  buffer = io.BytesIO()
  dataframe_group.to_csv(buffer, index=False, header=False)
  buffer.seek(0)
  return buffer
       
def put_to_s3(bucket, path, file):
   s3_client.put_object(
        Bucket = str(bucket),
        Key    = f"{path}.{datetime.now().strftime('%Y%m%d%H%M%S')}.txt",
        Body   = file.read()
   )

def delete(bucket,path):
  response = s3_client.list_objects_v2(Bucket=bucket, Prefix=path)
  for object in response['Contents']:
    s3_client.delete_object(
      Bucket= str(bucket),
      Key=object['Key']
      )
   




   
