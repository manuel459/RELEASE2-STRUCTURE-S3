import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import boto3
import io
import json
import importlib.util
from boto3.dynamodb.conditions import Key

#INICIALIZAR OBJETOS
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
job = Job(glueContext)
job.init(args["JOB_NAME"], args)
s3_client = boto3.client('s3')
s3r = boto3.resource('s3')
cliente_dynamodb = boto3.client("dynamodb")
ssm = boto3.client('ssm', 'us-east-1')

def execute_script(name_bucket, name_object):
    path_temp = 'fileTemp.py'
    s3r.Object(name_bucket, name_object).download_file(path_temp)
    #importando
    spec = importlib.util.spec_from_file_location('module', path_temp)
    structure = importlib.util.module_from_spec(spec)
    #lee mi script
    spec.loader.exec_module(structure)
    return structure

def get_ssm():
        parameter_name = "/configuracion/variable/entorno"
        response = ssm.get_parameter(
                Name=parameter_name,
                WithDecryption=True  # Descifra el valor si es un SecureString
            )
        return response['Parameter']['Value']
    
#EXTRACCION DE LAS CONFIGURACIONES Y RETORNARLAS EN UN DICCIONARIO DE DATOS        
def extract_config(l_configuraciones, nombre_tabla):
    #CREAR UN DICCIONARIO PARA ESTABLECER LAS CONFIGURACIONES
    l_dic_config = {}
    
    #Extraer las CONFIGURACIONES NECESARIAS segun la lista
    for dominio in l_configuraciones:
        config = cliente_dynamodb.get_item(TableName=nombre_tabla, Key={'NOMBRE_DOMINIO': {'S': str(dominio['DOMINIO'])}})
        l_dic_config[config['Item']['NOMBRE_DOMINIO']['S']] = json.loads(config['Item'][dominio['COLUMNA']]['S'])
       
    return l_dic_config

try:
    #-------------------------------------#
    #   EXTRAER VARIABLES DE ENTORNO
    #-------------------------------------#
    env = get_ssm()
    
    #-------------------------------------#
    #   EXTRAER CONFIGURACIONES DYNAMODB
    #-------------------------------------#
    
    #CREAR UNA LISTA CON TODAS LAS CONFIGURACIONES NECESARIAS SEGUN EL DOMINIO
    l_configuraciones = [{ "DOMINIO": "GENERAL" , "COLUMNA": "ESTRUCTURA" }, { "DOMINIO": "PRODUCTOS" , "COLUMNA": "NEGOCIO" }]
    
    #NOMBRE DE LA TABLA DE CONFIGURACIONES
    
    nombre_tabla = f'TablaTestIFRS17'
    
    #CREAR UN DICCIONARIO PARA ESTABLECER LAS CONFIGURACIONES
    l_dic_config = {}

    #EXTRAER CONFIGURACIONES
    l_dic_config = extract_config(l_configuraciones, nombre_tabla)
    
    print(l_dic_config)
                 
    #--------------------------------------------------#
    #    EJECUCIÓN DEL GRUPO DE INFORMACIÓN PRODUCTOS
    #--------------------------------------------------#
    for config in l_dic_config['PRODUCTOS']['path_file_tmp']:
        if config['flag'] == 1:
            #OBTENER SCRIPTS ALMACENADOS EN S3
            structure = execute_script(l_dic_config['GENERAL']['bucket']['artifact'], config['script'])

            #LLAMAR Y LANZAR LOS PARAMETROS A LA FUNCION getData
            L_DF_PRODUCTOS = structure.get_data(glueContext, l_dic_config['GENERAL']['bucket']['artifact'] ,config['tablas'])
        
            #Trasformar a bit escrito en formato txt
            L_BUFFER_PRODUCTOS = io.BytesIO()
            L_DF_PRODUCTOS.toPandas().to_parquet(L_BUFFER_PRODUCTOS, index=False)
            L_BUFFER_PRODUCTOS.seek(0)
            
            # Escribir el objeto Parquet en S3
            s3_client.put_object(
                Bucket = l_dic_config['GENERAL']['bucket']['artifact'],
                Key = config['path'],
                Body=L_BUFFER_PRODUCTOS.read()
                )

except Exception as e:
    # Log the error for debugging purposes
    print(f"Error: {str(e)}")

job.commit()