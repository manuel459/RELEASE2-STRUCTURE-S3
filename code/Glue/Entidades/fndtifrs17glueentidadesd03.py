import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, concat, concat_ws, lit, trim, StringType, coalesce, expr
from datetime import datetime
from pyspark.sql import Row
import boto3
import io
import json
import importlib.util

#INICIALIZAR OBJETOS
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)
cliente_dynamodb = boto3.client("dynamodb")
s3r = boto3.resource('s3')
ssm = boto3.client('ssm', 'us-east-1')

#EXTRACCION DE LAS CONFIGURACIONES Y RETORNARLAS EN UN DICCIONARIO DE DATOS        
def extract_config(l_configuraciones, nombre_tabla):
    #CREAR UN DICCIONARIO PARA ESTABLECER LAS CONFIGURACIONES
    l_dic_config = {}
    
    #Extraer las CONFIGURACIONES NECESARIAS segun la lista
    for dominio in l_configuraciones:
        config = cliente_dynamodb.get_item(TableName=nombre_tabla, Key={'NOMBRE_DOMINIO': {'S': str(dominio['DOMINIO'])}})
        l_dic_config[config['Item']['NOMBRE_DOMINIO']['S']] = json.loads(config['Item'][dominio['COLUMNA']]['S'])
       
    return l_dic_config

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

try:
    #-------------------------------------#
    #   EXTRAER VARIABLES DE ENTORNO
    #-------------------------------------#
    env = get_ssm()
    
    #-------------------------------------#
    #   EXTRAER CONFIGURACIONES DYNAMODB
    #-------------------------------------#
    
    #CREAR UNA LISTA CON TODAS LAS CONFIGURACIONES NECESARIAS SEGUN EL DOMINIO
    l_configuraciones = [{ "DOMINIO": "GENERAL" , "COLUMNA": "ESTRUCTURA" }, { "DOMINIO": "ENTIDADES" , "COLUMNA": "ESTRUCTURA" }]
    
    #NOMBRE DE LA TABLA DE CONFIGURACIONES
    #nombre_tabla = f'fndtifrs17dydb{env}01'
    nombre_tabla = f'TablaTestIFRS17'
    
    #EXTRAER CONFIGURACIONES
    l_dic_config = extract_config(l_configuraciones, nombre_tabla)
    
    #----------------------------------------------------------#
    #   EJECUTAR FUNCION QUE ESTRUCTURA LOS FILES IFRS17 A TXT
    #----------------------------------------------------------#
    #OBTENER LAS FUNCIONES ALMACENADAS EN S3
    structure = execute_script(l_dic_config['GENERAL']['bucket']['artifact'], l_dic_config['GENERAL']['funciones']['structure'])
    
    #LLAMAR Y LANZAR LOS PARAMETROS A LA FUNCION generate_files
    domain = 'ENTIDADES'
    structure.generate_files(l_dic_config, domain)
 
except Exception as e:
    # Log the error for debugging purposes
    print(f"Error Glue de Estructura del Dominio de Entidades: {str(e)}")
    sys.exit(1)

job.commit()