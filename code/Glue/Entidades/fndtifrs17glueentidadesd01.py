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
from datetime import datetime, timedelta

#INICIALIZAR OBJETOS
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)
s3_client = boto3.client('s3')
s3r = boto3.resource('s3')
secretSession = boto3.session.Session()
cliente_dynamodb = boto3.client("dynamodb")
ssm = boto3.client('ssm', 'us-east-1')
glue_client = boto3.client('glue')
id = 'ENTIDADES'
nombre_error = '-'

#FUNCIÓN PARA EJECUTAR UN SCRIPT GUARDADO EN UN BUCKET S3
def execute_script(name_bucket, name_object):
        path_temp = 'fileTemp.py'
        s3r.Object(name_bucket, name_object).download_file(path_temp)
        
        #importando
        spec = importlib.util.spec_from_file_location('module', path_temp)
        structure = importlib.util.module_from_spec(spec)
        
        #lee mi script
        spec.loader.exec_module(structure)
        return structure

#EXTRACCION DE LAS CONFIGURACIONES Y RETORNARLAS EN UN DICCIONARIO DE DATOS        
def extract_config(l_configuraciones, nombre_tabla):
    #CREAR UN DICCIONARIO PARA ESTABLECER LAS CONFIGURACIONES
    l_dic_config = {}
    
    #Extraer las CONFIGURACIONES NECESARIAS segun la lista
    for dominio in l_configuraciones:
        config = cliente_dynamodb.get_item(TableName=nombre_tabla, Key={'NOMBRE_DOMINIO': {'S': str(dominio['DOMINIO'])}})
        l_dic_config[config['Item']['NOMBRE_DOMINIO']['S']] = json.loads(config['Item'][dominio['COLUMNA']]['S'])
       
    return l_dic_config
    
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
    #   OBTENER LA FECHA INICIO DEL JOB
    #-------------------------------------#
    job_name = f'fndtifrs17glueentidades{env}01_test'
    
    response = glue_client.get_job_runs(JobName=job_name, MaxResults=1)
    
    last_run = response['JobRuns'][0]

    last_start_time = last_run['StartedOn']
    
    #-------------------------------------#
    #   EXTRAER CONFIGURACIONES DYNAMODB
    #-------------------------------------#
    
    #CREAR UNA LISTA CON TODAS LAS CONFIGURACIONES NECESARIAS SEGUN EL DOMINIO
    l_configuraciones = [{ "DOMINIO": "GENERAL" , "COLUMNA": "ESTRUCTURA" }, { "DOMINIO": "ENTIDADES" , "COLUMNA": "LECTURA" }]
    
    #NOMBRE DE LA TABLA DE CONFIGURACIONES
    
    nombre_tabla = 'TablaTestIFRS17'
    
    #EXTRAER CONFIGURACIONES
    l_dic_config = extract_config(l_configuraciones, nombre_tabla)
    
    #--------------------------------------#
    #  CONTROL DE EJECUCION DYNAMODB
    #--------------------------------------#
    
    #EXTRAR LA FUNCION DE LA TRAZABILIDAD 
    trazabilidad = execute_script(l_dic_config['GENERAL']['bucket']['artifact'],l_dic_config['GENERAL']['funciones']['log'])
    
    #EXTRAR EL TIPO DE CARGA DEL DYNAMODB
    tipo_carga = l_dic_config['GENERAL']['tipoCarga']

    #------------------------------------------------------------------------#        
    #  EJECUTAR LA TRAZABILIDAD
    #------------------------------------------------------------------------#
    #  Parametros
    #  cliente_dynamodb: cliente, 
    #  id: nombre del dominio,
    #  step: codigo 1 = Inicio del proceso - codigo 2 = Fin del proceso
    #  number: Job Actual(Lectura = 0, Regla de negocio = 1, Estructura = 2) 
    #  nombre_error : Captura el Error
    #  last_start_time : Captura la fecha del proceso
    #  tipo_carga : Tipo de carga INCREMENTAL O INICIAL
    #------------------------------------------------------------------------#
    trazabilidad.update_log(cliente_dynamodb, id, 1,0, nombre_error,last_start_time,tipo_carga)
    
    #--------------------------------------#
    #  CONEXIÓN A LA BASE DE DATOS AURORA
    #--------------------------------------#
    structure = execute_script(l_dic_config['GENERAL']['bucket']['artifact'], l_dic_config['GENERAL']['funciones']['secret'])

    #validar secreto para generar la conexion al RDS
    connection = structure.get_secret(secretSession, env)
    print(connection)
    
    #--------------------------------------------------#
    #    EJECUCIÓN DEL GRUPO DE INFORMACIÓN ENTIDADES
    #--------------------------------------------------#
    
    print(l_dic_config['ENTIDADES']["ENTIDADES"].items())
    #ITERAR TODOS LOS SCRIPTS SEGUN LA COMPAÑIA
    for producto, values in l_dic_config['ENTIDADES']["ENTIDADES"].items():
        
        if values['flag'] == 1:
            
            #extraer la funcion base del script de cada compañia en el path
            script = execute_script(l_dic_config['GENERAL']['bucket']['artifact'], values['path'])
        
            #parametros 1: Nombre_bucket_destino 2: lista de tablas, 3: ContextGlue , 4: Conexion a base de datos , 5 : Cliente de S3, 6: IO 
            L_DF = script.generate_entidad_parquets(l_dic_config['GENERAL']['bucket']['artifact'], values['tablas'], glueContext, connection, s3_client, io)
            
    #------------------------------------------------------------------------#        
    # EJECUTAR LA TRAZABILIDAD
    #------------------------------------------------------------------------#
    trazabilidad.update_log(cliente_dynamodb, id, 2,0,nombre_error, last_start_time,tipo_carga)

except Exception as e:
    # Log the error for debugging purposes
    print(f"Error Glue de lectura del Dominio de ENTIDADES: {str(e)}")
    nombre_error = str(e)
    trazabilidad.update_log(cliente_dynamodb, id, 2,0,nombre_error, last_start_time,tipo_carga)
    sys.exit(1)

job.commit()