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
glue_client = boto3.client('glue')
id = 'ENTIDADES'
nombre_error = '-'

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
    #   OBTENER LA FECHA INICIO DEL JOB
    #-------------------------------------#
    job_name = f'fndtifrs17glueentidades{env}02'
    
    response = glue_client.get_job_runs(JobName=job_name, MaxResults=1)
    
    last_run = response['JobRuns'][0]

    last_start_time = last_run['StartedOn']
    
    #-------------------------------------#
    #   EXTRAER CONFIGURACIONES DYNAMODB
    #-------------------------------------#
    
    #CREAR UNA LISTA CON TODAS LAS CONFIGURACIONES NECESARIAS SEGUN EL DOMINIO
    l_configuraciones = [{ "DOMINIO": "GENERAL" , "COLUMNA": "ESTRUCTURA" }, { "DOMINIO": "ENTIDADES" , "COLUMNA": "NEGOCIO" }]
    
    #NOMBRE DE LA TABLA DE CONFIGURACIONES
    
    nombre_tabla = f'fndtifrs17dydb{env}01'
    
    #CREAR UN DICCIONARIO PARA ESTABLECER LAS CONFIGURACIONES
    l_dic_config = {}

    #EXTRAER CONFIGURACIONES
    l_dic_config = extract_config(l_configuraciones, nombre_tabla)
    
    print(l_dic_config)
    
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
    trazabilidad.update_log(cliente_dynamodb, id, 1, 1, nombre_error,last_start_time,tipo_carga)
                 
    #--------------------------------------------------#
    #    EJECUCIÓN DEL GRUPO DE INFORMACIÓN PRODUCTOS
    #--------------------------------------------------#
    
    for config in l_dic_config['ENTIDADES']['path_file_tmp']:
        if config['flag'] == 1:
            if tipo_carga == 'INI' or tipo_carga == 'INC':
                #VALIDAR EL TIPO DE CARGA : INI = INICIAL | INC = INCREMENTAL
                if tipo_carga == 'INI':
                    script_key = config['script_inicial']
                elif tipo_carga == 'INC':
                    script_key = config['script_incremental']
                
                structure = execute_script(l_dic_config['GENERAL']['bucket']['artifact'], script_key)
                
                #LLAMAR Y LANZAR LOS PARAMETROS A LA FUNCION getData
                L_DF_ENTIDAD = structure.get_data(glueContext, l_dic_config['GENERAL']['bucket']['artifact'] ,config['tablas'],l_dic_config['GENERAL']['fechas']['dFecha_Inicio'], l_dic_config['GENERAL']['fechas']['dFecha_Fin'])
                
                print(L_DF_ENTIDAD)
            
                #Trasformar a bit escrito en formato txt
                L_BUFFER_ENTIDAD = io.BytesIO()
                L_DF_ENTIDAD.toPandas().to_parquet(L_BUFFER_ENTIDAD, index=False)
                L_BUFFER_ENTIDAD.seek(0)
                
                # Escribir el objeto Parquet en S3
                s3_client.put_object(
                    Bucket = l_dic_config['GENERAL']['bucket']['artifact'],
                    Key = config['path'],
                    Body=L_BUFFER_ENTIDAD.read()
                )
            
    #------------------------------------------------------------------------#        
    # EJECUTAR LA TRAZABILIDAD
    #------------------------------------------------------------------------#
    trazabilidad.update_log(cliente_dynamodb, id, 2,1,nombre_error, last_start_time,tipo_carga)

except Exception as e:
    # Log the error for debugging purposes
    print(f"Error Glue de Regla de Negocio del Dominio de ENTIDADES: {str(e)}")
    nombre_error = str(e)
    trazabilidad.update_log(cliente_dynamodb, id, 2,1,nombre_error, last_start_time,tipo_carga)
    sys.exit(1)

job.commit()