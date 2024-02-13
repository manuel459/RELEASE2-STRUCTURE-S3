from datetime import datetime
import pytz

limaTz = pytz.timezone("America/Lima")
table = 'fndtifrs17dydbd02'
steps = ['LECTURA', 'R.NEGOCIO','ESTRUCTURA']

def get_log(cliente_dynamodb, id):
    
    log = cliente_dynamodb.get_item(TableName=table, Key={"DOMINIO": {"S": id}})

    return log

def update_log(cliente_dynamodb, id, step,number,nombre_error,last_start_time,tipo_carga):

    log = get_log(cliente_dynamodb, id)

    last_start_time = last_start_time.astimezone(limaTz)

    fecha_inicio = last_start_time.astimezone(limaTz).strftime('%Y/%m/%d %I:%M:%S')

    if step == 1:
        cliente_dynamodb.update_item(
            TableName=table,
            Key={
                "DOMINIO": {"S": id}
            },
            UpdateExpression="set FECHA_INICIO = :fi, FECHA_FIN = :ff, STEP = :st, TIPO_CARGA = :tc,ERROR_MESSAGE = :ne,TIEMPO = :te,T_G_LECTURA = :tgl",
            ExpressionAttributeValues={
                ':fi': {'S': fecha_inicio},
                ':ff': {'S': 'ESPERE'},
                ':st': {'S': steps[number]},
                ':tc': {'S': tipo_carga},
                ':ne': {'S': '-'},
                ':te': {'S': ' '},
                ':tgl':{'S': ' '}
            }
        )

    else:
        
        fecha_fin = datetime.now(limaTz).strftime('%Y/%m/%d %I:%M:%S')
        fecha_inicio_dt = last_start_time.astimezone(limaTz)
        fecha_fin_dt = datetime.now(limaTz)
        diferencia = fecha_fin_dt - fecha_inicio_dt
        fecha_t_g_l = f"{fecha_fin} - {str(diferencia)}"

        cliente_dynamodb.update_item(
            TableName=table,
            Key={
                "DOMINIO": {"S": id}
            },
            UpdateExpression= "set FECHA_FIN = :ff, ERROR_MESSAGE = :ne, TIEMPO = :te,T_G_LECTURA = :tgl",
            ExpressionAttributeValues= {
                ':ff': {'S': fecha_fin},
                ':ne': {'S': nombre_error},
                ':te': {'S': str(diferencia)},
                ':tgl':{'S':fecha_t_g_l},
            }
        )