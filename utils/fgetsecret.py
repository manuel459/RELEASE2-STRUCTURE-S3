import json
def get_secret(secret_session, env):

    secret_name = f"fndtifrs17scmdb{env}01"
    region_name = "us-east-1"

    # Create a Secrets Manager client
    client = secret_session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        # For a list of exceptions thrown, see
        # https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
        logging.exception('Ocurrio un error al extraer las credenciales del secret manager')
        raise e

    # Decrypts secret using the associated KMS key.
    secret = get_secret_value_response['SecretString']
    secret = json.loads(secret)

    db_username = secret.get('username')
    db_password = secret.get('password')
    db_url = secret.get('host')
    db_name = ((secret.get('dbClusterIdentifier')).split('-'))[0]
    db_port = secret.get('port')
    db_engine = secret.get('engine')
    
    # Your code goes here.
    connection = {
        'url': f'jdbc:{db_engine}ql://{db_url}:{db_port}/{db_name}',
        'user': db_username,
        'password': db_password,
        'driver': 'org.postgresql.Driver'
    }
    
    return connection