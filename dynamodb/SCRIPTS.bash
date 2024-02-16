AWS DYNAMO
--CREAR UNA TABLA EN AWS DYNAMODB DESDE EL COMANDO CLI

aws dynamodb create-table \
  --table-name TablaTrazabilidadIFRS17 \
  --attribute-definitions \
    AttributeName=DOMINIO,AttributeType=S \
  --key-schema \
    AttributeName=DOMINIO,KeyType=HASH \
  --provisioned-throughput \
    ReadCapacityUnits=5,WriteCapacityUnits=5


 --CREAR ITEMS EN AWS DYNAMODB DESDE EL COMANDO CLI
aws dynamodb batch-write-item \
    --request-items '{
        "TablaTrazabilidadIFRS17": [
            {
                "PutRequest": {
                    "Item": {
                        "DOMINIO": {"S": "ENTIDADES"}, 
                        "FECHA_INICIO": {"S": " "} , 
                        "FECHA_FIN": {"S": " "} ,
                        "TIEMPO": {"S": " "} ,
                        "STEP": {"S": " "} ,
                        "ERROR": {"S": " "} ,
                        "T_G_LECTURA": {"S": " "} ,
                        "T_G_NEGOCIO": {"S": " "} ,
                        "T_G_ESTRUCTURA": {"S": " "}
                    }
                }
            },
            {
                "PutRequest": {
                    "Item": {
                        "DOMINIO": {"S": "PRODUCTO"}, 
                        "FECHA_INICIO": {"S": " "} , 
                        "FECHA_FIN": {"S": " "} ,
                        "TIEMPO": {"S": " "} ,
                        "STEP": {"S": " "} ,
                        "ERROR": {"S": " "} ,
                        "T_G_LECTURA": {"S": " "} ,
                        "T_G_NEGOCIO": {"S": " "} ,
                        "T_G_ESTRUCTURA": {"S": " "}
                    }
                }
            },
            {
                "PutRequest": {
                    "Item": {
                        "DOMINIO": {"S": "POLIZA"}, 
                        "FECHA_INICIO": {"S": " "} , 
                        "FECHA_FIN": {"S": " "} ,
                        "TIEMPO": {"S": " "} ,
                        "STEP": {"S": " "} ,
                        "ERROR": {"S": " "} ,
                        "T_G_LECTURA": {"S": " "} ,
                        "T_G_NEGOCIO": {"S": " "} ,
                        "T_G_ESTRUCTURA": {"S": " "}
                    }
                }
            },
            {
                "PutRequest": {
                    "Item": {
                        "DOMINIO": {"S": "SINIESTRO"}, 
                        "FECHA_INICIO": {"S": " "} , 
                        "FECHA_FIN": {"S": " "} ,
                        "TIEMPO": {"S": " "} ,
                        "STEP": {"S": " "} ,
                        "ERROR": {"S": " "} ,
                        "T_G_LECTURA": {"S": " "} ,
                        "T_G_NEGOCIO": {"S": " "} ,
                        "T_G_ESTRUCTURA": {"S": " "}
                    }
                }
            },
            {
                "PutRequest": {
                    "Item": {
                        "DOMINIO": {"S": "REASEGURO"}, 
                        "FECHA_INICIO": {"S": " "} , 
                        "FECHA_FIN": {"S": " "} ,
                        "TIEMPO": {"S": " "} ,
                        "STEP": {"S": " "} ,
                        "ERROR": {"S": " "} ,
                        "T_G_LECTURA": {"S": " "} ,
                        "T_G_NEGOCIO": {"S": " "} ,
                        "T_G_ESTRUCTURA": {"S": " "}
                    }
                }
            },
            {
                "PutRequest": {
                    "Item": {
                        "DOMINIO": {"S": "RECIBO"}, 
                        "FECHA_INICIO": {"S": " "} , 
                        "FECHA_FIN": {"S": " "} ,
                        "TIEMPO": {"S": " "} ,
                        "STEP": {"S": " "} ,
                        "ERROR": {"S": " "} ,
                        "T_G_LECTURA": {"S": " "} ,
                        "T_G_NEGOCIO": {"S": " "} ,
                        "T_G_ESTRUCTURA": {"S": " "}
                    }
                }
            }
        ]
    }'

aws dynamodb create-backup --table-name TablaTrazabilidadIFRS17 --backup-name TablaTrazabilidadIFRS17bk

aws dynamodb restore-table-from-backup --target-table-name TablaTrazabilidadIFRS17 --backup-arn ARN_DE_LA_COPIA_DE_SEGURIDAD
