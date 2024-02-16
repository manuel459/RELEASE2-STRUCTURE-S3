def generate_product_parquets(bucketName, config_dominio, glue_context, connection, s3_client, io):

    l_product = '''
                (
                    SELECT 
                    PRO.USERCOMP,
                    PRO.COMPANY,
                    PRO.BRANCH,                    
                    PRO.PRODUCT,
                    PRO.BRANCHT,
                    CAST(cast(PRO.EFFECDATE as date) AS VARCHAR) EFFECDATE,                    
                    CAST(cast(PRO.NULLDATE as date) AS VARCHAR) NULLDATE,
                    PRO.id_replicacion_positiva
                    FROM USINSUV01.PRODUCT PRO
                ) AS TMP
                '''

    l_life_cover = '''
                 (
                    SELECT 
                    LC.branch,
                    LC.product,
                    CAST(cast(LC.effecdate as date) AS VARCHAR) effecdate,
                    covergen,
                    currency,
                    bill_item,
                    ADDCAPII,
                    ROUCHACA,
                    CACALFIX,
                    CACALCOV,
                    CACALFRI,
                    CAST(CAST(LC.COMPDATE AS DATE) AS VARCHAR) COMPDATE
                    FROM USINSUV01.LIFE_COVER LC
                 ) AS TMP
                 '''
                 
    l_acc_autom2 = '''
                 (
                    SELECT 
                    AA.BRANCH_PYG,
                    AA.BRANCH,
                    AA.PRODUCT,
                    AA.CONCEPT_FAC
                    FROM USINSUV01.ACC_AUTOM2 AA
                 ) AS TMP
                 '''
                 
    # Iterate over tablas
    for tabla in config_dominio:
                
        df_result = glue_context.read.format('jdbc').options(**connection).option("fetchsize", 10000).option("dbtable", locals()[tabla['var']]).load()
     
        # Repartir el DataFrame manualmente
        df_result = df_result.repartition(10)
            
        #Trasformar a bit escrito en formato parquet
        L_BUFFER = io.BytesIO()
        df_result.toPandas().to_parquet(L_BUFFER, index=False)
        L_BUFFER.seek(0)
        
        # Escribir el objeto Parquet en S3
        s3_client.put_object(
            Bucket = bucketName,
            Key = tabla['name'], 
            Body=L_BUFFER.read()
        )