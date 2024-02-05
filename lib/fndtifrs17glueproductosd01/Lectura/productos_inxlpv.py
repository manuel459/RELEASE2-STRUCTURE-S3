def generate_product_parquets(bucketName, config_dominio, glue_context, connection, s3_client, io):

    l_product = '''
                (
                    SELECT 
                    PRO.USERCOMP,
                    PRO.COMPANY,
                    PRO.BRANCH,                    
                    PRO.PRODUCT,
                    PRO.BRANCHT,
                    cast(PRO.EFFECDATE as date) EFFECDATE,                    
                    cast(PRO.NULLDATE as date) NULLDATE
                    FROM USINSUV01.PRODUCT PRO
                ) AS TMP
                '''

    l_gen_life = '''
                 (
                    SELECT 
                    LC.branch,
                    LC.product,
                    cast(LC.effecdate as date) effecdate
                    FROM USINSUV01.LIFE_COVER LC
                 ) AS TMP
                 '''
                 
    # Iterate over tablas
    for tabla in config_dominio:
                
        df_result = glue_context.read.format('jdbc').options(**connection).option("dbtable", locals()[tabla['var']]).load() # read.execute_query(glue_context, connection, locals()[tabla['var']])
     
        # Verificar si el DataFrame está en caché antes de cachearlo
        if not df_result.is_cached:
            df_result.cache()
            print(f"El DataFrame {tabla['name']} está en caché.")
            
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
        
        # Liberar la caché después de procesar la tabla
        df_result.unpersist()
        print(f"La caché del DataFrame {tabla['name']} ha sido liberada.")