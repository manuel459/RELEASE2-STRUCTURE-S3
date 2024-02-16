
def generate_reaseguro_parquets(bucketName, config_dominio, glue_context, connection, s3_client, io):

    #---------------------------------------------INSIS VIDA-----------------------------------------------------------#
    #TABLAS CORE#
    l_ri_treaty_reinsurers = '''
                (
                    SELECT 
                    RTR."REINSURER_ID",
                    RTR."REINRINSR_SHARE",
                    RTR."ACTIVE_FROM",
                    RTR."ACTIVE_TO"
                    FROM USINSIV01."RI_TREATY_REINSURERS" RTR
                ) AS TMP
                '''
    #--------------------------------------------------------------------------------------------------------------------#
    l_ri_treaty = '''
                (
                    SELECT 
                    RT."TREATY_ID",
                    RT."END_DATE",
                    RT."TREATY_SUBTYPE",
                    RT."START_DATE"   
                    FROM USINSIV01."RI_TREATY" RT
                ) AS TMP
                '''
    #--------------------------------------------------------------------------------------------------------------------#
    l_p_insurers = '''
                (
                    SELECT 
                    PIN."MAN_ID",
                    PIN.FECHA_REPLICACION_POSITIVA
                    FROM USINSIV01."P_INSURERS" PIN
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