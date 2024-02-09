
def generate_entidad_parquets(bucketName, config_dominio, glue_context, connection, s3_client, io):

    #---------------------------------------------INSIS VIDA-----------------------------------------------------------#
    #TABLAS CORE#
    l_p_people = '''
                (
                    SELECT 
                    CAST(CAST(PP."REGISTRATION_DATE" AS DATE ) AS VARCHAR) AS "REGISTRATION_DATE",
                    CAST(CAST(PP."BIRTH_DATE" AS DATE )AS VARCHAR ) AS "BIRTH_DATE",
                    PP."COMP_TYPE",
                    PP."SEX",
                    PP."MAN_ID",
                    PP."NATIONALITY",
                    PP."NAME"
                    FROM USINSIV01."P_PEOPLE" PP
                ) AS TMP
                '''
    #--------------------------------------------------------------------------------------------------------------------#
    l_intrf_lpv_people_ids = '''
                (
                    SELECT 
                    ILPI."LEGACY_ID",
                    ILPI."MAN_ID"
                    FROM USINSIV01."INTRF_LPV_PEOPLE_IDS" ILPI
                ) AS TMP
                '''
    #--------------------------------------------------------------------------------------------------------------------#
    l_p_people_changes = '''
                (
                    SELECT 
                    CAST(PPC."VALID_TO" AS DATE) AS "VALID_TO",
                    PPC."MAN_ID",
                    CAST(PPC."VALID_FROM" AS DATE) AS "VALID_FROM"
                    FROM USINSIV01."P_PEOPLE_CHANGES" PPC 
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