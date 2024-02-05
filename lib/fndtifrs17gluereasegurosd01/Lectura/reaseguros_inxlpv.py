
def generate_product_parquets(bucketName, config_dominio, glue_context, connection, s3_client, io):

    #---------------------------------------------INSUNIX VIDA-----------------------------------------------------------#
    #TABLAS CORE#
    l_product = '''
                (
                    SELECT 
                    CC.EFFECDATE,
                    CC.BRANCH,
                    CC."NUMBER",
                    CC."TYPE",
                    CC.YEAR_CONTR,
                    CC.CURRENCY,
                    CC.SUPERVIS,
                    CC.COMPANYC,
                    CC.COMPDATE
                    FROM USINSUV01.CONTR_COMP CC
                ) AS TMP
                '''
    #--------------------------------------------------------------------------------------------------------------------#
    l_contrproc = '''
                (
                    SELECT 
                    CT.EFFECDATE
                    CT."NUMBER"
                    CT.BRANCH
                    CT.STARTDAT
                    CT.EXPIRDAT
                    CT.YEAR_CONTR
                    CT."TYPE"
                    CT.COMPDATE
                    FROM USINSUV01.CONTRPROC CT
                ) AS TMP
                '''

    # Iterate over tablas
    for tabla in config_dominio:
                
        df_result = glue_context.read.format('jdbc').options(**connection).option("dbtable", locals()[tabla['var']]).load() # read.execute_query(glue_context, connection, locals()[tabla['var']])
            
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