
def generate_product_parquets(bucketName, config_dominio, glue_context, connection, s3_client, io):

    #---------------------------------------------INSUNIX GENERAL-----------------------------------------------------------#
    #TABLAS CORE#
    l_product = '''
                (
                    SELECT 
                    CC.EFFECDATE,
                    CC."NUMBER",
                    CC.BRANCH,
                    CC."TYPE",
                    CC.YEAR_CONTR,
                    CC.CURRENCY,
                    CC.COMPANYC,
                    CC.SUPERVIS,
                    CC.SHARE,
                    CC.OMPDATE
                    FROM USINSUG01.CONTR_COMP CC
                ) AS TMP
                '''
    #--------------------------------------------------------------------------------------------------------------------#
    l_equi_vt_inx = '''
                (
                    SELECT 
                    EVI.SCOD_VT,
                    EVI.SCOD_INX
                    FROM USINSUG01.EQUI_VT_INX EVI
                ) AS TMP
                '''
    #--------------------------------------------------------------------------------------------------------------------#
    l_company = '''
                (
                    SELECT 
                    C.CLIENT,
                    C.CODE,
                    C.EFFECDATE,
                    C.COMPDATE
                    FROM USINSUG01.COMPANY C
                ) AS TMP
                '''
    #--------------------------------------------------------------------------------------------------------------------#
    l_contrproc = '''
                (
                    SELECT 
                    CT.EFFECDATE,
                    CT.BRANCH,
                    CT."NUMBER",
                    CT.CURRENCY,
                    CT.STARTDAT,
                    CT.EXPIRDAT,
                    CT.YEAR_CONTR,
                    CT."TYPE",
                    CT.COMPDATE
                    FROM USINSUG01.CONTRPROC CT
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