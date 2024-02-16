def generate_product_parquets(bucketName, config_dominio, glue_context, connection, s3_client, io):

    l_product = '''
                (
                    SELECT 
                     CAST(CAST(PRO.EFFECDATE AS DATE) AS VARCHAR) EFFECDATE,
                     PRO.BRANCHT,
                     PRO.SUB_PRODUCT,
                     PRO.USERCOMP,
                     PRO.COMPANY,
                     PRO.BRANCH,
                     PRO.PRODUCT,
                     CAST(CAST(PRO.NULLDATE AS DATE) AS VARCHAR) NULLDATE,
                     PRO.id_replicacion_positiva
                    FROM USINSUG01.PRODUCT PRO
                ) AS TMP
                '''
    l_gen_cover = '''
                (
                    SELECT 
                     CAST(CAST(GC.EFFECDATE AS DATE) AS VARCHAR) EFFECDATE,
                     GC.COVERGEN,
                     GC.CURRENCY,
                     GC.ADDSUINI,
                     GC.ROUCAPIT,
                     GC.CACALFIX,
                     GC.CACALCOV,
                     GC.CACALFRI,
                     GC.CACALILI,
                     GC.CACALMAX,
                     GC.BRANCH,
                     GC.PRODUCT,
                     GC.BILL_ITEM,
                     CAST(CAST(GC.COMPDATE AS DATE) AS VARCHAR) COMPDATE 
                    FROM USINSUG01.GEN_COVER GC
                ) AS TMP
                '''
    l_table10b = '''
                (
                    SELECT 
                    TB.BRANCH,
                    TB.COMPANY 
                    FROM USINSUG01.TABLE10B TB
                ) AS TMP
                '''
    l_acc_autom2 = '''
                (
                    SELECT 
                    AA.BRANCH_PYG,
                    AA.BRANCH,
                    AA.PRODUCT,
                    AA.CONCEPT_FAC
                    FROM USINSUG01.ACC_AUTOM2 AA
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