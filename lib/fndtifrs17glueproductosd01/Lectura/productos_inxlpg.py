def generate_product_parquets(bucketName, config_dominio, glue_context, connection, s3_client, io):

    l_product = '''
                (
                    SELECT 
                     PRO.EFFECDATE,
                     PRO.BRANCHT,
                     PRO.SUB_PRODUCT,
                     PRO.USERCOMP,
                     PRO.COMPANY,
                     PRO.BRANCH,
                     PRO.PRODUCT,
                     PRO.NULLDATE
                    FROM USINSUG01.PRODUCT PRO
                ) AS TMP
                '''
    l_gen_cover = '''
                (
                    SELECT 
                     CAST (GC.EFFECDATE AS DATE) as EFFECDATE,
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
                     GC.BILL_ITEM
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