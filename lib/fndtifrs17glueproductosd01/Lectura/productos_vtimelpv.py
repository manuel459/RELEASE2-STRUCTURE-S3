def generate_product_parquets(bucketName, config_dominio, glue_context, connection, s3_client, io):

    l_product = ''' 
                (
                    SELECT                    
                      PRO."NBRANCH",
                      PRO."NPRODUCT",
                      cast(cast(PRO."DEFFECDATE" as date) as varchar) "DEFFECDATE",
                      CAST(CAST(PRO."DNULLDATE" AS DATE) AS VARCHAR) "DNULLDATE",
                      id_replicacion_positiva
                      from USVTIMV01."PRODUCT" PRO
                ) AS TMP
                '''
    #----------------------------------------------------------------------------------------------------------#

    l_prodmaster = '''
                   (
                     SELECT 
                     PM."NBRANCH",
                     PM."NPRODUCT",
                     PM."SBRANCHT"
                     FROM USVTIMV01."PRODMASTER" PM
                   ) AS TMP
                   '''
    #----------------------------------------------------------------------------------------------------------#

    l_life_cover = '''
                  (
                    SELECT
                    LC."NBRANCH",
                    LC."NPRODUCT",
                    cast(cast(LC."DEFFECDATE" as date) as varchar) "DEFFECDATE",
                    LC."NCOVERGEN",
                    LC."NBRANCH_LED",
                    LC."NMODULEC",
                    "SADDSUINI",
                    "SROURESER",
                    CAST(CAST(LC."DCOMPDATE" AS DATE) AS VARCHAR) "DCOMPDATE"
                    FROM USVTIMV01."LIFE_COVER" LC
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
    


