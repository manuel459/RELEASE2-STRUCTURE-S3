
def generate_product_parquets(bucketName, config_dominio, glue_context, connection, s3_client, io):

    l_product = '''
            (
                SELECT                
                "NBRANCH",
                "NPRODUCT",
                CAST(CAST("DEFFECDATE" AS DATE) AS VARCHAR) "DEFFECDATE",
                CAST(CAST("DNULLDATE" AS DATE) AS VARCHAR) "DNULLDATE",
                id_replicacion_positiva
                FROM USVTIMG01."PRODUCT"
            ) AS TMP
            '''
    
    #----------------------------------------------------------------------------------------------------------#

    l_prodmaster = '''
               (
                 SELECT 
                 "NBRANCH",
                 "NPRODUCT",
                 "SBRANCHT"
                 FROM usvtimg01."PRODMASTER"
               ) AS TMP
               '''

    #----------------------------------------------------------------------------------------------------------#

    l_gen_cover =  '''
               (
                 SELECT
                  "NBRANCH",
                  "NPRODUCT",
                  "NBRANCH_LED",
                  cast("DEFFECDATE" as date) "DEFFECDATE",
                  "NCOVERGEN",
                  "SADDSUINI",
                  "SROUCAPIT",
                  "NCACALFIX",
                  "NCACALCOV",
                  "SCACALFRI",
                  "SCACALILI",
                  "NMODULEC",
                  'LPG' COMPANIA,
                  CAST(CAST("DCOMPDATE" AS DATE) AS VARCHAR) "DCOMPDATE"
                  FROM USVTIMG01."GEN_COVER" 
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






     