

def generate_product_parquets(bucketName, config_dominio, glue_context, connection, s3_client, io):

    #---------------------------------------------INSIS VIDA-----------------------------------------------------------#
    #TABLAS CORE#
    l_cfg_nl_covers = '''
                      (
                         SELECT 
                         CAST(CAST(CNC."VALID_FROM" AS DATE) AS VARCHAR),
                         CNC."COVER_REP_ID",
                         CNC."PRODUCT_LINK_ID",
                         CNC."COVER_DESIGNATION",
                         CNC."MANDATORY",
                         CNC."IV_RULE"
                         FROM USINSIV01."CFG_NL_COVERS" CNC
                      ) AS TMP
                      '''
    #--------------------------------------------------------------------------------------------------------------------#
    l_cfg_nl_product = '''
                       (
                           SELECT 
                           C_NL_PROD."PRODUCT_CODE",
                           C_NL_PROD."PRODUCT_LINK_ID",
                           CAST(CAST(C_NL_PROD."VALID_FROM" AS DATE) AS VARCHAR) "VALID_FROM",
                           C_NL_PROD."PRODUCT_LOB"
                           FROM USINSIV01."CFG_NL_PRODUCT" C_NL_PROD 
                       ) AS TMP
                       '''
    #--------------------------------------------------------------------------------------------------------------------#

    l_cfg_nl_product_conds = '''
                      (
                         SELECT 
                         C_NL_PROD_C."PRODUCT_LINK_ID",
                         C_NL_PROD_C."PARAM_CPR_ID"
                         FROM USINSIV01."CFG_NL_PRODUCT_CONDS" C_NL_PROD_C
                      ) AS TMP
                      '''
    #--------------------------------------------------------------------------------------------------------------------#

    l_cpr_params = '''
                      (
                         SELECT 
                         C_PARAM."FOLDER",
                         C_PARAM."PARAM_NAME",
                         C_PARAM."PARAM_CPR_ID"
                         FROM USINSIV01."CPR_PARAMS" C_PARAM 
                      ) AS TMP
                      '''
    #--------------------------------------------------------------------------------------------------------------------#

    l_cprs_param_value = '''
                      (
                         SELECT 
                         C_PARAM_V."PARAM_ID",
                         C_PARAM_V."PARAM_VALUE"
                         FROM USINSIV01."CPRS_PARAM_VALUE" C_PARAM_V
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
        








                  