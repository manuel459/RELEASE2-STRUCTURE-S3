def get_data(glue_context, bucket ,tablas):
  
  l_arprsap_insunix_lpg = '''
                         SELECT
                          'D' INDDETREC,
                          'ARPRSAP' TABLAIFRS17,
                          '' AS PK,
                          '' AS DTPREG,
                          '' AS TIOCPROC,
                          COALESCE(CAST(GC.EFFECDATE AS STRING), '')  AS TIOCFRM,
                          '' AS TIOCTO,
                          'PIG' AS KGIORIGM,
                          'LPG' AS DCOMPA,
                          '' AS DMARCA,
                          COALESCE(P.BRANCH, 0) || '-' || COALESCE(P.PRODUCT, 0) || '-' || COALESCE(P.SUB_PRODUCT) AS KABPRODT,
                          COALESCE(GC.COVERGEN, 0) || '-' || COALESCE(GC.CURRENCY, 0) AS KGCTPCBT,
                          '' AS KACCDFDO,
                          '' AS KACFUNAU,
                          COALESCE(
                                    COALESCE(
                                              (SELECT CAST(MAX(AA.BRANCH_PYG) AS STRING)
                                               FROM ACC_AUTOM2 AA 
                                               WHERE GC.BRANCH = AA.BRANCH  
                                               AND   GC.PRODUCT = AA.PRODUCT 
                                               AND   GC.BILL_ITEM = AA.CONCEPT_FAC), 
                                              (SELECT CAST(MAX(AA.BRANCH_PYG) AS STRING)
                                               FROM ACC_AUTOM2 AA 
                                               WHERE GC.BRANCH = AA.BRANCH  
                                               AND   GC.PRODUCT = AA.PRODUCT)), 
                                              (SELECT CAST(MAX(AA.BRANCH_PYG) AS STRING)
                                               FROM ACC_AUTOM2 AA 
                                               WHERE GC.BRANCH = AA.BRANCH)
                          )  AS KGCRAMO_SAP,
                          '' AS DMASTER,
                          '' AS KACTPSPR, 
                          '' AS KACPARES, 
                          COALESCE(P.BRANCHT, '') AS KACCLAPD,
                          '' AS KACSCLAPD,
                          '' AS DRAMOSAP,
                          '' AS DPRODSAP,
                          '' AS KACCDFDO_PR/*,
                          GC.MODULEC AS MODULO*/
                          FROM GEN_COVER GC 
                          LEFT JOIN
                          (
                          	SELECT	
                          	PRO.PRODUCT,
                          	PRO.BRANCH,
                            PRO.BRANCHT,
                            PRO.SUB_PRODUCT,
                            CASE WHEN PRO.NULLDATE IS NOT NULL THEN 1 ELSE 0 END FLAG_NULLDATE
                            FROM (	
                                  SELECT	PRO.*,
                                  CASE	
                                  WHEN PRO.NULLDATE IS NULL THEN	PRO.id_replicacion_positiva
                          		 	  ELSE	CASE	
                                        WHEN (	SELECT	count(*)
                          		 				              	FROM	PRODUCT PR1
                          		 				              	WHERE PR1.USERCOMP = PRO.USERCOMP
                          		 				              	AND 	PR1.COMPANY = PRO.COMPANY
                          		 				              	AND 	PR1.BRANCH = PRO.BRANCH
                          		 				              	AND 	PR1.PRODUCT = PRO.PRODUCT
                          		 				              	AND		PR1.NULLDATE IS NULL) > 0 THEN 	NULL
                          		 		      ELSE  CASE	
                                              WHEN PRO.NULLDATE = (	SELECT	MAX(PR1.NULLDATE)
                          		 				 						                  FROM	PRODUCT PR1
                          		 				 						                  WHERE 	PR1.USERCOMP = PRO.USERCOMP
                          		 				 						                  AND 	PR1.COMPANY = PRO.COMPANY
                          		 				 						                  AND 	PR1.BRANCH = PRO.BRANCH
                          		 				 						                  AND 	PR1.PRODUCT = PRO.PRODUCT) THEN PRO.id_replicacion_positiva
                          		 			          ELSE NULL 
                                              END 
                                        END 
                                  END PRO_ID
                          		    FROM	PRODUCT PRO
                          		    WHERE	BRANCH IN (SELECT BRANCH FROM TABLE10B WHERE COMPANY = 1)) PR0, PRODUCT PRO
                          	WHERE PRO.id_replicacion_positiva = PR0.PRO_ID) P 
                          ON GC.BRANCH = P.BRANCH  AND GC.PRODUCT  = P.PRODUCT
                          '''
  #--------------------------------------------------------------------------------------------------------------------------# 
                        
  l_arprsap_insunix_lpv = '''
                              SELECT
                              'D' INDDETREC,
                              'ARPRSAP' TABLAIFRS17,
                              '' AS PK,
                              '' AS DTPREG,
                              '' AS TIOCPROC,
                              COALESCE(CAST(LC.EFFECDATE AS STRING), '')  AS TIOCFRM,
                              '' AS TIOCTO,
                              'PIV' AS KGIORIGM,
                              'LPV' AS DCOMPA,
                              '' AS DMARCA,
                              COALESCE(P.BRANCH, 0) || '-' || COALESCE(P.PRODUCT, 0) AS KABPRODT,
                              COALESCE(LC.covergen, 0) || '-' || coalesce(LC.currency, 0) AS KGCTPCBT,
                              '' AS KACCDFDO,
                              '' AS KACFUNAU,
                              COALESCE(
                                        COALESCE( (SELECT CAST(MAX(AA.BRANCH_PYG) AS STRING)
                                                  FROM ACC_AUTOM2 AA 
                                                  WHERE LC.BRANCH = AA.BRANCH  
                                                  AND   LC.PRODUCT = AA.PRODUCT 
                                                  AND   LC.BILL_ITEM = AA.CONCEPT_FAC), 
                                                  (SELECT CAST(MAX(AA.BRANCH_PYG) AS STRING)
                                                  FROM ACC_AUTOM2 AA 
                                                  WHERE LC.BRANCH = AA.BRANCH  
                                                  AND   LC.PRODUCT = AA.PRODUCT)), 
                                                  (SELECT CAST(MAX(AA.BRANCH_PYG) AS STRING)
                                                  FROM ACC_AUTOM2 AA 
                                                  WHERE LC.BRANCH = AA.BRANCH)) AS KGCRAMO_SAP,
                              '' AS DMASTER,
                              '' AS KACTPSPR, 
                              '' AS KACPARES, 
                              COALESCE(P.BRANCHT, '') AS KACCLAPD,
                              '' AS KACSCLAPD,
                              '' AS DRAMOSAP,
                              '' AS DPRODSAP,
                              '' AS KACCDFDO_PR/*,
                              0  AS MODULO*/
                              FROM LIFE_COVER LC 
                              LEFT JOIN
                              (
                                SELECT	
                                PRO.PRODUCT,
                                PRO.BRANCH,
                                PRO.BRANCHT,
                                CASE WHEN PRO.NULLDATE IS NOT NULL THEN 1 ELSE 0 END FLAG_NULLDATE
                                FROM (	
                                      SELECT	PRO.*,
                                      CASE	
                                      WHEN PRO.NULLDATE IS NULL THEN	PRO.id_replicacion_positiva
                                      ELSE	CASE	
                                            WHEN (	SELECT	count(*)
                                                          FROM	PRODUCT PR1
                                                          WHERE 	PR1.USERCOMP = PRO.USERCOMP
                                                          AND 	PR1.COMPANY = PRO.COMPANY
                                                          AND 	PR1.BRANCH = PRO.BRANCH
                                                          AND 	PR1.PRODUCT = PRO.PRODUCT
                                                          AND		PR1.NULLDATE IS NULL) > 0 THEN 	NULL
                                            ELSE  CASE	
                                                  WHEN PRO.NULLDATE = (	SELECT	MAX(PR1.NULLDATE)
                                                                        FROM	PRODUCT PR1
                                                                        WHERE 	PR1.USERCOMP = PRO.USERCOMP
                                                                        AND 	PR1.COMPANY = PRO.COMPANY
                                                                        AND 	PR1.BRANCH = PRO.BRANCH
                                                                        AND 	PR1.PRODUCT = PRO.PRODUCT) THEN PRO.id_replicacion_positiva
                                                  ELSE NULL 
                                                  END 
                                            END 
                                      END PRO_ID
                                      FROM	PRODUCT PRO
                                      WHERE	BRANCH IN (SELECT BRANCH FROM TABLE10B WHERE COMPANY = 2)) PR0, PRODUCT PRO
                                WHERE PRO.id_replicacion_positiva = PR0.PRO_ID) P
                              ON LC.BRANCH = P.BRANCH  AND LC.PRODUCT  = P.PRODUCT
                          '''
    #EJECUTAR CONSULTA
  
  #--------------------------------------------------------------------------------------------------------------------------#

  l_arprsap_vtime_lpg = '''
                           SELECT 
                            'D' INDDETREC,
                            'ARPRSAP' TABLAIFRS17,
                            '' AS PK,
                            '' AS DTPREG,
                            '' AS TIOCPROC,
                            CAST(GC.DEFFECDATE AS STRING) TIOCFRM,
                            '' AS TIOCTO,
                            'PVG' AS KGIORIGM,
                            'LPG' AS DCOMPA,
                            '' AS DMARCA,
                            COALESCE(PM.NBRANCH, 0) || '-' || COALESCE(PM.NPRODUCT, 0) AS KABPRODT,
                            COALESCE(GC.NCOVERGEN, 0) AS KGCTPCBT,
                            '' AS KACCDFDO,
                            '' AS KACFUNAU,
                            COALESCE(CAST(GC.NBRANCH_LED AS STRING), '')  AS KGCRAMO_SAP,
                            '' AS DMASTER,
                            '' AS KACTPSPR, 
                            '' AS KACPARES,
                            PM.SBRANCHT  AS KACCLAPD, 
                            '' AS KACSCLAPD,
                            '' AS DRAMOSAP,
                            '' AS DPRODSAP,
                            '' AS KACCDFDO_PR/*,
                            GC.NMODULEC AS MODULO*/
                            FROM GEN_COVER GC 
                            LEFT JOIN PRODMASTER PM  ON GC.NBRANCH = PM.NBRANCH  AND GC.NPRODUCT = PM.NPRODUCT
                       '''
    #EJECUTAR CONSULTA
   
  #--------------------------------------------------------------------------------------------------------------------------#
  
  l_arprsap_vtime_lpv= '''
                          SELECT
                          'D' INDDETREC,
                          'ARPRSAP' TABLAIFRS17,
                          '' AS PK,
                          '' AS DTPREG,
                          '' AS TIOCPROC,
                          CAST(LC.DEFFECDATE AS STRING) as TIOCFRM,
                          '' AS TIOCTO,
                          'PVV' AS KGIORIGM,
                          'LPV' AS DCOMPA,
                          '' AS DMARCA,
                          COALESCE(PM.NBRANCH, 0) || '-' || COALESCE(PM.NPRODUCT, 0) AS KABPRODT,
                          COALESCE(LC.NCOVERGEN, 0) AS KGCTPCBT,
                          '' AS KACCDFDO,
                          '' AS KACFUNAU,
                          COALESCE(CAST(LC.NBRANCH_LED AS STRING), '')  AS KGCRAMO_SAP,
                          '' AS DMASTER,
                          '' AS KACTPSPR,
                          '' AS KACPARES,
                          PM.SBRANCHT AS KACCLAPD,
                          '' AS KACSCLAPD,
                          '' AS DRAMOSAP,
                          '' AS DPRODSAP,
                          '' AS KACCDFDO_PR/*,
                          LC.NMODULEC AS MODULO*/                          
                          FROM LIFE_COVER LC
                          LEFT JOIN PRODMASTER PM ON LC.NBRANCH = PM.NBRANCH AND LC.NPRODUCT = PM.NPRODUCT
                      '''
    #EJECUTAR CONSULTA

  #--------------------------------------------------------------------------------------------------------------------------#
    
  l_arprsap_insis = '''
                      SELECT 
                      'D' INDDETREC,
                      'ARPRSAP' TABLAIFRS17,
                      '' AS PK,
                      '' AS DTPREG,
                      '' AS TIOCPROC,
                      CAST(C_NL_COV.VALID_FROM AS DATE) AS TIOCFRM,
                      '' AS TIOCTO,
                      'PNV' AS KGIORIGM,
                      'LPV' AS DCOMPA,
                      '' AS DMARCA,
                      CAST(C_NL_PROD.PRODUCT_CODE AS STRING) || '-' || C_PARAM_V.PARAM_VALUE AS KABPRODT,
                      SUBSTRING(CAST(CAST(C_NL_COV.COVER_REP_ID as BIGINT) AS STRING), 5, 10) AS KGCTPCBT,
                      '' AS KACCDFDO,
                      '' AS KACFUNAU,
                      '' AS KGCRAMO_SAP,
                      '' AS DMASTER,  
                      '' AS KACTPSPR, 
                      '' AS KACPARES,
                      '' AS KACCLAPD,
                      '' as KACSCLAPD,
                      '' AS DRAMOSAP,
                      '' AS DPRODSAP,
                      '' AS KACCDFDO_PR/*,                        
                      C_NL_COV.OBJECT_LINK_ID AS MODULO*/
                      FROM CFG_NL_COVERS C_NL_COV 
                      LEFT JOIN CFG_NL_PRODUCT C_NL_PROD ON C_NL_COV.PRODUCT_LINK_ID = C_NL_PROD.PRODUCT_LINK_ID
                      INNER JOIN CFG_NL_PRODUCT_CONDS C_NL_PROD_C ON C_NL_PROD.PRODUCT_LINK_ID = C_NL_PROD_C.PRODUCT_LINK_ID
                      INNER JOIN CPR_PARAMS C_PARAM ON C_NL_PROD_C.PARAM_CPR_ID = C_PARAM.PARAM_CPR_ID AND C_PARAM.FOLDER = 'LPV' AND C_PARAM.PARAM_NAME LIKE 'AS_IS%'
                      JOIN CPRS_PARAM_VALUE C_PARAM_V ON C_PARAM.PARAM_CPR_ID = C_PARAM_V.PARAM_ID
                    '''
    
    #EJECUTAR CONSULTA
      
  #--------------------------------------------------------------------------------------------------------------------------#
  spark = glue_context.spark_session
  l_df_arprsap = None
  
  print(tablas)
  
  for tabla in tablas:
    print('Aqui esta la lista de tablas:',tabla['lista'])
    
    for item in tabla['lista']:
      view_name = item['vista']
      file_path = item['path']
      
      print('la vista : ', view_name)
      print('el path origen: ',file_path)
      
      # Leer datos desde Parquet usando pandas
      pandas_df = spark.read.parquet('s3://'+bucket+'/'+file_path)

      pandas_df.createOrReplaceTempView(view_name)
      
    current_df = spark.sql(locals()[tabla['var']])
    print('la variable a ejecutar', tabla['var'])
    
    if l_df_arprsap is None:
      l_df_arprsap = current_df
    else: 
      # Ejecutar la consulta final
      l_df_arprsap = l_df_arprsap.union(current_df)
    current_df.show()
  
  print('Proceso Final')
  l_df_arprsap.show()
    
  return l_df_arprsap