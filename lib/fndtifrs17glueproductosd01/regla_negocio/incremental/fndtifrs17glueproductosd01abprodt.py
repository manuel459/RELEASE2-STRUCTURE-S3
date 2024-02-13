
def get_data(glue_context, bucket ,tablas, p_fecha_inicio, p_fecha_fin):

  l_abprodt_insunix_lpg = '''
                          SELECT 
                          'D' AS INDDETREC,
                          'ABPRODT' AS TABLAIFRS17,
                          '' AS PK,
                          '' AS DTPREG,
                          '' AS TIOCPROC,
                          COALESCE(P.EFFECDATE, '') AS TIOCFRM,
                          '' AS TIOCTO,
                          'PIG' AS KGIORIGM,
                          COALESCE(P.BRANCH, 0) || '-' || COALESCE(P.PRODUCT, 0) || '-' || COALESCE(P.SUB_PRODUCT, 0) AS DCODIGO,
                          '' AS DDESC,
                          COALESCE(CAST(P.BRANCH AS STRING), '') AS KGCRAMO,
                          'LPG' AS DCOMPA,
                          '' AS DMARCA,
                          'PER' AS KACPAIS,
                          COALESCE(P.BRANCHT, '') AS KACTPPRD,
                          '' AS KACTPSUB, 
                          '' AS KACPARES, 
                          '' AS KACRESGA, 
                          '' AS KACINPPR, 
                          '' AS KACCLREN,
                          '' AS KACTPCTR,
                          '' AS KACTPGRP,
                          '' AS KACCRHAB,
                          COALESCE(P.EFFECDATE , '') AS TINICOME,
                          '' AS TFIMCOME,
                          '' AS KACFPCAP,
                          '' AS DINDREDU, 
                          '' AS DINDRESG,
                          '' AS KACTPDUR,
                          '' AS DCDCNGR,
                          '' AS KACREINTE,
                          '' AS KABPRODT_RINV,
                          '' AS DSEGMENT,
                          '' AS KACSEGMEN,
                          '' AS KACTARTAB,
                          '' AS KACTPCRED,
                          '' AS KACTPOPS,
                          '' AS KACMOEDA,  
                          '' AS KACPERISC,
                          '' AS DINDGRPADES,
                          '' AS DINDENTEXT,
                          '' AS KACSBTPRD, 
                          '' AS KACTPPMA, 
                          '' AS KACTIPIFAP 
                          FROM
                          (
                          	SELECT	
                          	PRO.EFFECDATE,
                          	PRO.PRODUCT,
                          	PRO.BRANCH,
                            PRO.BRANCHT,
                          	PRO.NULLDATE,
                            PRO.SUB_PRODUCT,
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
                          		    WHERE	BRANCH IN (SELECT BRANCH FROM TABLE10B WHERE COMPANY = 1)) PR0, PRODUCT PRO
                          	WHERE PRO.id_replicacion_positiva = PR0.PRO_ID) P
                          '''

  l_abprodt_insunix_lpv = '''                     
                            SELECT
                            'D' AS INDDETREC,
                            'ABPRODT' AS TABLAIFRS17,
                            '' AS PK,
                            '' AS DTPREG,
                            '' AS TIOCPROC,
                            COALESCE(P.EFFECDATE, '')  AS TIOCFRM,
                            '' AS TIOCTO,
                            'PIV' AS KGIORIGM,
                            COALESCE(P.BRANCH, '0') || '-' || COALESCE(CAST(P.PRODUCT AS STRING), '0') AS DCODIGO,
                            '' AS DDESC,
                            COALESCE(CAST(P.BRANCH AS STRING), '') AS KGCRAMO,
                            'LPV' AS DCOMPA,
                            '' AS DMARCA,
                            'PER' AS KACPAIS,
                            COALESCE(P.BRANCHT, '') AS KACTPPRD, 
                            '' AS KACTPSUB, 
                            '' AS KACPARES, 
                            '' AS KACRESGA, 
                            '' AS KACINPPR, 
                            '' AS KACCLREN,
                            '' AS KACTPCTR,
                            '' AS KACTPGRP,
                            '' AS KACCRHAB, 
                            COALESCE(P.EFFECDATE, '')  AS TINICOME,
                            '' AS TFIMCOME,
                            '' AS KACFPCAP,
                            '' AS DINDREDU,  
                            '' AS DINDRESG,
                            '' AS KACTPDUR,
                            '' AS DCDCNGR,
                            '' AS KACREINTE,
                            '' AS KABPRODT_RINV, 
                            '' AS DSEGMENT,   
                            '' AS KACSEGMEN,   
                            '' AS KACTARTAB,   
                            '' AS KACTPCRED, 
                            '' AS KACTPOPS,  
                            '' AS KACMOEDA,  
                            '' AS KACPERISC,
                            '' AS DINDGRPADES,
                            '' AS DINDENTEXT,
                            '' AS KACSBTPRD,   
                            '' AS KACTPPMA,
                            '' AS KACTIPIFAP
                            FROM
                            (
                          	    SELECT	
                          	    PRO.EFFECDATE,
                          	    PRO.PRODUCT,
                          	    PRO.BRANCH,
                                PRO.BRANCHT,
                          	    PRO.NULLDATE,
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
                          	    WHERE PRO.id_replicacion_positiva = PR0.PRO_ID limit 4
                            ) P
                          '''
  
  #--------------------------------------------------------------------------------------------------------------------------#

  l_abprodt_vtime_lpg = '''
                        SELECT
                            'D' AS INDDETREC,
                            'ABPRODT' AS TABLAIFRS17,
                            '' AS PK,
                            '' AS DTPREG,
                            '' AS TIOCPROC,
                            coalesce(P.DEFFECDATE, '') AS TIOCFRM,
                            '' AS TIOCTO,
                            'PVG' AS KGIORIGM,
                            coalesce(p.NBRANCH, 0)|| '-' || coalesce(P.NPRODUCT, 0) AS DCODIGO,
                            '' AS DDESC,
                            P.NBRANCH AS KGCRAMO,
                            'LPG' AS DCOMPA,
                            '' AS DMARCA,
                            'PER' AS KACPAIS,
                            PM.SBRANCHT AS KACTPPRD,
                            '' AS KACTPSUB, 
                            '' AS KACPARES, 
                            '' AS KACRESGA, 
                            '' AS KACINPPR, 
                            '' AS KACCLREN,
                            '' AS KACTPCTR,
                            '' AS KACTPGRP,
                            '' AS KACCRHAB, 
                            coalesce(P.DEFFECDATE, '') AS TINICOME,
                            '' AS TFIMCOME,
                            '' AS KACFPCAP,
                            '' AS DINDREDU,  
                            '' AS DINDRESG,
                            '' AS KACTPDUR,
                            '' AS DCDCNGR, 
                            '' AS KACREINTE,
                            '' AS KABPRODT_RINV, 
                            '' AS DSEGMENT,
                            '' AS KACSEGMEN,
                            '' AS KACTARTAB,    
                            '' AS KACTPCRED,
                            '' AS KACTPOPS,
                            '' AS KACMOEDA,  
                            '' AS KACPERISC,
                            '' AS DINDGRPADES,
                            '' AS DINDENTEXT,  
                            '' AS KACSBTPRD,
                            '' AS KACTPPMA,
                            '' AS KACTIPIFAP
                          FROM
                          (
                          	SELECT	
                          	PRO.DEFFECDATE,
                          	PRO.NPRODUCT,
                          	PRO.NBRANCH,
                          	PRO.DNULLDATE,
                            CASE WHEN PRO.DNULLDATE IS NOT NULL THEN 1 ELSE 0 END FLAG_NULLDATE
                            FROM (	
                                  SELECT	PRO.*,
                                  CASE	
                                  WHEN PRO.DNULLDATE IS NULL THEN	PRO.id_replicacion_positiva
                          		 	  ELSE	CASE	
                                        WHEN ( SELECT	count(*)
                                                FROM	PRODUCT PR1
                                                WHERE PR1.NBRANCH  = PRO.NBRANCH
                                                AND 	PR1.NPRODUCT = PRO.NPRODUCT
                                                AND		PR1.DNULLDATE IS NULL) > 0 THEN 	NULL
                          		 		      ELSE  CASE	
                                              WHEN PRO.DNULLDATE = (	SELECT	MAX(PR1.DNULLDATE)
                          		 				 						                  FROM	PRODUCT PR1
                          		 				 						                  WHERE PR1.NBRANCH  = PRO.NBRANCH 
                          		 				 						                  AND 	PR1.NPRODUCT = PRO.NPRODUCT) THEN PRO.id_replicacion_positiva
                          		 			          ELSE NULL 
                                              END 
                                        END 
                                  END PRO_ID
                          		    FROM	PRODUCT PRO) PR0, PRODUCT PRO
                          	WHERE PRO.id_replicacion_positiva = PR0.PRO_ID) P
                          LEFT JOIN PRODMASTER PM ON PM.NBRANCH = P.NBRANCH AND PM.NPRODUCT = P.NPRODUCT limit 4
                        '''

  l_abprodt_vtime_lpv = '''
                          SELECT
                            'D' AS INDDETREC,
                            'ABPRODT' AS TABLAIFRS17,
                            '' AS PK,
                            '' AS DTPREG,
                            '' AS TIOCPROC,
                            coalesce(P.DEFFECDATE, '') AS TIOCFRM,
                            '' AS TIOCTO,
                            'PVV' AS KGIORIGM,
                            coalesce(p.NBRANCH, 0)|| '-' || coalesce(P.NPRODUCT, 0) AS DCODIGO,
                            '' AS DDESC,
                            P.NBRANCH AS KGCRAMO,
                            'LPV' AS DCOMPA,
                            '' AS DMARCA,
                            'PER' AS KACPAIS,
                            PM.SBRANCHT AS KACTPPRD,
                            '' AS KACTPSUB, 
                            '' AS KACPARES, 
                            '' AS KACRESGA, 
                            '' AS KACINPPR, 
                            '' AS KACCLREN,
                            '' AS KACTPCTR,
                            '' AS KACTPGRP,
                            '' AS KACCRHAB, 
                            coalesce(P.DEFFECDATE, '') AS TINICOME,
                            '' AS TFIMCOME,
                            '' AS KACFPCAP,
                            '' AS DINDREDU,  
                            '' AS DINDRESG,
                            '' AS KACTPDUR,
                            '' AS DCDCNGR,    
                            '' AS KACREINTE,
                            '' AS KABPRODT_RINV, 
                            '' AS DSEGMENT,       
                            '' AS KACSEGMEN,      
                            '' AS KACTARTAB,   	 
                            '' AS KACTPCRED,   	  
                            '' AS KACTPOPS,    	  
                            '' AS KACMOEDA,   
                            '' AS KACPERISC,   	  
                            '' AS DINDGRPADES, 	  
                            '' AS DINDENTEXT,  	 
                            '' AS KACSBTPRD,   	 
                            '' AS KACTPPMA,    	  
                            '' AS KACTIPIFAP   	    
                          FROM
                          (
                          	SELECT	
                          	PRO.DEFFECDATE,
                          	PRO.NPRODUCT,
                          	PRO.NBRANCH,
                          	PRO.DNULLDATE,
                            CASE WHEN PRO.DNULLDATE IS NOT NULL THEN 1 ELSE 0 END FLAG_NULLDATE
                            FROM (	
                                  SELECT	PRO.*,
                                  CASE	
                                  WHEN PRO.DNULLDATE IS NULL THEN	PRO.id_replicacion_positiva
                          		 	  ELSE	CASE	
                                        WHEN (	SELECT	count(*)
                          		 				              	FROM	PRODUCT PR1
                          		 				              	WHERE PR1.NBRANCH  = PRO.NBRANCH
                          		 				              	AND 	PR1.NPRODUCT = PRO.NPRODUCT
                          		 				              	AND		PR1.DNULLDATE IS NULL) > 0 THEN 	NULL
                          		 		      ELSE  CASE	
                                              WHEN PRO.DNULLDATE = (	SELECT	MAX(PR1.DNULLDATE)
                          		 				 						                  FROM	PRODUCT PR1
                          		 				 						                  WHERE PR1.NBRANCH  = PRO.NBRANCH 
                          		 				 						                  AND 	PR1.NPRODUCT = PRO.NPRODUCT) THEN PRO.id_replicacion_positiva
                          		 			          ELSE NULL 
                                              END 
                                        END 
                                  END PRO_ID
                          		    FROM	PRODUCT PRO) PR0, PRODUCT PRO
                          	WHERE PRO.id_replicacion_positiva = PR0.PRO_ID) P
                          LEFT JOIN PRODMASTER PM ON PM.NBRANCH = P.NBRANCH AND PM.NPRODUCT = P.NPRODUCT
                       '''
  
  #--------------------------------------------------------------------------------------------------------------------------#

  l_abprodt_insis_lpv = '''                         
					              SELECT
                        'D' AS INDDETREC,
                        'ABPRODT' AS TABLAIFRS17,
                        '' AS PK,
                        '' AS DTPREG,
                        '' AS TIOCPROC,
                        CAST(C_NL_PROD.VALID_FROM AS DATE)  AS TIOCFRM,
                        '' AS TIOCTO,
                        'PNV' AS KGIORIGM,
                        C_NL_PROD.PRODUCT_CODE || '-' || C_PARAM_V.PARAM_VALUE AS DCODIGO,
                        '' AS DDESC,
                        '' AS KGCRAMO,
                        'LPV' AS DCOMPA,
                        '' AS DMARCA,
                        'PER' AS KACPAIS,
                        COALESCE(C_NL_PROD.PRODUCT_LOB, '')  AS KACTPPRD,
                        '' AS KACTPSUB,
                        '' AS KACPARES,
                        COALESCE
                        (
                          (
                            SELECT max('SI')
                            FROM CFG_NL_COVERS CNC 
                            WHERE CNC.PRODUCT_LINK_ID = C_NL_PROD.PRODUCT_LINK_ID
                            AND CNC.MANDATORY = 'Y'
                            AND COVER_DESIGNATION = 'INV_GCV'
                        ),'NO'
                        ) AS KACRESGA,
                        '' AS KACINPPR, 
                        '' AS KACCLREN,
                        '' AS KACTPCTR,
                        '' AS KACTPGRP,
                        '' AS KACCRHAB, 
                        COALESCE(C_NL_PROD.VALID_FROM, '')  AS TINICOME,
                        '' AS TFIMCOME,
                        '' AS KACFPCAP,
                        '' AS DINDREDU,  
                        '' AS DINDRESG,
                        '' AS KACTPDUR,
                        '' AS DCDCNGR, 
                        '' AS KACREINTE,
                        '' AS KABPRODT_RINV, 
                        '' AS DSEGMENT,   
                        '' AS KACSEGMEN,
                        '' AS KACTARTAB,    
                        '' AS KACTPCRED,  
                        '' AS KACTPOPS,   
                        '' AS KACMOEDA,  
                        '' AS KACPERISC,   
                        '' AS DINDGRPADES, 
                        '' AS DINDENTEXT,
                        '' AS KACSBTPRD,
                        '' AS KACTPPMA,  
                        '' AS KACTIPIFAP 
                        FROM CFG_NL_PRODUCT C_NL_PROD
                        INNER JOIN CFG_NL_PRODUCT_CONDS C_NL_PROD_C ON C_NL_PROD.PRODUCT_LINK_ID = C_NL_PROD_C.PRODUCT_LINK_ID
                       	INNER JOIN CPR_PARAMS C_PARAM ON C_NL_PROD_C.PARAM_CPR_ID = C_PARAM.PARAM_CPR_ID AND C_PARAM.FOLDER = 'LPV' AND C_PARAM.PARAM_NAME LIKE 'AS_IS%'
                        JOIN CPRS_PARAM_VALUE C_PARAM_V ON C_PARAM.PARAM_CPR_ID = C_PARAM_V.PARAM_ID                          
                       '''   
  
  #--------------------------------------------------------------------------------------------------------------------------#
  spark = glue_context.spark_session
  l_df_abprodt = None
  
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
    
    if l_df_abprodt is None:
      l_df_abprodt = current_df
    else: 
      # Ejecutar la consulta final
      l_df_abprodt = l_df_abprodt.union(current_df)
    current_df.show()
  
  print('Proceso Final')
  l_df_abprodt.show()

  return l_df_abprodt