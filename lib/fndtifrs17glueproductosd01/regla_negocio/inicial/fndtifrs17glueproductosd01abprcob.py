def get_data(glue_context, bucket ,tablas):  
  l_abprcob_insunix_lpg = '''
                            SELECT 
                              'D' INDDETREC,
                              'ABPRCOB' TABLAIFRS17,
                              '' AS PK,
                              '' AS DTPREG,       
                              '' AS TIOCPROC,     
                              COALESCE(CAST(GC.EFFECDATE AS STRING), '') AS TIOCFRM,
                              '' AS TIOCTO,  	    
                              'PIG' AS KGIORIGM,  
                              'LPG' AS DCOMPA,
                              '' AS DMARCA,       
                              COALESCE(P.BRANCH, 0) || '-' || COALESCE (P.PRODUCT, 0) || '-' ||COALESCE(P.SUB_PRODUCT,0) AS KABPRODT,
                              COALESCE(CAST(GC.COVERGEN AS STRING), '') || '-' || COALESCE(GC.CURRENCY, 0) AS KGCTPCBT,
                              '' AS KACINDOPS,    
                              '' AS KACTIPCB,
                              '' AS KACTCOMP,     
                              '' AS DINDNIVEL,     
                              COALESCE(GC.ADDSUINI, '') AS KACSOCAP,
                              CASE 
                              WHEN GC.ROUCAPIT IS NOT NULL AND TRIM(GC.ROUCAPIT) != '' THEN 'RUTINA'
                              WHEN GC.CACALFIX IS NOT NULL THEN 'FIJO'
                              WHEN GC.CACALCOV IS NOT NULL THEN 'OTRACOBERTURA'
                              WHEN GC.CACALFRI = '1' THEN 'LIBRE'
                              WHEN GC.CACALILI = '1' THEN 'ILIMITADO'
                              ELSE ''
                              END KACFMCAL,
                              '' AS KACTPDPRIS,
                              '' AS KGCTPCBT_SUP,    
                              --GC.MODULEC AS KGCTPCBT_SUP, 
                              '' AS DINDANOS,      
                              '' AS KSCTPDAN,      
                              '' AS KACGPRIS,      
                              '' AS KGRAMO_SAP,    
                              '' AS DDURLIMINF,
                              '' AS DDURLIMSUP,
                              '' AS DLIMINFMASC, 
                              '' AS DLIMSUPMASC,
                              '' AS DLIMINFFEM,    
                              '' AS DLIMSUPFEM,
                              '' AS KACCLCAP,
                              '' AS KACFCAP,
                              '' AS DINDCARPF,
                              '' AS KACVALCB,      
                              COALESCE(CAST(GC.CACALMAX AS STRING), '') AS VMTLIMCB,
                              '' AS VMTDEFCB,      
                              '' AS VTXLIMCB,
                              '' AS VTXDEFCB,      
                              '' AS KACTPTARCB,    
                              '' AS DINDLIBTAR
                              FROM GEN_COVER GC 
                              JOIN (SELECT	
	                                  PRO.PRODUCT,
	                                  PRO.BRANCH,
                                    PRO.SUB_PRODUCT,
                                    CASE WHEN PRO.NULLDATE IS NOT NULL THEN 1 ELSE 0 END FLAG_NULLDATE
                                    FROM (	
                                            SELECT	PRO.*,
                                            CASE	
                                            WHEN PRO.NULLDATE IS NULL THEN	PRO.id_replicacion_positiva
	                          	 	            ELSE	CASE	
                                                  WHEN
	                          	 	        			    (	SELECT  count(*)
	                          	 	        			    	FROM	PRODUCT PR1
	                          	 	        			    	WHERE PR1.USERCOMP = PRO.USERCOMP
	                          	 	        			    	AND 	PR1.COMPANY = PRO.COMPANY
	                          	 	        			    	AND 	PR1.BRANCH = PRO.BRANCH
	                          	 	        			    	AND 	PR1.PRODUCT = PRO.PRODUCT
	                          	 	        			    	AND		PR1.NULLDATE IS NULL) > 0 THEN 	NULL
	                          	 	        		ELSE  CASE	
                                                  WHEN PRO.NULLDATE =  
	                          	 	        			 		(	SELECT	MAX(PR1.NULLDATE)
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
                              ON GC.BRANCH = P.BRANCH  AND GC.PRODUCT  = P.PRODUCT LIMIT 4
                              '''                                
  l_abprcob_insunix_lpv = '''
                              SELECT 
                              'D' INDDETREC,
                              'ABPRCOB' TABLAIFRS17,
                              '' AS PK,
                              '' AS DTPREG,       
                              '' AS TIOCPROC,     
                              COALESCE(CAST(LC.EFFECDATE AS STRING), '')   AS TIOCFRM,
                              '' AS TIOCTO,  	    
                              'PIV' AS KGIORIGM,  
                              'LPV' AS DCOMPA,   
                              '' AS DMARCA,       
                              COALESCE(LC.BRANCH, 0) || '-' || COALESCE (LC.PRODUCT, 0)  AS KABPRODT,
                              COALESCE(CAST(LC.COVERGEN as STRING), '') || '-' || COALESCE(LC.CURRENCY, 0) AS KGCTPCBT,
                              '' AS KACINDOPS,    
                              '' AS KACTIPCB,    
                              '' AS KACTCOMP,     
                              '' AS DINDNIVEL,     
                              COALESCE(LC.ADDCAPII, '') AS KACSOCAP,
                              CASE 
                              WHEN LC.ROUCHACA IS NOT NULL THEN 'RUTINA'
                              WHEN LC.CACALFIX IS NOT NULL THEN 'FIJO'
                              WHEN LC.CACALCOV IS NOT NULL THEN 'OTRACOBERTURA'
                              WHEN LC.CACALFRI = '1' THEN 'LIBRE'
                              ELSE ''
                              END KACFMCAL,
                              '' AS KACTPDPRIS,    
                              '' AS KGCTPCBT_SUP,
                              --0 AS KGCTPCBT_SUP,
                              '' AS DINDANOS,      
                              '' AS KSCTPDAN,      
                              '' AS KACGPRIS,      
                              '' AS KGRAMO_SAP,    
                              '' AS DDURLIMINF,
                              '' AS DDURLIMSUP,
                              '' AS DLIMINFMASC,  
                              '' AS DLIMSUPMASC,
                              '' AS DLIMINFFEM,    
                              '' AS DLIMSUPFEM,
                              '' AS KACCLCAP,
                              '' AS KACFCAP,
                              '' AS DINDCARPF,
                              '' AS KACVALCB,      
                              '' AS VMTLIMCB,
                              '' AS VMTDEFCB,      
                              '' AS VTXLIMCB,
                              '' AS VTXDEFCB,      
                              '' AS KACTPTARCB,    
                              '' AS DINDLIBTAR
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
  #----------------------------------------------------------------------------------------------------------------------#

  l_abprcob_vtime_lpg = '''
                        SELECT 
                          'D' INDDETREC,
                          'ABPRCOB' TABLAIFRS17,
                          '' AS PK,
                          '' AS DTPREG,
                          '' AS TIOCPROC,
                          CAST(CAST(GC.DEFFECDATE AS DATE) AS STRING) AS TIOCFRM,
                          '' AS TIOCTO,
                          'PVG' AS KGIORIGM,
                          'LPG' AS DCOMPA,
                          '' AS DMARCA,
                          COALESCE(P.NBRANCH, 0) || '-' || COALESCE(P.NPRODUCT, 0) AS KABPRODT,
                          COALESCE(CAST(GC.NCOVERGEN AS STRING), '') AS KGCTPCBT,
                          '' AS KACINDOPS,
                          '' AS KACTIPCB,
                          '' AS KACTCOMP,
                          '' AS DINDNIVEL,
                          COALESCE(GC.SADDSUINI, '') AS KACSOCAP,
                          CASE 
                          WHEN GC.SROUCAPIT  IS NOT NULL THEN 'RUTINA'
                          WHEN GC.NCACALFIX  IS NOT NULL THEN 'FIJO'
                          WHEN GC.NCACALCOV  IS NOT NULL THEN 'OTRACOBERTURA'
                          WHEN GC.SCACALFRI  = '1' THEN 'LIBRE'
                          WHEN GC.SCACALILI  = '1' THEN 'ILIMITADO'
                          ELSE ''
                          END KACFMCAL,
                          '' AS KACTPDPRIS,
                          '' AS KGCTPCBT_SUP,
                          --GC.NMODULEC AS KGCTPCBT_SUP,
                          '' AS DINDANOS,
                          '' AS KSCTPDAN,
                          '' AS KACGPRIS,
                          '' AS KGCRAMO_SAP,
                          '' AS DDURLIMINF,
                          '' AS DDURLIMSUP,
                          '' AS DLIMINFMASC,
                          '' AS DLIMSUPMASC,
                          '' AS DLIMINFFEM,
                          '' AS DLIMSUPFEM,
                          '' AS KACCLCAP,
                          '' AS KACFPCAP,
                          '' AS DINDCAPF,
                          '' AS KACVALCB,
                          '' AS VMTLIMCB,
                          '' AS VMTDEFCB,
                          '' AS VTXLIMCB,
                          '' AS VTXDEFCB,
                          '' AS KACTPTARCB,
                          '' AS DINDLIBTAR
                          FROM GEN_COVER GC  
                          LEFT JOIN ( SELECT	
                                    PRO.DEFFECDATE,
                                    PRO.NPRODUCT,
                                    PRO.NBRANCH,
                                    PRO.DNULLDATE,
                                      CASE WHEN PRO.DNULLDATE IS NOT NULL THEN 1 ELSE 0 END FLAG_NULLDATE
                                      FROM (SELECT PRO.*,
                                            CASE	
                                            WHEN PRO.DNULLDATE IS NULL THEN	PRO.id_replicacion_positiva
                                      ELSE	CASE	
                                            WHEN (	SELECT count(*)
                                            FROM	PRODUCT PR1
                                            WHERE PR1.NBRANCH  = PRO.NBRANCH
                                            AND 	PR1.NPRODUCT = PRO.NPRODUCT
                                            AND	PR1.DNULLDATE IS NULL) > 0 THEN NULL
                                                ELSE  CASE	
                                                                WHEN PRO.DNULLDATE = (SELECT MAX(PR1.DNULLDATE)
                                                          FROM	PRODUCT PR1
                                                          WHERE PR1.NBRANCH  = PRO.NBRANCH 
                                                          AND 	PR1.NPRODUCT = PRO.NPRODUCT) THEN PRO.id_replicacion_positiva
                                                  ELSE NULL 
                                                                END 
                                                          END 
                                          END PRO_ID
                                    FROM PRODUCT PRO) PR0, PRODUCT PRO WHERE PRO.id_replicacion_positiva = PR0.PRO_ID) P 
                          ON GC.NBRANCH = P.NBRANCH AND GC.NPRODUCT  = P.NPRODUCT
                        '''
                                       
  l_abprcob_vtime_lpv = '''
                            SELECT 
                            'D' AS INDDETREC,
                            'ABPRCOB' AS TABLAIFRS17,
                            '' AS PK,
                            '' AS DTPREG,
                            '' AS TIOCPROC,
                            COALESCE(CAST(CAST(LC.DEFFECDATE AS DATE) AS STRING), '') AS TIOCFRM,
                            '' AS TIOCTO,
                            'PVV' AS KGIORIGM,
                            'LPV' AS DCOMPA,
                            '' AS DMARCA,
                            COALESCE(P.NBRANCH, 0) || '-' || COALESCE(P.NPRODUCT, 0) AS KABPRODT,
                            COALESCE(CAST(LC.NCOVERGEN AS STRING), '') AS KGCTPCBT,
                            '' AS KACINDOPS,
                            '' AS KACTIPCB,
                            '' AS KACTCOMP,
                            '' AS DINDNIVEL,
                            COALESCE(LC.SADDSUINI, '') AS KACSOCAP,
                            CASE 
                            WHEN LC.SROURESER  IS NOT NULL THEN 'RUTINA'
                            ELSE ''
                            END KACFMCAL,
                            '' AS KACTPDPRIS,
                            '' AS KGCTPCBT_SUP,
                            --LC.NMODULEC AS KGCTPCBT_SUP,
                            '' AS DINDANOS,
                            '' AS KSCTPDAN,
                            '' AS KACGPRIS,
                            '' AS KGCRAMO_SAP,
                            '' AS DDURLIMINF,
                            '' AS DDURLIMSUP,
                            '' AS DLIMINFMASC,
                            '' AS DLIMSUPMASC,
                            '' AS DLIMINFFEM,
                            '' AS DLIMSUPFEM,
                            '' AS KACCLCAP,
                            '' AS KACFPCAP,
                            '' AS DINDCAPF,
                            '' AS KACVALCB,
                            '' AS VMTLIMCB,
                            '' AS VMTDEFCB,
                            '' AS VTXLIMCB,
                            '' AS VTXDEFCB,
                            '' AS KACTPTARCB,
                            '' AS DINDLIBTAR
                            FROM LIFE_COVER LC  
                            LEFT JOIN (
                              SELECT	
                              PRO.DEFFECDATE,
                              PRO.NPRODUCT,
                              PRO.NBRANCH,
                              PRO.DNULLDATE,
                            CASE WHEN PRO.DNULLDATE IS NOT NULL THEN 1 ELSE 0 END FLAG_NULLDATE
                            FROM ( SELECT	PRO.*,
                                    CASE	
                                    WHEN PRO.DNULLDATE IS NULL THEN	PRO.id_replicacion_positiva
                                    ELSE	CASE	
                                          WHEN (SELECT	count(*)
                                      FROM	PRODUCT PR1
                                      WHERE PR1.NBRANCH  = PRO.NBRANCH
                                      AND 	PR1.NPRODUCT = PRO.NPRODUCT
                                      AND		PR1.DNULLDATE IS NULL) > 0 THEN NULL
                                          ELSE  CASE	
                                                            WHEN PRO.DNULLDATE = (SELECT MAX(PR1.DNULLDATE)
                                                FROM	PRODUCT PR1
                                                WHERE PR1.NBRANCH = PRO.NBRANCH 
                                                AND 	PR1.NPRODUCT = PRO.NPRODUCT) THEN PRO.id_replicacion_positiva
                                            ELSE NULL 
                                                            END 
                                          END 
                                    END PRO_ID
                                FROM	PRODUCT PRO) PR0, PRODUCT PRO WHERE PRO.id_replicacion_positiva = PR0.PRO_ID) P 
                            ON LC.NBRANCH = P.NBRANCH AND LC.NPRODUCT  = P.NPRODUCT
                        '''
  #----------------------------------------------------------------------------------------------------------------------#

  l_abprcob_insis = '''
                      SELECT
                      'D' AS INDDETREC,
                      'ABPRCOB' AS TABLAIFRS17,
                      '' AS PK,
                      '' AS DTPREG,
                      '' AS TIOCPROC,
                      CNC.VALID_FROM AS TIOCFRM,
                      '' AS TIOCTO,
                      'PNV' AS KGIORIGM,
                      'LPV' AS DCOMPA,
                      '' AS DMARCA,
                      C_NL_PROD.PRODUCT_CODE || '-' || C_PARAM_V.PARAM_VALUE AS KABPRODT,
                      SUBSTRING(CAST(CAST(CNC.COVER_REP_ID AS BIGINT) AS STRING),5,10) AS KGCTPCBT,
                      '' AS KACINDOPS,
                      CASE CNC.MANDATORY
                      WHEN 'Y' THEN 'SI'
                      WHEN 'N' THEN 'NO'
                      ELSE ''
                      END KACTIPCB,
                      '' AS KACTCOMP,
                      '' AS DINDNIVEL,
                      CAST(CAST(CNC.IV_RULE AS INT) AS STRING) AS KACSOCAP,
                      '' AS KACFMCAL,
                      '' AS KACTPDPRIS, 
                      '' AS KGCTPCBT_SUP,
                      --TRUNC(CNC.OBJECT_LINK_ID, 0) AS KGCTPCBT_SUP,
                      '' AS DINDANOS,
                      '' AS KSCTPDAN,
                      '' AS KACGPRIS,
                      '' AS KGCRAMO_SAP,
                      '' AS DDURLIMINF,
                      '' AS DDURLIMSUP,
                      '' AS DLIMINFMASC, 
                      /*(
                          SELECT CPC.DEFAULT_VALUE
                          FROM  CFGLPV_POLICY_CONDITIONS CPC
                          WHERE CPC.INSR_TYPE = 2002 
                          AND   CPC.AS_IS_PRODUCT IN (7)
                          AND   CPC.COND_TYPE = ' MAX_ENTRY_AGE'
                      ) AS DLIMSUPMASC*/
                      '' AS DLIMSUPMASC,
                      '' AS DLIMINFFEM,
                      /*(
                          SELECT CPC.DEFAULT_VALUE
                          FROM  CFGLPV_POLICY_CONDITIONS CPC
                          WHERE CPC.INSR_TYPE = 2002 
                          AND   CPC.AS_IS_PRODUCT IN (7)
                          AND   CPC.COND_TYPE = ' MAX_ENTRY_AGE'
                      ) AS DLIMSUPFEM*/
                      '' AS DLIMSUPFEM,  
                      '' AS KACCLCAP,
                      '' AS KACFCAP,
                      '' AS DINDCAPF,
                      '' AS KACVALCB,
                      /*(
                          SELECT CIL.MAX_IV
                          FROM  CFGLPV_IV_LIMITS  CIL
                          WHERE CIL.INSR_TYPE     = CNC.PRODUCT_CODE
                          AND   CIL.AS_IS_PRODUCT = SUPTIPO_PRODUCTO 
                      ) AS VMTLIMCB*/
                      '' AS VMTLIMCB,
                      '' AS VMTDEFCB,
                      '' AS VTXLIMCB,
                      '' AS VTXDEFCB,
                      '' AS KACTPTARCB,
                      '' AS DINDLIBTAR
                      FROM CFG_NL_COVERS CNC
                      LEFT JOIN CFG_NL_PRODUCT C_NL_PROD ON C_NL_PROD.PRODUCT_LINK_ID = CNC.PRODUCT_LINK_ID
                      INNER JOIN CFG_NL_PRODUCT_CONDS C_NL_PROD_C ON C_NL_PROD.PRODUCT_LINK_ID = C_NL_PROD_C.PRODUCT_LINK_ID
                      INNER JOIN CPR_PARAMS C_PARAM ON C_NL_PROD_C.PARAM_CPR_ID = C_PARAM.PARAM_CPR_ID AND C_PARAM.FOLDER = 'LPV' AND C_PARAM.PARAM_NAME LIKE 'AS_IS%'
                      JOIN CPRS_PARAM_VALUE C_PARAM_V ON C_PARAM.PARAM_CPR_ID = C_PARAM_V.PARAM_ID
                    '''
  #----------------------------------------------------------------------------------------------------------------------#
  spark = glue_context.spark_session
  l_df_abprcob = None
  
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
    
    if l_df_abprcob is None:
      l_df_abprcob = current_df
    else: 
      # Ejecutar la consulta final
      l_df_abprcob = l_df_abprcob.union(current_df)
    current_df.show()
  
  print('Proceso Final')
  l_df_abprcob.show()

  return l_df_abprcob