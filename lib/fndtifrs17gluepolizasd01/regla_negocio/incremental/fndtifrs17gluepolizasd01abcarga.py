from pyspark.sql.types import DecimalType
from pyspark.sql.functions import col

def get_data(glue_context, connection, p_fecha_inicio, p_fecha_fin):         

   l_polizas_vtime_general = f'''
                             (
                               SELECT  
                               'D' INDDETREC, 
                               'ABCARGA' TABLAIFRS17, 
                               '' PK,      
                               '' DTPREG,   --NO
                               '' TIOCPROC, --NO
                               COALESCE(CAST(CAST(DX."DEFFECDATE" AS DATE) AS VARCHAR), '') TIOCFRM, --PENDIENTE
                               '' TIOCTO,   --NO
                               'PVG' KGIORIGM, 
                               COALESCE(DX."NBRANCH",0) || '-' || COALESCE(DX."NPRODUCT",0) || '-' || COALESCE(DX."NPOLICY",0) || '-' || COALESCE(DX."NCERTIF",0) KABAPOL, --FK
                               '' KABUNRIS, --FK  
                               '' KGCTPCBT, 
                               '' KACCDFDO, --VALOR VACIO
                               COALESCE(DE."SDISEXPRI" , '') KACTPCAG,
                               DX."NBRANCH" || '-' || DX."NPRODUCT" || '-' || DX."NDISC_CODE" KACCDCAG, 
                               DX."NAMOUNT" VMTCARGA, 
                               COALESCE(CAST(CAST(DX."DEFFECDATE" AS DATE) AS VARCHAR), '') TULTMALT, 
                               '' DUSRUPD, --NO
                               'LPG' DCOMPA, 
                               '' DMARCA,  --NO
                               '' DINCPRM, --VALOR VACIO
                               CASE 
                               WHEN ( DX."NAMOUNT" != 0 AND DX."NAMOUNT" IS NOT NULL) AND ( CAST(DX."NPERCENT" AS INTEGER)= 0 AND DX."NPERCENT" IS NULL) 
                               THEN 'IMPORTE' 
                               ELSE 'PORCENTAJE' 
                               END KACTPVCG, 
                               '' DDURACAO, 
                               '' KACTPCBB --VALOR VACIO
                               FROM USVTIMG01."POLICY" P
                               LEFT JOIN USVTIMG01."CERTIFICAT" CERT 
                               	    ON  CERT."SCERTYPE" = P."SCERTYPE" 
                               	    AND CERT."NBRANCH"  = P."NBRANCH" 
                               	    AND CERT."NPRODUCT" = P."NPRODUCT" 
                               	    AND CERT."NPOLICY"  = P."NPOLICY"
                               JOIN USVTIMG01."DISC_XPREM" DX        
                               	    ON  DX."SCERTYPE" = P."SCERTYPE" 
                               	    AND DX."NBRANCH"  = P."NBRANCH" 
                               	    AND DX."NPRODUCT" = P."NPRODUCT" 
                               	    AND DX."NPOLICY"  = P."NPOLICY" 
                               	    AND DX."NCERTIF"  = CERT."NCERTIF" 
                               	    AND DX."DEFFECDATE" <= P."DSTARTDATE" 
                               	    AND (DX."DNULLDATE" IS NULL OR DX."DNULLDATE" > P."DSTARTDATE")
                               LEFT JOIN USVTIMG01."DISCO_EXPR" DE   
                               	    ON  DX."NBRANCH"    = DE."NBRANCH" 
                               	    AND DX."NPRODUCT"   = DE."NPRODUCT" 
                               	    AND DX."NDISC_CODE" = DE."NDISEXPRC"
                               WHERE P."SCERTYPE" = '2' 
                               AND P."SSTATUS_POL" NOT IN ('2', '3')
                               AND (
                                     (P."SPOLITYPE" = '1' AND P."DCOMPDATE" BETWEEN '{p_fecha_inicio}' AND '{p_fecha_fin}')
                               OR 
                               	     (P."SPOLITYPE" <> '1' AND CERT."DCOMPDATE" BETWEEN '{p_fecha_inicio}' AND '{p_fecha_fin}')
                                   )
                             ) AS TMP
                            '''
   
   l_df_polizas_vtime_general = glue_context.read.format('jdbc').options(**connection).option("dbtable",l_polizas_vtime_general).load() 

   print('USVTIMG01 EXITOSO')

   l_polizas_vtime_vida = f'''
                          (
                            SELECT 
                            'D' INDDETREC, 
                            'ABCARGA' TABLAIFRS17, 
                            '' PK, --PENDIENTE
                            '' DTPREG, --NO
                            '' TIOCPROC, --NO
                            COALESCE(CAST(CAST(DX."DEFFECDATE" AS DATE) AS VARCHAR), '') TIOCFRM, --PENDIENTE
                            '' TIOCTO, --NO
                            'PVV' KGIORIGM, 
                            COALESCE(DX."NBRANCH", 0) || '-' || COALESCE(DX."NPRODUCT",0) || '-' || COALESCE(DX."NPOLICY",0) || '-' || COALESCE(DX."NCERTIF",0) KABAPOL, --FK
                            '' KABUNRIS, --VALOR VACIO  
                            '' KGCTPCBT, 
                            '' KACCDFDO, --VALOR VACIO
                            COALESCE(DE."SDISEXPRI" , '') KACTPCAG,
                            DE."NBRANCH" || '-' || DE."NPRODUCT" || '-' || DE."NDISEXPRC" KACCDCAG,
                            DX."NAMOUNT" VMTCARGA, 
                            COALESCE(CAST(CAST(DX."DEFFECDATE" AS DATE) AS VARCHAR), '') TULTMALT, 
                            '' DUSRUPD, --NO
                            'LPV' DCOMPA, 
                            '' DMARCA, --NO
                            '' DINCPRM, --VALOR VACIO
                            CASE 
                            WHEN (DX."NAMOUNT" != 0 AND DX."NAMOUNT" IS NOT NULL) AND (CAST(DX."NPERCENT" AS INTEGER) = 0 AND DX."NPERCENT" IS NULL) THEN 'IMPORTE' 
                            ELSE 'PORCENTAJE' 
                            END KACTPVCG, 
                            '' DDURACAO, --VALOR VACIO
                            '' KACTPCBB --VALOR VACIO                 
                            FROM USVTIMV01."POLICY" P
                            LEFT JOIN USVTIMV01."CERTIFICAT" CERT 
                              ON CERT."SCERTYPE"  = P."SCERTYPE" 
                              AND CERT."NBRANCH"  = P."NBRANCH" 
                              AND CERT."NPRODUCT" = P."NPRODUCT" 
                              AND CERT."NPOLICY"  = P."NPOLICY"
                            JOIN USVTIMV01."DISC_XPREM" DX 
                              ON DX."SCERTYPE"  = P."SCERTYPE" 
                              AND DX."NBRANCH"  = P."NBRANCH" 
                              AND DX."NPRODUCT" = P."NPRODUCT"
                              AND DX."NPOLICY"  = P."NPOLICY" 
                              AND DX."NCERTIF"  = CERT."NCERTIF" 
                              AND DX."DEFFECDATE" <= P."DSTARTDATE" 
                              AND (DX."DNULLDATE" IS NULL OR DX."DNULLDATE" > P."DSTARTDATE")
                            LEFT JOIN USVTIMV01."DISCO_EXPR" DE 
                              ON DX."NBRANCH"     = DE."NBRANCH" 
                              AND DX."NPRODUCT"   = DE."NPRODUCT" 
                              AND DX."NDISC_CODE" = DE."NDISEXPRC"
                            WHERE P."SCERTYPE" = '2' 
                            AND P."SSTATUS_POL" NOT IN ('2', '3')
                            AND (
                                  (P."SPOLITYPE" = '1' AND P."DCOMPDATE" BETWEEN '{p_fecha_inicio}' AND '{p_fecha_fin}')
                                  OR  
                                  (P."SPOLITYPE" <> '1' AND CERT."DCOMPDATE" BETWEEN '{p_fecha_inicio}' AND '{p_fecha_fin}')                              
                            )
                          ) AS TMP
                          '''
   
   l_df_polizas_vtime_vida = glue_context.read.format('jdbc').options(**connection).option("dbtable",l_polizas_vtime_vida).load() 
   
   print('USVTIMV01 EXITOSO')
   
   #----------------------------------------------------------------------------------------------------------------------------------#

   #DECLARAR CONSULTA INSUNIX
   l_polizas_insunix_general = f'''
                               (
                                 SELECT
                                 'D' INDDETREC,
                                 'ABCARGA' TABLAIFRS17,
                                 '' PK,--PENDIENTE
                                 '' DTPREG,--NO
                                 '' TIOCPROC,--NO
                                 COALESCE(CAST(CAST(DX.EFFECDATE AS DATE) AS VARCHAR),'') TIOCFRM,--PENDIENTE
                                 '' TIOCTO,--NO
                                 'PIG' KGIORIGM,
                                 COALESCE(DX.BRANCH, 0) || '-' || COALESCE(DX.PRODUCT,0) || '-' || COALESCE(DX.SUB_PRODUCT,0) || '-' || COALESCE(DX.POLICY,0) || '-' || COALESCE(DX.CERTIF,0) KABAPOL,--FK PENDIENTE
                                 '' KABUNRIS,  --VALOR VACIO
                                 '' KGCTPCBT,  --VALOR VACIO
                                 '' KACCDFDO,  -- VALOR VACIO
                                 COALESCE(DX.TYPE, '') KACTPCAG,
                                 DX.BRANCH || '-' || DX.PRODUCT || '-' || DX.SUB_PRODUCT || '-' || DX.CURRENCY || '-' || DX.DISEXPRC || '-' || DX.DISEXPRI  KACCDCAG,
                                 COALESCE(DX.AMOUNT, 0) VMTCARGA,
                                 COALESCE(CAST(CAST(DX.EFFECDATE AS DATE) AS VARCHAR),'') TULTMALT,
                                 '' DUSRUPD,   --NO
                                 'LPG' DCOMPA,
                                 '' DMARCA,    --NO
                                 '' DINCPRM,    --VALOR VACIO
                                 CASE
                                 WHEN (DX.AMOUNT != 0 AND DX.AMOUNT IS NOT NULL) AND (CAST(DX.PERCENT AS INTEGER) = 0 AND DX.PERCENT IS NULL) THEN 'IMPORTE'
                                 ELSE 'PORCENTAJE'
                                 END KACTPVCG,
                                 '' DDURACAO, --VALOR VACIO
                                 '' KACTPCBB  --VALOR VACIO
                                 FROM
                                 (
                                  SELECT PC.*, 
                                  DX.EFFECDATE,
                                  DX.TYPE,
                                  DX.AMOUNT,
                                  DX.PERCENT ,    
                                  DEX.CURRENCY,
                                  DEX.DISEXPRI,
                                  DEX.DISEXPRC
                                  FROM 
                                  (
                                    SELECT              
                                    P.USERCOMP,
                                    P.COMPANY,
                                    P.BRANCH,
                                    P.PRODUCT,
                                    PSP.SUB_PRODUCT,
                                    P.POLICY,
                                    P.CERTYPE, 
                                    P.POLITYPE,
                                    P.STATUS_POL,
                                    COALESCE(CERT.CERTIF, 0) CERTIF,
                                    P.COMPDATE P_COMPDATE,
                                    CERT.COMPDATE C_COMPDATE,     
                                    CASE 
                                        WHEN P.POLITYPE  = '1' THEN P.EFFECDATE
                                        WHEN P.POLITYPE <> '1' THEN CERT.EFFECDATE 
                                    END EFFECDATE_VALID     
                                    FROM USINSUG01.POLICY P 
                                    LEFT JOIN USINSUG01.CERTIFICAT CERT 
                                    	ON CERT.USERCOMP = P.USERCOMP 
                                    	AND CERT.COMPANY = P.COMPANY 
                                    	AND CERT.CERTYPE = P.CERTYPE 
                                    	AND CERT.BRANCH  = P.BRANCH 
                                    	AND CERT.PRODUCT = P.PRODUCT 
                                    	AND CERT.POLICY  = P.POLICY                                     
                                    JOIN USINSUG01.POL_SUBPRODUCT PSP 
                                    	ON  PSP.USERCOMP = P.USERCOMP 
                                    	AND PSP.COMPANY  = P.COMPANY 
                                    	AND PSP.CERTYPE  = P.CERTYPE 
                                    	AND PSP.BRANCH   = P.BRANCH 
                                    	AND PSP.POLICY   = P.POLICY		    
                                    	AND PSP.PRODUCT  = P.PRODUCT           
                                    )PC
                                    LEFT JOIN USINSUG01.DISC_XPREM DX 
                                      ON DX.USERCOMP = PC.USERCOMP 
                                      AND DX.COMPANY = PC.COMPANY 
                                      AND DX.BRANCH  = PC.BRANCH 
                                      AND DX.CERTYPE = PC.CERTYPE 
                                      AND DX.POLICY  = PC.POLICY 
                                      AND DX.CERTIF  = PC.CERTIF 
                                      AND DX.EFFECDATE <= (PC.EFFECDATE_VALID)
                                      AND (DX.NULLDATE IS NULL OR DX.NULLDATE > PC.EFFECDATE_VALID)
                                    LEFT JOIN USINSUG01.DISCO_EXPR DEX
                                      ON  DEX.USERCOMP = DX.USERCOMP
                                      AND DEX.COMPANY  = DX.COMPANY
                                      AND DEX.BRANCH   = DX.BRANCH
                                      AND DEX.PRODUCT  = PC.PRODUCT
                                      AND DEX.SUB_PRODUCT = PC.SUB_PRODUCT
                                      AND DEX.CURRENCY = DX.CURRENCY
                                      WHERE PC.CERTYPE  = '2'
                                      AND   PC.STATUS_POL NOT IN ('2','3')  -- 2: que no esten invalidas  3: que no esten pendientes de informacion
                                      AND (
                                            (P.POLITYPE = '1' AND P.COMPDATE between '{p_fecha_inicio}' AND '{p_fecha_fin}') --individual
                                          OR 
                                            (P.POLITYPE <> '1' AND CERT.COMPDATE between '{p_fecha_inicio}' AND '{p_fecha_fin}') --colectiva
                                          )
                                  ) DX                                   
                                ) AS TMP
                                '''
   #Ejecutar consulta
   l_df_polizas_insunix_general = glue_context.read.format('jdbc').options(**connection).option("dbtable",l_polizas_insunix_general).load()

   print('USINSUG01 EXITOSO')
   
   l_polizas_insunix_vida = f'''
                            (                              
                              SELECT
                              'D' INDDETREC, 
                              'ABCARGA' TABLAIFRS17, 
                              '' PK, --PENDIENTE
                              '' DTPREG, --NO
                              '' TIOCPROC, --NO
                              COALESCE(CAST(CAST(DX.EFFECDATE AS DATE) AS VARCHAR), '') TIOCFRM, --PENDIENTE
                              '' TIOCTO, --NO
                              'PIV' KGIORIGM, 
                              COALESCE(PC.BRANCH, 0) || '-' || COALESCE(PC.PRODUCT, 0) || '-' || COALESCE(PC.POLICY,0) || '-' || PC.CERTIF KABAPOL, --FK
                              '' KABUNRIS, --VALOR VACIO
                              '' KGCTPCBT, 
                              '' KACCDFDO, --VALOR VACIO
                              COALESCE(DX.TYPE, '') KACTPCAG, 
                              DX.CURRENCY,
                              COALESCE(DX.BRANCH, 0) || '-' || COALESCE(PC.PRODUCT, 0) || '-' || COALESCE(DX.CURRENCY, 0) || '-' || COALESCE(DX.CODE, 0) || '-' || COALESCE(DX.TYPE, '0') KACCDCAG,
                              COALESCE (DX.AMOUNT, 0) VMTCARGA, 
                              COALESCE (CAST(CAST(DX.EFFECDATE AS DATE) AS VARCHAR), '') TULTMALT, 
                              '' DUSRUPD, --NO
                              'LPV' DCOMPA, 
                              '' DMARCA, --NO
                              '' DINCPRM, 
                              CASE WHEN (DX.AMOUNT != 0 AND DX.AMOUNT IS NOT NULL) AND (CAST(DX.PERCENT AS INTEGER)= 0 AND DX.PERCENT IS NULL) 
                              THEN 'IMPORTE' ELSE 'PORCENTAJE' END KACTPVCG, 
                              '' DDURACAO, 
                              '' KACTPCBB --VALOR VACIO 
                              FROM 
                              (
                              	SELECT 
                              	P.USERCOMP, 
                              	P.COMPANY, 
                              	P.CERTYPE, 
                              	P.BRANCH, 
                              	P.PRODUCT, 
                              	P.POLICY, 
                              	CASE 
                              		  WHEN P.POLITYPE = '1' THEN 0
                                   	WHEN P.POLITYPE <> '1'THEN C.CERTIF
                              	END CERTIF, --PARA POLIZAS INDV
                              	CASE
                              	    WHEN P.POLITYPE = '1'  THEN P.EFFECDATE 
                              	    WHEN P.POLITYPE <> '1' THEN C.EFFECDATE 
                              	END  EFFECDATE
                              	FROM USINSUV01.POLICY P 
                              	LEFT JOIN USINSUV01.CERTIFICAT C
                              	ON  C.USERCOMP = P.USERCOMP 
                              	AND C.COMPANY  = P.COMPANY 
                              	AND C.CERTYPE  = P.CERTYPE 
                              	AND C.BRANCH   = P.BRANCH 
                              	AND C.POLICY   = P.POLICY  
                              	AND C.PRODUCT  = P.PRODUCT
                              	WHERE P.CERTYPE  = '2' AND P.STATUS_POL NOT IN ('2','3') 
                              	AND (
                              			(P.POLITYPE = '1' AND P.COMPDATE BETWEEN '{p_fecha_inicio}' AND '{p_fecha_fin}') 
                              		OR 
                              			(P.POLITYPE <> '1' AND  C.COMPDATE BETWEEN  '{p_fecha_inicio}'AND '{p_fecha_fin}')
                              		)
                              ) PC
                               LEFT JOIN USINSUV01.DISC_XPREM DX 
                              	    ON DX.USERCOMP = PC.USERCOMP 
                              	    AND DX.COMPANY = PC.COMPANY 
                              	    AND DX.CERTYPE = PC.CERTYPE 
                              	    AND DX.BRANCH  = PC.BRANCH 
                              	    AND DX.POLICY  = PC.POLICY 
                              	    AND DX.CERTIF  = PC.CERTIF 
                              	    AND DX.EFFECDATE <= PC.EFFECDATE
                              	    AND (DX.NULLDATE IS NULL OR DX.NULLDATE > PC.EFFECDATE)	    
                            ) AS TMP
                            '''
   #Ejecutar consulta
   l_df_polizas_insunix_vida = glue_context.read.format('jdbc').options(**connection).option("dbtable", l_polizas_insunix_vida).load()
   
   print('USINSUV01 EXITOSO')

   #----------------------------------------------------------------------------------------------------------------------------------#

   #Declara consulta INSIS
   l_polizas_insis = f'''
                      (
                        SELECT
                          'D' INDDETREC,
                          'ABCARGA' TABLAIFRS17,
                          '' PK,
                          '' DTPREG,
                          '' TIOCPROC,
                          coalesce(CAST(CAST(GRC."INSR_BEGIN" AS DATE)AS VARCHAR),'') AS TIOCFRM, --BEGIN OF INSURING. puede tambien ser "REGISTRATION_DATE"
                          '' TIOCTO,
                          'PNV' KGIORIGM,
                          SUBSTRING(CAST(P."POLICY_ID" AS VARCHAR),6,12)  KABAPOL,
                          SUBSTRING(CAST(P."POLICY_ID" AS VARCHAR),6,12) || '-' || COALESCE(CAST(GRD."INSURED_OBJ_ID" AS VARCHAR),'0') AS KABUNRIS,
                          '' KGCTPCBT,--EN BLANCO
                          '' KACCDFDO,--EN BLANCO
                          COALESCE(GRD."DISCOUNT_TYPE", '') AS KACTPCAG,
                          TRUNC(GRD."DISCOUNT_ID", 0) AS KACCDCAG,
                          GRD."DISCOUNT_VALUE" AS VMTCARGA,
                          '' TULTMALT,--EN BLANCO
                          '' DUSRUPD,
                          'LPV' DCOMPA,
                          '' DMARCA,
                          '' DINCPRM,--EN BLANCO
                          '' KACTPVCG,--EN BLANCO 
                          '' DDURACAO,--EN BLANCO
                          COALESCE(GRD."COVER_TYPE",'') AS KACTPCBB
                          FROM
                          USINSIV01."POLICY" P
                          LEFT JOIN USINSIV01."POLICY_ENG_POLICIES" PP 
                              ON P."POLICY_ID" = PP."POLICY_ID" 
                          LEFT JOIN USINSIV01."GEN_RISK_DISCOUNT" GRD --TABLA DE RECARGOS Y DESCUENTOS (CONTIENE EL DETALLE) 
                              ON GRD."POLICY_ID" = P."POLICY_ID"
                          left join usinsiv01."GEN_RISK_COVERED" grc 
                              on grc."POLICY_ID" = GRD."POLICY_ID" 
                              and grc."ANNEX_ID" = GRD."ANNEX_ID" 
                              and grc."INSURED_OBJ_ID" = GRD."INSURED_OBJ_ID" 
                              and grc."COVER_TYPE" = grc."COVER_TYPE"
                          WHERE  
                          P."REGISTRATION_DATE" BETWEEN '{p_fecha_inicio}' AND '{p_fecha_fin}'
                      ) AS TMP
                      '''

   #Ejecutar consulta
   l_df_polizas_insis = glue_context.read.format('jdbc').options(**connection).option("dbtable",l_polizas_insis).load()

   print('USINSIV01 EXITOSO')
   
   #----------------------------------------------------------------------------------------------------------------------------------#

   #Perform the union operation
   l_df_polizas = l_df_polizas_vtime_general.union(l_df_polizas_vtime_vida).union(l_df_polizas_insis).union(l_df_polizas_insunix_general).union(l_df_polizas_insunix_vida)

   l_df_polizas = l_df_polizas.withColumn("VMTCARGA", col("VMTCARGA").cast(DecimalType(15, 5)))

   return l_df_polizas