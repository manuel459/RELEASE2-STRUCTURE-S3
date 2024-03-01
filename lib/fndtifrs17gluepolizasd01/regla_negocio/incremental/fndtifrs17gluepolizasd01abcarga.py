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
                                '' TIOCTO, --NO
                                'PVG' KGIORIGM, 
                                COALESCE(DX."NBRANCH",0) || '-' || COALESCE(DX."NPRODUCT",0) || '-' || COALESCE(DX."NPOLICY",0) || '-' || COALESCE(DX."NCERTIF",0) KABAPOL, --FK
                                '' KABUNRIS, --FK  
                                /*
                                *  DE ACUERDO A JAOS EL DATO SE PUDIERA OBTENER EN BASE A LA PRIMERA COVERTURA PERO LOS SISTEMAS 
                                NO TIENEN ESA INFORMACION DE MANERA DIRECTA EN LA TABLA DE RECARGOS Y DESCUENTOS
                                (
                                  SELECT C."NCOVER" FROM USVTIMG01."COVER" C 
                                  WHERE C."SCERTYPE" = '2' 
                                  AND   C."NBRANCH" = DXP."NBRANCH"  
                                  AND   C."NPOLICY" = DXP."NPOLICY"
                                  AND   C."NCERTIF" = DXP."NCERTIF" 
                                  AND   C."NCOVER"  = 1
                                  AND   C."DEFFECDATE" <= DXP."DEFFECDATE" 
                                  AND (C."DNULLDATE" IS NULL OR C."DNULLDATE" > DXP."DEFFECDATE") 
                                )*/
                                '' KGCTPCBT, 
                                '' KACCDFDO, --VALOR VACIO
                                COALESCE(DE."SDISEXPRI" , '') KACTPCAG,
                                COALESCE(DX."NBRANCH",0) || '-' || COALESCE(DX."NPRODUCT",0) || '-' || COALESCE(DX."NDISC_CODE", 0) KACCDCAG, 
                                DX."NAMOUNT" VMTCARGA, 
                                COALESCE(CAST(CAST(DX."DEFFECDATE" AS DATE) AS VARCHAR), '') TULTMALT, 
                                '' DUSRUPD, --NO
                                'LPG' DCOMPA, 
                                '' DMARCA, --NO
                                '' DINCPRM, --VALOR VACIO
                                CASE 
                                WHEN ( DX."NAMOUNT" != 0 AND DX."NAMOUNT" IS NOT NULL) AND ( CAST(DX."NPERCENT" AS INTEGER)= 0 AND DX."NPERCENT" IS NULL) 
                                THEN '1' --IMPORTE 
                                ELSE '2' --PORCENTAJE
                                END KACTPVCG, 
                                '' DDURACAO, 
                                '' KACTPCBB --valor vacio
                                FROM USVTIMG01."POLICY" P
                                LEFT JOIN USVTIMG01."CERTIFICAT" CERT 
                                ON CERT."SCERTYPE" = P."SCERTYPE" 
                                AND CERT."NBRANCH" = P."NBRANCH" 
                                AND CERT."NPRODUCT" = P."NPRODUCT" 
                                AND CERT."NPOLICY" = P."NPOLICY"
                                JOIN USVTIMG01."DISC_XPREM" DX        
                                ON DX."SCERTYPE" = P."SCERTYPE" 
                                AND   DX."NBRANCH" = P."NBRANCH" 
                                AND DX."NPRODUCT" = P."NPRODUCT" 
                                AND DX."NPOLICY" = P."NPOLICY" 
                                AND DX."NCERTIF" = CERT."NCERTIF" 
                                AND DX."DEFFECDATE" <= P."DSTARTDATE" 
                                AND (DX."DNULLDATE" IS NULL OR DX."DNULLDATE" > P."DSTARTDATE")
                                LEFT JOIN USVTIMG01."DISCO_EXPR" DE   
                                ON DX."NBRANCH" = DE."NBRANCH" 
                                AND DX."NPRODUCT" = DE."NPRODUCT" 
                                AND DX."NDISC_CODE" = DE."NDISEXPRC"
                                WHERE P."SCERTYPE" = '2' 
                                AND P."SSTATUS_POL" NOT IN ('2', '3')
                                AND (
                                      (P."SPOLITYPE" = '1' AND CAST(P."DCOMPDATE" AS DATE) BETWEEN '{p_fecha_inicio}' AND '{p_fecha_fin}')
                                      OR  
                                      (P."SPOLITYPE" <> '1' AND CAST(CERT."DCOMPDATE" AS DATE) BETWEEN '{p_fecha_inicio}' AND '{p_fecha_fin}')
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
                            /*
                            DE ACUERDO A JAOS EL DATO SE PUDIERA OBTENER EN BASE A LA PRIMERA COVERTURA PERO LOS SISTEMAS 
                            NO TIENEN ESA INFORMACION DE MANERA DIRECTA EN LA TABLA DE RECARGOS Y DESCUENTOS
                            (SELECT C."NCOVER" FROM USVTIMV01."COVER" C 
                              WHERE C."SCERTYPE" = '2' 
                              AND   C."NBRANCH" = DX."NBRANCH"  
                              AND   C."NPOLICY" = DX."NPOLICY"
                              AND   C."NCERTIF" = DX."NCERTIF" 
                              AND   C."NCOVER"  = 1
                              AND   C."DEFFECDATE" <= DX."DEFFECDATE" 
                              AND (C."DNULLDATE" IS NULL OR C."DNULLDATE" > DX."DEFFECDATE"))*/
                              '' KGCTPCBT, 
                              '' KACCDFDO, --VALOR VACIO
                              COALESCE(DE."SDISEXPRI" , '') KACTPCAG,
                              COALESCE(DX."NBRANCH", 0) || '-' || COALESCE(DX."NPRODUCT",0) || '-' || DX."NDISC_CODE" KACCDCAG, 
                              DX."NAMOUNT" VMTCARGA, 
                              COALESCE(CAST(CAST(DX."DEFFECDATE" AS DATE) AS VARCHAR), '') TULTMALT, 
                              '' DUSRUPD, --NO
                              'LPV' DCOMPA, 
                              '' DMARCA, --NO
                              '' DINCPRM, --VALOR VACIO
                              CASE WHEN (DX."NAMOUNT" != 0 AND DX."NAMOUNT" IS NOT NULL) 
                                    AND  (CAST(DX."NPERCENT" AS INTEGER) = 0 AND DX."NPERCENT" IS NULL) THEN  '1' --IMPORTE
                              ELSE '2' --PORCENTAJE
                              END KACTPVCG, 
                              '' DDURACAO, --VALOR VACIO
                              '' KACTPCBB --VALOR VACIO
                              FROM USVTIMV01."POLICY" P
                              LEFT JOIN USVTIMV01."CERTIFICAT" CERT 
                              ON CERT."SCERTYPE" = P."SCERTYPE" 
                              AND CERT."NBRANCH" = P."NBRANCH" 
                              AND CERT."NPRODUCT" = P."NPRODUCT" 
                              AND CERT."NPOLICY" = P."NPOLICY"
                              JOIN USVTIMV01."DISC_XPREM" DX 
                              ON DX."SCERTYPE" = P."SCERTYPE" 
                              AND DX."NBRANCH" = P."NBRANCH" 
                              AND DX."NPRODUCT" = P."NPRODUCT" 
                              AND DX."NPOLICY" = P."NPOLICY" 
                              AND DX."NCERTIF" = CERT."NCERTIF" 
                              AND DX."DEFFECDATE" <= P."DSTARTDATE" 
                              AND (DX."DNULLDATE" IS NULL OR DX."DNULLDATE" > P."DSTARTDATE")
                              LEFT JOIN USVTIMV01."DISCO_EXPR" DE 
                              ON DX."NBRANCH" = DE."NBRANCH" 
                              AND DX."NPRODUCT" = DE."NPRODUCT" 
                              AND DX."NDISC_CODE" = DE."NDISEXPRC"
                              WHERE P."SCERTYPE" = '2'  
                              AND P."SSTATUS_POL" NOT IN ('2', '3')
                              AND (
                                      (P."SPOLITYPE" = '1' AND CAST(P."DCOMPDATE" AS DATE) BETWEEN '{p_fecha_inicio}' AND '{p_fecha_fin}')
                                        OR  
                                      (P."SPOLITYPE" <> '1' AND CAST(CERT."DCOMPDATE" AS DATE) BETWEEN '{p_fecha_inicio}' AND '{p_fecha_fin}')
                                  )
                          ) AS TMP
                          '''
   
   l_df_polizas_vtime_vida = glue_context.read.format('jdbc').options(**connection).option("dbtable",l_polizas_vtime_vida).load() 
   
   print('USVTIMV01 EXITOSO')
   
   #----------------------------------------------------------------------------------------------------------------------------------#

   #DECLARAR CONSULTA INSUNIX
   l_polizas_insunix_general = f'''
                                (
                                  select
                                  'd' inddetrec,
                                  'abcarga' tablaifrs17,
                                  '' pk,--pendiente
                                  '' dtpreg,--no
                                  '' tiocproc,--no
                                  coalesce(cast(cast(dx.effecdate as date) as varchar),'') tiocfrm,--pendiente
                                  '' tiocto,--no
                                  'pig' kgiorigm,
                                  coalesce(p.branch, 0) || '-' || coalesce(p.product,0) || '-' || coalesce(psp.sub_product,0) || '-' || coalesce(p.policy,0) || '-' || coalesce(cert.certif,0) kabapol,--fk pendiente
                                  '' kabunris,  --valor vacio
                                  '' kgctpcbt,  --valor vacio
                                  '' kaccdfdo,  -- valor vacio
                                  coalesce(dx.type, '') kactpcag,
                                  coalesce(p.branch, 0) || '-' || coalesce(p.product,0) || '-' || coalesce(psp.sub_product,0) || '-' || dx.currency || '-' || coalesce(dx.code, 0) || '-' || coalesce(dx.type, '0') kaccdcag,
                                  coalesce(dx.amount, 0) vmtcarga,
                                  coalesce(cast(cast(dx.effecdate as date) as varchar),'') tultmalt,
                                  '' dusrupd,   --no
                                  'lpg' dcompa,
                                  '' dmarca,    --no
                                  '' dincprm,    --valor vacio
                                  case
                                  when (dx.amount != 0 and dx.amount is not null) and (cast(dx.percent as integer) = 0 and dx.percent is null) then '1' --importe
                                  else '2' --porcentaje
                                  end kactpvcg,
                                  '' dduracao, --valor vacio
                                  '' kactpcbb  --valor vacio
                                  from usinsug01.policy p 
                                left join usinsug01.certificat cert 
                                on cert.usercomp = p.usercomp 
                                and cert.company = p.company 
                                and cert.certype = p.certype 
                                and cert.branch = p.branch 
                                and cert.product = p.product 
                                and cert.policy = p.policy                                     
                                join usinsug01.pol_subproduct psp 
                                on  psp.usercomp = p.usercomp 
                                and psp.company  = p.company 
                                and psp.certype  = p.certype 
                                and psp.branch   = p.branch 
                                and psp.policy   = p.policy		    
                                and psp.product  = p.product
                                left join usinsug01.disc_xprem dx 
                                on dx.usercomp = p.usercomp 
                                and dx.company = p.company 
                                and dx.branch  = p.branch 
                                and dx.certype = p.certype 
                                and dx.policy  = p.policy
                                and dx.certif  = cert.certif
                                and (case 
                                        when p.politype  = '1' 
                                          then dx.effecdate <= p.effecdate and (dx.nulldate is null or dx.nulldate > p.effecdate)
                                        when p.politype <> '1' 
                                          then dx.effecdate <= cert.effecdate and (dx.nulldate is null or dx.nulldate > cert.effecdate)
                                    end)
                                where p.certype  = '2'
                                and p.status_pol not in ('2','3')  -- 2: que no esten invalidas  3: que no esten pendientes de informacion
                                and (
                                      (P.POLITYPE = '1' AND CAST(P.COMPDATE AS DATE) between '{p_fecha_inicio}' AND '{p_fecha_fin}') --individual
                                        OR 
                                        (P.POLITYPE <> '1' AND CAST(CERT.COMPDATE AS DATE) between '{p_fecha_inicio}' AND '{p_fecha_fin}') --colectiva
                                    )
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
                                COALESCE(p.BRANCH, 0) || '-' || COALESCE(p.PRODUCT, 0) || '-' || COALESCE(p.POLICY,0) || '-' || COALESCE(cert.CERTIF,0) KABAPOL, --FK
                                '' KABUNRIS, --VALOR VACIO
                                /*
                                DE ACUERDO A JAOS EL DATO SE PUDIERA OBTENER EN BASE A LA PRIMERA COVERTURA PERO LOS SISTEMAS 
                                NO TIENEN ESA INFORMACION DE MANERA DIRECTA EN LA TABLA DE RECARGOS Y DESCUENTOS
                                (
                                  SELECT C.COVER FROM USINSUV01.COVER C 
                                  WHERE C.CERTYPE = '2' 
                                  AND C.BRANCH = DX.BRANCH 
                                  AND C.POLICY = DX.POLICY
                                  AND C.CERTIF = DX.CERTIF 
                                  AND C.COVER = 1
                                  AND C.EFFECDATE <= DX.EFFECDATE 
                                  AND (C.NULLDATE IS NULL OR C.NULLDATE > DX.EFFECDATE) 
                                ),*/
                                '' KGCTPCBT, 
                                '' KACCDFDO, --VALOR VACIO
                                COALESCE(DX.TYPE, '') KACTPCAG, 
                                COALESCE(DX.BRANCH, 0) || '-' || COALESCE(p.PRODUCT, 0) || '-' || COALESCE(DX.CURRENCY, 0) || '-' || COALESCE (DX.CODE, 0) || '-' || COALESCE(DX.TYPE, '0') KACCDCAG, 
                                COALESCE (DX.AMOUNT, 0) VMTCARGA, 
                                COALESCE (CAST(CAST(DX.EFFECDATE AS DATE) AS VARCHAR), '') TULTMALT, 
                                '' DUSRUPD, --NO
                                'LPV' DCOMPA, 
                                '' DMARCA, --NO
                                '' DINCPRM, 
                                CASE WHEN (DX.AMOUNT != 0 AND DX.AMOUNT IS NOT NULL) AND (CAST(DX.PERCENT AS INTEGER)= 0 AND DX.PERCENT IS NULL) 
                                THEN '1' --IMPORTE 
                                ELSE '2' --PORCENTAJE
                                END KACTPVCG, 
                                '' DDURACAO, 
                                '' KACTPCBB --VALOR VACIO
                                FROM USINSUV01.POLICY P 
                                LEFT JOIN USINSUV01.CERTIFICAT CERT 
                                ON CERT.USERCOMP = P.USERCOMP 
                                AND CERT.COMPANY = P.COMPANY 
                                AND CERT.CERTYPE = P.CERTYPE 
                                AND CERT.BRANCH = P.BRANCH 
                                AND CERT.POLICY = P.POLICY  
                                AND CERT.PRODUCT = P.PRODUCT
                                LEFT JOIN USINSUV01.DISC_XPREM DX 
                                ON DX.USERCOMP = P.USERCOMP 
                                AND DX.COMPANY = P.COMPANY 
                                AND DX.CERTYPE = P.CERTYPE 
                                AND DX.BRANCH = P.BRANCH 
                                AND DX.POLICY = P.POLICY 
                                AND DX.CERTIF = CERT.CERTIF
                                and (case when P.POLITYPE = '1' 
                                      then DX.EFFECDATE <= P.EFFECDATE and (DX.NULLDATE IS NULL OR DX.NULLDATE > P.EFFECDATE )
                                      when P.POLITYPE <> '1' 
                                      then DX.EFFECDATE <= cert.EFFECDATE and (DX.NULLDATE IS NULL OR DX.NULLDATE > cert.EFFECDATE )
                                    end)
                                WHERE P.CERTYPE  = '2'
                                AND P.STATUS_POL NOT IN ('2','3')
                                and (
                                    (P.POLITYPE = '1' AND CAST(P.COMPDATE AS DATE) between '{p_fecha_inicio}' AND '{p_fecha_fin}') --individual
                                      OR 
                                      (P.POLITYPE <> '1' AND CAST(CERT.COMPDATE AS DATE) between '{p_fecha_inicio}' AND '{p_fecha_fin}') --colectiva
                                  )
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
                          CAST(P."REGISTRATION_DATE" AS DATE) BETWEEN '{p_fecha_inicio}' AND '{p_fecha_fin}'
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