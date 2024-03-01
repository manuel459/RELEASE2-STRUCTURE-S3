from pyspark.sql.types import DecimalType
from pyspark.sql.functions import col

def get_data(glue_context, connection, p_fecha_inicio, p_fecha_fin):
    
    l_abcosseg_insunix_g = f'''
                             (
                                   SELECT
                                    'D' AS INDDETREC,
                                    'ABCOSSEG' AS TABLAIFRS17,
                                    '' AS PK,
                                    '' AS DTPREG, --NO
                                    '' AS TIOCPROC, --NO
                                    COALESCE(CAST(C.EFFECDATE AS varchar), '') AS TIOCFRM,
                                    '' AS TIOCTO, --NO
                                    'PIG' AS KGIORIGM, --NO
                                    C.BRANCH || '-' ||  P.PRODUCT ||  '-' || PSP.SUB_PRODUCT ||  '-' ||  C.POLICY ||  '-' || CERT.CERTIF AS KABAPOL,
                                    'LPG' AS DCOMPA,
                                    '' AS DMARCA, --NO
                                    '' AS TDPLANO,--NO
                                    '' AS KACAREA, --NO
                                    case when coalesce(cast(c.companyc as varchar),'') in ('1','12') then '1'
                                    else '2' 
                                    end  AS KACTPCSG,
                                    COALESCE(CAST(C.COMPANYC AS VARCHAR), '') AS DCODCSG,
                                    COALESCE 
                                    (
                                    right ((
                                    SELECT (
                                          SELECT VT.SCOD_VT
                                          FROM USINSUG01.EQUI_VT_INX VT
                                          WHERE VT.SCOD_INX = COMP.CLIENT
                                    )
                                    FROM USINSUG01.COMPANY COMP
                                    WHERE COMP.CODE = C.COMPANYC
                                          ),13),
                                    ''
                                    ) AS DCREFERE,
                                    COALESCE(CAST(C.SHARE AS numeric(9,6)), '0') AS VTXQUOTA,
                                    '' AS VMTCAPIT,
                                    0 AS VTXCOMCB,
                                    0 AS VTXCOMMD,
                                    COALESCE(CAST(C.EXPENSIV AS numeric(10,7)), '0') AS VTXGESTAO,
                                    CASE 
                                    WHEN C.COMPANYC IN (1, 12) THEN 'S'
                                    ELSE 'N'
                                    END  DINDNSQ,
                                    '' AS DINDLID, --NO
                                    '' AS DNUMDIST, --NO
                                    '' AS KACTPDIS,
                                    '' AS TULTALT, --NO
                                    '' AS DUSRUPD --no
                                    FROM USINSUG01.POLICY P 
                                    LEFT JOIN USINSUG01.CERTIFICAT CERT 
                                    ON P.USERCOMP = CERT.USERCOMP 
                                    AND P.COMPANY = CERT.COMPANY 
                                    AND P.CERTYPE = CERT.CERTYPE 
                                    AND P.BRANCH  = CERT.BRANCH 
                                    AND P.POLICY  = CERT.policy
                                    JOIN USINSUG01.POL_SUBPRODUCT PSP
                                    ON  PSP.USERCOMP = P.USERCOMP
                                    AND PSP.COMPANY  = P.COMPANY
                                    AND PSP.CERTYPE  = P.CERTYPE
                                    AND PSP.BRANCH   = P.BRANCH		   
                                    AND PSP.PRODUCT  = P.PRODUCT
                                    AND PSP.POLICY   = P.policy
                                    JOIN USBI01."IFRS170_T_RAMOS_POR_TIPO_RIESGO" RTR 
                                    ON RTR."BRANCHCOM" = P.BRANCH 
                                    AND  RTR."RISKTYPEN" = 1 
                                    AND RTR."SOURCESCHEMA" = 'usinsug01'
                                    join USINSUG01.COINSURAN C
                                    ON  C.USERCOMP = P.USERCOMP 
                                    AND C.COMPANY  = P.COMPANY 
                                    AND C.CERTYPE  = P.CERTYPE
                                    AND C.BRANCH   = P.BRANCH 
                                    AND C.POLICY   = P.POLICY 
                                    AND C.EFFECDATE <= P.EFFECDATE 
                                    AND (C.NULLDATE IS NULL OR C.NULLDATE > P.EFFECDATE)
                                    WHERE P.CERTYPE = '2' 
                                    AND P.STATUS_POL NOT IN ('2','3')
                                    and CAST(c.compdate AS DATE) between '{p_fecha_inicio}' and '{p_fecha_fin}'
                             ) AS TMP
                             '''
    #EJECUTAR CONSULTA
    print("1-TERMINO TABLA ABCOSSEG_INX_G")
    l_df_abcosseg_insunix_g = glue_context.read.format('jdbc').options(**connection).option("dbtable",l_abcosseg_insunix_g).load()
    print("2-TERMINO TABLA ABCOSSEG_INX_G")

    l_abcosseg_insunix_v = f'''
                             (
                                   SELECT
                                    'D' AS INDDETREC,
                                    'ABCOSSEG' AS TABLAIFRS17,
                                    '' AS PK,
                                    '' AS DTPREG, --NO
                                    '' AS TIOCPROC, --NO
                                    COALESCE(CAST(C.EFFECDATE AS varchar), '') AS TIOCFRM,
                                    '' AS TIOCTO, --NO
                                    'PIV' AS KGIORIGM, --NO
                                    C.BRANCH || '-' ||  P.PRODUCT ||  '-' ||  P.POLICY ||  '-' || CERT.CERTIF AS KABAPOL,
                                    'LPV' AS DCOMPA,
                                    '' AS DMARCA, --NO
                                    '' AS TDPLANO,--NO
                                    '' AS KACAREA, --NO
                                    case when coalesce(cast(c.companyc as varchar),'') in ('1','12') then '1'
                                    else '2' 
                                    end  AS KACTPCSG,
                                    COALESCE(CAST(C.COMPANYC AS VARCHAR), '') AS DCODCSG,
                                    COALESCE 
                                    (
                                    right ((
                                    SELECT (
                                          SELECT VT.SCOD_VT
                                          FROM USINSUG01.EQUI_VT_INX VT
                                          WHERE VT.SCOD_INX = COMP.CLIENT
                                    )
                                    FROM USINSUG01.COMPANY COMP
                                    WHERE COMP.CODE = C.COMPANYC
                                          ),13),
                                    ''
                                    ) AS DCREFERE,
                                    COALESCE(CAST(C.SHARE AS numeric(9,6)), '0') AS VTXQUOTA,
                                    '' AS VMTCAPIT,
                                    0 AS VTXCOMCB,
                                    0 AS VTXCOMMD,
                                    COALESCE(CAST(C.EXPENSIV AS numeric(10,7)), '0') AS VTXGESTAO,
                                    CASE 
                                    WHEN C.COMPANYC IN (1, 12) THEN 'S'
                                    ELSE 'N'
                                    END  DINDNSQ,
                                    '' AS DINDLID, --NO
                                    '' AS DNUMDIST, --NO
                                    '' AS KACTPDIS,
                                    '' AS TULTALT, --NO
                                    '' AS DUSRUPD --NO
                                    FROM USINSUV01.POLICY P 
                                    LEFT JOIN USINSUV01.CERTIFICAT CERT 
                                    ON P.USERCOMP = CERT.USERCOMP 
                                    AND P.COMPANY = CERT.COMPANY 
                                    AND P.CERTYPE = CERT.CERTYPE 
                                    AND P.BRANCH  = CERT.BRANCH 
                                    AND P.POLICY  = CERT.policy	
                                    JOIN USBI01."IFRS170_T_RAMOS_POR_TIPO_RIESGO" RTR
                                    ON RTR."BRANCHCOM" = P.BRANCH 
                                    AND  RTR."RISKTYPEN" = 1 
                                    AND RTR."SOURCESCHEMA" = 'usinsuv01'
                                    join usinsuv01.COINSURAN C
                                    ON  C.USERCOMP = P.USERCOMP 
                                          AND C.COMPANY  = P.COMPANY 
                                          AND C.CERTYPE  = P.CERTYPE
                                          AND C.BRANCH   = P.BRANCH 
                                          AND C.POLICY   = P.POLICY 
                                          AND C.EFFECDATE <= P.EFFECDATE 
                                          AND (C.NULLDATE IS NULL OR C.NULLDATE > P.EFFECDATE) --1997-10-02	2020-11-02
                                    WHERE P.CERTYPE = '2'
                                    AND P.STATUS_POL NOT IN ('2','3')
                                    and CAST(C.COMPDATE AS DATE) between '{p_fecha_inicio}' and '{p_fecha_fin}'
                             ) AS TMP
                             '''
    
    #EJECUTAR CONSULTA
    print("1-TERMINO TABLA ABCOSSEG_INX_V")
    l_df_abcosseg_insunix_v = glue_context.read.format('jdbc').options(**connection).option("dbtable",l_abcosseg_insunix_v).load()
    print("2-TERMINO TABLA ABCOSSEG_INX_V")
    
    l_abcosseg_vtime_g = f'''
                            (
                                  SELECT 
                                    'D' AS INDDETREC,
                                    'ABCOSSEG' AS TABLAIFRS17,
                                    '' AS PK,
                                    '' AS DTPREG, --NO
                                    '' AS TIOCPROC, --NO
                                    COALESCE(CAST (cast(C."DEFFECDATE"  AS date)AS varchar) , '' ) AS TIOCFRM,
                                    '' AS TIOCTO, --NO
                                    'PVG' AS KGIORIGM, --NO	
                                    P."NBRANCH" ||'-'|| P."NPRODUCT" ||'-'|| P."NPOLICY" ||'-'|| CERT."NCERTIF" AS KABAPOL,
                                    'LPG' AS DCOMPA,
                                    '' AS DMARCA, --NO
                                    '' AS TDPLANO,--NO
                                    '' AS KACAREA, --NO
                                    case when coalesce(cast(C."NCOMPANY" as varchar),'') <> '1' then '2'
                                    else '1' 
                                    end  AS KACTPCSG,
                                    CAST( C."NCOMPANY"  AS VARCHAR) AS DCODCSG,
                                    COALESCE 
                                    (
                                          right((
                                          SELECT  COMP."SCLIENT"  
                                          FROM USVTIMG01."COMPANY"   COMP
                                          WHERE COMP."NCOMPANY" = C."NCOMPANY" 
                                          ),13),
                                          ''
                                    ) AS DCREFERE,
                                    COALESCE ( CAST ( C."NSHARE"  AS numeric(9,6)), '0') AS VTXQUOTA,
                                    '' AS VMTCAPIT,
                                    0 AS VTXCOMCB, 
                                    0 AS VTXCOMMD,
                                    COALESCE ( CAST (C."NEXPENSES" AS numeric(10,7)), '0') AS VTXGESTAO,
                                    CASE C."NCOMPANY"
                                    WHEN 1 THEN 'S' --CODIGO GENERALES
                                    ELSE 'N'
                                    END  DINDNSQ,
                                          '' AS DINDLID, --NO
                                          '' AS DNUMDIST, --NO
                                          '' AS KACTPDIS,
                                          '' AS TULTALT, --NO
                                          '' AS DUSRUPD --NO
                                    FROM USVTIMG01."POLICY" P 
                                    LEFT JOIN USVTIMG01."CERTIFICAT" CERT 
                                    ON  P."SCERTYPE" = CERT."SCERTYPE" 
                                    AND P."NBRANCH"  = CERT."NBRANCH"
                                    AND P."NPRODUCT" = CERT."NPRODUCT"
                                    AND P."NPOLICY"  = CERT."NPOLICY"
                                    JOIN USBI01."IFRS170_T_RAMOS_POR_TIPO_RIESGO" RTR 
                                    ON RTR."BRANCHCOM" = P."NBRANCH" 
                                    AND  RTR."RISKTYPEN" = 1 
                                    AND RTR."SOURCESCHEMA" = 'usvtimg01'
                                    join USVTIMG01."COINSURAN" C
                                    ON  C."SCERTYPE"  = P."SCERTYPE"
                                    AND C."NBRANCH"   = P."NBRANCH" 
                                    AND C."NPRODUCT"  = P."NPRODUCT"
                                    AND C."NPOLICY"   = P."NPOLICY"
                                    AND C."DEFFECDATE" <= P."DSTARTDATE" 
                                    AND (C."DNULLDATE" IS NULL OR C."DNULLDATE" > P."DSTARTDATE") --0029-09-20	2019-12-17
                                    WHERE P."SCERTYPE" = '2' 
                                    AND P."SSTATUS_POL" NOT IN ('2','3')
                                    and CAST(C."DCOMPDATE" AS DATE) between '{p_fecha_inicio}' and '{p_fecha_fin}'
                            ) AS TMP
                           '''
    #EJECUTAR CONSULTA
    print("1-TERMINO TABLA ABCOSSEG_VT")
    l_df_abcosseg_vtime_g = glue_context.read.format('jdbc').options(**connection).option("dbtable",l_abcosseg_vtime_g).load()
    print("2-TERMINO TABLA ABCOSSEG_VT")    

    l_abcosseg_vtime_v = f'''
                            (
                              SELECT 
                              'D' AS INDDETREC,
                              'ABCOSSEG' AS TABLAIFRS17,
                              '' AS PK,
                              '' AS DTPREG, --NO
                              '' AS TIOCPROC, --NO
                              COALESCE(CAST (cast(C."DEFFECDATE"  AS date)AS varchar) , '' ) AS TIOCFRM,
                              '' AS TIOCTO, --NO
                              'PVV' AS KGIORIGM, --NO	
                              P."NBRANCH" ||'-'|| P."NPRODUCT" ||'-'|| P."NPOLICY" ||'-'|| CERT."NCERTIF" AS KABAPOL,
                              'LPV' AS DCOMPA,
                              '' AS DMARCA, --NO
                              '' AS TDPLANO,--NO
                              '' AS KACAREA, --NO
                              case when coalesce(cast(C."NCOMPANY" as varchar),'') <> '2' then '2'
                              else '1' 
                              end  AS KACTPCSG,
                              CAST( C."NCOMPANY"  AS VARCHAR) AS DCODCSG,
                              COALESCE 
                              (
                                    right((
                                    SELECT  COMP."SCLIENT"  
                                    FROM USVTIMG01."COMPANY"   COMP
                                    WHERE COMP."NCOMPANY" = C."NCOMPANY" 
                                    ),13),
                                    ''
                              ) AS DCREFERE,
                              COALESCE ( CAST ( C."NSHARE"  AS numeric(9,6)), '0') AS VTXQUOTA,
                              '' AS VMTCAPIT,
                              0 AS VTXCOMCB, 
                              0 AS VTXCOMMD,
                              COALESCE ( CAST (C."NEXPENSES" AS numeric(10,7)), '0') AS VTXGESTAO,
                              CASE C."NCOMPANY"
                              WHEN 1 THEN 'S' --CODIGO GENERALES
                              ELSE 'N'
                              END  DINDNSQ,
                                    '' AS DINDLID, --NO
                                    '' AS DNUMDIST, --NO
                                    '' AS KACTPDIS,
                                    '' AS TULTALT, --NO
                                    '' AS DUSRUPD --NO
                              FROM USVTIMV01."POLICY" P 
                              LEFT JOIN USVTIMV01."CERTIFICAT" CERT 
                              ON  P."SCERTYPE" = CERT."SCERTYPE" 
                              AND P."NBRANCH"  = CERT."NBRANCH"
                              AND P."NPRODUCT" = CERT."NPRODUCT"
                              AND P."NPOLICY"  = CERT."NPOLICY"
                              JOIN USBI01."IFRS170_T_RAMOS_POR_TIPO_RIESGO" RTR 
                              ON RTR."BRANCHCOM" = P."NBRANCH" 
                              AND  RTR."RISKTYPEN" = 1 
                              AND RTR."SOURCESCHEMA" = 'usvtimv01'
                              join USVTIMV01."COINSURAN" C
                              ON  C."SCERTYPE"  = P."SCERTYPE"
                              AND C."NBRANCH"   = P."NBRANCH" 
                              AND C."NPRODUCT"  = P."NPRODUCT"
                              AND C."NPOLICY"   = P."NPOLICY"  
                              AND C."DEFFECDATE" <= P."DSTARTDATE"
                              WHERE P."SCERTYPE" = '2' 
                              AND P."SSTATUS_POL" NOT IN ('2','3')
                              and CAST(c."DCOMPDATE" AS DATE) between '{p_fecha_inicio}' and '{p_fecha_fin}'
                            ) AS TMP
                           '''
    
    #EJECUTAR CONSULTA
    print("1-TERMINO TABLA ABCOSSEG_VT")
    l_df_abcosseg_vtime_v = glue_context.read.format('jdbc').options(**connection).option("dbtable",l_abcosseg_vtime_v).load()
    print("2-TERMINO TABLA ABCOSSEG_VT")
    
    #PERFORM THE UNION OPERATION 
    l_df_abcosseg = l_df_abcosseg_insunix_g.union(l_df_abcosseg_insunix_v).union(l_df_abcosseg_vtime_g).union(l_df_abcosseg_vtime_v)
    
    l_df_abcosseg = l_df_abcosseg.withColumn("VTXQUOTA",col("VTXQUOTA").cast(DecimalType(9,6))).withColumn("VTXQUOTA",col("VTXQUOTA").cast(DecimalType(9,6))).withColumn("VTXCOMCB",col("VTXCOMCB").cast(DecimalType(7,4))).withColumn("VTXCOMMD",col("VTXCOMMD").cast(DecimalType(7,4))).withColumn("VTXGESTAO",col("VTXGESTAO").cast(DecimalType(10,7)))

    print("AQUI SE MANDE EL CONTEO")
    print(l_df_abcosseg.count())

    return l_df_abcosseg