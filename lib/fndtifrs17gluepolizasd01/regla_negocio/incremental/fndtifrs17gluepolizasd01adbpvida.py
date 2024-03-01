def get_data(glue_context, connection, p_fecha_inicio, p_fecha_fin):

    l_abdpvida_insunix_life = f'''
                                  (
                                    SELECT 
                                    'D' AS INDDETREC,
                                    'ABDPVIDA' AS TABLAIFRS17, 
                                    COALESCE(P.BRANCH, 0) || '-' || COALESCE(P.PRODUCT, 0) || '-' || COALESCE(P.POLICY, 0) || '-' || COALESCE(CERT.CERTIF, 0) AS KABAPOL,
                                    '' AS DTPREG,
                                    '' AS TIOCPROC,
                                    CAST(L.EFFECDATE AS VARCHAR) AS TIOCFRM,
                                    '' AS TIOCTO,
                                    'PIV' AS KGIORIGM,
                                    '' AS DNUMPROP,
                                    '' AS TINIPROP,
                                    (SELECT  COUNT(DISTINCT R.CLIENT) FROM USINSUV01.ROLES R
                                      WHERE R.USERCOMP = 1
                                      AND R.COMPANY    = 1
                                      AND R.CERTYPE    = '2'
                                      AND R.BRANCH     = L.BRANCH
                                      AND R.POLICY     = L.POLICY
                                      AND R.EFFECDATE <= L.EFFECDATE
                                      AND (R.NULLDATE IS NULL OR R.NULLDATE > L.EFFECDATE)) AS DNPESSEG,
                                    COALESCE(L.CAPITAL,0)  AS VMTCAPVD,
                                    0 AS VMTCAPMT,
                                    COALESCE(L.PREMIUM ,0) AS VMTPRCBP,
                                    '' AS DOBSERV,
                                    '' AS DDIAPAG,
                                    '' AS DTABCOM,
                                    '' AS DREGCAL,
                                    '' AS DDURAPO,
                                    '' AS KACTPPMA,
                                    'LPV' AS DCOMPA,
                                    '' AS DMARCA,
                                    '' AS DPLPGVID,
                                    '' AS DPLPGMOR,
                                    '' AS KACFMDEC,
                                    '' AS KACTPCRE,
                                    '' AS KACMOCRE,
                                    0 AS KACTPREN,
                                    '' AS KACTPCRR,
                                    '' AS KACTPGRE,
                                    '' AS VTXCRPR,
                                    '' AS VMTCRPR,
                                    '' AS TINIPGRE,
                                    '' AS TFIMPGRE,
                                    '' AS VTXREVER,
                                    '' AS VTXCRRE,
                                    '' AS VTXSBPRE,
                                    '' AS VTXSLPAR,
                                    '' AS VTXAUCAPS,
                                    '' AS TEMISREN,
                                    '' AS KACINREIN,
                                    '' AS VMTRENAN,
                                    '' AS KACREINTE,
                                    0 AS KACCLREN,
                                    '' AS TFIMDIFE,
                                    '' as TVRENPAG,
                                    '' as KACPERISC,
                                    '' as DDURAPOMES,
                                    '' as KACPZREN,
                                    '' as KACTPDEF,
                                    '' as DPRAZODEF,
                                    '' as KACUNIDEF,
                                    0 as DDURRENDA,
                                    '' as VTXINDX,
                                    '' as DMESPST13,
                                    '' as DMESPST14,
                                    '' as KACPROVID,
                                    '' as DMESPROVID,
                                    '' as KACPRVIDSC,
                                    '' as KACPRVIDSN,
                                    '' as KACESTREN,
                                    '' as TDTESTREN,
                                    '' as KACTPGRE_FR,
                                    '' as VMTCAPANREN,
                                    '' as DCDTRAT_SO,
                                    '' as VTXRSSREN,
                                    '' as VMTPLENO,
                                    '' as VMTCAPRSS
                                  FROM usinsuv01.POLICY p
                                  join usinsuv01.CERTIFICAT CERT
                                  ON P.USERCOMP = CERT.USERCOMP
                                  AND P.COMPANY = CERT.COMPANY
                                  AND P.CERTYPE = CERT.CERTYPE 
                                  AND P.BRANCH  = CERT.BRANCH 
                                  AND P.POLICY  = CERT.policy
                                  JOIN USBI01."IFRS170_T_RAMOS_POR_TIPO_RIESGO" RTR
                                  ON  RTR."BRANCHCOM" = P.BRANCH 
                                  AND RTR."RISKTYPEN" = 1 
                                  AND RTR."SOURCESCHEMA" = 'usinsuv01'
                                  join USINSUV01.LIFE L
                                  on L.USERCOMP  = p .USERCOMP
                                  AND L.COMPANY   = P.COMPANY
                                  AND L.CERTYPE   = P.CERTYPE 
                                  AND L.BRANCH    = P.BRANCH
                                  AND L.POLICY  = P.POLICY 
                                  AND L.CERTIF    = cert.CERTIF
                                  WHERE P.CERTYPE = '2'
                                  AND P.STATUS_POL NOT IN ('2','3') 
                                  AND CAST(L.compdate AS DATE) between '{p_fecha_inicio}' and '{p_fecha_fin}'
                                  ) as TMP       
                               '''

    #EJECUTAR CONSULTA
    print("1-TERMINO TABLA ABDPVIDA_IN_LIFE")
    l_df_abdpvida_insunix_life = glue_context.read.format('jdbc').options(**connection).option("dbtable",l_abdpvida_insunix_life).load()
    print("2-TERMINO TABLA ABDPVIDA_IN_LIFE")
    
    l_abdpvida_insunix_life_prev = f'''
                                    (
                                      SELECT
                                      'D' as INDDETREC,
                                      'ABDPVIDA' as TABLAIFRS17, 
                                      coalesce(p.branch, 0) || '-' || coalesce(p.product, 0) || '-' || coalesce(p.policy, 0) || '-' || coalesce(cert.certif,0) as KABAPOL,
                                      '' as DTPREG,
                                      '' as TIOCPROC,
                                      cast(lp.effecdate as varchar) as TIOCFRM,
                                      '' as TIOCTO,
                                      'PIV' as KGIORIGM,
                                      '' as DNUMPROP,
                                      '' as TINIPROP,
                                      (select  count(distinct r.client) from usinsuv01.roles r
                                        where r.usercomp = 1
                                          and r.company = 1
                                          and r.certype = '2'
                                          and r.branch = lp.branch
                                          and r.policy = lp.policy
                                          and r.effecdate <= lp.effecdate
                                          and (r.nulldate is null or r.nulldate > lp.effecdate)
                                      ) AS DNPESSEG,
                                      coalesce(lp.capital,0)  as VMTCAPVD,
                                      0 as VMTCAPMT,
                                      coalesce(lp.premium ,0) as VMTPRCBP,
                                      '' as DOBSERV,
                                      '' as DDIAPAG,
                                      '' as DTABCOM,
                                      '' as DREGCAL,
                                      '' as DDURAPO,
                                      '' as KACTPPMA,
                                      'LPV' as DCOMPA,
                                      '' as DMARCA,
                                      '' as DPLPGVID,
                                      '' as DPLPGMOR,
                                      '' as KACFMDEC,
                                      '' as KACTPCRE,
                                      '' as KACMOCRE,
                                      coalesce(lp.rent_type,0) as KACTPREN,
                                      '' as KACTPCRR,
                                      '' as KACTPGRE,
                                      '' as VTXCRPR,
                                      '' as VMTCRPR,
                                      coalesce(cast(lp.pay_first as varchar),'')  as TINIPGRE,
                                      '' as TFIMPGRE,
                                      '' as VTXREVER,
                                      '' as VTXCRRE,
                                      '' as VTXSBPRE,
                                      '' as VTXSLPAR,
                                      '' as VTXAUCAPS,
                                      '' as TEMISREN,
                                      '' as KACINREIN,
                                      '' as VMTRENAN,
                                      '' as KACREINTE,
                                      coalesce(lp.rent_type,0)  as KACCLREN,
                                      COALESCE(CAST((lp.startdate + make_interval(years => lp.time_difer)) AS VARCHAR),CAST(lp.startdate AS VARCHAR)) AS TFIMDIFE,
                                      '' as TVRENPAG,
                                      '' as KACPERISC,
                                      '' as DDURAPOMES,
                                      '' as KACPZREN,
                                      '' as KACTPDEF,
                                      '' as DPRAZODEF,
                                      '' as KACUNIDEF,
                                      (coalesce(lp.time_difer ,0) + coalesce(lp.time_garant,0))  AS DDURRENDA,
                                      '' as VTXINDX,
                                      '' as DMESPST13,
                                      '' as DMESPST14,
                                      '' as KACPROVID,
                                      '' as DMESPROVID,
                                      '' as KACPRVIDSC,
                                      '' as KACPRVIDSN,
                                      '' as KACESTREN,
                                      '' as TDTESTREN,
                                      '' as KACTPGRE_FR,
                                      '' as VMTCAPANREN,
                                      '' as DCDTRAT_SO,
                                      '' as VTXRSSREN,
                                      '' as VMTPLENO,
                                      '' as VMTCAPRSS
                                      from usinsuv01.policy p
                                      join usinsuv01.certificat cert
                                      on p.usercomp = cert .usercomp
                                      and p.company = cert.company
                                      and p.certype = cert.certype
                                      and p.branch = cert .branch
                                      and p.policy = cert .policy
                                      JOIN USBI01."IFRS170_T_RAMOS_POR_TIPO_RIESGO" RTR 
                                      ON  RTR."BRANCHCOM" = P.BRANCH 
                                      AND RTR."RISKTYPEN" = 1 
                                      AND RTR."SOURCESCHEMA" = 'usinsuv01'
                                      join usinsuv01.life_prev lp
                                      on lp.usercomp = p.usercomp
                                      and lp.company = p.company
                                      and lp.certype = p.certype
                                      and lp.branch = p.branch
                                      and lp.policy = p.policy
                                      WHERE P.CERTYPE = '2'
                                      AND P.STATUS_POL NOT IN ('2','3') 
                                      AND CAST(lp.compdate AS DATE) between '{p_fecha_inicio}' and '{p_fecha_fin}'
                                    ) AS TMP
                                    '''
    #EJECUTAR CONSULTA
    print("1-TERMINO TABLA ABDPVIDA_LIFE_PREV")
    l_df_abdpvida_insunix_life_prev = glue_context.read.format('jdbc').options(**connection).option("dbtable",l_abdpvida_insunix_life_prev).load()
    print("2-TERMINO TABLA ABDPVIDA_LIFE_PREV")

    l_abdpvida_vtime_life = f'''
                            (
                              SELECT 
                              'D' AS INDDETREC,
                              'ABDPVIDA' AS TABLAIFRS17, 
                              p."NBRANCH" || '-' || p."NPRODUCT" || '-' || p."NPOLICY" || '-' || cert."NCERTIF" AS KABAPOL,
                              '' AS DTPREG,
                              '' AS TIOCPROC,
                              CAST(CAST(L."DEFFECDATE" AS DATE)AS VARCHAR) AS TIOCFRM,
                              '' AS TIOCTO,
                              'PVV' AS KGIORIGM,
                              '' AS DNUMPROP,
                              '' AS TINIPROP,
                              L."NCOUNT_INSU" AS DNPESSEG,
                              COALESCE(L."NCAPITAL",0)  AS VMTCAPVD,
                              0 AS VMTCAPMT,
                              COALESCE(L."NPREMIUM",0) AS VMTPRCBP,
                              '' AS DOBSERV,
                              '' AS DDIAPAG,
                              '' AS DTABCOM,
                              '' AS DREGCAL,
                              '' AS DDURAPO,
                              '' AS KACTPPMA,
                              'LPV' AS DCOMPA,
                              '' AS DMARCA,
                              '' AS DPLPGVID,
                              '' AS DPLPGMOR,
                              '' AS KACFMDEC,
                              '' AS KACTPCRE,
                              '' AS KACMOCRE,
                              0 AS KACTPREN,
                              '' AS KACTPCRR,
                              '' AS KACTPGRE,
                              '' AS VTXCRPR,
                              '' AS VMTCRPR,
                              '' AS TINIPGRE,
                              '' AS TFIMPGRE,
                              '' AS VTXREVER,
                              '' AS VTXCRRE,
                              '' AS VTXSBPRE,
                              '' AS VTXSLPAR,
                              '' AS VTXAUCAPS,
                              '' AS TEMISREN,
                              '' AS KACINREIN,
                              '' AS VMTRENAN,
                              '' AS KACREINTE,
                              0 AS KACCLREN,
                              '' AS TFIMDIFE,
                              '' AS TVRENPAG,
                              '' AS KACPERISC,
                              '' AS DDURAPOMES,
                              '' AS KACPZREN,
                              '' AS KACTPDEF,
                              '' AS DPRAZODEF,
                              '' AS KACUNIDEF,
                              0 AS DDURRENDA,
                              '' AS VTXINDX,
                              '' AS DMESPST13,
                              '' AS DMESPST14,
                              '' AS KACPROVID,
                              '' AS DMESPROVID,
                              '' AS KACPRVIDSC,
                              '' AS KACPRVIDSN,
                              '' AS KACESTREN,
                              '' AS TDTESTREN,
                              '' AS KACTPGRE_FR,
                              '' AS VMTCAPANREN,
                              '' AS DCDTRAT_SO,
                              '' AS VTXRSSREN,
                              '' AS VMTPLENO,
                              '' AS VMTCAPRSS
                              from USVTIMV01."POLICY" p
                              left join USVTIMV01."CERTIFICAT" cert
                              on p."SCERTYPE" = cert ."SCERTYPE"
                              and p."NBRANCH" = cert ."NBRANCH"
                              and p."NPRODUCT" = cert ."NPRODUCT"
                              and p."NPOLICY" = cert ."NPOLICY"
                              JOIN USBI01."IFRS170_T_RAMOS_POR_TIPO_RIESGO" RTR 
                              ON RTR."BRANCHCOM" = P."NBRANCH" 
                              AND  RTR."RISKTYPEN" = 1 
                              AND RTR."SOURCESCHEMA" = 'usvtimv01'
                              join USVTIMV01."LIFE" L
                              ON  L."SCERTYPE"  = p."SCERTYPE"
                              AND L."NBRANCH"   = p."NBRANCH" 
                              AND L."NPRODUCT"  = p."NPRODUCT"
                              AND L."NPOLICY"   = p."NPOLICY" 
                              AND L."NCERTIF"   = cert."NCERTIF"  
                              AND L."DEFFECDATE" <= P."DSTARTDATE" 
                              AND (L."DNULLDATE" IS NULL OR L."DNULLDATE" > P."DSTARTDATE")
                              WHERE P."SCERTYPE" = '2' 
                              AND CAST(L."DCOMPDATE" AS DATE) between '{p_fecha_inicio}' and '{p_fecha_fin}'
                            ) AS TMP
                             '''
    #EJECUTAR CONSULTA
    print("1-TERMINO TABLA ABDPVIDA_INS")
    l_df_abdpvida_vtime_life = glue_context.read.format('jdbc').options(**connection).option("dbtable",l_abdpvida_vtime_life).load()
    print("2-TERMINO TABLA ABDPVIDA_INS")
    
    #PERFORM THE UNION OPERATION
    l_df_abdpvida = l_df_abdpvida_insunix_life.union(l_df_abdpvida_insunix_life_prev).union(l_df_abdpvida_vtime_life)

    return l_df_abdpvida

