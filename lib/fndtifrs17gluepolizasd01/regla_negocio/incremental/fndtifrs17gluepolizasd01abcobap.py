from pyspark.sql.types import DecimalType
from pyspark.sql.functions import col, format_number

def get_data(glue_context, connection, p_fecha_inicio, p_fecha_fin):

  l_fecha_carga_inicial = '2021-12-31'

  l_abcobap_insunix_lpg = f'''
                          ( select 
                            'd' as inddetrec,
                            'abcobap' as tablaifrs17,
                            '' as pk,
                            '' as dtpreg,
                            '' as tiocproc,
                            tiocfrm,
                            '' as tiocto,
                            'pig' as kgiorigm,
                            kabapol,
                            '' as kabunris,
                            kgctpcbt,
                            tinicio,
                            ttermo,
                            '' as tsitcob,
                            '' as kacsitcb,
                            '' as vmtprmsp,
                            vmtcomr,
                            '' as vmtbomat,
                            '' as vtxbomat,
                            '' as vmtbocom,
                            '' as vtxbocom,
                            '' as vmtdecom,
                            '' as vtxdecom,
                            '' as vmtdetec,
                            '' as vtxdetec,
                            '' as vmtagrav,
                            '' as vtxagrav,
                            '' as vmtprmtr,
                            '' as vmtprliq,
                            vmtprmbr,
                            vtxcob,
                            vcapital, 
                            '' as vtxcapit,
                            '' as kactpidx,
                            '' as vtxindx,
                            'lpg' as dcompa,
                            '' as dmarca,
                            '' as tdacecob,
                            '' as tdcancob,
                            '' as tdcricob,
                            tdrenova,
                            '' as tdventra,
                            '' as dhoraini,
                            vmtpremc,
                            '' as vmibomat,
                            '' as vmibocom,
                            '' as vmidecom,
                            '' as vmidetec,
                            '' as vmirpmsp,
                            '' as vmiprmbr,
                            '' as vmicomr,
                            '' as vmiprliq,
                            '' as vmicmnqp,
                            '' as vmiprmtr,
                            '' as vmiagrav,
                            '' as kactipcb,
                            '' as vmtcapli,
                            '' as kactrare,
                            '' as kacfmcal,
                            '' as dfacmult,
                            vmtcapin,                     
                            vmtprein,
                            '' as dindeses,
                            '' as dindmoto,
                            '' as kacsalin,
                            '' as vmtsalmd,
                            '' as vtxlmres,
                            '' as vtxequip,
                            '' as vtxprior,
                            '' as vtxcontr,
                            '' as vtxespec,
                            '' as dcapmort,
                            vmtprres,
                            '' as didadetar,
                            '' as didadlimcoba,
                            '' as kactpdur,
                            '' as kgcramo_sap,
                            '' as kactcomp,
                            '' as kacindtx,
                            '' as kaccalida,
                            '' as dncabcalp,
                            '' as dindnivel,
                            '' as durcob,
                            '' as durpagcob,
                            '' as kactpdurcb,
                            '' as dincobindx,
                            '' as kacgrcbt,
                            '' as kabtrtab_2,
                            '' as vtxajtbua,
                            '' as vmtcaprem
                            --,modulo    
                            from( select 
                                  coalesce (cast(c.c_effecdate as varchar),'')  as tiocfrm,
                                  cast(coalesce(c.branch, 0) as varchar) ||'-'|| c.product || '-' || coalesce(c.sub_product, 0) || '-' || coalesce(c.policy, 0) || '-' || coalesce(c.certif, 0) as kabapol,
                                  coalesce(( select cast(coalesce(gc.covergen, 0) as varchar) || '-' || coalesce(gc.currency, 0) from usinsug01.gen_cover gc 
                                              where gc.usercomp = c.usercomp 
                                              and gc.company    = c.company 
                                              and gc.branch     = c.branch 
                                              and gc.product    = c.product
                                              and gc.currency   = c.currency
                                              and gc.modulec    = c.modulec
                                              and gc.cover      = c.cover 
                                              and gc.effecdate <= c.effecdate
                                              and (gc.nulldate is null or gc.nulldate > c.effecdate ) limit 1) ,'0') as kgctpcbt,
                                  coalesce(cast(c.effecdate as varchar),'')  as tinicio,
                                  coalesce(cast(c.nulldate  as varchar),'') as ttermo,
                                  coalesce(c.premium, 0) as vmtcomr,
                                  coalesce(c.premium,0)  as vmtprmbr,
                                  coalesce(c.ratecove, 0) as vtxcob,
                                  coalesce(cast(c.capital as varchar),'0') as vcapital,
                                  coalesce(cast (c.effecdate as varchar),'') as tdrenova,
                                  coalesce(cast(((select coalesce(co.share, 0)
                                                  from usinsug01.coinsuran co
                                                  where co.usercomp = c.usercomp 
                                                  and co.company    = c.company 
                                                  and co.certype    = c.certype
                                                  and co.branch     = c.branch 
                                                  and co.policy     = c.policy
                                                  and co.companyc   = 1
                                                  and co.effecdate <= c.effecdate
                                                  and (co.nulldate is null or co.nulldate > c.effecdate)) * c.premium) as varchar), '100') as vmtpremc,
                                  coalesce(c.capitali, 0) as vmtcapin,                       
                                    coalesce((select cov.premium from usinsug01.cover cov 
                                              left join usinsug01.certificat cert                           
                                              on cov.usercomp  =  cert.usercomp 
                                              and cov.company  = cert.company  
                                              and cov.certype  = cert.certype 
                                              and cov.branch   = cert.branch 
                                              and cov.policy   = cert.policy
                                              and cov.certif   = cert.certif
                                              and cov.currency = c.currency 
                                              and cov.cover    = c.cover
                                              and cov.modulec  = c.modulec
                                              and cov.effecdate <= c.c_date_origi
                                              and (cov.nulldate is null or cov.nulldate > c.c_date_origi) limit 1), (select cov.premium from usinsug01.cover cov 
                                                                                                                     left join usinsug01.certificat cert                           
                                                                                                                     on  cov.usercomp = cert.usercomp 
                                                                                                                     and cov.company  = cert.company  
                                                                                                                     and cov.certype  = cert.certype 
                                                                                                                     and cov.branch   = cert.branch 
                                                                                                                     and cov.policy   = cert.policy
                                                                                                                     and cov.certif   = cert.certif
                                                                                                                     and cov.currency = c.currency 
                                                                                                                     and cov.cover    = c.cover
                                                                                                                     and cov.modulec  = c.modulec
                                                                                                                     left join usinsug01.policy pol
                                                                                                                     on  pol.usercomp = cert.usercomp 
                                                                                                                     and pol.company  = cert.company  
                                                                                                                     and pol.certype  = cert.certype
                                                                                                                     and pol.branch   = cert.branch 
                                                                                                                     and pol.policy   = cert.policy                                    
                                                                                                                     and cov.effecdate <= c.p_date_origi
                                                                                                                     and (cov.nulldate is null or cov.nulldate > c.p_date_origi) limit 1)) as vmtprein,
                                  coalesce((coalesce ((select (sum(r.share/100)) * c.premium 
                                                        from usinsug01.reinsuran r 
                                                        where r.usercomp  = c.usercomp 
                                                        and   r.company   = c.company 
                                                        and   r.certype   = c.certype  
                                                        and   r.branch    = c.branch
                                                        and   r.policy    = c.policy
                                                        and   r.certif    = c.certif 
                                                        and   r.effecdate <= c.effecdate
                                                        and   (r.nulldate is null or r.nulldate > c.effecdate)
                                                        and r.type <> 1),
                                                        (select (sum(r.share/100)) * c.premium  from usinsug01.reinsuran r 
                                                        where r.usercomp = c.usercomp 
                                                        and r.company = c.company 
                                                        and r.certype = c.certype  
                                                        and r.branch = c.branch
                                                        and r.policy = c.policy
                                                        and r.certif = 0
                                                        and r.effecdate <= c.effecdate
                                                        and (r.nulldate is null or r.nulldate > c.effecdate)
                                                        and r.type <> 1))), 0) as vmtprres
                                --,c.modulec as modulo 
                                from
                                (   select                                                                 
                                    case
                                    when p.politype = '1'  then p.effecdate 
                                    when p.politype <> '1' then c.effecdate 
                                    end effecdate,                                     
                                    c.effecdate as c_effecdate,                               
                                    p.politype, 
                                    p.date_origi as p_date_origi,
                                    cert.date_origi as c_date_origi,
                                    c.usercomp, 
                                    c.company,                                  
                                    c.branch, 
                                    p.product, 
                                    psp.sub_product,
                                    c.policy, 
                                    c.nulldate, 
                                    c.premium, 
                                    c.ratecove, 
                                    c.capital, 
                                    c.currency, 
                                    c.cover, 
                                    c.modulec, 
                                    c.capitali,
                                    c.certype,
                                    cert.certif
                                    from usinsug01.cover c   
                                    left join usinsug01.certificat cert 
                                      on c.usercomp = cert.usercomp  
                                      and c.company = cert.company  
                                      and c.certype = cert.certype 
                                      and c.branch  = cert.branch 
                                      and c.policy  = cert.policy 
                                      and c.certif  = cert.certif
                                    join usinsug01.policy p
                                      on  p.usercomp = c.usercomp 
                                      and p.company  = c.company 
                                      and p.certype  = c.certype 
                                      and p.branch   = c.branch  
                                      and p.policy   = c.policy
                                    join usinsug01.pol_subproduct psp 
                                      on  psp.usercomp = p.usercomp 
                                      and psp.company  = p.company 
                                      and psp.certype  = p.certype 
                                      and psp.branch   = p.branch 
                                      and psp.product  = p.product 
                                      and psp.policy   = p.policy 
                                    where c.certype    = '2' and p.status_pol not in ('2','3') 
                                    and c.compdate between '{p_fecha_inicio}' and '{p_fecha_fin}') c                            
                                ) cov                                                              
                          ) as tmp
                          '''

  l_df_abcobap_insunix_lpg = glue_context.read.format('jdbc').options(**connection).option("dbtable", l_abcobap_insunix_lpg).load()

  print("ABCOBAP INSUNIX LPG EXITOSO")

  l_abcobap_insunix_lpv = f'''
                          (SELECT 
                          'D' AS INDDETREC,
                          'ABCOBAP' AS TABLAIFRS17,
                          '' AS PK,
                          '' AS DTPREG,
                          '' AS TIOCPROC,
                          TIOCFRM,
                          '' AS TIOCTO,
                          'PIV' AS KGIORIGM,
                          KABAPOL,
                          '' AS KABUNRIS,
                          KGCTPCBT,
                          TINICIO,
                          TTERMO,
                          '' AS TSITCOB,
                          '' AS KACSITCB,
                          '' AS VMTPRMSP,
                          VMTCOMR,
                          '' AS VMTBOMAT,
                          '' AS VTXBOMAT,
                          '' AS VMTBOCOM,
                          '' AS VTXBOCOM,
                          '' AS VMTDECOM,
                          '' AS VTXDECOM,
                          '' AS VMTDETEC,
                          '' AS VTXDETEC,
                          '' AS VMTAGRAV,
                          '' AS VTXAGRAV,
                          '' AS VMTPRMTR,
                          '' AS VMTPRLIQ,
                          VMTPRMBR,
                          VTXCOB,
                          VCAPITAL,
                          '' AS VTXCAPIT,
                          '' AS KACTPIDX,
                          '' AS VTXINDX,
                          'LPV' AS DCOMPA,
                          '' AS DMARCA,
                          '' AS TDACECOB,
                          '' AS TDCANCOB,
                          '' AS TDCRICOB,
                          TDRENOVA,
                          '' AS TDVENTRA,
                          '' AS DHORAINI,
                          VMTPREMC,
                          '' AS VMIBOMAT,
                          '' AS VMIBOCOM,
                          '' AS VMIDECOM,
                          '' AS VMIDETEC,
                          '' AS VMIRPMSP,
                          '' AS VMIPRMBR,
                          '' AS VMICOMR,
                          '' AS VMIPRLIQ,
                          '' AS VMICMNQP,
                          '' AS VMIPRMTR,
                          '' AS VMIAGRAV,
                          '' AS KACTIPCB,
                          '' AS VMTCAPLI,
                          '' AS KACTRARE,
                          '' AS KACFMCAL,
                          '' AS DFACMULT,
                          VMTCAPIN,
                          VMTPREIN,
                          '' AS DINDESES,
                          '' AS DINDMOTO,
                          '' AS KACSALIN,
                          '' AS VMTSALMD,
                          '' AS VTXLMRES,
                          '' AS VTXEQUIP,
                          '' AS VTXPRIOR,
                          '' AS VTXCONTR,
                          '' AS VTXESPEC,
                          '' AS DCAPMORT,
                          VMTPRRES,
                          '' AS DIDADETAR,
                          '' AS DIDADLIMCOBA,
                          '' AS KACTPDUR,
                          '' AS KGCRAMO_SAP,
                          '' AS KACTCOMP,
                          '' AS KACINDTX,
                          '' AS KACCALIDA,
                          '' AS DNCABCALP,
                          '' AS DINDNIVEL,
                          '' AS DURCOB,
                          '' AS DURPAGCOB,
                          '' AS KACTPDURCB,
                          '' AS DINCOBINDX,
                          '' AS KACGRCBT,
                          '' AS KABTRTAB_2,
                          '' AS VTXAJTBUA,
                          '' AS VMTCAPREM
                          --,MODULEC
                          FROM(
                                  SELECT 
                                  COALESCE (CAST(C.EFFECDATE AS VARCHAR),'')  AS TIOCFRM,
                                  COALESCE(C.BRANCH, 0) || '-'|| C.PRODUCT || '-' ||  COALESCE(C.POLICY, 0)|| '-' || COALESCE(C.CERTIF, 0)  AS KABAPOL,
                                  '' AS KABUNRIS,
                                  COALESCE((SELECT COALESCE(CAST(GLC.COVERGEN AS VARCHAR), '0') || '-' || COALESCE(GLC.CURRENCY, 0)  FROM 
                                          (SELECT GC.USERCOMP,
                          				GC.COMPANY,
                          				GC.BRANCH,
                          				GC.PRODUCT,
                          				GC.CURRENCY,
                          				GC.MODULEC,
                          				GC.COVER,
                          				GC.EFFECDATE,
                          				GC.NULLDATE,
                          				GC.COVERGEN
                          				FROM USINSUV01.GEN_COVER GC
                          				UNION 
                          				SELECT LC.USERCOMP,
                          				LC.COMPANY,
                          				LC.BRANCH,
                          				LC.PRODUCT,
                          				LC.CURRENCY,
                          				0 AS MODULEC,
                          				LC.COVER,
                          				LC.EFFECDATE,
                          				LC.NULLDATE,
                          				LC.COVERGEN
                          				FROM USINSUV01.LIFE_COVER LC) GLC 
                                          WHERE GLC.USERCOMP = C.USERCOMP 
                                          AND GLC.COMPANY = C.COMPANY 
                                          AND GLC.BRANCH = C.BRANCH 
                                          AND GLC.PRODUCT = C.PRODUCT 
                                          AND GLC.CURRENCY = C.CURRENCY
                                          AND GLC.MODULEC = C.MODULEC
                                          AND GLC.COVER = C.COVER 
                                          AND GLC.EFFECDATE <= (CASE WHEN C.POLITYPE = '1' THEN C.EFFECDATE_POL ELSE C.EFFECDATE_CERT END)
                                          AND (GLC.NULLDATE IS NULL OR GLC.NULLDATE > (CASE WHEN C.POLITYPE = '1' THEN C.EFFECDATE_POL ELSE C.EFFECDATE_CERT END)) LIMIT 1), '0') AS KGCTPCBT,
                                  COALESCE (CAST(C.EFFECDATE AS VARCHAR),'')  AS TINICIO,
                                  COALESCE (CAST(C.NULLDATE AS VARCHAR),'') AS TTERMO,
                                  COALESCE(C.PREMIUM, 0) AS VMTCOMR,
                                  COALESCE(C.PREMIUM,  0) AS VMTPRMBR,
                                  COALESCE(C.RATECOVE, 0) AS VTXCOB, --TASA APLICAR A LA COBERTURA
                                  COALESCE(CAST(C.CAPITAL AS VARCHAR), '0') AS VCAPITAL,-- IMPORTE DE CAPITAL ASEGURADO EN EL CERTIFICADO                         
                                  'LPV' AS DCOMPA,
                                  '' AS DMARCA,
                                  '' AS TDACECOB,
                                  '' AS TDCANCOB,
                                  '' AS TDCRICOB,
                                  COALESCE(CAST (C.EFFECDATE AS VARCHAR),'') AS TDRENOVA,
                                  '' AS TDVENTRA,
                                  '' AS DHORAINI,
                                  COALESCE(CAST(( (SELECT COALESCE(CO.SHARE, 0) 
                                                FROM USINSUV01.COINSURAN CO
                                                WHERE CO.USERCOMP = C.USERCOMP 
                                                AND CO.COMPANY = C.COMPANY 
                                                AND CO.CERTYPE = C.CERTYPE
                                                AND CO.BRANCH = C.BRANCH 
                                                AND CO.POLICY = C.POLICY
                                                AND CO.COMPANYC = 12
                                                AND CO.EFFECDATE <= C.EFFECDATE
                                                AND (CO.NULLDATE IS NULL OR CO.NULLDATE > C.EFFECDATE)) * C.PREMIUM) AS VARCHAR), '100') AS VMTPREMC,                       
                                  COALESCE(C.CAPITALI, 0) AS VMTCAPIN,
                                  COALESCE((SELECT COV.PREMIUM FROM USINSUV01.COVER COV 
                                         LEFT JOIN USINSUV01.CERTIFICAT CERT                           
                                         ON COV.USERCOMP = CERT.USERCOMP 
                                         AND COV.COMPANY  = CERT.COMPANY  
                                         AND COV.CERTYPE  = CERT.CERTYPE 
                                         AND COV.BRANCH   = CERT.BRANCH 
                                         AND COV.POLICY   = CERT.POLICY
                                         AND COV.CERTIF   = CERT.CERTIF
                                         AND COV.CURRENCY = C.CURRENCY 
                                         AND COV.COVER    = C.COVER
                                         AND COV.MODULEC  = C.MODULEC
                                         AND COV.EFFECDATE <= CERT.DATE_ORIGI
                                         AND (COV.NULLDATE IS NULL OR COV.NULLDATE > CERT.DATE_ORIGI) LIMIT 1),
                                         (SELECT COV.PREMIUM FROM USINSUV01.COVER COV 
                                         LEFT JOIN USINSUV01.CERTIFICAT CERT                           
                                         ON  COV.USERCOMP = CERT.USERCOMP 
                                         AND COV.COMPANY  = CERT.COMPANY  
                                         AND COV.CERTYPE  = CERT.CERTYPE 
                                         AND COV.BRANCH   = CERT.BRANCH 
                                         AND COV.POLICY   = CERT.POLICY
                                         AND COV.CERTIF   = CERT.CERTIF
                                         AND COV.CURRENCY = C.CURRENCY 
                                         AND COV.COVER    = C.COVER
                                         AND COV.MODULEC  = C.MODULEC
                                         LEFT JOIN USINSUV01.POLICY POL
                                         ON  POL.USERCOMP = CERT.USERCOMP 
                                         AND POL.COMPANY  = CERT.COMPANY  
                                         AND POL.CERTYPE  = CERT.CERTYPE
                                         AND POL.BRANCH   = CERT.BRANCH 
                                         AND POL.POLICY   = CERT.POLICY                                    
                                         AND COV.EFFECDATE <= POL.DATE_ORIGI
                                         AND (COV.NULLDATE IS NULL OR COV.NULLDATE > POL.DATE_ORIGI) LIMIT 1)) AS VMTPREIN,
                                  COALESCE((COALESCE ((SELECT (SUM(R.SHARE/100)) * C.PREMIUM  
                                                    FROM USINSUV01.REINSURAN R 
                                                    WHERE R.USERCOMP = C.USERCOMP 
                                                    AND R.COMPANY = C.COMPANY 
                                                    AND R.CERTYPE = C.CERTYPE  
                                                    AND R.BRANCH = C.BRANCH
                                                    AND R.POLICY = C.POLICY
                                                    AND R.CERTIF = C.CERTIF 
                                                    AND R.EFFECDATE <= C.EFFECDATE
                                                    AND (R.NULLDATE IS NULL OR R.NULLDATE > C.EFFECDATE)
                                                    AND R.TYPE <> 1),
                                                    (SELECT (SUM(R.SHARE/100)) * C.PREMIUM  FROM USINSUV01.REINSURAN R 
                                                    WHERE R.USERCOMP = C.USERCOMP 
                                                    AND R.COMPANY = C.COMPANY 
                                                    AND R.CERTYPE = C.CERTYPE  
                                                    AND R.BRANCH = C.BRANCH
                                                    AND R.POLICY = C.POLICY
                                                    AND R.CERTIF = 0
                                                    AND R.EFFECDATE <= C.EFFECDATE
                                                    AND (R.NULLDATE IS NULL OR R.NULLDATE > C.EFFECDATE)
                                                    AND R.TYPE <> 1))), 0) AS VMTPRRES
                                  --,C.MODULEC
                                  FROM 
                                  ( 
                                    (SELECT
                                     C.USERCOMP, C.COMPANY, C.CERTYPE, C.BRANCH, C.CURRENCY, C.COVER, C.EFFECDATE,        
                                     C.NULLDATE,C.POLICY, C.CERTIF, C.PREMIUM, C.RATECOVE, C.CAPITAL, C.CAPITALI, C.MODULEC,
                                     POL.PRODUCT,POL.POLITYPE,POL.EFFECDATE AS EFFECDATE_POL,CERT.EFFECDATE AS EFFECDATE_CERT,
                                     CASE
                                     WHEN POL.POLITYPE = '1' --INDIVIDUAL
                                     THEN 
                                        CASE
                                        WHEN (C.EFFECDATE <= POL.EFFECDATE AND
                                             (C.NULLDATE IS NULL OR
                                              C.NULLDATE > POL.EFFECDATE)) THEN 1
                                        ELSE 
                                            CASE
                                            WHEN EXISTS (SELECT 1
                                                      FROM USINSUV01.COVER COV1
                                                      WHERE COV1.CERTYPE = C.CERTYPE
                                                      AND COV1.USERCOMP = C.USERCOMP
                                                      AND COV1.COMPANY = C.COMPANY
                                                      AND COV1.BRANCH = C.BRANCH
                                                      --AND 	COV1.PRODUCT  = C.PRODUCT
                                                      AND COV1.MODULEC  = C.MODULEC
                                                      AND COV1.POLICY   = C.POLICY
                                                      AND COV1.CERTIF   = C.CERTIF
                                                      AND COV1.CURRENCY = C.CURRENCY
                                                      AND COV1.COVER    = C.COVER
                                                      AND COV1.EFFECDATE <= POL.EFFECDATE
                                                      AND (COV1.NULLDATE IS NULL
                                                      OR COV1.NULLDATE > POL.EFFECDATE)) THEN 0
                                            ELSE 
                                                CASE
                                                WHEN C.NULLDATE = (SELECT MAX(COV1.NULLDATE)
                                                                FROM USINSUV01.COVER COV1
                                                                WHERE COV1.USERCOMP = C.USERCOMP
                                                                AND COV1.CERTYPE = C.CERTYPE
                                                                AND COV1.COMPANY = C.COMPANY
                                                                AND COV1.BRANCH = C.BRANCH
                                                                --AND 	  COV1.PRODUCT  = C.PRODUCT
                                                                AND COV1.MODULEC = C.MODULEC
                                                                AND COV1.POLICY = C.POLICY
                                                                AND COV1.CERTIF = C.CERTIF
                                                                AND COV1.CURRENCY = C.CURRENCY
                                                                AND COV1.COVER = C.COVER) THEN 1
                                                ELSE 0
                                                END
                                            END
                                        END
                                        ELSE 
                                            CASE
                                            WHEN (C.EFFECDATE <= CERT.EFFECDATE AND (C.NULLDATE IS NULL OR C.NULLDATE > CERT.EFFECDATE)) THEN 1
                                            ELSE 
                                            CASE
                                            WHEN EXISTS (SELECT  1
                                                         FROM USINSUV01.COVER COV1
                                                         WHERE COV1.CERTYPE = C.CERTYPE
                                                         AND COV1.USERCOMP = C.USERCOMP
                                                         AND COV1.COMPANY = C.COMPANY
                                                         AND COV1.BRANCH = C.BRANCH
                                                         --AND 	COV1.PRODUCT  = C.PRODUCT
                                                         AND COV1.MODULEC = C.MODULEC
                                                         AND COV1.POLICY = C.POLICY
                                                         AND COV1.CERTIF = C.CERTIF
                                                         AND COV1.CURRENCY = C.CURRENCY
                                                         AND COV1.COVER = C.COVER
                                                         AND COV1.EFFECDATE <= CERT.EFFECDATE
                                                         AND (COV1.NULLDATE IS NULL
                                                         OR COV1.NULLDATE > CERT.EFFECDATE)) THEN 0
                                            ELSE CASE
                                                 WHEN C.NULLDATE = (SELECT
                                                     MAX(COV1.NULLDATE)
                                                   FROM USINSUV01.COVER COV1
                                                   WHERE COV1.USERCOMP = C.USERCOMP
                                                   AND COV1.CERTYPE = C.CERTYPE
                                                   AND COV1.COMPANY = C.COMPANY
                                                   AND COV1.BRANCH = C.BRANCH
                                                   --AND 	  COV1.PRODUCT  = C.PRODUCT
                                                   AND COV1.MODULEC = C.MODULEC
                                                   AND COV1.POLICY = C.POLICY
                                                   AND COV1.CERTIF = C.CERTIF
                                                   AND COV1.CURRENCY = C.CURRENCY
                                                   AND COV1.COVER = C.COVER) THEN 1
                                                 ELSE 0
                                            END
                                            END
                                        END
                                     END FLAG
                                     FROM USINSUV01.COVER C
                                     LEFT JOIN USINSUV01.CERTIFICAT CERT ON C.USERCOMP = CERT.USERCOMP AND C.COMPANY = CERT.COMPANY AND C.CERTYPE = CERT.CERTYPE AND C.BRANCH = CERT.BRANCH AND C.POLICY = CERT.policy AND C.CERTIF = CERT.CERTIF
                                     JOIN USINSUV01.POLICY POL ON POL.USERCOMP = C.USERCOMP AND POL.COMPANY = C.COMPANY AND POL.CERTYPE = C.CERTYPE AND POL.BRANCH = C.BRANCH AND POL.POLICY = C.POLICY
                                     WHERE C.CERTYPE = '2'
                                     AND POL.STATUS_POL NOT IN ('2', '3')
                                     -- JAOS
                                     AND C.COMPDATE BETWEEN '{p_fecha_inicio}' AND '{p_fecha_fin}')
                                     --AND ((POL.POLITYPE = '1' -- INDIVIDUAL 
                                     --      AND POL.EXPIRDAT >= '2021-12-31'
                                     --    AND (POL.NULLDATE IS NULL OR POL.NULLDATE > '2021-12-31'))
                                     --OR (POL.POLITYPE <> '1' -- COLECTIVAS 
                                     --    AND CERT.EXPIRDAT >= '2021-12-31'
                                     --AND (CERT.NULLDATE IS NULL OR CERT.NULLDATE > '2021-12-31')))
                                     --AND POL.EFFECDATE BETWEEN '{p_fecha_inicio}' AND '{p_fecha_fin}')
                                     
                                     /*  -- JAOS YA NO ES NECESARIO EN EL INCREMENTAL
                                  
                                  UNION
                          
                                   (SELECT
                                    C.USERCOMP, C.COMPANY, C.CERTYPE, C.BRANCH, C.CURRENCY, C.COVER, C.EFFECDATE,        
                                    C.NULLDATE,C.POLICY, C.CERTIF, C.PREMIUM, C.RATECOVE, C.CAPITAL, C.CAPITALI, C.MODULEC AS MODULO,
                                    POL.PRODUCT,POL.POLITYPE,POL.EFFECDATE AS EFFECDATE_POL,CERT.EFFECDATE AS EFFECDATE_CERT,
                                    CASE
                                    WHEN POL.POLITYPE = '1' --INDIVIDUAL
                                    THEN CASE
                                    WHEN (C.EFFECDATE <= POL.EFFECDATE AND
                                    (C.NULLDATE IS NULL OR
                                    C.NULLDATE > POL.EFFECDATE)) THEN 1
                                    ELSE CASE
                                     WHEN EXISTS (SELECT
                                          1
                                        FROM USINSUV01.COVER COV1
                                        WHERE COV1.CERTYPE = C.CERTYPE
                                        AND COV1.USERCOMP = C.USERCOMP
                                        AND COV1.COMPANY = C.COMPANY
                                        AND COV1.BRANCH = C.BRANCH
                                        --AND 	COV1.PRODUCT  = C.PRODUCT
                                        AND COV1.MODULEC = C.MODULEC
                                        AND COV1.POLICY = C.POLICY
                                        AND COV1.CERTIF = C.CERTIF
                                        AND COV1.CURRENCY = C.CURRENCY
                                        AND COV1.COVER = C.COVER
                                        AND COV1.EFFECDATE <= POL.EFFECDATE
                                        AND (COV1.NULLDATE IS NULL
                                        OR COV1.NULLDATE > POL.EFFECDATE)) THEN 0
                                     ELSE CASE
                                         WHEN C.NULLDATE = (SELECT
                                              MAX(COV1.NULLDATE)
                                            FROM USINSUV01.COVER COV1
                                            WHERE COV1.USERCOMP = C.USERCOMP
                                            AND COV1.CERTYPE = C.CERTYPE
                                            AND COV1.COMPANY = C.COMPANY
                                            AND COV1.BRANCH = C.BRANCH
                                            --AND 	  COV1.PRODUCT  = C.PRODUCT
                                            AND COV1.MODULEC = C.MODULEC
                                            AND COV1.POLICY = C.POLICY
                                            AND COV1.CERTIF = C.CERTIF
                                            AND COV1.CURRENCY = C.CURRENCY
                                            AND COV1.COVER = C.COVER) THEN 1
                                         ELSE 0
                                       END
                                    END
                                     END
                                     ELSE CASE
                                       WHEN (C.EFFECDATE <= CERT.EFFECDATE AND
                                       (C.NULLDATE IS NULL OR
                                       C.NULLDATE > CERT.EFFECDATE)) THEN 1
                                     ELSE CASE
                                      WHEN EXISTS (SELECT  1
                                        FROM USINSUV01.COVER COV1
                                        WHERE COV1.CERTYPE = C.CERTYPE
                                        AND COV1.USERCOMP = C.USERCOMP
                                        AND COV1.COMPANY = C.COMPANY
                                        AND COV1.BRANCH = C.BRANCH
                                        --AND 	COV1.PRODUCT  = C.PRODUCT
                                        AND COV1.MODULEC = C.MODULEC
                                        AND COV1.POLICY = C.POLICY
                                        AND COV1.CERTIF = C.CERTIF
                                        AND COV1.CURRENCY = C.CURRENCY
                                        AND COV1.COVER = C.COVER
                                        AND COV1.EFFECDATE <= CERT.EFFECDATE
                                        AND (COV1.NULLDATE IS NULL
                                        OR COV1.NULLDATE > CERT.EFFECDATE)) THEN 0
                                      ELSE CASE
                                          WHEN C.NULLDATE = (SELECT
                                              MAX(COV1.NULLDATE)
                                            FROM USINSUV01.COVER COV1
                                            WHERE COV1.USERCOMP = C.USERCOMP
                                            AND COV1.CERTYPE = C.CERTYPE
                                            AND COV1.COMPANY = C.COMPANY
                                            AND COV1.BRANCH = C.BRANCH
                                            --AND 	  COV1.PRODUCT  = C.PRODUCT
                                            AND COV1.MODULEC = C.MODULEC
                                            AND COV1.POLICY = C.POLICY
                                            AND COV1.CERTIF = C.CERTIF
                                            AND COV1.CURRENCY = C.CURRENCY
                                            AND COV1.COVER = C.COVER) THEN 1
                                          ELSE 0
                                        END
                                    END
                                    END
                                    END FLAG
                                    FROM USINSUV01.COVER C
                                    LEFT JOIN USINSUV01.CERTIFICAT CERT ON C.USERCOMP = CERT.USERCOMP AND C.COMPANY = CERT.COMPANY AND C.CERTYPE = CERT.CERTYPE AND C.BRANCH = CERT.BRANCH AND C.POLICY = CERT.policy AND C.CERTIF = CERT.CERTIF
                                    JOIN USINSUV01.POLICY POL ON POL.USERCOMP = C.USERCOMP AND POL.COMPANY = C.COMPANY AND POL.CERTYPE = C.CERTYPE AND POL.BRANCH = C.BRANCH AND POL.POLICY = C.POLICY
                                    WHERE POL.CERTYPE  = '2' 
                                    AND POL.STATUS_POL NOT IN ('2', '3') 
                                    AND (((POL.POLITYPE = '1' AND  POL.EXPIRDAT < '{l_fecha_carga_inicial}' OR POL.NULLDATE < '{l_fecha_carga_inicial}')
                                    AND EXISTS (SELECT 1 FROM  USINSUV01.CLAIM CLA    
                                                JOIN  USINSUV01.CLAIM_HIS CLH ON CLH.USERCOMP = CLA.USERCOMP AND CLH.COMPANY = CLA.COMPANY AND CLH.BRANCH = CLA.BRANCH AND CLH.CLAIM = CLA.CLAIM
                                                WHERE CLA.BRANCH = POL.BRANCH AND CLA.POLICY = POL.POLICY AND TRIM(CLH.OPER_TYPE) IN (SELECT CAST(TCL.OPERATION AS VARCHAR(2))
                                        		                                                                                          FROM 	USINSUG01.TAB_CL_OPE TCL
                                        		                                                                                          WHERE  (TCL.RESERVE = 1 OR TCL.AJUSTES = 1 OR TCL.PAY_AMOUNT = 1)) 
                                                                                                                                      AND   CLH.OPERDATE >= '{l_fecha_carga_inicial}')))                                                                                                                                        
                                    AND POL.EFFECDATE BETWEEN '{p_fecha_inicio}' AND '{p_fecha_fin}')
                                                                              
                                  UNION
                          
                                   (SELECT
                                   C.USERCOMP, C.COMPANY, C.CERTYPE, C.BRANCH, C.CURRENCY, C.COVER, C.EFFECDATE,        
                                   C.NULLDATE,C.POLICY, C.CERTIF, C.PREMIUM, C.RATECOVE, C.CAPITAL, C.CAPITALI, C.MODULEC AS MODULO,
                                   POL.PRODUCT,POL.POLITYPE,POL.EFFECDATE AS EFFECDATE_POL,CERT.EFFECDATE AS EFFECDATE_CERT,
                                   CASE
                                   WHEN POL.POLITYPE = '1' --INDIVIDUAL
                                   THEN CASE
                                   WHEN (C.EFFECDATE <= POL.EFFECDATE AND
                                    (C.NULLDATE IS NULL OR
                                    C.NULLDATE > POL.EFFECDATE)) THEN 1
                                   ELSE CASE
                                      WHEN EXISTS (SELECT
                                          1
                                        FROM USINSUV01.COVER COV1
                                        WHERE COV1.CERTYPE = C.CERTYPE
                                        AND COV1.USERCOMP = C.USERCOMP
                                        AND COV1.COMPANY = C.COMPANY
                                        AND COV1.BRANCH = C.BRANCH
                                        --AND 	COV1.PRODUCT  = C.PRODUCT
                                        AND COV1.MODULEC = C.MODULEC
                                        AND COV1.POLICY = C.POLICY
                                        AND COV1.CERTIF = C.CERTIF
                                        AND COV1.CURRENCY = C.CURRENCY
                                        AND COV1.COVER = C.COVER
                                        AND COV1.EFFECDATE <= POL.EFFECDATE
                                        AND (COV1.NULLDATE IS NULL
                                        OR COV1.NULLDATE > POL.EFFECDATE)) THEN 0
                                      ELSE CASE
                                          WHEN C.NULLDATE = (SELECT
                                              MAX(COV1.NULLDATE)
                                            FROM USINSUV01.COVER COV1
                                            WHERE COV1.USERCOMP = C.USERCOMP
                                            AND COV1.CERTYPE = C.CERTYPE
                                            AND COV1.COMPANY = C.COMPANY
                                            AND COV1.BRANCH = C.BRANCH
                                            --AND 	  COV1.PRODUCT  = C.PRODUCT
                                            AND COV1.MODULEC = C.MODULEC
                                            AND COV1.POLICY = C.POLICY
                                            AND COV1.CERTIF = C.CERTIF
                                            AND COV1.CURRENCY = C.CURRENCY
                                            AND COV1.COVER = C.COVER) THEN 1
                                          ELSE 0
                                        END
                                    END
                                   END
                                   ELSE CASE
                                       WHEN (C.EFFECDATE <= CERT.EFFECDATE AND
                                       (C.NULLDATE IS NULL OR
                                       C.NULLDATE > CERT.EFFECDATE)) THEN 1
                                   ELSE CASE
                                      WHEN EXISTS (SELECT  1
                                        FROM USINSUV01.COVER COV1
                                        WHERE COV1.CERTYPE = C.CERTYPE
                                        AND COV1.USERCOMP = C.USERCOMP
                                        AND COV1.COMPANY = C.COMPANY
                                        AND COV1.BRANCH = C.BRANCH
                                        --AND 	COV1.PRODUCT  = C.PRODUCT
                                        AND COV1.MODULEC = C.MODULEC
                                        AND COV1.POLICY = C.POLICY
                                        AND COV1.CERTIF = C.CERTIF
                                        AND COV1.CURRENCY = C.CURRENCY
                                        AND COV1.COVER = C.COVER
                                        AND COV1.EFFECDATE <= CERT.EFFECDATE
                                        AND (COV1.NULLDATE IS NULL
                                        OR COV1.NULLDATE > CERT.EFFECDATE)) THEN 0
                                      ELSE CASE
                                          WHEN C.NULLDATE = (SELECT
                                              MAX(COV1.NULLDATE)
                                            FROM USINSUV01.COVER COV1
                                            WHERE COV1.USERCOMP = C.USERCOMP
                                            AND COV1.CERTYPE = C.CERTYPE
                                            AND COV1.COMPANY = C.COMPANY
                                            AND COV1.BRANCH = C.BRANCH
                                            --AND 	  COV1.PRODUCT  = C.PRODUCT
                                            AND COV1.MODULEC = C.MODULEC
                                            AND COV1.POLICY = C.POLICY
                                            AND COV1.CERTIF = C.CERTIF
                                            AND COV1.CURRENCY = C.CURRENCY
                                            AND COV1.COVER = C.COVER) THEN 1
                                          ELSE 0
                                        END
                                    END
                                   END
                                   END FLAG
                                   FROM USINSUV01.COVER C
                                   LEFT JOIN USINSUV01.CERTIFICAT CERT ON C.USERCOMP = CERT.USERCOMP AND C.COMPANY = CERT.COMPANY AND C.CERTYPE = CERT.CERTYPE AND C.BRANCH = CERT.BRANCH AND C.POLICY = CERT.policy AND C.CERTIF = CERT.CERTIF
                                   JOIN USINSUV01.POLICY POL ON POL.USERCOMP = C.USERCOMP AND POL.COMPANY = C.COMPANY AND POL.CERTYPE = C.CERTYPE AND POL.BRANCH = C.BRANCH AND POL.POLICY = C.POLICY
                                   WHERE POL.CERTYPE  = '2' 
                                   AND POL.STATUS_POL NOT IN ('2', '3')
                                   AND (((POL.POLITYPE <> '1' AND CERT.EXPIRDAT < '{l_fecha_carga_inicial}'  OR  CERT.NULLDATE < '{l_fecha_carga_inicial}') 
                                   AND EXISTS (SELECT 1 FROM  USINSUV01.CLAIM CLA    
                                               JOIN  USINSUV01.CLAIM_HIS CLH  ON CLA.USERCOMP = CLH.USERCOMP AND CLA.COMPANY = CLH.COMPANY AND CLA.BRANCH = CLH.BRANCH  AND CLH.CLAIM = CLA.CLAIM
                                               WHERE CLA.BRANCH   = CERT.BRANCH
                                               AND   CLA.POLICY   = CERT.POLICY
                                               AND   CLA.CERTIF   = CERT.CERTIF
                                               AND   TRIM(CLH.OPER_TYPE) IN (SELECT CAST(TCL.OPERATION AS VARCHAR(2)) FROM  USINSUG01.TAB_CL_OPE TCL WHERE (TCL.RESERVE = 1 OR TCL.AJUSTES = 1 OR TCL.PAY_AMOUNT = 1)) AND  CLH.OPERDATE >= '{l_fecha_carga_inicial}')))
                                   AND POL.EFFECDATE BETWEEN {'p_fecha_inicio'} AND {'p_fecha_fin'})  -- JAOS YA NO ES NECESARIO */) 
                          C WHERE C.FLAG = 1) T ) AS TMP '''
 
  l_df_abcobap_insunix_lpv = glue_context.read.format('jdbc').options(**connection).option("dbtable", l_abcobap_insunix_lpv).load()
  
  print("ABCOBAP INSUNIX LPV EXITOSO")
  #-------------------------------------------------------------------------------------------------------------------------------#

  l_abcobap_vtime_lpg = f'''
                        (
                          SELECT 
                          'D' AS INDDETREC,
                          'ABCOBAP' AS TABLAIFRS17,
                          '' AS PK,
                          '' AS DTPREG,
                          '' AS TIOCPROC,
                          TIOCFRM,
                          '' AS TIOCTO,
                          'PVG' AS KGIORIGM,
                          KABAPOL,
                          '' AS KABUNRIS,
                          KGCTPCBT,
                          TINICIO,
                          TTERMO,
                          '' AS TSITCOB,
                          '' AS KACSITCB,
                          '' AS VMTPRMSP,
                          VMTCOMR,
                          '' AS VMTBOMAT,
                          '' AS VTXBOMAT,
                          '' AS VMTBOCOM,
                          '' AS VTXBOCOM,
                          '' AS VMTDECOM,
                          '' AS VTXDECOM,
                          '' AS VMTDETEC,
                          '' AS VTXDETEC,
                          '' AS VMTAGRAV,
                          '' AS VTXAGRAV,
                          '' AS VMTPRMTR,
                          '' AS VMTPRLIQ,
                          VMTPRMBR,
                          VTXCOB,
                          VCAPITAL, 
                          '' AS VTXCAPIT,
                          '' AS KACTPIDX,
                          '' AS VTXINDX,
                          'LPG' AS DCOMPA,
                          '' AS DMARCA,
                          '' AS TDACECOB,
                          '' AS TDCANCOB,
                          '' AS TDCRICOB,
                          TDRENOVA,
                          '' AS TDVENTRA,
                          '' AS DHORAINI,
                          VMTPREMC,
                          '' AS VMIBOMAT,
                          '' AS VMIBOCOM,
                          '' AS VMIDECOM,
                          '' AS VMIDETEC,
                          '' AS VMIRPMSP,
                          '' AS VMIPRMBR,
                          '' AS VMICOMR,
                          '' AS VMIPRLIQ,
                          '' AS VMICMNQP,
                          '' AS VMIPRMTR,
                          '' AS VMIAGRAV,
                          '' AS KACTIPCB,
                          '' AS VMTCAPLI,
                          '' AS KACTRARE, --PENDIENTE
                          '' AS KACFMCAL,
                          '' AS DFACMULT,
                          VMTCAPIN,
                          VMTPREIN,
                          '' AS DINDESES,
                          '' AS DINDMOTO,
                          '' AS KACSALIN,
                          '' AS VMTSALMD,
                          '' AS VTXLMRES,
                          '' AS VTXEQUIP,
                          '' AS VTXPRIOR,
                          '' AS VTXCONTR,
                          '' AS VTXESPEC,
                          '' AS DCAPMORT,
                          VMTPRRES,
                          '' AS DIDADETAR,
                          '' AS DIDADLIMCOBA,
                          KACTPDUR,
                          '' AS KGCRAMO_SAP,
                          '' AS KACTCOMP,
                          '' AS KACINDTX,
                          '' AS KACCALIDA,
                          '' AS DNCABCALP,
                          '' AS DINDNIVEL,
                          DURCOB,
                          '' AS DURPAGCOB,
                          '' AS KACTPDURCB,
                          '' AS DINCOBINDX,
                          '' AS KACGRCBT,
                          '' AS KABTRTAB_2,
                          '' AS VTXAJTBUA,
                          '' AS VMTCAPREM
                          --,MODULO
                          FROM
                          (
                                SELECT 
                                COALESCE(CAST(CAST(C."DEFFECDATE" AS DATE) AS VARCHAR),'') AS TIOCFRM,
                                C."NBRANCH" ||'-'|| C."NPRODUCT" ||'-'|| C."NPOLICY" ||'-'|| C."NCERTIF" AS KABAPOL,
                                COALESCE(COALESCE((SELECT CAST(GLC."NCOVERGEN" AS VARCHAR)
                                          FROM /*USBI01.IFRS170_V_GEN_LIFE_COVER_VTIMELPG*/
                                          (SELECT GC."NBRANCH",
                                          GC."NPRODUCT",
                                          GC."NMODULEC",
                                          GC."NCOVER",
                                          GC."DEFFECDATE",
                                          GC."DNULLDATE",
                                          GC."NCOVERGEN",
                                          GC."NCURRENCY"
                                          FROM USVTIMG01."GEN_COVER" GC
                                          UNION ALL
                                          SELECT LC."NBRANCH",
                                          LC."NPRODUCT",
                                          LC."NMODULEC",
                                          LC."NCOVER",
                                          LC."DEFFECDATE",
                                          LC."DNULLDATE",
                                          LC."NCOVERGEN",
                                          LC."NCURRENCY"
                                          FROM USVTIMG01."LIFE_COVER" LC)  GLC 
                                          WHERE GLC."NBRANCH" = C."NBRANCH" 
                                          AND   GLC."NPRODUCT"  = C."NPRODUCT"
                                          AND   GLC."NMODULEC"  = C."NMODULEC"
                                          AND   GLC."NCOVER"    = C."NCOVER"
                                          AND   GLC."DEFFECDATE" <= (case when C."SPOLITYPE" = '1' then C."POL_DSTARTDATE" else C."CERT_DSTARTDATE" end) 
                                          AND (GLC."DNULLDATE" IS NULL OR GLC."DNULLDATE" > (case when C."SPOLITYPE" = '1' then C."POL_DSTARTDATE" else C."CERT_DSTARTDATE" end) )), 
                                          (select CAST(GLC."NCOVERGEN" as VARCHAR) /*USBI01.IFRS170_V_GEN_LIFE_COVER_VTIMELPG*/
                                              from (SELECT GC."NBRANCH",
                                              GC."NPRODUCT",
                                              GC."NMODULEC",
                                              GC."NCOVER",
                                              GC."DEFFECDATE",
                                              GC."DNULLDATE",
                                              GC."NCOVERGEN",
                                              GC."NCURRENCY"
                                              FROM USVTIMG01."GEN_COVER" GC
                                              UNION ALL
                                              SELECT LC."NBRANCH",
                                              LC."NPRODUCT",
                                              LC."NMODULEC",
                                              LC."NCOVER",
                                              LC."DEFFECDATE",
                                              LC."DNULLDATE",
                                              LC."NCOVERGEN",
                                              LC."NCURRENCY"
                                              FROM USVTIMG01."LIFE_COVER" LC)  GLC 
                                              WHERE GLC."NBRANCH" = C."NBRANCH" 
                                              AND   GLC."NPRODUCT"  = C."NPRODUCT"
                                              AND   GLC."NMODULEC"  = C."NMODULEC"
                                              AND   GLC."NCOVER"    = C."NCOVER"
                                              and   GLC."DNULLDATE" = (SELECT MAX("DNULLDATE")
                                                            FROM /*USBI01.IFRS170_V_GEN_LIFE_COVER_VTIMELPG*/
                                                            (SELECT GC."NBRANCH",
                                                            GC."NPRODUCT",
                                                            GC."NMODULEC",
                                                            GC."NCOVER",
                                                            GC."DEFFECDATE",
                                                            GC."DNULLDATE",
                                                            GC."NCOVERGEN",
                                                            GC."NCURRENCY"
                                                            FROM USVTIMG01."GEN_COVER" GC
                                                            UNION ALL
                                                            SELECT LC."NBRANCH",
                                                            LC."NPRODUCT",
                                                            LC."NMODULEC",
                                                            LC."NCOVER",
                                                            LC."DEFFECDATE",
                                                            LC."DNULLDATE",
                                                            LC."NCOVERGEN",
                                                            LC."NCURRENCY"
                                                            FROM USVTIMG01."LIFE_COVER" LC)  GLC 
                                                            WHERE GLC."NBRANCH" = C."NBRANCH" 
                                                            AND   GLC."NPRODUCT"  = C."NPRODUCT"
                                                            AND   GLC."NMODULEC"  = C."NMODULEC"
                                                            AND   GLC."NCOVER"    = C."NCOVER"))), ('-'||cast(C."NCOVER" as VARCHAR))) AS KGCTPCBT,
                                COALESCE(CAST(CAST(C."DEFFECDATE" AS DATE) AS VARCHAR),'') AS TINICIO,
                                COALESCE(CAST(CAST(C."DNULLDATE" AS DATE) AS VARCHAR),'') AS TTERMO,
                                COALESCE(C."NPREMIUM_O",0) AS VMTCOMR,
                                COALESCE(C."NPREMIUM_O",0) AS VMTPRMBR,
                                COALESCE(C."NRATECOVE",0) AS VTXCOB, --TASA APLICAR A LA COBERTURA
                                COALESCE(CAST(C."NCAPITAL" AS VARCHAR), '0')  AS VCAPITAL,
                                COALESCE(CAST(CAST(C."DEFFECDATE" AS DATE )AS VARCHAR),'') AS TDRENOVA,
                                COALESCE(CAST(((SELECT COALESCE(CO."NSHARE")  
                                                FROM USVTIMG01."COINSURAN" CO
                                                WHERE CO."SCERTYPE" = C."SCERTYPE" 
                                                AND CO."NBRANCH"  = C."NBRANCH" 
                                                AND CO."NPRODUCT" = C."NPRODUCT"
                                                AND CO."NPOLICY" = C."NPOLICY"
                                                AND CO."NCOMPANY" = 2
                                                AND CO."DEFFECDATE"  <= C."DEFFECDATE"
                                                AND (CO."DNULLDATE" IS NULL AND CO."DNULLDATE"  > C."DEFFECDATE")) * C."NPREMIUM") AS VARCHAR), '100') AS VMTPREMC,                                  
                                COALESCE(C."NCAPITALI", 0)  AS VMTCAPIN,
                                COALESCE(TRUNC(C."NPREMIUM_O", 2), 0) AS VMTPREIN,
                                COALESCE((COALESCE ((SELECT (SUM(R."NSHARE"/100)) * C."NPREMIUM"  FROM USVTIMG01."REINSURAN" R 
                                      WHERE R."SCERTYPE" = C."SCERTYPE"  
                                      AND R."NBRANCH"  = C."NBRANCH"
                                      AND R."NPRODUCT" = C."NPRODUCT"
                                      AND R."NPOLICY"  = C."NPOLICY"
                                      AND R."NCERTIF"  = C."NCERTIF"
                                      AND R."NMODULEC" = C."NMODULEC"
                                      AND R."NCOVER" = C."NCOVER"
                                      AND R."DEFFECDATE" <= C."DEFFECDATE"
                                      AND (R."DNULLDATE" IS NULL OR R."DNULLDATE" > C."DEFFECDATE")
                                      AND R."NTYPE_REIN" <> 1),
                                      (SELECT (SUM(R."NSHARE"/100)) * C."NPREMIUM" FROM USVTIMG01."REINSURAN" R 
                                      WHERE R."SCERTYPE" = C."SCERTYPE"  
                                      AND R."NBRANCH"  = C."NBRANCH"
                                      AND R."NPRODUCT" = C."NPRODUCT"
                                      AND R."NPOLICY"  = C."NPOLICY"
                                      AND R."NCERTIF"  = 0
                                      AND R."NMODULEC" = C."NMODULEC"
                                      AND R."NCOVER" = C."NCOVER"
                                      AND R."DEFFECDATE" <= C."DEFFECDATE"
                                      AND (R."DNULLDATE" IS NULL OR R."DNULLDATE" > C."DEFFECDATE")
                                      AND R."NTYPE_REIN" <> 1))), 0) AS VMTPRRES,
                                COALESCE(CAST(C."NTYPDURINS" AS VARCHAR),'0') AS KACTPDUR,
                                COALESCE(CAST(C."NDURINSUR" AS VARCHAR),'0') AS DURCOB
                                --,C."NMODULEC" AS MODULO   
                                FROM 
                                (
                                  ( SELECT
                                    C."SCERTYPE",
                                    C."NBRANCH",
                                    C."NPRODUCT",
                                    C."NPOLICY",
                                    C."NCERTIF",                                    
                                    C."DEFFECDATE",
                                    C."DNULLDATE",
                                    C."NPREMIUM",
                                    C."NPREMIUM_O",
                                    C."NRATECOVE",
                                    C."NCAPITAL",
                                    C."NCAPITALI",
                                    C."NTYPDURINS",
                                    C."NDURINSUR",
                                    C."NMODULEC", 
                                    C."NCOVER",
                                    C."NCURRENCY",
                                    POL."SPOLITYPE",
                                    POL."DSTARTDATE" as "POL_DSTARTDATE",
                                    CERT."DSTARTDATE" as "CERT_DSTARTDATE",
                                    CASE 
                                    WHEN POL."SPOLITYPE" = '1' --INDIVIDUAL
                                    THEN 
                                    CASE WHEN (C."DEFFECDATE" <= POL."DSTARTDATE" AND (C."DNULLDATE" IS NULL OR C."DNULLDATE" > POL."DSTARTDATE")) THEN 1		                               
                                        ELSE 
                                    CASE	
                                        WHEN EXISTS ( SELECT	1
                                                      FROM	usvtimg01."COVER" COV1
                                                      WHERE 	COV1."SCERTYPE" = C."SCERTYPE"
                                                      AND     COV1."NBRANCH"    = C."NBRANCH"
                                                      AND 	  COV1."NPRODUCT"   = C."NPRODUCT"		                              	                  
                                                        AND     COV1."NMODULEC"   = C."NMODULEC"
                                                        AND     COV1."NPOLICY"    = C."NPOLICY"
                                                        AND     COV1."NCERTIF"    = C."NCERTIF"
                                                        AND     COV1."NCURRENCY"  = C."NCURRENCY"
                                                        AND     COV1."NCOVER"     = C."NCOVER" 
                                                      AND		  COV1."DEFFECDATE" <= POL."DSTARTDATE"
                                                      AND     (COV1."DNULLDATE" IS NULL OR COV1."DNULLDATE" > POL."DSTARTDATE")) THEN 0
                                            ELSE 
                                                CASE	
                                            WHEN C."DNULLDATE" = (SELECT MAX(COV1."DNULLDATE")
                                                                  FROM	usvtimg01."COVER" COV1
                                                                  WHERE  COV1."SCERTYPE" = C."SCERTYPE"
                                                                  AND 	COV1."NBRANCH"  = C."NBRANCH"
                                                                  AND    COV1."NPRODUCT" = C."NPRODUCT"
                                                                      AND   COV1."NMODULEC"  = C."NMODULEC"
                                                                      AND   COV1."NPOLICY"   = C."NPOLICY"
                                                                      AND   COV1."NCERTIF"   = C."NCERTIF"
                                                                      AND   COV1."NCURRENCY" = C."NCURRENCY"
                                                                      AND   COV1."NCOVER"    = C."NCOVER" ) THEN 1
                                            ELSE 0
                                            END 
                                        END
                                    END  
                                              ELSE
                                                    CASE WHEN (C."DEFFECDATE" <= CERT."DSTARTDATE" AND (C."DNULLDATE" IS NULL OR C."DNULLDATE" > CERT."DSTARTDATE")) THEN 1		                               
                                          ELSE 
                                                CASE	
                                          WHEN EXISTS ( SELECT	1
                                                      FROM	usvtimg01."COVER" COV1
                                                      WHERE 	COV1."SCERTYPE" = C."SCERTYPE"
                                                      AND     COV1."NBRANCH"    = C."NBRANCH"
                                                      AND 	  COV1."NPRODUCT"   = C."NPRODUCT"		                              	                  
                                                        AND     COV1."NMODULEC"   = C."NMODULEC"
                                                        AND     COV1."NPOLICY"    = C."NPOLICY"
                                                        AND     COV1."NCERTIF"    = C."NCERTIF"
                                                        AND     COV1."NCURRENCY"  = C."NCURRENCY"
                                                        AND     COV1."NCOVER"     = C."NCOVER" 
                                                      AND		  COV1."DEFFECDATE" <= cert."DSTARTDATE"
                                                      AND     (COV1."DNULLDATE" IS NULL OR COV1."DNULLDATE" > cert."DSTARTDATE")) THEN 0
                                          ELSE 
                                              CASE	
                                          WHEN C."DNULLDATE" = (SELECT MAX(COV1."DNULLDATE")
                                                                  FROM	usvtimg01."COVER" COV1
                                                                  WHERE  COV1."SCERTYPE" = C."SCERTYPE"
                                                                  AND 	COV1."NBRANCH"  = C."NBRANCH"
                                                                  AND    COV1."NPRODUCT" = C."NPRODUCT"
                                                                      AND   COV1."NMODULEC"  = C."NMODULEC"
                                                                      AND   COV1."NPOLICY"   = C."NPOLICY"
                                                                      AND   COV1."NCERTIF"   = C."NCERTIF"
                                                                      AND   COV1."NCURRENCY" = C."NCURRENCY"
                                                                      AND   COV1."NCOVER"    = C."NCOVER") THEN 1
                                          ELSE 0
                                          END 
                                      END
                                    END 
                                    END FLAG
                                    FROM USVTIMG01."COVER" C  
                                    LEFT JOIN USVTIMG01."CERTIFICAT" CERT
                                    ON  C."SCERTYPE"      = CERT."SCERTYPE"  
                                    AND C."NBRANCH"   = CERT."NBRANCH"
                                    AND C."NPRODUCT"  = CERT."NPRODUCT"
                                    AND C."NPOLICY"   = CERT."NPOLICY"
                                    AND C."NCERTIF"   = CERT."NCERTIF"
                                    JOIN USVTIMG01."POLICY" POL
                                    ON  POL."SCERTYPE"  = C."SCERTYPE"
                                    AND POL."NBRANCH"   = C."NBRANCH" 
                                    AND POL."NPRODUCT"  = C."NPRODUCT"
                                    AND POL."NPOLICY"   = C."NPOLICY" 
                                    WHERE POL."SCERTYPE" = '2' 
                                    AND   POL."SSTATUS_POL" NOT IN ('2','3') 
                                    -- JAOS
                                    AND C.DCOMPDATE BETWEEN '{p_fecha_inicio}' AND '{p_fecha_fin}')
                                    --AND ((POL."SPOLITYPE" = '1' -- INDIVIDUAL 
                                    --AND POL."DEXPIRDAT" >= '{l_fecha_carga_inicial}' 
                                    --AND (POL."DNULLDATE" IS NULL OR POL."DNULLDATE" > '{l_fecha_carga_inicial}'))
                                    --OR 
                                    --(POL."SPOLITYPE" <> '1' -- COLECTIVAS 
                                    --AND CERT."DEXPIRDAT" >= '{l_fecha_carga_inicial}' 
                                    --AND (CERT."DNULLDATE" IS NULL OR CERT."DNULLDATE" > '{l_fecha_carga_inicial}')))
                                    --AND POL."DSTARTDATE" BETWEEN '{p_fecha_inicio}' AND '{p_fecha_fin}') 
                                  /*  -- JAOS YA NO ES NECESARIO EN EL INCREMENTAL
                                  UNION

                                  ( SELECT
                                    C."SCERTYPE",
                                    C."NBRANCH",
                                    C."NPRODUCT",
                                    C."NPOLICY",
                                    C."NCERTIF",                                    
                                    C."DEFFECDATE",
                                    C."DNULLDATE",
                                    C."NPREMIUM",
                                    C."NPREMIUM_O",
                                    C."NRATECOVE",
                                    C."NCAPITAL",
                                    C."NCAPITALI",
                                    C."NTYPDURINS",
                                    C."NDURINSUR",
                                    C."NMODULEC", 
                                    C."NCOVER",
                                    C."NCURRENCY",
                                    POL."SPOLITYPE",
                                    POL."DSTARTDATE" as "POL_DSTARTDATE",
                                    CERT."DSTARTDATE" as "CERT_DSTARTDATE",
                                    CASE 
                                    WHEN POL."SPOLITYPE" = '1' --INDIVIDUAL
                                    THEN 
                                    CASE WHEN (C."DEFFECDATE" <= POL."DSTARTDATE" AND (C."DNULLDATE" IS NULL OR C."DNULLDATE" > POL."DSTARTDATE")) THEN 1		                               
                                        ELSE 
                                    CASE	
                                        WHEN EXISTS ( SELECT	1
                                                      FROM	usvtimg01."COVER" COV1
                                                      WHERE 	COV1."SCERTYPE" = C."SCERTYPE"
                                                      AND     COV1."NBRANCH"    = C."NBRANCH"
                                                      AND 	  COV1."NPRODUCT"   = C."NPRODUCT"		                              	                  
                                                        AND     COV1."NMODULEC"   = C."NMODULEC"
                                                        AND     COV1."NPOLICY"    = C."NPOLICY"
                                                        AND     COV1."NCERTIF"    = C."NCERTIF"
                                                        AND     COV1."NCURRENCY"  = C."NCURRENCY"
                                                        AND     COV1."NCOVER"     = C."NCOVER" 
                                                      AND		  COV1."DEFFECDATE" <= POL."DSTARTDATE"
                                                      AND     (COV1."DNULLDATE" IS NULL OR COV1."DNULLDATE" > POL."DSTARTDATE")) THEN 0
                                            ELSE 
                                                CASE	
                                            WHEN C."DNULLDATE" = (SELECT MAX(COV1."DNULLDATE")
                                                                  FROM	usvtimg01."COVER" COV1
                                                                  WHERE  COV1."SCERTYPE" = C."SCERTYPE"
                                                                  AND 	COV1."NBRANCH"  = C."NBRANCH"
                                                                  AND    COV1."NPRODUCT" = C."NPRODUCT"
                                                                      AND   COV1."NMODULEC"  = C."NMODULEC"
                                                                      AND   COV1."NPOLICY"   = C."NPOLICY"
                                                                      AND   COV1."NCERTIF"   = C."NCERTIF"
                                                                      AND   COV1."NCURRENCY" = C."NCURRENCY"
                                                                      AND   COV1."NCOVER"    = C."NCOVER" ) THEN 1
                                            ELSE 0
                                            END 
                                        END
                                    END  
                                              ELSE
                                                    CASE WHEN (C."DEFFECDATE" <= CERT."DSTARTDATE" AND (C."DNULLDATE" IS NULL OR C."DNULLDATE" > CERT."DSTARTDATE")) THEN 1		                               
                                          ELSE 
                                                CASE	
                                          WHEN EXISTS ( SELECT	1
                                                      FROM	usvtimg01."COVER" COV1
                                                      WHERE 	COV1."SCERTYPE" = C."SCERTYPE"
                                                      AND     COV1."NBRANCH"    = C."NBRANCH"
                                                      AND 	  COV1."NPRODUCT"   = C."NPRODUCT"		                              	                  
                                                        AND     COV1."NMODULEC"   = C."NMODULEC"
                                                        AND     COV1."NPOLICY"    = C."NPOLICY"
                                                        AND     COV1."NCERTIF"    = C."NCERTIF"
                                                        AND     COV1."NCURRENCY"  = C."NCURRENCY"
                                                        AND     COV1."NCOVER"     = C."NCOVER" 
                                                      AND		  COV1."DEFFECDATE" <= cert."DSTARTDATE"
                                                      AND     (COV1."DNULLDATE" IS NULL OR COV1."DNULLDATE" > cert."DSTARTDATE")) THEN 0
                                          ELSE 
                                              CASE	
                                          WHEN C."DNULLDATE" = (SELECT MAX(COV1."DNULLDATE")
                                                                  FROM	usvtimg01."COVER" COV1
                                                                  WHERE  COV1."SCERTYPE" = C."SCERTYPE"
                                                                  AND 	COV1."NBRANCH"  = C."NBRANCH"
                                                                  AND    COV1."NPRODUCT" = C."NPRODUCT"
                                                                      AND   COV1."NMODULEC"  = C."NMODULEC"
                                                                      AND   COV1."NPOLICY"   = C."NPOLICY"
                                                                      AND   COV1."NCERTIF"   = C."NCERTIF"
                                                                      AND   COV1."NCURRENCY" = C."NCURRENCY"
                                                                      AND   COV1."NCOVER"    = C."NCOVER") THEN 1
                                          ELSE 0
                                          END 
                                      END
                                    END 
                                    END FLAG
                                    FROM USVTIMG01."COVER" C  
                                    LEFT JOIN USVTIMG01."CERTIFICAT" CERT
                                    ON  C."SCERTYPE"  = CERT."SCERTYPE"  
                                    AND C."NBRANCH"   = CERT."NBRANCH"
                                    AND C."NPRODUCT"  = CERT."NPRODUCT"
                                    AND C."NPOLICY"   = CERT."NPOLICY"
                                    AND C."NCERTIF"   = CERT."NCERTIF"
                                    JOIN USVTIMG01."POLICY" POL
                                    ON  POL."SCERTYPE"  = C."SCERTYPE"
                                    AND POL."NBRANCH"   = C."NBRANCH" 
                                    AND POL."NPRODUCT"  = C."NPRODUCT"
                                    AND POL."NPOLICY"   = C."NPOLICY" 
                                    WHERE POL."SCERTYPE" = '2' 
                                    AND   POL."SSTATUS_POL" NOT IN ('2','3')
                                    AND ((POL."SPOLITYPE" = '1' AND (POL."DEXPIRDAT" < '{l_fecha_carga_inicial}' OR POL."DNULLDATE" < '{l_fecha_carga_inicial}')
                                    AND (EXISTS (SELECT 1 FROM USVTIMG01."CLAIM" CLA                                           
                                           JOIN (SELECT DISTINCT CLH."NCLAIM" FROM (SELECT CAST("SVALUE" AS INT4) "SVALUE" FROM USVTIMG01."CONDITION_SERV" CS 
                                                 WHERE "NCONDITION" IN (71, 72, 73)) CSV 
                                                 JOIN USVTIMG01."CLAIM_HIS" CLH 
                                                 ON COALESCE(CLH."NCLAIM", 0) > 0 
                                                 AND CLH."NOPER_TYPE" = CSV."SVALUE" 
                                                 AND CLH."DOPERDATE" >= '{l_fecha_carga_inicial}') CLH 
                                           ON CLH."NCLAIM" = CLA."NCLAIM"
                                           WHERE CLA."SCERTYPE"  = POL."SCERTYPE" 
                                           AND CLA."NBRANCH"  = POL."NBRANCH" 
                                           AND CLA."NPRODUCT" = POL."NPRODUCT"
                                           AND CLA."NPOLICY"  = POL."NPOLICY"  
                                           AND CLA."NCERTIF"  =  0)))
                                    OR (POL."SPOLITYPE" <> '1' AND (CERT."DEXPIRDAT" < '{l_fecha_carga_inicial}' OR CERT."DNULLDATE" < '{l_fecha_carga_inicial}')
                                    AND ((EXISTS (SELECT 1 FROM USVTIMG01."CLAIM" CLA                                           
                                           JOIN (SELECT DISTINCT CLH."NCLAIM" FROM (SELECT CAST("SVALUE" AS INT4) "SVALUE" FROM USVTIMG01."CONDITION_SERV" CS 
                                                 WHERE "NCONDITION" IN (71, 72, 73)) CSV 
                                                 JOIN USVTIMG01."CLAIM_HIS" CLH 
                                                 ON COALESCE(CLH."NCLAIM", 0) > 0 
                                                 AND CLH."NOPER_TYPE" = CSV."SVALUE" 
                                                 AND CLH."DOPERDATE" >= '{l_fecha_carga_inicial}') CLH 
                                           ON CLH."NCLAIM" = CLA."NCLAIM"
                                           WHERE CLA."SCERTYPE"  = POL."SCERTYPE" 
                                           AND CLA."NBRANCH"  = POL."NBRANCH" 
                                           AND CLA."NPRODUCT" = POL."NPRODUCT"
                                           AND CLA."NPOLICY"  = POL."NPOLICY"  
                                           AND CLA."NCERTIF"  = CERT."NCERTIF")))))) -- JAOS NO ES NECESARIO EN EL INCREMENTAL */
                                ) C WHERE FLAG = 1
                          ) COVER                                            
                        ) AS TMP'''

  l_df_abcobap_vtime_lpg = glue_context.read.format('jdbc').options(**connection).option("dbtable", l_abcobap_vtime_lpg).load()

  print("ABCOBAP VTIME LPG EXITOSO")

  l_abcobap_vtime_lpv = f'''
                        ( 
                          SELECT 
                          'D' AS INDDETREC,
                          'ABCOBAP' AS TABLAIFRS17,
                          '' AS PK,
                          '' AS DTPREG,
                          '' AS TIOCPROC,
                          TIOCFRM,
                          '' AS TIOCTO,
                          'PVV' AS KGIORIGM,
                          KABAPOL,
                          '' AS KABUNRIS,
                          KGCTPCBT,
                          TINICIO,
                          TTERMO,
                          '' AS TSITCOB,
                          '' AS KACSITCB,
                          '' AS VMTPRMSP,
                          VMTCOMR,
                          '' AS VMTBOMAT,
                          '' AS VTXBOMAT,
                          '' AS VMTBOCOM,
                          '' AS VTXBOCOM,
                          '' AS VMTDECOM,
                          '' AS VTXDECOM,
                          '' AS VMTDETEC,
                          '' AS VTXDETEC,
                          '' AS VMTAGRAV,
                          '' AS VTXAGRAV,
                          '' AS VMTPRMTR,
                          '' AS VMTPRLIQ,
                          VMTPRMBR,
                          VTXCOB, --TASA APLICAR A LA COBERTURA
                          VCAPITAL,-- IMPORTE DE CAPITAL ASEGURADO EN EL CERTIFICADO 
                          '' AS VTXCAPIT,
                          '' AS KACTPIDX,
                          '' AS VTXINDX,
                          'LPV' AS DCOMPA,
                          '' AS DMARCA,
                          '' AS TDACECOB,
                          '' AS TDCANCOB,
                          '' AS TDCRICOB,
                          TDRENOVA,
                          '' AS TDVENTRA,
                          '' AS DHORAINI,
                          VMTPREMC,
                          '' AS VMIBOMAT,
                          '' AS VMIBOCOM,
                          '' AS VMIDECOM,
                          '' AS VMIDETEC,
                          '' AS VMIRPMSP,
                          '' AS VMIPRMBR,
                          '' AS VMICOMR,
                          '' AS VMIPRLIQ,
                          '' AS VMICMNQP,
                          '' AS VMIPRMTR,
                          '' AS VMIAGRAV,
                          '' AS KACTIPCB,
                          '' AS VMTCAPLI,
                          '' AS KACTRARE,
                          '' AS KACFMCAL,
                          '' AS DFACMULT,
                          VMTCAPIN, 
                          VMTPREIN,
                          '' AS DINDESES,
                          '' AS DINDMOTO,
                          '' AS KACSALIN,
                          '' AS VMTSALMD,
                          '' AS VTXLMRES,
                          '' AS VTXEQUIP,
                          '' AS VTXPRIOR,
                          '' AS VTXCONTR,
                          '' AS VTXESPEC,
                          '' AS DCAPMORT,
                          VMTPRRES,
                          '' AS DIDADETAR,
                          '' AS DIDADLIMCOBA,
                          KACTPDUR,
                          '' AS KGCRAMO_SAP,
                          '' AS KACTCOMP,
                          '' AS KACINDTX,
                          '' AS KACCALIDA,
                          '' AS DNCABCALP,
                          '' AS DINDNIVEL,
                          DURCOB,
                          '' AS DURPAGCOB,
                          '' AS KACTPDURCB,
                          '' AS DINCOBINDX,
                          '' AS KACGRCBT,
                          '' AS KABTRTAB_2,
                          '' AS VTXAJTBUA,
                          '' AS VMTCAPREM/*,
                          MODULO*/
                          FROM 
                          (
                            SELECT                       
                            COALESCE(CAST(CAST(C."DEFFECDATE" AS DATE ) AS VARCHAR),'') AS TIOCFRM,
                            C."NBRANCH" ||'-'|| C."NPRODUCT" ||'-'|| C."NPOLICY" ||'-'|| C."NCERTIF" AS KABAPOL,
                            COALESCE(COALESCE((SELECT COALESCE(CAST(LC."NCOVERGEN" AS VARCHAR),'0') FROM USVTIMV01."LIFE_COVER" LC 
                            WHERE LC."NBRANCH" = C."NBRANCH" 
                            AND LC."NPRODUCT" = C."NPRODUCT"
                            AND LC."NMODULEC" = C."NMODULEC"
                            AND LC."NCOVER" = C."NCOVER"
                            AND LC."DEFFECDATE" <= (CASE WHEN C."SPOLITYPE" = '1' THEN C."POL_DSTARTDATE" ELSE C."CERT_DSTARTDATE" END)
                            AND (LC."DNULLDATE" IS NULL OR LC."DNULLDATE" > (CASE WHEN C."SPOLITYPE" = '1' THEN C."POL_DSTARTDATE" ELSE C."CERT_DSTARTDATE" END))
                            ), 
                            (
                              select CAST(GLC."NCOVERGEN" as VARCHAR) /*USBI01.IFRS170_V_GEN_LIFE_COVER_VTIMELPG*/
                                from (SELECT GC."NBRANCH",
                                GC."NPRODUCT",
                                GC."NMODULEC",
                                GC."NCOVER",
                                GC."DEFFECDATE",
                                GC."DNULLDATE",
                                GC."NCOVERGEN",
                                GC."NCURRENCY"
                                FROM USVTIMV01."GEN_COVER" GC
                                UNION ALL
                                SELECT LC."NBRANCH",
                                LC."NPRODUCT",
                                LC."NMODULEC",
                                LC."NCOVER",
                                LC."DEFFECDATE",
                                LC."DNULLDATE",
                                LC."NCOVERGEN",
                                LC."NCURRENCY"
                                FROM USVTIMV01."LIFE_COVER" LC)  GLC 
                                WHERE GLC."NBRANCH" = C."NBRANCH" 
                                AND   GLC."NPRODUCT"  = C."NPRODUCT"
                                AND   GLC."NMODULEC"  = C."NMODULEC"
                                AND   GLC."NCOVER"    = C."NCOVER"
                                and   GLC."DNULLDATE" = (SELECT MAX("DNULLDATE")
                                              FROM /*USBI01.IFRS170_V_GEN_LIFE_COVER_VTIMELPG*/
                                              (SELECT GC."NBRANCH",
                                              GC."NPRODUCT",
                                              GC."NMODULEC",
                                              GC."NCOVER",
                                              GC."DEFFECDATE",
                                              GC."DNULLDATE",
                                              GC."NCOVERGEN",
                                              GC."NCURRENCY"
                                              FROM USVTIMV01."GEN_COVER" GC
                                              UNION ALL
                                              SELECT LC."NBRANCH",
                                              LC."NPRODUCT",
                                              LC."NMODULEC",
                                              LC."NCOVER",
                                              LC."DEFFECDATE",
                                              LC."DNULLDATE",
                                              LC."NCOVERGEN",
                                              LC."NCURRENCY"
                                              FROM USVTIMV01."LIFE_COVER" LC)  GLC 
                                              WHERE GLC."NBRANCH" = C."NBRANCH" 
                                              AND   GLC."NPRODUCT"  = C."NPRODUCT"
                                              AND   GLC."NMODULEC"  = C."NMODULEC"
                                              AND   GLC."NCOVER"    = C."NCOVER")
                            )), ('-'||cast(C."NCOVER" as VARCHAR))) AS KGCTPCBT,
                            COALESCE(CAST(CAST(C."DEFFECDATE" AS DATE )AS VARCHAR),'') AS TINICIO,
                            COALESCE(CAST(CAST(C."DNULLDATE" AS DATE )AS VARCHAR),'') AS TTERMO,
                            COALESCE(C."NPREMIUM_O", 0) AS VMTCOMR,
                            COALESCE(C."NPREMIUM_O", 0) AS VMTPRMBR,
                            COALESCE(C."NRATECOVE", 0) AS VTXCOB, --TASA APLICAR A LA COBERTURA
                            COALESCE(CAST(C."NCAPITAL" AS VARCHAR), '0') AS VCAPITAL,
                            COALESCE(CAST(CAST(C."DEFFECDATE" AS DATE )AS VARCHAR),'') AS TDRENOVA,
                            COALESCE((CAST(((SELECT COALESCE (CO."NSHARE", 0)  FROM USVTIMV01."COINSURAN" CO
                            WHERE CO."SCERTYPE" = C."SCERTYPE" 
                            AND CO."NBRANCH" = C."NBRANCH" 
                            AND CO."NPOLICY" = C."NPOLICY"
                            AND CO."NCOMPANY" = 2
                            AND CO."DEFFECDATE"  <= C."DEFFECDATE"
                            AND (CO."DNULLDATE" IS NULL AND CO."DNULLDATE"  > C."DEFFECDATE")
                            ) * C."NPREMIUM") AS VARCHAR)), '100') AS VMTPREMC,
                            COALESCE(C."NCAPITALI", 0)  AS VMTCAPIN, -- IMPORTE DE CAPITAL DE LA COBERTURA A LA FECHA DE EMISION DE LA POLIZA,
                            COALESCE(TRUNC(C."NPREMIUM_O", 2), 0) AS VMTPREIN,
                            COALESCE((COALESCE ((SELECT (SUM(R."NSHARE"/100)) * C."NPREMIUM"  FROM USVTIMV01."REINSURAN" R 
                            WHERE R."SCERTYPE" = C."SCERTYPE"  
                            AND R."NBRANCH"  = C."NBRANCH"
                            AND R."NPRODUCT" = C."NPRODUCT"
                            AND R."NPOLICY"  = C."NPOLICY"
                            AND R."NCERTIF"  = C."NCERTIF"
                            AND R."NMODULEC" = C."NMODULEC"
                            AND R."NCOVER" = C."NCOVER"
                            AND R."DEFFECDATE" <= C."DEFFECDATE"
                            AND (R."DNULLDATE" IS NULL OR R."DNULLDATE" > C."DEFFECDATE")
                            AND R."NTYPE_REIN" <> 1),
                            (SELECT (SUM(R."NSHARE"/100)) * C."NPREMIUM" FROM USVTIMV01."REINSURAN" R 
                            WHERE R."SCERTYPE" = C."SCERTYPE"  
                            AND R."NBRANCH"  = C."NBRANCH"
                            AND R."NPRODUCT" = C."NPRODUCT"
                            AND R."NPOLICY"  = C."NPOLICY"
                            AND R."NCERTIF"  = 0
                            AND R."NMODULEC" = C."NMODULEC"
                            AND R."NCOVER" = C."NCOVER"
                            AND R."DEFFECDATE" <= C."DEFFECDATE"
                            AND (R."DNULLDATE" IS NULL OR R."DNULLDATE" > C."DEFFECDATE")
                            AND R."NTYPE_REIN" <> 1))), 0) AS VMTPRRES,
                            COALESCE(CAST(C."NTYPDURINS" AS VARCHAR),'0') AS KACTPDUR,
                            COALESCE(CAST(C."NDURINSUR" AS VARCHAR),'0') AS DURCOB/*,
                            C."NMODULEC" AS MODULO*/
                            FROM 
                            (                               	   
                              (SELECT
                                    C."SCERTYPE",
                                    C."NBRANCH",
                                    C."NPRODUCT",
                                    C."NPOLICY",
                                    C."NCERTIF",                                    
                                    C."DEFFECDATE",
                                    C."DNULLDATE",
                                    C."NPREMIUM",
                                    C."NPREMIUM_O",
                                    C."NRATECOVE",
                                    C."NCAPITAL",
                                    C."NCAPITALI",
                                    C."NTYPDURINS",
                                    C."NDURINSUR",
                                    C."NMODULEC", 
                                    C."NCOVER",
                                    C."NCURRENCY",
                                    POL."SPOLITYPE",
                                    POL."DSTARTDATE" as "POL_DSTARTDATE",
                                    CERT."DSTARTDATE" as "CERT_DSTARTDATE"
                                    FROM USVTIMV01."COVER" C  
                                    LEFT JOIN USVTIMV01."CERTIFICAT" CERT
                                    ON  C."SCERTYPE"  = CERT."SCERTYPE"  
                                    AND C."NBRANCH"   = CERT."NBRANCH"
                                    AND C."NPRODUCT"  = CERT."NPRODUCT"
                                    AND C."NPOLICY"   = CERT."NPOLICY"
                                    AND C."NCERTIF"   = CERT."NCERTIF"
                                    JOIN USVTIMV01."POLICY" POL
                                    ON  POL."SCERTYPE"  = C."SCERTYPE"
                                    AND POL."NBRANCH"   = C."NBRANCH" 
                                    AND POL."NPRODUCT"  = C."NPRODUCT"
                                    AND POL."NPOLICY"   = C."NPOLICY" 
                                    WHERE POL."SCERTYPE" = '2' 
                                    AND POL."SSTATUS_POL" NOT IN ('2','3') 
                                    -- JAOS
                                    -- C.DCOMPDATE BETWEEN '{p_fecha_inicio}' AND '{p_fecha_fin}')
                                    --AND ((POL."SPOLITYPE" = '1' -- INDIVIDUAL 
                                    --AND POL."DEXPIRDAT" >= '{l_fecha_carga_inicial}' 
                                    --AND (POL."DNULLDATE" IS NULL OR POL."DNULLDATE" > '{l_fecha_carga_inicial}'))
                                    --OR 
                                    --(POL."SPOLITYPE" <> '1' -- COLECTIVAS 
                                    --AND CERT."DEXPIRDAT" >= '{l_fecha_carga_inicial}' 
                                    --AND (CERT."DNULLDATE" IS NULL OR CERT."DNULLDATE" > '{l_fecha_carga_inicial}')))
                                    --AND POL."DSTARTDATE" BETWEEN '{p_fecha_inicio}' AND '{p_fecha_fin}')
                           
                              /*UNION  -- JAOS NO ES NECESARIO EN EL INCREMENTAL 
                              (SELECT
                                    C."SCERTYPE",
                                    C."NBRANCH",
                                    C."NPRODUCT",
                                    C."NPOLICY",
                                    C."NCERTIF",                                    
                                    C."DEFFECDATE",
                                    C."DNULLDATE",
                                    C."NPREMIUM",
                                    C."NPREMIUM_O",
                                    C."NRATECOVE",
                                    C."NCAPITAL",
                                    C."NCAPITALI",
                                    C."NTYPDURINS",
                                    C."NDURINSUR",
                                    C."NMODULEC", 
                                    C."NCOVER",
                                    C."NCURRENCY",
                                    POL."SPOLITYPE",
                                    POL."DSTARTDATE" as "POL_DSTARTDATE",
                                    CERT."DSTARTDATE" as "CERT_DSTARTDATE"
                                    FROM USVTIMV01."COVER" C  
                                    LEFT JOIN USVTIMV01."CERTIFICAT" CERT
                                    ON  C."SCERTYPE"  = CERT."SCERTYPE"  
                                    AND C."NBRANCH"   = CERT."NBRANCH"
                                    AND C."NPRODUCT"  = CERT."NPRODUCT"
                                    AND C."NPOLICY"   = CERT."NPOLICY"
                                    AND C."NCERTIF"   = CERT."NCERTIF"
                                    JOIN USVTIMV01."POLICY" POL
                                    ON  POL."SCERTYPE"  = C."SCERTYPE"
                                    AND POL."NBRANCH"   = C."NBRANCH" 
                                    AND POL."NPRODUCT"  = C."NPRODUCT"
                                    AND POL."NPOLICY"   = C."NPOLICY" 
                                    WHERE POL."SCERTYPE" = '2' 
                                    AND POL."SSTATUS_POL" NOT IN ('2','3')
                                    AND ((POL."SPOLITYPE" = '1' AND (POL."DEXPIRDAT" < '{l_fecha_carga_inicial}' OR POL."DNULLDATE" < '{l_fecha_carga_inicial}')
                                    AND (EXISTS (SELECT 1 FROM USVTIMV01."CLAIM" CLA                                           
                                          JOIN (SELECT DISTINCT CLH."NCLAIM" FROM (SELECT CAST("SVALUE" AS INT4) "SVALUE" FROM USVTIMV01."CONDITION_SERV" CS 
                                                WHERE "NCONDITION" IN (71, 72, 73)) CSV 
                                                JOIN USVTIMV01."CLAIM_HIS" CLH 
                                                ON COALESCE(CLH."NCLAIM", 0) > 0 
                                                AND CLH."NOPER_TYPE" = CSV."SVALUE" 
                                                AND CLH."DOPERDATE" >= '{l_fecha_carga_inicial}') CLH 
                                          ON CLH."NCLAIM" = CLA."NCLAIM"
                                          WHERE CLA."SCERTYPE"  = POL."SCERTYPE" 
                                          AND CLA."NBRANCH"  = POL."NBRANCH" 
                                          AND CLA."NPRODUCT" = POL."NPRODUCT"
                                          AND CLA."NPOLICY"  = POL."NPOLICY"  
                                          AND CLA."NCERTIF"  =  0)))
                                    OR (POL."SPOLITYPE" <> '1' AND (CERT."DEXPIRDAT" < '{l_fecha_carga_inicial}' OR CERT."DNULLDATE" < '{l_fecha_carga_inicial}')
                                    AND ((EXISTS (SELECT 1 FROM USVTIMV01."CLAIM" CLA                                           
                                          JOIN (SELECT DISTINCT CLH."NCLAIM" FROM (SELECT CAST("SVALUE" AS INT4) "SVALUE" FROM USVTIMV01."CONDITION_SERV" CS 
                                                WHERE "NCONDITION" IN (71, 72, 73)) CSV 
                                                JOIN USVTIMV01."CLAIM_HIS" CLH 
                                                ON COALESCE(CLH."NCLAIM", 0) > 0 
                                                AND CLH."NOPER_TYPE" = CSV."SVALUE" 
                                                AND CLH."DOPERDATE" >= '{l_fecha_carga_inicial}') CLH 
                                          ON CLH."NCLAIM" = CLA."NCLAIM"
                                          WHERE CLA."SCERTYPE"  = POL."SCERTYPE" 
                                          AND CLA."NBRANCH"  = POL."NBRANCH" 
                                          AND CLA."NPRODUCT" = POL."NPRODUCT"
                                          AND CLA."NPOLICY"  = POL."NPOLICY"  
                                          AND CLA."NCERTIF"  = CERT."NCERTIF"))))))
                             -- JAOS NO ES NECESARIO EN EL INCREMENTAL.
                              */            
                            ) AS C
                          ) COVER 
                        ) AS TMP
                        '''

  l_df_abcobap_vtime_lpv = glue_context.read.format('jdbc').options(**connection).option("dbtable", l_abcobap_vtime_lpv).load()

  print("ABCOBAP VTIME LPV EXITOSO")

  #-------------------------------------------------------------------------------------------------------------------------------#

  l_abcobap_insis = f'''
                    ( SELECT 
                      'D' AS INDDETREC,
                      'ABCOBAP' AS TABLAIFRS17,
                      '' AS PK,
                      '' AS DTPREG,  --NO
                      '' AS TIOCPROC,--NO
                      CAST(CAST(GRC."INSR_BEGIN" AS DATE)AS VARCHAR) AS TIOCFRM, --BEGIN OF INSURING. puede tambien ser "REGISTRATION_DATE"
                      '' AS TIOCTO,
                      'PNV' AS KGIORIGM,
                      SUBSTRING(CAST(POL."POLICY_ID" AS VARCHAR),6,12)  AS KABAPOL,
                      GRC."INSURED_OBJ_ID" ||'-'|| GRC."ANNEX_ID"  AS KABUNRIS,
                      (SELECT 
                      CASE WHEN "COVER_CPR_ID" < 1000000 THEN CAST(ROUND("COVER_CPR_ID",0) AS VARCHAR) 
                      ELSE SUBSTRING(CAST(ROUND("COVER_CPR_ID",0) AS VARCHAR),5)
                      END 
                      FROM USINSIV01."CPR_COVER" CC WHERE CC."COVER_TYPE" = GRC."COVER_TYPE" ) AS KGCTPCBT,
                      CAST(CAST(GRC."INSR_BEGIN" AS DATE) AS VARCHAR) AS TINICIO,
                      CAST(CAST(GRC."INSR_END" AS DATE)AS VARCHAR)  AS TTERMO,
                      '' AS TSITCOB,
                      '' AS KACSITCB,
                      '' AS VMTPRMSP,
                      TRUNC(GRC."PREMIUM", 2) AS VMTCOMR,
                      '' AS VMTBOMAT,
                      '' AS VTXBOMAT,
                      '' AS VMTBOCOM,
                      '' AS VTXBOCOM,
                      '' AS VMTDECOM,
                      '' AS VTXDECOM,
                      '' AS VMTDETEC,
                      '' AS VTXDETEC,
                      '' AS VMTAGRAV,
                      '' AS VTXAGRAV,
                      '' AS VMTPRMTR,
                      '' AS VMTPRLIQ,
                      TRUNC(GRC."PREMIUM", 2) AS VMTPRMBR,
                      TRUNC(GRC."TARIFF_PERCENT", 9) AS VTXCOB,
                      CAST(TRUNC(GRC."INSURED_VALUE", 2) AS VARCHAR) AS VCAPITAL,
                      '' AS VTXCAPIT, --EN BLANCO
                      '' AS KACTPIDX, --NO
                      '' AS VTXINDX,  --EN BLANCO
                      'LPV' AS DCOMPA,
                      '' AS DMARCA,   --NO  
                      '' AS TDACECOB, --NO
                      '' AS TDCANCOB, --NO
                      '' AS TDCRICOB, --NO
                      CAST(CAST(GRC."INSR_BEGIN" AS DATE)AS VARCHAR) AS TDRENOVA,
                      '' AS TDVENTRA, --NO
                      '' AS DHORAINI, --NO
                      '' AS VMTPREMC, --PENDIENTE
                      '' AS VMIBOMAT, --NO
                      '' AS VMIBOCOM, --NO
                      '' AS VMIDECOM, --NO
                      '' AS VMIDETEC, --NO
                      '' AS VMIRPMSP, --NO
                      '' AS VMIPRMBR, --NO
                      '' AS VMICOMR,  --NO
                      '' AS VMIPRLIQ, --NO
                      '' AS VMICMNQP, --NO
                      '' AS VMIPRMTR, --NO
                      '' AS VMIAGRAV, --NO
                      '' AS KACTIPCB, --EN BLANCO
                      '' AS VMTCAPLI, --EN BLANCO
                      '' AS KACTRARE, --EN BLANCO
                      '' AS KACFMCAL, --EN BLANCO
                      '' AS DFACMULT, --NO
                      TRUNC(GRC."INSURED_VALUE", 0)  AS VMTCAPIN,
                      TRUNC(GRC."ANNUAL_PREMIUM", 0) AS VMTPREIN,
                      '' AS DINDESES,    --NO
                      '' AS DINDMOTO,    --NO
                      '' AS KACSALIN,    --NO
                      '' AS VMTSALMD,    --NO
                      '' AS VTXLMRES,    --EN BLANCO
                      '' AS VTXEQUIP,    --NO
                      '' AS VTXPRIOR,    --NO
                      '' AS VTXCONTR,    --NO
                      '' AS VTXESPEC,    --NO
                      '' AS DCAPMORT,    --NO
                      0 AS VMTPRRES,    --PENDIENTE
                      '' AS DIDADETAR,   --EN BLANCO
                      '' AS DIDADLIMCOBA,--EN BLANCO
                      '' AS KACTPDUR,    --EN BLANCO
                      '' AS KGCRAMO_SAP, --NO
                      '' AS KACTCOMP,    --NO
                      '' AS KACINDTX,    --EN BLANCO
                      '' AS KACCALIDA,   --EN BLANCO
                      '' AS DNCABCALP,   --EN BLANCO
                      '' AS DINDNIVEL,   --NO
                      '' AS DURCOB,      --EN BLANCO
                      '' AS DURPAGCOB,   --EN BLANCO
                      '' AS KACTPDURCB,  --NO
                      '' AS DINCOBINDX,  --NO
                      '' AS KACGRCBT,    --NO
                      '' AS KABTRTAB_2,  --NO
                      '' AS VTXAJTBUA,   --NO
                      '' AS VMTCAPREM   --NO
                      FROM USINSIV01."GEN_RISK_COVERED" GRC
                      JOIN USINSIV01."POLICY" POL ON POL."POLICY_ID" = GRC."POLICY_ID" AND POL."INSR_TYPE" = GRC."INSR_TYPE"
                      WHERE --JAOS 
                            GRC."REGISTRATION_DATE" BETWEEN '{p_fecha_inicio}' AND '{p_fecha_fin} 
                            --(P."INSR_END" >= '{l_fecha_carga_inicial}'
                              --      OR  (P."INSR_END" < '{l_fecha_carga_inicial}' AND EXISTS (
                              --   	                              		           SELECT 1 FROM USINSIV01."CLAIM" C
                              --    	                              		           JOIN USINSIV01."CLAIM_OBJECTS" CO ON CO."CLAIM_ID" = C."CLAIM_ID" AND CO."POLICY_ID" = C."POLICY_ID"
                              --      	                              		           JOIN USINSIV01."CLAIM_RESERVE_HISTORY" CRH ON CO."CLAIM_ID" = CRH."CLAIM_ID" AND CO."REQUEST_ID" = CO."REQUEST_ID" AND CO."CLAIM_OBJ_SEQ" = CRH."CLAIM_OBJECT_SEQ"
                              --      	                              		           WHERE C."POLICY_ID" = P."POLICY_ID"
                              --	                              		           AND "OP_TYPE" IN ('REG','EST','CLC','PAYMCONF','PAYMINV')
                              --	                              		           AND CAST(CRH."REGISTRATION_DATE" AS DATE) >= '{l_fecha_carga_inicial}'
                              --	                                                    )
                              --        )
                           -- )
                      /*AND POL."REGISTRATION_DATE" BETWEEN '{p_fecha_inicio}' AND '{p_fecha_fin}'*/
                   ) AS TMP
                    '''
    
    #EJECUTAR CONSULTA
  l_df_abcobap_insis = glue_context.read.format('jdbc').options(**connection).option("dbtable",l_abcobap_insis).load()

  print("ABCOBAP INSIS EXITOSO")

  #PERFORM THE UNION OPERATION
  l_df_abcobap = l_df_abcobap_insunix_lpg.union(l_df_abcobap_insunix_lpv).union(l_df_abcobap_vtime_lpg).union(l_df_abcobap_vtime_lpv).union(l_df_abcobap_insis)

  l_df_abcobap = l_df_abcobap.withColumn("VMTCOMR", col("VMTCOMR").cast(DecimalType(12, 2))).withColumn("VMTPRMBR", col("VMTPRMBR").cast(DecimalType(12, 2))).withColumn("VTXCOB", format_number("VTXCOB",9)).withColumn("VCAPITAL", col("VCAPITAL").cast(DecimalType(14, 2))).withColumn("VTXCAPIT", col("VTXCAPIT").cast(DecimalType(9, 5))).withColumn("VTXINDX", col("VTXINDX").cast(DecimalType(7, 4))).withColumn("VMTPREMC", col("VMTPREMC").cast(DecimalType(12, 2))).withColumn("VMTCAPLI", col("VMTCAPLI").cast(DecimalType(14, 2))).withColumn("VMTCAPIN", col("VMTCAPIN").cast(DecimalType(14, 2))).withColumn("VMTPREIN", col("VMTPREIN").cast(DecimalType(14, 2))).withColumn("VTXLMRES", col("VTXLMRES").cast(DecimalType(7, 4))).withColumn("VMTPRRES", col("VMTPRRES").cast(DecimalType(12, 2))).withColumn("VTXAJTBUA", col("VTXAJTBUA").cast(DecimalType(9, 4))).withColumn("VMTCAPREM", col("VMTCAPREM").cast(DecimalType(12, 2)))
  
  return l_df_abcobap