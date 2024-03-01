from pyspark.sql.types import DecimalType
from pyspark.sql.functions import col, format_number

def get_data(glue_context, connection, p_fecha_inicio, p_fecha_fin):

  l_abcobap_insunix_lpg = f'''
                        (
                          SELECT 
                          'D' AS INDDETREC,
                          'ABCOBAP' AS TABLAIFRS17,
                          '' AS PK,
                          '' AS DTPREG,
                          '' AS TIOCPROC,
                          TIOCFRM,
                          '' AS TIOCTO,
                          'PIG' AS KGIORIGM,
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
                          '' AS KACTRARE,
                          '' AS KACFMCAL,
                          '' AS DFACMULT,
                          VMTCAPIN,                     
                                      VMTPREIN,
                          '' AS KABTRTAB,
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
                          '' AS DIDADLIMCOB,
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
                          --,MODULO                                                   
                          FROM(
                              SELECT 
                              COALESCE (CAST(C.EFFECDATE AS VARCHAR),'')  AS TIOCFRM,
                              CAST(COALESCE(C.BRANCH, 0) AS VARCHAR) ||'-'|| POL.PRODUCT || '-' || COALESCE(PSP.SUB_PRODUCT, 0) || '-' || COALESCE(C.POLICY, 0) || '-' || COALESCE(CERT.CERTIF, 0) AS KABAPOL,
                              COALESCE(( SELECT CAST(COALESCE(GC.COVERGEN, 0) AS VARCHAR) || '-' || COALESCE(GC.CURRENCY, 0) FROM USINSUG01.GEN_COVER GC 
                              WHERE GC.USERCOMP = C.USERCOMP 
                              AND GC.COMPANY    = C.COMPANY 
                              AND GC.BRANCH     = C.BRANCH 
                              AND GC.PRODUCT    = POL.PRODUCT
                              AND GC.CURRENCY   = C.CURRENCY
                              AND GC.MODULEC    = C.MODULEC
                              AND GC.COVER      = C.COVER 
                              AND GC.EFFECDATE <= (CASE WHEN POL.POLITYPE = '1' THEN POL.EFFECDATE ELSE CERT.EFFECDATE END)
                              AND (GC.NULLDATE IS NULL OR GC.NULLDATE > (CASE WHEN POL.POLITYPE = '1' THEN POL.EFFECDATE ELSE CERT.EFFECDATE END)) LIMIT 1
                              ) ,'0') AS KGCTPCBT,
                              COALESCE (CAST(C.EFFECDATE AS VARCHAR),'')  AS TINICIO,
                              COALESCE (CAST(C.NULLDATE  AS VARCHAR),'') AS TTERMO,
                              COALESCE(C.PREMIUM, 0) AS VMTCOMR,
                              COALESCE(C.PREMIUM,0)  AS VMTPRMBR,
                              COALESCE(C.RATECOVE, 0) AS VTXCOB,
                              COALESCE(CAST(C.CAPITAL AS VARCHAR),'0') AS VCAPITAL,
                              COALESCE(CAST (C.EFFECDATE AS VARCHAR),'') AS TDRENOVA,
                              COALESCE(CAST(((SELECT COALESCE(CO.SHARE, 0)
                                              FROM USINSUG01.COINSURAN CO
                                              WHERE CO.USERCOMP = C.USERCOMP 
                                              AND CO.COMPANY = C.COMPANY 
                                              AND CO.CERTYPE = C.CERTYPE
                                              AND CO.BRANCH  = C.BRANCH 
                                              AND CO.POLICY  = C.POLICY
                                              AND CO.COMPANYC = 1
                                              AND CO.EFFECDATE <= C.EFFECDATE
                                              AND (CO.NULLDATE IS NULL OR CO.NULLDATE > C.EFFECDATE)) * C.PREMIUM) AS VARCHAR), '100') AS VMTPREMC,
                              COALESCE(C.CAPITALI, 0) AS VMTCAPIN,                       
                                        COALESCE((SELECT COV.PREMIUM FROM USINSUG01.COVER COV 
                                        LEFT JOIN USINSUG01.CERTIFICAT CERT                           
                                        ON COV.USERCOMP =  CERT.USERCOMP 
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
                                        (SELECT COV.PREMIUM FROM USINSUG01.COVER COV 
                                        LEFT JOIN USINSUG01.CERTIFICAT CERT                           
                                        ON  COV.USERCOMP = CERT.USERCOMP 
                                        AND COV.COMPANY  = CERT.COMPANY  
                                        AND COV.CERTYPE  = CERT.CERTYPE 
                                        AND COV.BRANCH   = CERT.BRANCH 
                                        AND COV.POLICY   = CERT.POLICY
                                        AND COV.CERTIF   = CERT.CERTIF
                                        AND COV.CURRENCY = C.CURRENCY 
                                        AND COV.COVER    = C.COVER
                                        AND COV.MODULEC  = C.MODULEC
                                        LEFT JOIN USINSUG01.POLICY POL
                                        ON  POL.USERCOMP = CERT.USERCOMP 
                                        AND POL.COMPANY  = CERT.COMPANY  
                                        AND POL.CERTYPE  = CERT.CERTYPE
                                        AND POL.BRANCH   = CERT.BRANCH 
                                        AND POL.POLICY   = CERT.POLICY                                    
                                        AND COV.EFFECDATE <= POL.DATE_ORIGI
                                        AND (COV.NULLDATE IS NULL OR COV.NULLDATE > POL.DATE_ORIGI) LIMIT 1)) AS VMTPREIN,
                                        COALESCE((COALESCE ((SELECT (SUM(R.SHARE/100)) * C.PREMIUM  FROM USINSUG01.REINSURAN R 
                                                    WHERE R.USERCOMP = C.USERCOMP 
                                                    AND R.COMPANY = C.COMPANY 
                                                    AND R.CERTYPE = C.CERTYPE  
                                                    AND R.BRANCH = C.BRANCH
                                                    AND R.POLICY = C.POLICY
                                                    AND R.CERTIF = C.CERTIF 
                                                    AND R.EFFECDATE <= C.EFFECDATE
                                                    AND (R.NULLDATE IS NULL OR R.NULLDATE > C.EFFECDATE)
                                                    AND R.TYPE <> 1),
                                                    (SELECT (SUM(R.SHARE/100)) * C.PREMIUM  FROM USINSUG01.REINSURAN R 
                                                    WHERE R.USERCOMP = C.USERCOMP 
                                                    AND R.COMPANY = C.COMPANY 
                                                    AND R.CERTYPE = C.CERTYPE  
                                                    AND R.BRANCH = C.BRANCH
                                                    AND R.POLICY = C.POLICY
                                                    AND R.CERTIF = 0
                                                    AND R.EFFECDATE <= C.EFFECDATE 
                                                    AND (R.NULLDATE IS NULL OR R.NULLDATE > C.EFFECDATE)
                                                    AND R.TYPE <> 1))), 0) AS VMTPRRES
                              --,C.MODULEC AS MODULO 
                              from USINSUG01.COVER C   
                              LEFT JOIN USINSUG01.CERTIFICAT CERT 
                              ON C.USERCOMP = CERT.USERCOMP  
                              AND C.COMPANY = CERT.COMPANY  
                              AND C.CERTYPE = CERT.CERTYPE 
                              AND C.BRANCH = CERT.BRANCH  
                              AND C.POLICY = CERT.POLICY 
                              AND C.CERTIF = CERT.CERTIF
                              JOIN USINSUG01.POLICY POL 
                              ON  POL.USERCOMP = C.USERCOMP 
                              AND POL.COMPANY = C.COMPANY 
                              AND POL.CERTYPE = C.CERTYPE 
                              AND POL.BRANCH = C.BRANCH 
                              AND POL.POLICY = C.POLICY
                              JOIN USINSUG01.POL_SUBPRODUCT PSP 
                              ON  PSP.USERCOMP = POL.USERCOMP 
                              AND PSP.COMPANY = POL.COMPANY 
                              AND PSP.CERTYPE = POL.CERTYPE 
                              AND PSP.BRANCH = POL.BRANCH 
                              AND PSP.PRODUCT  = POL.PRODUCT 
                              AND PSP.POLICY   = POL.POLICY 
                              WHERE C.CERTYPE  = '2' AND POL.STATUS_POL NOT IN ('2','3') 
                              and CAST(c.compdate AS DATE) between '{p_fecha_inicio}' and '{p_fecha_fin}'
                              ) C          
                        ) AS T
                        '''

  l_df_abcobap_insunix_lpg = glue_context.read.format('jdbc').options(**connection).option("dbtable", l_abcobap_insunix_lpg).load()

  print("ABCOBAP INSUNIX LPG EXITOSO")

  l_abcobap_insunix_lpv = f'''
                          (
                            select 
                              'd' as inddetrec,
                              'abcobap' as tablaifrs17,
                              '' as pk,
                              '' as dtpreg,
                              '' as tiocproc,
                              tiocfrm,
                              '' as tiocto,
                              'piv' as kgiorigm,
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
                              'lpv' as dcompa,
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
                              '' as kabtrtab,
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
                              '' as didadlimcob,
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
                              --,modulec
                              from(   select 
                                      coalesce (cast(c.effecdate as varchar),'')  as tiocfrm,
                                      coalesce(c.branch, 0) || '-'|| c.product || '-' ||  coalesce(c.policy, 0)|| '-' || coalesce(c.certif, 0)  as kabapol,
                                      '' as kabunris,
                                      coalesce(coalesce(   (select 	coalesce(gco.covergen, 0) || '-' || coalesce(gco.currency, 0) || '-g' 
                                                            from 	usinsug01.gen_cover gco
                                                            where	gco.usercomp = 1
                                                            and	gco.company    = 1
                                                            and	gco.branch     = c.branch
                                                            and	gco.product    = c.product
                                                            and gco.currency = c.currency
                                                            and gco.modulec  = c.modulec
                                                            and gco.cover    = c.cover
                                                            and gco.effecdate <= c.effecdate
                                                            and (gco.nulldate is null or gco.nulldate > c.effecdate)
                                                            and (( select distinct pro.brancht
                                                                    from usinsuv01.product pro
                                                                    where pro.usercomp = gco.usercomp 
                                                                    and pro.company    = gco.company 
                                                                    and pro.branch     = gco.branch 
                                                                    and pro.product    = gco.product 
                                                                  and pro.nulldate is null)) not in ('1', '5') limit 1), 
                                                          ( select 	coalesce(lco.covergen, 0) || '-' || coalesce(lco.currency, 0) || '-l' 
                                                            from 	usinsug01.life_cover lco
                                                            where	lco.usercomp = 1
                                                            and	lco.company    = 1
                                                            and	lco.branch     = c.branch
                                                            and	lco.product    = c.product
                                                            and lco.currency   = c.currency
                                                            and lco.cover      = c.cover
                                                            and lco.effecdate <= c.effecdate
                                                            and (lco.nulldate is null or lco.nulldate > c.effecdate)
                                                            and ((  select distinct pro.brancht
                                                                          from usinsuv01.product pro
                                                                          where pro.usercomp = lco.usercomp 
                                                                          and pro.company    = lco.company 
                                                                          and pro.branch     = lco.branch 
                                                                          and pro.product    = lco.product 
                                                                          and pro.nulldate is null) in ('1', '5')) limit 1 )), cast(c.cover as varchar)) as kgctpcbt,
                                      coalesce (cast(c.effecdate as varchar),'')  as tinicio,
                                      coalesce (cast(c.nulldate as varchar),'') as ttermo,
                                      coalesce(c.premium, 0) as vmtcomr,
                                      coalesce(c.premium,  0) as vmtprmbr,
                                      coalesce(c.ratecove, 0) as vtxcob, --tasa aplicar a la cobertura
                                      coalesce(cast(c.capital as varchar), '0') as vcapital,-- importe de capital asegurado en el certificado                         
                                      'lpv' as dcompa,
                                      '' as dmarca,
                                      '' as tdacecob,
                                      '' as tdcancob,
                                      '' as tdcricob,
                                      coalesce(cast (c.effecdate as varchar),'') as tdrenova,
                                      '' as tdventra,
                                      '' as dhoraini,
                                      coalesce(cast(( (select coalesce(co.share, 0) 
                                                      from usinsuv01.coinsuran co
                                                      where co.usercomp = c.usercomp 
                                                      and co.company = c.company 
                                                      and co.certype = c.certype
                                                      and co.branch = c.branch 
                                                      and co.policy = c.policy
                                                      and co.companyc = 12
                                                      and co.effecdate <= c.effecdate
                                                      and (co.nulldate is null or co.nulldate > c.effecdate)) * c.premium) as varchar), '100') as vmtpremc,                       
                                      coalesce(c.capitali, 0) as vmtcapin,
                                      coalesce((select cov.premium from usinsuv01.cover cov 
                                                left join usinsuv01.certificat cert                           
                                                on cov.usercomp = cert.usercomp 
                                                and cov.company  = cert.company  
                                                and cov.certype  = cert.certype 
                                                and cov.branch   = cert.branch 
                                                and cov.policy   = cert.policy
                                                and cov.certif   = cert.certif
                                                and cov.currency = c.currency 
                                                and cov.cover    = c.cover
                                                and cov.modulec  = c.modulec
                                                and cov.effecdate <= cert.date_origi
                                                and (cov.nulldate is null or cov.nulldate > cert.date_origi) limit 1),
                                                (select cov.premium from usinsuv01.cover cov 
                                                left join usinsuv01.certificat cert                           
                                                on  cov.usercomp = cert.usercomp 
                                                and cov.company  = cert.company  
                                                and cov.certype  = cert.certype 
                                                and cov.branch   = cert.branch 
                                                and cov.policy   = cert.policy
                                                and cov.certif   = cert.certif
                                                and cov.currency = c.currency 
                                                and cov.cover    = c.cover
                                                and cov.modulec  = c.modulec
                                                left join usinsuv01.policy pol
                                                on  pol.usercomp = cert.usercomp 
                                                and pol.company  = cert.company  
                                                and pol.certype  = cert.certype
                                                and pol.branch   = cert.branch 
                                                and pol.policy   = cert.policy                                    
                                                and cov.effecdate <= pol.date_origi
                                                and (cov.nulldate is null or cov.nulldate > pol.date_origi) limit 1)) as vmtprein,
                                      coalesce((coalesce ((select (sum(r.share/100)) * c.premium  
                                                          from usinsuv01.reinsuran r 
                                                          where r.usercomp = c.usercomp 
                                                          and r.company = c.company 
                                                          and r.certype = c.certype  
                                                          and r.branch = c.branch
                                                          and r.policy = c.policy
                                                          and r.certif = c.certif 
                                                          and r.effecdate <= c.effecdate
                                                          and (r.nulldate is null or r.nulldate > c.effecdate)
                                                          and r.type <> 1),
                                                          (select (sum(r.share/100)) * c.premium  from usinsuv01.reinsuran r 
                                                            where r.usercomp = c.usercomp 
                                                            and r.company = c.company 
                                                            and r.certype = c.certype  
                                                            and r.branch = c.branch
                                                            and r.policy = c.policy
                                                            and r.certif = 0
                                                            and r.effecdate <= c.effecdate
                                                            and (r.nulldate is null or r.nulldate > c.effecdate)
                                                            and r.type <> 1))), 0) as vmtprres
                                      --,c.modulec
                                      from 
                                      ( 
                                        select
                                        c.usercomp, c.company, c.certype, c.branch, c.currency, c.cover, c.effecdate,        
                                        c.nulldate,c.policy, c.certif, c.premium, c.ratecove, c.capital, c.capitali, c.modulec,
                                        pol.product,pol.politype,pol.effecdate as effecdate_pol,cert.effecdate as effecdate_cert,
                                        case
                                        when pol.politype = '1' --individual
                                        then 
                                            case
                                            when (c.effecdate <= pol.effecdate and
                                                (c.nulldate is null or
                                                  c.nulldate > pol.effecdate)) then 1
                                            else 
                                                case
                                                when exists (select 1
                                                          from usinsuv01.cover cov1
                                                          where cov1.certype = c.certype
                                                          and cov1.usercomp = c.usercomp
                                                          and cov1.company = c.company
                                                          and cov1.branch = c.branch
                                                          --and 	cov1.product  = c.product
                                                          and cov1.modulec  = c.modulec
                                                          and cov1.policy   = c.policy
                                                          and cov1.certif   = c.certif
                                                          and cov1.currency = c.currency
                                                          and cov1.cover    = c.cover
                                                          and cov1.effecdate <= pol.effecdate
                                                          and (cov1.nulldate is null
                                                          or cov1.nulldate > pol.effecdate)) then 0
                                                else 
                                                    case
                                                    when c.nulldate = (select max(cov1.nulldate)
                                                                    from usinsuv01.cover cov1
                                                                    where cov1.usercomp = c.usercomp
                                                                    and cov1.certype = c.certype
                                                                    and cov1.company = c.company
                                                                    and cov1.branch = c.branch
                                                                    --and 	  cov1.product  = c.product
                                                                    and cov1.modulec = c.modulec
                                                                    and cov1.policy = c.policy
                                                                    and cov1.certif = c.certif
                                                                    and cov1.currency = c.currency
                                                                    and cov1.cover = c.cover) then 1
                                                    else 0
                                                    end
                                                end
                                            end
                                            else 
                                                case
                                                when (c.effecdate <= cert.effecdate and (c.nulldate is null or c.nulldate > cert.effecdate)) then 1
                                                else 
                                                case
                                                when exists (select  1
                                                            from usinsuv01.cover cov1
                                                            where cov1.certype = c.certype
                                                            and cov1.usercomp = c.usercomp
                                                            and cov1.company = c.company
                                                            and cov1.branch = c.branch
                                                            --and 	cov1.product  = c.product
                                                            and cov1.modulec = c.modulec
                                                            and cov1.policy = c.policy
                                                            and cov1.certif = c.certif
                                                            and cov1.currency = c.currency
                                                            and cov1.cover = c.cover
                                                            and cov1.effecdate <= cert.effecdate
                                                            and (cov1.nulldate is null
                                                            or cov1.nulldate > cert.effecdate)) then 0
                                                else case
                                                    when c.nulldate = (select
                                                        max(cov1.nulldate)
                                                      from usinsuv01.cover cov1
                                                      where cov1.usercomp = c.usercomp
                                                      and cov1.certype = c.certype
                                                      and cov1.company = c.company
                                                      and cov1.branch = c.branch
                                                      --and 	  cov1.product  = c.product
                                                      and cov1.modulec = c.modulec
                                                      and cov1.policy = c.policy
                                                      and cov1.certif = c.certif
                                                      and cov1.currency = c.currency
                                                      and cov1.cover = c.cover) then 1
                                                    else 0
                                                end
                                                end
                                            end
                                        end flag
                                        from usinsuv01.cover c
                                        left join usinsuv01.certificat cert on c.usercomp = cert.usercomp and c.company = cert.company and c.certype = cert.certype and c.branch = cert.branch and c.policy = cert.policy and c.certif = cert.certif
                                        join usinsuv01.policy pol on pol.usercomp = c.usercomp and pol.company = c.company and pol.certype = c.certype and pol.branch = c.branch and pol.policy = c.policy
                                        where c.certype = '2'
                                        and pol.status_pol not in ('2', '3')
                                        and CAST(c.compdate AS DATE) between '{p_fecha_inicio}' and '{p_fecha_fin}'
                                      ) c 
                                  where c.flag = 1) t
                          ) AS TMP '''
 
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
                            '' AS KABTRTAB,
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
                            '' AS DIDADLIMCOB,
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
                                  COALESCE( COALESCE((SELECT CAST(GLC."NCOVERGEN" AS VARCHAR)
                                                      FROM USBI01."ifrs170_v_gen_life_cover_vtimelpg" GLC
                                                        WHERE GLC."NBRANCH"   = C."NBRANCH" 
                                                        AND   GLC."NPRODUCT"  = C."NPRODUCT"
                                                        AND   GLC."NMODULEC"  = C."NMODULEC"
                                                        AND   GLC."NCOVER"    = C."NCOVER"
                                                        AND   GLC."DEFFECDATE" <= (CASE WHEN C."SPOLITYPE" = '1' THEN C."POL_DSTARTDATE" ELSE C."CERT_DSTARTDATE" END) 
                                                        AND   (GLC."DNULLDATE" IS NULL OR GLC."DNULLDATE" > (CASE WHEN C."SPOLITYPE" = '1' THEN C."POL_DSTARTDATE" ELSE C."CERT_DSTARTDATE" END))), ( SELECT CAST(GLC."NCOVERGEN" AS VARCHAR)
                                                                                                                                                                                                      FROM  USBI01."ifrs170_v_gen_life_cover_vtimelpg"  GLC 
                                                                                                                                                                                                      WHERE GLC."NBRANCH"   = C."NBRANCH" 
                                                                                                                                                                                                      AND   GLC."NPRODUCT"  = C."NPRODUCT"
                                                                                                                                                                                                      AND   GLC."NMODULEC"  = C."NMODULEC"
                                                                                                                                                                                                      AND   GLC."NCOVER"    = C."NCOVER"
                                                                                                                                                                                                      AND   GLC."DNULLDATE" = ( SELECT MAX("DNULLDATE") FROM USBI01."ifrs170_v_gen_life_cover_vtimelpg" GLC
                                                                                                                                                                                                                                WHERE GLC."NBRANCH"   = C."NBRANCH" 
                                                                                                                                                                                                                                AND   GLC."NPRODUCT"  = C."NPRODUCT"
                                                                                                                                                                                                                                AND   GLC."NMODULEC"  = C."NMODULEC"
                                                                                                                                                                                                                                AND   GLC."NCOVER"    = C."NCOVER"))), ('-'||CAST(C."NCOVER" AS VARCHAR))) AS KGCTPCBT,
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
                                    SELECT
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
                                      AND   cast(POL."DCOMPDATE" as date) between '{p_fecha_inicio}' and '{p_fecha_fin}'
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
                          '' AS KABTRTAB,
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
                          '' AS DIDADLIMCOB,
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
                              select CAST(GLC."NCOVERGEN" as VARCHAR)
                                from USBI01."ifrs170_v_gen_life_cover_vtimelpg" GLC 
                                WHERE GLC."NBRANCH" = C."NBRANCH" 
                                AND   GLC."NPRODUCT"  = C."NPRODUCT"
                                AND   GLC."NMODULEC"  = C."NMODULEC"
                                AND   GLC."NCOVER"    = C."NCOVER"
                                and   GLC."DNULLDATE" = (SELECT MAX("DNULLDATE")
                                              FROM USBI01."ifrs170_v_gen_life_cover_vtimelpg" GLC
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
                              SELECT
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
                                    and cast(C."DCOMPDATE" as date) between '{p_fecha_inicio}' and '{p_fecha_fin}'
                            ) AS C
                          ) COVER 
                        ) AS TMP
                        '''

  l_df_abcobap_vtime_lpv = glue_context.read.format('jdbc').options(**connection).option("dbtable", l_abcobap_vtime_lpv).load()

  print("ABCOBAP VTIME LPV EXITOSO")

  #-------------------------------------------------------------------------------------------------------------------------------#

  l_abcobap_insis = f'''
                    (
                      SELECT 
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
                      (SELECT SUBSTRING(CAST(CAST("COVER_CPR_ID" AS BIGINT) AS VARCHAR), 5, 10) FROM USINSIV01."CPR_COVER" CC WHERE CC."COVER_TYPE" = GRC."COVER_TYPE" ) AS KGCTPCBT,
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
                      '' AS KABTRTAB,
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
                      '' AS DIDADLIMCOB,--EN BLANCO
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
                      where GRC."REGISTRATION_DATE" BETWEEN '{p_fecha_inicio}' AND '{p_fecha_fin}
                    ) AS TMP
                    '''
    
    #EJECUTAR CONSULTA
  l_df_abcobap_insis = glue_context.read.format('jdbc').options(**connection).option("dbtable",l_abcobap_insis).load()

  print("ABCOBAP INSIS EXITOSO")

  #PERFORM THE UNION OPERATION
  l_df_abcobap = l_df_abcobap_insunix_lpg.union(l_df_abcobap_insunix_lpv).union(l_df_abcobap_vtime_lpg).union(l_df_abcobap_vtime_lpv).union(l_df_abcobap_insis)

  l_df_abcobap = l_df_abcobap.withColumn("VMTCOMR", col("VMTCOMR").cast(DecimalType(12, 2))).withColumn("VMTPRMBR", col("VMTPRMBR").cast(DecimalType(12, 2))).withColumn("VTXCOB", format_number("VTXCOB",9)).withColumn("VCAPITAL", col("VCAPITAL").cast(DecimalType(14, 2))).withColumn("VTXCAPIT", col("VTXCAPIT").cast(DecimalType(9, 5))).withColumn("VTXINDX", col("VTXINDX").cast(DecimalType(7, 4))).withColumn("VMTPREMC", col("VMTPREMC").cast(DecimalType(12, 2))).withColumn("VMTCAPLI", col("VMTCAPLI").cast(DecimalType(14, 2))).withColumn("VMTCAPIN", col("VMTCAPIN").cast(DecimalType(14, 2))).withColumn("VMTPREIN", col("VMTPREIN").cast(DecimalType(14, 2))).withColumn("VTXLMRES", col("VTXLMRES").cast(DecimalType(7, 4))).withColumn("VMTPRRES", col("VMTPRRES").cast(DecimalType(12, 2))).withColumn("VTXAJTBUA", col("VTXAJTBUA").cast(DecimalType(9, 4))).withColumn("VMTCAPREM", col("VMTCAPREM").cast(DecimalType(12, 2)))
  
  return l_df_abcobap