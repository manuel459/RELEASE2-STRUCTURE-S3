from pyspark.sql.types import DecimalType
from pyspark.sql.functions import col, format_number

def get_data(glue_context, connection, p_fecha_inicio, p_fecha_fin):

  l_fecha_carga_inicial = '2021-12-31'

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
                          '' AS KABTRTAB_2,
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
                          --,MODULO                                                   
                          FROM(
                          	   SELECT 
                               COALESCE (CAST(C.EFFECDATE AS VARCHAR),'')  AS TIOCFRM,
                               CAST(COALESCE(C.BRANCH, 0) AS VARCHAR) ||'-'|| C.POL_PRODUCT || '-' || COALESCE(C.SUB_PRODUCT, 0) || '-' || COALESCE(C.POLICY, 0) || '-' || COALESCE(C.CERTIF, 0) AS KABAPOL,
                               COALESCE(( SELECT CAST(COALESCE(GC.COVERGEN, 0) AS VARCHAR) || '-' || COALESCE(GC.CURRENCY, 0) FROM USINSUG01.GEN_COVER GC 
                               WHERE GC.USERCOMP = C.USERCOMP 
                               AND GC.COMPANY    = C.COMPANY 
                               AND GC.BRANCH     = C.BRANCH 
                               AND GC.PRODUCT    = C.POL_PRODUCT
                               AND GC.CURRENCY   = C.CURRENCY
                               AND GC.MODULEC    = C.MODULEC
                               AND GC.COVER      = C.COVER 
                               AND GC.EFFECDATE <= (CASE WHEN C.POL_POLITYPE = '1' THEN C.POL_EFFECDATE ELSE C.CERT_EFFECDATE END)
                               AND (GC.NULLDATE IS NULL OR GC.NULLDATE > (CASE WHEN C.POL_POLITYPE = '1' THEN C.POL_EFFECDATE ELSE C.CERT_EFFECDATE END)) LIMIT 1
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
                                         AND COV.EFFECDATE <= C.CERT_DATE_ORIGI
                                         AND (COV.NULLDATE IS NULL OR COV.NULLDATE > C.CERT_DATE_ORIGI) LIMIT 1),
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
                                         AND COV.EFFECDATE <= C.POL_DATE_ORIGI
                                         AND (COV.NULLDATE IS NULL OR COV.NULLDATE > C.POL_DATE_ORIGI) LIMIT 1)) AS VMTPREIN,
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
                               FROM
                               (
                                 (SELECT                                  
                                  POL.POLICY AS POL_POLICY, POL.PRODUCT AS POL_PRODUCT, PSP.SUB_PRODUCT, POL.EFFECDATE AS POL_EFFECDATE, POL.POLITYPE AS POL_POLITYPE, POL.DATE_ORIGI as POL_DATE_ORIGI, 
                                  C.USERCOMP, C.COMPANY, C.BRANCH, C.POLICY, C.EFFECDATE, C.NULLDATE, C.PREMIUM, C.RATECOVE, C.CAPITAL, C.CURRENCY, C.COVER, C.MODULEC, C.CAPITALI,
                                  CERT.USERCOMP AS CERT_USERCOMP, CERT.COMPANY AS CERT_COMPANY, CERT.CERTYPE, CERT.DATE_ORIGI AS CERT_DATE_ORIGI, CERT.EFFECDATE as CERT_EFFECDATE, 
                                  CERT.COMPANY AS CERT_COMPANY, CERT.CERTYPE AS CERT_CERTYPE, CERT.BRANCH AS CERT_BRANCH, CERT.CERTIF AS CERTIF  
                                  FROM USINSUG01.COVER C   
                                  LEFT JOIN USINSUG01.CERTIFICAT CERT ON C.USERCOMP = CERT.USERCOMP  AND C.COMPANY = CERT.COMPANY  AND C.CERTYPE = CERT.CERTYPE AND C.BRANCH = CERT.BRANCH  AND C.POLICY = CERT.POLICY AND C.CERTIF = CERT.CERTIF
                                  JOIN USINSUG01.POLICY POL ON  POL.USERCOMP = C.USERCOMP AND POL.COMPANY = C.COMPANY AND POL.CERTYPE = C.CERTYPE AND POL.BRANCH = C.BRANCH  AND POL.POLICY = C.POLICY
                                  JOIN USINSUG01.POL_SUBPRODUCT PSP ON  PSP.USERCOMP = POL.USERCOMP AND PSP.COMPANY = POL.COMPANY AND PSP.CERTYPE = POL.CERTYPE AND PSP.BRANCH = POL.BRANCH AND PSP.PRODUCT  = POL.PRODUCT AND PSP.POLICY   = POL.POLICY 
                                  WHERE C.CERTYPE  = '2' AND POL.STATUS_POL NOT IN ('2','3') 
                                  AND ((POL.POLITYPE = '1' AND POL.EXPIRDAT >= '2021-12-31'  AND (POL.NULLDATE IS NULL OR POL.NULLDATE > '2021-12-31')) 
                                  OR (POL.POLITYPE <> '1' AND CERT.EXPIRDAT >= '2021-12-31'  AND (CERT.NULLDATE IS NULL OR CERT.NULLDATE > '2021-12-31')))
                                  AND POL.EFFECDATE BETWEEN '{p_fecha_inicio}' AND '{p_fecha_fin}')
                                  
                                  UNION /*SE QUITO EL UNION DESDE AQUI */

                                  (SELECT
                                   POL.POLICY AS POL_POLICY, POL.PRODUCT AS POL_PRODUCT, PSP.SUB_PRODUCT, POL.EFFECDATE AS POL_EFFECDATE, POL.POLITYPE AS POL_POLITYPE, POL.DATE_ORIGI as POL_DATE_ORIGI, 
                                   C.USERCOMP, C.COMPANY, C.BRANCH, C.POLICY, C.EFFECDATE, C.NULLDATE, C.PREMIUM, C.RATECOVE, C.CAPITAL, C.CURRENCY, C.COVER, C.MODULEC, C.CAPITALI,
                                   CERT.USERCOMP AS CERT_USERCOMP, CERT.COMPANY AS CERT_COMPANY, CERT.CERTYPE, CERT.DATE_ORIGI AS CERT_DATE_ORIGI, CERT.EFFECDATE as CERT_EFFECDATE, 
                                   CERT.COMPANY AS CERT_COMPANY, CERT.CERTYPE AS CERT_CERTYPE, CERT.BRANCH AS CERT_BRANCH, CERT.CERTIF AS CERTIF 
                                   FROM USINSUG01.COVER C  
                                   LEFT JOIN USINSUG01.CERTIFICAT CERT ON C.USERCOMP = CERT.USERCOMP  AND C.COMPANY = CERT.COMPANY  AND C.CERTYPE = CERT.CERTYPE AND C.BRANCH = CERT.BRANCH  AND C.POLICY = CERT.POLICY AND C.CERTIF = CERT.CERTIF
                                   JOIN USINSUG01.POLICY POL ON  POL.USERCOMP = C.USERCOMP AND POL.COMPANY = C.COMPANY AND POL.CERTYPE = C.CERTYPE AND POL.BRANCH = C.BRANCH  AND POL.POLICY = C.POLICY
                                   JOIN USINSUG01.POL_SUBPRODUCT PSP ON  PSP.USERCOMP = POL.USERCOMP AND PSP.COMPANY = POL.COMPANY AND PSP.CERTYPE = POL.CERTYPE AND PSP.BRANCH = POL.BRANCH AND PSP.PRODUCT  = POL.PRODUCT AND PSP.POLICY   = POL.POLICY 
                                   WHERE POL.CERTYPE  = '2' 
                                   AND POL.STATUS_POL NOT IN ('2', '3') 
                                   AND (((POL.POLITYPE = '1' AND  POL.EXPIRDAT < '{l_fecha_carga_inicial}' OR POL.NULLDATE < '{l_fecha_carga_inicial}')
                                   AND EXISTS (SELECT 1 FROM  USINSUG01.CLAIM CLA    
                                               JOIN  USINSUG01.CLAIM_HIS CLH ON CLH.USERCOMP = CLA.USERCOMP AND CLH.COMPANY = CLA.COMPANY AND CLH.BRANCH = CLA.BRANCH AND CLH.CLAIM = CLA.CLAIM
                                               WHERE CLA.BRANCH = POL.BRANCH AND CLA.POLICY = POL.POLICY AND TRIM(CLH.OPER_TYPE) IN (SELECT CAST(TCL.OPERATION AS VARCHAR(2))
                                      		                                                                                           FROM 	USINSUG01.TAB_CL_OPE TCL
                                      		                                                                                           WHERE  (TCL.RESERVE = 1 OR TCL.AJUSTES = 1 OR TCL.PAY_AMOUNT = 1)) 
                                                                                                                                     AND   CLH.OPERDATE >= '{l_fecha_carga_inicial}'))) 
                                   AND POL.EFFECDATE BETWEEN '{p_fecha_inicio}' AND '{p_fecha_fin}')   

                                  UNION

                                  (SELECT
                                   POL.POLICY AS POL_POLICY, POL.PRODUCT AS POL_PRODUCT, PSP.SUB_PRODUCT, POL.EFFECDATE AS POL_EFFECDATE, POL.POLITYPE AS POL_POLITYPE, POL.DATE_ORIGI as POL_DATE_ORIGI, 
                                   C.USERCOMP, C.COMPANY, C.BRANCH, C.POLICY, C.EFFECDATE, C.NULLDATE, C.PREMIUM, C.RATECOVE, C.CAPITAL, C.CURRENCY, C.COVER, C.MODULEC, C.CAPITALI,
                                   CERT.USERCOMP AS CERT_USERCOMP, CERT.COMPANY AS CERT_COMPANY, CERT.CERTYPE, CERT.DATE_ORIGI AS CERT_DATE_ORIGI, CERT.EFFECDATE as CERT_EFFECDATE, 
                                   CERT.COMPANY AS CERT_COMPANY, CERT.CERTYPE AS CERT_CERTYPE, CERT.BRANCH AS CERT_BRANCH, CERT.CERTIF AS CERTIF 
                                   FROM USINSUG01.COVER C  
                                   LEFT JOIN USINSUG01.CERTIFICAT CERT ON C.USERCOMP = CERT.USERCOMP  AND C.COMPANY = CERT.COMPANY  AND C.CERTYPE = CERT.CERTYPE AND C.BRANCH = CERT.BRANCH  AND C.POLICY = CERT.POLICY AND C.CERTIF = CERT.CERTIF
                                   JOIN USINSUG01.POLICY POL ON  POL.USERCOMP = C.USERCOMP AND POL.COMPANY = C.COMPANY AND POL.CERTYPE = C.CERTYPE AND POL.BRANCH = C.BRANCH  AND POL.POLICY = C.POLICY
                                   JOIN USINSUG01.POL_SUBPRODUCT PSP ON  PSP.USERCOMP = POL.USERCOMP AND PSP.COMPANY = POL.COMPANY AND PSP.CERTYPE = POL.CERTYPE AND PSP.BRANCH = POL.BRANCH AND PSP.PRODUCT  = POL.PRODUCT AND PSP.POLICY   = POL.POLICY 
                                   WHERE POL.CERTYPE  = '2' 
                                   AND POL.STATUS_POL NOT IN ('2', '3')
                                   AND (((POL.POLITYPE <> '1' AND CERT.EXPIRDAT < '{l_fecha_carga_inicial}'  OR  CERT.NULLDATE < '{l_fecha_carga_inicial}') 
                                   AND EXISTS (SELECT 1 FROM  USINSUG01.CLAIM CLA    
                                               JOIN  USINSUG01.CLAIM_HIS CLH  ON CLA.USERCOMP = CLH.USERCOMP AND CLA.COMPANY = CLH.COMPANY AND CLA.BRANCH = CLH.BRANCH  AND CLH.CLAIM = CLA.CLAIM
                                               WHERE CLA.BRANCH   = CERT.BRANCH AND   CLA.POLICY   = CERT.POLICY AND   CLA.CERTIF   = CERT.CERTIF AND TRIM(CLH.OPER_TYPE) IN (SELECT CAST(TCL.OPERATION AS VARCHAR(2)) 
                                                                                                                                                                              FROM  USINSUG01.TAB_CL_OPE TCL 
                                                                                                                                                                              WHERE (TCL.RESERVE = 1 OR TCL.AJUSTES = 1 OR TCL.PAY_AMOUNT = 1)) 
                                                                                                                                                                              AND  CLH.OPERDATE >= '{l_fecha_carga_inicial}')))
                                   AND POL.EFFECDATE BETWEEN '{p_fecha_inicio}' AND '{p_fecha_fin}') /*SE QUITO EL UNION HASTA AQUI */
                               ) C                                       
                            ) C2                    
                        ) AS T
                        '''

  l_df_abcobap_insunix_lpg = glue_context.read.format('jdbc').options(**connection).option("dbtable", l_abcobap_insunix_lpg).load()

  print("ABCOBAP INSUNIX LPG EXITOSO")

  l_abcobap_insunix_lpv = f'''
                          ( select 
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
                            '' as kabtrtab_2,
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
                                      (select
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
                                       and ((pol.politype = '1' -- individual 
                                             and pol.expirdat >= '{p_fecha_carga_inicial}'
                                           and (pol.nulldate is null or pol.nulldate > '{p_fecha_carga_inicial}'))
                                       or (pol.politype <> '1' -- colectivas 
                                           and cert.expirdat >= '{p_fecha_carga_inicial}'
                                       and (cert.nulldate is null or cert.nulldate > '{p_fecha_carga_inicial}')))
                                       and pol.effecdate between '{p_fecha_inicio}' and '{p_fecha_fin}')
                                    
                                    union /*se quito el union desde aqui */
                            
                                     (select
                                      c.usercomp, c.company, c.certype, c.branch, c.currency, c.cover, c.effecdate,        
                                      c.nulldate,c.policy, c.certif, c.premium, c.ratecove, c.capital, c.capitali, c.modulec as modulo,
                                      pol.product,pol.politype,pol.effecdate as effecdate_pol,cert.effecdate as effecdate_cert,
                                      case
                                      when pol.politype = '1' --individual
                                      then case
                                      when (c.effecdate <= pol.effecdate and
                                      (c.nulldate is null or
                                      c.nulldate > pol.effecdate)) then 1
                                      else case
                                       when exists (select
                                            1
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
                                          and cov1.effecdate <= pol.effecdate
                                          and (cov1.nulldate is null
                                          or cov1.nulldate > pol.effecdate)) then 0
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
                                       else case
                                         when (c.effecdate <= cert.effecdate and
                                         (c.nulldate is null or
                                         c.nulldate > cert.effecdate)) then 1
                                       else case
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
                                      where pol.certype  = '2' 
                                      and pol.status_pol not in ('2', '3') 
                                      and (((pol.politype = '1' and  pol.expirdat < '{p_fecha_carga_inicial}' or pol.nulldate < '{p_fecha_carga_inicial}')
                                      and exists (select 1 from  usinsuv01.claim cla    
                                                  join  usinsuv01.claim_his clh on clh.usercomp = cla.usercomp and clh.company = cla.company and clh.branch = cla.branch and clh.claim = cla.claim
                                                  where cla.branch = pol.branch and cla.policy = pol.policy and trim(clh.oper_type) in (select cast(tcl.operation as varchar(2))
                                          		                                                                                          from 	usinsug01.tab_cl_ope tcl
                                          		                                                                                          where  (tcl.reserve = 1 or tcl.ajustes = 1 or tcl.pay_amount = 1)) 
                                                                                                                                        and   clh.operdate >= '{p_fecha_carga_inicial}')))                                                                                                                                        
                                      and pol.effecdate between '{p_fecha_inicio}' and '{p_fecha_fin}')
                                                                                
                                    union
                            
                                     (select
                                     c.usercomp, c.company, c.certype, c.branch, c.currency, c.cover, c.effecdate,        
                                     c.nulldate,c.policy, c.certif, c.premium, c.ratecove, c.capital, c.capitali, c.modulec as modulo,
                                     pol.product,pol.politype,pol.effecdate as effecdate_pol,cert.effecdate as effecdate_cert,
                                     case
                                     when pol.politype = '1' --individual
                                     then case
                                     when (c.effecdate <= pol.effecdate and
                                      (c.nulldate is null or
                                      c.nulldate > pol.effecdate)) then 1
                                     else case
                                        when exists (select
                                            1
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
                                          and cov1.effecdate <= pol.effecdate
                                          and (cov1.nulldate is null
                                          or cov1.nulldate > pol.effecdate)) then 0
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
                                     else case
                                         when (c.effecdate <= cert.effecdate and
                                         (c.nulldate is null or
                                         c.nulldate > cert.effecdate)) then 1
                                     else case
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
                                     where pol.certype  = '2' 
                                     and pol.status_pol not in ('2', '3')
                                     and (((pol.politype <> '1' and cert.expirdat < '{p_fecha_carga_inicial}'  or  cert.nulldate < '{p_fecha_carga_inicial}') 
                                     and exists (select 1 from  usinsuv01.claim cla    
                                                 join  usinsuv01.claim_his clh  on cla.usercomp = clh.usercomp and cla.company = clh.company and cla.branch = clh.branch  and clh.claim = cla.claim
                                                 where cla.branch   = cert.branch
                                                 and   cla.policy   = cert.policy
                                                 and   cla.certif   = cert.certif
                                                 and   trim(clh.oper_type) in (select cast(tcl.operation as varchar(2)) from  usinsug01.tab_cl_ope tcl where (tcl.reserve = 1 or tcl.ajustes = 1 or tcl.pay_amount = 1)) and  clh.operdate >= '{p_fecha_carga_inicial}')))
                                     and pol.effecdate between '{p_fecha_inicio}' and '{p_fecha_fin}')/*se quito el union hasta aqui */) 
                            c where c.flag = 1) t) as tmp '''
 
  l_df_abcobap_insunix_lpv = glue_context.read.format('jdbc').options(**connection).option("dbtable", l_abcobap_insunix_lpv).load()
  
  print("ABCOBAP INSUNIX LPV EXITOSO")
  #-------------------------------------------------------------------------------------------------------------------------------#

  l_abcobap_vtime_lpg = f'''
                        ( SELECT 
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
                          '' AS KABTRTAB_2,
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
                                COALESCE( COALESCE((SELECT CAST(GLC."NCOVERGEN" AS VARCHAR)
                                                    FROM /*USBI01.IFRS170_V_GEN_LIFE_COVER_VTIMELPG*/
                                                    ( SELECT 
                                                      GC."NBRANCH",
                                                      GC."NPRODUCT",
                                                      GC."NMODULEC",
                                                      GC."NCOVER",
                                                      GC."DEFFECDATE",
                                                      GC."DNULLDATE",
                                                      GC."NCURRENCY",
                                                      CAST(GC."NCOVERGEN" AS VARCHAR) || '-g' "NCOVERGEN"
								                                      FROM USVTIMG01."GEN_COVER" GC 
								                                      WHERE EXISTS ( SELECT DISTINCT "NCOVERGEN" FROM USVTIMG01."GEN_COVER" GCO
							 	                                     		             WHERE GCO."NCOVERGEN" = GCO."NCOVERGEN"
							 	                                     		             AND   GCO."NBRANCH" <> 21 )
								                                     		        
                                                      UNION
								                                     		         
								                                      SELECT 
								                                      LC."NBRANCH",
		                                                  LC."NPRODUCT",
		                                                  LC."NMODULEC",
		                                                  LC."NCOVER",
		                                                  LC."DEFFECDATE",
		                                                  LC."DNULLDATE",
		                                                  LC."NCURRENCY",
		                                                  CAST(LC."NCOVERGEN" AS VARCHAR) || '-L' "NCOVERGEN"
								                                      FROM USVTIMG01."LIFE_COVER" LC
								                                      WHERE EXISTS (( SELECT	DISTINCT "NCOVERGEN" FROM USVTIMG01."LIFE_COVER" LCO
								                                      			          WHERE  LCO."NCOVERGEN" = LCO."NCOVERGEN"
								                                      			          AND    LCO."NBRANCH"   = 21)))  GLC
                                                      WHERE GLC."NBRANCH"   = C."NBRANCH" 
                                                      AND   GLC."NPRODUCT"  = C."NPRODUCT"
                                                      AND   GLC."NMODULEC"  = C."NMODULEC"
                                                      AND   GLC."NCOVER"    = C."NCOVER"
                                                      AND   GLC."DEFFECDATE" <= (CASE WHEN C."SPOLITYPE" = '1' THEN C."POL_DSTARTDATE" ELSE C."CERT_DSTARTDATE" END) 
                                                      AND   (GLC."DNULLDATE" IS NULL OR GLC."DNULLDATE" > (CASE WHEN C."SPOLITYPE" = '1' THEN C."POL_DSTARTDATE" ELSE C."CERT_DSTARTDATE" END))), ( SELECT CAST(GLC."NCOVERGEN" AS VARCHAR) /*USBI01.IFRS170_V_GEN_LIFE_COVER_VTIMELPG*/
                                                                                                                                                                                                    FROM 
                                                                                                                                                                                                    (SELECT 
                                                                                                                                                                                                     GC."NBRANCH",
                                                                                                                                                                                                     GC."NPRODUCT",
                                                                                                                                                                                                     GC."NMODULEC",
                                                                                                                                                                                                     GC."NCOVER",
                                                                                                                                                                                                     GC."DEFFECDATE",
                                                                                                                                                                                                     GC."DNULLDATE",
                                                                                                                                                                                                     GC."NCURRENCY",
                                                                                                                                                                                                     CAST(GC."NCOVERGEN" AS VARCHAR) || '-g' "NCOVERGEN"
								                                                                                                                                                                                     FROM USVTIMG01."GEN_COVER" GC 
								                                                                                                                                                                                     WHERE EXISTS ( SELECT DISTINCT "NCOVERGEN" FROM USVTIMG01."GEN_COVER" GCO
							 	                                                                                                                                                                             	       	              WHERE GCO."NCOVERGEN" = GCO."NCOVERGEN"
							 	                                                                                                                                                                             	       	              AND   GCO."NBRANCH" <> 21 )
								                                                                                                                                                                             		     
                                                                                                                                                                                                     UNION
								                                                                                                                                                                             		     
                                                                                                                                                                                                     SELECT 
								                                                                                                                                                                                     LC."NBRANCH",
		                                                                                                                                                                                                 LC."NPRODUCT",
		                                                                                                                                                                                                 LC."NMODULEC",
		                                                                                                                                                                                                 LC."NCOVER",
		                                                                                                                                                                                                 LC."DEFFECDATE",
		                                                                                                                                                                                                 LC."DNULLDATE",
		                                                                                                                                                                                                 LC."NCURRENCY",
		                                                                                                                                                                                                 CAST(LC."NCOVERGEN" AS VARCHAR) || '-l' "NCOVERGEN"
								                                                                                                                                                                                     FROM USVTIMG01."LIFE_COVER" LC
								                                                                                                                                                                                     WHERE EXISTS (( SELECT	DISTINCT "NCOVERGEN" FROM USVTIMG01."LIFE_COVER" LCO
								                                                                                                                                                                            	       		             WHERE  LCO."NCOVERGEN" = LCO."NCOVERGEN"
								                                                                                                                                                                            	                       AND    LCO."NBRANCH" = 21)))  GLC 
                                                                                                                                                                                                    WHERE GLC."NBRANCH"   = C."NBRANCH" 
                                                                                                                                                                                                    AND   GLC."NPRODUCT"  = C."NPRODUCT"
                                                                                                                                                                                                    AND   GLC."NMODULEC"  = C."NMODULEC"
                                                                                                                                                                                                    AND   GLC."NCOVER"    = C."NCOVER"
                                                                                                                                                                                                    AND   GLC."DNULLDATE" = ( SELECT MAX("DNULLDATE") FROM /*USBI01.IFRS170_V_GEN_LIFE_COVER_VTIMELPG*/
                                                                                                                                                                                                                              (SELECT 
                                                                                                                                                                                                                               GC."NBRANCH",
                                                                                                                                                                                                                               GC."NPRODUCT",
                                                                                                                                                                                                                               GC."NMODULEC",
                                                                                                                                                                                                                               GC."NCOVER",
                                                                                                                                                                                                                               GC."DEFFECDATE",
                                                                                                                                                                                                                               GC."DNULLDATE",
                                                                                                                                                                                                                               GC."NCURRENCY",
                                                                                                                                                                                                                               CAST(GC."NCOVERGEN" AS VARCHAR) || '-g' "NCOVERGEN"
								                                                                                                                                                                                                               FROM USVTIMG01."GEN_COVER" GC 
								                                                                                                                                                                                                               WHERE EXISTS ( SELECT DISTINCT "NCOVERGEN" FROM USVTIMG01."GEN_COVER" GCO
							 	                                                                                                                                                                             	                                 	              WHERE GCO."NCOVERGEN" = GCO."NCOVERGEN"
							 	                                                                                                                                                                             	                                 	              AND   GCO."NBRANCH" <> 21 )
								                                                                                                                                                                             		        
                                                                                                                                                                                                                               UNION
								                                                                                                                                                                             		         
								                                                                                                                                                                                                               SELECT 
								                                                                                                                                                                                                               LC."NBRANCH",
		                                                                                                                                                                                                                           LC."NPRODUCT",
		                                                                                                                                                                                                                           LC."NMODULEC",
		                                                                                                                                                                                                                           LC."NCOVER",
		                                                                                                                                                                                                                           LC."DEFFECDATE",
		                                                                                                                                                                                                                           LC."DNULLDATE",
		                                                                                                                                                                                                                           LC."NCURRENCY",
		                                                                                                                                                                                                                           CAST(LC."NCOVERGEN" AS VARCHAR) || '-l' "NCOVERGEN"
								                                                                                                                                                                                                               FROM USVTIMG01."LIFE_COVER" LC
								                                                                                                                                                                                                               WHERE EXISTS (( SELECT	DISTINCT "NCOVERGEN" FROM USVTIMG01."LIFE_COVER" LCO
								                                                                                                                                                                            	                                 		             WHERE  LCO."NCOVERGEN" = LCO."NCOVERGEN"
								                                                                                                                                                                            	                                 		             AND    LCO."NBRANCH" = 21)))  GLC 
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
                                    AND ((POL."SPOLITYPE" = '1' -- INDIVIDUAL 
                                    AND POL."DEXPIRDAT" >= '{l_fecha_carga_inicial}' 
                                    AND (POL."DNULLDATE" IS NULL OR POL."DNULLDATE" > '{l_fecha_carga_inicial}'))
                                    OR 
                                    (POL."SPOLITYPE" <> '1' -- COLECTIVAS 
                                    AND CERT."DEXPIRDAT" >= '{l_fecha_carga_inicial}' 
                                    AND (CERT."DNULLDATE" IS NULL OR CERT."DNULLDATE" > '{l_fecha_carga_inicial}')))
                                    AND POL."DSTARTDATE" BETWEEN '{p_fecha_inicio}' AND '{p_fecha_fin}') 
                                  /*
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
                                           AND CLA."NCERTIF"  = CERT."NCERTIF"))))))*/
                                ) C WHERE FLAG = 1
                          ) COVER                                            
                        ) AS TMP'''

  l_df_abcobap_vtime_lpg = glue_context.read.format('jdbc').options(**connection).option("dbtable", l_abcobap_vtime_lpg).load()

  print("ABCOBAP VTIME LPG EXITOSO")

  l_abcobap_vtime_lpv = f'''
                        ( SELECT 
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
                          '' AS KABTRTAB_2,
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
                                    AND ((POL."SPOLITYPE" = '1' -- INDIVIDUAL 
                                    AND POL."DEXPIRDAT" >= '{l_fecha_carga_inicial}' 
                                    AND (POL."DNULLDATE" IS NULL OR POL."DNULLDATE" > '{l_fecha_carga_inicial}'))
                                    OR 
                                    (POL."SPOLITYPE" <> '1' -- COLECTIVAS 
                                    AND CERT."DEXPIRDAT" >= '{l_fecha_carga_inicial}' 
                                    AND (CERT."DNULLDATE" IS NULL OR CERT."DNULLDATE" > '{l_fecha_carga_inicial}')))
                                    AND POL."DSTARTDATE" BETWEEN '{p_fecha_inicio}' AND '{p_fecha_fin}')
                           
                              UNION /*SE QUITO EL UNION DESDE AQUI */
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
                              /*SE QUITO EL UNION HASTA AQUI */
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
                      '' AS KABTRTAB_2,
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
                      WHERE (P."INSR_END" >= '{l_fecha_carga_inicial}'
                                    OR  (P."INSR_END" < '{l_fecha_carga_inicial}' AND EXISTS (
                        	      	                              		           SELECT 1 FROM USINSIV01."CLAIM" C
                        	      	                              		           JOIN USINSIV01."CLAIM_OBJECTS" CO ON CO."CLAIM_ID" = C."CLAIM_ID" AND CO."POLICY_ID" = C."POLICY_ID"
                        	      	                              		           JOIN USINSIV01."CLAIM_RESERVE_HISTORY" CRH ON CO."CLAIM_ID" = CRH."CLAIM_ID" AND CO."REQUEST_ID" = CO."REQUEST_ID" AND CO."CLAIM_OBJ_SEQ" = CRH."CLAIM_OBJECT_SEQ"
                        	      	                              		           WHERE C."POLICY_ID" = P."POLICY_ID"
                        	      	                              		           AND "OP_TYPE" IN ('REG','EST','CLC','PAYMCONF','PAYMINV')
                        	      	                              		           AND CAST(CRH."REGISTRATION_DATE" AS DATE) >= '{l_fecha_carga_inicial}'
                        	      	                                                    ))
                            )
                      AND POL."REGISTRATION_DATE" BETWEEN '{p_fecha_inicio}' AND '{p_fecha_fin}') AS TMP
                    '''
    
    #EJECUTAR CONSULTA
  l_df_abcobap_insis = glue_context.read.format('jdbc').options(**connection).option("dbtable",l_abcobap_insis).load()

  print("ABCOBAP INSIS EXITOSO")

  #PERFORM THE UNION OPERATION
  l_df_abcobap = l_df_abcobap_insunix_lpg.union(l_df_abcobap_insunix_lpv).union(l_df_abcobap_vtime_lpg).union(l_df_abcobap_vtime_lpv).union(l_df_abcobap_insis)

  l_df_abcobap = l_df_abcobap.withColumn("VMTCOMR", col("VMTCOMR").cast(DecimalType(12, 2))).withColumn("VMTPRMBR", col("VMTPRMBR").cast(DecimalType(12, 2))).withColumn("VTXCOB", format_number("VTXCOB",9)).withColumn("VCAPITAL", col("VCAPITAL").cast(DecimalType(14, 2))).withColumn("VTXCAPIT", col("VTXCAPIT").cast(DecimalType(9, 5))).withColumn("VTXINDX", col("VTXINDX").cast(DecimalType(7, 4))).withColumn("VMTPREMC", col("VMTPREMC").cast(DecimalType(12, 2))).withColumn("VMTCAPLI", col("VMTCAPLI").cast(DecimalType(14, 2))).withColumn("VMTCAPIN", col("VMTCAPIN").cast(DecimalType(14, 2))).withColumn("VMTPREIN", col("VMTPREIN").cast(DecimalType(14, 2))).withColumn("VTXLMRES", col("VTXLMRES").cast(DecimalType(7, 4))).withColumn("VMTPRRES", col("VMTPRRES").cast(DecimalType(12, 2))).withColumn("VTXAJTBUA", col("VTXAJTBUA").cast(DecimalType(9, 4))).withColumn("VMTCAPREM", col("VMTCAPREM").cast(DecimalType(12, 2)))
  
  return l_df_abcobap