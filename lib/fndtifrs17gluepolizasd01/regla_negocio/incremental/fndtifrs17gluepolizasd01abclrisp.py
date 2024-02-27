from pyspark.sql.types import StringType , DateType
from pyspark.sql.functions import col , coalesce , lit , format_number

def get_data(glue_context, connection, p_fecha_inicio, p_fecha_fin):

    l_fecha_carga_inicial = '2021-12-31'
    
    l_abclrisp_insunix_lpg = f'''
                             ( select
                              'd' inddetrec, 
                              'abclrisp' tablaifrs17, 
                              '' as pk,
                              '' as dtpreg,
                              '' as tiocproc,
                              cast(r.compdate as date) as tiocfrm,
                              '' as tiocto,
                              'pig' as kgiorigm,
                              p.branch ||'-'|| coalesce (p.product, 0) ||'-'|| coalesce(psp.sub_product, 0) ||'-'|| p.policy ||'-'|| cert.certif as kabapol,
                              p.branch ||'-'|| coalesce (p.product, 0) ||'-'|| p.policy ||'-'|| cert.certif || '-' || (select evi.scod_vt  from usinsug01.equi_vt_inx evi where evi.scod_inx = r.client)  as kabunris,
                              case p.politype
                              when '1' then ( select coalesce(gc.covergen, 0) ||'-'|| coalesce(gc.currency, 0)
                                              from usinsug01.gen_cover gc 
                                              join usinsug01.cover c  
                                              on  gc.usercomp    = '1' 
                                              and gc.company     = '1'
                                              and gc.branch      = c.branch
                                              and gc.product     = p.product
                                              and gc.sub_product = psp.sub_product
                                              and gc.currency    = c.currency
                                              and gc.modulec     = c.modulec
                                              and gc.cover       = c.cover
                                              and gc.effecdate  <= p.effecdate
                     	             	         and (gc.nulldate is null or gc.nulldate > p.effecdate)		       		   
                                              where c.usercomp   = p.usercomp 
                                              and   c.company    = p.company 
                                              and   c.certype    = '2' 
                                              and   c.branch     = p.branch 
                                              and   c.policy     = p.policy
                                              and   c.certif     = cert.certif  
                                              and   c.effecdate <= p.effecdate
                                              and  (c.nulldate is null or c.nulldate > p.effecdate)
                                              and  c.cover = 1 limit 1) 
                              else   ( select coalesce(gc.covergen, 0) ||'-'|| coalesce(gc.currency, 0)
                                                    from usinsug01.gen_cover gc 
                                                    join usinsug01.cover c  
                                                    on  gc.usercomp    = '1'
                                                    and gc.company     = '1'
                                                    and gc.branch      = c.branch
                                                    and gc.product     = p.product
                                                    and gc.sub_product = psp.sub_product
                                                    and gc.currency    = c.currency
                                                    and gc.modulec     = c.modulec
                                                    and gc.cover       = c.cover
                                                    and gc.effecdate <=  cert.effecdate
                             	            	    and (gc.nulldate is null or gc.nulldate > cert.effecdate)		       		   
                                                    where c.usercomp   = p.usercomp 
                                                    and   c.company    = p.company 
                                                    and   c.certype    = '2' 
                                                    and   c.branch     = p.branch 
                                                    and   c.policy     = p.policy
                                                    and   c.certif     = cert.certif  
                                                    and   c.effecdate <= cert.effecdate
                                                    and  (c.nulldate is null or c.nulldate > cert.effecdate)
                                                    and  c.cover = 1 limit 1)
                              end as kgctpcbt,
                              row_number () over (partition  by p.branch, coalesce (p.product, 0), p.policy, cert.certif order by r.client) as dnpeseg,
                              (select 'pae' || '-' || evi.scod_vt  from usinsug01.equi_vt_inx evi where evi.scod_inx  = r.client) as kebentid_ps,
                              (select date_part('year', age(current_date, cli.birthdat)) from usinsug01.client cli where cli.code = r.client) as didadeac,
                              '' as danoref, --en blanco
                              '' as kacempr,
                              case p.branch
                              when 23 
                              then (select coalesce (insu_he.anual_sal, 0) from usinsug01.insured_he insu_he
                                    where insu_he.usercomp = p.usercomp
                                    and   insu_he.company  = p.company
                                    and   insu_he.certype  = p.certype
                                    and   insu_he.branch   = p.branch
                                    and 	 insu_he.policy   = p.policy
                              	and   insu_he.certif   = cert.certif
                              	and   insu_he.client   = r.client
                              	and   insu_he.effecdate <= (case when politype = '1' then p.effecdate else cert.effecdate end)
                              	and   (insu_he.nulldate is null or insu_he.nulldate > (case when politype = '1' then p.effecdate else cert.effecdate end)))
                              else 0
                              end vmtsalar,
                              '' as kactpsal,
                              '' as tadmemp,
                              '' as tadmgrp,
                              '' as tsaidgrp,
                              'lpg' as dcompa,
                              '' as dmarca,
                              '' as tdnascim,
                              '' as dqdiasub,
                              '' as dqesping,
                              '' as kacsexo,
                              '' as kaccae,
                              '' as dqtrabal,
                              '' as dinapmen,
                              '' as dqcoefeq,
                              '' as dqhorsem, --en blanco
                              '' as dqmeses,
                              '' as vmtalime, --en blanco
                              '' as dqmesali, --en blanco
                              '' as vmtaloja,
                              '' as dqmesalo,
                              '' as vmtrenum, --en blanco
                              '' as dqmesren,
                              '' as dqdiatra,
                              '' as dqcopre,
                              '' as ditiner,
                              '' as dnomes,
                              '' as kacutiliz,
                              '' as vmtdesco, --en blanco
                              '' as dfranqu,  --en blanco
                              '' as dtarifa,
                              '' as dintipexp,
                              '' as kacespan,
                              '' as dqanimal,
                              '' as kactipsg,
                              '' as daproesc,
                              '' as dcmudesc,
                              '' as dqcapita,
                              '' as vtxcompa,
                              '' as dqcaes,
                              '' as dinextte,
                              '' as kaccltari,
                              '' as kactipvei,
                              '' as kaccatris,
                              '' as dindcol,
                              '' as daconstr,
                              '' as dqpesso1,
                              '' as dqpesso2,
                              '' as dqvias,
                              '' as kacagrav,
                              '' as kacparti,
                              '' as kacsermd,
                              '' as kacmrisc,
                              '' as dindcon,
                              '' as dqprazo,
                              r.role as kactppes,
                              '' as kacespes, --en blanco
                              '' as kacmepes,
                              '' as tdespes,  --en blanco
                              '' as kactppra,
                              '' as demprest,
                              '' as vmtprest,
                              '' as vmtempre,
                              '' as vmtprcrd,
                              '' as dcontcgd,
                              '' as dnclicgd,
                              '' as dcertifc,
                              r.effecdate as tinicio,
                              r.nulldate  as ttermo,
                              '' as dnomepar,
                              '' as kacprof,
                              '' as kacactiv,
                              '' as kacsactiv,
                              '' as vmtsalmd,
                              '' as dcodsub,
                              '' as kactpcon,
                              '' as dareaccv,
                              '' as dareacul,
                              '' as kaczonag,
                              '' as kactpidx,
                              '' as tdtindex,
                              '' as vmtprmin,
                              '' as dcdregim,
                              '' as dqhortra,
                              '' as dqsemtra,  --en blanco
                              '' as dcampanh,
                              '' as kacmodal,
                              '' as dentidso,
                              '' as dlocref,
                              '' as kacintni,
                              '' as kacclris,
                              '' as kacambcb,
                              '' as kactrain,
                              '' as dindcirs,
                              '' as dincerpa,
                              '' as dindmoto,
                              '' as dmatric,
                              '' as dindmark,
                              '' as kacopcbt,
                              '' as vtxindx,
                              '' as dagridad,   --en blanco
                              '' as kacpais_dt, --no
                              '' as kacmdac,    --en blanco
                              case p.branch
                              when 23 
                              then (select coalesce (insu_he.age_limit, 0) from usinsug01.insured_he insu_he
                                    where insu_he.usercomp = p.usercomp
                                    and   insu_he.company  =  p.company
                                    and   insu_he.certype  =  p.certype
                                    and   insu_he.branch = p.branch
                                    and 	insu_he.policy = p.policy
                              	and   insu_he.certif = cert.certif
                              	and   insu_he.client = r.client
                              	and   insu_he.effecdate <= (case when politype = '1' then p.effecdate else cert.effecdate end)
                              	and   (insu_he.nulldate is null or insu_he.nulldate > (case when politype = '1' then p.effecdate else cert.effecdate end)))
                              else 0
                              end didadecom,
                             '' as vtxperindc,
                             '' as tpgmybenef                           
                              from usinsug01.policy p 
                              left join usinsug01.certificat cert 
                              on p.usercomp = cert.usercomp 
                              and p.company = cert.company 
                              and p.certype = cert.certype 
                              and p.branch  = cert.branch 
                              and p.policy  = cert.policy
                              join usinsug01.pol_subproduct psp   
                              on  psp.usercomp = p.usercomp 
                              and psp.company  = p.company 
                              and psp.certype  = p.certype 
                              and psp.branch   = p.branch		    
                              and psp.product  = p.product 
                              and psp.policy   = p.policy	
                              join /*usbi01."ifrs170_t_ramos_por_tipo_riesgo"*/
                              (    select    unnest(array['usinsug01','usinsug01','usinsug01','usinsug01','usinsug01','usinsug01','usinsug01','usinsug01','usinsug01','usinsug01','usinsug01','usinsug01','usinsug01','usinsug01','usinsug01','usinsug01','usinsug01','usinsug01','usinsug01','usinsug01','usinsug01','usinsug01','usinsug01','usinsug01']) as "sourceschema",  
				                     unnest(array[5,21,22,23,24,25,27,31,32,33,34,35,36,37,40,41,42,59,68,71,75,77,91,99]) as "branchcom",
				                     unnest(array[1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1]) as "risktypen") rtr 
                              on   rtr."branchcom" = p.branch 
                              and  rtr."risktypen" = 1 
                              and  rtr."sourceschema" = 'usinsug01'
                              join usinsug01.roles r                           
                              on  r.usercomp = '1'
                              and r.company  = '1'
                              and r.certype  = p.certype 
                              and r.branch   = p.branch 
                              and r.policy   = p.policy 
                              and r.certif   = cert.certif  
                              and r.effecdate <= (case when politype = '1' then p.effecdate else cert.effecdate end)
                              and (r.nulldate is null or r.nulldate > case when politype = '1' then p.effecdate else cert.effecdate end)
                              where p.certype = '2' 
                              and   p.status_pol not in ('2','3')
                              and   r.role in (2,8) 
                              and   r.compdate between '{p_fecha_inicio}' and '{p_fecha_fin}'
                              ) as tmp
                             '''
    
    l_df_abclrisp_insunix_lpg = glue_context.read.format('jdbc').options(**connection).option("dbtable", l_abclrisp_insunix_lpg).load()

    print("INSUNIX LPG")

    l_abclrisp_insunix_lpv = f'''
                             (
                               SELECT
                                    'D' INDDETREC, 
                                    'ABCLRISP' TABLAIFRS17, 
                                    '' AS PK,
                                    '' AS DTPREG,
                                    '' AS TIOCPROC,
                                    cast(R.COMPDATE as date) AS TIOCFRM,
                                    '' AS TIOCTO,
                                    'PIV' AS KGIORIGM,
                                    PC.BRANCH ||'-'|| COALESCE (PC.PRODUCT, 0) ||'-'|| PC.POLICY ||'-'|| PC.CERTIF AS KABAPOL,
                                    PC.BRANCH ||'-'|| COALESCE (PC.PRODUCT, 0) ||'-'|| PC.POLICY ||'-'|| PC.CERTIF || '-' || COALESCE((SELECT EVI.SCOD_VT FROM USINSUG01.EQUI_VT_INX EVI WHERE EVI.SCOD_INX = R.CLIENT), '0') AS KABUNRIS,
                                    case PC.POLITYPE when  '1'
                                    then                                    
                                    ( SELECT COALESCE(GC.COVERGEN, 0) ||'-'|| COALESCE(GC.CURRENCY, 0)
                                          FROM /*USBI01.IFRS170_V_GEN_LIFE_COVER_INXLPV*/
                                          (SELECT GC.USERCOMP,
										  GC.COMPANY,GC.BRANCH,GC.PRODUCT,GC.CURRENCY,
										  GC.MODULEC,GC.COVER,GC.EFFECDATE,GC.NULLDATE,
										  GC.COVERGEN
										  FROM USINSUV01.GEN_COVER GC
										  UNION 
										  SELECT LC.USERCOMP,
										  LC.COMPANY,LC.BRANCH,LC.PRODUCT,LC.CURRENCY,
										  0 AS MODULEC,LC.COVER,LC.EFFECDATE,LC.NULLDATE,
										  LC.COVERGEN
										  FROM USINSUV01.LIFE_COVER LC) GC 
                                          JOIN USINSUV01.COVER C  
                                          ON  GC.USERCOMP = C.USERCOMP 
                                          AND GC.COMPANY  = C.COMPANY 
                                          AND GC.BRANCH   = C.BRANCH
                                          AND GC.PRODUCT  = PC.PRODUCT
                                          --AND GC.SUB_PRODUCT = PC.SUB_PRODUCT
                                          AND GC.CURRENCY = C.CURRENCY
                                          --AND GC.MODULEC =  C.MODULEC
                                          AND GC.COVER   =  C.COVER
                                          AND GC.EFFECDATE <= PC.EFFECDATE
                                          AND (GC.NULLDATE IS NULL OR GC.NULLDATE > PC.EFFECDATE)		       		   
                                          WHERE C.USERCOMP   = PC.USERCOMP 
                                          AND   C.COMPANY    = PC.COMPANY 
                                          AND   C.CERTYPE    = '2' 
                                          AND   C.BRANCH     = PC.BRANCH 
                                          AND   C.POLICY     = PC.POLICY
                                          AND   C.CERTIF     = PC.CERTIF  
                                          AND   C.EFFECDATE <= PC.EFFECDATE
                                          AND  (C.NULLDATE IS NULL OR C.NULLDATE > PC.EFFECDATE)
                                          AND  C.COVER = 1 limit 1
                                           ) else
		                                           ( SELECT COALESCE(GC.COVERGEN, 0) ||'-'|| COALESCE(GC.CURRENCY, 0)
		                                          FROM /*USBI01.IFRS170_V_GEN_LIFE_COVER_INXLPV*/
		                                          (SELECT GC.USERCOMP,
												  GC.COMPANY,GC.BRANCH,GC.PRODUCT,GC.CURRENCY,
												  GC.MODULEC,GC.COVER,GC.EFFECDATE,GC.NULLDATE,
												  GC.COVERGEN
												  FROM USINSUV01.GEN_COVER GC
												  UNION 
												  SELECT LC.USERCOMP,
												  LC.COMPANY,LC.BRANCH,LC.PRODUCT,LC.CURRENCY,
												  0 AS MODULEC,LC.COVER,LC.EFFECDATE,LC.NULLDATE,
												  LC.COVERGEN
												  FROM USINSUV01.LIFE_COVER LC) GC 
		                                          JOIN USINSUV01.COVER C  
		                                          ON  GC.USERCOMP = C.USERCOMP 
		                                          AND GC.COMPANY  = C.COMPANY 
		                                          AND GC.BRANCH   = C.BRANCH
		                                          AND GC.PRODUCT  = PC.PRODUCT
		                                          --AND GC.SUB_PRODUCT = PC.SUB_PRODUCT
		                                          AND GC.CURRENCY = C.CURRENCY
		                                          --AND GC.MODULEC =  C.MODULEC
		                                          AND GC.COVER   =  C.COVER
		                                          AND GC.EFFECDATE <= PC.EFFECDATE_CERT
		                                          AND (GC.NULLDATE IS NULL OR GC.NULLDATE > PC.EFFECDATE_CERT)		       		   
		                                          WHERE C.USERCOMP   = PC.USERCOMP 
		                                          AND   C.COMPANY    = PC.COMPANY 
		                                          AND   C.CERTYPE    = '2' 
		                                          AND   C.BRANCH     = PC.BRANCH 
		                                          AND   C.POLICY     = PC.POLICY
		                                          AND   C.CERTIF     = PC.CERTIF  
		                                          AND   C.EFFECDATE <= PC.EFFECDATE_CERT
		                                          AND  (C.NULLDATE IS NULL OR C.NULLDATE > PC.EFFECDATE_CERT)
		                                          AND  C.COVER = 1 limit 1)
                                          END AS KGCTPCBT,
                                    ROW_NUMBER () OVER ( PARTITION  BY PC.BRANCH, COALESCE (PC.PRODUCT, 0), PC.POLICY, PC.CERTIF order by R.CLIENT) AS DNPESEG, --PENDIENTE
                                    '' AS KEBENTID_PS,
                                    (SELECT (CURRENT_DATE - CLI.BIRTHDAT)/365 FROM USINSUG01.CLIENT CLI WHERE CLI.CODE = PC.TITULARC) AS DIDADEAC,
                                    '' AS DANOREF, --EN BLANCO
                                    '' AS KACEMPR,
                                    CASE PC.BRANCH
                                    WHEN 23 
                                    THEN (SELECT COALESCE (INSU_HE.ANUAL_SAL, 0) FROM USINSUV01.INSURED_HE INSU_HE
                                          WHERE INSU_HE.USERCOMP = PC.USERCOMP
                                          AND   INSU_HE.COMPANY  =  PC.COMPANY
                                          AND   INSU_HE.CERTYPE  =  PC.CERTYPE
                                          AND   INSU_HE.BRANCH = PC.BRANCH
                                          AND 	INSU_HE.POLICY = PC.POLICY
                                          AND   INSU_HE.CERTIF = PC.CERTIF
                                          AND   INSU_HE.CLIENT = R.CLIENT
                                          AND   INSU_HE.EFFECDATE <= PC.EFFECDATE
                                          AND   (INSU_HE.NULLDATE IS NULL OR INSU_HE.NULLDATE > PC.EFFECDATE))
                                    ELSE 0
                                    END VMTSALAR,
                                    '' AS KACTPSAL,
                                    '' AS TADMEMP,
                                    '' AS TADMGRP,
                                    '' AS TSAIDGRP,
                                    'LPV' AS DCOMPA,
                                    '' AS DMARCA,
                                    '' AS TDNASCIM,
                                    '' AS DQDIASUB,
                                    '' AS DQESPING,
                                    '' AS KACSEXO,
                                    '' AS KACCAE,
                                    '' AS DQTRABAL,
                                    '' AS DINAPMEN,
                                    '' AS DQCOEFEQ,
                                    '' AS DQHORSEM, --EN BLANCO
                                    '' AS DQMESES,
                                    '' AS VMTALIME, --EN BLANCO
                                    '' AS DQMESALI, --EN BLANCO
                                    '' AS VMTALOJA,
                                    '' AS DQMESALO,
                                    '' AS VMTRENUM, --EN BLANCO
                                    '' AS DQMESREN,
                                    '' AS DQDIATRA,
                                    '' AS DQCOPRE,
                                    '' AS DITINER,
                                    '' AS DNOMES,
                                    '' AS KACUTILIZ,
                                    '' AS VMTDESCO, --EN BLANCO
                                    '' AS DFRANQU,  --EN BLANCO
                                    '' AS DTARIFA,
                                    '' AS DINTIPEXP,
                                    '' AS KACESPAN,
                                    '' AS DQANIMAL,
                                    '' AS KACTIPSG,
                                    '' AS DAPROESC,
                                    '' AS DCMUDESC,
                                    '' AS DQCAPITA,
                                    '' AS VTXCOMPA,
                                    '' AS DQCAES,
                                    '' AS DINEXTTE,
                                    '' AS KACCLTARI,
                                    '' AS KACTIPVEI,
                                    '' AS KACCATRIS,
                                    '' AS DINDCOL,
                                    '' AS DACONSTR,
                                    '' AS DQPESSO1,
                                    '' AS DQPESSO2,
                                    '' AS DQVIAS,
                                    '' AS KACAGRAV,
                                    '' AS KACPARTI,
                                    '' AS KACSERMD,
                                    '' AS KACMRISC,
                                    '' AS DINDCON,
                                    '' AS DQPRAZO,
                                    R.ROLE AS KACTPPES,
                                    '' AS KACESPES, --EN BLANCO
                                    '' AS KACMEPES,
                                    '' AS TDESPES,  --EN BLANCO
                                    '' AS KACTPPRA,
                                    '' AS DEMPREST,
                                    '' AS VMTPREST,
                                    '' AS VMTEMPRE,
                                    '' AS VMTPRCRD,
                                    '' AS DCONTCGD,
                                    '' AS DNCLICGD,
                                    '' AS DCERTIFC,
                                    R.EFFECDATE AS TINICIO,
                                    coalesce(cast(cast(R.NULLDATE as date) as varchar), '') AS TTERMO,
                                    '' AS DNOMEPAR,
                                    '' AS KACPROF,
                                    '' AS KACACTIV,
                                    '' AS KACSACTIV,
                                    '' AS VMTSALMD,
                                    '' AS DCODSUB,
                                    '' AS KACTPCON,
                                    '' AS DAREACCV,
                                    '' AS DAREACUL,
                                    '' AS KACZONAG,
                                    '' AS KACTPIDX,
                                    '' AS TDTINDEX,
                                    '' AS VMTPRMIN,
                                    '' AS DCDREGIM,
                                    '' AS DQHORTRA,
                                    '' AS DQSEMTRA,  --EN BLANCO
                                    '' AS DCAMPANH,
                                    '' AS KACMODAL,
                                    '' AS DENTIDSO,
                                    '' AS DLOCREF,
                                    '' AS KACINTNI,
                                    '' AS KACCLRIS,
                                    '' AS KACAMBCB,
                                    '' AS KACTRAIN,
                                    '' AS DINDCIRS,
                                    '' AS DINCERPA,
                                    '' AS DINDMOTO,
                                    '' AS DMATRIC,
                                    '' AS DINDMARK,
                                    '' AS KACOPCBT,
                                    '' AS VTXINDX,
                                    '' AS DAGRIDAD,   --EN BLANCO
                                    '' AS KACPAIS_DT, --NO
                                    '' AS KACMDAC,    --EN BLANCO
                                    CASE PC.BRANCH
                                    WHEN 23 
                                    THEN (SELECT COALESCE (INSU_HE.AGE_LIMIT, 0) FROM USINSUV01.INSURED_HE INSU_HE
                                          WHERE INSU_HE.USERCOMP = PC.USERCOMP
                                          AND   INSU_HE.COMPANY  =  PC.COMPANY
                                          AND   INSU_HE.CERTYPE  =  PC.CERTYPE
                                          AND   INSU_HE.BRANCH = PC.BRANCH
                                          AND 	INSU_HE.POLICY = PC.POLICY
                                          AND   INSU_HE.CERTIF = PC.CERTIF
                                          AND   INSU_HE.CLIENT = R.CLIENT
                                          AND   INSU_HE.EFFECDATE <= PC.EFFECDATE
                                          AND   (INSU_HE.NULLDATE IS NULL OR INSU_HE.NULLDATE > PC.EFFECDATE))
                                    ELSE 0
                                    END DIDADECOM,
                                    '' AS VTXPERINDC,
                                    '' AS TPGMYBENEF
                                    FROM USINSUV01.ROLES R
                                    JOIN 
                                    (   
                                        (SELECT P.USERCOMP, P.COMPANY, P.CERTYPE, P.BRANCH, P.PRODUCT, P.POLICY, CERT.CERTIF, P.TITULARC, P.EFFECDATE , P.POLITYPE, CERT.EFFECDATE as EFFECDATE_CERT
                                          FROM USINSUV01.POLICY P 
                                          LEFT JOIN USINSUV01.CERTIFICAT CERT 
                                          ON P.USERCOMP = CERT.USERCOMP 
                                          AND P.COMPANY = CERT.COMPANY 
                                          AND P.CERTYPE = CERT.CERTYPE 
                                          AND P.BRANCH  = CERT.BRANCH 
                                          AND P.POLICY  = CERT.policy	
                                          JOIN /*USBI01."IFRS170_T_RAMOS_POR_TIPO_RIESGO"*/
                                          (SELECT unnest(ARRAY['usinsuv01','usinsuv01','usinsuv01','usinsuv01','usinsuv01','usinsuv01','usinsuv01',
					                                 'usinsuv01','usinsuv01','usinsuv01','usinsuv01','usinsuv01','usinsuv01','usinsuv01',
					                                 'usinsuv01','usinsuv01','usinsuv01','usinsuv01','usinsuv01','usinsuv01','usinsuv01',
					                                 'usinsuv01','usinsuv01','usinsuv01']) AS "SOURCESCHEMA",  
						                          unnest(ARRAY[5,21,22,23,24,25,27,31,32,33,34,35,36,37,40,41,42,59,68,71,75,77,91,99]) AS "BRANCHCOM",
							                      unnest(ARRAY[1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1]) AS "RISKTYPEN") RTR ON RTR."BRANCHCOM" = P.BRANCH AND  RTR."RISKTYPEN" = 1 AND RTR."SOURCESCHEMA" = 'usinsuv01'
                                          WHERE P.CERTYPE = '2' 
                                          AND P.STATUS_POL NOT IN ('2','3') 
                                          -- JAOS
                                          --AND ( (P.POLITYPE = '1' -- INDIVIDUAL 
                                          --AND P.EXPIRDAT >= '{l_fecha_carga_inicial}' 
                                          --AND (P.NULLDATE IS NULL OR P.NULLDATE > '{l_fecha_carga_inicial}') )
                                          --OR 
                                          --(P.POLITYPE <> '1' -- COLECTIVAS 
                                          --AND CERT.EXPIRDAT >= '{l_fecha_carga_inicial}' 
                                          --AND (CERT.NULLDATE IS NULL OR CERT.NULLDATE > '{l_fecha_carga_inicial}'))))

                                          /*UNION -- JAOS YA NO ES NECESARIO EN EL INCREMENTAL 

                                          (SELECT P.USERCOMP, P.COMPANY, P.CERTYPE, P.BRANCH, P.PRODUCT, PSP.SUB_PRODUCT, P.POLICY, CERT.CERTIF, P.TITULARC, P.EFFECDATE ,P.POLITYPE , CERT.EFFECDATE as EFFECDATE_CERT
                                           FROM USINSUV01.POLICY P 
                             	             LEFT JOIN USINSUV01.CERTIFICAT CERT ON P.USERCOMP = CERT.USERCOMP AND P.COMPANY = CERT.COMPANY AND P.CERTYPE = CERT.CERTYPE AND P.BRANCH  = CERT.BRANCH AND P.POLICY  = CERT.policy                             	           
                             	             JOIN /*USBI01."IFRS170_T_RAMOS_POR_TIPO_RIESGO"*/
                                           (SELECT  unnest(ARRAY['usinsuv01','usinsuv01','usinsuv01','usinsuv01','usinsuv01','usinsuv01','usinsuv01',
					                                             'usinsuv01','usinsuv01','usinsuv01','usinsuv01','usinsuv01','usinsuv01','usinsuv01',
					                                             'usinsuv01','usinsuv01','usinsuv01','usinsuv01','usinsuv01','usinsuv01','usinsuv01',
					                                             'usinsuv01','usinsuv01','usinsuv01']) AS "SOURCESCHEMA",  
						                            unnest(ARRAY[5,21,22,23,24,25,27,31,32,33,34,35,36,37,40,41,42,59,68,71,75,77,91,99]) AS "BRANCHCOM",
							                        unnest(ARRAY[1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1]) AS "RISKTYPEN") RTR ON RTR."BRANCHCOM" = P.BRANCH AND  RTR."RISKTYPEN" = 1 AND RTR."SOURCESCHEMA" = 'usinsuv01'
                             	             WHERE P.CERTYPE  = '2' 
                                           AND P.STATUS_POL NOT IN ('2', '3') 
                                           AND (((P.POLITYPE = '1' AND  P.EXPIRDAT < '2021-12-31' OR P.NULLDATE < '2021-12-31')
                                           AND EXISTS (SELECT 1 FROM USINSUV01.CLAIM CLA    
                                                       JOIN  USINSUV01.CLAIM_HIS CLH 
                                                       ON CLH.USERCOMP = CLA.USERCOMP 
                                                       AND CLH.COMPANY = CLA.COMPANY 
                                                       AND CLH.BRANCH = CLA.BRANCH 
                                                       AND CLH.CLAIM = CLA.CLAIM
                                                       WHERE CLA.BRANCH = P.BRANCH 
                                                       AND CLA.POLICY = P.POLICY 
                                                       AND TRIM(CLH.OPER_TYPE) IN (SELECT CAST(TCL.OPERATION AS VARCHAR(2))
                                                  		                       FROM USINSUG01.TAB_CL_OPE TCL
                                                  		                       WHERE (TCL.RESERVE = 1 OR TCL.AJUSTES = 1 OR TCL.PAY_AMOUNT = 1)) 
                                                       AND  CLH.OPERDATE >= '{l_fecha_carga_inicial}'
                                           AND P.EFFECDATE BETWEEN '{p_fecha_inicio}' AND '{p_fecha_fin}'))))

                                           UNION

                                           (SELECT P.USERCOMP, P.COMPANY, P.CERTYPE, P.BRANCH, P.PRODUCT, PSP.SUB_PRODUCT, P.POLICY, CERT.CERTIF, P.TITULARC, P.EFFECDATE ,P.POLITYPE , CERT.EFFECDATE as EFFECDATE_CERT
                                            FROM USINSUV01.POLICY P 
                             	              LEFT JOIN USINSUV01.CERTIFICAT CERT ON P.USERCOMP = CERT.USERCOMP AND P.COMPANY = CERT.COMPANY AND P.CERTYPE = CERT.CERTYPE AND P.BRANCH  = CERT.BRANCH AND P.POLICY  = CERT.policy                             	              
                             	              JOIN /*USBI01."IFRS170_T_RAMOS_POR_TIPO_RIESGO"*/
                                              (SELECT  unnest(ARRAY['usinsuv01','usinsuv01','usinsuv01','usinsuv01','usinsuv01','usinsuv01','usinsuv01',
					                                             'usinsuv01','usinsuv01','usinsuv01','usinsuv01','usinsuv01','usinsuv01','usinsuv01',
					                                             'usinsuv01','usinsuv01','usinsuv01','usinsuv01','usinsuv01','usinsuv01','usinsuv01',
					                                             'usinsuv01','usinsuv01','usinsuv01']) AS "SOURCESCHEMA",  
						                            unnest(ARRAY[5,21,22,23,24,25,27,31,32,33,34,35,36,37,40,41,42,59,68,71,75,77,91,99]) AS "BRANCHCOM",
							                        unnest(ARRAY[1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1]) AS "RISKTYPEN") RTR ON RTR."BRANCHCOM" = P.BRANCH AND  RTR."RISKTYPEN" = 1 AND RTR."SOURCESCHEMA" = 'usinsuv01'
                                            WHERE P.CERTYPE  = '2' 
                                            AND P.STATUS_POL NOT IN ('2', '3')
                                            AND (((P.POLITYPE <> '1' AND CERT.EXPIRDAT < '{l_fecha_carga_inicial}'  OR  CERT.NULLDATE < '{l_fecha_carga_inicial}') 
                                            AND EXISTS (SELECT 1 FROM  USINSUV01.CLAIM CLA    
                                                        JOIN  USINSUV01.CLAIM_HIS CLH  
                                                        ON CLA.USERCOMP = CLH.USERCOMP 
                                                        AND CLA.COMPANY = CLH.COMPANY 
                                                        AND CLA.BRANCH = CLH.BRANCH  
                                                        AND CLH.CLAIM = CLA.CLAIM
                                                        WHERE CLA.BRANCH   = CERT.BRANCH
                                                        AND   CLA.POLICY   = CERT.POLICY
                                                        AND   CLA.CERTIF   = CERT.CERTIF
                                                        AND   TRIM(CLH.OPER_TYPE) IN (SELECT CAST(TCL.OPERATION AS VARCHAR(2)) 
                                                                                      FROM  USINSUG01.TAB_CL_OPE TCL 
                                                                                      WHERE (TCL.RESERVE = 1 OR TCL.AJUSTES = 1 OR TCL.PAY_AMOUNT = 1)) 
                                                        AND  CLH.OPERDATE >= '{l_fecha_carga_inicial}')))
                                            AND P.EFFECDATE BETWEEN '{p_fecha_inicio}' AND '{p_fecha_fin}')JAOS YA NO ES NECESARIO EN INCREMENTAL*/                                          
                                    ) AS PC	
                                    ON  R.USERCOMP = PC.USERCOMP 
                                    AND R.COMPANY  = PC.COMPANY 
                                    AND R.CERTYPE  = PC.CERTYPE
                                    AND R.BRANCH   = PC.BRANCH 
                                    AND R.POLICY   = PC.POLICY 
                                    AND R.CERTIF   = PC.CERTIF  
                                    --AND R.CLIENT   = PC.TITULARC
                                    AND R.EFFECDATE <= PC.EFFECDATE 
                                    AND (R.NULLDATE IS NULL OR R.NULLDATE > PC.EFFECDATE)
                                    WHERE R.ROLE IN (2,8)
                                    -- JAOS
                                    AND R.COMPDATE BETWEEN '{p_fecha_inicio}' AND '{p_fecha_fin}'
                                    --AND PC.EFFECDATE BETWEEN '{p_fecha_inicio}' AND '{p_fecha_fin}' 
                                    LIMIT 100      
                             ) AS TMP
                             '''
    
    l_df_abclrisp_insunix_lpv = glue_context.read.format('jdbc').options(**connection).option("dbtable", l_abclrisp_insunix_lpv).load()

    print("INSUNIX LPV")
    #----------------------------------------------------------------------------------------------------------------------------------#

    l_abclrisp_vtime_lpg = f'''
                              ( SELECT
                               'D' INDDETREC, 
                               'ABCLRISP' TABLAIFRS17, 
                               '' AS PK,
                               '' AS DTPREG,
                               '' AS TIOCPROC,
                               cast(R."DCOMPDATE" as date) AS TIOCFRM,
                               '' AS TIOCTO,
                               'PVG' AS KGIORIGM,
                               PC."NBRANCH" ||'-'|| PC."NPRODUCT" ||'-'|| PC."NPOLICY" ||'-'|| PC."NCERTIF" AS KABAPOL,
                               PC."NBRANCH" ||'-'|| PC."NPRODUCT" ||'-'|| PC."NPOLICY" ||'-'|| PC."NCERTIF" || '-' || R."SCLIENT"  AS KABUNRIS,
                               case PC."SPOLITYPE"  when '1' 
                               then
                               (SELECT COALESCE(cast(GLC."NCOVERGEN" as varchar), '0')
                                              FROM /*USBI01.IFRS170_V_GEN_LIFE_COVER GLC*/
                                              (SELECT GC."NBRANCH",
                                              GC."NPRODUCT",GC."NMODULEC",GC."NCOVER",
                                              GC."DEFFECDATE",GC."DNULLDATE",GC."NCOVERGEN",
                                              GC."NCURRENCY"
										  FROM USVTIMG01."GEN_COVER" GC
										  UNION 
										  SELECT LC."NBRANCH",
                                              LC."NPRODUCT",LC."NMODULEC",LC."NCOVER",
                                              LC."DEFFECDATE",LC."DNULLDATE",LC."NCOVERGEN",
                                              LC."NCURRENCY"
										  FROM USVTIMG01."LIFE_COVER" LC) GLC
                                              JOIN USVTIMG01."COVER" C  
                                              ON  GLC."NBRANCH"   = C."NBRANCH"
                                              AND GLC."NPRODUCT"  = PC."NPRODUCT"
                                              --AND GLC."NCURRENCY" = C."NCURRENCY"
                                              AND GLC."NMODULEC" =  C."NMODULEC"
                                              AND GLC."NCOVER"   =  C."NCOVER"
                                              AND GLC."DEFFECDATE" <= PC."DSTARTDATE"
                                              AND (GLC."DNULLDATE" IS NULL OR GLC."DNULLDATE" > PC."DSTARTDATE")		       		   
                                              WHERE C."SCERTYPE"    = PC."SCERTYPE" 
                                              AND   C."NBRANCH"     = PC."NBRANCH"
                                              AND   C."NPRODUCT"    = PC."NPRODUCT"
                                              AND   C."NPOLICY"     = PC."NPOLICY"
                                              AND   C."NCERTIF"     = PC."NCERTIF"
                                              AND   C."DEFFECDATE" <= PC."DSTARTDATE"
                                              AND  (C."DNULLDATE" IS NULL OR C."DNULLDATE" > PC."DSTARTDATE")
                                              AND  C."NCOVER" = 1
                               ) else (SELECT COALESCE(cast(GLC."NCOVERGEN" as varchar), '0')
                                              FROM /*USBI01.IFRS170_V_GEN_LIFE_COVER GLC*/
                                              (SELECT GC."NBRANCH",
                                              GC."NPRODUCT",GC."NMODULEC",GC."NCOVER",
                                              GC."DEFFECDATE",GC."DNULLDATE",GC."NCOVERGEN",
                                              GC."NCURRENCY"
										  FROM USVTIMG01."GEN_COVER" GC
										  UNION 
										  SELECT LC."NBRANCH",
                                              LC."NPRODUCT",LC."NMODULEC",LC."NCOVER",
                                              LC."DEFFECDATE",LC."DNULLDATE",LC."NCOVERGEN",
                                              LC."NCURRENCY"
										  FROM USVTIMG01."LIFE_COVER" LC) GLC
                                              JOIN USVTIMG01."COVER" C  
                                              ON  GLC."NBRANCH"   = C."NBRANCH"
                                              AND GLC."NPRODUCT"  = PC."NPRODUCT"
                                              --AND GLC."NCURRENCY" = C."NCURRENCY"
                                              AND GLC."NMODULEC" =  C."NMODULEC"
                                              AND GLC."NCOVER"   =  C."NCOVER"
                                              AND GLC."DEFFECDATE" <= PC."DSTARTDATE_CERT"
                                              AND (GLC."DNULLDATE" IS NULL OR GLC."DNULLDATE" > PC."DSTARTDATE_CERT")		       		   
                                              WHERE C."SCERTYPE"    = PC."SCERTYPE" 
                                              AND   C."NBRANCH"     = PC."NBRANCH"
                                              AND   C."NPRODUCT"    = PC."NPRODUCT"
                                              AND   C."NPOLICY"     = PC."NPOLICY"
                                              AND   C."NCERTIF"     = PC."NCERTIF"
                                              AND   C."DEFFECDATE" <= PC."DSTARTDATE_CERT"
                                              AND  (C."DNULLDATE" IS NULL OR C."DNULLDATE" > PC."DSTARTDATE_CERT")
                                              AND  C."NCOVER" = 1
                               )
                               END AS KGCTPCBT,
                               ROW_NUMBER () OVER (PARTITION  BY PC."NBRANCH", PC."NPRODUCT", PC."NPOLICY", PC."NCERTIF" ORDER BY R."SCLIENT") AS DNPESEG,
                               R."SCLIENT" AS KEBENTID_PS,
                               (SELECT DATE_PART('YEAR', AGE(CURRENT_DATE, CLI."DBIRTHDAT")) FROM USVTIMG01."CLIENT" CLI WHERE CLI."SCLIENT" = R."SCLIENT") AS DIDADEAC,
                               '' AS DANOREF, --EN BLANCO
                               '' AS KACEMPR,
                               0 AS VMTSALAR,
                               '' AS KACTPSAL,
                               '' AS TADMEMP,
                               '' AS TADMGRP,
                               '' AS TSAIDGRP,
                               'LPG' AS DCOMPA,
                               '' AS DMARCA,
                               '' AS TDNASCIM,
                               '' AS DQDIASUB,
                               '' AS DQESPING,
                               '' AS KACSEXO,
                               '' AS KACCAE,
                               '' AS DQTRABAL,
                               '' AS DINAPMEN,
                               '' AS DQCOEFEQ,
                               '' AS DQHORSEM, --EN BLANCO
                               '' AS DQMESES,
                               '' AS VMTALIME, --EN BLANCO
                               '' AS DQMESALI, --EN BLANCO
                               '' AS VMTALOJA,
                               '' AS DQMESALO,
                               '' AS VMTRENUM, --EN BLANCO
                               '' AS DQMESREN,
                               '' AS DQDIATRA,
                               '' AS DQCOPRE,
                               '' AS DITINER,
                               '' AS DNOMES,
                               '' AS KACUTILIZ,
                               '' AS VMTDESCO, --EN BLANCO
                               '' AS DFRANQU,  --EN BLANCO
                               '' AS DTARIFA,
                               '' AS DINTIPEXP,
                               '' AS KACESPAN,
                               '' AS DQANIMAL,
                               '' AS KACTIPSG,
                               '' AS DAPROESC,
                               '' AS DCMUDESC,
                               '' AS DQCAPITA,
                               '' AS VTXCOMPA,
                               '' AS DQCAES,
                               '' AS DINEXTTE,
                               '' AS KACCLTARI,
                               '' AS KACTIPVEI,
                               '' AS KACCATRIS,
                               '' AS DINDCOL,
                               '' AS DACONSTR,
                               '' AS DQPESSO1,
                               '' AS DQPESSO2,
                               '' AS DQVIAS,
                               '' AS KACAGRAV,
                               '' AS KACPARTI,
                               '' AS KACSERMD,
                               '' AS KACMRISC,
                               '' AS DINDCON,
                               '' AS DQPRAZO,
                               R."NROLE" AS KACTPPES,
                               '' AS KACESPES, --EN BLANCO
                               '' AS KACMEPES,
                               '' AS TDESPES,  --EN BLANCO
                               '' AS KACTPPRA,
                               '' AS DEMPREST,
                               '' AS VMTPREST,
                               '' AS VMTEMPRE,
                               '' AS VMTPRCRD,
                               '' AS DCONTCGD,
                               '' AS DNCLICGD,
                               '' AS DCERTIFC,
                               coalesce(cast(cast(R."DEFFECDATE" as date) as varchar), '') AS TINICIO,
                               coalesce(cast(cast(R."DNULLDATE" as date) as varchar), '') AS TTERMO,
                               '' AS DNOMEPAR,
                               '' AS KACPROF,
                               '' AS KACACTIV,
                               '' AS KACSACTIV,
                               '' AS VMTSALMD,
                               '' AS DCODSUB,
                               '' AS KACTPCON,
                               '' AS DAREACCV,
                               '' AS DAREACUL,
                               '' AS KACZONAG,
                               '' AS KACTPIDX,
                               '' AS TDTINDEX,
                               '' AS VMTPRMIN,
                               '' AS DCDREGIM,
                               '' AS DQHORTRA,
                               '' AS DQSEMTRA,  --EN BLANCO
                               '' AS DCAMPANH,
                               '' AS KACMODAL,
                               '' AS DENTIDSO,
                               '' AS DLOCREF,
                               '' AS KACINTNI,
                               '' AS KACCLRIS,
                               '' AS KACAMBCB,
                               '' AS KACTRAIN,
                               '' AS DINDCIRS,
                               '' AS DINCERPA,
                               '' AS DINDMOTO,
                               '' AS DMATRIC,
                               '' AS DINDMARK,
                               '' AS KACOPCBT,
                               '' AS VTXINDX,
                               '' AS DAGRIDAD,   --EN BLANCO
                               '' AS KACPAIS_DT, --NO
                               '' AS KACMDAC,    --EN BLANCO
                               '' AS DIDADECOM,  --PENDIENTE
                               '' AS VTXPERINDC,
                               '' AS TPGMYBENEF
                               FROM USVTIMG01."ROLES" R
                               JOIN 
                               (     (SELECT P."SCERTYPE", P."NBRANCH", P."NPRODUCT", P."NPOLICY", CERT."NCERTIF", P."SCLIENT", P."DSTARTDATE" ,P."SPOLITYPE" ,CERT."DSTARTDATE" as "DSTARTDATE_CERT"
                                     FROM USVTIMG01."POLICY" P 
                                     LEFT JOIN USVTIMG01."CERTIFICAT" CERT 
                                     ON  P."SCERTYPE" = CERT."SCERTYPE" 
                                     AND P."NBRANCH"  = CERT."NBRANCH"
                                     AND P."NPRODUCT" = CERT."NPRODUCT"
                                     AND P."NPOLICY"  = CERT."NPOLICY"
                                     JOIN /*USBI01."IFRS170_T_RAMOS_POR_TIPO_RIESGO"*/
                                     (SELECT unnest(ARRAY['usvtimg01','usvtimg01','usvtimg01','usvtimg01','usvtimg01','usvtimg01','usvtimg01','usvtimg01',
                                                          'usvtimg01','usvtimg01','usvtimg01','usvtimg01','usvtimg01','usvtimg01','usvtimg01','usvtimg01','usvtimg01']) AS "SOURCESCHEMA",  
			                      unnest(ARRAY[21, 23, 24, 27, 31, 32, 33, 34, 35, 36, 37, 40, 42, 64, 71, 75, 91]) AS "BRANCHCOM",
			       	         unnest(ARRAY[1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1]) AS "RISKTYPEN") RTR ON RTR."BRANCHCOM" = P."NBRANCH" AND  RTR."RISKTYPEN" = 1 AND RTR."SOURCESCHEMA" = 'usvtimg01'
                                     WHERE P."SCERTYPE" = '2' 
                                     AND P."SSTATUS_POL" NOT IN ('2','3') 
                                     -- JAOS
                                     --AND ( (P."SPOLITYPE" = '1' -- INDIVIDUAL 
                                      --     AND P."DEXPIRDAT" >= '2018-12-31'
                                      --     AND (P."DNULLDATE" IS NULL OR P."DNULLDATE" > '2018-12-31') )
                                      --     OR 
                                      --     (P."SPOLITYPE" <> '1' -- COLECTIVAS 
                                      --     AND CERT."DEXPIRDAT" >= '2018-12-31' 
                                      --     AND (CERT."DNULLDATE" IS NULL OR CERT."DNULLDATE" > '2018-12-31'))))
                                     
                                     /*UNION  -- JAOS YA NO ES NECESARO EN INCREMENTAL

                                     (SELECT P."SCERTYPE", P."NBRANCH", P."NPRODUCT", P."NPOLICY", CERT."NCERTIF", P."SCLIENT", P."DSTARTDATE" ,P."SPOLITYPE" ,CERT."DSTARTDATE" AS "DSTARTDATE_CERT"
                                      FROM USVTIMG01."POLICY" P 
                                      JOIN USVTIMG01."CERTIFICAT" CERT 
                                      ON  P."SCERTYPE" = CERT."SCERTYPE" 
                                      AND P."NBRANCH"  = CERT."NBRANCH"
                                      AND P."NPRODUCT" = CERT."NPRODUCT"
                                      AND P."NPOLICY"  = CERT."NPOLICY"
                                      JOIN /*USBI01."IFRS170_T_RAMOS_POR_TIPO_RIESGO"*/
                                      (SELECT UNNEST(ARRAY['USVTIMG01','USVTIMG01','USVTIMG01','USVTIMG01','USVTIMG01','USVTIMG01','USVTIMG01','USVTIMG01',
                                                          'USVTIMG01','USVTIMG01','USVTIMG01','USVTIMG01','USVTIMG01','USVTIMG01','USVTIMG01','USVTIMG01','USVTIMG01']) AS "SOURCESCHEMA",  
                                              UNNEST(ARRAY[21, 23, 24, 27, 31, 32, 33, 34, 35, 36, 37, 40, 42, 64, 71, 75, 91]) AS "BRANCHCOM",
                                              UNNEST(ARRAY[1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1]) AS "RISKTYPEN") RTR ON RTR."BRANCHCOM" = P."NBRANCH" AND  RTR."RISKTYPEN" = 1 AND RTR."SOURCESCHEMA" = 'USVTIMG01'
                                      JOIN USVTIMG01."CLAIM" CLA ON CLA."SCERTYPE" = POL."SCERTYPE" AND CLA."NPOLICY" = POL."NPOLICY" AND CLA."NBRANCH" = POL."NBRANCH"
                                      JOIN (
                                             SELECT DISTINCT CLH."NCLAIM" 
                                             FROM (
                                                    SELECT CAST("SVALUE" AS INT4) "SVALUE" 
                                                    FROM USVTIMG01."CONDITION_SERV" CS WHERE "NCONDITION" IN (71, 72, 73)
                                                  ) CSV 
                                             JOIN USVTIMG01."CLAIM_HIS" CLH 
                                             ON COALESCE(CLH."NCLAIM", 0) > 0 
                                             AND CLH."NOPER_TYPE" = CSV."SVALUE" 
                                             AND CLH."DOPERDATE" >= '{l_fecha_carga_inicial}'
                                           ) CLH ON CLH."NCLAIM" = CLA."NCLAIM"
                                      WHERE P."SCERTYPE" = '2' 
                                      AND P."SSTATUS_POL" NOT IN ('2','3') 
                                      AND P."SPOLITYPE" = '1' 
                                      AND (P."DEXPIRDAT" < '{l_fecha_carga_inicial}' OR P."DNULLDATE" < '{l_fecha_carga_inicial}'))


                                     UNION

                                     (SELECT P."SCERTYPE", P."NBRANCH", P."NPRODUCT", P."NPOLICY", CERT."NCERTIF", P."SCLIENT", P."DSTARTDATE" ,P."SPOLITYPE" ,CERT."DSTARTDATE" AS "DSTARTDATE_CERT"
                                      FROM USVTIMG01."POLICY" P 
                                      JOIN USVTIMG01."CERTIFICAT" CERT 
                                      ON  P."SCERTYPE" = CERT."SCERTYPE" 
                                      AND P."NBRANCH"  = CERT."NBRANCH"
                                      AND P."NPRODUCT" = CERT."NPRODUCT"
                                      AND P."NPOLICY"  = CERT."NPOLICY"
                                      JOIN /*USBI01."IFRS170_T_RAMOS_POR_TIPO_RIESGO"*/
                                      (SELECT UNNEST(ARRAY['USVTIMG01','USVTIMG01','USVTIMG01','USVTIMG01','USVTIMG01','USVTIMG01','USVTIMG01','USVTIMG01',
                                                          'USVTIMG01','USVTIMG01','USVTIMG01','USVTIMG01','USVTIMG01','USVTIMG01','USVTIMG01','USVTIMG01','USVTIMG01']) AS "SOURCESCHEMA",  
                                              UNNEST(ARRAY[21, 23, 24, 27, 31, 32, 33, 34, 35, 36, 37, 40, 42, 64, 71, 75, 91]) AS "BRANCHCOM",
                                              UNNEST(ARRAY[1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1]) AS "RISKTYPEN") RTR ON RTR."BRANCHCOM" = P."NBRANCH" AND  RTR."RISKTYPEN" = 1 AND RTR."SOURCESCHEMA" = 'USVTIMG01'
                                      JOIN USVTIMG01."CLAIM" CLA ON CLA."SCERTYPE" = CERT."SCERTYPE" AND CLA."NBRANCH" = CERT."NBRANCH" AND CLA."NPOLICY" = CERT."NPOLICY"  AND CLA."NCERTIF" =  CERT."NCERTIF"
                                      JOIN (
                                            SELECT DISTINCT CLH."NCLAIM" 
                                            FROM (
                                                  SELECT CAST("SVALUE" AS INT4) "SVALUE" 
                                                  FROM USVTIMG01."CONDITION_SERV" CS WHERE "NCONDITION" IN (71, 72, 73)
                                                 ) CSV 
                                            JOIN USVTIMG01."CLAIM_HIS" CLH 
                                            ON COALESCE(CLH."NCLAIM", 0) > 0 
                                            AND CLH."NOPER_TYPE" = CSV."SVALUE" 
                                            AND CLH."DOPERDATE" >= '{l_fecha_carga_inicial}'
                                           ) CLH ON CLH."NCLAIM" = CLA."NCLAIM"
                                      WHERE P."SCERTYPE" = '2' 
                                      AND P."SSTATUS_POL" NOT IN ('2','3') 
                                      AND P."SPOLITYPE" <> '1' 
                                      AND (CERT."DEXPIRDAT" < '{l_fecha_carga_inicial}' OR CERT."DNULLDATE" < '{l_fecha_carga_inicial}'))   JAOS NO ES NECESARIO EN EL INCREMENTAL*/
                               ) AS PC	
                               ON  R."SCERTYPE"  = PC."SCERTYPE"
                               AND R."NBRANCH"   = PC."NBRANCH" 
                               AND R."NPRODUCT"  = PC."NPRODUCT"
                               AND R."NPOLICY"   = PC."NPOLICY" 
                               AND R."NCERTIF"   = PC."NCERTIF"  
                               AND R."DEFFECDATE" <= PC."DSTARTDATE" 
                               AND (R."DNULLDATE" IS NULL OR R."DNULLDATE" > PC."DSTARTDATE")
                               WHERE R."NROLE" IN (2,8) 
                               -- JAOS
                                AND R.DCOMPDATE BETWEEN '{p_fecha_inicio}' AND '{p_fecha_fin}'
                               LIMIT 100
                              ) AS TMP
                            '''
    
    l_df_abclrisp_vtime_lpg = glue_context.read.format('jdbc').options(**connection).option("dbtable", l_abclrisp_vtime_lpg).load()

    print("VTIME LPG")

    l_abclrisp_vtime_lpv = f'''
                           (SELECT
                           'D' INDDETREC, 
                           'ABCLRISP' TABLAIFRS17, 
                           '' AS PK,
                           '' AS DTPREG,
                           '' AS TIOCPROC,
                           cast(R."DCOMPDATE" as date) AS TIOCFRM,
                           '' AS TIOCTO,
                           'PVV' AS KGIORIGM,
                           PC."NBRANCH" ||'-'|| PC."NPRODUCT" ||'-'|| PC."NPOLICY" ||'-'|| PC."NCERTIF" AS KABAPOL,
                           PC."NBRANCH" ||'-'|| PC."NPRODUCT" ||'-'|| PC."NPOLICY" ||'-'|| PC."NCERTIF" || '-' || R."SCLIENT"  AS KABUNRIS,
                           case PC."SPOLITYPE"  when '1' 
                           then
                           ( SELECT COALESCE(cast(GC."NCOVERGEN" as varchar), '0')
                                      FROM USVTIMV01."LIFE_COVER" GC 
                                      JOIN USVTIMV01."COVER" C  
                                      ON  GC."NBRANCH"   = C."NBRANCH"
                                      AND GC."NPRODUCT"  = PC."NPRODUCT"
                                      AND GC."NCURRENCY" = C."NCURRENCY"
                                      AND GC."NMODULEC" =  C."NMODULEC"
                                      AND GC."NCOVER"   =  C."NCOVER"
                                      AND GC."DEFFECDATE" <= PC."DSTARTDATE"
                           		   AND (GC."DNULLDATE" IS NULL OR GC."DNULLDATE" > PC."DSTARTDATE")		       		   
                                      WHERE C."SCERTYPE"    = PC."SCERTYPE" 
                                      AND   C."NBRANCH"     = PC."NBRANCH"
                                      AND   C."NPRODUCT"    = PC."NPRODUCT"
                                      AND   C."NPOLICY"     = PC."NPOLICY"
                                      AND   C."NCERTIF"     = PC."NCERTIF"
                                      AND   C."DEFFECDATE" <= PC."DSTARTDATE"
                                      AND  (C."DNULLDATE" IS NULL OR C."DNULLDATE" > PC."DSTARTDATE")
                                      AND  C."NCOVER" = 1 LIMIT 1                                      
                           ) else ( SELECT COALESCE(cast(GC."NCOVERGEN" as varchar), '0')
                                      FROM USVTIMV01."LIFE_COVER" GC 
                                      JOIN USVTIMV01."COVER" C  
                                      ON  GC."NBRANCH"   = C."NBRANCH"
                                      AND GC."NPRODUCT"  = PC."NPRODUCT"
                                      AND GC."NCURRENCY" = C."NCURRENCY"
                                      AND GC."NMODULEC" =  C."NMODULEC"
                                      AND GC."NCOVER"   =  C."NCOVER"
                                      AND GC."DEFFECDATE" <= PC."DSTARTDATE_CERT"
                           		   AND (GC."DNULLDATE" IS NULL OR GC."DNULLDATE" > PC."DSTARTDATE_CERT")		       		   
                                      WHERE C."SCERTYPE"    = PC."SCERTYPE" 
                                      AND   C."NBRANCH"     = PC."NBRANCH"
                                      AND   C."NPRODUCT"    = PC."NPRODUCT"
                                      AND   C."NPOLICY"     = PC."NPOLICY"
                                      AND   C."NCERTIF"     = PC."NCERTIF"
                                      AND   C."DEFFECDATE" <= PC."DSTARTDATE_CERT"
                                      AND  (C."DNULLDATE" IS NULL OR C."DNULLDATE" > PC."DSTARTDATE_CERT")
                                      AND  C."NCOVER" = 1 LIMIT 1                                      
                           )
                           END AS KGCTPCBT,
                           ROW_NUMBER () OVER (PARTITION  BY PC."NBRANCH", PC."NPRODUCT", PC."NPOLICY", PC."NCERTIF" ORDER BY R."SCLIENT") AS DNPESEG,
                           R."SCLIENT" AS KEBENTID_PS,
                           (SELECT DATE_PART('YEAR', AGE(CURRENT_DATE, CLI."DBIRTHDAT")) FROM USVTIMG01."CLIENT" CLI WHERE CLI."SCLIENT" = R."SCLIENT") AS DIDADEAC,
                           '' AS DANOREF, --EN BLANCO
                           '' AS KACEMPR,
                           0 AS VMTSALAR,
                           '' AS KACTPSAL,
                           '' AS TADMEMP,
                           '' AS TADMGRP,
                           '' AS TSAIDGRP,
                           'LPV' AS DCOMPA,
                           '' AS DMARCA,
                           '' AS TDNASCIM,
                           '' AS DQDIASUB,
                           '' AS DQESPING,
                           '' AS KACSEXO,
                           '' AS KACCAE,
                           '' AS DQTRABAL,
                           '' AS DINAPMEN,
                           '' AS DQCOEFEQ,
                           '' AS DQHORSEM, --EN BLANCO
                           '' AS DQMESES,
                           '' AS VMTALIME, --EN BLANCO
                           '' AS DQMESALI, --EN BLANCO
                           '' AS VMTALOJA,
                           '' AS DQMESALO,
                           '' AS VMTRENUM, --EN BLANCO
                           '' AS DQMESREN,
                           '' AS DQDIATRA,
                           '' AS DQCOPRE,
                           '' AS DITINER,
                           '' AS DNOMES,
                           '' AS KACUTILIZ,
                           '' AS VMTDESCO, --EN BLANCO
                           '' AS DFRANQU,  --EN BLANCO
                           '' AS DTARIFA,
                           '' AS DINTIPEXP,
                           '' AS KACESPAN,
                           '' AS DQANIMAL,
                           '' AS KACTIPSG,
                           '' AS DAPROESC,
                           '' AS DCMUDESC,
                           '' AS DQCAPITA,
                           '' AS VTXCOMPA,
                           '' AS DQCAES,
                           '' AS DINEXTTE,
                           '' AS KACCLTARI,
                           '' AS KACTIPVEI,
                           '' AS KACCATRIS,
                           '' AS DINDCOL,
                           '' AS DACONSTR,
                           '' AS DQPESSO1,
                           '' AS DQPESSO2,
                           '' AS DQVIAS,
                           '' AS KACAGRAV,
                           '' AS KACPARTI,
                           '' AS KACSERMD,
                           '' AS KACMRISC,
                           '' AS DINDCON,
                           '' AS DQPRAZO,
                           R."NROLE" AS KACTPPES,
                           '' AS KACESPES, --EN BLANCO
                           '' AS KACMEPES,
                           '' AS TDESPES,  --EN BLANCO
                           '' AS KACTPPRA,
                           '' AS DEMPREST,
                           '' AS VMTPREST,
                           '' AS VMTEMPRE,
                           '' AS VMTPRCRD,
                           '' AS DCONTCGD,
                           '' AS DNCLICGD,
                           '' AS DCERTIFC,
                           R."DEFFECDATE" AS TINICIO,
                           coalesce(cast(cast(R."DNULLDATE" as date) as varchar), '') AS TTERMO,
                           '' AS DNOMEPAR,
                           '' AS KACPROF,
                           '' AS KACACTIV,
                           '' AS KACSACTIV,
                           '' AS VMTSALMD,
                           '' AS DCODSUB,
                           '' AS KACTPCON,
                           '' AS DAREACCV,
                           '' AS DAREACUL,
                           '' AS KACZONAG,
                           '' AS KACTPIDX,
                           '' AS TDTINDEX,
                           '' AS VMTPRMIN,
                           '' AS DCDREGIM,
                           '' AS DQHORTRA,
                           '' AS DQSEMTRA,  --EN BLANCO
                           '' AS DCAMPANH,
                           '' AS KACMODAL,
                           '' AS DENTIDSO,
                           '' AS DLOCREF,
                           '' AS KACINTNI,
                           '' AS KACCLRIS,
                           '' AS KACAMBCB,
                           '' AS KACTRAIN,
                           '' AS DINDCIRS,
                           '' AS DINCERPA,
                           '' AS DINDMOTO,
                           '' AS DMATRIC,
                           '' AS DINDMARK,
                           '' AS KACOPCBT,
                           '' AS VTXINDX,
                           '' AS DAGRIDAD,   --EN BLANCO
                           '' AS KACPAIS_DT, --NO
                           '' AS KACMDAC,    --EN BLANCO
                           '' AS DIDADECOM,  --PENDIENTE
                           '' AS VTXPERINDC,
                           '' AS TPGMYBENEF
                           FROM USVTIMV01."ROLES" R
                           JOIN (   SELECT P."SCERTYPE", P."NBRANCH", P."NPRODUCT", P."NPOLICY", CERT."NCERTIF", P."SCLIENT", P."DSTARTDATE" ,P."SPOLITYPE" ,CERT."DSTARTDATE" as "DSTARTDATE_CERT"
                                    FROM USVTIMV01."POLICY" P 
                           	      LEFT JOIN USVTIMV01."CERTIFICAT" CERT 
                           	      ON  P."SCERTYPE" = CERT."SCERTYPE" 
                           	      AND P."NBRANCH"  = CERT."NBRANCH"
                           	      AND P."NPRODUCT" = CERT."NPRODUCT"
                           	      AND P."NPOLICY"  = CERT."NPOLICY"
                           	      JOIN /*USBI01."IFRS170_T_RAMOS_POR_TIPO_RIESGO"*/
                                    (SELECT unnest(ARRAY['usvtimv01','usvtimv01','usvtimv01','usvtimv01','usvtimv01','usvtimv01','usvtimv01','usvtimv01','usvtimv01',
                                                      'usvtimv01','usvtimv01','usvtimv01','usvtimv01','usvtimv01','usvtimv01','usvtimv01','usvtimv01']) AS "SOURCESCHEMA",  
						        unnest(ARRAY[21, 23, 24, 27, 31, 32, 33, 34, 35, 36, 37, 40, 42, 64, 71, 75, 91]) AS "BRANCHCOM",
						        unnest(ARRAY[1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1]) AS "RISKTYPEN") RTR ON RTR."BRANCHCOM" = P."NBRANCH" AND  RTR."RISKTYPEN" = 1 AND RTR."SOURCESCHEMA" = 'usvtimv01'
                           	      WHERE P."SCERTYPE" = '2' 
                                    AND P."SSTATUS_POL" NOT IN ('2','3') 
                                  -- JAOS
                                  --  AND ((P."SPOLITYPE" = '1' -- INDIVIDUAL 
                                  --  AND P."DEXPIRDAT" >= '{l_fecha_carga_inicial}' 
                                  --  AND (P."DNULLDATE" IS NULL OR P."DNULLDATE" > '{l_fecha_carga_inicial}') )
                                  --  OR 
                                  --  (P."SPOLITYPE" <> '1' -- COLECTIVAS 
                                  --  AND CERT."DEXPIRDAT" >= '{l_fecha_carga_inicial}' 
                                  -- AND (CERT."DNULLDATE" IS NULL OR CERT."DNULLDATE" > '{l_fecha_carga_inicial}'))
                                  --  AND p."DSTARTDATE" between '{p_fecha_inicio}' and '{p_fecha_fin}')

                                    /*UNION   -- JAOS YA NO ES NECESARIO
                              
                                    (SELECT P."SCERTYPE", P."NBRANCH", P."NPRODUCT", P."NPOLICY", CERT."NCERTIF", P."SCLIENT", P."DSTARTDATE" ,P."SPOLITYPE" ,CERT."DSTARTDATE" AS "DSTARTDATE_CERT"
                                     FROM USVTIMV01."POLICY" P 
                                     JOIN USVTIMV01."CERTIFICAT" CERT 
                                     ON  P."SCERTYPE" = CERT."SCERTYPE" 
                                     AND P."NBRANCH"  = CERT."NBRANCH"
                                     AND P."NPRODUCT" = CERT."NPRODUCT"
                                     AND P."NPOLICY"  = CERT."NPOLICY"
                                     JOIN /*USBI01."IFRS170_T_RAMOS_POR_TIPO_RIESGO"*/
                                           (SELECT unnest(ARRAY['usvtimv01','usvtimv01','usvtimv01','usvtimv01','usvtimv01','usvtimv01','usvtimv01','usvtimv01','usvtimv01',
                                                                'usvtimv01','usvtimv01','usvtimv01','usvtimv01','usvtimv01','usvtimv01','usvtimv01','usvtimv01']) AS "SOURCESCHEMA",  
						        unnest(ARRAY[21, 23, 24, 27, 31, 32, 33, 34, 35, 36, 37, 40, 42, 64, 71, 75, 91]) AS "BRANCHCOM",
						        unnest(ARRAY[1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1]) AS "RISKTYPEN") RTR ON RTR."BRANCHCOM" = P."NBRANCH" AND  RTR."RISKTYPEN" = 1 AND RTR."SOURCESCHEMA" = 'usvtimv01'
                                     JOIN USVTIMV01."CLAIM" CLA ON CLA."SCERTYPE" = POL."SCERTYPE" AND CLA."NPOLICY" = POL."NPOLICY" AND CLA."NBRANCH" = POL."NBRANCH"
                                     JOIN (
                                           SELECT DISTINCT CLH."NCLAIM" FROM (SELECT CAST("SVALUE" AS INT4) "SVALUE" FROM USVTIMV01."CONDITION_SERV" CS WHERE "NCONDITION" IN (71, 72, 73)) CSV 
                                           JOIN USVTIMV01."CLAIM_HIS" CLH ON COALESCE(CLH."NCLAIM", 0) > 0 AND CLH."NOPER_TYPE" = CSV."SVALUE" AND CLH."DOPERDATE" >= '{l_fecha_carga_inicial}'
                                          ) CLH ON CLH."NCLAIM" = CLA."NCLAIM"
                                     WHERE P."SCERTYPE" = '2' 
                                     AND P."SSTATUS_POL" NOT IN ('2','3') 
                                     AND P."SPOLITYPE" = '1' 
                                     AND (P."DEXPIRDAT" < '{l_fecha_carga_inicial}' OR P."DNULLDATE" < '{l_fecha_carga_inicial}'))

                                     UNION

                                    (SELECT P."SCERTYPE", P."NBRANCH", P."NPRODUCT", P."NPOLICY", CERT."NCERTIF", P."SCLIENT", P."DSTARTDATE" ,P."SPOLITYPE" ,CERT."DSTARTDATE" AS "DSTARTDATE_CERT"
                                    FROM USVTIMV01."POLICY" P 
                                    JOIN USVTIMV01."CERTIFICAT" CERT 
                                    ON  P."SCERTYPE" = CERT."SCERTYPE" 
                                    AND P."NBRANCH"  = CERT."NBRANCH"
                                    AND P."NPRODUCT" = CERT."NPRODUCT"
                                    AND P."NPOLICY"  = CERT."NPOLICY"
                                    JOIN /*USBI01."IFRS170_T_RAMOS_POR_TIPO_RIESGO"*/
                                    (SELECT unnest(ARRAY['usvtimv01','usvtimv01','usvtimv01','usvtimv01','usvtimv01','usvtimv01','usvtimv01','usvtimv01','usvtimv01',
                                                                'usvtimv01','usvtimv01','usvtimv01','usvtimv01','usvtimv01','usvtimv01','usvtimv01','usvtimv01']) AS "SOURCESCHEMA",  
						        unnest(ARRAY[21, 23, 24, 27, 31, 32, 33, 34, 35, 36, 37, 40, 42, 64, 71, 75, 91]) AS "BRANCHCOM",
						        unnest(ARRAY[1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1]) AS "RISKTYPEN") RTR ON RTR."BRANCHCOM" = P."NBRANCH" AND  RTR."RISKTYPEN" = 1 AND RTR."SOURCESCHEMA" = 'usvtimv01'
                                    JOIN USVTIMV01."CLAIM" CLA ON CLA."SCERTYPE" = CERT."SCERTYPE" AND CLA."NBRANCH" = CERT."NBRANCH" AND CLA."NPOLICY" = CERT."NPOLICY"  AND CLA."NCERTIF" =  CERT."NCERTIF"
                                    JOIN (
                                           SELECT DISTINCT CLH."NCLAIM" FROM (SELECT CAST("SVALUE" AS INT4) "SVALUE" 
                                           FROM USVTIMV01."CONDITION_SERV" CS WHERE "NCONDITION" IN (71, 72, 73)) CSV 
                                           JOIN USVTIMV01."CLAIM_HIS" CLH 
                                           ON COALESCE(CLH."NCLAIM", 0) > 0 
                                           AND CLH."NOPER_TYPE" = CSV."SVALUE" 
                                           AND CLH."DOPERDATE" >= '{l_fecha_carga_inicial}'
                                         ) CLH ON CLH."NCLAIM" = CLA."NCLAIM"
                                    WHERE P."SCERTYPE" = '2' 
                                    AND P."SSTATUS_POL" NOT IN ('2','3') 
                                    AND P."SPOLITYPE" <> '1' 
                                    AND (CERT."DEXPIRDAT" < '{l_fecha_carga_inicial}' OR CERT."DNULLDATE" < '{l_fecha_carga_inicial}'))  JAOS YA NO ES NECESARIO EN EL INCREMENTAL */ 

                              ) AS PC	
                           ON  R."SCERTYPE"  = PC."SCERTYPE"
                           AND R."NBRANCH"   = PC."NBRANCH" 
                           AND R."NPRODUCT"  = PC."NPRODUCT"
                           AND R."NPOLICY"   = PC."NPOLICY" 
                           AND R."NCERTIF"   = PC."NCERTIF"  
                           AND R."DEFFECDATE" <= PC."DSTARTDATE" 
                           AND (R."DNULLDATE" IS NULL OR R."DNULLDATE" > PC."DSTARTDATE")
                           WHERE R."NROLE" IN (2,8)
                           -- JAOS
                           AND R.DCOMPDATE BETWEEN '{p_fecha_inicio}' AND '{p_fecha_fin}' 
                           LIMIT 100) AS VTIME_LPV
                           '''
    
    l_df_abclrisp_vtime_lpv = glue_context.read.format('jdbc').options(**connection).option("dbtable", l_abclrisp_vtime_lpv).load()
    
    print("VTIME LPV")

    #----------------------------------------------------------------------------------------------------------------------------------#

    l_abclrisp_insis_lpv = f'''
                           (SELECT
                            'D' INDDETREC, 
                            'ABCLRISP' TABLAIFRS17, 
                            '' AS PK,
                            '' AS DTPREG,
                            '' AS TIOCPROC,
                            cast(IO."INSR_BEGIN" as date) AS TIOCFRM,
                            '' AS TIOCTO,
                            'PNV' AS KGIORIGM,
                            SUBSTRING(CAST(P."POLICY_ID" AS VARCHAR),6,12) AS KABAPOL,
                            SUBSTRING(CAST(P."POLICY_ID" AS VARCHAR),6,12) || '-' || coalesce(cast(IO."OBJECT_ID" as varchar), '0') AS KABUNRIS,
                            '' AS KGCTPCBT, --EN BLANCO
                            ROW_NUMBER () OVER (PARTITION  BY P."ATTR1", P."ATTR2", P."POLICY_ID", P."POLICY_NO" /*ORDER BY R."SCLIENT"*/) AS DNPESEG,
                            (
                            SELECT 'PAE' || '-' || ILPI."LEGACY_ID" 
                            FROM USINSIV01."INTRF_LPV_PEOPLE_IDS" ILPI
                            WHERE ILPI."MAN_ID" = OA."MAN_ID"
                            )
                            AS KEBENTID_PS,
                            '' AS DIDADEAC, --PENDIENTE
                            '' AS DANOREF,  --EN BLANCO
                            '' AS KACEMPR,
                            cast(OA."OAIP1" as numeric(12,2)) AS VMTSALAR,
                            '' AS KACTPSAL,
                            '' AS TADMEMP,
                            IO."INSR_BEGIN" AS TADMGRP,
                            '' AS TSAIDGRP,
                            'LPV' AS DCOMPA,
                            '' AS DMARCA,
                            '' AS TDNASCIM,
                            '' AS DQDIASUB,
                            '' AS DQESPING,
                            '' AS KACSEXO,
                            '' AS KACCAE,
                            '' AS DQTRABAL,
                            '' AS DINAPMEN,
                            '' AS DQCOEFEQ,
                            '' AS DQHORSEM, --EN BLANCO
                            '' AS DQMESES,
                            '' AS VMTALIME, --EN BLANCO
                            '' AS DQMESALI, --EN BLANCO
                            '' AS VMTALOJA,
                            '' AS DQMESALO,
                            '' AS VMTRENUM, --EN BLANCO
                            '' AS DQMESREN,
                            '' AS DQDIATRA,
                            '' AS DQCOPRE,
                            '' AS DITINER,
                            '' AS DNOMES,
                            '' AS KACUTILIZ,
                            '' AS VMTDESCO, --EN BLANCO
                            '' AS DFRANQU,  --EN BLANCO
                            '' AS DTARIFA,
                            '' AS DINTIPEXP,
                            '' AS KACESPAN,
                            '' AS DQANIMAL,
                            '' AS KACTIPSG,
                            '' AS DAPROESC,
                            '' AS DCMUDESC,
                            '' AS DQCAPITA,
                            '' AS VTXCOMPA,
                            '' AS DQCAES,
                            '' AS DINEXTTE,
                            '' AS KACCLTARI,
                            '' AS KACTIPVEI,
                            '' AS KACCATRIS,
                            '' AS DINDCOL,
                            '' AS DACONSTR,
                            '' AS DQPESSO1,
                            '' AS DQPESSO2,
                            '' AS DQVIAS,
                            '' AS KACAGRAV,
                            '' AS KACPARTI,
                            '' AS KACSERMD,
                            '' AS KACMRISC,
                            '' AS DINDCON,
                            '' AS DQPRAZO,
                            '' AS KACTPPES,
                            IO."OBJECT_STATE" AS KACESPES, --EN BLANCO
                            '' AS KACMEPES,
                            '' AS TDESPES,  --EN BLANCO
                            '' AS KACTPPRA,
                            '' AS DEMPREST,
                            '' AS VMTPREST,
                            '' AS VMTEMPRE,
                            '' AS VMTPRCRD,
                            '' AS DCONTCGD,
                            '' AS DNCLICGD,
                            '' AS DCERTIFC,
                            '' AS TINICIO,  --EN BLANCO
                            ''  AS TTERMO,  --EN BLANCO
                            '' AS DNOMEPAR,
                            '' AS KACPROF,
                            '' AS KACACTIV,
                            '' AS KACSACTIV,
                            '' AS VMTSALMD,
                            '' AS DCODSUB,
                            '' AS KACTPCON,
                            '' AS DAREACCV,
                            '' AS DAREACUL,
                            '' AS KACZONAG,
                            '' AS KACTPIDX,
                            '' AS TDTINDEX,
                            '' AS VMTPRMIN,
                            '' AS DCDREGIM,
                            '' AS DQHORTRA,
                            '' AS DQSEMTRA,  --EN BLANCO
                            '' AS DCAMPANH,
                            '' AS KACMODAL,
                            '' AS DENTIDSO,
                            '' AS DLOCREF,
                            '' AS KACINTNI,
                            '' AS KACCLRIS,
                            '' AS KACAMBCB,
                            '' AS KACTRAIN,
                            '' AS DINDCIRS,
                            '' AS DINCERPA,
                            '' AS DINDMOTO,
                            '' AS DMATRIC,
                            '' AS DINDMARK,
                            '' AS KACOPCBT,
                            '' AS VTXINDX,
                            '' AS DAGRIDAD,   --EN BLANCO
                            '' AS KACPAIS_DT, --NO
                            '' AS KACMDAC,    --EN BLANCO
                            OA."AGE" AS DIDADECOM,  
                            '' AS VTXPERINDC,
                            '' AS TPGMYBENEF
                            FROM USINSIV01."INSURED_OBJECT" IO
                            JOIN USINSIV01."O_ACCINSURED" OA ON OA."OBJECT_ID" = IO."OBJECT_ID"
                            JOIN USINSIV01."POLICY" P on P."POLICY_ID" = IO."POLICY_ID" and P."INSR_TYPE" = IO."INSR_TYPE"
                            LEFT JOIN USINSIV01."POLICY_ENG_POLICIES" PP ON P."POLICY_ID" = PP."POLICY_ID"
                            WHERE --JAOS (P."INSR_END" >= '{l_fecha_carga_inicial}'
                                  --  OR  (P."INSR_END" < '{l_fecha_carga_inicial}' AND EXISTS (
                        	  --    	                              		           SELECT 1 FROM USINSIV01."CLAIM" C
                        	  --    	                              		           JOIN USINSIV01."CLAIM_OBJECTS" CO ON CO."CLAIM_ID" = C."CLAIM_ID" AND CO."POLICY_ID" = C."POLICY_ID"
                        	   --   	                              		           JOIN USINSIV01."CLAIM_RESERVE_HISTORY" CRH ON CO."CLAIM_ID" = CRH."CLAIM_ID" AND CO."REQUEST_ID" = CO."REQUEST_ID" AND CO."CLAIM_OBJ_SEQ" = CRH."CLAIM_OBJECT_SEQ"
                        	    --  	                              		           WHERE C."POLICY_ID" = P."POLICY_ID"
                        	     -- 	                              		           AND "OP_TYPE" IN ('REG','EST','CLC','PAYMCONF','PAYMINV')
                        	     -- 	                              		           AND CAST(CRH."REGISTRATION_DATE" AS DATE) >= '{l_fecha_carga_inicial}'
                        	      --	                                                    )
                                       -- )
                                --)
                            AND IO."REGISTRATION_DATE" BETWEEN '{p_fecha_inicio}' AND '{p_fecha_fin}') AS PNV'''
                            --AND P."REGISTRATION_DATE" BETWEEN '{p_fecha_inicio}' AND '{p_fecha_fin}') AS PNV'''
    
    l_df_abclrisp_insis_lpv = glue_context.read.format('jdbc').options(**connection).option("dbtable", l_abclrisp_insis_lpv).load()

    print("INSIS LPV")
    
    l_df_abclrisp = l_df_abclrisp_insunix_lpg.union(l_df_abclrisp_insunix_lpv).union(l_df_abclrisp_vtime_lpg).union(l_df_abclrisp_vtime_lpv).union(l_df_abclrisp_insis_lpv)

    l_df_abclrisp = l_df_abclrisp.withColumn("KGCTPCBT" , coalesce(col("KGCTPCBT"),lit("").cast(StringType()))).withColumn("DNPESEG", coalesce(col("DNPESEG"),lit("").cast(StringType()))).withColumn("DIDADEAC", coalesce(col("DIDADEAC"),lit("").cast(StringType()))).withColumn("VMTSALAR", format_number("VMTSALAR",2)).withColumn("KACTPPES", coalesce(col("KACTPPES"),lit("").cast(StringType()))).withColumn("TINICIO", coalesce(col("TINICIO"),lit("").cast(DateType()))).withColumn("TTERMO", coalesce(col("TTERMO"),lit("").cast(StringType())))

    return l_df_abclrisp