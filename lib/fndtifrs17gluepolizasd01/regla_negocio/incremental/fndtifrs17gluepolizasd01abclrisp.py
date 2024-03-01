from pyspark.sql.types import StringType , DateType
from pyspark.sql.functions import col , coalesce , lit , format_number

def get_data(glue_context, connection, p_fecha_inicio, p_fecha_fin):
    
    l_abclrisp_insunix_lpg = f'''
                             (
                                   select
                                    'd' inddetrec, 
                                    'abclrisp' tablaifrs17, 
                                    '' as pk,
                                    '' as dtpreg,
                                    '' as tiocproc,
                                    cast(pcr.compdate as date) as tiocfrm,
                                    '' as tiocto,
                                    'pig' as kgiorigm,
                                    pcr.branch ||'-'|| coalesce (pcr.product, 0) ||'-'|| coalesce(pcr.sub_product, 0) ||'-'|| pcr.policy ||'-'|| pcr.certif as kabapol,
                                    pcr.branch ||'-'|| coalesce (pcr.product, 0) ||'-'|| pcr.policy ||'-'|| pcr.certif || '-' || (select evi.scod_vt  from usinsug01.equi_vt_inx evi where evi.scod_inx = pcr.client)  as kabunris,
                                    case pcr.politype
                                    when '1' then ( select coalesce(gc.covergen, 0) ||'-'|| coalesce(gc.currency, 0)
                                                from usinsug01.gen_cover gc 
                                                join usinsug01.cover c  
                                                on  gc.usercomp    = c.usercomp 
                                                and gc.company     = c.company 
                                                and gc.branch      = c.branch
                                                and gc.product     = pcr.product
                                                and gc.sub_product = pcr.sub_product
                                                and gc.currency    = c.currency
                                                and gc.modulec     = c.modulec
                                                and gc.cover       = c.cover
                                                and gc.effecdate <=  pcr.p_effecdate
                                                and (gc.nulldate is null or gc.nulldate > pcr.p_effecdate)		       		   
                                                where c.usercomp   = pcr.usercomp 
                                                and   c.company    = pcr.company 
                                                and   c.certype    = '2' 
                                                and   c.branch     = pcr.branch 
                                                and   c.policy     = pcr.policy
                                                and   c.certif     = pcr.certif  
                                                and   c.effecdate <= pcr.p_effecdate
                                                and  (c.nulldate is null or c.nulldate > pcr.p_effecdate)
                                                and  c.cover = 1 limit 1) 
                                    else   ( select coalesce(gc.covergen, 0) ||'-'|| coalesce(gc.currency, 0)
                                    from usinsug01.gen_cover gc 
                                    join usinsug01.cover c  
                                    on  gc.usercomp    = c.usercomp 
                                    and gc.company     = c.company 
                                    and gc.branch      = c.branch
                                    and gc.product     = pcr.product
                                    and gc.sub_product = pcr.sub_product
                                    and gc.currency    = c.currency
                                    and gc.modulec     =  c.modulec
                                    and gc.cover       =  c.cover
                                    and gc.effecdate <=   pcr.c_effecdate
                                    and (gc.nulldate is null or gc.nulldate > pcr.c_effecdate)		       		   
                                    where c.usercomp   = pcr.usercomp 
                                    and   c.company    = pcr.company 
                                    and   c.certype    = '2' 
                                          and   c.branch     = pcr.branch 
                                          and   c.policy     = pcr.policy
                                          and   c.certif     = pcr.certif  
                                          and   c.effecdate <= pcr.c_effecdate
                                          and  (c.nulldate is null or c.nulldate > pcr.c_effecdate)
                                          and  c.cover = 1 limit 1)
                                    end as kgctpcbt,
                                    row_number () over (partition  by pcr.branch, coalesce (pcr.product, 0), pcr.policy, pcr.certif order by pcr.client) as dnpeseg,
                                    (select 'pae' || '-' || evi.scod_vt  from usinsug01.equi_vt_inx evi where evi.scod_inx  = pcr.client) as kebentid_ps,
                                    (select date_part('year', age(current_date, cli.birthdat)) from usinsug01.client cli where cli.code = pcr.client) as didadeac,
                                    '' as danoref, --en blanco
                                    '' as kacempr,
                                    case 
                                    pcr.branch  when 23 then (select coalesce (insu_he.anual_sal, 0) from usinsug01.insured_he insu_he
                                                            where insu_he.usercomp = pcr.usercomp
                                                            and   insu_he.company  = pcr.company
                                                            and   insu_he.certype  = pcr.certype
                                                            and   insu_he.branch   = pcr.branch
                                                            and   insu_he.policy   = pcr.policy
                                                            and   insu_he.certif   = pcr.certif
                                                            and   insu_he.client   = pcr.client
                                                            and   insu_he.effecdate <= pcr.p_effecdate
                                                            and   (insu_he.nulldate is null or insu_he.nulldate > pcr.p_effecdate))
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
                                    pcr.role as kactppes,
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
                                    pcr.r_effecdate as tinicio,
                                    pcr.r_nulldate  as ttermo,
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
                                    case pcr.branch
                                    when 23 
                                    then (select coalesce (insu_he.age_limit, 0) from usinsug01.insured_he insu_he
                                          where insu_he.usercomp = pcr.usercomp
                                          and   insu_he.company  = pcr.company
                                          and   insu_he.certype  = pcr.certype
                                          and   insu_he.branch   = pcr.branch
                                          and   insu_he.policy   = pcr.policy
                                          and   insu_he.certif   = pcr.certif
                                          and   insu_he.client   = pcr.client
                                          and   insu_he.effecdate <= pcr.p_effecdate
                                          and   (insu_he.nulldate is null or insu_he.nulldate > pcr.p_effecdate))
                                    else 0
                                    end didadecom,
                                    '' as vtxperindc,
                                    '' as tpgmybenef
                                    from (  select 
                                                t.*,
                                                case 
                                                when t.role = '8' then   case
                                                                        when exists (  select 1 from usinsug01.roles r2 
                                                                                    where r2.usercomp = 1
                                                                                    and   r2.company  = 1
                                                                                    and   r2.branch   = t.branch 
                                                                                    and   r2.policy   = t.policy
                                                                                    and   r2.certif   = t.certif                
                                                                                    and   r2.client   = t.client 
                                                                                    and   r2.role   = '2')  then 0
                                                                        else 1
                                                                        end
                                          else 1                  
                                                end flag  
                                                from ( select 
                                                            p.usercomp,
                                                            p.company,
                                                            p.certype,
                                                            p.politype,
                                                            p.effecdate p_effecdate,
                                                            cert.effecdate c_effecdate,
                                                      p.branch,
                                                      p.product,
                                                      psp.sub_product,
                                                            p.policy,
                                                            cert.certif, 
                                                            r.client, 
                                                            r.role,
                                                      r.effecdate as r_effecdate,
                                                      r.nulldate  as r_nulldate,
                                                      r.compdate
                                                      from usinsug01.policy p 
                                                      left join usinsug01.certificat cert on p.usercomp = cert.usercomp and p.company = cert.company and p.certype = cert.certype and p.branch  = cert.branch and p.policy  = cert.policy
                                                      join usinsug01.pol_subproduct psp   on  psp.usercomp = p.usercomp and psp.company  = p.company and psp.certype  = p.certype and psp.branch  = p.branch	and psp.product  = p.product and psp.policy   = p.policy	
                                                      join usbi01."IFRS170_T_RAMOS_POR_TIPO_RIESGO" rtr 
                                                on   rtr."BRANCHCOM" = p.branch 
                                                and  rtr."RISKTYPEN" = 1 
                                                and  rtr."SOURCESCHEMA" = 'usinsug01'
                                                join usinsug01.roles r
                                                      on  r.usercomp = p.usercomp 
                                                      and r.company  = p.company 
                                                      and r.certype  = p.certype 
                                                      and r.branch   = p.branch 
                                                      and r.policy   = p.policy 
                                                      and r.certif   = cert.certif  
                                                      and r.effecdate <= p.effecdate 
                                                      and (r.nulldate is null or r.nulldate > p.effecdate)
                                                      where r.role in ('2', '8')
                                                ) as t 
                                          ) pcr where flag = 1 and CAST(pcr.compdate AS DATE) between '{p_fecha_inicio}' and '{p_fecha_fin}'
                             ) AS PIG
                             '''
    
    l_df_abclrisp_insunix_lpg = glue_context.read.format('jdbc').options(**connection).option("dbtable", l_abclrisp_insunix_lpg).load()

    print("INSUNIX LPG")

    l_abclrisp_insunix_lpv = f'''
                             (
                              select
                              'd' inddetrec, 
                              'abclrisp' tablaifrs17, 
                              '' as pk,
                              '' as dtpreg,
                              '' as tiocproc,
                              cast(r.compdate as date) as tiocfrm,
                              '' as tiocto,
                              'piv' as kgiorigm,
                              p.branch ||'-'|| coalesce (p.product, 0) ||'-'|| p.policy ||'-'|| cert.certif as kabapol,
                              p.branch ||'-'|| coalesce (p.product, 0) ||'-'|| p.policy ||'-'|| cert.certif || '-' || coalesce((select evi.scod_vt from usinsug01.equi_vt_inx evi where evi.scod_inx = r.client), '0') as kabunris,
                              case p.politype when  '1'
                              then  coalesce ((  select coalesce(gc.covergen, '0')
                                    from usbi01.ifrs170_v_gen_life_cover_inxlpv gc 
                                    join usinsuv01.cover c  
                                    on  gc.usercomp = c.usercomp 
                                    and gc.company  = c.company 
                                    and gc.branch   = c.branch
                                    and gc.product  = p.product
                                    --and gc.sub_product = pc.sub_product
                                    and gc.currency = c.currency
                                    --and gc.modulec =  c.modulec
                                    and gc.cover   =  c.cover
                                    and gc.effecdate <= p.effecdate
                                    and (gc.nulldate is null or gc.nulldate > p.effecdate)		       		   
                                    where c.usercomp   = p.usercomp 
                                    and   c.company    = p.company 
                                    and   c.certype    = '2' 
                                    and   c.branch     = p.branch 
                                    and   c.policy     = p.policy
                                    and   c.certif     = cert.certif  
                                    and   c.effecdate <= p.effecdate
                                    and  (c.nulldate is null or c.nulldate > p.effecdate)
                                    and  c.cover = 1 limit 1), '0')                                         
                              else  (coalesce((  select coalesce(gc.covergen, '0')
                                          from usbi01.ifrs170_v_gen_life_cover_inxlpv gc 
                                          join usinsuv01.cover c  
                                          on  gc.usercomp = c.usercomp 
                                          and gc.company  = c.company  
                                          and gc.branch   = c.branch
                                          and gc.product  = p.product
                                          --and gc.sub_product = pc.sub_product
                                          and gc.currency = c.currency
                                          --and gc.modulec =  c.modulec
                                          and gc.cover   =  c.cover
                                          and gc.effecdate <= cert.effecdate
                                          and (gc.nulldate is null or gc.nulldate > cert.effecdate)		       		   
                                          where c.usercomp   = p.usercomp 
                                          and   c.company    = p.company 
                                          and   c.certype    = '2' 
                                          and   c.branch     = p.branch 
                                          and   c.policy     = p.policy
                                          and   c.certif     = cert.certif  
                                          and   c.effecdate <= cert.effecdate
                                          and  (c.nulldate is null or c.nulldate > cert.effecdate)
                                          and  c.cover = 1 limit 1), '0'))
                              end as kgctpcbt,
                              row_number () over ( partition  by p.branch, coalesce (p.product, 0), p.policy, cert.certif order by r.client) as dnpeseg, --pendiente
                              '' as kebentid_ps,
                              (select (current_date - cli.birthdat)/365 from usinsug01.client cli where cli.code = p.titularc) as didadeac,
                              '' as danoref, --en blanco
                              '' as kacempr,
                              case p.branch
                              when 23 
                              then (select coalesce (insu_he.anual_sal, 0) from usinsuv01.insured_he insu_he
                                    where insu_he.usercomp = p.usercomp
                                    and   insu_he.company  =  p.company
                                    and   insu_he.certype  =  p.certype
                                    and   insu_he.branch = p.branch
                                    and 	insu_he.policy = p.policy
                                    and   insu_he.certif = cert.certif
                                    and   insu_he.client = r.client
                                    and   insu_he.effecdate <= p.effecdate
                                    and   (insu_he.nulldate is null or insu_he.nulldate > p.effecdate))
                              else 0
                              end vmtsalar,
                              '' as kactpsal,
                              '' as tadmemp,
                              '' as tadmgrp,
                              '' as tsaidgrp,
                              'lpv' as dcompa,
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
                              coalesce(cast(cast(r.nulldate as date) as varchar), '') as ttermo,
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
                              then (select coalesce (insu_he.age_limit, 0) from usinsuv01.insured_he insu_he
                                    where insu_he.usercomp = p.usercomp
                                    and   insu_he.company  =  p.company
                                    and   insu_he.certype  =  p.certype
                                    and   insu_he.branch = p.branch
                                    and 	insu_he.policy = p.policy
                                    and   insu_he.certif = cert.certif
                                    and   insu_he.client = r.client
                                    and   insu_he.effecdate <= p.effecdate
                                    and   (insu_he.nulldate is null or insu_he.nulldate > p.effecdate))
                              else 0
                              end didadecom,
                              '' as vtxperindc,
                              '' as tpgmybenef
                              from usinsuv01.policy p 
                              left join usinsuv01.certificat cert 
                              on p.usercomp = cert.usercomp 
                              and p.company = cert.company 
                              and p.certype = cert.certype 
                              and p.branch  = cert.branch 
                              and p.policy  = cert.policy	
                              join usbi01."IFRS170_T_RAMOS_POR_TIPO_RIESGO" rtr 
                              on rtr."BRANCHCOM" = p.branch 
                              and  rtr."RISKTYPEN" = 1 
                              and rtr."SOURCESCHEMA" = 'usinsuv01'
                              join usinsuv01.roles r
                              on r.usercomp = p.usercomp 
                              and r.company  = p.company 
                              and r.certype  = p.certype
                              and r.branch   = p.branch 
                              and r.policy   = p.policy 
                              and r.certif   = cert.certif  
                              --and r.client   = pc.titularc
                              and r.effecdate <= p.effecdate 
                              and (r.nulldate is null or r.nulldate > p.effecdate)
                              and r.role in (2,8)
                              where p.certype = '2' 
                              and p.status_pol not in ('2','3')
                              and CAST(r.compdate AS DATE) between '{p_fecha_inicio}' and '{p_fecha_fin}'
                             ) AS TMP
                             '''
    
    l_df_abclrisp_insunix_lpv = glue_context.read.format('jdbc').options(**connection).option("dbtable", l_abclrisp_insunix_lpv).load()

    print("INSUNIX LPV")
    #----------------------------------------------------------------------------------------------------------------------------------#

    l_abclrisp_vtime_lpg = f'''
                              (
                                    SELECT
                                    'D' INDDETREC, 
                                    'ABCLRISP' TABLAIFRS17, 
                                    '' AS PK,
                                    '' AS DTPREG,
                                    '' AS TIOCPROC,
                                    CAST(R."DCOMPDATE" AS DATE) AS TIOCFRM,
                                    '' AS TIOCTO,
                                    'PVG' AS KGIORIGM,
                                    P."NBRANCH" ||'-'|| P."NPRODUCT" ||'-'|| P."NPOLICY" ||'-'|| CERT."NCERTIF" AS KABAPOL,
                                    P."NBRANCH" ||'-'|| P."NPRODUCT" ||'-'|| P."NPOLICY" ||'-'|| CERT."NCERTIF" || '-' || R."SCLIENT"  AS KABUNRIS,
                                    CASE P."SPOLITYPE"  WHEN '1' 
                                    THEN      COALESCE((SELECT COALESCE(GLC."NCOVERGEN", '0')
                                                      FROM USBI01."ifrs170_v_gen_life_cover_vtimelpg" GLC
                                                            JOIN USVTIMG01."COVER" C  
                                                            ON   GLC."NBRANCH"   = C."NBRANCH"
                                                            AND  GLC."NPRODUCT"  = P."NPRODUCT"
                                                            AND  GLC."NCURRENCY" = C."NCURRENCY"
                                                            AND  GLC."NMODULEC" =  C."NMODULEC"
                                                            AND  GLC."NCOVER"   =  C."NCOVER"
                                                            AND  GLC."DEFFECDATE" <= P."DSTARTDATE"
                                                            AND  (GLC."DNULLDATE" IS NULL OR GLC."DNULLDATE" > P."DSTARTDATE")		       		   
                                                            WHERE C."SCERTYPE"    = P."SCERTYPE" 
                                                            AND   C."NBRANCH"     = P."NBRANCH"
                                                            AND   C."NPRODUCT"    = P."NPRODUCT"
                                                            AND   C."NPOLICY"     = P."NPOLICY"
                                                            AND   C."NCERTIF"     = cert."NCERTIF"
                                                            AND   C."DEFFECDATE" <= P."DSTARTDATE"
                                                            AND  (C."DNULLDATE" IS NULL OR C."DNULLDATE" > P."DSTARTDATE")
                                                            AND  C."NCOVER" = 1), '0') 
                                    ELSE      COALESCE((SELECT COALESCE(GLC."NCOVERGEN", '0')
                                                      FROM USBI01."ifrs170_v_gen_life_cover_vtimelpg" GLC
                                                      JOIN USVTIMG01."COVER" C  
                                                      ON  GLC."NBRANCH"   = C."NBRANCH"
                                                      AND GLC."NPRODUCT"  = P."NPRODUCT"
                                                      AND GLC."NCURRENCY" = C."NCURRENCY"
                                                      AND GLC."NMODULEC" =  C."NMODULEC"
                                                      AND GLC."NCOVER"   =  C."NCOVER"
                                                      AND GLC."DEFFECDATE" <= cert."DSTARTDATE"
                                                      AND (GLC."DNULLDATE" IS NULL OR GLC."DNULLDATE" > cert."DSTARTDATE")		       		   
                                                      WHERE C."SCERTYPE"    = P."SCERTYPE" 
                                                      AND   C."NBRANCH"     = P."NBRANCH"
                                                      AND   C."NPRODUCT"    = P."NPRODUCT"
                                                      AND   C."NPOLICY"     = P."NPOLICY"
                                                      AND   C."NCERTIF"     = cert."NCERTIF"
                                                      AND   C."DEFFECDATE" <= cert."DSTARTDATE"
                                                      AND  (C."DNULLDATE" IS NULL OR C."DNULLDATE" > cert."DSTARTDATE")
                                                      AND  C."NCOVER" = 1), '0') END AS KGCTPCBT,
                                    ROW_NUMBER () OVER (PARTITION  BY P."NBRANCH", P."NPRODUCT", P."NPOLICY", cert."NCERTIF" ORDER BY R."SCLIENT") AS DNPESEG,
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
                                    COALESCE(CAST(CAST(R."DEFFECDATE" AS DATE) AS VARCHAR), '') AS TINICIO,
                                    COALESCE(CAST(CAST(R."DNULLDATE" AS DATE) AS VARCHAR), '') AS TTERMO,
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
                                    FROM USVTIMG01."POLICY" P 
                                    LEFT JOIN USVTIMG01."CERTIFICAT" CERT 
                                    ON  P."SCERTYPE" = CERT."SCERTYPE" 
                                    AND P."NBRANCH"  = CERT."NBRANCH"
                                    AND P."NPRODUCT" = CERT."NPRODUCT"
                                    AND P."NPOLICY"  = CERT."NPOLICY"
                                    JOIN USBI01."IFRS170_T_RAMOS_POR_TIPO_RIESGO" RTR 
                                    ON RTR."BRANCHCOM" = P."NBRANCH" 
                                    AND  RTR."RISKTYPEN" = 1 
                                    AND RTR."SOURCESCHEMA" = 'USVTIMG01'
                                    join USVTIMG01."ROLES" R
                                    ON  R."SCERTYPE"  = P."SCERTYPE"
                                    AND R."NBRANCH"   = P."NBRANCH" 
                                    AND R."NPRODUCT"  = P."NPRODUCT"
                                    AND R."NPOLICY"   = P."NPOLICY" 
                                    AND R."NCERTIF"   = CERT."NCERTIF"  
                                    AND R."DEFFECDATE" <= P."DSTARTDATE" 
                                    AND (R."DNULLDATE" IS NULL OR R."DNULLDATE" > P."DSTARTDATE")
                                    and R."NROLE" IN (2,8)
                                    WHERE P."SCERTYPE" = '2' 
                                    AND P."SSTATUS_POL" NOT IN ('2','3')
                                    and CAST(R."DCOMPDATE" AS DATE) between '{p_fecha_inicio}' and '{p_fecha_fin}'
                              ) AS TMP
                            '''
    
    l_df_abclrisp_vtime_lpg = glue_context.read.format('jdbc').options(**connection).option("dbtable", l_abclrisp_vtime_lpg).load()

    print("VTIME LPG")

    l_abclrisp_vtime_lpv = f'''
                           (
                                 SELECT
                                    'D' INDDETREC, 
                                    'ABCLRISP' TABLAIFRS17, 
                                    '' AS PK,
                                    '' AS DTPREG,
                                    '' AS TIOCPROC,
                                    cast(R."DCOMPDATE" as date) AS TIOCFRM,
                                    '' AS TIOCTO,
                                    'PVV' AS KGIORIGM,
                                    P."NBRANCH" ||'-'|| P."NPRODUCT" ||'-'|| P."NPOLICY" ||'-'|| cert."NCERTIF" AS KABAPOL,
                                    P."NBRANCH" ||'-'|| P."NPRODUCT" ||'-'|| P."NPOLICY" ||'-'|| cert."NCERTIF" || '-' || R."SCLIENT"  AS KABUNRIS,
                                    case P."SPOLITYPE"  when '1' 
                                    then
                                    ( SELECT COALESCE(cast(GC."NCOVERGEN" as varchar), '0')
                                                FROM USVTIMV01."LIFE_COVER" GC 
                                                JOIN USVTIMV01."COVER" C  
                                                ON  GC."NBRANCH"   = C."NBRANCH"
                                                AND GC."NPRODUCT"  = P."NPRODUCT"
                                                AND GC."NCURRENCY" = C."NCURRENCY"
                                                AND GC."NMODULEC" =  C."NMODULEC"
                                                AND GC."NCOVER"   =  C."NCOVER"
                                                AND GC."DEFFECDATE" <= P."DSTARTDATE"
                                                AND (GC."DNULLDATE" IS NULL OR GC."DNULLDATE" > P."DSTARTDATE")		       		   
                                                WHERE C."SCERTYPE"    = P."SCERTYPE" 
                                                AND   C."NBRANCH"     = P."NBRANCH"
                                                AND   C."NPRODUCT"    = P."NPRODUCT"
                                                AND   C."NPOLICY"     = P."NPOLICY"
                                                AND   C."NCERTIF"     = cert."NCERTIF"
                                                AND   C."DEFFECDATE" <= P."DSTARTDATE"
                                                AND  (C."DNULLDATE" IS NULL OR C."DNULLDATE" > P."DSTARTDATE")
                                                AND  C."NCOVER" = 1 LIMIT 1                                      
                                    ) else ( SELECT COALESCE(cast(GC."NCOVERGEN" as varchar), '0')
                                                FROM USVTIMV01."LIFE_COVER" GC 
                                                JOIN USVTIMV01."COVER" C  
                                                ON  GC."NBRANCH"   = C."NBRANCH"
                                                AND GC."NPRODUCT"  = P."NPRODUCT"
                                                AND GC."NCURRENCY" = C."NCURRENCY"
                                                AND GC."NMODULEC" =  C."NMODULEC"
                                                AND GC."NCOVER"   =  C."NCOVER"
                                                AND GC."DEFFECDATE" <= cert."DSTARTDATE"
                                                AND (GC."DNULLDATE" IS NULL OR GC."DNULLDATE" > cert."DSTARTDATE")		       		   
                                                WHERE C."SCERTYPE"    = P."SCERTYPE" 
                                                AND   C."NBRANCH"     = P."NBRANCH"
                                                AND   C."NPRODUCT"    = P."NPRODUCT"
                                                AND   C."NPOLICY"     = p."NPOLICY"
                                                AND   C."NCERTIF"     = cert."NCERTIF"
                                                AND   C."DEFFECDATE" <= cert."DSTARTDATE"
                                                AND  (C."DNULLDATE" IS NULL OR C."DNULLDATE" > cert."DSTARTDATE")
                                                AND  C."NCOVER" = 1 LIMIT 1                                      
                                    )
                                    END AS KGCTPCBT,
                                    ROW_NUMBER () OVER (PARTITION  BY P."NBRANCH", P."NPRODUCT", P."NPOLICY", cert."NCERTIF" ORDER BY R."SCLIENT") AS DNPESEG,
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
                                    join USVTIMV01."ROLES" R
                                    ON  R."SCERTYPE"  = P."SCERTYPE"
                                    AND R."NBRANCH"   = P."NBRANCH" 
                                    AND R."NPRODUCT"  = P."NPRODUCT"
                                    AND R."NPOLICY"   = P."NPOLICY" 
                                    AND R."NCERTIF"   = cert."NCERTIF"  
                                    AND R."DEFFECDATE" <= P."DSTARTDATE" 
                                    AND (R."DNULLDATE" IS NULL OR R."DNULLDATE" > P."DSTARTDATE")
                                    AND R."NROLE" IN (2,8)
                                    WHERE P."SCERTYPE" = '2' 
                                    AND P."SSTATUS_POL" NOT IN ('2','3')
                                    and CAST(r."DCOMPDATE" AS DATE) between '{p_fecha_inicio}' and '{p_fecha_fin}'
                           ) AS VTIME_LPV
                           '''
    
    l_df_abclrisp_vtime_lpv = glue_context.read.format('jdbc').options(**connection).option("dbtable", l_abclrisp_vtime_lpv).load()
    
    print("VTIME LPV")

    #----------------------------------------------------------------------------------------------------------------------------------#

    l_abclrisp_insis_lpv = f'''
                            (
                                  SELECT
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
                                    WHERE CAST(IO."REGISTRATION_DATE" AS DATE) BETWEEN '{p_fecha_inicio}' AND '{p_fecha_fin}'
                            )
                            '''
    
    l_df_abclrisp_insis_lpv = glue_context.read.format('jdbc').options(**connection).option("dbtable", l_abclrisp_insis_lpv).load()

    print("INSIS LPV")
    
    l_df_abclrisp = l_df_abclrisp_insunix_lpg.union(l_df_abclrisp_insunix_lpv).union(l_df_abclrisp_vtime_lpg).union(l_df_abclrisp_vtime_lpv).union(l_df_abclrisp_insis_lpv)

    l_df_abclrisp = l_df_abclrisp.withColumn("KGCTPCBT" , coalesce(col("KGCTPCBT"),lit("").cast(StringType()))).withColumn("DNPESEG", coalesce(col("DNPESEG"),lit("").cast(StringType()))).withColumn("DIDADEAC", coalesce(col("DIDADEAC"),lit("").cast(StringType()))).withColumn("VMTSALAR", format_number("VMTSALAR",2)).withColumn("KACTPPES", coalesce(col("KACTPPES"),lit("").cast(StringType()))).withColumn("TINICIO", coalesce(col("TINICIO"),lit("").cast(DateType()))).withColumn("TTERMO", coalesce(col("TTERMO"),lit("").cast(StringType())))

    return l_df_abclrisp