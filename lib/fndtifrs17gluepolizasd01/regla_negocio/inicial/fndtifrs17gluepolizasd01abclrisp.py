from pyspark.sql.types import StringType , DateType
from pyspark.sql.functions import col , coalesce , lit , format_number

def get_data(glue_context, connection, p_fecha_inicio, p_fecha_fin):

    l_fecha_carga_inicial = '2021-12-31'
    
    l_abclrisp_insunix_lpg = f'''
                             (      select
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
                                    from (      select 
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
                                    		       pc.usercomp,
                                    			 pc.company,
                                    			 pc.certype,
                                    			 pc.politype,
                                    			 pc.p_effecdate,
                                    			 pc.c_effecdate,
                                    		       pc.branch,
                                    		       pc.product,
                                    		       pc.sub_product,
                                    			 pc.policy,
                                    			 pc.certif, 
                                    			 r.client, 
                                    			 r.role,
                                    		       r.effecdate as r_effecdate,
                                    		       r.nulldate  as r_nulldate,
                                    		       r.compdate
                                    		       from usinsug01.roles r 
                                    		       join 
                                    		       (  ( select p.usercomp, p.company, p.certype, p.branch, p.product, psp.sub_product, p.policy, cert.certif, p.titularc, p.effecdate as p_effecdate ,p.politype , cert.effecdate as c_effecdate
                                    		        	from usinsug01.policy p 
                                    			      left join usinsug01.certificat cert on p.usercomp = cert.usercomp and p.company = cert.company and p.certype = cert.certype and p.branch  = cert.branch and p.policy  = cert.policy
                                    			      join usinsug01.pol_subproduct psp   on  psp.usercomp = p.usercomp and psp.company  = p.company and psp.certype  = p.certype and psp.branch  = p.branch	and psp.product  = p.product and psp.policy   = p.policy	
                                    			      join usbi01."IFRS170_T_RAMOS_POR_TIPO_RIESGO" rtr 
                                    		           	on   rtr."BRANCHCOM" = p.branch 
                                    		            and  rtr."RISKTYPEN" = 1 
                                    		            and  rtr."SOURCESCHEMA" = 'usinsug01'
                                    			      where p.certype = '2' 
                                    		            and p.status_pol not in ('2','3') 
                                    		            and  ((p.politype = '1' and p.expirdat >= '{l_fecha_carga_inicial}' and (p.nulldate is null or p.nulldate > '{l_fecha_carga_inicial}') ) -- individual
                                    		                  or 
                                    		                 (p.politype <> '1' and cert.expirdat >= '{l_fecha_carga_inicial}' and (cert.nulldate is null or cert.nulldate > '{l_fecha_carga_inicial}')))
                                    		            and p.effecdate between '{p_fecha_inicio}' and '{p_fecha_fin}')
                                    		            
                                    		           -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
                                    		         
                                    		            union  /*quitando union desde aqui*/
                                    	
                                    		            ( select p.usercomp, p.company, p.certype, p.branch, p.product, psp.sub_product, p.policy, cert.certif, p.titularc, p.effecdate as p_effecdate ,p.politype , cert.effecdate as c_effecdate
                                    		              from usinsug01.policy p 
                                    		              left join usinsug01.certificat cert on p.usercomp = cert.usercomp and p.company = cert.company and p.certype = cert.certype and p.branch  = cert.branch and p.policy  = cert.policy
                                    		              join usinsug01.pol_subproduct psp   on  psp.usercomp = p.usercomp and psp.company  = p.company and psp.certype  = p.certype and psp.branch   = p.branch		    and psp.product  = p.product and psp.policy   = p.policy	
                                    		              join  usbi01."IFRS170_T_RAMOS_POR_TIPO_RIESGO" rtr 
                                    		              on   rtr."BRANCHCOM" = p.branch 
                                    		              and  rtr."RISKTYPEN" = 1 
                                    		              and  rtr."SOURCESCHEMA" = 'usinsug01'
                                    		              where p.certype  = '2' 
                                    		              and   p.status_pol not in ('2', '3') 
                                    		              and   (((p.politype = '1' and  p.expirdat < '{l_fecha_carga_inicial}' or p.nulldate < '{l_fecha_carga_inicial}')
                                    		              and exists (select 1 from  usinsug01.claim cla  
                                    		                          join  usinsuv01.claim_his clh 
                                    		                          on clh.usercomp  = cla.usercomp 
                                    		                          and clh.company  = cla.company 
                                    		                          and clh.branch   = cla.branch 
                                    		                          and clh.claim    = cla.claim
                                    		                          where cla.branch = p.branch 
                                    		                          and cla.policy   = p.policy 
                                    		                          and trim(clh.oper_type) in (select cast(tcl.operation as varchar(2)) from usinsug01.tab_cl_ope tcl where  (tcl.reserve = 1 or tcl.ajustes = 1 or tcl.pay_amount = 1)) 
                                    		                          and clh.operdate >= '{l_fecha_carga_inicial}'
                                    		              and p.effecdate between '{p_fecha_inicio}' and '{p_fecha_fin}'))))
                                    		                            
                                    		                              
                                    		            -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
                                    		                
                                    		            union
                                    	
                                    		            ( select p.usercomp, p.company, p.certype, p.branch, p.product, psp.sub_product, p.policy, cert.certif, p.titularc, p.effecdate as p_effecdate ,p.politype , cert.effecdate as c_effecdate
                                    		              from usinsug01.policy p 
                                    		              left join usinsug01.certificat cert on p.usercomp = cert.usercomp and p.company = cert.company and p.certype = cert.certype and p.branch  = cert.branch and p.policy  = cert.policy
                                    		              join usinsug01.pol_subproduct psp   on  psp.usercomp = p.usercomp and psp.company  = p.company and psp.certype  = p.certype and psp.branch = p.branch and psp.product = p.product and psp.policy   = p.policy	
                                    		              join usbi01."IFRS170_T_RAMOS_POR_TIPO_RIESGO" rtr 
                                    		              on   rtr."BRANCHCOM" = p.branch 
                                    		              and  rtr."RISKTYPEN" = 1 
                                    		              and  rtr."SOURCESCHEMA" = 'usinsug01'
                                    		              where p.certype  = '2' 
                                    		              and p.status_pol not in ('2', '3')
                                    		              and (((p.politype <> '1' and cert.expirdat < '{l_fecha_carga_inicial}'  or  cert.nulldate < '{l_fecha_carga_inicial}') 
                                    		              and exists (select 1 from  usinsug01.claim cla    
                                    		                          join  usinsug01.claim_his clh  
                                    		                          on    cla.usercomp  = clh.usercomp 
                                    		                          and   cla.company   = clh.company 
                                    		                          and   cla.branch    = clh.branch  
                                    		                          and   clh.claim     = cla.claim
                                    		                          where cla.branch    = cert.branch
                                    		                          and   cla.policy    = cert.policy
                                    		                          and   cla.certif    = cert.certif
                                    		                          and   trim(clh.oper_type) in (select cast(tcl.operation as varchar(2)) from usinsug01.tab_cl_ope tcl  where (tcl.reserve = 1 or tcl.ajustes = 1 or tcl.pay_amount = 1)) 
                                    		                          and  clh.operdate >= '{l_fecha_carga_inicial}')))
                                    		              and p.effecdate between '{p_fecha_inicio}' and '{p_fecha_fin}') /*quitando union hasta aqui*/
                                    		) as pc	
                                    		on  r.usercomp = pc.usercomp 
                                    		and r.company  = pc.company 
                                    		and r.certype  = pc.certype 
                                    		and r.branch   = pc.branch 
                                    		and r.policy   = pc.policy 
                                    		and r.certif   = pc.certif  
                                    		and r.effecdate <= pc.p_effecdate 
                                    		and (r.nulldate is null or r.nulldate > pc.p_effecdate)
                                    		where r.role in ('2', '8')) as t ) pcr where flag = 1) as tmp
                                                '''
    
    l_df_abclrisp_insunix_lpg = glue_context.read.format('jdbc').options(**connection).option("dbtable", l_abclrisp_insunix_lpg).load()

    print("INSUNIX LPG")

    l_abclrisp_insunix_lpv = f'''
                             ( select
                              'd' inddetrec, 
                              'abclrisp' tablaifrs17, 
                              '' as pk,
                              '' as dtpreg,
                              '' as tiocproc,
                              cast(r.compdate as date) as tiocfrm,
                              '' as tiocto,
                              'piv' as kgiorigm,
                              pc.branch ||'-'|| coalesce (pc.product, 0) ||'-'|| pc.policy ||'-'|| pc.certif as kabapol,
                              pc.branch ||'-'|| coalesce (pc.product, 0) ||'-'|| pc.policy ||'-'|| pc.certif || '-' || coalesce((select evi.scod_vt from usinsug01.equi_vt_inx evi where evi.scod_inx = r.client), '0') as kabunris,
                              case pc.politype when  '1'
                              then  coalesce ((  select coalesce(gc.covergen, '0')
	                                           from /*usbi01.ifrs170_v_gen_life_cover_inxlpv gc*/
	                                           (   select gco.usercomp,
	                                               gco.company,
	                                               gco.branch,
	                                               gco.product,
	                                               gco.currency,
	                                               gco.modulec,
	                                               gco.cover,
	                                               gco.effecdate,
	                                               gco.nulldate,
	                                               (coalesce(gco.covergen, 0) || '-' || coalesce(gco.currency, 0) || '-g' ) as covergen
	                                               from usinsug01.gen_cover gco
	                                               where  (( select distinct pro.brancht
	                                   	                     from usinsuv01.product pro
	                                                         where pro.usercomp = gco.usercomp 
	                                                         and pro.company    = gco.company 
	                                                         and pro.branch     = gco.branch 
	                                                         and pro.product    = gco.product and pro.nulldate is null)) not in ('1', '5')
	                                               union
	                                               select gco.usercomp,
	                                               gco.company,
	                                               gco.branch,
	                                               gco.product,
	                                               gco.currency,
	                                               0 as modulec,
	                                               gco.cover,
	                                               gco.effecdate,
	                                               gco.nulldate,
	                                               (coalesce(gco.covergen, 0) || '-' || coalesce(gco.currency, 0) || '-l') as covergen
	                                               from usinsug01.life_cover gco
	                                               where ((( select distinct pro.brancht
			                                             from usinsuv01.product pro
			                                             where pro.usercomp = gco.usercomp 
			                                             and pro.company    = gco.company 
			                                             and pro.branch     = gco.branch 
			                                             and pro.product    = gco.product 
			                                             and pro.nulldate is null))::text) in ('1', '5')) gc 
		                                      join usinsuv01.cover c  
		                                      on  gc.usercomp = c.usercomp 
		                                      and gc.company  = c.company 
		                                      and gc.branch   = c.branch
		                                      and gc.product  = pc.product
		                                      --and gc.sub_product = pc.sub_product
		                                      and gc.currency = c.currency
		                                      --and gc.modulec =  c.modulec
		                                      and gc.cover   =  c.cover
		                                      and gc.effecdate <= pc.effecdate
		                                      and (gc.nulldate is null or gc.nulldate > pc.effecdate)		       		   
		                                      where c.usercomp   = pc.usercomp 
		                                      and   c.company    = pc.company 
		                                      and   c.certype    = '2' 
		                                      and   c.branch     = pc.branch 
		                                      and   c.policy     = pc.policy
		                                      and   c.certif     = pc.certif  
		                                      and   c.effecdate <= pc.effecdate
		                                      and  (c.nulldate is null or c.nulldate > pc.effecdate)
		                                      and  c.cover = 1 limit 1), '0')                                         
                              else  (coalesce((  select coalesce(gc.covergen, '0')
                                                 from /*usbi01.ifrs170_v_gen_life_cover_inxlpv*/
                                                 ( select gco.usercomp,
                                                   gco.company,
                                                   gco.branch,
                                                   gco.product,
                                                   gco.currency,
                                                   gco.modulec,
                                                   gco.cover,
                                                   gco.effecdate,
                                                   gco.nulldate,
                                                   ((gco.covergen || '-'::text) || gco.currency) || '-g'::text as covergen
                                                   from usinsug01.gen_cover gco
                                                   where ((( select distinct pro.brancht
                                                             from usinsuv01.product pro
                                                             where pro.usercomp = gco.usercomp and pro.company = gco.company and pro.branch = gco.branch and pro.product = gco.product and pro.nulldate is null))::text) <> all (array['1'::character varying, '5'::character varying]::text[])
                                                   union
                                                   select gco.usercomp,
                                                   gco.company,
                                                   gco.branch,
                                                   gco.product,
                                                   gco.currency,
                                                   0 as modulec,
                                                   gco.cover,
                                                   gco.effecdate,
                                                   gco.nulldate,
                                                   ((gco.covergen || '-'::text) || gco.currency) || '-l'::text as covergen
                                                   from usinsug01.life_cover gco
                                                   where ((( select distinct pro.brancht
                                                             from usinsuv01.product pro
                                                             where pro.usercomp = gco.usercomp and pro.company = gco.company and pro.branch = gco.branch and pro.product = gco.product and pro.nulldate is null))::text) = any (array['1'::character varying, '5'::character varying]::text[])) gc 
                                                   join usinsuv01.cover c  
                                                   on  gc.usercomp = c.usercomp 
                                                   and gc.company  = c.company 
                                                   and gc.branch   = c.branch
                                                   and gc.product  = pc.product
                                                   --and gc.sub_product = pc.sub_product
                                                   and gc.currency = c.currency
                                                   --and gc.modulec =  c.modulec
                                                   and gc.cover   =  c.cover
                                                   and gc.effecdate <= pc.effecdate_cert
                                                   and (gc.nulldate is null or gc.nulldate > pc.effecdate_cert)		       		   
                                                   where c.usercomp   = pc.usercomp 
                                                   and   c.company    = pc.company 
                                                   and   c.certype    = '2' 
                                                   and   c.branch     = pc.branch 
                                                   and   c.policy     = pc.policy
                                                   and   c.certif     = pc.certif  
                                                   and   c.effecdate <= pc.effecdate_cert
                                                   and  (c.nulldate is null or c.nulldate > pc.effecdate_cert)
                                                   and  c.cover = 1 limit 1), '0'))
                              end as kgctpcbt,
                              row_number () over ( partition  by pc.branch, coalesce (pc.product, 0), pc.policy, pc.certif order by r.client) as dnpeseg, --pendiente
                              '' as kebentid_ps,
                              (select (current_date - cli.birthdat)/365 from usinsug01.client cli where cli.code = pc.titularc) as didadeac,
                              '' as danoref, --en blanco
                              '' as kacempr,
                              case pc.branch
                              when 23 
                              then (select coalesce (insu_he.anual_sal, 0) from usinsuv01.insured_he insu_he
                                    where insu_he.usercomp = pc.usercomp
                                    and   insu_he.company  =  pc.company
                                    and   insu_he.certype  =  pc.certype
                                    and   insu_he.branch = pc.branch
                                    and 	insu_he.policy = pc.policy
                                    and   insu_he.certif = pc.certif
                                    and   insu_he.client = r.client
                                    and   insu_he.effecdate <= pc.effecdate
                                    and   (insu_he.nulldate is null or insu_he.nulldate > pc.effecdate))
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
                              case pc.branch
                              when 23 
                              then (select coalesce (insu_he.age_limit, 0) from usinsuv01.insured_he insu_he
                                    where insu_he.usercomp = pc.usercomp
                                    and   insu_he.company  =  pc.company
                                    and   insu_he.certype  =  pc.certype
                                    and   insu_he.branch = pc.branch
                                    and 	insu_he.policy = pc.policy
                                    and   insu_he.certif = pc.certif
                                    and   insu_he.client = r.client
                                    and   insu_he.effecdate <= pc.effecdate
                                    and   (insu_he.nulldate is null or insu_he.nulldate > pc.effecdate))
                              else 0
                              end didadecom,
                              '' as vtxperindc,
                              '' as tpgmybenef
                              from usinsuv01.roles r
                              join 
                              (   
                                  ( select p.usercomp, p.company, p.certype, p.branch, p.product, p.policy, cert.certif, p.titularc, p.effecdate , p.politype, cert.effecdate as effecdate_cert
                                    from usinsuv01.policy p 
                                    left join usinsuv01.certificat cert 
                                    on p.usercomp = cert.usercomp 
                                    and p.company = cert.company 
                                    and p.certype = cert.certype 
                                    and p.branch  = cert.branch 
                                    and p.policy  = cert.policy	
                                    join /*usbi01."ifrs170_t_ramos_por_tipo_riesgo"*/
                                    ( select unnest(array['usinsuv01','usinsuv01','usinsuv01','usinsuv01','usinsuv01','usinsuv01','usinsuv01','usinsuv01','usinsuv01','usinsuv01','usinsuv01','usinsuv01','usinsuv01','usinsuv01', 'usinsuv01','usinsuv01','usinsuv01','usinsuv01','usinsuv01','usinsuv01','usinsuv01','usinsuv01','usinsuv01','usinsuv01']) as "sourceschema",  
                                             unnest(array[5,21,22,23,24,25,27,31,32,33,34,35,36,37,40,41,42,59,68,71,75,77,91,99]) as "branchcom",
                                             unnest(array[1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1]) as "risktypen") rtr on rtr."branchcom" = p.branch and  rtr."risktypen" = 1 and rtr."sourceschema" = 'usinsuv01'
                                      where p.certype = '2' 
                                      and p.status_pol not in ('2','3') 
                                      and ( (p.politype = '1' -- individual 
                                      and p.expirdat >= '{l_fecha_carga_inicial}' 
                                      and (p.nulldate is null or p.nulldate > '{l_fecha_carga_inicial}') )
                                            or 
                                          (p.politype <> '1' -- colectivas 
                                          and cert.expirdat >= '{l_fecha_carga_inicial}' 
                                          and (cert.nulldate is null or cert.nulldate > '{l_fecha_carga_inicial}'))))
                                                         
                                  union /*se quito el union desde aqui */
                                                        
                                  (  select p.usercomp, p.company, p.certype, p.branch, p.product, p.policy, cert.certif, p.titularc, p.effecdate ,p.politype , cert.effecdate as effecdate_cert
                                       from usinsuv01.policy p 
                                       left join usinsuv01.certificat cert on p.usercomp = cert.usercomp and p.company = cert.company and p.certype = cert.certype and p.branch  = cert.branch and p.policy  = cert.policy                             	           
                                       join usbi01."IFRS170_T_RAMOS_POR_TIPO_RIESGO"* rtr on rtr."BRANCHCOM" = p.branch and  rtr."RISKTYPEN" = 1 and rtr."SOURCESCHEMA" = 'usinsuv01'
                                       where p.certype  = '2' 
                                       and p.status_pol not in ('2', '3') 
                                       and (((p.politype = '1' and  p.expirdat < '{l_fecha_carga_inicial}' or p.nulldate < '{l_fecha_carga_inicial}')
                                       and exists (select 1 from usinsuv01.claim cla    
                                       join  usinsuv01.claim_his clh 
                                       on clh.usercomp = cla.usercomp 
                                       and clh.company = cla.company 
                                       and clh.branch = cla.branch 
                                       and clh.claim = cla.claim
                                       where cla.branch = p.branch 
                                       and cla.policy = p.policy 
                                       and trim(clh.oper_type) in (select cast(tcl.operation as varchar(2))
              	                     	                         from usinsug01.tab_cl_ope tcl
              	                     	                         where (tcl.reserve = 1 or tcl.ajustes = 1 or tcl.pay_amount = 1)) 
                                       and  clh.operdate >= '{l_fecha_carga_inicial}'
                                       and p.effecdate between '{p_fecha_inicio}' and '{p_fecha_fin}'))))

                                  union

                                  (  select p.usercomp, p.company, p.certype, p.branch, p.product, p.policy, cert.certif, p.titularc, p.effecdate ,p.politype , cert.effecdate as effecdate_cert
                                       from usinsuv01.policy p 
                                       left join usinsuv01.certificat cert on p.usercomp = cert.usercomp and p.company = cert.company and p.certype = cert.certype and p.branch  = cert.branch and p.policy  = cert.policy                             	              
                                       join usbi01."IFRS170_T_RAMOS_POR_TIPO_RIESGO" rtr 
                                       on   rtr."BRANCHCOM"     = p.branch 
                                       and  rtr."RISKTYPEN"     = 1 
                                       and  rtr."SOURCESCHEMA"  = 'usinsuv01'
                                       where p.certype  = '2' 
                                       and p.status_pol not in ('2', '3')
                                       and (((p.politype <> '1' and cert.expirdat < '{l_fecha_carga_inicial}'  or  cert.nulldate < '{l_fecha_carga_inicial}') 
                                       and exists (select 1 from  usinsuv01.claim cla    
                                       join  usinsuv01.claim_his clh  
                                       on cla.usercomp = clh.usercomp 
                                       and cla.company = clh.company 
                                       and cla.branch = clh.branch  
                                       and clh.claim = cla.claim
                                       where cla.branch   = cert.branch
                                       and   cla.policy   = cert.policy
                                       and   cla.certif   = cert.certif
                                       and   trim(clh.oper_type) in (select cast(tcl.operation as varchar(2)) 
                                                                     from  usinsug01.tab_cl_ope tcl 
                                                                     where (tcl.reserve = 1 or tcl.ajustes = 1 or tcl.pay_amount = 1)) 
                                      and  clh.operdate >= '{l_fecha_carga_inicial}')))
                                      and p.effecdate between '{p_fecha_inicio}' and '{p_fecha_fin}') /*se quito el union hasta aqui */) as pc	
                                      on  r.usercomp = pc.usercomp 
                                      and r.company  = pc.company 
                                      and r.certype  = pc.certype
                                      and r.branch   = pc.branch 
                                      and r.policy   = pc.policy 
                                      and r.certif   = pc.certif  
                                      --and r.client   = pc.titularc
                                      and r.effecdate <= pc.effecdate 
                                      and (r.nulldate is null or r.nulldate > pc.effecdate)
                                      where r.role in (2,8)
                                      and pc.effecdate between '{p_fecha_inicio}' and '{p_fecha_fin}'
                             ) as tmp
                             '''
    
    l_df_abclrisp_insunix_lpv = glue_context.read.format('jdbc').options(**connection).option("dbtable", l_abclrisp_insunix_lpv).load()

    print("INSUNIX LPV")
    #----------------------------------------------------------------------------------------------------------------------------------#

    l_abclrisp_vtime_lpg = f'''
                           (   SELECT
                               'D' INDDETREC, 
                               'ABCLRISP' TABLAIFRS17, 
                               '' AS PK,
                               '' AS DTPREG,
                               '' AS TIOCPROC,
                               CAST(R."DCOMPDATE" AS DATE) AS TIOCFRM,
                               '' AS TIOCTO,
                               'PVG' AS KGIORIGM,
                               PC."NBRANCH" ||'-'|| PC."NPRODUCT" ||'-'|| PC."NPOLICY" ||'-'|| PC."NCERTIF" AS KABAPOL,
                               PC."NBRANCH" ||'-'|| PC."NPRODUCT" ||'-'|| PC."NPOLICY" ||'-'|| PC."NCERTIF" || '-' || R."SCLIENT"  AS KABUNRIS,
                               CASE PC."SPOLITYPE"  WHEN '1' 
                               THEN      COALESCE((SELECT COALESCE(GLC."NCOVERGEN", '0')
                                                  FROM /*USBI01.IFRS170_V_GEN_LIFE_COVER GLC*/
                                                  (SELECT 
                                                 GC."NBRANCH",
                                                 GC."NPRODUCT",
                                                 GC."NMODULEC",
                                                 GC."NCOVER",
                                                 GC."DEFFECDATE",
                                                 GC."DNULLDATE",
                                                 GC."NCURRENCY",
                                                 CAST(GC."NCOVERGEN" AS VARCHAR) || '-G' "NCOVERGEN"
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
								 			AND    LCO."NBRANCH" = 21))) GLC
		                                     JOIN USVTIMG01."COVER" C  
		                                     ON   GLC."NBRANCH"   = C."NBRANCH"
		                                     AND  GLC."NPRODUCT"  = PC."NPRODUCT"
		                                     AND  GLC."NCURRENCY" = C."NCURRENCY"
		                                     AND  GLC."NMODULEC" =  C."NMODULEC"
		                                     AND  GLC."NCOVER"   =  C."NCOVER"
		                                     AND  GLC."DEFFECDATE" <= PC."DSTARTDATE"
		                                     AND  (GLC."DNULLDATE" IS NULL OR GLC."DNULLDATE" > PC."DSTARTDATE")		       		   
		                                     WHERE C."SCERTYPE"    = PC."SCERTYPE" 
		                                     AND   C."NBRANCH"     = PC."NBRANCH"
		                                     AND   C."NPRODUCT"    = PC."NPRODUCT"
		                                     AND   C."NPOLICY"     = PC."NPOLICY"
		                                     AND   C."NCERTIF"     = PC."NCERTIF"
		                                     AND   C."DEFFECDATE" <= PC."DSTARTDATE"
		                                     AND  (C."DNULLDATE" IS NULL OR C."DNULLDATE" > PC."DSTARTDATE")
		                                     AND  C."NCOVER" = 1), '0') 
                               ELSE      COALESCE((SELECT COALESCE(GLC."NCOVERGEN", '0')
                                                   FROM /*USBI01.IFRS170_V_GEN_LIFE_COVER GLC*/
                                                   ( SELECT 
                                                      GC."NBRANCH",
                                                      GC."NPRODUCT",
                                                      GC."NMODULEC",
                                                      GC."NCOVER",
                                                      GC."DEFFECDATE",
                                                      GC."DNULLDATE",
                                                      GC."NCURRENCY",
                                                      CAST(GC."NCOVERGEN" AS VARCHAR) || '-G' "NCOVERGEN"
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
							          		        WHERE 	LCO."NCOVERGEN" = LCO."NCOVERGEN"
							          		        AND    LCO."NBRANCH" = 21))) GLC
                                                     JOIN USVTIMG01."COVER" C  
                                                     ON  GLC."NBRANCH"   = C."NBRANCH"
                                                     AND GLC."NPRODUCT"  = PC."NPRODUCT"
                                                     AND GLC."NCURRENCY" = C."NCURRENCY"
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
                                                     AND  C."NCOVER" = 1), '0') END AS KGCTPCBT,
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
                               FROM USVTIMG01."ROLES" R
                               JOIN 
                               (     (SELECT P."SCERTYPE", P."NBRANCH", P."NPRODUCT", P."NPOLICY", CERT."NCERTIF", P."SCLIENT", P."DSTARTDATE" ,P."SPOLITYPE" ,CERT."DSTARTDATE" AS "DSTARTDATE_CERT"
                                     FROM USVTIMG01."POLICY" P 
                                     LEFT JOIN USVTIMG01."CERTIFICAT" CERT 
                                     ON  P."SCERTYPE" = CERT."SCERTYPE" 
                                     AND P."NBRANCH"  = CERT."NBRANCH"
                                     AND P."NPRODUCT" = CERT."NPRODUCT"
                                     AND P."NPOLICY"  = CERT."NPOLICY"
                                     JOIN USBI01."IFRS170_T_RAMOS_POR_TIPO_RIESGO" RTR ON RTR."BRANCHCOM" = P."NBRANCH" AND  RTR."RISKTYPEN" = 1 AND RTR."SOURCESCHEMA" = 'USVTIMG01'
                                     WHERE P."SCERTYPE" = '2' 
                                     AND P."SSTATUS_POL" NOT IN ('2','3') 
                                     AND ( (P."SPOLITYPE" = '1' -- INDIVIDUAL 
                                           AND P."DEXPIRDAT" >= '2018-12-31'
                                           AND (P."DNULLDATE" IS NULL OR P."DNULLDATE" > '2018-12-31') )
                                           OR 
                                           (P."SPOLITYPE" <> '1' -- COLECTIVAS 
                                           AND CERT."DEXPIRDAT" >= '2018-12-31' 
                                           AND (CERT."DNULLDATE" IS NULL OR CERT."DNULLDATE" > '2018-12-31'))))
                                     
                                     UNION /*SE QUITO EL UNION DESDE AQUI */

                                     (SELECT P."SCERTYPE", P."NBRANCH", P."NPRODUCT", P."NPOLICY", CERT."NCERTIF", P."SCLIENT", P."DSTARTDATE" ,P."SPOLITYPE" ,CERT."DSTARTDATE" AS "DSTARTDATE_CERT"
                                      FROM USVTIMG01."POLICY" P 
                                      JOIN USVTIMG01."CERTIFICAT" CERT 
                                      ON  P."SCERTYPE" = CERT."SCERTYPE" 
                                      AND P."NBRANCH"  = CERT."NBRANCH"
                                      AND P."NPRODUCT" = CERT."NPRODUCT"
                                      AND P."NPOLICY"  = CERT."NPOLICY"
                                      JOIN USBI01."IFRS170_T_RAMOS_POR_TIPO_RIESGO" RTR ON RTR."BRANCHCOM" = P."NBRANCH" AND  RTR."RISKTYPEN" = 1 AND RTR."SOURCESCHEMA" = 'USVTIMG01'
                                      JOIN USVTIMG01."CLAIM" CLA ON CLA."SCERTYPE" = P."SCERTYPE" AND CLA."NPOLICY" = P."NPOLICY" AND CLA."NBRANCH" = P."NBRANCH"
                                      JOIN (
                                             SELECT DISTINCT CLH."NCLAIM" 
                                             FROM (
                                                    SELECT CAST("SVALUE" AS INT4) "SVALUE" 
                                                    FROM USVTIMG01."CONDITION_SERV" CS WHERE "NCONDITION" IN (71, 72, 73)
                                                  ) CSV 
                                             JOIN USVTIMG01."CLAIM_HIS" CLH 
                                             ON COALESCE(CLH."NCLAIM", 0) > 0 
                                             AND CLH."NOPER_TYPE" = CSV."SVALUE" 
                                             AND CLH."DOPERDATE" >= '2018-12-31'
                                           ) CLH ON CLH."NCLAIM" = CLA."NCLAIM"
                                      WHERE P."SCERTYPE" = '2' 
                                      AND P."SSTATUS_POL" NOT IN ('2','3') 
                                      AND P."SPOLITYPE" = '1' 
                                      AND (P."DEXPIRDAT" < '2018-12-31' OR P."DNULLDATE" < '2018-12-31'))


                                     UNION

                                     (SELECT P."SCERTYPE", P."NBRANCH", P."NPRODUCT", P."NPOLICY", CERT."NCERTIF", P."SCLIENT", P."DSTARTDATE" ,P."SPOLITYPE" ,CERT."DSTARTDATE" AS "DSTARTDATE_CERT"
                                      FROM USVTIMG01."POLICY" P 
                                      JOIN USVTIMG01."CERTIFICAT" CERT 
                                      ON  P."SCERTYPE" = CERT."SCERTYPE" 
                                      AND P."NBRANCH"  = CERT."NBRANCH"
                                      AND P."NPRODUCT" = CERT."NPRODUCT"
                                      AND P."NPOLICY"  = CERT."NPOLICY"
                                      JOIN USBI01."IFRS170_T_RAMOS_POR_TIPO_RIESGO" RTR ON RTR."BRANCHCOM" = P."NBRANCH" AND  RTR."RISKTYPEN" = 1 AND RTR."SOURCESCHEMA" = 'USVTIMG01'
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
                                            AND CLH."DOPERDATE" >= '2018-12-31'
                                           ) CLH ON CLH."NCLAIM" = CLA."NCLAIM"
                                      WHERE P."SCERTYPE" = '2' 
                                      AND P."SSTATUS_POL" NOT IN ('2','3') 
                                      AND P."SPOLITYPE" <> '1' 
                                      AND (CERT."DEXPIRDAT" < '2018-12-31' OR CERT."DNULLDATE" < '2018-12-31')) /*SE QUITO EL UNION HASTA AQUI */
                               ) AS PC	
                               ON  R."SCERTYPE"  = PC."SCERTYPE"
                               AND R."NBRANCH"   = PC."NBRANCH" 
                               AND R."NPRODUCT"  = PC."NPRODUCT"
                               AND R."NPOLICY"   = PC."NPOLICY" 
                               AND R."NCERTIF"   = PC."NCERTIF"  
                               AND R."DEFFECDATE" <= PC."DSTARTDATE" 
                               AND (R."DNULLDATE" IS NULL OR R."DNULLDATE" > PC."DSTARTDATE")
                               WHERE R."NROLE" IN (2,8)
                               AND PC."DSTARTDATE" BETWEEN '{p_fecha_inicio}' AND '{p_fecha_fin}') AS TMP
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
                           	      JOIN USBI01."IFRS170_T_RAMOS_POR_TIPO_RIESGO" RTR ON RTR."BRANCHCOM" = P."NBRANCH" AND  RTR."RISKTYPEN" = 1 AND RTR."SOURCESCHEMA" = 'usvtimv01'
                           	      WHERE P."SCERTYPE" = '2' 
                                    AND P."SSTATUS_POL" NOT IN ('2','3') 
                                    AND ((P."SPOLITYPE" = '1' -- INDIVIDUAL 
                                    AND P."DEXPIRDAT" >= '{l_fecha_carga_inicial}' 
                                    AND (P."DNULLDATE" IS NULL OR P."DNULLDATE" > '{l_fecha_carga_inicial}') )
                                    OR 
                                    (P."SPOLITYPE" <> '1' -- COLECTIVAS 
                                    AND CERT."DEXPIRDAT" >= '{l_fecha_carga_inicial}' 
                                    AND (CERT."DNULLDATE" IS NULL OR CERT."DNULLDATE" > '{l_fecha_carga_inicial}'))
                                    AND p."DSTARTDATE" between '{p_fecha_inicio}' and '{p_fecha_fin}')

                                    UNION /*SE QUITO EL UNION DESDE AQUI */
                              
                                    (SELECT P."SCERTYPE", P."NBRANCH", P."NPRODUCT", P."NPOLICY", CERT."NCERTIF", P."SCLIENT", P."DSTARTDATE" ,P."SPOLITYPE" ,CERT."DSTARTDATE" AS "DSTARTDATE_CERT"
                                     FROM USVTIMV01."POLICY" P 
                                     JOIN USVTIMV01."CERTIFICAT" CERT 
                                     ON  P."SCERTYPE" = CERT."SCERTYPE" 
                                     AND P."NBRANCH"  = CERT."NBRANCH"
                                     AND P."NPRODUCT" = CERT."NPRODUCT"
                                     AND P."NPOLICY"  = CERT."NPOLICY"
                                     JOIN USBI01."IFRS170_T_RAMOS_POR_TIPO_RIESGO" RTR ON RTR."BRANCHCOM" = P."NBRANCH" AND  RTR."RISKTYPEN" = 1 AND RTR."SOURCESCHEMA" = 'usvtimv01'
                                     JOIN USVTIMV01."CLAIM" CLA ON CLA."SCERTYPE" = P."SCERTYPE" AND CLA."NPOLICY" = P."NPOLICY" AND CLA."NBRANCH" = P."NBRANCH"
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
                                    JOIN USBI01."IFRS170_T_RAMOS_POR_TIPO_RIESGO" RTR ON RTR."BRANCHCOM" = P."NBRANCH" AND  RTR."RISKTYPEN" = 1 AND RTR."SOURCESCHEMA" = 'usvtimv01'
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
                                    AND (CERT."DEXPIRDAT" < '{l_fecha_carga_inicial}' OR CERT."DNULLDATE" < '{l_fecha_carga_inicial}'))/*SE QUITO EL UNION HASTA AQUI */
                              ) AS PC	
                           ON  R."SCERTYPE"  = PC."SCERTYPE"
                           AND R."NBRANCH"   = PC."NBRANCH" 
                           AND R."NPRODUCT"  = PC."NPRODUCT"
                           AND R."NPOLICY"   = PC."NPOLICY" 
                           AND R."NCERTIF"   = PC."NCERTIF"  
                           AND R."DEFFECDATE" <= PC."DSTARTDATE" 
                           AND (R."DNULLDATE" IS NULL OR R."DNULLDATE" > PC."DSTARTDATE")
                           WHERE R."NROLE" IN (2,8)) AS VTIME_LPV
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
                            --AND P."REGISTRATION_DATE" BETWEEN '{p_fecha_inicio}' AND '{p_fecha_fin}') AS PNV'''
    
    l_df_abclrisp_insis_lpv = glue_context.read.format('jdbc').options(**connection).option("dbtable", l_abclrisp_insis_lpv).load()

    print("INSIS LPV")
    
    l_df_abclrisp = l_df_abclrisp_insunix_lpg.union(l_df_abclrisp_insunix_lpv).union(l_df_abclrisp_vtime_lpg).union(l_df_abclrisp_vtime_lpv).union(l_df_abclrisp_insis_lpv)

    l_df_abclrisp = l_df_abclrisp.withColumn("KGCTPCBT" , coalesce(col("KGCTPCBT"),lit("").cast(StringType()))).withColumn("DNPESEG", coalesce(col("DNPESEG"),lit("").cast(StringType()))).withColumn("DIDADEAC", coalesce(col("DIDADEAC"),lit("").cast(StringType()))).withColumn("VMTSALAR", format_number("VMTSALAR",2)).withColumn("KACTPPES", coalesce(col("KACTPPES"),lit("").cast(StringType()))).withColumn("TINICIO", coalesce(col("TINICIO"),lit("").cast(DateType()))).withColumn("TTERMO", coalesce(col("TTERMO"),lit("").cast(StringType())))

    return l_df_abclrisp