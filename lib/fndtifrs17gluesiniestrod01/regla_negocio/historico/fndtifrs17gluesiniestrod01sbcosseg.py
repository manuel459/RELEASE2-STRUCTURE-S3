
def get_data(glue_context, connection, p_fecha_inicio, p_fecha_fin):

    INSUNIX_LPG = f'''
                      (select
                       'D' INDDETREC,
                       'SBCOSSEG' TABLAIFRS17,
                       '' PK,
	               '' DTPREG, --excluido
	               '' TIOCPROC, --excluido
	               cast(cl0.xxxxdate as varchar) TIOCFRM,
	               '' TIOCTO, --excluido
	               'PIG' KGIORIGM,
	               'LPG' DCOMPA,
	               '' DMARCA, --excluido
	               cast(cl0.claim as varchar) KSBSIN,
	               cast(case when cl0.bussityp = '2' then 1 else coalesce(coi.companyc,0) end as varchar) DCODCSG,
	               '' DNUMSEQ, --excluido
	               '' TDPLANO, --excluido
	               cast(cl0.capital as numeric(14,2)) VMTCAPIT,
	               cast(case when cl0.bussityp = '2' then 100 else coalesce(coi.share,0) end as numeric (9,6)) VTXQUOTA,
	               '' VMTINDEM, --NOAPP
	               case when cl0.bussityp = '2' then cl0.leadpoli else cast(cl0.policy as varchar) end DNUMAPO_CSG,
	               cast(cl0.moneda_cod as varchar) KSCMOEDA
                       from	(	select	cla.usercomp,
                                                               cla.company,
                                                               cla.branch,
                                                               cla.policy,
                                                               cla.certif,
                                                               cla.occurdat,
                                                               cla.claim,
                                                               cl0.pol_certype certype, --se renombr� de vuelta solo para el �ndice en el siguiente nivel
                                                               cl0.leadpoli,
                                                               cl0.bussityp,
                                                               cl0.xxxxdate,
                                                               coalesce(
                                                                       (	select	max(cpl.currency)
                                                                               from	usinsug01.curren_pol cpl
                                                                               where 	cpl.usercomp = cla.usercomp and cpl.company = cla.company and cpl.certype = cl0.pol_certype
                                                                               and		cpl.branch = cla.branch and cpl.policy = cla.policy and	cpl.certif = cla.certif),
                                                                       (	select	max(cpl.currency)
                                                                               from	usinsug01.curren_pol cpl
                                                                               where 	cpl.usercomp = cla.usercomp and cpl.company = cla.company and cpl.certype = cl0.pol_certype
                                                                               and		cpl.branch = cla.branch and cpl.policy = cla.policy),0) moneda_cod,
                                                               coalesce((	select  case	when	max(gco.addsuini) = '3'
                                                                                                                       then	max(case when gco.addsuini = '3' then coalesce(cov.capital,0) else 0 end)
                                                                                                                       else	sum(case when gco.addsuini = '1' then coalesce(cov.capital,0) else 0 end) end
                                                                                       from    usinsug01.cover cov
                                                                                       join	usinsug01.gen_cover gco on gco.ctid =
                                                                       coalesce(
                                                                                                               (   select  max(ctid)
                                                                               from	usinsug01.gen_cover
                                                                               where	usercomp = cla.usercomp and company = cla.company and branch = cla.branch --�ndice regular (1)
                                                                               and		product = cl0.pol_product and coalesce(sub_product,0) = 0 and currency = cov.currency --�ndice regular (2)
                                                                               and     modulec = cov.modulec and cover = cov.cover --variaci�n 1 con modulec
                                                                               and     effecdate <= cla.occurdat and (nulldate is null or nulldate > cla.occurdat) --variaci�n 2 vigencias
                                                                               and     statregt <> '4'), --variaci�n 3 reg. v�lido
                                                                       (   select  max(ctid)
                                                                               from	usinsug01.gen_cover
                                                                               where	usercomp = cla.usercomp and company = cla.company and branch = cla.branch --�ndice regular (1)
                                                                               and		product = cl0.pol_product and coalesce(sub_product,0) = 0 and currency = cov.currency --�ndice regular (2)
                                                                               and     coalesce(modulec,0) = 0 and cover = cov.cover --variaci�n 1 sin modulec
                                                                               and     effecdate <= cla.occurdat and (nulldate is null or nulldate > cla.occurdat) --variaci�n 2 vigencias
                                                                               and     statregt <> '4'), --variaci�n 3 reg. v�lido
                                                                                                               (	select  max(ctid)
                                                                               from	usinsug01.gen_cover
                                                                               where	usercomp = cla.usercomp and company = cla.company and branch = cla.branch --�ndice regular (1)
                                                                               and		product = cl0.pol_product and coalesce(sub_product,0) = 0 and currency = cov.currency --�ndice regular (2)
                                                                               and     modulec = cov.modulec and cover = cov.cover
                                                                               and     effecdate <= cla.occurdat and (nulldate is null or nulldate > cla.occurdat)
                                                                               and     statregt = '4'),
                                                                       (	select  max(ctid)
                                                                               from	usinsug01.gen_cover
                                                                               where	usercomp = cla.usercomp and company = cla.company and branch = cla.branch --�ndice regular (1)
                                                                               and		product = cl0.pol_product and coalesce(sub_product,0) = 0 and currency = cov.currency --�ndice regular (2)
                                                                               and     coalesce(modulec,0) = 0 and cover = cov.cover
                                                                               and     effecdate <= cla.occurdat and (nulldate is null or nulldate > cla.occurdat)
                                                                               and     statregt = '4'), --no est� cortado
                                                                                                               (	select  max(ctid)
                                                                               from	usinsug01.gen_cover
                                                                               where	usercomp = cla.usercomp and company = cla.company and branch = cla.branch --�ndice regular (1)
                                                                               and		product = cl0.pol_product and coalesce(sub_product,0) = 0 and currency = cov.currency --�ndice regular (2)
                                                                               and     modulec = cov.modulec and cover = cov.cover
                                                                               and     effecdate <= cla.occurdat and nulldate <= cla.occurdat
                                                                               and     statregt <> '4'),
                                                                       (   select  max(ctid)
                                                                               from	usinsug01.gen_cover
                                                                               where	usercomp = cla.usercomp and company = cla.company and branch = cla.branch --�ndice regular (1)
                                                                               and		product = cl0.pol_product and coalesce(sub_product,0) = 0 and currency = cov.currency --�ndice regular (2)
                                                                               and     coalesce(modulec,0) = 0 and cover = cov.cover
                                                                               and     effecdate <= cla.occurdat and nulldate <= cla.occurdat
                                                                               and     statregt <> '4'), -- no est� cortado, pero fue anulado antes del efecto del registro
                                                                                                               (	select  max(ctid)
                                                                               from	usinsug01.gen_cover
                                                                               where	usercomp = cla.usercomp and company = cla.company and branch = cla.branch --�ndice regular (1)
                                                                               and		product = cl0.pol_product and coalesce(sub_product,0) = 0 and currency = cov.currency --�ndice regular (2)
                                                                               and     modulec = cov.modulec and cover = cov.cover
                                                                               and     effecdate <= cla.occurdat and nulldate <= cla.occurdat
                                                                               and     statregt = '4'),
                                                                       (   select  max(ctid)
                                                                               from	usinsug01.gen_cover
                                                                               where	usercomp = cla.usercomp and company = cla.company and branch = cla.branch --�ndice regular (1)
                                                                               and		product = cl0.pol_product and coalesce(sub_product,0) = 0 and currency = cov.currency --�ndice regular (2)
                                                                               and     coalesce(modulec,0) = 0 and cover = cov.cover
                                                                               and     effecdate <= cla.occurdat and nulldate <= cla.occurdat
                                                                               and     statregt = '4'), --no est� cortado y aparte fue anulado antes del efecto del registro
                                                                                                               (	select  max(ctid)
                                                                               from	usinsug01.gen_cover
                                                                               where	usercomp = cla.usercomp and company = cla.company and branch = cla.branch --�ndice regular (1)
                                                                               and		product = cl0.pol_product and coalesce(sub_product,0) = 0 and currency = cov.currency --�ndice regular (2)
                                                                               and     modulec = cov.modulec and cover = cov.cover
                                                                               and     effecdate > cla.occurdat
                                                                               and     statregt <> '4'),
                                                                       (   select  max(ctid)
                                                                               from	usinsug01.gen_cover
                                                                               where	usercomp = cla.usercomp and company = cla.company and branch = cla.branch --�ndice regular (1)
                                                                               and		product = cl0.pol_product and coalesce(sub_product,0) = 0 and currency = cov.currency --�ndice regular (2)
                                                                               and     coalesce(modulec,0) = 0 and cover = cov.cover
                                                                               and     effecdate <= cla.occurdat and nulldate <= cla.occurdat
                                                                               and     statregt <> '4'), -- no est� cortado, pero tampoco al efecto de la tabla de datos particular
                                                                                                               (	select  max(ctid)
                                                                               from	usinsug01.gen_cover
                                                                               where	usercomp = cla.usercomp and company = cla.company and branch = cla.branch --�ndice regular (1)
                                                                               and		product = cl0.pol_product and coalesce(sub_product,0) = 0 and currency = cov.currency --�ndice regular (2)
                                                                               and     modulec = cov.modulec and cover = cov.cover
                                                                               and     effecdate <= cla.occurdat and nulldate <= cla.occurdat
                                                                               and     statregt = '4'),
                                                                       (   select  max(ctid)
                                                                               from	usinsug01.gen_cover
                                                                               where	usercomp = cla.usercomp and company = cla.company and branch = cla.branch --�ndice regular (1)
                                                                               and		product = cl0.pol_product and coalesce(sub_product,0) = 0 and currency = cov.currency --�ndice regular (2)
                                                                               and     coalesce(modulec,0) = 0 and cover = cov.cover
                                                                               and     effecdate > cla.occurdat
                                                                               and     statregt = '4')) --est� cortado y no al efecto de la tabla de datos particular adem�s de estar cortado
                                                                                       where   cov.usercomp = cla.usercomp
                                                                                       and     cov.company = cla.company
                                                                                       and     cov.certype = cl0.pol_certype
                                                                                       and     cov.branch = cla.branch
                                                                                       and     cov.policy = cla.policy
                                                                                       and     cov.certif = cla.certif
                                                                                       and     cov.effecdate <= cla.occurdat
                                                                                       and     (cov.nulldate is null or cov.nulldate > cla.occurdat)),0) *
                                                               case	when	cla.branch = 66
                                                                               then	coalesce((	select	max(exc.exchange)
                                                                                                                       from 	usinsug01.exchange exc
                                                                                                                       where	exc.usercomp = cla.usercomp
                                                                                                                       and 	exc.company = cla.company 
                                                                                                                       and 	exc.currency = 99
                                                                                                                       and 	exc.effecdate <= cla.occurdat
                                                                                                                       and 	(exc.nulldate is null or exc.nulldate > cla.occurdat)),0)
                                                                               else	1 end capital
                                               from	(	select	cla.ctid cla_id,
                                                                                       pol.certype pol_certype,
                                                                                       pol.product pol_product,
                                                                                       pol.leadpoli,
                                                                                       pol.bussityp,
                                                                                       coalesce((	select	sub_product
                                                                                                               from	usinsug01.pol_subproduct
                                                                                                               where	usercomp = cla.usercomp
                                                                                                               and 	company = cla.company
                                                                                                               and		certype = pol.certype
                                                                                                               and		branch = cla.branch
                                                                                                               and		policy = cla.policy
                                                                                                               and		product = pol.product),0) sub_product,
                                                                                       case	when	pol.bussityp = '2'
                                                                                                       then	cla.occurdat
                                                                                                       else	(	select 	max(greatest(
                                                                                                                                                       case when cla.occurdat <= dat.dat_lim then cla.occurdat else '0001-01-01'::date end,
                                                                                                                                                       case when coi.effecdate <= dat.dat_lim then coi.effecdate else '0001-01-01'::date end,
                                                                                                                                                       case when coi.compdate <= dat.dat_lim then coi.compdate else '0001-01-01'::date end))
                                                                                                                               from	usinsug01.coinsuran coi
                                                                                                                               where	coi.usercomp = cla.usercomp
                                                                                                                               and     coi.company = cla.company
                                                                                                                               and     coi.certype = pol.certype
                                                                                                                               and     coi.branch = cla.branch
                                                                                                                               and     coi.policy = cla.policy
                                                                                                                               and		coi.effecdate <= cla.occurdat
                                                                                                                               and		(coi.nulldate is null or coi.nulldate > cla.occurdat))
                                                                                                       end xxxxdate
                                                                       from 	(	select	prv.cla_id,
                                                                                                               prv.par_fin dat_lim
                                                                                               from 	(	--revisi�n fechas cabecera
                                                                                                                       select	cla.ctid cla_id,
                                                                                                                                       cla.claim,
                                                                                                                                       cla.compdate,
                                                                                                                                       dat.par_ini,
                                                                                                                                       dat.par_fin
                                                                                                                       from	(	select	cast('{p_fecha_inicio}' as date) par_ini, --rango inicial carga incremental (variable a elecci�n)
                                                                                                                                                cast('{p_fecha_fin}' as date) par_fin) dat --rango final carga incremental (variable a elecci�n)
                                                                                                                       join	usinsug01.claim cla
                                                                                                                                       on		cla.usercomp = 1
                                                                                                                                       and 	cla.company = 1
                                                                                                                                       and		cla.branch in (select branch from usinsug01.table10b where company = 1)
                                                                                                                                       and		(cla.staclaim <> '6' or (cla.staclaim = '6' and cla.branch = 23))
                                                                                                                                       and		cla.compdate between dat.par_ini and dat.par_fin
                                                                                                                       union
                                                                                                                       --revisi�n fechas transacciones
                                                                                                                       select	cla.ctid cla_id,
                                                                                                                                       cla.claim,
                                                                                                                                       cla.compdate,
                                                                                                                                       dat.par_ini,
                                                                                                                                       dat.par_fin
                                                                                                                       from	(	select	cast('{p_fecha_inicio}' as date) par_ini, --rango inicial carga incremental (variable a elecci�n)
                                                                                                                                                cast('{p_fecha_fin}' as date) par_fin) dat --rango final carga incremental (variable a elecci�n)
                                                                                                                       join	usinsug01.claim cla
                                                                                                                                       on		cla.usercomp = 1
                                                                                                                                       and 	cla.company = 1
                                                                                                                                       and		cla.branch in (select branch from usinsug01.table10b where company = 1)
                                                                                                                                       and		(cla.staclaim <> '6' or (cla.staclaim = '6' and cla.branch = 23))
                                                                                                                       join	usinsug01.claim_his clh
                                                                                                                                       on		clh.claim = cla.claim
                                                                                                                                       and		trim(clh.oper_type) in 
                                                                                                                                                       (select cast(operation as varchar(2)) from usinsug01.tab_cl_ope where reserve = 1 or ajustes = 1 or pay_amount = 1)
                                                                                                                                       and     (clh.operdate between dat.par_ini and dat.par_fin --se consideran siniestros con operaciones entre los rangos (limitaci�n carga inicial)
                                                                                                                                                       or clh.compdate between dat.par_ini and dat.par_fin)
                                                                                                                       union
                                                                                                                       --revisi�n fechas coberturas asociadas a transacciones
                                                                                                                       select	cla.ctid cla_id,
                                                                                                                                       cla.claim,
                                                                                                                                       cla.compdate,
                                                                                                                                       dat.par_ini,
                                                                                                                                       dat.par_fin
                                                                                                                       from	(	select	cast('{p_fecha_inicio}' as date) par_ini, --rango inicial carga incremental (variable a elecci�n)
                                                                                                                                                cast('{p_fecha_fin}' as date) par_fin) dat --rango final carga incremental (variable a elecci�n)
                                                                                                                       join	usinsug01.claim cla
                                                                                                                                       on		cla.usercomp = 1
                                                                                                                                       and 	cla.company = 1
                                                                                                                                       and		cla.branch in (select branch from usinsug01.table10b where company = 1)
                                                                                                                                       and		(cla.staclaim <> '6' or (cla.staclaim = '6' and cla.branch = 23))
                                                                                                                       join	usinsug01.claim_his clh
                                                                                                                                       on		clh.claim = cla.claim
                                                                                                                                       and		trim(clh.oper_type) in 
                                                                                                                                                       (select cast(operation as varchar(2)) from usinsug01.tab_cl_ope where reserve = 1 or ajustes = 1 or pay_amount = 1)
                                                                                                                       join	usinsug01.cl_m_cover clm
                                                                                                                                       on		clm.usercomp = clh.usercomp
                                                                                                                                       and		clm.company = clh.company
                                                                                                                                       and		clm.claim = clh.claim
                                                                                                                                       and		clm.movement = clh.transac
                                                                                                                                       and		clm.compdate between dat.par_ini and dat.par_fin) prv
                                                                                               where	(	select 	max(greatest(clm.compdate,clh.compdate,clh.operdate,prv.compdate))
                                                                                                                       from	usinsug01.claim_his clh
                                                                                                                       join	usinsug01.cl_m_cover clm
                                                                                                                                       on		clm.usercomp = clh.usercomp
                                                                                                                                       and		clm.company = clh.company
                                                                                                                                       and		clm.claim = clh.claim
                                                                                                                                       and		clm.movement = clh.transac
                                                                                                                       where	clh.claim = prv.claim
                                                                                                                       and		trim(clh.oper_type) in 
                                                                                                                                       (select cast(operation as varchar(2)) from usinsug01.tab_cl_ope where reserve = 1 or ajustes = 1 or pay_amount = 1)) <= prv.par_fin
                                                                                               and 	(	exists
                                                                                                                       (	select 	1
                                                                                                                               from	usinsug01.claim cla
                                                                                                                               join	usinsug01.coinsuran coi
                                                                                                                                               on		coi.usercomp = cla.usercomp
                                                                                                                                               and     coi.company = cla.company
                                                                                                                                               and     coi.certype = '2'
                                                                                                                                               and     coi.branch = cla.branch
                                                                                                                                               and     coi.policy = cla.policy
                                                                                                                                               and		coi.effecdate <= cla.occurdat
                                                                                                                                               and		(coi.nulldate is null or coi.nulldate > cla.occurdat)
                                                                                                                               where	cla.ctid = prv.cla_id)
                                                                                                                       or exists
                                                                                                                       (	select 	1
                                                                                                                               from	usinsug01.claim cla
                                                                                                                               join	usinsug01.policy pol
                                                                                                                                               on		pol.usercomp = cla.usercomp
                                                                                                                                               and     pol.company = cla.company
                                                                                                                                               and     pol.certype = '2'
                                                                                                                                               and     pol.branch = cla.branch
                                                                                                                                               and     pol.policy = cla.policy
                                                                                                                                               and		pol.bussityp = '2'
                                                                                                                               where	cla.ctid = prv.cla_id))) dat
                                                                       join	usinsug01.claim cla on cla.ctid = dat.cla_id
                                                                       join	usinsug01.policy pol
                                                                                       on		pol.usercomp = cla.usercomp
                                                                                       and 	pol.company = cla.company
                                                                                       and 	pol.certype = '2'
                                                                                       and 	pol.branch = cla.branch
                                                                                       and 	pol.policy = cla.policy) cl0
                                               join	usinsug01.claim cla on cla.ctid = cl0.cla_id) cl0
                       left 	join usinsug01.coinsuran coi
                                       on		coi.usercomp = cl0.usercomp
                                       and     coi.company = cl0.company
                                       and     coi.certype = cl0.certype
                                       and     coi.branch = cl0.branch
                                       and     coi.policy = cl0.policy
                                       and		coi.effecdate <= cl0.occurdat
                                       and		(coi.nulldate is null or coi.nulldate > cl0.occurdat)
                      ) AS TMP    
                   '''

    DF_INSUNIX_LPG = glue_context.read.format('jdbc').options(**connection).option("fetchsize",10000).option("dbtable",INSUNIX_LPG).load()
  
    INSUNIX_LPV = f'''
                      (select	
                       'D' INDDETREC,
                       'SBCOSSEG' TABLAIFRS17,
                       '' PK,
                       '' DTPREG, --excluido
                       '' TIOCPROC, --excluido
                       cast(cl0.xxxxdate as varchar) TIOCFRM,
                       '' TIOCTO, --excluido
                       'PIV' KGIORIGM,
                       'LPV' DCOMPA,
                       '' DMARCA, --excluido
                       cast(cl0.claim as varchar) KSBSIN,
                       cast(case when cl0.bussityp = '2' then 1 else coalesce(coi.companyc,0) end as varchar) DCODCSG,
                       '' DNUMSEQ, --excluido
                       '' TDPLANO, --excluido
                       cast(cl0.capital as numeric(14,2)) VMTCAPIT,
                       cast(case when cl0.bussityp = '2' then 100 else coalesce(coi.share,0) end as numeric (9,6)) VTXQUOTA,
                       '' VMTINDEM, --NOAPP
                       case when cl0.bussityp = '2' then cl0.leadpoli else cast(cl0.policy as varchar) end DNUMAPO_CSG,
                       cast(cl0.moneda_cod as varchar) KSCMOEDA
                       from	(	select	cla.usercomp,
                                                               cla.company,
                                                               cla.branch,
                                                               cla.policy,
                                                               cla.certif,
                                                               cla.occurdat,
                                                               cla.claim,
                                                               pol.certype, --se renombr� de vuelta solo para el �ndice en el siguiente nivel
                                                               pol.leadpoli,
                                                               pol.bussityp,
                                                               case	when	pol.bussityp = '2'
                                                                               then	cla.occurdat
                                                                               else	(	select 	max(greatest(
                                                                                                                               case when cla.occurdat <= dat.dat_lim then cla.occurdat else '0001-01-01'::date end,
                                                                                                                               case when coi.effecdate <= dat.dat_lim then coi.effecdate else '0001-01-01'::date end,
                                                                                                                               case when coi.compdate <= dat.dat_lim then coi.compdate else '0001-01-01'::date end))
                                                                                                       from	usinsuv01.coinsuran coi
                                                                                                       where	coi.usercomp = cla.usercomp
                                                                                                       and     coi.company = cla.company
                                                                                                       and     coi.certype = pol.certype
                                                                                                       and     coi.branch = cla.branch
                                                                                                       and     coi.policy = cla.policy
                                                                                                       and		coi.effecdate <= cla.occurdat
                                                                                                       and		(coi.nulldate is null or coi.nulldate > cla.occurdat))
                                                                               end xxxxdate,
                                                               coalesce(
                                                                       (	select	max(cpl.currency)
                                                                               from	usinsuv01.curren_pol cpl
                                                                               where 	cpl.usercomp = cla.usercomp and cpl.company = cla.company and cpl.certype = pol.certype
                                                                               and		cpl.branch = cla.branch and cpl.policy = cla.policy and	cpl.certif = cla.certif),
                                                                       (	select	max(cpl.currency)
                                                                               from	usinsuv01.curren_pol cpl
                                                                               where 	cpl.usercomp = cla.usercomp and cpl.company = cla.company and cpl.certype = pol.certype
                                                                               and		cpl.branch = cla.branch and cpl.policy = cla.policy),0) moneda_cod,
                                                               coalesce(	case    when    coalesce((  select  distinct pro.brancht
                                                                                                       from    usinsuv01.product pro
                                                                                                       where	pro.usercomp = cla.usercomp
                                                                                                       and		pro.company = cla.company
                                                                                                       and		pro.branch = cla.branch
                                                                                                       and		pro.product = pol.product
                                                                                                       and		pro.effecdate <= cla.occurdat
                                                                                                       and		(pro.nulldate is null or pro.nulldate > cla.occurdat)),'0') not in ('1','5')
                                                                                       then    (   select  sum(coalesce(cov.capital,0))
                                                                                                               from    usinsuv01.cover cov
                                                                                                               join	usinsuv01.gen_cover gco on gco.ctid =
                                                                                                                               coalesce(
                                                                                                                                       (   select  max(ctid)
                                                                                                                                               from	usinsuv01.gen_cover
                                                                                                                                               where   usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = pol.product and currency = cov.currency --�ndice regular
                                                                                                                                               and		modulec = cov.modulec and cover = cov.cover --variaci�n 1 con modulec
                                                                                                                                               and		effecdate <= cla.occurdat and (nulldate is null or nulldate > cla.occurdat) --variaci�n 2 vigencias
                                                                                                                                               and		statregt <> '4' and addsuini = '1'), --variaci�n 3 reg. v�lido
                                                                                                                                       (   select  max(ctid)
                                                                                                                                               from	usinsuv01.gen_cover
                                                                                                                                               where   usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = pol.product and currency = cov.currency --�ndice regular
                                                                                                                                               and		coalesce(modulec,0) = 0 and cover = cov.cover --variaci�n 1 sin modulec
                                                                                                                                               and		effecdate <= cla.occurdat and (nulldate is null or nulldate > cla.occurdat) --variaci�n 2 vigencias
                                                                                                                                               and		statregt <> '4' and addsuini = '1'), --variaci�n 3 reg. v�lido
                                                                                                                                       (  select  max(ctid)
                                                                                                                                               from	usinsuv01.gen_cover
                                                                                                                                               where   usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = pol.product and currency = cov.currency --�ndice regular
                                                                                                                                               and		modulec = cov.modulec and cover = cov.cover
                                                                                                                                               and		effecdate <= cla.occurdat and (nulldate is null or nulldate > cla.occurdat)
                                                                                                                                               and		statregt = '4' and addsuini = '1'),
                                                                                                                                       (   select  max(ctid)
                                                                                                                                               from	usinsuv01.gen_cover
                                                                                                                                               where   usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = pol.product and currency = cov.currency --�ndice regular
                                                                                                                                               and		coalesce(modulec,0) = 0 and cover = cov.cover
                                                                                                                                               and		effecdate <= cla.occurdat and (nulldate is null or nulldate > cla.occurdat)
                                                                                                                                               and		statregt = '4' and addsuini = '1'), --no est� cortado
                                                                                                                                       (   select  max(ctid)
                                                                                                                                               from	usinsuv01.gen_cover
                                                                                                                                               where   usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = pol.product and currency = cov.currency --�ndice regular
                                                                                                                                               and		modulec = cov.modulec and cover = cov.cover
                                                                                                                                               and		effecdate <= cla.occurdat and nulldate <= cla.occurdat
                                                                                                                                               and		statregt <> '4' and addsuini = '1'),
                                                                                                                                       (   select  max(ctid)
                                                                                                                                               from	usinsuv01.gen_cover
                                                                                                                                               where   usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = pol.product and currency = cov.currency --�ndice regular
                                                                                                                                               and		coalesce(modulec,0) = 0 and cover = cov.cover
                                                                                                                                               and		effecdate <= cla.occurdat and nulldate <= cla.occurdat
                                                                                                                                               and		statregt <> '4' and addsuini = '1'), -- no est� cortado, pero fue anulado antes del efecto del registro
                                                                                                                                       (   select  max(ctid)
                                                                                                                                               from	usinsuv01.gen_cover
                                                                                                                                               where   usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = pol.product and currency = cov.currency --�ndice regular
                                                                                                                                               and		modulec = cov.modulec and cover = cov.cover
                                                                                                                                               and		effecdate <= cla.occurdat and nulldate <= cla.occurdat
                                                                                                                                               and		statregt = '4' and addsuini = '1'),
                                                                                                                                       (   select  max(ctid)
                                                                                                                                               from	usinsuv01.gen_cover
                                                                                                                                               where   usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = pol.product and currency = cov.currency --�ndice regular
                                                                                                                                               and		coalesce(modulec,0) = 0 and cover = cov.cover
                                                                                                                                               and		effecdate <= cla.occurdat and nulldate <= cla.occurdat
                                                                                                                                               and		statregt = '4' and addsuini = '1'), --no est� cortado y aparte fue anulado antes del efecto del registro
                                                                                                                                       (   select  max(ctid)
                                                                                                                                               from	usinsuv01.gen_cover
                                                                                                                                               where   usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = pol.product and currency = cov.currency --�ndice regular
                                                                                                                                               and		modulec = cov.modulec and cover = cov.cover
                                                                                                                                               and		effecdate > cla.occurdat
                                                                                                                                               and		statregt <> '4' and addsuini = '1'),
                                                                                                                                       (   select  max(ctid)
                                                                                                                                               from	usinsuv01.gen_cover
                                                                                                                                               where   usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = pol.product and currency = cov.currency --�ndice regular
                                                                                                                                               and		coalesce(modulec,0) = 0 and cover = cov.cover
                                                                                                                                               and		effecdate <= cla.occurdat and nulldate <= cla.occurdat
                                                                                                                                               and		statregt <> '4' and addsuini = '1'), -- no est� cortado, pero tampoco al efecto de la tabla de datos particular
                                                                                                                                       (   select  max(ctid)
                                                                                                                                               from	usinsuv01.gen_cover
                                                                                                                                               where   usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = pol.product and currency = cov.currency --�ndice regular
                                                                                                                                               and		modulec = cov.modulec and cover = cov.cover
                                                                                                                                               and		effecdate <= cla.occurdat and nulldate <= cla.occurdat
                                                                                                                                               and		statregt = '4' and addsuini = '1'),
                                                                                                                                       (   select  max(ctid)
                                                                                                                                               from	usinsuv01.gen_cover
                                                                                                                                               where   usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = pol.product and currency = cov.currency --�ndice regular
                                                                                                                                               and		coalesce(modulec,0) = 0 and cover = cov.cover
                                                                                                                                               and		effecdate > cla.occurdat
                                                                                                                                               and		statregt = '4' and addsuini = '1')) --est� cortado y no al efecto de la tabla de datos particular adem�s de estar cortado
                                                                                                               where	cov.usercomp = cla.usercomp
                                                                                                               and		cov.company = cla.company
                                                                                                               and		cov.certype = pol.certype
                                                                                                               and		cov.branch = cla.branch
                                                                                                               and		cov.policy = cla.policy
                                                                                                               and		cov.certif = cla.certif
                                                                                                               and		cov.effecdate <= cla.occurdat
                                                                                                               and		(cov.nulldate is null or cov.nulldate > cla.occurdat))
                                                                                       else    (   select  sum(cov.capital)
                                                                                                               from    usinsuv01.cover cov
                                                                                                               join	usinsuv01.life_cover gco on gco.ctid =
                                                                                                                               coalesce(
                                                                                                                                       (	select  max(ctid)
                                                                                                                                               from 	usinsuv01.life_cover
                                                                                                                                               where   usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = pol.product and currency = cov.currency --�ndice regular
                                                                                                                                               and		cover = cov.cover
                                                                                                                                               and		effecdate <= cla.occurdat and (nulldate is null or nulldate > cla.occurdat)
                                                                                                                                               and		statregt <> '4'  and addcapii = '1'), --que no est� cortado
                                                                                                                                       (   select  max(ctid)
                                                                                                                                               from 	usinsuv01.life_cover
                                                                                                                                               where   usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = pol.product and currency = cov.currency --�ndice regular
                                                                                                                                               and		cover = cov.cover
                                                                                                                                               and		effecdate <= cla.occurdat and (nulldate is null or nulldate > cla.occurdat)
                                                                                                                                               and		statregt = '4'  and addcapii = '1'),--est� cortado
                                                                                                                                       (   select  max(ctid)
                                                                                                                                               from 	usinsuv01.life_cover
                                                                                                                                               where   usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = pol.product and currency = cov.currency --�ndice regular
                                                                                                                                               and		cover = cov.cover
                                                                                                                                               and		effecdate <= cla.occurdat and nulldate <= cla.occurdat
                                                                                                                                               and		statregt <> '4' and addcapii = '1'),--no est� cortado pero fue anulado antes del efecto del registro
                                                                                                                                       (   select  max(ctid)
                                                                                                                                               from 	usinsuv01.life_cover
                                                                                                                                               where   usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = pol.product and currency = cov.currency --�ndice regular
                                                                                                                                               and		cover = cov.cover
                                                                                                                                               and		effecdate <= cla.occurdat and nulldate <= cla.occurdat
                                                                                                                                               and		statregt = '4' and addcapii = '1'), --est� cortado y aparte fue anulado antes del efecto del registro
                                                                                                                                       (   select  max(ctid)
                                                                                                                                               from 	usinsuv01.life_cover
                                                                                                                                               where   usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = pol.product and currency = cov.currency --�ndice regular
                                                                                                                                               and		cover = cov.cover
                                                                                                                                               and		effecdate > cla.occurdat
                                                                                                                                               and		statregt <> '4' and addcapii = '1'),
                                                                                                                                       (   select  max(ctid)
                                                                                                                                               from 	usinsuv01.life_cover
                                                                                                                                               where   usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = pol.product and currency = cov.currency --�ndice regular
                                                                                                                                               and		cover = cov.cover
                                                                                                                                               and		effecdate > cla.occurdat --est� cortado pero no al efecto de la tabla de datos particular
                                                                                                                                       and		statregt = '4' and addcapii = '1'))
                                                                                                               where	cov.usercomp = cla.usercomp
                                                                                                               and		cov.company = cla.company
                                                                                                               and		cov.certype = pol.certype
                                                                                                               and		cov.branch = cla.branch
                                                                                                               and		cov.policy = cla.policy
                                                                                                               and		cov.certif = cla.certif
                                                                                                               and		cov.effecdate <= cla.occurdat
                                                                                                               and		(cov.nulldate is null or cov.nulldate > cla.occurdat))
                                                                               end, 0)  capital
                                               from 	(	select	prv.cla_id,
                                                                                       prv.par_fin dat_lim
                                                                       from 	(	--revisi�n fechas cabecera
                                                                                               select	cla.ctid cla_id,
                                                                                                               cla.claim,
                                                                                                               cla.compdate,
                                                                                                               dat.par_ini,
                                                                                                               dat.par_fin
                                                                                               from	(	select	cast('{p_fecha_inicio}' as date) par_ini, --rango inicial carga incremental (variable a elecci�n)
                                                                                                                        cast('{p_fecha_fin}' as date) par_fin) dat --rango final carga incremental (variable a elecci�n)
                                                                                               join	usinsuv01.claim cla
                                                                                                               on		cla.usercomp = 1
                                                                                                               and 	cla.company = 1
                                                                                                               and		cla.branch in (select branch from usinsug01.table10b where company = 2)
                                                                                                               and		cla.staclaim <> '6'
                                                                                                               and		cla.compdate between dat.par_ini and dat.par_fin
                                                                                               union
                                                                                               --revisi�n fechas transacciones
                                                                                               select	cla.ctid cla_id,
                                                                                                               cla.claim,
                                                                                                               cla.compdate,
                                                                                                               dat.par_ini,
                                                                                                               dat.par_fin
                                                                                               from	(	select	cast('{p_fecha_inicio}' as date) par_ini, --rango inicial carga incremental (variable a elecci�n)
                                                                                                                        cast('{p_fecha_fin}' as date) par_fin) dat --rango final carga incremental (variable a elecci�n)
                                                                                               join	usinsuv01.claim cla
                                                                                                               on		cla.usercomp = 1
                                                                                                               and 	cla.company = 1
                                                                                                               and		cla.branch in (select branch from usinsug01.table10b where company = 2)
                                                                                                               and		cla.staclaim <> '6'
                                                                                               join	usinsuv01.claim_his clh
                                                                                                               on		clh.claim = cla.claim
                                                                                                               and		trim(clh.oper_type) in 
                                                                                                                               (select cast(operation as varchar(2)) from usinsug01.tab_cl_ope where reserve = 1 or ajustes = 1 or pay_amount = 1)
                                                                                                               and     (clh.operdate between dat.par_ini and dat.par_fin --se consideran siniestros con operaciones entre los rangos (limitaci�n carga inicial)
                                                                                                                               or clh.compdate between dat.par_ini and dat.par_fin)
                                                                                               union
                                                                                               --revisi�n fechas coberturas asociadas a transacciones
                                                                                               select	cla.ctid cla_id,
                                                                                                               cla.claim,
                                                                                                               cla.compdate,
                                                                                                               dat.par_ini,
                                                                                                               dat.par_fin
                                                                                               from	(	select	cast('{p_fecha_inicio}' as date) par_ini, --rango inicial carga incremental (variable a elecci�n)
                                                                                                                        cast('{p_fecha_fin}' as date) par_fin) dat --rango final carga incremental (variable a elecci�n)
                                                                                               join	usinsuv01.claim cla
                                                                                                               on		cla.usercomp = 1
                                                                                                               and 	cla.company = 1
                                                                                                               and		cla.branch in (select branch from usinsug01.table10b where company = 2)
                                                                                                               and		cla.staclaim <> '6'
                                                                                               join	usinsuv01.claim_his clh
                                                                                                               on		clh.claim = cla.claim
                                                                                                               and		trim(clh.oper_type) in 
                                                                                                                               (select cast(operation as varchar(2)) from usinsug01.tab_cl_ope where reserve = 1 or ajustes = 1 or pay_amount = 1)
                                                                                               join	usinsuv01.cl_m_cover clm
                                                                                                               on		clm.usercomp = clh.usercomp
                                                                                                               and		clm.company = clh.company
                                                                                                               and		clm.claim = clh.claim
                                                                                                               and		clm.movement = clh.transac
                                                                                                               and		clm.compdate between dat.par_ini and dat.par_fin) prv
                                                                       where	(	select 	max(greatest(clm.compdate,clh.compdate,clh.operdate,prv.compdate))
                                                                                               from	usinsuv01.claim_his clh
                                                                                               join	usinsuv01.cl_m_cover clm
                                                                                                               on		clm.usercomp = clh.usercomp
                                                                                                               and		clm.company = clh.company
                                                                                                               and		clm.claim = clh.claim
                                                                                                               and		clm.movement = clh.transac
                                                                                               where	clh.claim = prv.claim
                                                                                               and		trim(clh.oper_type) in 
                                                                                                               (select cast(operation as varchar(2)) from usinsug01.tab_cl_ope where reserve = 1 or ajustes = 1 or pay_amount = 1)) <= prv.par_fin
                                                                       and 	(	exists
                                                                                               (	select 	1
                                                                                                       from	usinsuv01.claim cla
                                                                                                       join	usinsuv01.coinsuran coi
                                                                                                                       on		coi.usercomp = cla.usercomp
                                                                                                                       and     coi.company = cla.company
                                                                                                                       and     coi.certype = '2'
                                                                                                                       and     coi.branch = cla.branch
                                                                                                                       and     coi.policy = cla.policy
                                                                                                                       and		coi.effecdate <= cla.occurdat
                                                                                                                       and		(coi.nulldate is null or coi.nulldate > cla.occurdat)
                                                                                                       where	cla.ctid = prv.cla_id)
                                                                                               or exists
                                                                                               (	select 	1
                                                                                                       from	usinsuv01.claim cla
                                                                                                       join	usinsuv01.policy pol
                                                                                                                       on		pol.usercomp = cla.usercomp
                                                                                                                       and     pol.company = cla.company
                                                                                                                       and     pol.certype = '2'
                                                                                                                       and     pol.branch = cla.branch
                                                                                                                       and     pol.policy = cla.policy
                                                                                                                       and		pol.bussityp = '2'
                                                                                                       where	cla.ctid = prv.cla_id))) dat
                                               join	usinsuv01.claim cla on cla.ctid = dat.cla_id
                                               join	usinsuv01.policy pol
                                                               on		pol.usercomp = cla.usercomp
                                                               and 	pol.company = cla.company
                                                               and 	pol.certype = '2'
                                                               and 	pol.branch = cla.branch
                                                               and 	pol.policy = cla.policy) cl0
                       left 	join usinsuv01.coinsuran coi
                                       on		coi.usercomp = cl0.usercomp
                                       and     coi.company = cl0.company
                                       and     coi.certype = cl0.certype
                                       and     coi.branch = cl0.branch
                                       and     coi.policy = cl0.policy
                                       and		coi.effecdate <= cl0.occurdat
                                       and		(coi.nulldate is null or coi.nulldate > cl0.occurdat)
                      ) AS TMP
                   '''    

    DF_INSUNIX_LPV = glue_context.read.format('jdbc').options(**connection).option("fetchsize",10000).option("dbtable",INSUNIX_LPV).load()
  
    VTIME_LPG =  f'''
                     (select	
                      'D' INDDETREC,
                      'SBCOSSEG' TABLAIFRS17,
                      '' PK,
                      '' DTPREG, --excluido
                      '' TIOCPROC, --excluido
                      cast(cl0.xxxxdate as varchar) TIOCFRM,
                      '' TIOCTO, --excluido
                      'PVG' KGIORIGM,
                      'LPG' DCOMPA,
                      '' DMARCA, --excluido
                      cast(cl0.nclaim as varchar) KSBSIN,
                      cast(case when cl0.sbussityp = '2' then 1 else coalesce(coi."NCOMPANY",0) end as varchar) DCODCSG,
                      '' DNUMSEQ, --excluido
                      '' TDPLANO, --excluido
                      cast(cl0.ncapital as numeric(14,2)) VMTCAPIT,
                      cast(case when cl0.sbussityp = '2' then 100 else coalesce(coi."NSHARE",0) end as numeric (9,6)) VTXQUOTA,
                      '' VMTINDEM, --NOAPP
                      case when cl0.sbussityp = '2' then cl0.sleadpoli else cast(cl0.npolicy as varchar) end DNUMAPO_CSG,
                      cast(cl0.moneda_cod as varchar) KSCMOEDA
                      from	(	select  cla."NBRANCH" nbranch,
                                      cla."NPOLICY" npolicy,
                                      cla."NCERTIF" ncertif,
                                                              cla."NCLAIM" nclaim,
                                      cast(cla."DOCCURDAT" as date) doccurdat,
                                      pol."SCERTYPE" scertype,
                                      pol."NPRODUCT" nproduct,
                                      pol."SLEADPOLI" sleadpoli,
                                                              pol."SBUSSITYP" sbussityp,
                                                              coalesce(
                                                                      (	select  max(cpl."NCURRENCY")
                                                                              from    usvtimg01."CURREN_POL" cpl
                                                                              where 	cpl."SCERTYPE" = cla."SCERTYPE" and	cpl."NBRANCH" = cla."NBRANCH" 
                                                                              and		cpl."NPRODUCT" = pol."NPRODUCT" and cpl."NPOLICY" = cla."NPOLICY"
                                                                              and		cpl."NCERTIF" = cla."NCERTIF"),
                                              (   select  max(cpl."NCURRENCY")
                                                                              from    usvtimg01."CURREN_POL" cpl
                                                                              where 	cpl."SCERTYPE" = cla."SCERTYPE" and	cpl."NBRANCH" = cla."NBRANCH" 
                                                                              and		cpl."NPRODUCT" = pol."NPRODUCT" and cpl."NPOLICY" = cla."NPOLICY"),0) moneda_cod,
                                                              coalesce(	case	when	cla."NBRANCH" = 21 
                                                                                                      then	(	select	case	when	max(gen."SADDSUINI") = '3'
                                                                                                                                                              then	max(case when gen."SADDSUINI" = '3' then coalesce(cov."NCAPITAL",0) else 0 end)
                                                                                                                                                              else	sum(case when gen."SADDSUINI" = '1' then coalesce(cov."NCAPITAL",0) else 0 end) end
                                                                                                                              from    usvtimg01."COVER" cov
                                                                                                                              join	usvtimg01."LIFE_COVER" gen
                                                                                                                                              on		gen."NBRANCH" = cov."NBRANCH"
                                                                                                                                              and     gen."NPRODUCT" = cov."NPRODUCT"
                                                                                                                                              and     gen."NMODULEC" = cov."NMODULEC"
                                                                                                                                              and     gen."NCOVER" = cov."NCOVER"
                                                                                                                                              and     gen."DEFFECDATE" <= cast(cla."DOCCURDAT" as date)
                                                                                                                                              and     (gen."DNULLDATE" is null or gen."DNULLDATE" > cast(cla."DOCCURDAT" as date))
                                                                                                                                              and     gen."SSTATREGT" <> '4'
                                                                                                                                              and     gen."SADDSUINI" in ('1','3') --NO HAY '3' EN VTIME
                                                                                                                              where   cov."SCERTYPE"  = pol."SCERTYPE" 
                                                                                                                              and     cov."NBRANCH" = cla."NBRANCH"
                                                                                                                              and     cov."NPRODUCT" = cla."NPRODUCT"
                                                                                                                              and     cov."NPOLICY" = cla."NPOLICY"
                                                                                                                              and     cov."NCERTIF" = cla."NCERTIF"
                                                                                                                              and     cast(cov."DEFFECDATE" as date) <= cast(cla."DOCCURDAT" as date)
                                                                                                                              and     (cov."DNULLDATE" is null or cast(cov."DNULLDATE" as date) > cast(cla."DOCCURDAT" as date)))
                                                                                                      else 	(	select	case	when	max(gen."SADDSUINI") = '3'
                                                                                                                                                              then	max(case when gen."SADDSUINI" = '3' then coalesce(cov."NCAPITAL",0) else 0 end)
                                                                                                                                                              else	sum(case when gen."SADDSUINI" = '1' then coalesce(cov."NCAPITAL",0) else 0 end)
                                                                                                                                                              end
                                                                                                                              from    usvtimg01."COVER" cov
                                                                                                                              join	usvtimg01."GEN_COVER" gen
                                                                                                                                              on		gen."NBRANCH" = cov."NBRANCH"
                                                                                                                                              and     gen."NPRODUCT" = cov."NPRODUCT"
                                                                                                                                              and     gen."NMODULEC" = cov."NMODULEC"
                                                                                                                                              and     gen."NCOVER" = cov."NCOVER"
                                                                                                                                              and     cast(gen."DEFFECDATE" as date) <= cast(cla."DOCCURDAT" as date)
                                                                                                                                              and     (gen."DNULLDATE" is null or cast(gen."DNULLDATE" as date) > cast(cla."DOCCURDAT" as date))
                                                                                                                                              and     gen."SSTATREGT" <> '4'
                                                                                                                                              and     gen."SADDSUINI" in ('1','3')
                                                                                                                              where   cov."SCERTYPE"  = pol."SCERTYPE" 
                                                                                                                              and     cov."NBRANCH" = cla."NBRANCH"
                                                                                                                              and     cov."NPRODUCT" = cla."NPRODUCT"
                                                                                                                              and     cov."NPOLICY" = cla."NPOLICY"
                                                                                                                              and     cov."NCERTIF" = cla."NCERTIF"
                                                                                                                              and     cast(cov."DEFFECDATE" as date) <= cast(cla."DOCCURDAT" as date)
                                                                                                                              and     (cov."DNULLDATE" is null or cast(cov."DNULLDATE" as date) > cast(cla."DOCCURDAT" as date))) --NO HAY '3' EN VTIME
                                                                                                      end, 0) ncapital,
                                                              case	when	pol."SBUSSITYP" = '2'
                                                                              then 	cast(cla."DOCCURDAT" as date)
                                                                              else	(	select 	max(greatest(
                                                                                                                              case when cast(cla."DOCCURDAT" as date) <= dat.par_fin then cast(cla."DOCCURDAT" as date) else '0001-01-01'::date end,
                                                                                                                              case when cast(coi."DEFFECDATE" as date) <= dat.par_fin then cast(coi."DEFFECDATE" as date) else '0001-01-01'::date end,
                                                                                                                              case when cast(coi."DCOMPDATE" as date) <= dat.par_fin then cast(coi."DCOMPDATE" as date) else '0001-01-01'::date end))
                                                                                                      from	usvtimg01."COINSURAN" coi
                                                                                                      where	coi."SCERTYPE" = cla."SCERTYPE"
                                                                                                      and     coi."NBRANCH" = cla."NBRANCH"
                                                                                                      and     coi."NPRODUCT" = cla."NPRODUCT"
                                                                                                      and     coi."NPOLICY" = cla."NPOLICY"
                                                                                                      and 	coi."NCOMPANY" is not null
                                                                                                      and     cast(coi."DEFFECDATE" as date) <= cast("DOCCURDAT" as date)
                                                                                                      and     (coi."DNULLDATE" is null or cast(coi."DNULLDATE" as date) > cast("DOCCURDAT" as date)))
                                                                              end xxxxdate
                                              from    (	select	ctid pol_id
                                                                      from	usvtimg01."POLICY"
                                                                      where	"SCERTYPE" = '2') po0
                                              join	(	select	cast('{p_fecha_inicio}' as date) par_ini, --rango inicial carga incremental (variable a elecci�n)
                                                                        cast('{p_fecha_fin}' as date) par_fin --rango final carga incremental (variable a elecci�n)
                                                              ) dat on 1 = 1
                                              join	usvtimg01."POLICY" pol on pol.ctid = po0.pol_id
                                              join	usvtimg01."CLAIM" cla
                                                              on		cla."SCERTYPE" = pol."SCERTYPE"
                                                      and     cla."NPOLICY" = pol."NPOLICY"
                                                      and     cla."NBRANCH" = pol."NBRANCH"
                                                      and     cla."SSTACLAIM" <> '6'
                                                      and		(cast(cla."DCOMPDATE" as date) between dat.par_ini and dat.par_fin
                                                                              or	exists
                                                                      (   select  1
                                                                      from    usvtimg01."CLAIM_HIS" clh
                                                                      where	clh."NCLAIM" = cla."NCLAIM"
                                                                      and		coalesce(clh."NCASE_NUM",0) >= 0
                                                                                              and     (cast(clh."DOPERDATE" as date) between dat.par_ini and dat.par_fin --se consideran siniestros con operaciones entre los rangos (limitaci�n carga inicial)
                                                                                                              or cast(clh."DCOMPDATE" as date) between dat.par_ini and dat.par_fin) --se consideran siniestros con modificaciones entre los rangos (limitaci�n carga inicial)
                                                                                              and		clh."NOPER_TYPE" in
                                                                                                              (select	cast("SVALUE" as INT4) from	usvtimg01."CONDITION_SERV" cs  where "NCONDITION" in (71,72,73)))
                                                                              or	exists
                                                                      (   select  1
                                                                      from    usvtimg01."CLAIM_HIS" clh
                                                                                      join	usvtimg01."CL_M_COVER" clm
                                                                                      on		clm."NCLAIM" = clh."NCLAIM"
                                                                                      and		clm."NCASE_NUM" = clH."NCASE_NUM"
                                                                                      and		clm."NDEMAN_TYPE" = clh."NDEMAN_TYPE"
                                                                                      and		clm."NTRANSAC" = clh."NTRANSAC"
                                                                                      and		cast(clm."DCOMPDATE" as date) between dat.par_ini and dat.par_fin --se consideran coberturas con modificaciones entre los rangos (limitaci�n carga inicial)
                                                                      where	clh."NCLAIM" = cla."NCLAIM"
                                                                      and		coalesce(clh."NCASE_NUM",0) >= 0
                                                                                              and		clh."NOPER_TYPE" in 
                                                                                                              (select	cast("SVALUE" as INT4) from	usvtimg01."CONDITION_SERV" cs  where "NCONDITION" in (71,72,73))))
                                                              and		not exists
                                                              (   select  1
                                                              from    usvtimg01."CLAIM_HIS" clh
                                                                                      join	(	select	cast("SVALUE" as INT4) "SVALUE"
                                                                                                              from	usvtimg01."CONDITION_SERV" cs 
                                                                                                              where	"NCONDITION" in (71,72,73)) csv --reservas, ajustes o pagos
                                                                                                      on	clh."NOPER_TYPE" = csv."SVALUE"
                                                              where	coalesce(clh."NCLAIM",0) = cla."NCLAIM"
                                                                                      and     (cast(clh."DOPERDATE" as date) > dat.par_fin --no existan operaciones posteriores al corte
                                                                                                      or cast(clh."DOPERDATE" as date) > dat.par_fin) --no existan modificaciones posteriores al corte
                                                                                      and		coalesce(clh."NAMOUNT",0) <> 0)
                                                              and		not exists
                                                              (   select  1
                                                              from    usvtimg01."CLAIM_HIS" clh
                                                                              join	usvtimg01."CL_M_COVER" clm
                                                                              on		clm."NCLAIM" = clh."NCLAIM"
                                                                              and		clm."NCASE_NUM" = clH."NCASE_NUM"
                                                                              and		clm."NDEMAN_TYPE" = clh."NDEMAN_TYPE"
                                                                              and		clm."NTRANSAC" = clh."NTRANSAC"
                                                                              and		cast(clm."DCOMPDATE" as date) > dat.par_fin --se consideran coberturas con modificaciones entre los rangos (limitaci�n carga inicial)
                                                              where	clh."NCLAIM" = cla."NCLAIM"
                                                              and		coalesce(clh."NCASE_NUM",0) >= 0
                                                                                      and		clh."NOPER_TYPE" in 
                                                                                                      (select	cast("SVALUE" as INT4) from	usvtimg01."CONDITION_SERV" cs  where "NCONDITION" in (71,72,73)))
                                                              and		(	(pol."SBUSSITYP" = '1' and exists
                                                                                      (	select	1
                                                                                              from	usvtimg01."COINSURAN" coi
                                                                                              where	coi."SCERTYPE" = cla."SCERTYPE"
                                                                                              and     coi."NBRANCH" = cla."NBRANCH"
                                                                                              and     coi."NPRODUCT" = cla."NPRODUCT"
                                                                                              and     coi."NPOLICY" = cla."NPOLICY"
                                                                                              and 	coi."NCOMPANY" is not null
                                                                                              and     cast(coi."DEFFECDATE" as date) <= cast("DOCCURDAT" as date)
                                                                                              and     (coi."DNULLDATE" is null or cast(coi."DNULLDATE" as date) > cast("DOCCURDAT" as date))))
                                                                                      or pol."SBUSSITYP" = '2')) cl0
                      left	join usvtimg01."COINSURAN" coi
                                      on		coi."SCERTYPE" = cl0.scertype
                                      and     coi."NBRANCH" = cl0.nbranch
                                      and     coi."NPRODUCT" = cl0.nproduct
                                      and     coi."NPOLICY" = cl0.npolicy
                                      and 	coi."NCOMPANY" is not null
                                      and     cast(coi."DEFFECDATE" as date) <= cl0.doccurdat
                                      and     (coi."DNULLDATE" is null or cast(coi."DNULLDATE" as date) > cl0.doccurdat)
                     ) AS TMP
                  '''

    DF_VTIME_LPG = glue_context.read.format('jdbc').options(**connection).option("fetchsize",10000).option("dbtable",VTIME_LPG).load()
  
    VTIME_LPV = f'''
                    (select	
                     'D' INDDETREC,
                     'SBCOSSEG' TABLAIFRS17,
                     '' PK,
		     '' DTPREG, --excluido
		     '' TIOCPROC, --excluido
		     cast(cl0.xxxxdate as varchar) TIOCFRM,
		     '' TIOCTO, --excluido
		     'PVV' KGIORIGM,
		     'LPV' DCOMPA,
		     '' DMARCA, --excluido
		     cast(cl0.nclaim as varchar) KSBSIN,
		     cast(case when cl0.sbussityp = '2' then 1 else coalesce(coi."NCOMPANY",0) end as varchar) DCODCSG,
		     '' DNUMSEQ, --excluido
		     '' TDPLANO, --excluido
		     cast(cl0.ncapital as numeric(14,2)) VMTCAPIT,
		     cast(case when cl0.sbussityp = '2' then 100 else coalesce(coi."NSHARE",0) end as numeric (9,6)) VTXQUOTA,
		     '' VMTINDEM, --NOAPP
		     case when cl0.sbussityp = '2' then cl0.sleadpoli else cast(cl0.npolicy as varchar) end DNUMAPO_CSG,
		     cast(cl0.moneda_cod as varchar) KSCMOEDA
                     from	(	select  cla."NBRANCH" nbranch,
                                     cla."NPOLICY" npolicy,
                                     cla."NCERTIF" ncertif,
                                                             cla."NCLAIM" nclaim,
                                     cast(cla."DOCCURDAT" as date) doccurdat,
                                     pol."SCERTYPE" scertype,
                                     pol."NPRODUCT" nproduct,
                                     pol."SLEADPOLI" sleadpoli,
                                                             pol."SBUSSITYP" sbussityp,
                                                             coalesce(
                                                                     (	select  max(cpl."NCURRENCY")
                                                                             from    usvtimv01."CURREN_POL" cpl
                                                                             where 	cpl."SCERTYPE" = cla."SCERTYPE" and	cpl."NBRANCH" = cla."NBRANCH" 
                                                                             and		cpl."NPRODUCT" = pol."NPRODUCT" and cpl."NPOLICY" = cla."NPOLICY"
                                                                             and		cpl."NCERTIF" = cla."NCERTIF"),
                                             (   select  max(cpl."NCURRENCY")
                                                                             from    usvtimv01."CURREN_POL" cpl
                                                                             where 	cpl."SCERTYPE" = cla."SCERTYPE" and	cpl."NBRANCH" = cla."NBRANCH" 
                                                                             and		cpl."NPRODUCT" = pol."NPRODUCT" and cpl."NPOLICY" = cla."NPOLICY"),0) moneda_cod,
                                                             coalesce((	select	case	when	max(gen."SADDSUINI") = '3'
                                                                                                                     then	max(case when gen."SADDSUINI" = '3' then coalesce(cov."NCAPITAL",0) else 0 end)
                                                                                                                     else	sum(case when gen."SADDSUINI" = '1' then coalesce(cov."NCAPITAL",0) else 0 end) end
                                                                                     from    usvtimv01."COVER" cov
                                                                                     join	usvtimv01."LIFE_COVER" gen
                                                                                                     on		gen."NCOVER" = cov."NCOVER"
                                                                                                     and     gen."NPRODUCT" = cov."NPRODUCT"
                                                                                                     and     gen."NMODULEC" = cov."NMODULEC"
                                                                                                     and     gen."NBRANCH" = cov."NBRANCH"
                                                                                                     and     gen."DEFFECDATE" <= cast(cla."DOCCURDAT" as date)
                                                                                                     and     (gen."DNULLDATE" is null or gen."DNULLDATE" > cast(cla."DOCCURDAT" as date))
                                                                                                     and     gen."SSTATREGT" <> '4'
                                                                                     where   cov."SCERTYPE" = pol."SCERTYPE" 
                                                                                     and     cov."NBRANCH" = cla."NBRANCH"
                                                                                     and     cov."NPRODUCT" = cla."NPRODUCT"
                                                                                     and     cov."NPOLICY" = cla."NPOLICY"
                                                                                     and     cov."NCERTIF" = cla."NCERTIF"
                                                                                     and     cast(cov."DEFFECDATE" as date) <= cast(cla."DOCCURDAT" as date)
                                                                                     and     (cov."DNULLDATE" is null or cast(cov."DNULLDATE" as date) > cast(cla."DOCCURDAT" as date))), 0) ncapital,
                                                             case	when	pol."SBUSSITYP" = '2'
                                                                             then 	cast(cla."DOCCURDAT" as date)
                                                                             else	(	select 	max(greatest(
                                                                                                                             case when cast(cla."DOCCURDAT" as date) <= dat.par_fin then cast(cla."DOCCURDAT" as date) else '0001-01-01'::date end,
                                                                                                                             case when cast(coi."DEFFECDATE" as date) <= dat.par_fin then cast(coi."DEFFECDATE" as date) else '0001-01-01'::date end,
                                                                                                                             case when cast(coi."DCOMPDATE" as date) <= dat.par_fin then cast(coi."DCOMPDATE" as date) else '0001-01-01'::date end))
                                                                                                     from	usvtimv01."COINSURAN" coi
                                                                                                     where	coi."SCERTYPE" = cla."SCERTYPE"
                                                                                                     and     coi."NBRANCH" = cla."NBRANCH"
                                                                                                     and     coi."NPRODUCT" = cla."NPRODUCT"
                                                                                                     and     coi."NPOLICY" = cla."NPOLICY"
                                                                                                     and 	coi."NCOMPANY" is not null
                                                                                                     and     cast(coi."DEFFECDATE" as date) <= cast("DOCCURDAT" as date)
                                                                                                     and     (coi."DNULLDATE" is null or cast(coi."DNULLDATE" as date) > cast("DOCCURDAT" as date)))
                                                                             end xxxxdate
                                             from    (	select	ctid pol_id
                                                                     from	usvtimv01."POLICY"
                                                                     where	"SCERTYPE" = '2') po0
                                             join	(	select	cast('{p_fecha_inicio}' as date) par_ini, --rango inicial carga incremental (variable a elecci�n)
                                                                        cast('{p_fecha_fin}' as date) par_fin --rango final carga incremental (variable a elecci�n)
                                                             ) dat on 1 = 1
                                             join	usvtimv01."POLICY" pol on pol.ctid = po0.pol_id
                                             join	usvtimv01."CLAIM" cla
                                                             on		cla."SCERTYPE" = pol."SCERTYPE"
                                                     and     cla."NPOLICY" = pol."NPOLICY"
                                                     and     cla."NBRANCH" = pol."NBRANCH"
                                                     and     cla."SSTACLAIM" <> '6'
                                                     and		(cast(cla."DCOMPDATE" as date) between dat.par_ini and dat.par_fin
                                                                             or	exists
                                                                     (   select  1
                                                                     from    usvtimv01."CLAIM_HIS" clh
                                                                     where	clh."NCLAIM" = cla."NCLAIM"
                                                                     and		coalesce(clh."NCASE_NUM",0) >= 0
                                                                                             and     (cast(clh."DOPERDATE" as date) between dat.par_ini and dat.par_fin --se consideran siniestros con operaciones entre los rangos (limitaci�n carga inicial)
                                                                                                             or cast(clh."DCOMPDATE" as date) between dat.par_ini and dat.par_fin) --se consideran siniestros con modificaciones entre los rangos (limitaci�n carga inicial)
                                                                                             and		clh."NOPER_TYPE" in
                                                                                                             (select	cast("SVALUE" as INT4) from	usvtimv01."CONDITION_SERV" cs  where "NCONDITION" in (71,72,73)))
                                                                             or	exists
                                                                     (   select  1
                                                                     from    usvtimv01."CLAIM_HIS" clh
                                                                                     join	usvtimv01."CL_M_COVER" clm
                                                                                     on		clm."NCLAIM" = clh."NCLAIM"
                                                                                     and		clm."NCASE_NUM" = clH."NCASE_NUM"
                                                                                     and		clm."NDEMAN_TYPE" = clh."NDEMAN_TYPE"
                                                                                     and		clm."NTRANSAC" = clh."NTRANSAC"
                                                                                     and		cast(clm."DCOMPDATE" as date) between dat.par_ini and dat.par_fin --se consideran coberturas con modificaciones entre los rangos (limitaci�n carga inicial)
                                                                     where	clh."NCLAIM" = cla."NCLAIM"
                                                                     and		coalesce(clh."NCASE_NUM",0) >= 0
                                                                                             and		clh."NOPER_TYPE" in 
                                                                                                             (select	cast("SVALUE" as INT4) from	usvtimv01."CONDITION_SERV" cs  where "NCONDITION" in (71,72,73))))
                                                             and		not exists
                                                             (   select  1
                                                             from    usvtimv01."CLAIM_HIS" clh
                                                                                     join	(	select	cast("SVALUE" as INT4) "SVALUE"
                                                                                                             from	usvtimv01."CONDITION_SERV" cs 
                                                                                                             where	"NCONDITION" in (71,72,73)) csv --reservas, ajustes o pagos
                                                                                                     on	clh."NOPER_TYPE" = csv."SVALUE"
                                                             where	coalesce(clh."NCLAIM",0) = cla."NCLAIM"
                                                                                     and     (cast(clh."DOPERDATE" as date) > dat.par_fin --no existan operaciones posteriores al corte
                                                                                                     or cast(clh."DOPERDATE" as date) > dat.par_fin) --no existan modificaciones posteriores al corte
                                                                                     and		coalesce(clh."NAMOUNT",0) <> 0)
                                                             and		not exists
                                                             (   select  1
                                                             from    usvtimv01."CLAIM_HIS" clh
                                                                             join	usvtimv01."CL_M_COVER" clm
                                                                             on		clm."NCLAIM" = clh."NCLAIM"
                                                                             and		clm."NCASE_NUM" = clH."NCASE_NUM"
                                                                             and		clm."NDEMAN_TYPE" = clh."NDEMAN_TYPE"
                                                                             and		clm."NTRANSAC" = clh."NTRANSAC"
                                                                             and		cast(clm."DCOMPDATE" as date) > dat.par_fin --se consideran coberturas con modificaciones entre los rangos (limitaci�n carga inicial)
                                                             where	clh."NCLAIM" = cla."NCLAIM"
                                                             and		coalesce(clh."NCASE_NUM",0) >= 0
                                                                                     and		clh."NOPER_TYPE" in 
                                                                                                     (select	cast("SVALUE" as INT4) from	usvtimv01."CONDITION_SERV" cs  where "NCONDITION" in (71,72,73)))
                                                             and		(	(pol."SBUSSITYP" = '1' and exists
                                                                                     (	select	1
                                                                                             from	usvtimv01."COINSURAN" coi
                                                                                             where	coi."SCERTYPE" = cla."SCERTYPE"
                                                                                             and     coi."NBRANCH" = cla."NBRANCH"
                                                                                             and     coi."NPRODUCT" = cla."NPRODUCT"
                                                                                             and     coi."NPOLICY" = cla."NPOLICY"
                                                                                             and 	coi."NCOMPANY" is not null
                                                                                             and     cast(coi."DEFFECDATE" as date) <= cast("DOCCURDAT" as date)
                                                                                             and     (coi."DNULLDATE" is null or cast(coi."DNULLDATE" as date) > cast("DOCCURDAT" as date))))
                                                                                     or pol."SBUSSITYP" = '2')) cl0
                     left	join usvtimv01."COINSURAN" coi
                                     on		coi."SCERTYPE" = cl0.scertype
                                     and     coi."NBRANCH" = cl0.nbranch
                                     and     coi."NPRODUCT" = cl0.nproduct
                                     and     coi."NPOLICY" = cl0.npolicy
                                     and 	coi."NCOMPANY" is not null
                                     and     cast(coi."DEFFECDATE" as date) <= cl0.doccurdat
                                     and     (coi."DNULLDATE" is null or cast(coi."DNULLDATE" as date) > cl0.doccurdat)
                    ) AS TMP
                 '''

    DF_VTIME_LPV = glue_context.read.format('jdbc').options(**connection).option("fetchsize",10000).option("dbtable",VTIME_LPV).load()
                           
    L_DF_SBCOSSEG =  DF_INSUNIX_LPG.union(DF_INSUNIX_LPV).union(DF_VTIME_LPG).union(DF_VTIME_LPV)

    return L_DF_SBCOSSEG