def get_data(glue_context, connection, p_fecha_inicio, p_fecha_fin):

    INSUNIX_LPG = f'''
                      (select	
                       'D' INDDETREC,
		       'SBHISPR' TABLAIFRS17,
		       '' PK,
		       '' DTPREG, --excluido
		       '' TIOCPROC,
		       cast(cl0.xxxxdate as varchar) TIOCFRM,
		       '' TIOCTO, --excluido
		       'PIG' KGIORIGM, --excluido
		       cast(cla.claim as varchar) KSBSIN,
		       cast(cla.claim as varchar) DNUMSIN,
		       coalesce(cast(cla.policy as varchar),'') DNUMAPO,
		       coalesce(cast(cla.certif as varchar),'') DNMCERT,
		       coalesce(cl0.cover,'') KGCTPCBT,
		       coalesce(cast(clh.operdate as varchar),'') TPROVI,
		       coalesce(cast(clh.transac as varchar),'') DSEQMOV,
		       coalesce(cast(clh.oper_type as varchar),'') KSCTPMPR,
		       coalesce(cl0.reserva,0) VMTPROVI,
		       coalesce(cl0.ajustes,0) VMTPRVAR,
		       '' TULTALT, --excluido
		       '' DUSRUPD, --excluido
		       'LPG' DCOMPA,
		       '' DMARCA, --excluido
		       '' KSBSUBSN, --excluido
		       '' KSCMTDPR, --excluido
		       coalesce((	select	evi.scod_vt
		       			from	usinsug01.equi_vt_inx evi 
		       			where	evi.scod_inx = (select bene_code from usinsug01.claimbenef where ctid = cl0.clb_id)),
		       	 (select bene_code from usinsug01.claimbenef where ctid = cl0.clb_id)) KEBENTID_SN,
		       '' DENTIDSO, --excluido
		       '' DIDADELES, --excluido
		       '' TCRIACAO --excluido
                       from	(	select	cla.cla_id,
                                                               clh.ctid clh_id,
                                                               coalesce((	select  distinct(gco.covergen || '-' || gco.currency) --se registr� error para AMF con m�s de un registro a nivel de cobertura en tabla cover
                                                                                       from    usinsug01.cover cov
                                                                                       join	usinsug01.gen_cover gco on gco.ctid =
                                                       coalesce(
                                                                                               (   select  max(ctid)
                                                               from	usinsug01.gen_cover
                                                               where	usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = cla.pol_product and coalesce(sub_product,0) = cla.sub_product and currency = cov.currency --con subproducto
                                                               and     modulec = cov.modulec and cover = cov.cover --con m�dulo
                                                               and     effecdate <= cla.occurdat and (nulldate is null or nulldate > cla.occurdat)
                                                               and     statregt <> '4'),
                                                       (   select  max(ctid)
                                                               from	usinsug01.gen_cover
                                                               where	usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = cla.pol_product and coalesce(sub_product,0) = cla.sub_product and currency = cov.currency --con subproducto
                                                               and     coalesce(modulec,0) = 0 and cover = cov.cover --sin m�dulo
                                                               and     effecdate <= cla.occurdat and (nulldate is null or nulldate > cla.occurdat)
                                                               and     statregt <> '4'),
                                                                                               (   select  max(ctid)
                                                               from	usinsug01.gen_cover
                                                               where	usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = cla.pol_product and coalesce(sub_product,0) = 0 and currency = cov.currency --sin subproducto
                                                               and     modulec = cov.modulec and cover = cov.cover --con m�dulo
                                                               and     effecdate <= cla.occurdat and (nulldate is null or nulldate > cla.occurdat) --variaci�n 2 vigencias
                                                               and     statregt <> '4'), --variaci�n 3 reg. v�lido
                                                       (   select  max(ctid)
                                                               from	usinsug01.gen_cover
                                                               where	usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = cla.pol_product and coalesce(sub_product,0) = 0 and currency = cov.currency --sin subproducto
                                                               and     coalesce(modulec,0) = 0 and cover = cov.cover --sin m�dulo
                                                               and     effecdate <= cla.occurdat and (nulldate is null or nulldate > cla.occurdat) --variaci�n 2 vigencias
                                                               and     statregt <> '4'), --variaci�n 3 reg. v�lido
                                                                                               (	select  max(ctid)
                                                               from	usinsug01.gen_cover
                                                               where	usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = cla.pol_product and coalesce(sub_product,0) = cla.sub_product and currency = cov.currency --con subproducto
                                                               and     modulec = cov.modulec and cover = cov.cover --con m�dulo
                                                               and     effecdate <= cla.occurdat and (nulldate is null or nulldate > cla.occurdat)
                                                               and     statregt = '4'),
                                                       (	select  max(ctid)
                                                               from	usinsug01.gen_cover
                                                               where	usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = cla.pol_product and coalesce(sub_product,0) = cla.sub_product and currency = cov.currency --con subproducto
                                                               and     coalesce(modulec,0) = 0 and cover = cov.cover --sin m�dulo
                                                               and     effecdate <= cla.occurdat and (nulldate is null or nulldate > cla.occurdat)
                                                               and     statregt = '4'),
                                                                                               (	select  max(ctid)
                                                               from	usinsug01.gen_cover
                                                               where	usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = cla.pol_product and coalesce(sub_product,0) = 0 and currency = cov.currency --sin subproducto
                                                               and     modulec = cov.modulec and cover = cov.cover --con m�dulo
                                                               and     effecdate <= cla.occurdat and (nulldate is null or nulldate > cla.occurdat)
                                                               and     statregt = '4'),
                                                       (	select  max(ctid)
                                                               from	usinsug01.gen_cover
                                                               where	usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = cla.pol_product and coalesce(sub_product,0) = 0 and currency = cov.currency --sin subproducto
                                                               and     coalesce(modulec,0) = 0 and cover = cov.cover --sin m�dulo
                                                               and     effecdate <= cla.occurdat and (nulldate is null or nulldate > cla.occurdat)
                                                               and     statregt = '4'), --no est� cortado
                                                                                               (	select  max(ctid)
                                                               from	usinsug01.gen_cover
                                                               where	usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = cla.pol_product and coalesce(sub_product,0) = cla.sub_product and currency = cov.currency --con subproducto
                                                               and     modulec = cov.modulec and cover = cov.cover --con m�dulo
                                                               and     effecdate <= cla.occurdat and nulldate <= cla.occurdat
                                                               and     statregt <> '4'),
                                                       (   select  max(ctid)
                                                               from	usinsug01.gen_cover
                                                               where	usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = cla.pol_product and coalesce(sub_product,0) = cla.sub_product and currency = cov.currency --con subproducto
                                                               and     coalesce(modulec,0) = 0 and cover = cov.cover --sin m�dulo
                                                               and     effecdate <= cla.occurdat and nulldate <= cla.occurdat
                                                               and     statregt <> '4'),
                                                                                               (	select  max(ctid)
                                                               from	usinsug01.gen_cover
                                                               where	usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = cla.pol_product and coalesce(sub_product,0) = 0 and currency = cov.currency --sin subproducto
                                                               and     modulec = cov.modulec and cover = cov.cover --con m�dulo
                                                               and     effecdate <= cla.occurdat and nulldate <= cla.occurdat
                                                               and     statregt <> '4'),
                                                       (   select  max(ctid)
                                                               from	usinsug01.gen_cover
                                                               where	usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = cla.pol_product and coalesce(sub_product,0) = 0 and currency = cov.currency --sin subproducto
                                                               and     coalesce(modulec,0) = 0 and cover = cov.cover --sin m�dulo
                                                               and     effecdate <= cla.occurdat and nulldate <= cla.occurdat
                                                               and     statregt <> '4'), -- no est� cortado, pero fue anulado antes del efecto del registro
                                                                                               (	select  max(ctid)
                                                               from	usinsug01.gen_cover
                                                               where	usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = cla.pol_product and coalesce(sub_product,0) = cla.sub_product and currency = cov.currency --con subproducto
                                                               and     modulec = cov.modulec and cover = cov.cover --con m�dulo
                                                               and     effecdate <= cla.occurdat and nulldate <= cla.occurdat
                                                               and     statregt = '4'),
                                                       (   select  max(ctid)
                                                               from	usinsug01.gen_cover
                                                               where 	usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = cla.pol_product and coalesce(sub_product,0) = cla.sub_product and currency = cov.currency --con subproducto
                                                               and     coalesce(modulec,0) = 0 and cover = cov.cover --sin m�dulo
                                                               and     effecdate <= cla.occurdat and nulldate <= cla.occurdat
                                                               and     statregt = '4'),
                                                                                               (	select  max(ctid)
                                                               from	usinsug01.gen_cover
                                                               where	usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = cla.pol_product and coalesce(sub_product,0) = 0 and currency = cov.currency --sin subproducto
                                                               and     modulec = cov.modulec and cover = cov.cover --con m�dulo
                                                               and     effecdate <= cla.occurdat and nulldate <= cla.occurdat
                                                               and     statregt = '4'),
                                                       (   select  max(ctid)
                                                               from	usinsug01.gen_cover
                                                               where 	usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = cla.pol_product and coalesce(sub_product,0) = 0 and currency = cov.currency --sin subproducto
                                                               and     coalesce(modulec,0) = 0 and cover = cov.cover --con m�dulo
                                                               and     effecdate <= cla.occurdat and nulldate <= cla.occurdat
                                                               and     statregt = '4'), --no est� cortado y aparte fue anulado antes del efecto del registro
                                                                                               (	select  max(ctid)
                                                               from	usinsug01.gen_cover
                                                               where 	usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = cla.pol_product and coalesce(sub_product,0) = cla.sub_product and currency = cov.currency --con subproducto
                                                               and     modulec = cov.modulec and cover = cov.cover --con m�dulo
                                                               and     effecdate > cla.occurdat
                                                               and     statregt <> '4'),
                                                       (   select  max(ctid)
                                                               from	usinsug01.gen_cover
                                                               where	usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = cla.pol_product and coalesce(sub_product,0) = cla.sub_product and currency = cov.currency --con subproducto
                                                               and     coalesce(modulec,0) = 0 and cover = cov.cover --sin m�dulo
                                                               and     effecdate <= cla.occurdat and nulldate <= cla.occurdat
                                                               and     statregt <> '4'), -- no est� cortado, pero tampoco al efecto de la tabla de datos particular
                                                                       (	select  max(ctid)
                                                               from	usinsug01.gen_cover
                                                               where 	usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = cla.pol_product and coalesce(sub_product,0) = 0 and currency = cov.currency --sin subproducto
                                                               and     modulec = cov.modulec and cover = cov.cover --con m�dulo
                                                               and     effecdate > cla.occurdat
                                                               and     statregt <> '4'),
                                                       (   select  max(ctid)
                                                               from	usinsug01.gen_cover
                                                               where	usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = cla.pol_product and coalesce(sub_product,0) = 0 and currency = cov.currency --sin subproducto
                                                               and     coalesce(modulec,0) = 0 and cover = cov.cover --sin m�dulo
                                                               and     effecdate <= cla.occurdat and nulldate <= cla.occurdat
                                                               and     statregt <> '4'), -- no est� cortado, pero tampoco al efecto de la tabla de datos particular
                                                                                               (	select  max(ctid)
                                                               from	usinsug01.gen_cover
                                                               where	usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = cla.pol_product and coalesce(sub_product,0) = cla.sub_product and currency = cov.currency --con sub_producto
                                                               and     modulec = cov.modulec and cover = cov.cover --con m�dulo
                                                               and     effecdate <= cla.occurdat and nulldate <= cla.occurdat
                                                               and     statregt = '4'),
                                                       (   select  max(ctid)
                                                               from	usinsug01.gen_cover
                                                               where	usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = cla.pol_product and coalesce(sub_product,0) = cla.sub_product and currency = cov.currency --con sub_producto
                                                               and     coalesce(modulec,0) = 0 and cover = cov.cover --sin m�dulo
                                                               and     effecdate > cla.occurdat
                                                               and     statregt = '4'),
                                                                                               (	select  max(ctid)
                                                               from	usinsug01.gen_cover
                                                               where	usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = cla.pol_product and coalesce(sub_product,0) = 0 and currency = cov.currency --sin sub_producto
                                                               and     modulec = cov.modulec and cover = cov.cover --con m�dulo
                                                               and     effecdate <= cla.occurdat and nulldate <= cla.occurdat
                                                               and     statregt = '4'),
                                                       (   select  max(ctid)
                                                               from	usinsug01.gen_cover
                                                               where	usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = cla.pol_product and coalesce(sub_product,0) = 0 and currency = cov.currency --sin sub_producto
                                                               and     coalesce(modulec,0) = 0 and cover = cov.cover --sin m�dulo
                                                               and     effecdate > cla.occurdat
                                                               and     statregt = '4')) --est� cortado y no al efecto de la tabla de datos particular adem�s de estar cortado
                                                                                       where   cov.usercomp = cla.usercomp
                                                                                       and     cov.company = cla.company
                                                                                       and     cov.certype = cla.pol_certype
                                                                                       and     cov.branch = cla.branch
                                                                                       and     cov.policy = cla.policy
                                                                                       and     cov.certif = cla.certif
                                                                                       and     cov.effecdate <= cla.occurdat
                                                                                       and     (cov.nulldate is null or cov.nulldate > cla.occurdat)
                                                                                       and		cov.cover = clm.cover),
                                                                       cast(coalesce(cover,0) * -1 as varchar) || cla.moneda_cod) cover,
                                                               max(cla.clb_id) clb_id,
                                                               max(cla.xxxxdate) xxxxdate, --valor �nico, se evita hacer group by por este campo
                                                               sum(case	when	trim(clh.oper_type) in (select cast(operation as varchar(2)) from usinsug01.tab_cl_ope where reserve = 1)
                                                                                       then	case	when	clm.currency = cla.moneda_cod
                                                                                                                       then	coalesce(clm.amount,0)
                                                                                                                       else	coalesce(clm.amount,0) *
                                                                                                                                       case	when	clh.currency in (1,2)
                                                                                                                                                       then	(	select	case	when clh.currency = 2
                                                                                                                                                                                                               then max(clh0.exchange)
                                                                                                                                                                                                               else 1 / max(clh0.exchange) end
                                                                                                                                                                               from	usinsug01.claim_his clh0
                                                                                                                                                                               where	clh0.claim = clh.claim
                                                                                                                                                                               and		clh0.transac =
                                                                                                                                                                                               (	select	max(clh1.transac)
                                                                                                                                                                                                       from	usinsug01.claim_his clh1
                                                                                                                                                                                                       where 	clh1.claim = clh.claim
                                                                                                                                                                                                       and		clh1.transac <= clh.transac
                                                                                                                                                                                                       and		clh1.exchange not in (1,0)))
                                                                                                                                                       else	1 end end
                                                                                       else 	0 end) reserva,
                                                               sum(case	when	trim(clh.oper_type) in (select cast(operation as varchar(2)) from usinsug01.tab_cl_ope where ajustes = 1)
                                                                                       then	case	when	clm.currency = cla.moneda_cod
                                                                                                                       then	coalesce(clm.amount,0)
                                                                                                                       else	coalesce(clm.amount,0) *
                                                                                                                                       case	when	clh.currency in (1,2)
                                                                                                                                                       then	(	select	case	when clh.currency = 2
                                                                                                                                                                                                               then max(clh0.exchange)
                                                                                                                                                                                                               else 1 / max(clh0.exchange) end
                                                                                                                                                                               from	usinsug01.claim_his clh0
                                                                                                                                                                               where	clh0.claim = clh.claim
                                                                                                                                                                               and		clh0.transac =
                                                                                                                                                                                               (	select	max(clh1.transac)
                                                                                                                                                                                                       from	usinsug01.claim_his clh1
                                                                                                                                                                                                       where 	clh1.claim = clh.claim
                                                                                                                                                                                                       and		clh1.transac <= clh.transac
                                                                                                                                                                                                       and		clh1.exchange not in (1,0)))
                                                                                                                                                       else	1 end end
                                                                                       else 	0 end) ajustes
                                               from 	(	select	pol.ctid pol_id,
                                                                                       cla.ctid cla_id,
                                                                                       cla.usercomp,
                                                                                       cla.company,
                                                                                       pol.certype pol_certype,
                                                                                       cla.branch,
                                                                                       cla.policy,
                                                                                       cla.certif,
                                                                                       cla.occurdat,
                                                                                       pol.product pol_product,
                                                                                       cla.claim,
                                                                                       coalesce(
                                                                                               (	select	max(cpl.currency)
                                                                                                       from	usinsug01.curren_pol cpl
                                                                                                       where 	cpl.usercomp = cla.usercomp and cpl.company = cla.company and cpl.certype = pol.certype
                                                                                                       and		cpl.branch = cla.branch and cpl.policy = cla.policy and	cpl.certif = cla.certif),
                                                                                               (	select	max(cpl.currency)
                                                                                                       from	usinsug01.curren_pol cpl
                                                                                                       where 	cpl.usercomp = cla.usercomp and cpl.company = cla.company and cpl.certype = pol.certype
                                                                                                       and		cpl.branch = cla.branch and cpl.policy = cla.policy),0) moneda_cod,
                                                                                       coalesce(
                                                                                               (	select  min(ctid)
                                                                                                       from    usinsug01.claimbenef
                                                                                                       where   usercomp = cla.usercomp
                                                                                                       and     company = cla.company
                                                                                                       and     claim = cla.claim
                                                                                                       and     bene_type = 2),
                                                                                               (	select  min(ctid)
                                                                                                       from    usinsug01.claimbenef
                                                                                                       where   usercomp = cla.usercomp
                                                                                                       and     company = cla.company
                                                                                                       and     claim = cla.claim
                                                                                                       and     bene_type = 8),
                                                                                               (	select  min(ctid)
                                                                                                       from    usinsug01.claimbenef
                                                                                                       where   usercomp = cla.usercomp
                                                                                                       and     company = cla.company
                                                                                                       and     claim = cla.claim
                                                                                                       and     bene_type = 19),
                                                                                               (	select  min(ctid)
                                                                                                       from    usinsug01.claimbenef
                                                                                                       where   usercomp = cla.usercomp
                                                                                                       and     company = cla.company
                                                                                                       and     claim = cla.claim
                                                                                                       and     bene_type = 18)) clb_id,
                                                                                       coalesce((	select	sub_product
                                                                                                               from	usinsug01.pol_subproduct
                                                                                                               where	usercomp = cla.usercomp
                                                                                                               and 	company = cla.company
                                                                                                               and		certype = pol.certype
                                                                                                               and		branch = cla.branch
                                                                                                               and		policy = cla.policy
                                                                                                               and		product = pol.product),0) sub_product,
                                                                                       (	select 	max(greatest(
                                                                                                                       case when clm.compdate <= dat.par_fin then clm.compdate else '0001-01-01'::date end,
                                                                                                                       case when clh.compdate <= dat.par_fin then clh.compdate else '0001-01-01'::date end,
                                                                                                                       case when clh.operdate <= dat.par_fin then clh.operdate else '0001-01-01'::date end))
                                                                                               from	usinsug01.claim_his clh
                                                                                                               join	usinsug01.cl_m_cover clm
                                                                                                               on		clm.usercomp = clh.usercomp
                                                                                                               and		clm.company = clh.company
                                                                                                               and		clm.claim = clh.claim
                                                                                                               and		clm.movement = clh.transac
                                                                                               where	clh.claim = cla.claim
                                                                                               and		trim(clh.oper_type) in 
                                                                                                               (select cast(operation as varchar(2)) from usinsug01.tab_cl_ope where reserve = 1 or ajustes = 1)) xxxxdate,
                                                                                       dat.par_fin dat_lim
                                                                       from 	(	select	prv.cla_id,
                                                                                                               prv.par_fin
                                                                                               from 	(	--revisi�n fechas transacciones
                                                                                                                       select	cla.ctid cla_id,
                                                                                                                                       cla.claim,
                                                                                                                                       cla.compdate,
                                                                                                                                       dat.par_fin
                                                                                                                       from	(	select	cast('{p_fecha_inicio}' as date) par_ini,  --rango inicial carga hist�rica (variable a elecci�n)
                                                                                                                                                cast('{p_fecha_fin}' as date) par_fin) dat  --rango final carga hist�rica (variable a elecci�n)
                                                                                                                       join	usinsug01.claim cla
                                                                                                                                       on		cla.usercomp = 1
                                                                                                                                       and 	cla.company = 1
                                                                                                                                       and		cla.branch in (select branch from usinsug01.table10b where company = 1)
                                                                                                                                       and		(cla.staclaim <> '6' or (cla.staclaim = '6' and cla.branch = 23))
                                                                                                                                       and		cla.compdate <= dat.par_fin --no existe actualizaci�n posterior al rango (1� filtro)
                                                                                                                       join	usinsug01.claim_his clh --siniestros con transacciones/modificaciones en el rango (1� consideraci�n) 
                                                                                                                                       on		clh.claim = cla.claim
                                                                                                                                       and		trim(clh.oper_type) in 
                                                                                                                                                       (select cast(operation as varchar(2)) from usinsug01.tab_cl_ope where reserve = 1 or ajustes = 1)
                                                                                                                                       and     (clh.operdate between dat.par_ini and dat.par_fin
                                                                                                                                                       or clh.compdate between dat.par_ini and dat.par_fin)
                                                                                                                       union
                                                                                                                       --revisi�n fechas coberturas asociadas a transacciones
                                                                                                                       select	cla.ctid cla_id,
                                                                                                                                       cla.claim,
                                                                                                                                       cla.compdate,
                                                                                                                                       dat.par_fin
                                                                                                                       from	(	select	cast('{p_fecha_inicio}' as date) par_ini,  --rango inicial carga hist�rica (variable a elecci�n)
                                                                                                                                                cast('{p_fecha_fin}' as date) par_fin) dat  --rango final carga hist�rica (variable a elecci�n)
                                                                                                                       join	usinsug01.claim cla
                                                                                                                                       on		cla.usercomp = 1
                                                                                                                                       and 	cla.company = 1
                                                                                                                                       and		cla.branch in (select branch from usinsug01.table10b where company = 1)
                                                                                                                                       and		(cla.staclaim <> '6' or (cla.staclaim = '6' and cla.branch = 23))
                                                                                                                                       and		cla.compdate <= dat.par_fin --no existe actualizaci�n posterior al rango (1� filtro)
                                                                                                                       join	usinsug01.claim_his clh
                                                                                                                                       on		clh.claim = cla.claim
                                                                                                                                       and		trim(clh.oper_type) in 
                                                                                                                                                       (select cast(operation as varchar(2)) from usinsug01.tab_cl_ope where reserve = 1 or ajustes = 1)
                                                                                                                       join	usinsug01.cl_m_cover clm --siniestros con coberturas/modificaciones asociadas a transacciones en el rango (1� consideraci�n)
                                                                                                                                       on		clm.usercomp = clh.usercomp
                                                                                                                                       and		clm.company = clh.company
                                                                                                                                       and		clm.claim = clh.claim
                                                                                                                                       and		clm.movement = clh.transac
                                                                                                                                       and		clm.compdate between dat.par_ini and dat.par_fin) prv
                                                                                               --descarte de siniestros en el caso que posean operaciones/modificaciones de cualquier tipo posterior al rango (2� consideraci�n, la exclusi�n incluye los pagos)
                                                                                               where	(	select 	max(greatest(clm.compdate,clh.compdate,clh.operdate,prv.compdate))
                                                                                                                       from	usinsug01.claim_his clh
                                                                                                                       join	usinsug01.cl_m_cover clm
                                                                                                                                       on		clm.usercomp = clh.usercomp
                                                                                                                                       and		clm.company = clh.company
                                                                                                                                       and		clm.claim = clh.claim
                                                                                                                                       and		clm.movement = clh.transac
                                                                                                                       where	clh.claim = prv.claim
                                                                                                                       and		trim(clh.oper_type) in 
                                                                                                                                       (select cast(operation as varchar(2)) from usinsug01.tab_cl_ope where reserve = 1 or ajustes = 1 or pay_amount = 1)
                                                                                                               ) <= prv.par_fin) dat
                                                                       join	usinsug01.claim cla on cla.ctid = dat.cla_id
                                                                       join	usinsug01.policy pol
                                                                                       on		pol.usercomp = cla.usercomp
                                                                                       and 	pol.company = cla.company
                                                                                       and 	pol.certype = '2'
                                                                                       and 	pol.branch = cla.branch
                                                                                       and 	pol.policy = cla.policy) cla
                                               join 	usinsug01.claim_his clh
                                                               on		clh.claim = cla.claim
                                                               and		trim(clh.oper_type) in (select cast(operation as varchar(2)) from usinsug01.tab_cl_ope where reserve = 1 or ajustes = 1)
                                                               and		clh.operdate <= cla.dat_lim --en caso de ejecutar a una fecha posterior, alterar esta fecha a la de la ejecuci�n
                                               join 	usinsug01.cl_m_cover clm
                                                               on		clm.usercomp = clh.usercomp
                                                               and		clm.company = clh.company
                                                               and		clm.claim = clh.claim
                                                               and 	clm.movement = clh.transac
                                               group	by 1,2,3) cl0
                       join 	usinsug01.claim cla on cla.ctid = cl0.cla_id
                       join	usinsug01.claim_his clh on clh.ctid = cl0.clh_id
                       where	(cl0.reserva <> 0 or cl0.ajustes <> 0)
                      ) AS TMP
                   '''
        
    DF_INSUNIX_LPG = glue_context.read.format('jdbc').options(**connection).option("fetchsize",10000).option("dbtable",INSUNIX_LPG).load()

    INSUNIX_LPV = f'''
                      (select	
                       'D' INDDETREC,
		       'RBRECIN' TABLAIFRS17,
		       '' PK,
		       '' DTPREG, --excluido
		       '' TIOCPROC, --excluido
		       cast(cla.xxxxdate as varchar) TIOCFRM,
		       '' TIOCTO, --excluido
		       '' KRITPREG, --indeterminado (pendiente coordinar con FID/LP)
		       '' KRITPRGI, --indeterminado (pendiente coordinar con FID/LP)
		       case	when coalesce(clh.amount,0) <= 0
		       		then 1
		       		else 2 end KRCTPRCI,
		       'PIV' KGIORIGM, --excluido
		       cast(cla.branch as varchar) KGCRAMO,
		       cast(cla.policy as varchar) DNUMAPO,
		       cast(cla.certif as varchar) DNMCERT,
		       cast(cla.claim as varchar) DNUMSIN,
		       '0' DNUMSSIN,
		       '' DNUMPENS, --excluido
		       cla.claim || '-' || clh.transac DNUMREC,
		       '' DNMAGRE, --no disponible
		       '' NSAGREG, --excluido
		       '' NSEQSIN, --excluido
		       coalesce(cast(extract(year from cla.occurdat) as varchar),'') DANOSIN,
		       coalesce(cast(clh.operdate as varchar),'') TEMISSAO,
		       '' TINICIO, --no disponible
		       '' TTERMO, --no disponible
		       '' TESTADO, --no disponible
		       /*
		       coalesce((	select	cast(max(cl0.operdate) as varchar)
		       			from	usinsuv01.claim_his cl0
		       			where 	cl0.claim = clh.claim
		       			and		cl0.transac = 	
		       					(	select 	max(csp.transac)
		       						from	usinsuv01.claim_pay_sap csp
		       						where	csp.claim = cl0.claim 
		       						and 	csp.transac_pay = clh.transac)),'')*/ '' TPGCOB,
		       '' TANSUSP, --PENDIENTE CONOCER SI ES POSIBLE/VIALBE
		       '' KRCESPRI, --no disponible
		       '' KRCESTRI, --no disponible
		       '' KRCMOSTI, --excluido
		       cast(cla.moneda_cod as varchar) KRCMOEDA,
		       '' VCAMBIO, --no disponible
		       abs(cast(coalesce(	case	cla.moneda_cod
		       							when	clh.currency-- = cl0.moneda_cod
		       							then    coalesce(clh.amount,0)
		       							else	coalesce(clh.amount,0) *
		       									case	when	clh.currency in (1,2)
		       											then	(	select	case	when clh.currency = 2
		       																		then max(clh0.exchange)
		       																		else 1 / max(clh0.exchange) end
		       														from	usinsuv01.claim_his clh0
		       														where 	clh0.usercomp = clh.usercomp
		       														and		clh0.company = clh.company
		       														and		clh0.claim = clh.claim
		       														and		clh0.transac =
		       																(	select	max(clh1.transac)
		       																	from	usinsuv01.claim_his clh1
		       																	where 	clh1.usercomp = clh.usercomp
		       																	and		clh1.company = clh.company
		       																	and		clh1.claim = clh.claim
		       																	and		clh1.transac <= clh.transac
		       																	and		clh1.exchange not in (1,0)))
		       											else	1 end end, 0) as numeric(12,2))) VMTTOTRI,
		       '' VMTIVA, --no disponible
		       '' VMTIRSRT, --no disponible
		       '' VTXIRSRT, --no disponible
		       '' VMTLIQRC, --excluido
		       abs(cast(coalesce(	case	cla.moneda_cod
		       							when	clh.currency-- = cl0.moneda_cod
		       							then    coalesce(clh.amount,0)
		       							else	coalesce(clh.amount,0) *
		       									case	when	clh.currency in (1,2)
		       											then	(	select	case	when clh.currency = 2
		       																		then max(clh0.exchange)
		       																		else 1 / max(clh0.exchange) end
		       														from	usinsuv01.claim_his clh0
		       														where 	clh0.usercomp = clh.usercomp
		       														and		clh0.company = clh.company
		       														and		clh0.claim = clh.claim
		       														and		clh0.transac =
		       																(	select	max(clh1.transac)
		       																	from	usinsuv01.claim_his clh1
		       																	where 	clh1.usercomp = clh.usercomp
		       																	and		clh1.company = clh.company
		       																	and		clh1.claim = clh.claim
		       																	and		clh1.transac <= clh.transac
		       																	and		clh1.exchange not in (1,0)))
		       											else	1 end end, 0)
		       		* (case when cla.bussityp = '2' then 100 else cla.share_coa end/100) as numeric(12,2))) VMTCOSEG,
		       abs(cast(coalesce(	case	cla.moneda_cod
		       							when	clh.currency-- = cl0.moneda_cod
		       							then    coalesce(clh.amount,0)
		       							else	coalesce(clh.amount,0) *
		       									case	when	clh.currency in (1,2)
		       											then	(	select	case	when clh.currency = 2
		       																		then max(clh0.exchange)
		       																		else 1 / max(clh0.exchange) end
		       														from	usinsuv01.claim_his clh0
		       														where 	clh0.usercomp = clh.usercomp
		       														and		clh0.company = clh.company
		       														and		clh0.claim = clh.claim
		       														and		clh0.transac =
		       																(	select	max(clh1.transac)
		       																	from	usinsuv01.claim_his clh1
		       																	where 	clh1.usercomp = clh.usercomp
		       																	and		clh1.company = clh.company
		       																	and		clh1.claim = clh.claim
		       																	and		clh1.transac <= clh.transac
		       																	and		clh1.exchange not in (1,0)))
		       											else	1 end end, 0)
		       		* (case when cla.bussityp = '2' then 100 else cla.share_coa end/100) as numeric(12,2))) VMTRESSG,
		       cla.branch || '-' || cla.product || '-0' KABPRODT,
		       '' KRCFMPGI, --excluido
		       '' KMEDCB, --excluido
		       '' KMEDPG, --excluido
		       '' DUSRORI, --excluido
		       '' DUSRAPR, --excluido
		       case	when cla.bussityp = '1' and cla.share_coa = 100 then '1' --Sin coaseguro
		       		when cla.bussityp = '1' and cla.share_coa <> 100 then '2' --Con coaseguro
		       		when cla.bussityp = '2' then '3' --LP compa??a no l?der
		       		else '0' end KRCTPCSG,
		       '' DCODENPG, --excluido
		       '' DCODENRC, --excluido
		       '' VMFAT, --no disponible
		       'LPG' DCOMPA,
		       '' DMARCA, --excluido
		       '' TDAPROV, --excluido
		       '' DNMACJUD, --excluido
		       '' KRCTPERP, --excluido
		       '' KRBRECIN_MP, --excluido
		       '' TMIGPARA, --excluido
		       '' KRBRECIN_MD, --excluido
		       '' TMIGDE, --excluido
		       cast(cla.claim as varchar) KSBSIN,
		       cla.branch || '-' || cla.product || '-' || cla.policy || '-' || cla.certif KABAPOL,
		       '' KABAPOL_EFT, --excluido
		       coalesce(cast(cla.date_origi as varchar),'') TINICIOA,
		       cast(coalesce(cla.branch_gyp,0) as varchar) KGCRAMO_SAP,
		       '' KCBCONTA_AE, --excluido
		       '' KCBCONTA_PD, --excluido
		       '' KCBCONTA_FN, --excluido
		       '' KRCCATFIS, --excluido
		       '' KRCTPRET, --excluido
		       '' KRCZNRET, --excluido
		       '' KRCTPRND, --excluido
		       '' TCRIASO, --excluido
		       '' DNUCHEQ, --excluido
		       '' DNIB, --excluido
		       coalesce(cla.client,'') KEBENTID_TO,
		       '' TCONTAB, --excluido
		       coalesce(cast(extract(year from cla.occurdat) as varchar),'') DANOABER,
		       '' DINDIBNR, --excluido
		       '' DINDDESD, --excluido
		       '' DINDASVI, --excluido
		       '' KRCTRESI, --excluido
		       '' VMTSUIRS, --excluido
		       '' DNISS, --excluido
		       '' DPRESERV, --excluido
		       '' KRCTPDCP, --no disponible
		       '' DNUMDOC, --excluido
		       '' TDOCPGPR, --excluido
		       '' DREGACUM, --excluido
		       '' VMTCNTSS, --excluido
		       '' VMTRETSS, --excluido
		       '' VMTSUJSS, --excluido
		       '' KRCTPPGP, --excluido
		       '' DCDRTIRS, --excluido
		       '' DNIF, --excluido
		       '' DARQUIVO, --excluido
		       '' TARQUIVO, --excluido
		       '' VMTINDSTR, --excluido
		       '' VMTPRVERC, --excluido
		       '' VMTPRVINI, --excluido
		       '' VMTINDCTR --excluido
                       from 	(	select	cla.claim,
                                                               cla.usercomp,
                                                               cla.company,
                                                               cla.branch,
                                                               cla.policy,
                                                               cla.certif,
                                                               cla.occurdat,
                                                               coalesce((	select	evi.scod_vt
                                                                                       from	usinsug01.equi_vt_inx evi 
                                                                                       where	evi.scod_inx = cla.client),
                                                                       cla.client) client,
                                                               pol.product,
                                                               pol.bussityp,
                                                               case	coalesce(cla.certif,0)
                                                                               when	0
                                                                               then	pol.date_origi
                                                                               else	(	select	cer.date_origi
                                                                                                       from 	usinsuv01.certificat cer
                                                                                                       where	cer.usercomp = cla.usercomp 
                                                                                                       and 	cer.company = cla.company
                                                                                                       and 	cer.certype = pol.certype
                                                                                                       and 	cer.branch = cla.branch 
                                                                                                       and 	cer.policy = cla.policy
                                                                                                       and		cer.certif = cla.certif) end date_origi,
                                                               coalesce(
                                                                       (	select	max(cpl.currency)
                                                                               from	usinsuv01.curren_pol cpl
                                                                               where 	cpl.usercomp = cla.usercomp and cpl.company = cla.company and cpl.certype = pol.certype
                                                                               and		cpl.branch = cla.branch and cpl.policy = cla.policy and	cpl.certif = cla.certif),
                                                                       (	select	max(cpl.currency)
                                                                               from	usinsuv01.curren_pol cpl
                                                                               where 	cpl.usercomp = cla.usercomp and cpl.company = cla.company and cpl.certype = pol.certype
                                                                               and		cpl.branch = cla.branch and cpl.policy = cla.policy),0) moneda_cod,
                                                               case	when	pol.bussityp <> '2'
                                                                               then	coalesce((	select	coi.share
                                                                                                                       from	usinsuv01.coinsuran coi
                                                                                                                       where   coi.usercomp = cla.usercomp and coi.company = cla.company and coi.certype = pol.certype
                                                                                                                       and     coi.branch = cla.branch and coi.policy = cla.policy and coi.effecdate <= cla.occurdat
                                                                                                                       and     (coi.nulldate is null or coi.nulldate > cla.occurdat)
                                                                                                                       and 	coalesce(coi.companyc,0) in (1,12)),100)
                                                                               else	100 end share_coa,
                                                               case	when	cla.branch = 31
                                                                               then	case	when	substr(pol.titularc,0,1) = 'E'
                                                                                               then 	73
                                                                                               else	case	when
                                                                                                               (	select  sum(ili.quantity)
                                                                                                               from    usinsuv01.insured_li ili
                                                                                                               where   ili.usercomp = cla.usercomp
                                                                                                               and     ili.company = cla.company
                                                                                                               and     ili.certype = pol.certype
                                                                                                               and     ili.branch = cla.branch
                                                                                                               and     ili.policy = cla.policy
                                                                                                               and     ili.certif = 0
                                                                                                               and     ili.effecdate <= pol.effecdate
                                                                                                               and     (ili.nulldate is null or ili.nulldate > pol.effecdate)
                                                                                               and     quantity is not null) = 1
                                                                                                               then	82
                                                                                                               else	(	select  min(sbs.cod_sbs_gyp)
                                                                                                                                                                       from    usinsuv01.product_sbs sbs
                                                                                                                                                                       where   sbs.usercomp = cla.usercomp
                                                                                                                                                                       and     sbs.company = cla.company
                                                                                                                                                                       and     sbs.branch = cla.branch
                                                                                                                                                                       and     sbs.product = pol.product
                                                                                                                                                                       and     sbs.effecdate <= cla.occurdat
                                                                                                                                                                       and     (sbs.nulldate is null or sbs.nulldate > cla.occurdat))
                                                                                                                                               end end
                                                                               when	(cla.branch = 75 and pol.product = 1)
                                                                               then 	case	(	select 	type_cla
                                                                                                                       from	usinsuv01.life_prev
                                                                                                                       where	ctid = 
                                                                                                                       coalesce(
                                                                                                                               (   select max(ctid) from usinsuv01.life_prev
                                                                                                                                       where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                       and branch = cla.branch and policy = cla.policy and certif = cla.certif
                                                                                                                                       and effecdate <= cla.occurdat
                                                                                                                                       and (nulldate is null or nulldate > cla.occurdat)
                                                                                                                                       and statusva not in ('2','3')),
                                                                                                                               (   select max(ctid) from usinsuv01.life_prev
                                                                                                                                       where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                       and branch = cla.branch and policy = cla.policy and certif = cla.certif
                                                                                                                                       and effecdate <= cla.occurdat
                                                                                                                                       and (nulldate is null or nulldate >= cla.occurdat)
                                                                                                                                       and statusva not in ('2','3')),
                                                                                                                               (   select max(ctid) from usinsuv01.life_prev
                                                                                                                                       where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                       and branch = cla.branch and policy = cla.policy and certif = cla.certif
                                                                                                                                       and effecdate <= cla.occurdat
                                                                                                                                       and (nulldate is null or nulldate < cla.occurdat)
                                                                                                                                       and statusva not in ('2','3')),
                                                                                                                               (   select min(ctid) from usinsuv01.life_prev
                                                                                                                                       where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                       and branch = cla.branch and policy = cla.policy and certif = cla.certif
                                                                                                                                       and effecdate > cla.occurdat
                                                                                                                                       and (nulldate is null or nulldate > cla.occurdat)
                                                                                                                                       and statusva not in ('2','3')),
                                                                                                                               (   select min(ctid) from usinsuv01.life_prev
                                                                                                                                       where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                       and branch = cla.branch and policy = cla.policy and certif = cla.certif
                                                                                                                                       and statusva not in ('2','3')),
                                                                                                                               (   select min(ctid) from usinsuv01.life_prev
                                                                                                                                       where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                       and branch = cla.branch and policy = cla.policy and certif = cla.certif
                                                                                                                                       and statusva in ('2','3'))))
                                                                                                               when	4 then 76
                                                                                                               when	5 then 76
                                                                                                               when	2 then 94
                                                                                                               when	3 then 94
                                                                                                               when	1 then 95
                                                                                                               else	0 end 
                                                                               else 	coalesce((	select  distinct(sbs.cod_sbs_gyp)
                                                                                                                       from    usinsuv01.product_sbs tnb,
                                                                                                                                       usinsuv01.anexo1_sbs sbs
                                                                                                                       where   tnb.branch = cla.branch
                                                                                                                       and 	tnb.product = pol.product
                                                                                                                       and 	tnb.nulldate is null
                                                                                                                       and 	sbs.cod_sbs_bal = tnb.cod_sbs_bal
                                                                                                                       and     sbs.cod_sbs_gyp = tnb.cod_sbs_gyp),0) end branch_gyp,
                                                               (	select 	max(greatest(
                                                                                               case when clm.compdate <= dat.par_fin then clm.compdate else '0001-01-01'::date end,
                                                                                               case when clh.compdate <= dat.par_fin then clh.compdate else '0001-01-01'::date end,
                                                                                               case when clh.operdate <= dat.par_fin then clh.operdate else '0001-01-01'::date end))
                                                                       from	usinsuv01.claim_his clh
                                                                                       join	usinsuv01.cl_m_cover clm
                                                                                       on		clm.usercomp = clh.usercomp
                                                                                       and		clm.company = clh.company
                                                                                       and		clm.claim = clh.claim
                                                                                       and		clm.movement = clh.transac
                                                                       where	clh.claim = cla.claim
                                                                       and		trim(clh.oper_type) in 
                                                                                       (select cast(operation as varchar(2)) from usinsug01.tab_cl_ope where pay_amount = 1)) xxxxdate, --se busca la mayor fecha a la nueva fecha de quiebre
                                                               dat.par_fin dat_lim
                                               from 	(	select	prv.cla_id,
                                                                                       prv.par_fin
                                                                       from 	(	--revisi�n fechas transacciones
                                                                                               select	cla.ctid cla_id,
                                                                                                               cla.claim,
                                                                                                               cla.compdate,
                                                                                                               dat.par_fin
                                                                                               from	(	select	cast('{p_fecha_inicio}' as date) par_ini,  --rango inicial carga hist�rica (variable a elecci�n)
                                                                                                                        cast('{p_fecha_fin}' as date) par_fin) dat  --rango final carga hist�rica (variable a elecci�n)
                                                                                               join	usinsuv01.claim cla
                                                                                                               on		cla.usercomp = 1
                                                                                                               and 	cla.company = 1
                                                                                                               and		cla.branch in (select branch from usinsug01.table10b where company = 2)
                                                                                                               and		cla.staclaim <> '6'
                                                                                                               and		cla.compdate <= dat.par_fin --no existe actualizaci�n posterior al rango (1� filtro)
                                                                                               join	usinsuv01.claim_his clh
                                                                                                               on		clh.claim = cla.claim
                                                                                                               and		trim(clh.oper_type) in 
                                                                                                                               (select cast(operation as varchar(2)) from usinsug01.tab_cl_ope where pay_amount = 1)
                                                                                                               and     (clh.operdate between dat.par_ini and dat.par_fin --se consideran siniestros con operaciones entre los rangos (limitaci�n carga inicial)
                                                                                                                               or clh.compdate between dat.par_ini and dat.par_fin)
                                                                                               union
                                                                                               --revisi�n fechas coberturas asociadas a transacciones
                                                                                               select	cla.ctid cla_id,
                                                                                                               cla.claim,
                                                                                                               cla.compdate,
                                                                                                               dat.par_fin
                                                                                               from	(	select	cast('{p_fecha_inicio}' as date) par_ini,  --rango inicial carga hist�rica (variable a elecci�n)
                                                                                                                        cast('{p_fecha_fin}' as date) par_fin) dat  --rango final carga hist�rica (variable a elecci�n)
                                                                                               join	usinsuv01.claim cla
                                                                                                               on		cla.usercomp = 1
                                                                                                               and 	cla.company = 1
                                                                                                               and		cla.branch in (select branch from usinsug01.table10b where company = 2)
                                                                                                               and		cla.staclaim <> '6'
                                                                                                               and		cla.compdate <= dat.par_fin --no existe actualizaci�n posterior al rango (1� filtro)
                                                                                               join	usinsuv01.claim_his clh
                                                                                                               on		clh.claim = cla.claim
                                                                                                               and		trim(clh.oper_type) in 
                                                                                                                               (select cast(operation as varchar(2)) from usinsug01.tab_cl_ope where pay_amount = 1)
                                                                                               join	usinsuv01.cl_m_cover clm
                                                                                                               on		clm.usercomp = clh.usercomp
                                                                                                               and		clm.company = clh.company
                                                                                                               and		clm.claim = clh.claim
                                                                                                               and		clm.movement = clh.transac
                                                                                                               and		clm.compdate between dat.par_ini and dat.par_fin) prv
                                                                       --descarte de siniestros en el caso que posean operaciones/modificaciones de cualquier tipo posterior al rango (2� consideraci�n, la exclusi�n incluye los pagos)
                                                                       where	(	select 	max(greatest(clm.compdate,clh.compdate,clh.operdate,prv.compdate))
                                                                                               from	usinsuv01.claim_his clh
                                                                                               join	usinsuv01.cl_m_cover clm
                                                                                                               on		clm.usercomp = clh.usercomp
                                                                                                               and		clm.company = clh.company
                                                                                                               and		clm.claim = clh.claim
                                                                                                               and		clm.movement = clh.transac
                                                                                               where	clh.claim = prv.claim
                                                                                               and		trim(clh.oper_type) in 
                                                                                                               (select cast(operation as varchar(2)) from usinsug01.tab_cl_ope where reserve = 1 or ajustes = 1 or pay_amount = 1)
                                                                                       ) <= prv.par_fin) dat
                                               join	usinsuv01.claim cla on cla.ctid = dat.cla_id
                                               join	usinsuv01.policy pol
                                                               on		pol.usercomp = cla.usercomp
                                                               and 	pol.company = cla.company
                                                               and 	pol.certype = '2'
                                                               and 	pol.branch = cla.branch
                                                               and 	pol.policy = cla.policy) cla
                       join 	usinsuv01.claim_his clh
                                       on		clh.claim = cla.claim
                                       and		trim(clh.oper_type) in (select cast(operation as varchar(2)) from usinsug01.tab_cl_ope where pay_amount = 1)
                                       and		clh.operdate <= cla.dat_lim
                                       and		coalesce(clh.amount,0) <> 0
                      ) AS TMP
                   '''

    DF_INSUNIX_LPV = glue_context.read.format('jdbc').options(**connection).option("fetchsize",10000).option("dbtable",INSUNIX_LPV).load()
        
    VTIME_LPG = f'''
                    (select	
                     'D' INDDETREC,
		     'SBHISPR' TABLAIFRS17,
		     '' PK,
		     '' DTPREG, --excluido
		     '' TIOCPROC,
		     cast(cl0.xxxxdate as varchar) TIOCFRM,
		     '' TIOCTO, --excluido
		     'PVG' KGIORIGM, --excluido
		     cast(cla."NCLAIM" as varchar) KSBSIN,
		     cast(cla."NCLAIM" as varchar) DNUMSIN,
		     coalesce(cast(cla."NPOLICY" as varchar),'') DNUMAPO,
		     coalesce(cast(cla."NCERTIF" as varchar),'') DNMCERT,
		     case	when	coalesce(cl0.ncover,0) = 0
		     		then	''
		     		else 	cl0.ncover || '-' ||
		     				case 	when	cla."NBRANCH" = 21
		     						then	'l'
		     						else 	'g' end end KGCTPCBT,
		     coalesce(cast(cast(clh."DOPERDATE" as date) as varchar),'') TPROVI,
		     coalesce(cast(clh."NTRANSAC" as varchar),'') DSEQMOV,
		     coalesce(cast(clh."NOPER_TYPE" as varchar),'') KSCTPMPR,
		     coalesce(cl0.reserva,0) VMTPROVI,
		     coalesce(cl0.ajustes,0) VMTPRVAR,
		     '' TULTALT, --excluido
		     '' DUSRUPD, --excluido
		     'LPG' DCOMPA,
		     '' DMARCA, --excluido
		     '' KSBSUBSN, --excluido
		     '' KSCMTDPR, --excluido
		     coalesce(	
		     	coalesce(	
		     		coalesce(	
		     			coalesce((	select  max("SCLIENT")
		     								from    usvtimg01."CLAIMBENEF" 
		     								where   "NCLAIM" = clh."NCLAIM"
		     								and 	"NCASE_NUM" = clh."NCASE_NUM"
		     								and 	"NDEMAN_TYPE" = clh."NDEMAN_TYPE"
		     								and 	"NBENE_TYPE" = 2),
		     							 (	select  max("SCLIENT")
		     								from    usvtimg01."CLAIMBENEF" 
		     								where   "NCLAIM" = clh."NCLAIM"
		     								and 	"NCASE_NUM" = clh."NCASE_NUM"
		     								and 	"NDEMAN_TYPE" = clh."NDEMAN_TYPE"
		     								and 	"NBENE_TYPE" = 7)),
		     						 (	select  max("SCLIENT")
		     							from    usvtimg01."CLAIMBENEF" 
		     							where   "NCLAIM" = clh."NCLAIM"
		     							and 	"NCASE_NUM" = clh."NCASE_NUM"
		     							and 	"NDEMAN_TYPE" = clh."NDEMAN_TYPE"
		     							and 	"NBENE_TYPE" = 14)),
		     				coalesce((	select  max("SCLIENT")
		     							from    usvtimg01."CLAIMBENEF" 
		     							where   "NCLAIM" = clh."NCLAIM"
		     							and 	"NCASE_NUM" = clh."NCASE_NUM"
		     							and 	"NDEMAN_TYPE" = clh."NDEMAN_TYPE"
		     							and 	"NBENE_TYPE" = 119),
		     						 (	select  max("SCLIENT")
		     							from    usvtimg01."CLAIMBENEF" 
		     							where   "NCLAIM" = clh."NCLAIM"
		     							and 	"NCASE_NUM" = clh."NCASE_NUM"
		     							and 	"NDEMAN_TYPE" = clh."NDEMAN_TYPE"
		     							and 	"NBENE_TYPE" = 118))),'') KEBENTID_SN,
		     '' DENTIDSO, --excluido
		     '' DIDADELES, --excluido
		     '' TCRIACAO --excluido
                     from	(	select	cla.cla_id,
                                                             clh.ctid clh_id,
                                                             case	when	cla.nbranch = 21
                                                                     then	(   select  gco."NCOVERGEN"
                                                                             from    usvtimg01."LIFE_COVER" gco
                                                                             where   gco."NCOVER" = clm."NCOVER"
                                                                             and     gco."NPRODUCT" = cla.nproduct
                                                                             and     gco."NMODULEC" = clm."NMODULEC"
                                                                             and     gco."NBRANCH" = cla.nbranch
                                                                             and     cast(gco."DEFFECDATE" as date) <= cla.doccurdat
                                                                             and     (gco."DNULLDATE" is null or cast(gco."DNULLDATE" as date) > cla.doccurdat)
                                                                             and     gco."SSTATREGT" <> '4')
                                                                             else	(	select  gco."NCOVERGEN"
                                                                             from    usvtimg01."GEN_COVER" gco
                                                                             where   gco."NCOVER" = clm."NCOVER"
                                                                             and     gco."NPRODUCT" = cla.nproduct
                                                                             and     gco."NMODULEC" = clm."NMODULEC"
                                                                             and     gco."NBRANCH" = cla.nbranch
                                                                             and     cast(gco."DEFFECDATE" as date) <= cla.doccurdat
                                                                             and     (gco."DNULLDATE" is null or cast(gco."DNULLDATE" as date) > cla.doccurdat)
                                                                             and     gco."SSTATREGT" <> '4') end ncover,
                                                             max(cla.xxxxdate) xxxxdate,
                                                             sum(coalesce(	case	when	cla.moneda_cod = 1
                                                                                                             then	case	when	clm."NCURRENCY" = 1
                                                                                                                                             then	coalesce(clm."NAMOUNT",0)
                                                                                                                                             when	clm."NCURRENCY" = 2
                                                                                                                                             then	coalesce(clm."NAMOUNT",0) * clm."NEXCHANGE"
                                                                                                                                             else	0 end
                                                                                                             when	cla.moneda_cod = 2
                                                                                                             then	case	when	clm."NCURRENCY" = 2
                                                                                                                                             then	coalesce(clm."NAMOUNT",0)
                                                                                                                                             when	clm."NCURRENCY" = 1
                                                                                                                                             then	coalesce(clm."NLOC_AMOUNT",0)
                                                                                                                                             else	0 end
                                                                                                             else	0 end, 0) * case csv.tipo when 1 then 1 else 0 end) reserva,
                                                             sum(coalesce(	case	when	cla.moneda_cod = 1
                                                                                                             then	case	when	clm."NCURRENCY" = 1
                                                                                                                                             then	coalesce(clm."NAMOUNT",0)
                                                                                                                                             when	clm."NCURRENCY" = 2
                                                                                                                                             then	coalesce(clm."NAMOUNT",0) * clm."NEXCHANGE"
                                                                                                                                             else	0 end
                                                                                                             when	cla.moneda_cod = 2
                                                                                                             then	case	when	clm."NCURRENCY" = 2
                                                                                                                                             then	coalesce(clm."NAMOUNT",0)
                                                                                                                                             when	clm."NCURRENCY" = 1
                                                                                                                                             then	coalesce(clm."NLOC_AMOUNT",0)
                                                                                                                                             else	0 end
                                                                                                             else	0 end, 0) * case csv.tipo when 2 then 1 else 0 end) ajustes
                                             from	(	select  cla."NCLAIM" nclaim,
                                                                                     cla.ctid cla_id,
                                                             cla."NBRANCH" nbranch,
                                                             cla."NPOLICY" npolicy,
                                                             cla."NCERTIF" ncertif,
                                                             pol."NPRODUCT" nproduct,
                                                             cast(cla."DOCCURDAT" as date) doccurdat,
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
                                                                                     (	select 	max(greatest(
                                                                                                                     case when cast(clm."DCOMPDATE" as date) <= dat.par_fin then cast(clm."DCOMPDATE" as date) else '0001-01-01'::date end,
                                                                                                                     case when cast(clh."DCOMPDATE" as date) <= dat.par_fin then cast(clh."DCOMPDATE" as date) else '0001-01-01'::date end,
                                                                                                                     case when cast(clh."DOPERDATE" as date) <= dat.par_fin then cast(clh."DOPERDATE" as date) else '0001-01-01'::date end))
                                                                                             from	usvtimg01."CLAIM_HIS" clh
                                                                                     join	usvtimg01."CL_M_COVER" clm
                                                                                     on		clm."NCLAIM" = clh."NCLAIM"
                                                                                     and		clm."NCASE_NUM" = clH."NCASE_NUM"
                                                                                     and		clm."NDEMAN_TYPE" = clh."NDEMAN_TYPE"
                                                                                     and		clm."NTRANSAC" = clh."NTRANSAC"
                                                                                             where	clh."NCLAIM" = cla."NCLAIM"
                                                                                             and		clh."NOPER_TYPE" in (select	cast("SVALUE" as INT4) from usvtimg01."CONDITION_SERV" where "NCONDITION" in (71,72))
                                                                                             and		(clh."DOPERDATE" <= dat.par_fin or clh."DCOMPDATE" <= dat.par_fin) --evita tomar operaciones posteriores (carga incremental)
                                                                                             ) xxxxdate,
                                                                                     dat.par_fin dat_lim
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
                                                                             and		(	exists
                                                                                             (   select  1
                                                                                             from    usvtimg01."CLAIM_HIS" clh
                                                                                             where	clh."NCLAIM" = cla."NCLAIM"
                                                                                             and		coalesce(clh."NCASE_NUM",0) >= 0
                                                                                                                     and     (cast(clh."DOPERDATE" as date) between dat.par_ini and dat.par_fin --se consideran siniestros con operaciones entre los rangos (limitaci�n carga inicial)
                                                                                                                                     or cast(clh."DCOMPDATE" as date) between dat.par_ini and dat.par_fin) --se consideran siniestros con modificaciones entre los rangos (limitaci�n carga inicial)
                                                                                                                     and		clh."NOPER_TYPE" in 
                                                                                                                                     (select	cast("SVALUE" as INT4) from	usvtimg01."CONDITION_SERV" cs  where "NCONDITION" in (71,72)))
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
                                                                                                                                             (select	cast("SVALUE" as INT4) from	usvtimg01."CONDITION_SERV" cs  where "NCONDITION" in (71,72))))
                                                                                     and		cast(cla."DCOMPDATE" as date) <= dat.par_fin --registro no considerado en ninguna carga inicial anterior (nivel cabecera)
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
                                                                                                                             (select	cast("SVALUE" as INT4) from	usvtimg01."CONDITION_SERV" cs  where "NCONDITION" in (71,72,73)))) cla
                                             join	usvtimg01."CL_M_COVER" clm on clm."NCLAIM" = cla.nclaim
                                             join	usvtimg01."CLAIM_HIS" clh
                                                             on		clh."NCLAIM" = clm."NCLAIM"
                                                             and 	clh."NCASE_NUM" = clm."NCASE_NUM"
                                                             and 	clh."NDEMAN_TYPE" = clm."NDEMAN_TYPE"
                                                             and 	clh."NTRANSAC" = clm."NTRANSAC"
                                                             and		cast(clh."DOPERDATE" as date) <= cla.dat_lim
                                             join	(	select	case 	when	"NCONDITION" = 71 then 1
                                                                                                     when	"NCONDITION" = 72 then 2
                                                                                                     else	0 end tipo,
                                                                                     cast("SVALUE" as INT4) svalue
                                                                     from	usvtimg01."CONDITION_SERV"
                                                                     where	"NCONDITION" in (71,72)) csv
                                                             on		csv.SVALUE = clh."NOPER_TYPE"
                                             group 	by 1,2,3) cl0
                     join	usvtimg01."CLAIM" cla on cla.ctid = CL0.cla_id
                     join	usvtimg01."CLAIM_HIS" clh on clh.ctid = cl0.clh_id
                     where	(cl0.reserva <> 0 or cl0.ajustes <> 0)
                    ) AS TMP        
                 '''
        
    DF_VTIME_LPG = glue_context.read.format('jdbc').options(**connection).option("fetchsize",10000).option("dbtable",VTIME_LPG).load()

    VTIME_LPV = f'''
                    (select	
                     'D' INDDETREC,
		     'SBHISPR' TABLAIFRS17,
		     '' PK,
		     '' DTPREG, --excluido
		     '' TIOCPROC,
		     cast(cl0.xxxxdate as varchar) TIOCFRM,
		     '' TIOCTO, --excluido
		     'PVV' KGIORIGM, --excluido
		     cast(cla."NCLAIM" as varchar) KSBSIN,
		     cast(cla."NCLAIM" as varchar) DNUMSIN,
		     coalesce(cast(cla."NPOLICY" as varchar),'') DNUMAPO,
		     coalesce(cast(cla."NCERTIF" as varchar),'') DNMCERT,
		     case	when	coalesce(cl0.ncover,0) = 0
		     		then	''
		     		else 	cl0.ncover || '-' ||
		     				case 	when	cla."NBRANCH" = 21
		     						then	'l'
		     						else 	'g' end end KGCTPCBT,
		     coalesce(cast(cast(clh."DOPERDATE" as date) as varchar),'') TPROVI,
		     coalesce(cast(clh."NTRANSAC" as varchar),'') DSEQMOV,
		     coalesce(cast(clh."NOPER_TYPE" as varchar),'') KSCTPMPR,
		     coalesce(cl0.reserva,0) VMTPROVI,
		     coalesce(cl0.ajustes,0) VMTPRVAR,
		     '' TULTALT, --excluido
		     '' DUSRUPD, --excluido
		     'LPV' DCOMPA,
		     '' DMARCA, --excluido
		     '' KSBSUBSN, --excluido
		     '' KSCMTDPR, --excluido
		     coalesce(	
		     	coalesce(	
		     		coalesce(	
		     			coalesce((	select  max("SCLIENT")
		     								from    usvtimv01."CLAIMBENEF" 
		     								where   "NCLAIM" = clh."NCLAIM"
		     								and 	"NCASE_NUM" = clh."NCASE_NUM"
		     								and 	"NDEMAN_TYPE" = clh."NDEMAN_TYPE"
		     								and 	"NBENE_TYPE" = 2),
		     							 (	select  max("SCLIENT")
		     								from    usvtimv01."CLAIMBENEF" 
		     								where   "NCLAIM" = clh."NCLAIM"
		     								and 	"NCASE_NUM" = clh."NCASE_NUM"
		     								and 	"NDEMAN_TYPE" = clh."NDEMAN_TYPE"
		     								and 	"NBENE_TYPE" = 7)),
		     						 (	select  max("SCLIENT")
		     							from    usvtimv01."CLAIMBENEF" 
		     							where   "NCLAIM" = clh."NCLAIM"
		     							and 	"NCASE_NUM" = clh."NCASE_NUM"
		     							and 	"NDEMAN_TYPE" = clh."NDEMAN_TYPE"
		     							and 	"NBENE_TYPE" = 14)),
		     				coalesce((	select  max("SCLIENT")
		     							from    usvtimv01."CLAIMBENEF" 
		     							where   "NCLAIM" = clh."NCLAIM"
		     							and 	"NCASE_NUM" = clh."NCASE_NUM"
		     							and 	"NDEMAN_TYPE" = clh."NDEMAN_TYPE"
		     							and 	"NBENE_TYPE" = 119),
		     						 (	select  max("SCLIENT")
		     							from    usvtimv01."CLAIMBENEF" 
		     							where   "NCLAIM" = clh."NCLAIM"
		     							and 	"NCASE_NUM" = clh."NCASE_NUM"
		     							and 	"NDEMAN_TYPE" = clh."NDEMAN_TYPE"
		     							and 	"NBENE_TYPE" = 118))),'') KEBENTID_SN,
		     '' DENTIDSO, --excluido
		     '' DIDADELES, --excluido
		     '' TCRIACAO --excluido
                     from	(	select	cla.cla_id,
                                                             clh.ctid clh_id,
                                                             (   select  gco."NCOVERGEN"
                                                     from    usvtimv01."LIFE_COVER" gco
                                                     where   gco."NCOVER" = clm."NCOVER"
                                                     and     gco."NPRODUCT" = cla.nproduct
                                                     and     gco."NMODULEC" = clm."NMODULEC"
                                                     and     gco."NBRANCH" = cla.nbranch
                                                     and     cast(gco."DEFFECDATE" as date) <= doccurdat
                                                     and     (gco."DNULLDATE" is null or cast(gco."DNULLDATE" as date) > doccurdat)
                                                     and     gco."SSTATREGT" <> '4') ncover,
                                                             max(cla.xxxxdate) xxxxdate,
                                                             sum(coalesce(	case	when	cla.moneda_cod = 1
                                                                                                             then	case	when	clm."NCURRENCY" = 1
                                                                                                                                             then	coalesce(clm."NAMOUNT",0)
                                                                                                                                             when	clm."NCURRENCY" = 2
                                                                                                                                             then	coalesce(clm."NAMOUNT",0) * clm."NEXCHANGE"
                                                                                                                                             else	0 end
                                                                                                             when	cla.moneda_cod = 2
                                                                                                             then	case	when	clm."NCURRENCY" = 2
                                                                                                                                             then	coalesce(clm."NAMOUNT",0)
                                                                                                                                             when	clm."NCURRENCY" = 1
                                                                                                                                             then	coalesce(clm."NLOC_AMOUNT",0)
                                                                                                                                             else	0 end
                                                                                                             else	0 end, 0) * case csv.tipo when 1 then 1 else 0 end) reserva,
                                                             sum(coalesce(	case	when	cla.moneda_cod = 1
                                                                                                             then	case	when	clm."NCURRENCY" = 1
                                                                                                                                             then	coalesce(clm."NAMOUNT",0)
                                                                                                                                             when	clm."NCURRENCY" = 2
                                                                                                                                             then	coalesce(clm."NAMOUNT",0) * clm."NEXCHANGE"
                                                                                                                                             else	0 end
                                                                                                             when	cla.moneda_cod = 2
                                                                                                             then	case	when	clm."NCURRENCY" = 2
                                                                                                                                             then	coalesce(clm."NAMOUNT",0)
                                                                                                                                             when	clm."NCURRENCY" = 1
                                                                                                                                             then	coalesce(clm."NLOC_AMOUNT",0)
                                                                                                                                             else	0 end
                                                                                                             else	0 end, 0) * case csv.tipo when 2 then 1 else 0 end) ajustes
                                             from	(	select  cla."NCLAIM" nclaim,
                                                                                     cla.ctid cla_id,
                                                             cla."NBRANCH" nbranch,
                                                             cla."NPOLICY" npolicy,
                                                             cla."NCERTIF" ncertif,
                                                             pol."NPRODUCT" nproduct,
                                                             cast(cla."DOCCURDAT" as date) doccurdat,
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
                                                                                     (	select 	max(greatest(
                                                                                                                     case when cast(clm."DCOMPDATE" as date) <= dat.par_fin then cast(clm."DCOMPDATE" as date) else '0001-01-01'::date end,
                                                                                                                     case when cast(clh."DCOMPDATE" as date) <= dat.par_fin then cast(clh."DCOMPDATE" as date) else '0001-01-01'::date end,
                                                                                                                     case when cast(clh."DOPERDATE" as date) <= dat.par_fin then cast(clh."DOPERDATE" as date) else '0001-01-01'::date end))
                                                                                             from	usvtimv01."CLAIM_HIS" clh
                                                                                     join	usvtimv01."CL_M_COVER" clm
                                                                                     on		clm."NCLAIM" = clh."NCLAIM"
                                                                                     and		clm."NCASE_NUM" = clH."NCASE_NUM"
                                                                                     and		clm."NDEMAN_TYPE" = clh."NDEMAN_TYPE"
                                                                                     and		clm."NTRANSAC" = clh."NTRANSAC"
                                                                                             where	clh."NCLAIM" = cla."NCLAIM"
                                                                                             and		clh."NOPER_TYPE" in (select	cast("SVALUE" as INT4) from usvtimv01."CONDITION_SERV" where "NCONDITION" in (71,72,73))
                                                                                             and		(clh."DOPERDATE" <= dat.par_fin or clh."DCOMPDATE" <= dat.par_fin) --evita tomar operaciones posteriores (carga incremental)
                                                                                             ) xxxxdate,
                                                                                     dat.par_fin dat_lim
                                                                     from    (	select	ctid pol_id
                                                                                             from	usvtimv01."POLICY"
                                                                                             where	"SCERTYPE" = '2') po0
                                                                     join	(	select	cast('01/01/2015' as date) par_ini, --rango inicial carga incremental (variable a elecci�n)
                                                                                                             cast('12/30/2015' as date) par_fin --rango final carga incremental (variable a elecci�n)
                                                                                     ) dat on 1 = 1
                                                                     join	usvtimv01."POLICY" pol on pol.ctid = po0.pol_id
                                                                     join	usvtimv01."CLAIM" cla
                                                                                     on		cla."SCERTYPE" = pol."SCERTYPE"
                                                                             and     cla."NPOLICY" = pol."NPOLICY"
                                                                             and     cla."NBRANCH" = pol."NBRANCH"
                                                                             and     cla."SSTACLAIM" <> '6'
                                                                             and		(	exists
                                                                                             (   select  1
                                                                                             from    usvtimv01."CLAIM_HIS" clh
                                                                                             where	clh."NCLAIM" = cla."NCLAIM"
                                                                                             and		coalesce(clh."NCASE_NUM",0) >= 0
                                                                                                                     and     (cast(clh."DOPERDATE" as date) between dat.par_ini and dat.par_fin --se consideran siniestros con operaciones entre los rangos (limitaci�n carga inicial)
                                                                                                                                     or cast(clh."DCOMPDATE" as date) between dat.par_ini and dat.par_fin) --se consideran siniestros con modificaciones entre los rangos (limitaci�n carga inicial)
                                                                                                                     and		clh."NOPER_TYPE" in 
                                                                                                                                     (select	cast("SVALUE" as INT4) from	usvtimv01."CONDITION_SERV" cs  where "NCONDITION" in (71,72)))
                                                                                                             or	exists
                                                                                                     (   select  1
                                                                                                     from    usvtimv01."CLAIM_HIS" clh
                                                                                                                     join	usvtimv01."CL_M_COVER" clm
                                                                                                                     on		clm."NCLAIM" = clh."NCLAIM"
                                                                                                                     and		clm."NCASE_NUM" = clh."NCASE_NUM"
                                                                                                                     and		clm."NDEMAN_TYPE" = clh."NDEMAN_TYPE"
                                                                                                                     and		clm."NTRANSAC" = clh."NTRANSAC"
                                                                                                                     and		cast(clm."DCOMPDATE" as date) between dat.par_ini and dat.par_fin --se consideran coberturas con modificaciones entre los rangos (limitaci�n carga inicial)
                                                                                                     where	clh."NCLAIM" = cla."NCLAIM"
                                                                                                     and		coalesce(clh."NCASE_NUM",0) >= 0
                                                                                                                             and		clh."NOPER_TYPE" in 
                                                                                                                                             (select	cast("SVALUE" as INT4) from	usvtimv01."CONDITION_SERV" cs  where "NCONDITION" in (71,72))))
                                                                                     and		cast(cla."DCOMPDATE" as date) <= dat.par_fin --registro no considerado en ninguna carga inicial anterior (nivel cabecera)
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
                                                                                                                             (select	cast("SVALUE" as INT4) from	usvtimv01."CONDITION_SERV" cs  where "NCONDITION" in (71,72,73)))) cla
                                             join	usvtimv01."CL_M_COVER" clm on clm."NCLAIM" = cla.nclaim
                                             join	usvtimv01."CLAIM_HIS" clh
                                                             on		clh."NCLAIM" = clm."NCLAIM"
                                                             and 	clh."NCASE_NUM" = clm."NCASE_NUM"
                                                             and 	clh."NDEMAN_TYPE" = clm."NDEMAN_TYPE"
                                                             and 	clh."NTRANSAC" = clm."NTRANSAC"
                                                             and		cast(clh."DOPERDATE" as date) <= cla.dat_lim
                                             join	(	select	case 	when	"NCONDITION" = 71 then 1
                                                                                                     when	"NCONDITION" = 72 then 2
                                                                                                     else	0 end tipo,
                                                                                     cast("SVALUE" as INT4) svalue
                                                                     from	usvtimv01."CONDITION_SERV"
                                                                     where	"NCONDITION" in (71,72)) csv
                                                             on		csv.SVALUE = clh."NOPER_TYPE"
                                             group 	by 1,2,3) cl0
                     join	usvtimv01."CLAIM" cla on cla.ctid = CL0.cla_id
                     join	usvtimv01."CLAIM_HIS" clh on clh.ctid = cl0.clh_id
                     where	(cl0.reserva <> 0 or cl0.ajustes <> 0)
                    ) AS TMP
                 '''
        
    DF_VTIME_LPV = glue_context.read.format('jdbc').options(**connection).option("fetchsize",10000).option("dbtable",VTIME_LPV).load()

    INSIS_LPV = f'''
                    (select	
                     'D' INDDETREC,
		     'SBHISPR' TABLAIFRS17,
		     '' PK,
		     '' DTPREG, --excluido
		     '' TIOCPROC,
		     cast(cla.xxxxdate as varchar) TIOCFRM,
		     '' TIOCTO, --excluido
		     'PNV' KGIORIGM, --excluido
		     coalesce(cast(cla.nclaim as varchar),'') KSBSIN,
		     coalesce(cast(cla.nclaim as varchar),'') DNUMSIN,
		     substring(cast(coalesce(cla.pep_id,cla.policy_id) as varchar),6) DNUMAPO,
		     coalesce(substring(cast(case when cla.pep_id is not null then cla.policy_id else null end as varchar),6),'0') DNMCERT,
		     coalesce((	select 	case	when "COVER_CPR_ID" < 1000000
	     								then cast(round("COVER_CPR_ID",0) as varchar)
	     								else substring(cast(round("COVER_CPR_ID",0) as varchar),5)
	     								end
	     				from	usinsiv01."CPR_COVER"
	     				where	"COVER_TYPE" = clo."COVER_TYPE"),'') KGCTPCBT,
		     coalesce(cast(cast(clh."REGISTRATION_DATE" as date) as varchar),'') TPROVI,
		     coalesce(cast(round("RESERV_SEQ",0) as varchar),'') DSEQMOV,
		     coalesce(cast(clh."OP_TYPE" as varchar),'') KSCTPMPR,
		     case when clh."OP_TYPE" in ('REG') then coalesce("RESERV_CHANGE",0) else 0 end VMTPROVI,
		     case when clh."OP_TYPE" in ('EST','CLC') then coalesce("RESERV_CHANGE",0) else 0 end VMTPRVAR,
		     '' TULTALT, --excluido
		     '' DUSRUPD, --excluido
		     'LPV' DCOMPA,
		     '' DMARCA, --excluido
		     '' KSBSUBSN, --excluido
		     '' KSCMTDPR, --excluido
		     coalesce(
	     		coalesce((	select	lpi."LEGACY_ID"
	     					from	usinsiv01."INTRF_LPV_PEOPLE_IDS" lpi
	                     	where	lpi."MAN_ID" = cla.rol_a_man_id),
	     				cast(cla.rol_a_man_id as varchar)),'') KEBENTID_SN,
		     '' DENTIDSO, --excluido
		     '' DIDADELES, --excluido
		     '' TCRIACAO --excluido
                     from	(	select	cla."CLAIM_ID" nclaim,
                                                             pol."POLICY_ID" policy_id,
                                                             (select "MASTER_POLICY_ID" from usinsiv01."POLICY_ENG_POLICIES" where "POLICY_ID" = CLA."POLICY_ID") pep_id,
                                     coalesce((  select  min(acc."MAN_ID")
                                                     from    usinsiv01."INSURED_OBJECT" obj
                                                                                     join	usinsiv01."O_ACCINSURED" acc
                                                                                                     on		acc."OBJECT_ID" = obj."OBJECT_ID"
                                                                                                     and     coalesce(acc."ACCINS_TYPE",'0') = case obj."INSR_TYPE" when 2007 then '0' when 2004 then '0' when 2001 then '0' else '1' end
                                                                                     join	usinsiv01."P_PEOPLE" peo on peo."MAN_ID" = acc."MAN_ID"
                                                     where   obj."POLICY_ID" = cla."POLICY_ID"
                                                     and     cast(cla."EVENT_DATE" as date) 
                                                                     between cast(obj."INSR_BEGIN" as date) and cast(coalesce(obj."INSR_END",'12/31/9999') as date)),
                                                     (   select  min(acc."MAN_ID")
                                                     from    usinsiv01."INSURED_OBJECT" obj
                                                     join	usinsiv01."O_ACCINSURED" acc
                                                                     on		acc."OBJECT_ID" = obj."OBJECT_ID"
                                                                     and     coalesce(acc."ACCINS_TYPE",'0') = case obj."INSR_TYPE" when 2007 then '0' when 2004 then '0' when 2001 then '0' else '1' end
                                                                                     join	usinsiv01."P_PEOPLE" peo on peo."MAN_ID" = acc."MAN_ID"
                                                     where   obj."POLICY_ID" = cla."POLICY_ID"
                                                     and		cast(cla."EVENT_DATE" as date)
                                                                     between cast(obj."INSR_BEGIN" as date) and cast(coalesce(obj."INSR_END",'12/31/9999') as date))) rol_a_man_id,
                                                             (	select	max(greatest(
                                                                                             case when cast(cla."REGISTRATION_DATE" as date) <= dat.par_fin then cast(cla."REGISTRATION_DATE" as date) else '0001-01-01'::date end,
                                                                                             case when cast(clh."REGISTRATION_DATE" as date) <= dat.par_fin then cast(clh."REGISTRATION_DATE" as date) else '0001-01-01'::date end,
                                                                                             case when cast(clh."CHANGE_DATE" as date) <= dat.par_fin then cast(clh."CHANGE_DATE" as date) else '0001-01-01'::date end))
                                                                     from	usinsiv01."CLAIM_RESERVE_HISTORY" clh
                                                                     join	usinsiv01."CLAIM_OBJECTS" clo
                                                                                     on		clo."CLAIM_ID" = clh."CLAIM_ID"
                                                                                     and		clo."CLAIM_OBJ_SEQ" = clh."CLAIM_OBJECT_SEQ"
                                                                                     and		clo."REQUEST_ID" = clh."REQUEST_ID"
                                                                                     and		clo."CLAIM_STATE" <> -1
                                                                     where	clh."CLAIM_ID" = cla."CLAIM_ID"
                                                                     and		clh."OP_TYPE" IN ('REG','EST','CLC')) xxxxdate,
                                                             dat.par_fin dat_lim
                                             from	usinsiv01."CLAIM" cla
                                             join	(	select	cast('{p_fecha_inicio}' as date) par_ini, --rango inicial carga hist�rica (variable a elecci�n)
                                                                        cast('{p_fecha_fin}' as date) par_fin --rango final carga hist�rica (variable a elecci�n)
                                                             ) dat on 1 = 1
                                             join	usinsiv01."POLICY" pol
                                                             on 		pol."POLICY_ID" = cla."POLICY_ID" 
                                                             and 	pol."POLICY_STATE" >= 0
                                             where	exists 
                                                             (	select	1 
                                                                     from	usinsiv01."CLAIM_RESERVE_HISTORY" clh
                                                                     join	usinsiv01."CLAIM_OBJECTS" clo 
                                                                                     on		clo."CLAIM_ID" = clh."CLAIM_ID"
                                                                                     and		clo."CLAIM_OBJ_SEQ" = clh."CLAIM_OBJECT_SEQ"
                                                                                     and		clo."REQUEST_ID" = clh."REQUEST_ID"
                                                                                     and		clo."CLAIM_STATE" <> -1
                                                                     where	clh."CLAIM_ID" = cla."CLAIM_ID"
                                                                     and		clh."OP_TYPE" IN ('REG','EST','CLC')
                                                                     and		(cast(clh."REGISTRATION_DATE" as date) between dat.par_ini and dat.par_fin --se consideran siniestros operaciones en el nuevo periodo
                                                                                     or cast(clh."CHANGE_DATE" as date) between dat.par_ini and dat.par_fin))
                                             and		cast(cla."REGISTRATION_DATE" as date) <= dat.par_fin --registro no considerado en ninguna carga inicial anterior (nivel cabecera)
                                             and 	not exists --registro no considerado si existen operaciones posteriores al rango (nivel transacci�n)
                                                             (	select	1 
                                                                     from	usinsiv01."CLAIM_RESERVE_HISTORY" clh
                                                                     join	usinsiv01."CLAIM_OBJECTS" clo
                                                                                     on		clo."CLAIM_ID" = clh."CLAIM_ID"
                                                                                     and		clo."CLAIM_OBJ_SEQ" = clh."CLAIM_OBJECT_SEQ"
                                                                                     and		clo."REQUEST_ID" = clh."REQUEST_ID"
                                                                                     and		clo."CLAIM_STATE" <> -1
                                                                     where	clh."CLAIM_ID" = cla."CLAIM_ID"
                                                                     and		clh."OP_TYPE" IN ('REG','EST','CLC','PAYMCONF','PAYMINV') --se considera la exclusi�n de todos los movimientos
                                                                     and		(cast(clh."REGISTRATION_DATE" as date) > dat.par_fin --no existan operaciones posteriores al corte
                                                                                     or cast(clh."CHANGE_DATE" as date) > dat.par_fin))) cla --no existan modificaciones posteriores al corte
                     join	usinsiv01."CLAIM_OBJECTS" clo
                                     on		clo."CLAIM_ID" = cla.nclaim
                                     and		clo."CLAIM_STATE" <> -1
                     join	usinsiv01."CLAIM_RESERVE_HISTORY" clh
                                     on		clh."CLAIM_ID" = clo."CLAIM_ID"
                                     and		clh."CLAIM_OBJECT_SEQ" = clo."CLAIM_OBJ_SEQ"
                                     and		clh."REQUEST_ID" = clo."REQUEST_ID"
                                     and		clh."OP_TYPE" IN ('REG','EST','CLC')
                                     and		cast(clh."REGISTRATION_DATE" as date) <= cla.dat_lim --en caso de ejecutar a una fecha posterior, alterar esta fecha a la de la ejecuci�n
                                     and		coalesce("RESERV_CHANGE",0)	<> 0
                    ) AS TMP
                 '''

    DF_INSIS_LPV = glue_context.read.format('jdbc').options(**connection).option("dbtable",INSIS_LPV).load()

    L_DF_SBHISPR = DF_INSUNIX_LPG.union(DF_INSUNIX_LPV).union(DF_VTIME_LPG).union(DF_VTIME_LPV).union(DF_INSIS_LPV)

    return L_DF_SBHISPR