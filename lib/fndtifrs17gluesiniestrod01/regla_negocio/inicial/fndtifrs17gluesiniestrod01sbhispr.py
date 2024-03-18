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
                                                                                                                (select cast(operation as varchar(2)) from usinsug01.tab_cl_ope where reserve = 1 or ajustes = 1)) xxxxdate, --se busca la mayor fecha a la nueva fecha de quiebre
                                                                                        dat.par_fin dat_lim
                                                                        from 	(	--revisi�n fechas transacciones
                                                                                                select	cla.ctid cla_id,
                                                                                                                dat.par_fin
                                                                                                from	(	select	cast('{p_fecha_inicio}' as date) par_ini, --ejecutar al a�o 2021 (2019 es para pruebas)
                                                                                                                        cast('{p_fecha_fin}' as date) par_fin) dat --limitador para la carga inicial (fecha l�mite a�o 2023, 2022 para prueba)
                                                                                                join	usinsug01.claim cla
                                                                                                                on		cla.usercomp = 1
                                                                                                                and 	cla.company = 1
                                                                                                                and		cla.branch in (select branch from usinsug01.table10b where company = 1)
                                                                                                                and		(cla.staclaim <> '6' or (cla.staclaim = '6' and cla.branch = 23))
                                                                                                join	usinsug01.claim_his clh
                                                                                                                on		clh.claim = cla.claim
                                                                                                                and		trim(clh.oper_type) in 
                                                                                                                                (select cast(operation as varchar(2)) from usinsug01.tab_cl_ope where reserve = 1 or ajustes = 1)
                                                                                                                and     (clh.operdate between dat.par_ini and dat.par_fin --se consideran siniestros con operaciones entre los rangos (limitaci�n carga inicial)
                                                                                                                                or clh.compdate between dat.par_ini and dat.par_fin)
                                                                                                union
                                                                                                --revisi�n fechas coberturas asociadas a transacciones
                                                                                                select	cla.ctid cla_id,
                                                                                                                dat.par_fin
                                                                                                from	(	select	cast('{p_fecha_inicio}' as date) par_ini, --ejecutar al a�o 2021 (2019 es para pruebas)
                                                                                                                        cast('{p_fecha_fin}' as date) par_fin) dat --limitador para la carga inicial (fecha l�mite a�o 2023, 2022 para prueba)
                                                                                                join	usinsug01.claim cla
                                                                                                                on		cla.usercomp = 1
                                                                                                                and 	cla.company = 1
                                                                                                                and		cla.branch in (select branch from usinsug01.table10b where company = 1)
                                                                                                                and		(cla.staclaim <> '6' or (cla.staclaim = '6' and cla.branch = 23))
                                                                                                join	usinsug01.claim_his clh
                                                                                                                on		clh.claim = cla.claim
                                                                                                                and		trim(clh.oper_type) in 
                                                                                                                                (select cast(operation as varchar(2)) from usinsug01.tab_cl_ope where reserve = 1 or ajustes = 1)
                                                                                                join	usinsug01.cl_m_cover clm
                                                                                                                on		clm.usercomp = clh.usercomp
                                                                                                                and		clm.company = clh.company
                                                                                                                and		clm.claim = clh.claim
                                                                                                                and		clm.movement = clh.transac
                                                                                                                and		clm.compdate between dat.par_ini and dat.par_fin) dat
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
                      'SBHISPR' TABLAIFRS17,
                      '' PK,
                      '' DTPREG, --excluido
                      '' TIOCPROC,
                      cast(cl0.xxxxdate as varchar) TIOCFRM,
                      '' TIOCTO, --excluido
                      'PIV' KGIORIGM, --excluido
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
                      'LPV' DCOMPA,
                      '' DMARCA, --excluido
                      '' KSBSUBSN, --excluido
                      '' KSCMTDPR, --excluido
                      coalesce((	select	evi.scod_vt
                                              from	usinsug01.equi_vt_inx evi 
                                              where	evi.scod_inx = (select bene_code from usinsuv01.claimbenef where ctid = cl0.clb_id)),
                              (select bene_code from usinsuv01.claimbenef where ctid = cl0.clb_id)) KEBENTID_SN,
                      '' DENTIDSO, --excluido
                      '' DIDADELES, --excluido
                      '' TCRIACAO --excluido
                      from	(	select	cla.cla_id,
                                                                clh.ctid clh_id,
                                                                coalesce(	case    when    coalesce((  select  distinct pro.brancht
                                                                                                                                                from    usinsuv01.product pro
                                                                                                                                                where	pro.usercomp = cla.usercomp
                                                                                                                                                and		pro.company = cla.company
                                                                                                                                                and		pro.branch = cla.branch
                                                                                                                                                and		pro.product = cla.pol_product
                                                                                                                                                and		pro.effecdate <= cla.occurdat
                                                                                                                                                and		(pro.nulldate is null or pro.nulldate > cla.occurdat)),'0') not in ('1','5')
                                                                                                        then	coalesce((	select  gco.covergen || '-' || gco.currency || '-' || 'g'
                                                                                                                                                from    usinsuv01.gen_cover gco
                                                                                                                                                where	gco.ctid =
                                                                                                                                coalesce(
                                                                                                                                                (   select  max(ctid)
                                                                                                                from	usinsuv01.gen_cover gco
                                                                                                                where	gco.usercomp = cla.usercomp and gco.company = cla.company and gco.branch = cla.branch and gco.product = cla.pol_product and gco.currency = cla.moneda_cod --�ndice regular
                                                                                                                and     gco.modulec = cla.cla_modulec and gco.cover = clm.cover --variaci�n 1 con modulec
                                                                                                                and     gco.effecdate <= cla.occurdat and (gco.nulldate is null or gco.nulldate > cla.occurdat) --variaci�n 2 vigencias
                                                                                                                and     gco.statregt <> '4'), --variaci�n 3 reg. v�lido
                                                                                                                (   select  max(ctid)
                                                                                                                from	usinsuv01.gen_cover gco
                                                                                                                where	gco.usercomp = cla.usercomp and gco.company = cla.company and gco.branch = cla.branch and gco.product = cla.pol_product and gco.currency = cla.moneda_cod --�ndice regular
                                                                                                                and     gco.cover = clm.cover --variaci�n 1 sin modulec
                                                                                                                and     gco.effecdate <= cla.occurdat and (gco.nulldate is null or gco.nulldate > cla.occurdat) --variaci�n 2 vigencias
                                                                                                                and     gco.statregt <> '4'), --variaci�n 3 reg. v�lido
                                                                                                                        (	select  max(ctid)
                                                                                                                from	usinsuv01.gen_cover gco
                                                                                                                where	gco.usercomp = cla.usercomp and gco.company = cla.company and gco.branch = cla.branch and gco.product = cla.pol_product and gco.currency = cla.moneda_cod --�ndice regular
                                                                                                                and     gco.modulec = cla.cla_modulec and gco.cover = clm.cover
                                                                                                                and     gco.effecdate <= cla.occurdat and (gco.nulldate is null or gco.nulldate > cla.occurdat)
                                                                                                                and     gco.statregt = '4'),
                                                                                                                (	select  max(ctid)
                                                                                                                from	usinsuv01.gen_cover gco
                                                                                                                where	gco.usercomp = cla.usercomp and gco.company = cla.company and gco.branch = cla.branch and gco.product = cla.pol_product and gco.currency = cla.moneda_cod --�ndice regular
                                                                                                                and     gco.cover = clm.cover
                                                                                                                and     gco.effecdate <= cla.occurdat and (gco.nulldate is null or gco.nulldate > cla.occurdat)
                                                                                                                and     gco.statregt = '4'), --no est� cortado
                                                                                                                                        (	select  max(ctid)
                                                                                                                from	usinsuv01.gen_cover gco
                                                                                                                where	gco.usercomp = cla.usercomp and gco.company = cla.company and gco.branch = cla.branch and gco.product = cla.pol_product and gco.currency = cla.moneda_cod --�ndice regular
                                                                                                                and     gco.modulec = cla.cla_modulec and gco.cover = clm.cover
                                                                                                                and     gco.effecdate <= cla.occurdat and gco.nulldate <= cla.occurdat
                                                                                                                and     gco.statregt <> '4'),
                                                                                                                (   select  max(ctid)
                                                                                                                from	usinsuv01.gen_cover gco
                                                                                                                where	gco.usercomp = cla.usercomp and gco.company = cla.company and gco.branch = cla.branch and gco.product = cla.pol_product and gco.currency = cla.moneda_cod --�ndice regular
                                                                                                                and     gco.cover = clm.cover
                                                                                                                and     gco.effecdate <= cla.occurdat and gco.nulldate <= cla.occurdat
                                                                                                                and     gco.statregt <> '4'), -- no est� cortado, pero fue anulado antes del efecto del registro
                                                                                                                (   select  max(ctid)
                                                                                                                        from	usinsuv01.gen_cover gco
                                                                                                                        where	gco.usercomp = cla.usercomp and gco.company = cla.company and gco.branch = cla.branch and gco.product = cla.pol_product and gco.currency = cla.moneda_cod --�ndice regular
                                                                                                                        and     gco.modulec = cla.cla_modulec and gco.cover = clm.cover
                                                                                                                        and     gco.effecdate <= cla.occurdat and gco.nulldate <= cla.occurdat
                                                                                                                        and     gco.statregt = '4'),
                                                                                                                (   select  max(ctid)
                                                                                                                        from	usinsuv01.gen_cover gco
                                                                                                                        where	gco.usercomp = cla.usercomp and gco.company = cla.company and gco.branch = cla.branch and gco.product = cla.pol_product and gco.currency = cla.moneda_cod --�ndice regular
                                                                                                                        and     gco.cover = clm.cover
                                                                                                                        and     gco.effecdate <= cla.occurdat and gco.nulldate <= cla.occurdat
                                                                                                                        and     statregt = '4'), --no est� cortado y aparte fue anulado antes del efecto del registro
                                                                                                                                        (	select  max(ctid)
                                                                                                                        from	usinsuv01.gen_cover gco
                                                                                                                        where	gco.usercomp = cla.usercomp and gco.company = cla.company and gco.branch = cla.branch and gco.product = cla.pol_product and gco.currency = cla.moneda_cod --�ndice regular
                                                                                                                        and     gco.modulec = cla.cla_modulec and gco.cover = clm.cover
                                                                                                                        and     gco.effecdate > cla.occurdat
                                                                                                                        and     gco.statregt <> '4'),
                                                                                                                        (   select  max(ctid)
                                                                                                                        from	usinsuv01.gen_cover gco
                                                                                                                        where	gco.usercomp = cla.usercomp and gco.company = cla.company and gco.branch = cla.branch and gco.product = cla.pol_product and gco.currency = cla.moneda_cod --�ndice regular
                                                                                                                        and     gco.cover = clm.cover
                                                                                                                        and     gco.effecdate <= cla.occurdat and gco.nulldate <= cla.occurdat
                                                                                                                        and     gco.statregt <> '4'), -- no est� cortado, pero tampoco al efecto de la tabla de datos particular
                                                                                                                                (	select  max(ctid)
                                                                                                                                from	usinsuv01.gen_cover gco
                                                                                                                                where	gco.usercomp = cla.usercomp and gco.company = cla.company and gco.branch = cla.branch and gco.product = cla.pol_product and gco.currency = cla.moneda_cod --�ndice regular
                                                                                                                                and     gco.modulec = cla.cla_modulec and gco.cover = clm.cover
                                                                                                                                and     gco.effecdate <= cla.occurdat and gco.nulldate <= cla.occurdat
                                                                                                                                and     gco.statregt = '4'),
                                                                                                                        (   select  max(ctid)
                                                                                                                                from	usinsuv01.gen_cover gco
                                                                                                                                where	gco.usercomp = cla.usercomp and gco.company = cla.company and gco.branch = cla.branch and gco.product = cla.pol_product and gco.currency = cla.moneda_cod --�ndice regular
                                                                                                                                and     gco.cover = clm.cover
                                                                                                                                and     gco.effecdate > cla.occurdat
                                                                                                                                and     gco.statregt = '4'))), --est� cortado y no al efecto de la tabla de datos particular adem�s de estar cortado
                                                                                                                                cast(coalesce(clm.cover,0)* -1 as varchar) || '-' || cla.moneda_cod || '-' || 'g')
                                                                                                        else    coalesce((	select  gco.covergen || '-' || gco.currency || '-' || 'l'
                                                                                                                                                from    usinsuv01.life_cover gco
                                                                                                                                                where	gco.ctid =
                                                                                                                                                                coalesce(
                                                                                                                                                                        (	select  max(ctid)
                                                                                                                                                                                from 	usinsuv01.life_cover
                                                                                                                                                                                where   usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = cla.pol_product and currency = cla.moneda_cod --�ndice regular
                                                                                                                                                                                and		cover = clm.cover
                                                                                                                                                                                and		effecdate <= cla.occurdat and (nulldate is null or nulldate > cla.occurdat)
                                                                                                                                                                                and		statregt <> '4'), --que no est� cortado
                                                                                                                                                                        (   select  max(ctid)
                                                                                                                                                                                from 	usinsuv01.life_cover
                                                                                                                                                                                where   usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = cla.pol_product and currency = cla.moneda_cod --�ndice regular
                                                                                                                                                                                and		cover = clm.cover
                                                                                                                                                                                and		effecdate <= cla.occurdat and (nulldate is null or nulldate > cla.occurdat)
                                                                                                                                                                                and		statregt = '4'),--est� cortado
                                                                                                                                                                        (	select  max(ctid)
                                                                                                                                                                                from 	usinsuv01.life_cover
                                                                                                                                                                                where   usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = cla.pol_product and currency = cla.moneda_cod --�ndice regular
                                                                                                                                                                                and		cover = clm.cover
                                                                                                                                                                                and		effecdate <= cla.occurdat and nulldate <= cla.occurdat
                                                                                                                                                                                and		statregt <> '4'),--no est� cortado pero fue anulado antes del efecto del registro
                                                                                                                                                                        (   select  max(ctid)
                                                                                                                                                                                from 	usinsuv01.life_cover
                                                                                                                                                                                where   usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = cla.pol_product and currency = cla.moneda_cod --�ndice regular
                                                                                                                                                                                and		cover = clm.cover
                                                                                                                                                                                and		effecdate <= cla.occurdat and nulldate <= cla.occurdat
                                                                                                                                                                                and		statregt = '4'), --est� cortado y aparte fue anulado antes del efecto del registro
                                                                                                                                                                        (	select  max(ctid)
                                                                                                                                                                                from 	usinsuv01.life_cover
                                                                                                                                                                                where   usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = cla.pol_product and currency = cla.moneda_cod --�ndice regular
                                                                                                                                                                                and		cover = clm.cover
                                                                                                                                                                                and		effecdate > cla.occurdat
                                                                                                                                                                                and		statregt <> '4'),
                                                                                                                                                                        (   select  max(ctid)
                                                                                                                                                                                from 	usinsuv01.life_cover
                                                                                                                                                                                where   usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = cla.pol_product and currency = cla.moneda_cod --�ndice regular
                                                                                                                                                                                and		cover = clm.cover
                                                                                                                                                                                and		effecdate > cla.occurdat --est� cortado pero no al efecto de la tabla de datos particular
                                                                                                                                                                        and		statregt = '4'))),
                                                                                                                        cast(coalesce(clm.cover,0)* -1 as varchar) || '-' || cla.moneda_cod || '-' || 'l') end, 
                                                                                                cast(coalesce(clm.cover,0)* -1 as varchar) || '-' || cla.moneda_cod || '-' ||
                                                                                        case    when    coalesce((  select  distinct pro.brancht --, select * from usinsug01.table37
                                                                                                                                                from    usinsuv01.product pro
                                                                                                                                                where	pro.usercomp = cla.usercomp
                                                                                                                                                and		pro.company = cla.company
                                                                                                                                                and		pro.branch = cla.branch
                                                                                                                                                and		pro.product = cla.pol_product
                                                                                                                                                and		pro.effecdate <= cla.occurdat
                                                                                                                                                and		(pro.nulldate is null or pro.nulldate > cla.occurdat)),'0') not in ('1','5')
                                                                                                        then	'g'
                                                                                                        else	'l' end) cover,
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
                                                                                                                                                                                from	usinsuv01.claim_his clh0
                                                                                                                                                                                where	clh0.claim = clh.claim
                                                                                                                                                                                and		clh0.transac =
                                                                                                                                                                                                (	select	max(clh1.transac)
                                                                                                                                                                                                        from	usinsuv01.claim_his clh1
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
                                                                                                                                                                                from	usinsuv01.claim_his clh0
                                                                                                                                                                                where	clh0.claim = clh.claim
                                                                                                                                                                                and		clh0.transac =
                                                                                                                                                                                                (	select	max(clh1.transac)
                                                                                                                                                                                                        from	usinsuv01.claim_his clh1
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
                                                                                                        from	usinsuv01.curren_pol cpl
                                                                                                        where 	cpl.usercomp = cla.usercomp and cpl.company = cla.company and cpl.certype = pol.certype
                                                                                                        and		cpl.branch = cla.branch and cpl.policy = cla.policy and	cpl.certif = cla.certif),
                                                                                                (	select	max(cpl.currency)
                                                                                                        from	usinsuv01.curren_pol cpl
                                                                                                        where 	cpl.usercomp = cla.usercomp and cpl.company = cla.company and cpl.certype = pol.certype
                                                                                                        and		cpl.branch = cla.branch and cpl.policy = cla.policy),0) moneda_cod,
                                                                                        coalesce(
                                                                                                (	select  min(ctid)
                                                                                                        from    usinsuv01.claimbenef
                                                                                                        where   usercomp = cla.usercomp
                                                                                                        and     company = cla.company
                                                                                                        and     claim = cla.claim
                                                                                                        and     bene_type = 2),
                                                                                                (	select  min(ctid)
                                                                                                        from    usinsuv01.claimbenef
                                                                                                        where   usercomp = cla.usercomp
                                                                                                        and     company = cla.company
                                                                                                        and     claim = cla.claim
                                                                                                        and     bene_type = 8),
                                                                                                (	select  min(ctid)
                                                                                                        from    usinsuv01.claimbenef
                                                                                                        where   usercomp = cla.usercomp
                                                                                                        and     company = cla.company
                                                                                                        and     claim = cla.claim
                                                                                                        and     bene_type = 19),
                                                                                                (	select  min(ctid)
                                                                                                        from    usinsuv01.claimbenef
                                                                                                        where   usercomp = cla.usercomp
                                                                                                        and     company = cla.company
                                                                                                        and     claim = cla.claim
                                                                                                        and     bene_type = 18)) clb_id,
                                                                                        coalesce((	select	distinct(coalesce(modulec,0))
                                                                                                                from	usinsuv01.modules
                                                                                                                where	usercomp = cla.usercomp
                                                                                                                and		company = cla.company
                                                                                                                and		certype = pol.certype
                                                                                                                and		branch = cla.branch
                                                                                                                and		policy = cla.policy
                                                                                                                and		certif = cla.certif
                                                                                                                and		effecdate <= cla.occurdat
                                                                                                                and		(nulldate is null or nulldate > cla.occurdat)),0) cla_modulec,
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
                                                                                                                (select cast(operation as varchar(2)) from usinsug01.tab_cl_ope where reserve = 1 or ajustes = 1)) xxxxdate, --se busca la mayor fecha a la nueva fecha de quiebre
                                                                                        dat.par_fin dat_lim
                                                                        from 	(	--revisi�n fechas transacciones
                                                                                                select	cla.ctid cla_id,
                                                                                                                dat.par_fin
                                                                                                from	(	select	cast('{p_fecha_inicio}' as date) par_ini, --ejecutar al a�o 2021 (2019 es para pruebas)
                                                                                                                        cast('{p_fecha_fin}' as date) par_fin) dat --limitador para la carga inicial (fecha l�mite a�o 2023, 2022 para prueba)
                                                                                                join	usinsuv01.claim cla
                                                                                                                on		cla.usercomp = 1
                                                                                                                and 	cla.company = 1
                                                                                                                and		cla.branch in (select branch from usinsug01.table10b where company = 2)
                                                                                                                and		cla.staclaim <> '6'
                                                                                                join	usinsuv01.claim_his clh
                                                                                                                on		clh.claim = cla.claim
                                                                                                                and		trim(clh.oper_type) in 
                                                                                                                                (select cast(operation as varchar(2)) from usinsug01.tab_cl_ope where reserve = 1 or ajustes = 1)
                                                                                                                and     (clh.operdate between dat.par_ini and dat.par_fin --se consideran siniestros con operaciones entre los rangos (limitaci�n carga inicial)
                                                                                                                                or clh.compdate between dat.par_ini and dat.par_fin)
                                                                                                union
                                                                                                --revisi�n fechas coberturas asociadas a transacciones
                                                                                                select	cla.ctid cla_id,
                                                                                                                dat.par_fin
                                                                                                from	(	select	cast('{p_fecha_inicio}' as date) par_ini, --ejecutar al a�o 2021 (2019 es para pruebas)
                                                                                                                        cast('{p_fecha_fin}' as date) par_fin) dat --limitador para la carga inicial (fecha l�mite a�o 2023, 2022 para prueba)
                                                                                                join	usinsuv01.claim cla
                                                                                                                on		cla.usercomp = 1
                                                                                                                and 	cla.company = 1
                                                                                                                and		cla.branch in (select branch from usinsug01.table10b where company = 2)
                                                                                                                and		cla.staclaim <> '6'
                                                                                                join	usinsuv01.claim_his clh
                                                                                                                on		clh.claim = cla.claim
                                                                                                                and		trim(clh.oper_type) in 
                                                                                                                                (select cast(operation as varchar(2)) from usinsug01.tab_cl_ope where reserve = 1 or ajustes = 1)
                                                                                                join	usinsuv01.cl_m_cover clm
                                                                                                                on		clm.usercomp = clh.usercomp
                                                                                                                and		clm.company = clh.company
                                                                                                                and		clm.claim = clh.claim
                                                                                                                and		clm.movement = clh.transac
                                                                                                                and		clm.compdate between dat.par_ini and dat.par_fin) dat
                                                                        join	usinsuv01.claim cla on cla.ctid = dat.cla_id
                                                                        join	usinsuv01.policy pol
                                                                                        on		pol.usercomp = cla.usercomp
                                                                                        and 	pol.company = cla.company
                                                                                        and 	pol.certype = '2'
                                                                                        and 	pol.branch = cla.branch
                                                                                        and 	pol.policy = cla.policy) cla
                                                join 	usinsuv01.claim_his clh
                                                                on		clh.claim = cla.claim
                                                                and		trim(clh.oper_type) in (select cast(operation as varchar(2)) from usinsug01.tab_cl_ope where reserve = 1 or ajustes = 1)
                                                                and		clh.operdate <= cla.dat_lim --en caso de ejecutar a una fecha posterior, alterar esta fecha a la de la ejecuci�n
                                                join 	usinsuv01.cl_m_cover clm
                                                                on		clm.usercomp = clh.usercomp
                                                                and		clm.company = clh.company
                                                                and		clm.claim = clh.claim
                                                                and 	clm.movement = clh.transac
                                                group	by 1,2,3) cl0
                      join 	usinsuv01.claim cla on cla.ctid = cl0.cla_id
                      join	usinsuv01.claim_his clh on clh.ctid = cl0.clh_id
                      where	(cl0.reserva <> 0 or cl0.ajustes <> 0)
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
                                                                join	(	select	cast('{p_fecha_inicio}' as date) par_ini, --ejecutar al a�o 2021 (2019 es para pruebas)
                                                                                        cast('{p_fecha_fin}' as date) par_fin --limitador para la carga inicial (fecha l�mite a�o 2023, 2022 para prueba)
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
                                                                                                                                        (select	cast("SVALUE" as INT4) from	usvtimg01."CONDITION_SERV" cs  where "NCONDITION" in (71,72))))) cla
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
                                                                        and		clh."NOPER_TYPE" in (select	cast("SVALUE" as INT4) from usvtimv01."CONDITION_SERV" where "NCONDITION" in (71,72))
                                                                        and		(clh."DOPERDATE" <= dat.par_fin or clh."DCOMPDATE" <= dat.par_fin) --evita tomar operaciones posteriores (carga incremental)
                                                                        ) xxxxdate,
                                                                dat.par_fin dat_lim
                                                from    (	select	ctid pol_id
                                                                        from	usvtimv01."POLICY"
                                                                        where	"SCERTYPE" = '2') po0
                                                join	(	select	cast('{p_fecha_inicio}' as date) par_ini, --ejecutar al a�o 2021 (2015 es para pruebas)
                                                                        cast('{p_fecha_fin}' as date) par_fin --limitador para la carga inicial (fecha l�mite a�o 2023, 2017 para prueba)
                                                                ) dat on 1 = 1
                                                join	usvtimv01."POLICY" pol on pol.ctid = po0.pol_id
                                                join	usvtimv01."CLAIM" cla
                                                        on	cla."SCERTYPE" = pol."SCERTYPE"
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
                                                                                                and		clm."NCASE_NUM" = clH."NCASE_NUM"
                                                                                                and		clm."NDEMAN_TYPE" = clh."NDEMAN_TYPE"
                                                                                                and		clm."NTRANSAC" = clh."NTRANSAC"
                                                                                                and		cast(clm."DCOMPDATE" as date) between dat.par_ini and dat.par_fin --se consideran coberturas con modificaciones entre los rangos (limitaci�n carga inicial)
                                                                                where	clh."NCLAIM" = cla."NCLAIM"
                                                                                and		coalesce(clh."NCASE_NUM",0) >= 0
                                                                                                        and		clh."NOPER_TYPE" in 
                                                                                                                        (select	cast("SVALUE" as INT4) from	usvtimv01."CONDITION_SERV" cs  where "NCONDITION" in (71,72))))) cla
                        join	usvtimv01."CL_M_COVER" clm on clm."NCLAIM" = cla.nclaim
                        join	usvtimv01."CLAIM_HIS" clh
                                        on	clh."NCLAIM" = clm."NCLAIM"
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
                   where (cl0.reserva <> 0 or cl0.ajustes <> 0)
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
                    from (	select	cla."CLAIM_ID" nclaim,
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
                        join	(	select	cast('{p_fecha_inicio}' as date) par_ini, --ejecutar al a�o 2021 (2019 es para pruebas)
                                                cast('{p_fecha_fin}' as date) par_fin) dat --limitador para la carga inicial (fecha l�mite a�o 2023, 2022 para prueba)
                                        ) dat on 1 = 1
                        join	usinsiv01."POLICY" pol
                                        on 	pol."POLICY_ID" = cla."POLICY_ID" 
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
                                                                or cast(clh."CHANGE_DATE" as date) between dat.par_ini and dat.par_fin))) cla --se consideran siniestros con modificaciones en el nuevo period
                    join	usinsiv01."CLAIM_OBJECTS" clo
                    on		clo."CLAIM_ID" = cla.nclaim
                    and		clo."CLAIM_STATE" <> -1
                    join	usinsiv01."CLAIM_RESERVE_HISTORY" clh
                    on		clh."CLAIM_ID" = clo."CLAIM_ID"
                    and		clh."CLAIM_OBJECT_SEQ" = clo."CLAIM_OBJ_SEQ"
                    and		clh."REQUEST_ID" = clo."REQUEST_ID"
                    and		clh."OP_TYPE" IN ('REG','EST','CLC')
                    and		cast(clh."REGISTRATION_DATE" as date) <= cla.dat_lim --en caso de ejecutar a una fecha posterior, alterar esta fecha a la de la ejecuci�n
                    and		coalesce("RESERV_CHANGE",0) <> 0
                   ) AS TMP
                '''

    DF_INSIS_LPV = glue_context.read.format('jdbc').options(**connection).option("dbtable",INSIS_LPV).load()
        
    L_DF_SBHISPR = DF_INSUNIX_LPG.union(DF_INSUNIX_LPV).union(DF_VTIME_LPG).union(DF_VTIME_LPV).union(DF_INSIS_LPV)

    return L_DF_SBHISPR 