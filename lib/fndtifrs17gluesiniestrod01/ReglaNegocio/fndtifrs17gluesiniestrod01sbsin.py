def get_data(glue_context, connection):

    L_SBSIN_INSUNIX_LPG_SOAT  =  '''
                                    (
                                        select	        'D' INDDETREC,
                                                        'SBSIN' TABLAIFRS17,
                                                        '' PK,
                                                        '' DTPREG, --excluido
                                                        '' TIOCPROC, --no disponible
                                                        coalesce(cast(cla.compdate as varchar),'') TIOCFRM,
                                                        '' TIOCTO, --excluido
                                                        'PIG' KGIORIGM,
                                                        'LPG' KSCCOMPA,
                                                        coalesce(cast(cla.branch as varchar),'') KGCRAMO,
                                                        cla.branch || '-' || pol.product || '-' ||
                                                                coalesce((	select	sub_product
                                                                                        from	usinsug01.pol_subproduct
                                                                                        where	usercomp = cla.usercomp
                                                                                        and 	company = cla.company
                                                                                        and		certype = pol.certype
                                                                                        and		branch = cla.branch
                                                                                        and		policy = cla.policy
                                                                                        and		product = pol.product),0) KABPRODT,
                                                        coalesce(cast(cla.policy as varchar),'') DNUMAPO,
                                                        coalesce(cast(cla.certif as varchar),'') DNMCERT,
                                                        coalesce(cast(cla.claim as varchar),'') DNUMSIN,
                                                        coalesce(cast(cla.occurdat as varchar),'') TOCURSIN,
                                                        '' DHSINIST, --excluido
                                                        coalesce(cast(cla.occurdat as varchar),'') TABERSIN,
                                                        '' TPARTSIN, --excluido
                                                        coalesce(
                                                                case	when	cla.staclaim in ('1','5','7')
                                                                                then	(	select	cast(max(operdate) as varchar)
                                                                                                        from	usinsug01.claim_his
                                                                                                        where	claim = cla.claim
                                                                                                        and		case	when	cla.staclaim in ('1') then oper_type in ('3','17')
                                                                                                                                        when	cla.staclaim in ('5') then oper_type in ('11')
                                                                                                                                        when	cla.staclaim in ('7') then oper_type in ('21')
                                                                                                                                        else	oper_type is null end)
                                                                                else 	'' end, '') TFECHTEC,
                                                        coalesce(
                                                                case	when	cla.staclaim in ('1','5','7')
                                                                                then	(	select	cast(max(operdate) as varchar)
                                                                                                        from	usinsug01.claim_his
                                                                                                        where	claim = cla.claim
                                                                                                        and		case	when	cla.staclaim in ('1') then oper_type in ('3','17')
                                                                                                                                        when	cla.staclaim in ('5') then oper_type in ('52')
                                                                                                                                        when	cla.staclaim in ('7') then oper_type in ('21')
                                                                                                                                        else	oper_type is null end)
                                                                                else 	'' end, '') TFECHADM,
                                                        coalesce(
                                                                (	select	cast(max(operdate) as varchar)
                                                                        from	usinsug01.claim_his
                                                                        where	claim = cla.claim
                                                                        and		oper_type in
                                                                                        (	select	cast(codigint as varchar(2))
                                                                                                from	usinsug01.table140
                                                                                                where	(	codigint in 
                                                                                                                        (	select	operation
                                                                                                                                from	usinsug01.tab_cl_ope
                                                                                                                                where	(reserve = 1 or ajustes = 1 or pay_amount in (1,3)))) 
                                                                                                                or codigint in (60))), '') TESTADO,
                                                        coalesce(cast(cla.staclaim as varchar),'') KSCSITUA,
                                                        '' KSCMOTSI, --excluido
                                                        '' KSCTPSIN, --no disponible
                                                        coalesce(cast(cla.causecod as varchar),'') KSCCAUSA,
                                                        '' KSCARGES, --excluido
                                                        '' KSCFMPGS, --no disponible
                                                        '' KCBMED_DRA, --excluido
                                                        '' KCBMED_PG, --excluido
                                                        '' KCBMED_PD, --excluido
                                                        '' KCBMED_P2, --excluido
                                                        case	when pol.bussityp = '1' and cl0.share_coa = 100 then '1' --Sin coaseguro
                                                                        when pol.bussityp = '1' and cl0.share_coa <> 100 then '2' --Con coaseguro, compa��a l�der
                                                                        when pol.bussityp = '2' then '3' --Con coaseguro, compa��a no l�der
                                                                        else '0' end KSCTPCSG,
                                                        cast(coalesce(
                                                                        coalesce((	select	max(cpl.currency)
                                                                                                from	usinsug01.curren_pol cpl
                                                                                                where 	cpl.usercomp = cla.usercomp
                                                                                                and 	cpl.company = cla.company
                                                                                                and		cpl.certype = pol.certype
                                                                                                and		cpl.branch = cla.branch
                                                                                                and 	cpl.policy = cla.policy
                                                                                                and		cpl.certif = cla.certif),
                                                                                        (	select	max(cpl.currency)
                                                                                                from	usinsug01.curren_pol cpl
                                                                                                where 	cpl.usercomp = cla.usercomp
                                                                                                and 	cpl.company = cla.company
                                                                                                and		cpl.certype = pol.certype
                                                                                                and		cpl.branch = cla.branch
                                                                                                and 	cpl.policy = cla.policy)),0) as varchar) KSCMOEDA,
                                                        '' VCAMBIO, --no disponible
                                                        '' VTXRESPN, --no disponible
                                                        cast(cl0.reserva + cl0.ajustes as numeric(14,2)) VMTPROVI,
                                                        '' VMTPRVINI, --excluido
                                                        cast((cl0.reserva + cl0.ajustes)
                                                                * (case when pol.bussityp = '2' then 100 else share_coa end/100)
                                                * (share_rea/100) as numeric(14,2)) VMTPRVRS,
                                                        cast((cl0.reserva + cl0.ajustes)
                                                                        * (case when pol.bussityp = '2' then 100 else share_coa end/100) as numeric(14,2)) VMTCOSEG,
                                                        '' KSCNATUR, --excluido
                                                        '' TALTENAT, --excluido
                                                        '' DUSRREG, --excluido
                                                        coalesce(coalesce(
                                                                                (	select	max(sclient_vig)
                                                                                        from	usinsug01.wbstblclidepequi
                                                                                        where	sclient_old = 
                                                                                                        (	select	evi.scod_vt
                                                                                                                from	usinsug01.equi_vt_inx evi 
                                                                                                                where	evi.scod_inx = cla.client)),
                                                                                (	select	evi.scod_vt
                                                                                        from	usinsug01.equi_vt_inx evi 
                                                                                        where	evi.scod_inx = cla.client)),
                                                                cla.client) KEBENTID_TO,
                                                        cast(coalesce((	select  case	when	max(gco.addsuini) = '3'
                                                                                                                        then	max(case when gco.addsuini = '3' then coalesce(cov.capital,0) else 0 end)
                                                                                                                        else	sum(case when gco.addsuini = '1' then coalesce(cov.capital,0) else 0 end) end
                                                                                        from    usinsug01.cover cov
                                                                                        join	usinsug01.gen_cover gco on gco.ctid =
                                                                        coalesce(coalesce(coalesce(coalesce(
                                                                                                                                        (   select  max(ctid)
                                                                                                        from	usinsug01.gen_cover
                                                                                                        where	usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = pol.product and currency = cov.currency --�ndice regular
                                                                                                        and     modulec = cov.modulec and cover = cov.cover --variaci�n 1 con modulec
                                                                                                        and     effecdate <= cla.occurdat and (nulldate is null or nulldate > cla.occurdat) --variaci�n 2 vigencias
                                                                                                        and     statregt <> '4' and addsuini in ('1','3')), --variaci�n 3 reg. v�lido
                                                                                                        (   select  max(ctid)
                                                                                                        from	usinsug01.gen_cover
                                                                                                        where	usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = pol.product and currency = cov.currency --�ndice regular
                                                                                                        and     cover = cov.cover --variaci�n 1 sin modulec
                                                                                                        and     effecdate <= cla.occurdat and (nulldate is null or nulldate > cla.occurdat) --variaci�n 2 vigencias
                                                                                                        and     statregt <> '4' and addsuini in ('1','3'))), --variaci�n 3 reg. v�lido
                                                                                                coalesce((	select  max(ctid)
                                                                                                        from	usinsug01.gen_cover
                                                                                                        where	usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = pol.product and currency = cov.currency --�ndice regular
                                                                                                        and     modulec = cov.modulec and cover = cov.cover
                                                                                                        and     effecdate <= cla.occurdat and (nulldate is null or nulldate > cla.occurdat)
                                                                                                        and     statregt = '4' and addsuini in ('1','3')),
                                                                                                        (	select  max(ctid)
                                                                                                        from	usinsug01.gen_cover
                                                                                                        where	usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = pol.product and currency = cov.currency --�ndice regular
                                                                                                        and     cover = cov.cover
                                                                                                        and     effecdate <= cla.occurdat and (nulldate is null or nulldate > cla.occurdat)
                                                                                                        and     statregt = '4' and addsuini in ('1','3')))), --no est� cortado
                                                                                        coalesce(coalesce((	select  max(ctid)
                                                                                                        from	usinsug01.gen_cover
                                                                                                        where	usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = pol.product and currency = cov.currency --�ndice regular
                                                                                                        and     modulec = cov.modulec and cover = cov.cover
                                                                                                        and     effecdate <= cla.occurdat and nulldate <= cla.occurdat
                                                                                                        and     statregt <> '4' and addsuini in ('1','3')),
                                                                                                        (   select  max(ctid)
                                                                                                        from	usinsug01.gen_cover
                                                                                                        where	usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = pol.product and currency = cov.currency --�ndice regular
                                                                                                        and     cover = cov.cover
                                                                                                        and     effecdate <= cla.occurdat and nulldate <= cla.occurdat
                                                                                                        and     statregt <> '4' and addsuini in ('1','3'))), -- no est� cortado, pero fue anulado antes del efecto del registro
                                                                                        coalesce((	select  max(ctid)
                                                                                                        from	usinsug01.gen_cover
                                                                                                        where	usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = pol.product and currency = cov.currency --�ndice regular
                                                                                                        and     modulec = cov.modulec and cover = cov.cover
                                                                                                        and     effecdate <= cla.occurdat and nulldate <= cla.occurdat
                                                                                                        and     statregt = '4' and addsuini in ('1','3')),
                                                                                                (   select  max(ctid)
                                                                                                        from	usinsug01.gen_cover
                                                                                                        where 	usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = pol.product and currency = cov.currency --�ndice regular
                                                                                                        and     cover = cov.cover
                                                                                                        and     effecdate <= cla.occurdat and nulldate <= cla.occurdat
                                                                                                        and     statregt = '4' and addsuini in ('1','3'))))), --no est� cortado y aparte fue anulado antes del efecto del registro
                                                                                coalesce(coalesce((	select  max(ctid)
                                                                                                from	usinsug01.gen_cover
                                                                                                where 	usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = pol.product and currency = cov.currency --�ndice regular
                                                                                                and     modulec = cov.modulec and cover = cov.cover
                                                                                                and     effecdate > cla.occurdat
                                                                                                and     statregt <> '4' and addsuini in ('1','3')),
                                                                                                (   select  max(ctid)
                                                                                                from	usinsug01.gen_cover
                                                                                                where	usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = pol.product and currency = cov.currency --�ndice regular
                                                                                                and     cover = cov.cover
                                                                                                and     effecdate <= cla.occurdat and nulldate <= cla.occurdat
                                                                                                and     statregt <> '4' and addsuini in ('1','3'))), -- no est� cortado, pero tampoco al efecto de la tabla de datos particular
                                                                                coalesce((	select  max(ctid)
                                                                                                from	usinsug01.gen_cover
                                                                                                where	usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = pol.product and currency = cov.currency --�ndice regular
                                                                                                and     modulec = cov.modulec and cover = cov.cover
                                                                                                and     effecdate <= cla.occurdat and nulldate <= cla.occurdat
                                                                                                and     statregt = '4' and addsuini in ('1','3')),
                                                                                        (   select  max(ctid)
                                                                                                from	usinsug01.gen_cover
                                                                                                where	usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = pol.product and currency = cov.currency --�ndice regular
                                                                                                and     cover = cov.cover
                                                                                                and     effecdate > cla.occurdat
                                                                                                and     statregt = '4' and addsuini in ('1','3'))))) --est� cortado y no al efecto de la tabla de datos particular adem�s de estar cortado
                                                                                        where   cov.usercomp = cla.usercomp
                                                                                        and     cov.company = cla.company
                                                                                        and     cov.certype = pol.certype
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
                                                                                                else	1 end as numeric(14,2)) VCAPITAL,
                                                        '' VTXINDEM, --no disponible
                                                        '' VMTINDEM, --no disponible
                                                        '' TINICIND, --no disponible
                                                        '' DULTACTA, --excluido
                                                        coalesce(	case	trim((select distinct(lower(tabname)) from usinsug01.tab_name_b where branch = cla.branch))
                                                                                                when 	'accident'
                                                                                                then 	(	select	statusva
                                                                                                                        from	usinsug01.accident
                                                                                                                        where	ctid = 
                                                                                                                        (	coalesce(coalesce(coalesce(coalesce(coalesce(
                                                                                                                        (   select max(ctid) from usinsug01.accident
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate <= cla.occurdat
                                                                                                                                and (nulldate is null or nulldate > cla.occurdat)
                                                                                                                                and statusva not in ('2','3')),
                                                                                                                        (   select max(ctid) from usinsug01.accident
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate <= cla.occurdat
                                                                                                                                and (nulldate is null or nulldate >= cla.occurdat)
                                                                                                                                and statusva not in ('2','3'))),
                                                                                                                        (   select max(ctid) from usinsug01.accident
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate <= cla.occurdat
                                                                                                                                and (nulldate is null or nulldate < cla.occurdat)
                                                                                                                                and statusva not in ('2','3'))),
                                                                                                                        (   select min(ctid) from usinsug01.accident
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate > cla.occurdat
                                                                                                                                and (nulldate is null or nulldate > cla.occurdat)
                                                                                                                                and statusva not in ('2','3'))),
                                                                                                                        (   select min(ctid) from usinsug01.accident
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif
                                                                                                                                and statusva not in ('2','3'))),
                                                                                                                        (   select min(ctid) from usinsug01.accident
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif
                                                                                                                                and statusva in ('2','3')))))
                                                                                                when 	'auto_peru'
                                                                                                then 	(	select	statusva
                                                                                                                        from	usinsug01.auto_peru
                                                                                                                        where	ctid = 
                                                                                                                                        (	coalesce(coalesce(coalesce(coalesce(coalesce(
                                                                                                                        (   select max(ctid) from usinsug01.auto_peru
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate <= cla.occurdat
                                                                                                                                and (nulldate is null or nulldate > cla.occurdat)
                                                                                                                                and statusva not in ('2','3')),
                                                                                                                        (   select max(ctid) from usinsug01.auto_peru
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate <= cla.occurdat
                                                                                                                                and (nulldate is null or nulldate >= cla.occurdat)
                                                                                                                                and statusva not in ('2','3'))),
                                                                                                                        (   select max(ctid) from usinsug01.auto_peru
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate <= cla.occurdat
                                                                                                                                and (nulldate is null or nulldate < cla.occurdat)
                                                                                                                                and statusva not in ('2','3'))),
                                                                                                                        (   select min(ctid) from usinsug01.auto_peru
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate > cla.occurdat
                                                                                                                                and (nulldate is null or nulldate > cla.occurdat)
                                                                                                                                and statusva not in ('2','3'))),
                                                                                                                        (   select min(ctid) from usinsug01.auto_peru
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif
                                                                                                                                and statusva not in ('2','3'))),
                                                                                                                        (   select min(ctid) from usinsug01.auto_peru
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif
                                                                                                                                and statusva in ('2','3')))))
                                                                                                when 	'civil'
                                                                                                then 	(	select	statusva
                                                                                                                        from	usinsug01.civil
                                                                                                                        where	ctid = 
                                                                                                                        (	coalesce(coalesce(coalesce(coalesce(coalesce(
                                                                                                                        (   select max(ctid) from usinsug01.civil
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate <= cla.occurdat
                                                                                                                                and (nulldate is null or nulldate > cla.occurdat)
                                                                                                                                and statusva not in ('2','3')),
                                                                                                                        (   select max(ctid) from usinsug01.civil
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate <= cla.occurdat
                                                                                                                                and (nulldate is null or nulldate >= cla.occurdat)
                                                                                                                                and statusva not in ('2','3'))),
                                                                                                                        (   select max(ctid) from usinsug01.civil
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate <= cla.occurdat
                                                                                                                                and (nulldate is null or nulldate < cla.occurdat)
                                                                                                                                and statusva not in ('2','3'))),
                                                                                                                        (   select min(ctid) from usinsug01.civil
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate > cla.occurdat
                                                                                                                                and (nulldate is null or nulldate > cla.occurdat)
                                                                                                                                and statusva not in ('2','3'))),
                                                                                                                        (   select min(ctid) from usinsug01.civil
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif
                                                                                                                                and statusva not in ('2','3'))),
                                                                                                                        (   select min(ctid) from usinsug01.civil
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif
                                                                                                                                and statusva in ('2','3')))))
                                                                                                when 	'credit'
                                                                                                then 	(	select	statusva
                                                                                                                        from	usinsug01.credit
                                                                                                                        where	ctid = 
                                                                                                                        (	coalesce(coalesce(coalesce(coalesce(coalesce(
                                                                                                                        (   select max(ctid) from usinsug01.credit
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate <= cla.occurdat
                                                                                                                                and (nulldate is null or nulldate > cla.occurdat)
                                                                                                                                and statusva not in ('2','3')),
                                                                                                                        (   select max(ctid) from usinsug01.credit
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate <= cla.occurdat
                                                                                                                                and (nulldate is null or nulldate >= cla.occurdat)
                                                                                                                                and statusva not in ('2','3'))),
                                                                                                                        (   select max(ctid) from usinsug01.credit
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate <= cla.occurdat
                                                                                                                                and (nulldate is null or nulldate < cla.occurdat)
                                                                                                                                and statusva not in ('2','3'))),
                                                                                                                        (   select min(ctid) from usinsug01.credit
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate > cla.occurdat
                                                                                                                                and (nulldate is null or nulldate > cla.occurdat)
                                                                                                                                and statusva not in ('2','3'))),
                                                                                                                        (   select min(ctid) from usinsug01.credit
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif
                                                                                                                                and statusva not in ('2','3'))),
                                                                                                                        (   select min(ctid) from usinsug01.credit
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif
                                                                                                                                and statusva in ('2','3')))))
                                                                                                when 	'deshones'
                                                                                                then 	(	select	statusva
                                                                                                                        from	usinsug01.deshones
                                                                                                                        where	ctid = 
                                                                                                                        (	coalesce(coalesce(coalesce(coalesce(coalesce(
                                                                                                                        (   select max(ctid) from usinsug01.deshones
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate <= cla.occurdat
                                                                                                                                and (nulldate is null or nulldate > cla.occurdat)
                                                                                                                                and statusva not in ('2','3')),
                                                                                                                        (   select max(ctid) from usinsug01.deshones
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate <= cla.occurdat
                                                                                                                                and (nulldate is null or nulldate >= cla.occurdat)
                                                                                                                                and statusva not in ('2','3'))),
                                                                                                                        (   select max(ctid) from usinsug01.deshones
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate <= cla.occurdat
                                                                                                                                and (nulldate is null or nulldate < cla.occurdat)
                                                                                                                                and statusva not in ('2','3'))),
                                                                                                                        (   select min(ctid) from usinsug01.deshones
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate > cla.occurdat
                                                                                                                                and (nulldate is null or nulldate > cla.occurdat)
                                                                                                                                and statusva not in ('2','3'))),
                                                                                                                        (   select min(ctid) from usinsug01.deshones
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif
                                                                                                                                and statusva not in ('2','3'))),
                                                                                                                        (   select min(ctid) from usinsug01.deshones
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif
                                                                                                                                and statusva in ('2','3')))))
                                                                                                when 	'eqele_peru'
                                                                                                then 	(	select	statusva
                                                                                                                        from	usinsug01.eqele_peru
                                                                                                                        where	ctid = 
                                                                                                                        (	coalesce(coalesce(coalesce(coalesce(coalesce(
                                                                                                                        (   select max(ctid) from usinsug01.eqele_peru
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate <= cla.occurdat
                                                                                                                                and (nulldate is null or nulldate > cla.occurdat)
                                                                                                                                and statusva not in ('2','3')),
                                                                                                                        (   select max(ctid) from usinsug01.eqele_peru
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate <= cla.occurdat
                                                                                                                                and (nulldate is null or nulldate >= cla.occurdat)
                                                                                                                                and statusva not in ('2','3'))),
                                                                                                                        (   select max(ctid) from usinsug01.eqele_peru
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate <= cla.occurdat
                                                                                                                                and (nulldate is null or nulldate < cla.occurdat)
                                                                                                                                and statusva not in ('2','3'))),
                                                                                                                        (   select min(ctid) from usinsug01.eqele_peru
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate > cla.occurdat
                                                                                                                                and (nulldate is null or nulldate > cla.occurdat)
                                                                                                                                and statusva not in ('2','3'))),
                                                                                                                        (   select min(ctid) from usinsug01.eqele_peru
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif
                                                                                                                                and statusva not in ('2','3'))),
                                                                                                                        (   select min(ctid) from usinsug01.eqele_peru
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif
                                                                                                                                and statusva in ('2','3')))))
                                                                                                when 	'fire_lc'
                                                                                                then 	(	select	statusva
                                                                                                                        from	usinsug01.fire_lc
                                                                                                                        where	ctid = 
                                                                                                                        (	coalesce(coalesce(coalesce(coalesce(coalesce(
                                                                                                                        (   select max(ctid) from usinsug01.fire_lc
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate <= cla.occurdat
                                                                                                                                and (nulldate is null or nulldate > cla.occurdat)
                                                                                                                                and statusva not in ('2','3')),
                                                                                                                        (   select max(ctid) from usinsug01.fire_lc
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate <= cla.occurdat
                                                                                                                                and (nulldate is null or nulldate >= cla.occurdat)
                                                                                                                                and statusva not in ('2','3'))),
                                                                                                                        (   select max(ctid) from usinsug01.fire_lc
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate <= cla.occurdat
                                                                                                                                and (nulldate is null or nulldate < cla.occurdat)
                                                                                                                                and statusva not in ('2','3'))),
                                                                                                                        (   select min(ctid) from usinsug01.fire_lc
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate > cla.occurdat
                                                                                                                                and (nulldate is null or nulldate > cla.occurdat)
                                                                                                                                and statusva not in ('2','3'))),
                                                                                                                        (   select min(ctid) from usinsug01.fire_lc
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif
                                                                                                                                and statusva not in ('2','3'))),
                                                                                                                        (   select min(ctid) from usinsug01.fire_lc
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif
                                                                                                                                and statusva in ('2','3')))))
                                                                                                when 	'fire_peru'
                                                                                                then 	(	select	statusva
                                                                                                                        from	usinsug01.fire_peru
                                                                                                                        where	ctid = 
                                                                                                                        (	coalesce(coalesce(coalesce(coalesce(coalesce(
                                                                                                                        (   select max(ctid) from usinsug01.fire_peru
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate <= cla.occurdat
                                                                                                                                and (nulldate is null or nulldate > cla.occurdat)
                                                                                                                                and statusva not in ('2','3')),
                                                                                                                        (   select max(ctid) from usinsug01.fire_peru
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate <= cla.occurdat
                                                                                                                                and (nulldate is null or nulldate >= cla.occurdat)
                                                                                                                                and statusva not in ('2','3'))),
                                                                                                                        (   select max(ctid) from usinsug01.fire_peru
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate <= cla.occurdat
                                                                                                                                and (nulldate is null or nulldate < cla.occurdat)
                                                                                                                                and statusva not in ('2','3'))),
                                                                                                                        (   select min(ctid) from usinsug01.fire_peru
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate > cla.occurdat
                                                                                                                                and (nulldate is null or nulldate > cla.occurdat)
                                                                                                                                and statusva not in ('2','3'))),
                                                                                                                        (   select min(ctid) from usinsug01.fire_peru
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif
                                                                                                                                and statusva not in ('2','3'))),
                                                                                                                        (   select min(ctid) from usinsug01.fire_peru
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif
                                                                                                                                and statusva in ('2','3')))))
                                                                                                when 	'health'
                                                                                                then 	(	select	statusva
                                                                                                                        from	usinsug01.health
                                                                                                                        where	ctid = 
                                                                                                                        (	coalesce(coalesce(coalesce(coalesce(coalesce(
                                                                                                                        (   select max(ctid) from usinsug01.health
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate <= cla.occurdat
                                                                                                                                and (nulldate is null or nulldate > cla.occurdat)
                                                                                                                                and statusva not in ('2','3')),
                                                                                                                        (   select max(ctid) from usinsug01.health
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate <= cla.occurdat
                                                                                                                                and (nulldate is null or nulldate >= cla.occurdat)
                                                                                                                                and statusva not in ('2','3'))),
                                                                                                                        (   select max(ctid) from usinsug01.health
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate <= cla.occurdat
                                                                                                                                and (nulldate is null or nulldate < cla.occurdat)
                                                                                                                                and statusva not in ('2','3'))),
                                                                                                                        (   select min(ctid) from usinsug01.health
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate > cla.occurdat
                                                                                                                                and (nulldate is null or nulldate > cla.occurdat)
                                                                                                                                and statusva not in ('2','3'))),
                                                                                                                        (   select min(ctid) from usinsug01.health
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif
                                                                                                                                and statusva not in ('2','3'))),
                                                                                                                        (   select min(ctid) from usinsug01.health
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif
                                                                                                                                and statusva in ('2','3')))))
                                                                                                when 	'machine'
                                                                                                then 	(	select	statusva
                                                                                                                        from	usinsug01.machine
                                                                                                                        where	ctid = 
                                                                                                                        (	coalesce(coalesce(coalesce(coalesce(coalesce(
                                                                                                                        (   select max(ctid) from usinsug01.machine
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate <= cla.occurdat
                                                                                                                                and (nulldate is null or nulldate > cla.occurdat)
                                                                                                                                and statusva not in ('2','3')),
                                                                                                                        (   select max(ctid) from usinsug01.machine
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate <= cla.occurdat
                                                                                                                                and (nulldate is null or nulldate >= cla.occurdat)
                                                                                                                                and statusva not in ('2','3'))),
                                                                                                                        (   select max(ctid) from usinsug01.machine
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate <= cla.occurdat
                                                                                                                                and (nulldate is null or nulldate < cla.occurdat)
                                                                                                                                and statusva not in ('2','3'))),
                                                                                                                        (   select min(ctid) from usinsug01.machine
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate > cla.occurdat
                                                                                                                                and (nulldate is null or nulldate > cla.occurdat)
                                                                                                                                and statusva not in ('2','3'))),
                                                                                                                        (   select min(ctid) from usinsug01.machine
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif
                                                                                                                                and statusva not in ('2','3'))),
                                                                                                                        (   select min(ctid) from usinsug01.machine
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif
                                                                                                                                and statusva in ('2','3')))))
                                                                                                when 	'machine_lc'
                                                                                                then 	(	select	statusva
                                                                                                                        from	usinsug01.machine_lc
                                                                                                                        where	ctid = 
                                                                                                                        (	coalesce(coalesce(coalesce(coalesce(coalesce(
                                                                                                                        (   select max(ctid) from usinsug01.machine_lc
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate <= cla.occurdat
                                                                                                                                and (nulldate is null or nulldate > cla.occurdat)
                                                                                                                                and statusva not in ('2','3')),
                                                                                                                        (   select max(ctid) from usinsug01.machine_lc
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate <= cla.occurdat
                                                                                                                                and (nulldate is null or nulldate >= cla.occurdat)
                                                                                                                                and statusva not in ('2','3'))),
                                                                                                                        (   select max(ctid) from usinsug01.machine_lc
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate <= cla.occurdat
                                                                                                                                and (nulldate is null or nulldate < cla.occurdat)
                                                                                                                                and statusva not in ('2','3'))),
                                                                                                                        (   select min(ctid) from usinsug01.machine_lc
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate > cla.occurdat
                                                                                                                                and (nulldate is null or nulldate > cla.occurdat)
                                                                                                                                and statusva not in ('2','3'))),
                                                                                                                        (   select min(ctid) from usinsug01.machine_lc
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif
                                                                                                                                and statusva not in ('2','3'))),
                                                                                                                        (   select min(ctid) from usinsug01.machine_lc
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif
                                                                                                                                and statusva in ('2','3')))))
                                                                                                when 	'risk_3d'
                                                                                                then 	(	select	statusva
                                                                                                                        from	usinsug01.risk_3d
                                                                                                                        where	ctid = 
                                                                                                                        (	coalesce(coalesce(coalesce(coalesce(coalesce(
                                                                                                                        (   select max(ctid) from usinsug01.risk_3d
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate <= cla.occurdat
                                                                                                                                and (nulldate is null or nulldate > cla.occurdat)
                                                                                                                                and statusva not in ('2','3')),
                                                                                                                        (   select max(ctid) from usinsug01.risk_3d
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate <= cla.occurdat
                                                                                                                                and (nulldate is null or nulldate >= cla.occurdat)
                                                                                                                                and statusva not in ('2','3'))),
                                                                                                                        (   select max(ctid) from usinsug01.risk_3d
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate <= cla.occurdat
                                                                                                                                and (nulldate is null or nulldate < cla.occurdat)
                                                                                                                                and statusva not in ('2','3'))),
                                                                                                                        (   select min(ctid) from usinsug01.risk_3d
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate > cla.occurdat
                                                                                                                                and (nulldate is null or nulldate > cla.occurdat)
                                                                                                                                and statusva not in ('2','3'))),
                                                                                                                        (   select min(ctid) from usinsug01.risk_3d
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif
                                                                                                                                and statusva not in ('2','3'))),
                                                                                                                        (   select min(ctid) from usinsug01.risk_3d
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif
                                                                                                                                and statusva in ('2','3')))))
                                                                                                when 	'ship'
                                                                                                then 	(	select	statusva
                                                                                                                        from	usinsug01.ship
                                                                                                                        where	ctid = 
                                                                                                                        (	coalesce(coalesce(coalesce(coalesce(coalesce(
                                                                                                                        (   select max(ctid) from usinsug01.ship
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate <= cla.occurdat
                                                                                                                                and (nulldate is null or nulldate > cla.occurdat)
                                                                                                                                and statusva not in ('2','3')),
                                                                                                                        (   select max(ctid) from usinsug01.ship
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate <= cla.occurdat
                                                                                                                                and (nulldate is null or nulldate >= cla.occurdat)
                                                                                                                                and statusva not in ('2','3'))),
                                                                                                                        (   select max(ctid) from usinsug01.ship
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate <= cla.occurdat
                                                                                                                                and (nulldate is null or nulldate < cla.occurdat)
                                                                                                                                and statusva not in ('2','3'))),
                                                                                                                        (   select min(ctid) from usinsug01.ship
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate > cla.occurdat
                                                                                                                                and (nulldate is null or nulldate > cla.occurdat)
                                                                                                                                and statusva not in ('2','3'))),
                                                                                                                        (   select min(ctid) from usinsug01.ship
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif
                                                                                                                                and statusva not in ('2','3'))),
                                                                                                                        (   select min(ctid) from usinsug01.ship
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif
                                                                                                                                and statusva in ('2','3')))))
                                                                                                when 	'theft'
                                                                                                then 	(	select	statusva
                                                                                                                        from	usinsug01.theft
                                                                                                                        where	ctid = 
                                                                                                                        (	coalesce(coalesce(coalesce(coalesce(coalesce(
                                                                                                                        (   select max(ctid) from usinsug01.theft
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate <= cla.occurdat
                                                                                                                                and (nulldate is null or nulldate > cla.occurdat)
                                                                                                                                and statusva not in ('2','3')),
                                                                                                                        (   select max(ctid) from usinsug01.theft
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate <= cla.occurdat
                                                                                                                                and (nulldate is null or nulldate >= cla.occurdat)
                                                                                                                                and statusva not in ('2','3'))),
                                                                                                                        (   select max(ctid) from usinsug01.theft
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate <= cla.occurdat
                                                                                                                                and (nulldate is null or nulldate < cla.occurdat)
                                                                                                                                and statusva not in ('2','3'))),
                                                                                                                        (   select min(ctid) from usinsug01.theft
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate > cla.occurdat
                                                                                                                                and (nulldate is null or nulldate > cla.occurdat)
                                                                                                                                and statusva not in ('2','3'))),
                                                                                                                        (   select min(ctid) from usinsug01.theft
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif
                                                                                                                                and statusva not in ('2','3'))),
                                                                                                                        (   select min(ctid) from usinsug01.theft
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif
                                                                                                                                and statusva in ('2','3')))))
                                                                                                when 	'transport'
                                                                                                then 	(	select	statusva
                                                                                                                        from	usinsug01.transport
                                                                                                                        where	ctid = 
                                                                                                                        (	coalesce(coalesce(coalesce(coalesce(coalesce(
                                                                                                                        (   select max(ctid) from usinsug01.transport
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate <= cla.occurdat
                                                                                                                                and (nulldate is null or nulldate > cla.occurdat)
                                                                                                                                and statusva not in ('2','3')),
                                                                                                                        (   select max(ctid) from usinsug01.transport
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate <= cla.occurdat
                                                                                                                                and (nulldate is null or nulldate >= cla.occurdat)
                                                                                                                                and statusva not in ('2','3'))),
                                                                                                                        (   select max(ctid) from usinsug01.transport
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate <= cla.occurdat
                                                                                                                                and (nulldate is null or nulldate < cla.occurdat)
                                                                                                                                and statusva not in ('2','3'))),
                                                                                                                        (   select min(ctid) from usinsug01.transport
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate > cla.occurdat
                                                                                                                                and (nulldate is null or nulldate > cla.occurdat)
                                                                                                                                and statusva not in ('2','3'))),
                                                                                                                        (   select min(ctid) from usinsug01.transport
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif
                                                                                                                                and statusva not in ('2','3'))),
                                                                                                                        (   select min(ctid) from usinsug01.transport
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif
                                                                                                                                and statusva in ('2','3')))))
                                                                                                when 	'trec'
                                                                                                then 	(	select	statusva
                                                                                                                        from	usinsug01.trec
                                                                                                                        where	ctid = 
                                                                                                                        (	coalesce(coalesce(coalesce(coalesce(coalesce(
                                                                                                                        (   select max(ctid) from usinsug01.trec
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate <= cla.occurdat
                                                                                                                                and (nulldate is null or nulldate > cla.occurdat)
                                                                                                                                and statusva not in ('2','3')),
                                                                                                                        (   select max(ctid) from usinsug01.trec
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate <= cla.occurdat
                                                                                                                                and (nulldate is null or nulldate >= cla.occurdat)
                                                                                                                                and statusva not in ('2','3'))),
                                                                                                                        (   select max(ctid) from usinsug01.trec
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate <= cla.occurdat
                                                                                                                                and (nulldate is null or nulldate < cla.occurdat)
                                                                                                                                and statusva not in ('2','3'))),
                                                                                                                        (   select min(ctid) from usinsug01.trec
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate > cla.occurdat
                                                                                                                                and (nulldate is null or nulldate > cla.occurdat)
                                                                                                                                and statusva not in ('2','3'))),
                                                                                                                        (   select min(ctid) from usinsug01.trec
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif
                                                                                                                                and statusva not in ('2','3'))),
                                                                                                                        (   select min(ctid) from usinsug01.trec
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif
                                                                                                                                and statusva in ('2','3')))))
                                                                                                else  '' end,
                                                                        coalesce(case	when	coalesce(cla.certif,0) = 0
                                                                                                        then	pol.status_pol
                                                                                                        else	coalesce((	select	cer.statusva
                                                                                                                                                from 	usinsug01.certificat cer
                                                                                                                                                where	cer.usercomp = cla.usercomp 
                                                                                                                                                and 	cer.company = cla.company
                                                                                                                                                and 	cer.certype = pol.certype
                                                                                                                                                and 	cer.branch = cla.branch 
                                                                                                                                                and 	cer.policy = cla.policy
                                                                                                                                                and		cer.certif = cla.certif),'')
                                                                                                        end), '') KACESTAP,
                                                        cast(case	coalesce(cla.certif,0)
                                                                                when	0
                                                                                then	case	when	coalesce(pol.payfreq,'0') = '1'
                                                                                                                then	1
                                                                                                                else	0 end
                                                                                else	case	when	coalesce((	select	cer.payfreq
                                                                                                                                                        from 	usinsug01.certificat cer
                                                                                                                                                        where	cer.usercomp = cla.usercomp 
                                                                                                                                                        and 	cer.company = cla.company
                                                                                                                                                        and 	cer.certype = pol.certype
                                                                                                                                                        and 	cer.branch = cla.branch 
                                                                                                                                                        and 	cer.policy = cla.policy
                                                                                                                                                        and		cer.certif = cla.certif),'0') = '1'
                                                                                                                then	1
                                                                                                                else	0 end
                                                                                end as varchar) DTERMO,
                                                        '' TULTALT, --excluido
                                                        '' DUSRUPD, --excluido
                                                        '' KABUNRIS, --SE DESCONOCE ABUNRIS.PK
                                                        '' DUSRENT, --excluido
                                                        '' DUSRSUP, --excluido
                                                        coalesce(cast(cla.staclaim as varchar),'') KSCESTSN,
                                                        '' KSCFMPTI, --excluido
                                                        '' KSCTPAUT, --excluido
                                                        '' KSCLOCSN, --excluido
                                                        '' KSCINBMT, --excluido
                                                        '' KSCINIDS, --excluido
                                                        '' DPROCIDS, --excluido
                                                        '' KSCETIDS, --excluido
                                                        'LPG' DCOMPA,
                                                        '' DMARCA, --excluido
                                                        '' KSCNATZA_IN, --excluido
                                                        '' KSCNATZA_FI, --excluido
                                                        '' KSCCDEST, --excluido
                                                        '' KACARGES, --excluido
                                                        '' DNUMOBJ, --excluido
                                                        '' DNUMOB2, --excluido
                                                        coalesce(	case	/*(	select	"RISKTYPEN"
                                                                                                        from	usbi01."IFRS170_T_RAMOS_POR_TIPO_RIESGO"
                                                                                                        where	"SOURCESCHEMA" = 'usinsug01'
                                                                                                        and		"BRANCHCOM" = cla.branch)*/
                                                                                                --when	3
                                                                                                when	cla.branch in (6,15,26,29,62,66,67)
                                                                                                then	coalesce((	select	case	when coalesce(trim(tnb.regist),'') <> '' and coalesce(trim(tnb.chassis),'') <> ''
                                                                                                                                                                        then coalesce(trim(tnb.regist),'') || '-' || coalesce(trim(tnb.chassis),'')
                                                                                                                                                                        when coalesce(trim(tnb.regist),'') <> '' and coalesce(trim(tnb.chassis),'') = ''
                                                                                                                                                                        then coalesce(trim(tnb.regist),'')
                                                                                                                                                                        when coalesce(trim(tnb.regist),'') = '' and coalesce(trim(tnb.chassis),'') <> ''
                                                                                                                                                                        then coalesce(trim(tnb.chassis),'')
                                                                                                                                                                        else '' end
                                                                                                                                        from	usinsug01.auto_peru tnb
                                                                                                                                        where	ctid = 
                                                                                                                                                        (	coalesce(coalesce(coalesce(coalesce(coalesce(
                                                                                                                                        (   select max(ctid) from usinsug01.auto_peru
                                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate <= cla.occurdat
                                                                                                                                                and (nulldate is null or nulldate > cla.occurdat)
                                                                                                                                                and statusva not in ('2','3')),
                                                                                                                                        (   select max(ctid) from usinsug01.auto_peru
                                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate <= cla.occurdat
                                                                                                                                                and (nulldate is null or nulldate >= cla.occurdat)
                                                                                                                                                and statusva not in ('2','3'))),
                                                                                                                                        (   select max(ctid) from usinsug01.auto_peru
                                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate <= cla.occurdat
                                                                                                                                                and (nulldate is null or nulldate < cla.occurdat)
                                                                                                                                                and statusva not in ('2','3'))),
                                                                                                                                        (   select min(ctid) from usinsug01.auto_peru
                                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate > cla.occurdat
                                                                                                                                                and (nulldate is null or nulldate > cla.occurdat)
                                                                                                                                                and statusva not in ('2','3'))),
                                                                                                                                        (   select min(ctid) from usinsug01.auto_peru
                                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif
                                                                                                                                                and statusva not in ('2','3'))),
                                                                                                                                        (   select min(ctid) from usinsug01.auto_peru
                                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif
                                                                                                                                                and statusva in ('2','3'))))),'')
                                                                                                --when	2
                                                                                                when	cla.branch in (1,2,3,4,7,8,9,10,11,12,13,14,16,17,18,19,28,30,38,39,55,57,58)
                                                                                                then	cla.branch || '-' || cla.policy || '-' || cla.certif
                                                                                                --when	1
                                                                                                when	cla.branch in (5,21,22,23,24,25,27,31,32,33,34,35,36,37,40,41,42,59,68,71,75,77,91,99)
                                                                                                then	coalesce(coalesce(
                                                                                                                                        (	select	max(sclient_vig)
                                                                                                                                                from	usinsug01.wbstblclidepequi
                                                                                                                                                where	sclient_old = 
                                                                                                                                                                (	select	evi.scod_vt
                                                                                                                                                                        from	usinsug01.equi_vt_inx evi 
                                                                                                                                                                        where	evi.scod_inx = cla.client)),
                                                                                                                                        (	select	evi.scod_vt
                                                                                                                                                from	usinsug01.equi_vt_inx evi 
                                                                                                                                                where	evi.scod_inx = cla.client)),
                                                                                                                        cla.client)
                                                                                                else 	'' end, '') DUNIDRISC,
                                                        coalesce((	select	cast(max(operdate) as varchar)
                                                                                from	usinsug01.claim_his
                                                                                where	claim = cla.claim
                                                                                and		oper_type = '16'),'') TDTREABE,
                                                        '' DEQREGUL, --excluido
                                                        '' KACMOEST, --excluido
                                                        '' KACCONCE, --excluido
                                                        '' KSCCONTE, --excluido
                                                        '' KSCSTREE, --no disponible
                                                        cast(coalesce(	case	when	pol.bussityp = '2'
                                                                                                        then	pol.leadshare
                                                                                                        else	cl0.share_coa
                                                                                                        end,0) as numeric(7,4)) VTXCOSEG,
                                                        '' KSCPAIS, --no disponible
                                                        '' KSCDEFRP, --excluido
                                                        '' KSCORPAR, --excluido
                                                        '' DTPRCAS, --excluido
                                                        '' TDTESTAD, --no disponible
                                                        '' KSBSIN_MP, --excluido
                                                        '' TMIGPARA, --excluido
                                                        '' KSBSIN_MD, --excluido
                                                        '' TMIGDE, --excluido
                                                        cla.branch || '-' || pol.product || '-' || cla.policy || '-' || cla.certif KABAPOL,
                                                        '' TPRENCER, --excluido
                                                        '' TDTREEMB, --no disponible
                                                        '' TENTPLAT, --excluido
                                                        '' DHENTPLA, --excluido
                                                        '' TENTCOMP, --excluido
                                                        '' DHENTCOM, --excluido
                                                        '' TPEDPART, --excluido
                                                        '' TDTRECLA, --excluido
                                                        '' TDECFIN, --excluido
                                                        '' TASSRESP, --excluido
                                                        '' DINDENCU, --excluido
                                                        '' KSCMTENC, --excluido
                                                        '' DQTDAAA, --excluido
                                                        '' DINFACTO, --excluido
                                                        '' TINISUSP, --excluido
                                                        '' TFIMSUSP, --excluido
                                                        '' DINSOPRE, --excluido
                                                        '' KSCTPDAN, --excluido
                                                        '' KABAPOL_EFT, --excluido
                                                        '' DARQUIVO, --excluido
                                                        '' TARQUIVO, --excluido
                                                        '' DLOCREF, --excluido
                                                        '' KACPARES, --no disponible
                                                        '' KGCRAMO_SAP, --excluido
                                                        '' DNUMPGRE, --no disponible
                                                        '' DINDSINTER, --FALTA C�LCULO
                                                        '' DQDREABER, --excluido
                                                        '' TPLANOCOSEG, --excluido
                                                        '' TPLANORESEG, --excluido
                                                        case	when pol.bussityp = '1' and cl0.share_coa = 100 then '0' --Paga todo
                                                                        when pol.bussityp = '1' and cl0.share_coa <> 100 then '1' --No paga todo
                                                                        when pol.bussityp = '2' then '0' --Paga todo
                                                                        else '0' end KSCPAGCSG,
                                                        '' KSCAPLGES, --excluido
                                                        '' DANUAGR, --excluido
                                                        '' DENTIDSO, --excluido
                                                        '' DNOFSIN, --excluido
                                                        '' DIMAGEM, --excluido
                                                        '' KEBENTID_GS, --excluido
                                                        '' KOCSCOPE, --excluido
                                                        '' DCDINTTRA --excluido
                                        from	(	select	clh.cla_id,
                                                                                clh.pol_id,
                                                                                clh.share_coa,
                                                                                clh.share_rea,
                                                                                sum(clh.monto_trans * case clh.tipo when 1 then 1 else 0 end ) reserva,
                                                                                sum(clh.monto_trans * case clh.tipo when 2 then 1 else 0 end ) ajustes,
                                                                                sum(clh.monto_trans * case clh.tipo when 3 then 1 else 0 end ) pagos
                                                                from	(	select 	cla.*,
                                                                                                        tcl.tipo,
                                                                                                        coalesce(	case	when	clh.currency = cla.moneda_cod
                                                                                                                                                then    clh.amount
                                                                                                                                                else	case	when	cla.moneda_cod = 1
                                                                                                                                                                                then	clh.amount *
                                                                                                                                                                                                case	when	clh.currency = 2
                                                                                                                                                                                                                then	(	select	max(clh0.exchange)
                                                                                                                                                                                                                                        from	usinsug01.claim_his clh0
                                                                                                                                                                                                                                        where	clh0.claim = clh.claim
                                                                                                                                                                                                                                        and		clh0.transac =
                                                                                                                                                                                                                                                        (	select	max(clh1.transac)
                                                                                                                                                                                                                                                                from	usinsug01.claim_his clh1
                                                                                                                                                                                                                                                                where 	clh1.claim = clh.claim
                                                                                                                                                                                                                                                                and		clh1.transac <= clh.transac
                                                                                                                                                                                                                                                                and		clh1.exchange not in (1,0)))
                                                                                                                                                                                                                else    1 end
                                                                                                                                                                                when	cla.moneda_cod = 2
                                                                                                                                                                                then	clh.amount /
                                                                                                                                                                                                case	when	clh.currency = 1
                                                                                                                                                                                                                then	(	select	max(clh0.exchange)
                                                                                                                                                                                                                                        from	usinsug01.claim_his clh0
                                                                                                                                                                                                                                        where	clh0.claim = clh.claim
                                                                                                                                                                                                                                        and		clh0.transac =
                                                                                                                                                                                                                                                        (	select	max(clh1.transac)
                                                                                                                                                                                                                                                                from	usinsug01.claim_his clh1
                                                                                                                                                                                                                                                                where 	clh1.claim = clh.claim
                                                                                                                                                                                                                                                                and		clh1.transac <= clh.transac
                                                                                                                                                                                                                                                                and		clh1.exchange not in (1,0)))
                                                                                                                                                                                                                else    1 end
                                                                                                                                                                                else	0 end end, 0) monto_trans
                                                                                        from 	(	select 	case 	when 	(cla.staclaim <> '6' or (cla.branch = 23 and cla.staclaim = '6')) and pol.certype = '2'
                                                                                                                                                                and 1 = (select t10b.company from usinsug01.table10b t10b where t10b.branch = cla.branch)
                                                                                                                                                then	cla.claim
                                                                                                                                                else 	null end claim,
                                                                                                                                case 	when 	(cla.staclaim <> '6' or (cla.branch = 23 and cla.staclaim = '6')) and pol.certype = '2'
                                                                                                                                                then	cla.ctid 
                                                                                                                                                else	null end cla_id,
                                                                                                                                case	when	pol.certype = '2'
                                                                                                                                                then	pol.ctid
                                                                                                                                                else	null end pol_id,
                                                                                                                                case 	when 	(cla.staclaim <> '6' or (cla.branch = 23 and cla.staclaim = '6')) and pol.certype = '2'
                                                                                                                                                                and 1 = (select t10b.company from usinsug01.table10b t10b where t10b.branch = cla.branch)
                                                                                                                                                then	case	pol.bussityp
                                                                                                                                                                                when	'2'
                                                                                                                                                                                then 	100 - coalesce(pol.leadshare,0)
                                                                                                                                                                                when	'3'
                                                                                                                                                                                then 	null
                                                                                                                                                                                else	coalesce((	select	coi.share
                                                                                                                                                                                                                        from	usinsug01.coinsuran coi
                                                                                                                                                                                                                        where   coi.usercomp = cla.usercomp
                                                                                                                                                                                                                        and     coi.company = cla.company
                                                                                                                                                                                                                        and     coi.certype = pol.certype
                                                                                                                                                                                                                        and     coi.branch = cla.branch
                                                                                                                                                                                                                        and     coi.policy = cla.policy
                                                                                                                                                                                                                        and     coi.effecdate <= cla.occurdat
                                                                                                                                                                                                                        and     (coi.nulldate is null or coi.nulldate > cla.occurdat)
                                                                                                                                                                                                                        and 	coalesce(coi.companyc,0) in (1,12)),100) end 
                                                                                                                                                else	0 end share_coa,
                                                                                                                                case 	when 	(cla.staclaim <> '6' or (cla.branch = 23 and cla.staclaim = '6')) and pol.certype = '2'
                                                                                                                                                                and 1 = (select t10b.company from usinsug01.table10b t10b where t10b.branch = cla.branch)
                                                                                                                                                then	coalesce((	select  sum(coalesce(rei.share,0))
                                                                                                                                                                                        from    usinsug01.reinsuran rei
                                                                                                                                                                                        where   rei.usercomp = cla.usercomp
                                                                                                                                                                                        and     rei.company = cla.company
                                                                                                                                                                                        and     rei.certype = pol.certype
                                                                                                                                                                                        and     rei.branch = cla.branch
                                                                                                                                                                                        and     rei.policy = cla.policy
                                                                                                                                                                                        and     rei.certif = cla.certif
                                                                                                                                                                                        and     rei.effecdate <= cla.occurdat
                                                                                                                                                                                        and     (rei.nulldate is null or rei.nulldate > cla.occurdat)
                                                                                                                                                                                        and     coalesce(rei.type,0) = 1),100)
                                                                                                                                                else	0 end share_rea,
                                                                                                                                case 	when 	(cla.staclaim <> '6' or (cla.branch = 23 and cla.staclaim = '6')) and pol.certype = '2'
                                                                                                                                                                and 1 = (select t10b.company from usinsug01.table10b t10b where t10b.branch = cla.branch)
                                                                                                                                                then	coalesce(	coalesce((	select	max(cpl.currency)
                                                                                                                                                                                                                from	usinsug01.curren_pol cpl
                                                                                                                                                                                                                where 	cpl.usercomp = cla.usercomp
                                                                                                                                                                                                                and 	cpl.company = cla.company
                                                                                                                                                                                                                and		cpl.certype = pol.certype
                                                                                                                                                                                                                and		cpl.branch = cla.branch
                                                                                                                                                                                                                and 	cpl.policy = cla.policy
                                                                                                                                                                                                                and		cpl.certif = cla.certif),
                                                                                                                                                                                                        (	select	max(cpl.currency)
                                                                                                                                                                                                                from	usinsug01.curren_pol cpl
                                                                                                                                                                                                                where 	cpl.usercomp = cla.usercomp
                                                                                                                                                                                                                and 	cpl.company = cla.company
                                                                                                                                                                                                                and		cpl.certype = pol.certype
                                                                                                                                                                                                                and		cpl.branch = cla.branch
                                                                                                                                                                                                                and 	cpl.policy = cla.policy)),0) 
                                                                                                                                                else	0 end moneda_cod
                                                                                                                from    usinsug01.policy pol
                                                                                                                join	(select cast('12/31/2015' as date) fecha) par on 1 = 1 --ejecutar al a�o 2021 (2015 es para pruebas)
                                                                                                                join	usinsug01.claim cla
                                                                                                                        on		cla.usercomp = pol.usercomp
                                                                                                                        and     cla.company = pol.company
                                                                                                                        and     cla.branch = pol.branch
                                                                                                                        and     cla.policy = pol.policy
                                                                                                                        and		cla.branch = 66 --consideraci�n �nica de este ramo por el volumen de datos existentes
                                                                                                                        and     exists
                                                                                                                                                (   select  1
                                                                                                                                                        from    (	select	cast(codigint as varchar(2)) operation
                                                                                                                                                                                from	usinsug01.table140
                                                                                                                                                                                where	codigint in 
                                                                                                                                                                                                (	select 	operation
                                                                                                                                                                                                        from	usinsug01.tab_cl_ope
                                                                                                                                                                                                        where	reserve = 1 or ajustes = 1 or pay_amount = 1)) tcl --solo pagos
                                                                                                                                                        join	usinsug01.claim_his clh on trim(clh.oper_type) = tcl.operation
                                                                                                                                                        where   coalesce (clh.claim,0) = cla.claim
                                                                                                                                                        and     clh.operdate >= par.fecha)) cla
                                                                                        join	usinsug01.claim_his clh
                                                                                                        on		clh.claim = cla.claim
                                                                                                        and		clh.operdate <= '12/31/2023' --en caso de ejecutar a una fecha posterior, alterar esta fecha a la de la ejecuci�n
                                                                                        join	(	select	case	when tcl.reserve = 1 then 1
                                                                                                                                                when tcl.ajustes = 1 then 2
                                                                                                                                                when tcl.pay_amount = 1 then 3
                                                                                                                                                else 0 end tipo,
                                                                                                                                cast(tcl.operation as varchar(2)) operation
                                                                                                                from	usinsug01.tab_cl_ope tcl
                                                                                                                where	(tcl.reserve = 1 or tcl.ajustes = 1 or tcl.pay_amount = 1)) tcl
                                                                                                        on		trim(clh.oper_type) = tcl.operation) clh
                                                                group	by 1,2,3,4) cl0
                                        join	usinsug01.claim cla on cla.ctid = cl0.cla_id
                                        join	usinsug01.policy pol on pol.ctid = cl0.pol_id
                                    ) AS TMP    
                                    '''
                            
    L_DF_SBSIN_INSUNIX_LPG_SOAT = glue_context.read.format('jdbc').options(**connection).option("fetchsize",10000).option("dbtable",L_SBSIN_INSUNIX_LPG_SOAT).load()
    
    L_SBSIN_INSUNIX_LPG_AMF  =  '''
                                    (
                                        select	        'D' INDDETREC,
                                                        'SBSIN' TABLAIFRS17,
                                                        '' PK,
                                                        '' DTPREG, --excluido
                                                        '' TIOCPROC, --no disponible
                                                        coalesce(cast(cla.compdate as varchar),'') TIOCFRM,
                                                        '' TIOCTO, --excluido
                                                        'PIG' KGIORIGM,
                                                        'LPG' KSCCOMPA,
                                                        coalesce(cast(cla.branch as varchar),'') KGCRAMO,
                                                        cla.branch || '-' || pol.product || '-' ||
                                                                coalesce((	select	sub_product
                                                                                        from	usinsug01.pol_subproduct
                                                                                        where	usercomp = cla.usercomp
                                                                                        and 	company = cla.company
                                                                                        and		certype = pol.certype
                                                                                        and		branch = cla.branch
                                                                                        and		policy = cla.policy
                                                                                        and		product = pol.product),0) KABPRODT,
                                                        coalesce(cast(cla.policy as varchar),'') DNUMAPO,
                                                        coalesce(cast(cla.certif as varchar),'') DNMCERT,
                                                        coalesce(cast(cla.claim as varchar),'') DNUMSIN,
                                                        coalesce(cast(cla.occurdat as varchar),'') TOCURSIN,
                                                        '' DHSINIST, --excluido
                                                        coalesce(cast(cla.occurdat as varchar),'') TABERSIN,
                                                        '' TPARTSIN, --excluido
                                                        coalesce(
                                                                case	when	cla.staclaim in ('1','5','7')
                                                                                then	(	select	cast(max(operdate) as varchar)
                                                                                                        from	usinsug01.claim_his
                                                                                                        where	claim = cla.claim
                                                                                                        and		case	when	cla.staclaim in ('1') then oper_type in ('3','17')
                                                                                                                                        when	cla.staclaim in ('5') then oper_type in ('11')
                                                                                                                                        when	cla.staclaim in ('7') then oper_type in ('21')
                                                                                                                                        else	oper_type is null end)
                                                                                else 	'' end, '') TFECHTEC,
                                                        coalesce(
                                                                case	when	cla.staclaim in ('1','5','7')
                                                                                then	(	select	cast(max(operdate) as varchar)
                                                                                                        from	usinsug01.claim_his
                                                                                                        where	claim = cla.claim
                                                                                                        and		case	when	cla.staclaim in ('1') then oper_type in ('3','17')
                                                                                                                                        when	cla.staclaim in ('5') then oper_type in ('52')
                                                                                                                                        when	cla.staclaim in ('7') then oper_type in ('21')
                                                                                                                                        else	oper_type is null end)
                                                                                else 	'' end, '') TFECHADM,
                                                        coalesce(
                                                                (	select	cast(max(operdate) as varchar)
                                                                        from	usinsug01.claim_his
                                                                        where	claim = cla.claim
                                                                        and		oper_type in
                                                                                        (	select	cast(codigint as varchar(2))
                                                                                                from	usinsug01.table140
                                                                                                where	(	codigint in 
                                                                                                                        (	select	operation
                                                                                                                                from	usinsug01.tab_cl_ope
                                                                                                                                where	(reserve = 1 or ajustes = 1 or pay_amount in (1,3)))) 
                                                                                                                or codigint in (60))), '') TESTADO,
                                                        coalesce(cast(cla.staclaim as varchar),'') KSCSITUA,
                                                        '' KSCMOTSI, --excluido
                                                        '' KSCTPSIN, --no disponible
                                                        coalesce(cast(cla.causecod as varchar),'') KSCCAUSA,
                                                        '' KSCARGES, --excluido
                                                        '' KSCFMPGS, --no disponible
                                                        '' KCBMED_DRA, --excluido
                                                        '' KCBMED_PG, --excluido
                                                        '' KCBMED_PD, --excluido
                                                        '' KCBMED_P2, --excluido
                                                        case	when pol.bussityp = '1' and cl0.share_coa = 100 then '1' --Sin coaseguro
                                                                        when pol.bussityp = '1' and cl0.share_coa <> 100 then '2' --Con coaseguro, compa��a l�der
                                                                        when pol.bussityp = '2' then '3' --Con coaseguro, compa��a no l�der
                                                                        else '0' end KSCTPCSG,
                                                        cast(coalesce(
                                                                        coalesce((	select	max(cpl.currency)
                                                                                                from	usinsug01.curren_pol cpl
                                                                                                where 	cpl.usercomp = cla.usercomp
                                                                                                and 	cpl.company = cla.company
                                                                                                and		cpl.certype = pol.certype
                                                                                                and		cpl.branch = cla.branch
                                                                                                and 	cpl.policy = cla.policy
                                                                                                and		cpl.certif = cla.certif),
                                                                                        (	select	max(cpl.currency)
                                                                                                from	usinsug01.curren_pol cpl
                                                                                                where 	cpl.usercomp = cla.usercomp
                                                                                                and 	cpl.company = cla.company
                                                                                                and		cpl.certype = pol.certype
                                                                                                and		cpl.branch = cla.branch
                                                                                                and 	cpl.policy = cla.policy)),0) as varchar) KSCMOEDA,
                                                        '' VCAMBIO, --no disponible
                                                        '' VTXRESPN, --no disponible
                                                        cast(cl0.reserva + cl0.ajustes as numeric(14,2)) VMTPROVI,
                                                        '' VMTPRVINI, --excluido
                                                        cast((cl0.reserva + cl0.ajustes)
                                                                * (case when pol.bussityp = '2' then 100 else share_coa end/100)
                                                * (share_rea/100) as numeric(14,2)) VMTPRVRS,
                                                        cast((cl0.reserva + cl0.ajustes)
                                                                        * (case when pol.bussityp = '2' then 100 else share_coa end/100) as numeric(14,2)) VMTCOSEG,
                                                        '' KSCNATUR, --excluido
                                                        '' TALTENAT, --excluido
                                                        '' DUSRREG, --excluido
                                                        coalesce(coalesce(
                                                                                (	select	max(sclient_vig)
                                                                                        from	usinsug01.wbstblclidepequi
                                                                                        where	sclient_old = 
                                                                                                        (	select	evi.scod_vt
                                                                                                                from	usinsug01.equi_vt_inx evi 
                                                                                                                where	evi.scod_inx = cla.client)),
                                                                                (	select	evi.scod_vt
                                                                                        from	usinsug01.equi_vt_inx evi 
                                                                                        where	evi.scod_inx = cla.client)),
                                                                cla.client) KEBENTID_TO,
                                                        cast(coalesce((	select  case	when	max(gco.addsuini) = '3'
                                                                                                                        then	max(case when gco.addsuini = '3' then coalesce(cov.capital,0) else 0 end)
                                                                                                                        else	sum(case when gco.addsuini = '1' then coalesce(cov.capital,0) else 0 end) end
                                                                                        from    usinsug01.cover cov
                                                                                        join	usinsug01.gen_cover gco on gco.ctid =
                                                                        coalesce(coalesce(coalesce(coalesce(
                                                                                                                                        (   select  max(ctid)
                                                                                                        from	usinsug01.gen_cover
                                                                                                        where	usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = pol.product and currency = cov.currency --�ndice regular
                                                                                                        and     modulec = cov.modulec and cover = cov.cover --variaci�n 1 con modulec
                                                                                                        and     effecdate <= cla.occurdat and (nulldate is null or nulldate > cla.occurdat) --variaci�n 2 vigencias
                                                                                                        and     statregt <> '4' and addsuini in ('1','3')), --variaci�n 3 reg. v�lido
                                                                                                        (   select  max(ctid)
                                                                                                        from	usinsug01.gen_cover
                                                                                                        where	usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = pol.product and currency = cov.currency --�ndice regular
                                                                                                        and     cover = cov.cover --variaci�n 1 sin modulec
                                                                                                        and     effecdate <= cla.occurdat and (nulldate is null or nulldate > cla.occurdat) --variaci�n 2 vigencias
                                                                                                        and     statregt <> '4' and addsuini in ('1','3'))), --variaci�n 3 reg. v�lido
                                                                                                coalesce((	select  max(ctid)
                                                                                                        from	usinsug01.gen_cover
                                                                                                        where	usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = pol.product and currency = cov.currency --�ndice regular
                                                                                                        and     modulec = cov.modulec and cover = cov.cover
                                                                                                        and     effecdate <= cla.occurdat and (nulldate is null or nulldate > cla.occurdat)
                                                                                                        and     statregt = '4' and addsuini in ('1','3')),
                                                                                                        (	select  max(ctid)
                                                                                                        from	usinsug01.gen_cover
                                                                                                        where	usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = pol.product and currency = cov.currency --�ndice regular
                                                                                                        and     cover = cov.cover
                                                                                                        and     effecdate <= cla.occurdat and (nulldate is null or nulldate > cla.occurdat)
                                                                                                        and     statregt = '4' and addsuini in ('1','3')))), --no est� cortado
                                                                                        coalesce(coalesce((	select  max(ctid)
                                                                                                        from	usinsug01.gen_cover
                                                                                                        where	usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = pol.product and currency = cov.currency --�ndice regular
                                                                                                        and     modulec = cov.modulec and cover = cov.cover
                                                                                                        and     effecdate <= cla.occurdat and nulldate <= cla.occurdat
                                                                                                        and     statregt <> '4' and addsuini in ('1','3')),
                                                                                                        (   select  max(ctid)
                                                                                                        from	usinsug01.gen_cover
                                                                                                        where	usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = pol.product and currency = cov.currency --�ndice regular
                                                                                                        and     cover = cov.cover
                                                                                                        and     effecdate <= cla.occurdat and nulldate <= cla.occurdat
                                                                                                        and     statregt <> '4' and addsuini in ('1','3'))), -- no est� cortado, pero fue anulado antes del efecto del registro
                                                                                        coalesce((	select  max(ctid)
                                                                                                        from	usinsug01.gen_cover
                                                                                                        where	usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = pol.product and currency = cov.currency --�ndice regular
                                                                                                        and     modulec = cov.modulec and cover = cov.cover
                                                                                                        and     effecdate <= cla.occurdat and nulldate <= cla.occurdat
                                                                                                        and     statregt = '4' and addsuini in ('1','3')),
                                                                                                (   select  max(ctid)
                                                                                                        from	usinsug01.gen_cover
                                                                                                        where 	usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = pol.product and currency = cov.currency --�ndice regular
                                                                                                        and     cover = cov.cover
                                                                                                        and     effecdate <= cla.occurdat and nulldate <= cla.occurdat
                                                                                                        and     statregt = '4' and addsuini in ('1','3'))))), --no est� cortado y aparte fue anulado antes del efecto del registro
                                                                                coalesce(coalesce((	select  max(ctid)
                                                                                                from	usinsug01.gen_cover
                                                                                                where 	usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = pol.product and currency = cov.currency --�ndice regular
                                                                                                and     modulec = cov.modulec and cover = cov.cover
                                                                                                and     effecdate > cla.occurdat
                                                                                                and     statregt <> '4' and addsuini in ('1','3')),
                                                                                                (   select  max(ctid)
                                                                                                from	usinsug01.gen_cover
                                                                                                where	usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = pol.product and currency = cov.currency --�ndice regular
                                                                                                and     cover = cov.cover
                                                                                                and     effecdate <= cla.occurdat and nulldate <= cla.occurdat
                                                                                                and     statregt <> '4' and addsuini in ('1','3'))), -- no est� cortado, pero tampoco al efecto de la tabla de datos particular
                                                                                coalesce((	select  max(ctid)
                                                                                                from	usinsug01.gen_cover
                                                                                                where	usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = pol.product and currency = cov.currency --�ndice regular
                                                                                                and     modulec = cov.modulec and cover = cov.cover
                                                                                                and     effecdate <= cla.occurdat and nulldate <= cla.occurdat
                                                                                                and     statregt = '4' and addsuini in ('1','3')),
                                                                                        (   select  max(ctid)
                                                                                                from	usinsug01.gen_cover
                                                                                                where	usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = pol.product and currency = cov.currency --�ndice regular
                                                                                                and     cover = cov.cover
                                                                                                and     effecdate > cla.occurdat
                                                                                                and     statregt = '4' and addsuini in ('1','3'))))) --est� cortado y no al efecto de la tabla de datos particular adem�s de estar cortado
                                                                                        where   cov.usercomp = cla.usercomp
                                                                                        and     cov.company = cla.company
                                                                                        and     cov.certype = pol.certype
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
                                                                                                else	1 end as numeric(14,2)) VCAPITAL,
                                                        '' VTXINDEM, --no disponible
                                                        '' VMTINDEM, --no disponible
                                                        '' TINICIND, --no disponible
                                                        '' DULTACTA, --excluido
                                                        coalesce(	case	trim((select distinct(lower(tabname)) from usinsug01.tab_name_b where branch = cla.branch))
                                                                                                when 	'accident'
                                                                                                then 	(	select	statusva
                                                                                                                        from	usinsug01.accident
                                                                                                                        where	ctid = 
                                                                                                                        (	coalesce(coalesce(coalesce(coalesce(coalesce(
                                                                                                                        (   select max(ctid) from usinsug01.accident
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate <= cla.occurdat
                                                                                                                                and (nulldate is null or nulldate > cla.occurdat)
                                                                                                                                and statusva not in ('2','3')),
                                                                                                                        (   select max(ctid) from usinsug01.accident
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate <= cla.occurdat
                                                                                                                                and (nulldate is null or nulldate >= cla.occurdat)
                                                                                                                                and statusva not in ('2','3'))),
                                                                                                                        (   select max(ctid) from usinsug01.accident
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate <= cla.occurdat
                                                                                                                                and (nulldate is null or nulldate < cla.occurdat)
                                                                                                                                and statusva not in ('2','3'))),
                                                                                                                        (   select min(ctid) from usinsug01.accident
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate > cla.occurdat
                                                                                                                                and (nulldate is null or nulldate > cla.occurdat)
                                                                                                                                and statusva not in ('2','3'))),
                                                                                                                        (   select min(ctid) from usinsug01.accident
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif
                                                                                                                                and statusva not in ('2','3'))),
                                                                                                                        (   select min(ctid) from usinsug01.accident
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif
                                                                                                                                and statusva in ('2','3')))))
                                                                                                when 	'auto_peru'
                                                                                                then 	(	select	statusva
                                                                                                                        from	usinsug01.auto_peru
                                                                                                                        where	ctid = 
                                                                                                                                        (	coalesce(coalesce(coalesce(coalesce(coalesce(
                                                                                                                        (   select max(ctid) from usinsug01.auto_peru
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate <= cla.occurdat
                                                                                                                                and (nulldate is null or nulldate > cla.occurdat)
                                                                                                                                and statusva not in ('2','3')),
                                                                                                                        (   select max(ctid) from usinsug01.auto_peru
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate <= cla.occurdat
                                                                                                                                and (nulldate is null or nulldate >= cla.occurdat)
                                                                                                                                and statusva not in ('2','3'))),
                                                                                                                        (   select max(ctid) from usinsug01.auto_peru
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate <= cla.occurdat
                                                                                                                                and (nulldate is null or nulldate < cla.occurdat)
                                                                                                                                and statusva not in ('2','3'))),
                                                                                                                        (   select min(ctid) from usinsug01.auto_peru
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate > cla.occurdat
                                                                                                                                and (nulldate is null or nulldate > cla.occurdat)
                                                                                                                                and statusva not in ('2','3'))),
                                                                                                                        (   select min(ctid) from usinsug01.auto_peru
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif
                                                                                                                                and statusva not in ('2','3'))),
                                                                                                                        (   select min(ctid) from usinsug01.auto_peru
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif
                                                                                                                                and statusva in ('2','3')))))
                                                                                                when 	'civil'
                                                                                                then 	(	select	statusva
                                                                                                                        from	usinsug01.civil
                                                                                                                        where	ctid = 
                                                                                                                        (	coalesce(coalesce(coalesce(coalesce(coalesce(
                                                                                                                        (   select max(ctid) from usinsug01.civil
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate <= cla.occurdat
                                                                                                                                and (nulldate is null or nulldate > cla.occurdat)
                                                                                                                                and statusva not in ('2','3')),
                                                                                                                        (   select max(ctid) from usinsug01.civil
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate <= cla.occurdat
                                                                                                                                and (nulldate is null or nulldate >= cla.occurdat)
                                                                                                                                and statusva not in ('2','3'))),
                                                                                                                        (   select max(ctid) from usinsug01.civil
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate <= cla.occurdat
                                                                                                                                and (nulldate is null or nulldate < cla.occurdat)
                                                                                                                                and statusva not in ('2','3'))),
                                                                                                                        (   select min(ctid) from usinsug01.civil
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate > cla.occurdat
                                                                                                                                and (nulldate is null or nulldate > cla.occurdat)
                                                                                                                                and statusva not in ('2','3'))),
                                                                                                                        (   select min(ctid) from usinsug01.civil
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif
                                                                                                                                and statusva not in ('2','3'))),
                                                                                                                        (   select min(ctid) from usinsug01.civil
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif
                                                                                                                                and statusva in ('2','3')))))
                                                                                                when 	'credit'
                                                                                                then 	(	select	statusva
                                                                                                                        from	usinsug01.credit
                                                                                                                        where	ctid = 
                                                                                                                        (	coalesce(coalesce(coalesce(coalesce(coalesce(
                                                                                                                        (   select max(ctid) from usinsug01.credit
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate <= cla.occurdat
                                                                                                                                and (nulldate is null or nulldate > cla.occurdat)
                                                                                                                                and statusva not in ('2','3')),
                                                                                                                        (   select max(ctid) from usinsug01.credit
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate <= cla.occurdat
                                                                                                                                and (nulldate is null or nulldate >= cla.occurdat)
                                                                                                                                and statusva not in ('2','3'))),
                                                                                                                        (   select max(ctid) from usinsug01.credit
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate <= cla.occurdat
                                                                                                                                and (nulldate is null or nulldate < cla.occurdat)
                                                                                                                                and statusva not in ('2','3'))),
                                                                                                                        (   select min(ctid) from usinsug01.credit
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate > cla.occurdat
                                                                                                                                and (nulldate is null or nulldate > cla.occurdat)
                                                                                                                                and statusva not in ('2','3'))),
                                                                                                                        (   select min(ctid) from usinsug01.credit
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif
                                                                                                                                and statusva not in ('2','3'))),
                                                                                                                        (   select min(ctid) from usinsug01.credit
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif
                                                                                                                                and statusva in ('2','3')))))
                                                                                                when 	'deshones'
                                                                                                then 	(	select	statusva
                                                                                                                        from	usinsug01.deshones
                                                                                                                        where	ctid = 
                                                                                                                        (	coalesce(coalesce(coalesce(coalesce(coalesce(
                                                                                                                        (   select max(ctid) from usinsug01.deshones
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate <= cla.occurdat
                                                                                                                                and (nulldate is null or nulldate > cla.occurdat)
                                                                                                                                and statusva not in ('2','3')),
                                                                                                                        (   select max(ctid) from usinsug01.deshones
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate <= cla.occurdat
                                                                                                                                and (nulldate is null or nulldate >= cla.occurdat)
                                                                                                                                and statusva not in ('2','3'))),
                                                                                                                        (   select max(ctid) from usinsug01.deshones
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate <= cla.occurdat
                                                                                                                                and (nulldate is null or nulldate < cla.occurdat)
                                                                                                                                and statusva not in ('2','3'))),
                                                                                                                        (   select min(ctid) from usinsug01.deshones
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate > cla.occurdat
                                                                                                                                and (nulldate is null or nulldate > cla.occurdat)
                                                                                                                                and statusva not in ('2','3'))),
                                                                                                                        (   select min(ctid) from usinsug01.deshones
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif
                                                                                                                                and statusva not in ('2','3'))),
                                                                                                                        (   select min(ctid) from usinsug01.deshones
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif
                                                                                                                                and statusva in ('2','3')))))
                                                                                                when 	'eqele_peru'
                                                                                                then 	(	select	statusva
                                                                                                                        from	usinsug01.eqele_peru
                                                                                                                        where	ctid = 
                                                                                                                        (	coalesce(coalesce(coalesce(coalesce(coalesce(
                                                                                                                        (   select max(ctid) from usinsug01.eqele_peru
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate <= cla.occurdat
                                                                                                                                and (nulldate is null or nulldate > cla.occurdat)
                                                                                                                                and statusva not in ('2','3')),
                                                                                                                        (   select max(ctid) from usinsug01.eqele_peru
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate <= cla.occurdat
                                                                                                                                and (nulldate is null or nulldate >= cla.occurdat)
                                                                                                                                and statusva not in ('2','3'))),
                                                                                                                        (   select max(ctid) from usinsug01.eqele_peru
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate <= cla.occurdat
                                                                                                                                and (nulldate is null or nulldate < cla.occurdat)
                                                                                                                                and statusva not in ('2','3'))),
                                                                                                                        (   select min(ctid) from usinsug01.eqele_peru
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate > cla.occurdat
                                                                                                                                and (nulldate is null or nulldate > cla.occurdat)
                                                                                                                                and statusva not in ('2','3'))),
                                                                                                                        (   select min(ctid) from usinsug01.eqele_peru
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif
                                                                                                                                and statusva not in ('2','3'))),
                                                                                                                        (   select min(ctid) from usinsug01.eqele_peru
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif
                                                                                                                                and statusva in ('2','3')))))
                                                                                                when 	'fire_lc'
                                                                                                then 	(	select	statusva
                                                                                                                        from	usinsug01.fire_lc
                                                                                                                        where	ctid = 
                                                                                                                        (	coalesce(coalesce(coalesce(coalesce(coalesce(
                                                                                                                        (   select max(ctid) from usinsug01.fire_lc
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate <= cla.occurdat
                                                                                                                                and (nulldate is null or nulldate > cla.occurdat)
                                                                                                                                and statusva not in ('2','3')),
                                                                                                                        (   select max(ctid) from usinsug01.fire_lc
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate <= cla.occurdat
                                                                                                                                and (nulldate is null or nulldate >= cla.occurdat)
                                                                                                                                and statusva not in ('2','3'))),
                                                                                                                        (   select max(ctid) from usinsug01.fire_lc
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate <= cla.occurdat
                                                                                                                                and (nulldate is null or nulldate < cla.occurdat)
                                                                                                                                and statusva not in ('2','3'))),
                                                                                                                        (   select min(ctid) from usinsug01.fire_lc
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate > cla.occurdat
                                                                                                                                and (nulldate is null or nulldate > cla.occurdat)
                                                                                                                                and statusva not in ('2','3'))),
                                                                                                                        (   select min(ctid) from usinsug01.fire_lc
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif
                                                                                                                                and statusva not in ('2','3'))),
                                                                                                                        (   select min(ctid) from usinsug01.fire_lc
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif
                                                                                                                                and statusva in ('2','3')))))
                                                                                                when 	'fire_peru'
                                                                                                then 	(	select	statusva
                                                                                                                        from	usinsug01.fire_peru
                                                                                                                        where	ctid = 
                                                                                                                        (	coalesce(coalesce(coalesce(coalesce(coalesce(
                                                                                                                        (   select max(ctid) from usinsug01.fire_peru
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate <= cla.occurdat
                                                                                                                                and (nulldate is null or nulldate > cla.occurdat)
                                                                                                                                and statusva not in ('2','3')),
                                                                                                                        (   select max(ctid) from usinsug01.fire_peru
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate <= cla.occurdat
                                                                                                                                and (nulldate is null or nulldate >= cla.occurdat)
                                                                                                                                and statusva not in ('2','3'))),
                                                                                                                        (   select max(ctid) from usinsug01.fire_peru
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate <= cla.occurdat
                                                                                                                                and (nulldate is null or nulldate < cla.occurdat)
                                                                                                                                and statusva not in ('2','3'))),
                                                                                                                        (   select min(ctid) from usinsug01.fire_peru
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate > cla.occurdat
                                                                                                                                and (nulldate is null or nulldate > cla.occurdat)
                                                                                                                                and statusva not in ('2','3'))),
                                                                                                                        (   select min(ctid) from usinsug01.fire_peru
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif
                                                                                                                                and statusva not in ('2','3'))),
                                                                                                                        (   select min(ctid) from usinsug01.fire_peru
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif
                                                                                                                                and statusva in ('2','3')))))
                                                                                                when 	'health'
                                                                                                then 	(	select	statusva
                                                                                                                        from	usinsug01.health
                                                                                                                        where	ctid = 
                                                                                                                        (	coalesce(coalesce(coalesce(coalesce(coalesce(
                                                                                                                        (   select max(ctid) from usinsug01.health
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate <= cla.occurdat
                                                                                                                                and (nulldate is null or nulldate > cla.occurdat)
                                                                                                                                and statusva not in ('2','3')),
                                                                                                                        (   select max(ctid) from usinsug01.health
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate <= cla.occurdat
                                                                                                                                and (nulldate is null or nulldate >= cla.occurdat)
                                                                                                                                and statusva not in ('2','3'))),
                                                                                                                        (   select max(ctid) from usinsug01.health
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate <= cla.occurdat
                                                                                                                                and (nulldate is null or nulldate < cla.occurdat)
                                                                                                                                and statusva not in ('2','3'))),
                                                                                                                        (   select min(ctid) from usinsug01.health
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate > cla.occurdat
                                                                                                                                and (nulldate is null or nulldate > cla.occurdat)
                                                                                                                                and statusva not in ('2','3'))),
                                                                                                                        (   select min(ctid) from usinsug01.health
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif
                                                                                                                                and statusva not in ('2','3'))),
                                                                                                                        (   select min(ctid) from usinsug01.health
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif
                                                                                                                                and statusva in ('2','3')))))
                                                                                                when 	'machine'
                                                                                                then 	(	select	statusva
                                                                                                                        from	usinsug01.machine
                                                                                                                        where	ctid = 
                                                                                                                        (	coalesce(coalesce(coalesce(coalesce(coalesce(
                                                                                                                        (   select max(ctid) from usinsug01.machine
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate <= cla.occurdat
                                                                                                                                and (nulldate is null or nulldate > cla.occurdat)
                                                                                                                                and statusva not in ('2','3')),
                                                                                                                        (   select max(ctid) from usinsug01.machine
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate <= cla.occurdat
                                                                                                                                and (nulldate is null or nulldate >= cla.occurdat)
                                                                                                                                and statusva not in ('2','3'))),
                                                                                                                        (   select max(ctid) from usinsug01.machine
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate <= cla.occurdat
                                                                                                                                and (nulldate is null or nulldate < cla.occurdat)
                                                                                                                                and statusva not in ('2','3'))),
                                                                                                                        (   select min(ctid) from usinsug01.machine
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate > cla.occurdat
                                                                                                                                and (nulldate is null or nulldate > cla.occurdat)
                                                                                                                                and statusva not in ('2','3'))),
                                                                                                                        (   select min(ctid) from usinsug01.machine
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif
                                                                                                                                and statusva not in ('2','3'))),
                                                                                                                        (   select min(ctid) from usinsug01.machine
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif
                                                                                                                                and statusva in ('2','3')))))
                                                                                                when 	'machine_lc'
                                                                                                then 	(	select	statusva
                                                                                                                        from	usinsug01.machine_lc
                                                                                                                        where	ctid = 
                                                                                                                        (	coalesce(coalesce(coalesce(coalesce(coalesce(
                                                                                                                        (   select max(ctid) from usinsug01.machine_lc
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate <= cla.occurdat
                                                                                                                                and (nulldate is null or nulldate > cla.occurdat)
                                                                                                                                and statusva not in ('2','3')),
                                                                                                                        (   select max(ctid) from usinsug01.machine_lc
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate <= cla.occurdat
                                                                                                                                and (nulldate is null or nulldate >= cla.occurdat)
                                                                                                                                and statusva not in ('2','3'))),
                                                                                                                        (   select max(ctid) from usinsug01.machine_lc
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate <= cla.occurdat
                                                                                                                                and (nulldate is null or nulldate < cla.occurdat)
                                                                                                                                and statusva not in ('2','3'))),
                                                                                                                        (   select min(ctid) from usinsug01.machine_lc
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate > cla.occurdat
                                                                                                                                and (nulldate is null or nulldate > cla.occurdat)
                                                                                                                                and statusva not in ('2','3'))),
                                                                                                                        (   select min(ctid) from usinsug01.machine_lc
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif
                                                                                                                                and statusva not in ('2','3'))),
                                                                                                                        (   select min(ctid) from usinsug01.machine_lc
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif
                                                                                                                                and statusva in ('2','3')))))
                                                                                                when 	'risk_3d'
                                                                                                then 	(	select	statusva
                                                                                                                        from	usinsug01.risk_3d
                                                                                                                        where	ctid = 
                                                                                                                        (	coalesce(coalesce(coalesce(coalesce(coalesce(
                                                                                                                        (   select max(ctid) from usinsug01.risk_3d
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate <= cla.occurdat
                                                                                                                                and (nulldate is null or nulldate > cla.occurdat)
                                                                                                                                and statusva not in ('2','3')),
                                                                                                                        (   select max(ctid) from usinsug01.risk_3d
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate <= cla.occurdat
                                                                                                                                and (nulldate is null or nulldate >= cla.occurdat)
                                                                                                                                and statusva not in ('2','3'))),
                                                                                                                        (   select max(ctid) from usinsug01.risk_3d
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate <= cla.occurdat
                                                                                                                                and (nulldate is null or nulldate < cla.occurdat)
                                                                                                                                and statusva not in ('2','3'))),
                                                                                                                        (   select min(ctid) from usinsug01.risk_3d
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate > cla.occurdat
                                                                                                                                and (nulldate is null or nulldate > cla.occurdat)
                                                                                                                                and statusva not in ('2','3'))),
                                                                                                                        (   select min(ctid) from usinsug01.risk_3d
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif
                                                                                                                                and statusva not in ('2','3'))),
                                                                                                                        (   select min(ctid) from usinsug01.risk_3d
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif
                                                                                                                                and statusva in ('2','3')))))
                                                                                                when 	'ship'
                                                                                                then 	(	select	statusva
                                                                                                                        from	usinsug01.ship
                                                                                                                        where	ctid = 
                                                                                                                        (	coalesce(coalesce(coalesce(coalesce(coalesce(
                                                                                                                        (   select max(ctid) from usinsug01.ship
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate <= cla.occurdat
                                                                                                                                and (nulldate is null or nulldate > cla.occurdat)
                                                                                                                                and statusva not in ('2','3')),
                                                                                                                        (   select max(ctid) from usinsug01.ship
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate <= cla.occurdat
                                                                                                                                and (nulldate is null or nulldate >= cla.occurdat)
                                                                                                                                and statusva not in ('2','3'))),
                                                                                                                        (   select max(ctid) from usinsug01.ship
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate <= cla.occurdat
                                                                                                                                and (nulldate is null or nulldate < cla.occurdat)
                                                                                                                                and statusva not in ('2','3'))),
                                                                                                                        (   select min(ctid) from usinsug01.ship
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate > cla.occurdat
                                                                                                                                and (nulldate is null or nulldate > cla.occurdat)
                                                                                                                                and statusva not in ('2','3'))),
                                                                                                                        (   select min(ctid) from usinsug01.ship
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif
                                                                                                                                and statusva not in ('2','3'))),
                                                                                                                        (   select min(ctid) from usinsug01.ship
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif
                                                                                                                                and statusva in ('2','3')))))
                                                                                                when 	'theft'
                                                                                                then 	(	select	statusva
                                                                                                                        from	usinsug01.theft
                                                                                                                        where	ctid = 
                                                                                                                        (	coalesce(coalesce(coalesce(coalesce(coalesce(
                                                                                                                        (   select max(ctid) from usinsug01.theft
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate <= cla.occurdat
                                                                                                                                and (nulldate is null or nulldate > cla.occurdat)
                                                                                                                                and statusva not in ('2','3')),
                                                                                                                        (   select max(ctid) from usinsug01.theft
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate <= cla.occurdat
                                                                                                                                and (nulldate is null or nulldate >= cla.occurdat)
                                                                                                                                and statusva not in ('2','3'))),
                                                                                                                        (   select max(ctid) from usinsug01.theft
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate <= cla.occurdat
                                                                                                                                and (nulldate is null or nulldate < cla.occurdat)
                                                                                                                                and statusva not in ('2','3'))),
                                                                                                                        (   select min(ctid) from usinsug01.theft
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate > cla.occurdat
                                                                                                                                and (nulldate is null or nulldate > cla.occurdat)
                                                                                                                                and statusva not in ('2','3'))),
                                                                                                                        (   select min(ctid) from usinsug01.theft
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif
                                                                                                                                and statusva not in ('2','3'))),
                                                                                                                        (   select min(ctid) from usinsug01.theft
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif
                                                                                                                                and statusva in ('2','3')))))
                                                                                                when 	'transport'
                                                                                                then 	(	select	statusva
                                                                                                                        from	usinsug01.transport
                                                                                                                        where	ctid = 
                                                                                                                        (	coalesce(coalesce(coalesce(coalesce(coalesce(
                                                                                                                        (   select max(ctid) from usinsug01.transport
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate <= cla.occurdat
                                                                                                                                and (nulldate is null or nulldate > cla.occurdat)
                                                                                                                                and statusva not in ('2','3')),
                                                                                                                        (   select max(ctid) from usinsug01.transport
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate <= cla.occurdat
                                                                                                                                and (nulldate is null or nulldate >= cla.occurdat)
                                                                                                                                and statusva not in ('2','3'))),
                                                                                                                        (   select max(ctid) from usinsug01.transport
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate <= cla.occurdat
                                                                                                                                and (nulldate is null or nulldate < cla.occurdat)
                                                                                                                                and statusva not in ('2','3'))),
                                                                                                                        (   select min(ctid) from usinsug01.transport
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate > cla.occurdat
                                                                                                                                and (nulldate is null or nulldate > cla.occurdat)
                                                                                                                                and statusva not in ('2','3'))),
                                                                                                                        (   select min(ctid) from usinsug01.transport
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif
                                                                                                                                and statusva not in ('2','3'))),
                                                                                                                        (   select min(ctid) from usinsug01.transport
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif
                                                                                                                                and statusva in ('2','3')))))
                                                                                                when 	'trec'
                                                                                                then 	(	select	statusva
                                                                                                                        from	usinsug01.trec
                                                                                                                        where	ctid = 
                                                                                                                        (	coalesce(coalesce(coalesce(coalesce(coalesce(
                                                                                                                        (   select max(ctid) from usinsug01.trec
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate <= cla.occurdat
                                                                                                                                and (nulldate is null or nulldate > cla.occurdat)
                                                                                                                                and statusva not in ('2','3')),
                                                                                                                        (   select max(ctid) from usinsug01.trec
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate <= cla.occurdat
                                                                                                                                and (nulldate is null or nulldate >= cla.occurdat)
                                                                                                                                and statusva not in ('2','3'))),
                                                                                                                        (   select max(ctid) from usinsug01.trec
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate <= cla.occurdat
                                                                                                                                and (nulldate is null or nulldate < cla.occurdat)
                                                                                                                                and statusva not in ('2','3'))),
                                                                                                                        (   select min(ctid) from usinsug01.trec
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate > cla.occurdat
                                                                                                                                and (nulldate is null or nulldate > cla.occurdat)
                                                                                                                                and statusva not in ('2','3'))),
                                                                                                                        (   select min(ctid) from usinsug01.trec
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif
                                                                                                                                and statusva not in ('2','3'))),
                                                                                                                        (   select min(ctid) from usinsug01.trec
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif
                                                                                                                                and statusva in ('2','3')))))
                                                                                                else  '' end,
                                                                        coalesce(case	when	coalesce(cla.certif,0) = 0
                                                                                                        then	pol.status_pol
                                                                                                        else	coalesce((	select	cer.statusva
                                                                                                                                                from 	usinsug01.certificat cer
                                                                                                                                                where	cer.usercomp = cla.usercomp 
                                                                                                                                                and 	cer.company = cla.company
                                                                                                                                                and 	cer.certype = pol.certype
                                                                                                                                                and 	cer.branch = cla.branch 
                                                                                                                                                and 	cer.policy = cla.policy
                                                                                                                                                and		cer.certif = cla.certif),'')
                                                                                                        end), '') KACESTAP,
                                                        cast(case	coalesce(cla.certif,0)
                                                                                when	0
                                                                                then	case	when	coalesce(pol.payfreq,'0') = '1'
                                                                                                                then	1
                                                                                                                else	0 end
                                                                                else	case	when	coalesce((	select	cer.payfreq
                                                                                                                                                        from 	usinsug01.certificat cer
                                                                                                                                                        where	cer.usercomp = cla.usercomp 
                                                                                                                                                        and 	cer.company = cla.company
                                                                                                                                                        and 	cer.certype = pol.certype
                                                                                                                                                        and 	cer.branch = cla.branch 
                                                                                                                                                        and 	cer.policy = cla.policy
                                                                                                                                                        and		cer.certif = cla.certif),'0') = '1'
                                                                                                                then	1
                                                                                                                else	0 end
                                                                                end as varchar) DTERMO,
                                                        '' TULTALT, --excluido
                                                        '' DUSRUPD, --excluido
                                                        '' KABUNRIS, --SE DESCONOCE ABUNRIS.PK
                                                        '' DUSRENT, --excluido
                                                        '' DUSRSUP, --excluido
                                                        coalesce(cast(cla.staclaim as varchar),'') KSCESTSN,
                                                        '' KSCFMPTI, --excluido
                                                        '' KSCTPAUT, --excluido
                                                        '' KSCLOCSN, --excluido
                                                        '' KSCINBMT, --excluido
                                                        '' KSCINIDS, --excluido
                                                        '' DPROCIDS, --excluido
                                                        '' KSCETIDS, --excluido
                                                        'LPG' DCOMPA,
                                                        '' DMARCA, --excluido
                                                        '' KSCNATZA_IN, --excluido
                                                        '' KSCNATZA_FI, --excluido
                                                        '' KSCCDEST, --excluido
                                                        '' KACARGES, --excluido
                                                        '' DNUMOBJ, --excluido
                                                        '' DNUMOB2, --excluido
                                                        coalesce(	case	/*(	select	"RISKTYPEN"
                                                                                                        from	usbi01."IFRS170_T_RAMOS_POR_TIPO_RIESGO"
                                                                                                        where	"SOURCESCHEMA" = 'usinsug01'
                                                                                                        and		"BRANCHCOM" = cla.branch)*/
                                                                                                --when	3
                                                                                                when	cla.branch in (6,15,26,29,62,66,67)
                                                                                                then	coalesce((	select	case	when coalesce(trim(tnb.regist),'') <> '' and coalesce(trim(tnb.chassis),'') <> ''
                                                                                                                                                                        then coalesce(trim(tnb.regist),'') || '-' || coalesce(trim(tnb.chassis),'')
                                                                                                                                                                        when coalesce(trim(tnb.regist),'') <> '' and coalesce(trim(tnb.chassis),'') = ''
                                                                                                                                                                        then coalesce(trim(tnb.regist),'')
                                                                                                                                                                        when coalesce(trim(tnb.regist),'') = '' and coalesce(trim(tnb.chassis),'') <> ''
                                                                                                                                                                        then coalesce(trim(tnb.chassis),'')
                                                                                                                                                                        else '' end
                                                                                                                                        from	usinsug01.auto_peru tnb
                                                                                                                                        where	ctid = 
                                                                                                                                                        (	coalesce(coalesce(coalesce(coalesce(coalesce(
                                                                                                                                        (   select max(ctid) from usinsug01.auto_peru
                                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate <= cla.occurdat
                                                                                                                                                and (nulldate is null or nulldate > cla.occurdat)
                                                                                                                                                and statusva not in ('2','3')),
                                                                                                                                        (   select max(ctid) from usinsug01.auto_peru
                                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate <= cla.occurdat
                                                                                                                                                and (nulldate is null or nulldate >= cla.occurdat)
                                                                                                                                                and statusva not in ('2','3'))),
                                                                                                                                        (   select max(ctid) from usinsug01.auto_peru
                                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate <= cla.occurdat
                                                                                                                                                and (nulldate is null or nulldate < cla.occurdat)
                                                                                                                                                and statusva not in ('2','3'))),
                                                                                                                                        (   select min(ctid) from usinsug01.auto_peru
                                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate > cla.occurdat
                                                                                                                                                and (nulldate is null or nulldate > cla.occurdat)
                                                                                                                                                and statusva not in ('2','3'))),
                                                                                                                                        (   select min(ctid) from usinsug01.auto_peru
                                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif
                                                                                                                                                and statusva not in ('2','3'))),
                                                                                                                                        (   select min(ctid) from usinsug01.auto_peru
                                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif
                                                                                                                                                and statusva in ('2','3'))))),'')
                                                                                                --when	2
                                                                                                when	cla.branch in (1,2,3,4,7,8,9,10,11,12,13,14,16,17,18,19,28,30,38,39,55,57,58)
                                                                                                then	cla.branch || '-' || cla.policy || '-' || cla.certif
                                                                                                --when	1
                                                                                                when	cla.branch in (5,21,22,23,24,25,27,31,32,33,34,35,36,37,40,41,42,59,68,71,75,77,91,99)
                                                                                                then	coalesce(coalesce(
                                                                                                                                        (	select	max(sclient_vig)
                                                                                                                                                from	usinsug01.wbstblclidepequi
                                                                                                                                                where	sclient_old = 
                                                                                                                                                                (	select	evi.scod_vt
                                                                                                                                                                        from	usinsug01.equi_vt_inx evi 
                                                                                                                                                                        where	evi.scod_inx = cla.client)),
                                                                                                                                        (	select	evi.scod_vt
                                                                                                                                                from	usinsug01.equi_vt_inx evi 
                                                                                                                                                where	evi.scod_inx = cla.client)),
                                                                                                                        cla.client)
                                                                                                else 	'' end, '') DUNIDRISC,
                                                        coalesce((	select	cast(max(operdate) as varchar)
                                                                                from	usinsug01.claim_his
                                                                                where	claim = cla.claim
                                                                                and		oper_type = '16'),'') TDTREABE,
                                                        '' DEQREGUL, --excluido
                                                        '' KACMOEST, --excluido
                                                        '' KACCONCE, --excluido
                                                        '' KSCCONTE, --excluido
                                                        '' KSCSTREE, --no disponible
                                                        cast(coalesce(	case	when	pol.bussityp = '2'
                                                                                                        then	pol.leadshare
                                                                                                        else	cl0.share_coa
                                                                                                        end,0) as numeric(7,4)) VTXCOSEG,
                                                        '' KSCPAIS, --no disponible
                                                        '' KSCDEFRP, --excluido
                                                        '' KSCORPAR, --excluido
                                                        '' DTPRCAS, --excluido
                                                        '' TDTESTAD, --no disponible
                                                        '' KSBSIN_MP, --excluido
                                                        '' TMIGPARA, --excluido
                                                        '' KSBSIN_MD, --excluido
                                                        '' TMIGDE, --excluido
                                                        cla.branch || '-' || pol.product || '-' || cla.policy || '-' || cla.certif KABAPOL,
                                                        '' TPRENCER, --excluido
                                                        '' TDTREEMB, --no disponible
                                                        '' TENTPLAT, --excluido
                                                        '' DHENTPLA, --excluido
                                                        '' TENTCOMP, --excluido
                                                        '' DHENTCOM, --excluido
                                                        '' TPEDPART, --excluido
                                                        '' TDTRECLA, --excluido
                                                        '' TDECFIN, --excluido
                                                        '' TASSRESP, --excluido
                                                        '' DINDENCU, --excluido
                                                        '' KSCMTENC, --excluido
                                                        '' DQTDAAA, --excluido
                                                        '' DINFACTO, --excluido
                                                        '' TINISUSP, --excluido
                                                        '' TFIMSUSP, --excluido
                                                        '' DINSOPRE, --excluido
                                                        '' KSCTPDAN, --excluido
                                                        '' KABAPOL_EFT, --excluido
                                                        '' DARQUIVO, --excluido
                                                        '' TARQUIVO, --excluido
                                                        '' DLOCREF, --excluido
                                                        '' KACPARES, --no disponible
                                                        '' KGCRAMO_SAP, --excluido
                                                        '' DNUMPGRE, --no disponible
                                                        '' DINDSINTER, --FALTA C�LCULO
                                                        '' DQDREABER, --excluido
                                                        '' TPLANOCOSEG, --excluido
                                                        '' TPLANORESEG, --excluido
                                                        case	when pol.bussityp = '1' and cl0.share_coa = 100 then '0' --Paga todo
                                                                        when pol.bussityp = '1' and cl0.share_coa <> 100 then '1' --No paga todo
                                                                        when pol.bussityp = '2' then '0' --Paga todo
                                                                        else '0' end KSCPAGCSG,
                                                        '' KSCAPLGES, --excluido
                                                        '' DANUAGR, --excluido
                                                        '' DENTIDSO, --excluido
                                                        '' DNOFSIN, --excluido
                                                        '' DIMAGEM, --excluido
                                                        '' KEBENTID_GS, --excluido
                                                        '' KOCSCOPE, --excluido
                                                        '' DCDINTTRA --excluido
                                        from	(	select	clh.cla_id,
                                                                                clh.pol_id,
                                                                                clh.share_coa,
                                                                                clh.share_rea,
                                                                                sum(clh.monto_trans * case clh.tipo when 1 then 1 else 0 end ) reserva,
                                                                                sum(clh.monto_trans * case clh.tipo when 2 then 1 else 0 end ) ajustes,
                                                                                sum(clh.monto_trans * case clh.tipo when 3 then 1 else 0 end ) pagos
                                                                from	(	select 	cla.*,
                                                                                                        tcl.tipo,
                                                                                                        coalesce(	case	when	clh.currency = cla.moneda_cod
                                                                                                                                                then    clh.amount
                                                                                                                                                else	case	when	cla.moneda_cod = 1
                                                                                                                                                                                then	clh.amount *
                                                                                                                                                                                                case	when	clh.currency = 2
                                                                                                                                                                                                                then	(	select	max(clh0.exchange)
                                                                                                                                                                                                                                        from	usinsug01.claim_his clh0
                                                                                                                                                                                                                                        where	clh0.claim = clh.claim
                                                                                                                                                                                                                                        and		clh0.transac =
                                                                                                                                                                                                                                                        (	select	max(clh1.transac)
                                                                                                                                                                                                                                                                from	usinsug01.claim_his clh1
                                                                                                                                                                                                                                                                where 	clh1.claim = clh.claim
                                                                                                                                                                                                                                                                and		clh1.transac <= clh.transac
                                                                                                                                                                                                                                                                and		clh1.exchange not in (1,0)))
                                                                                                                                                                                                                else    1 end
                                                                                                                                                                                when	cla.moneda_cod = 2
                                                                                                                                                                                then	clh.amount /
                                                                                                                                                                                                case	when	clh.currency = 1
                                                                                                                                                                                                                then	(	select	max(clh0.exchange)
                                                                                                                                                                                                                                        from	usinsug01.claim_his clh0
                                                                                                                                                                                                                                        where	clh0.claim = clh.claim
                                                                                                                                                                                                                                        and		clh0.transac =
                                                                                                                                                                                                                                                        (	select	max(clh1.transac)
                                                                                                                                                                                                                                                                from	usinsug01.claim_his clh1
                                                                                                                                                                                                                                                                where 	clh1.claim = clh.claim
                                                                                                                                                                                                                                                                and		clh1.transac <= clh.transac
                                                                                                                                                                                                                                                                and		clh1.exchange not in (1,0)))
                                                                                                                                                                                                                else    1 end
                                                                                                                                                                                else	0 end end, 0) monto_trans
                                                                                        from 	(	select 	case 	when 	(cla.staclaim <> '6' or (cla.branch = 23 and cla.staclaim = '6')) and pol.certype = '2'
                                                                                                                                                                and 1 = (select t10b.company from usinsug01.table10b t10b where t10b.branch = cla.branch)
                                                                                                                                                then	cla.claim
                                                                                                                                                else 	null end claim,
                                                                                                                                case 	when 	(cla.staclaim <> '6' or (cla.branch = 23 and cla.staclaim = '6')) and pol.certype = '2'
                                                                                                                                                then	cla.ctid 
                                                                                                                                                else	null end cla_id,
                                                                                                                                case	when	pol.certype = '2'
                                                                                                                                                then	pol.ctid
                                                                                                                                                else	null end pol_id,
                                                                                                                                case 	when 	(cla.staclaim <> '6' or (cla.branch = 23 and cla.staclaim = '6')) and pol.certype = '2'
                                                                                                                                                                and 1 = (select t10b.company from usinsug01.table10b t10b where t10b.branch = cla.branch)
                                                                                                                                                then	case	pol.bussityp
                                                                                                                                                                                when	'2'
                                                                                                                                                                                then 	100 - coalesce(pol.leadshare,0)
                                                                                                                                                                                when	'3'
                                                                                                                                                                                then 	null
                                                                                                                                                                                else	coalesce((	select	coi.share
                                                                                                                                                                                                                        from	usinsug01.coinsuran coi
                                                                                                                                                                                                                        where   coi.usercomp = cla.usercomp
                                                                                                                                                                                                                        and     coi.company = cla.company
                                                                                                                                                                                                                        and     coi.certype = pol.certype
                                                                                                                                                                                                                        and     coi.branch = cla.branch
                                                                                                                                                                                                                        and     coi.policy = cla.policy
                                                                                                                                                                                                                        and     coi.effecdate <= cla.occurdat
                                                                                                                                                                                                                        and     (coi.nulldate is null or coi.nulldate > cla.occurdat)
                                                                                                                                                                                                                        and 	coalesce(coi.companyc,0) in (1,12)),100) end 
                                                                                                                                                else	0 end share_coa,
                                                                                                                                case 	when 	(cla.staclaim <> '6' or (cla.branch = 23 and cla.staclaim = '6')) and pol.certype = '2'
                                                                                                                                                                and 1 = (select t10b.company from usinsug01.table10b t10b where t10b.branch = cla.branch)
                                                                                                                                                then	coalesce((	select  sum(coalesce(rei.share,0))
                                                                                                                                                                                        from    usinsug01.reinsuran rei
                                                                                                                                                                                        where   rei.usercomp = cla.usercomp
                                                                                                                                                                                        and     rei.company = cla.company
                                                                                                                                                                                        and     rei.certype = pol.certype
                                                                                                                                                                                        and     rei.branch = cla.branch
                                                                                                                                                                                        and     rei.policy = cla.policy
                                                                                                                                                                                        and     rei.certif = cla.certif
                                                                                                                                                                                        and     rei.effecdate <= cla.occurdat
                                                                                                                                                                                        and     (rei.nulldate is null or rei.nulldate > cla.occurdat)
                                                                                                                                                                                        and     coalesce(rei.type,0) = 1),100)
                                                                                                                                                else	0 end share_rea,
                                                                                                                                case 	when 	(cla.staclaim <> '6' or (cla.branch = 23 and cla.staclaim = '6')) and pol.certype = '2'
                                                                                                                                                                and 1 = (select t10b.company from usinsug01.table10b t10b where t10b.branch = cla.branch)
                                                                                                                                                then	coalesce(	coalesce((	select	max(cpl.currency)
                                                                                                                                                                                                                from	usinsug01.curren_pol cpl
                                                                                                                                                                                                                where 	cpl.usercomp = cla.usercomp
                                                                                                                                                                                                                and 	cpl.company = cla.company
                                                                                                                                                                                                                and		cpl.certype = pol.certype
                                                                                                                                                                                                                and		cpl.branch = cla.branch
                                                                                                                                                                                                                and 	cpl.policy = cla.policy
                                                                                                                                                                                                                and		cpl.certif = cla.certif),
                                                                                                                                                                                                        (	select	max(cpl.currency)
                                                                                                                                                                                                                from	usinsug01.curren_pol cpl
                                                                                                                                                                                                                where 	cpl.usercomp = cla.usercomp
                                                                                                                                                                                                                and 	cpl.company = cla.company
                                                                                                                                                                                                                and		cpl.certype = pol.certype
                                                                                                                                                                                                                and		cpl.branch = cla.branch
                                                                                                                                                                                                                and 	cpl.policy = cla.policy)),0) 
                                                                                                                                                else	0 end moneda_cod
                                                                                                                from    usinsug01.policy pol
                                                                                                                join	(select cast('12/31/2015' as date) fecha) par on 1 = 1 --ejecutar al a�o 2021 (2015 es para pruebas)
                                                                                                                join	usinsug01.claim cla
                                                                                                                        on		cla.usercomp = pol.usercomp
                                                                                                                        and     cla.company = pol.company
                                                                                                                        and     cla.branch = pol.branch
                                                                                                                        and     cla.policy = pol.policy
                                                                                                                        and		cla.branch = 23 --consideraci�n �nica de este ramo por el volumen de datos existentes
                                                                                                                        and     exists
                                                                                                                                                (   select  1
                                                                                                                                                        from    (	select	cast(codigint as varchar(2)) operation
                                                                                                                                                                                from	usinsug01.table140
                                                                                                                                                                                where	codigint in 
                                                                                                                                                                                                (	select 	operation
                                                                                                                                                                                                        from	usinsug01.tab_cl_ope
                                                                                                                                                                                                        where	reserve = 1 or ajustes = 1 or pay_amount = 1)) tcl --solo pagos
                                                                                                                                                        join	usinsug01.claim_his clh on trim(clh.oper_type) = tcl.operation
                                                                                                                                                        where   coalesce (clh.claim,0) = cla.claim
                                                                                                                                                        and     clh.operdate >= par.fecha)) cla
                                                                                        join	usinsug01.claim_his clh
                                                                                                        on		clh.claim = cla.claim
                                                                                                        and		clh.operdate <= '12/31/2023' --en caso de ejecutar a una fecha posterior, alterar esta fecha a la de la ejecuci�n
                                                                                        join	(	select	case	when tcl.reserve = 1 then 1
                                                                                                                                                when tcl.ajustes = 1 then 2
                                                                                                                                                when tcl.pay_amount = 1 then 3
                                                                                                                                                else 0 end tipo,
                                                                                                                                cast(tcl.operation as varchar(2)) operation
                                                                                                                from	usinsug01.tab_cl_ope tcl
                                                                                                                where	(tcl.reserve = 1 or tcl.ajustes = 1 or tcl.pay_amount = 1)) tcl
                                                                                                        on		trim(clh.oper_type) = tcl.operation) clh
                                                                group	by 1,2,3,4) cl0
                                        join	usinsug01.claim cla on cla.ctid = cl0.cla_id
                                        join	usinsug01.policy pol on pol.ctid = cl0.pol_id
                                    ) AS TMP    
                                    '''
    L_DF_SBSIN_INSUNIX_LPG_AMF = glue_context.read.format('jdbc').options(**connection).option("fetchsize",10000).option("dbtable",L_SBSIN_INSUNIX_LPG_AMF).load()
                                       
    L_SBSIN_INSUNIX_LPG_OTROS  =  '''
                                    (
                                        select	        'D' INDDETREC,
                                                        'SBSIN' TABLAIFRS17,
                                                        '' PK,
                                                        '' DTPREG, --excluido
                                                        '' TIOCPROC, --no disponible
                                                        coalesce(cast(cla.compdate as varchar),'') TIOCFRM,
                                                        '' TIOCTO, --excluido
                                                        'PIG' KGIORIGM,
                                                        'LPG' KSCCOMPA,
                                                        coalesce(cast(cla.branch as varchar),'') KGCRAMO,
                                                        cla.branch || '-' || pol.product || '-' ||
                                                                coalesce((	select	sub_product
                                                                                        from	usinsug01.pol_subproduct
                                                                                        where	usercomp = cla.usercomp
                                                                                        and 	company = cla.company
                                                                                        and		certype = pol.certype
                                                                                        and		branch = cla.branch
                                                                                        and		policy = cla.policy
                                                                                        and		product = pol.product),0) KABPRODT,
                                                        coalesce(cast(cla.policy as varchar),'') DNUMAPO,
                                                        coalesce(cast(cla.certif as varchar),'') DNMCERT,
                                                        coalesce(cast(cla.claim as varchar),'') DNUMSIN,
                                                        coalesce(cast(cla.occurdat as varchar),'') TOCURSIN,
                                                        '' DHSINIST, --excluido
                                                        coalesce(cast(cla.occurdat as varchar),'') TABERSIN,
                                                        '' TPARTSIN, --excluido
                                                        coalesce(
                                                                case	when	cla.staclaim in ('1','5','7')
                                                                                then	(	select	cast(max(operdate) as varchar)
                                                                                                        from	usinsug01.claim_his
                                                                                                        where	claim = cla.claim
                                                                                                        and		case	when	cla.staclaim in ('1') then oper_type in ('3','17')
                                                                                                                                        when	cla.staclaim in ('5') then oper_type in ('11')
                                                                                                                                        when	cla.staclaim in ('7') then oper_type in ('21')
                                                                                                                                        else	oper_type is null end)
                                                                                else 	'' end, '') TFECHTEC,
                                                        coalesce(
                                                                case	when	cla.staclaim in ('1','5','7')
                                                                                then	(	select	cast(max(operdate) as varchar)
                                                                                                        from	usinsug01.claim_his
                                                                                                        where	claim = cla.claim
                                                                                                        and		case	when	cla.staclaim in ('1') then oper_type in ('3','17')
                                                                                                                                        when	cla.staclaim in ('5') then oper_type in ('52')
                                                                                                                                        when	cla.staclaim in ('7') then oper_type in ('21')
                                                                                                                                        else	oper_type is null end)
                                                                                else 	'' end, '') TFECHADM,
                                                        coalesce(
                                                                (	select	cast(max(operdate) as varchar)
                                                                        from	usinsug01.claim_his
                                                                        where	claim = cla.claim
                                                                        and		oper_type in
                                                                                        (	select	cast(codigint as varchar(2))
                                                                                                from	usinsug01.table140
                                                                                                where	(	codigint in 
                                                                                                                        (	select	operation
                                                                                                                                from	usinsug01.tab_cl_ope
                                                                                                                                where	(reserve = 1 or ajustes = 1 or pay_amount in (1,3)))) 
                                                                                                                or codigint in (60))), '') TESTADO,
                                                        coalesce(cast(cla.staclaim as varchar),'') KSCSITUA,
                                                        '' KSCMOTSI, --excluido
                                                        '' KSCTPSIN, --no disponible
                                                        coalesce(cast(cla.causecod as varchar),'') KSCCAUSA,
                                                        '' KSCARGES, --excluido
                                                        '' KSCFMPGS, --no disponible
                                                        '' KCBMED_DRA, --excluido
                                                        '' KCBMED_PG, --excluido
                                                        '' KCBMED_PD, --excluido
                                                        '' KCBMED_P2, --excluido
                                                        case	when pol.bussityp = '1' and cl0.share_coa = 100 then '1' --Sin coaseguro
                                                                        when pol.bussityp = '1' and cl0.share_coa <> 100 then '2' --Con coaseguro, compa��a l�der
                                                                        when pol.bussityp = '2' then '3' --Con coaseguro, compa��a no l�der
                                                                        else '0' end KSCTPCSG,
                                                        cast(coalesce(
                                                                        coalesce((	select	max(cpl.currency)
                                                                                                from	usinsug01.curren_pol cpl
                                                                                                where 	cpl.usercomp = cla.usercomp
                                                                                                and 	cpl.company = cla.company
                                                                                                and		cpl.certype = pol.certype
                                                                                                and		cpl.branch = cla.branch
                                                                                                and 	cpl.policy = cla.policy
                                                                                                and		cpl.certif = cla.certif),
                                                                                        (	select	max(cpl.currency)
                                                                                                from	usinsug01.curren_pol cpl
                                                                                                where 	cpl.usercomp = cla.usercomp
                                                                                                and 	cpl.company = cla.company
                                                                                                and		cpl.certype = pol.certype
                                                                                                and		cpl.branch = cla.branch
                                                                                                and 	cpl.policy = cla.policy)),0) as varchar) KSCMOEDA,
                                                        '' VCAMBIO, --no disponible
                                                        '' VTXRESPN, --no disponible
                                                        cast(cl0.reserva + cl0.ajustes as numeric(14,2)) VMTPROVI,
                                                        '' VMTPRVINI, --excluido
                                                        cast((cl0.reserva + cl0.ajustes)
                                                                * (case when pol.bussityp = '2' then 100 else share_coa end/100)
                                                * (share_rea/100) as numeric(14,2)) VMTPRVRS,
                                                        cast((cl0.reserva + cl0.ajustes)
                                                                        * (case when pol.bussityp = '2' then 100 else share_coa end/100) as numeric(14,2)) VMTCOSEG,
                                                        '' KSCNATUR, --excluido
                                                        '' TALTENAT, --excluido
                                                        '' DUSRREG, --excluido
                                                        coalesce(coalesce(
                                                                                (	select	max(sclient_vig)
                                                                                        from	usinsug01.wbstblclidepequi
                                                                                        where	sclient_old = 
                                                                                                        (	select	evi.scod_vt
                                                                                                                from	usinsug01.equi_vt_inx evi 
                                                                                                                where	evi.scod_inx = cla.client)),
                                                                                (	select	evi.scod_vt
                                                                                        from	usinsug01.equi_vt_inx evi 
                                                                                        where	evi.scod_inx = cla.client)),
                                                                cla.client) KEBENTID_TO,
                                                        cast(coalesce((	select  case	when	max(gco.addsuini) = '3'
                                                                                                                        then	max(case when gco.addsuini = '3' then coalesce(cov.capital,0) else 0 end)
                                                                                                                        else	sum(case when gco.addsuini = '1' then coalesce(cov.capital,0) else 0 end) end
                                                                                        from    usinsug01.cover cov
                                                                                        join	usinsug01.gen_cover gco on gco.ctid =
                                                                        coalesce(coalesce(coalesce(coalesce(
                                                                                                                                        (   select  max(ctid)
                                                                                                        from	usinsug01.gen_cover
                                                                                                        where	usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = pol.product and currency = cov.currency --�ndice regular
                                                                                                        and     modulec = cov.modulec and cover = cov.cover --variaci�n 1 con modulec
                                                                                                        and     effecdate <= cla.occurdat and (nulldate is null or nulldate > cla.occurdat) --variaci�n 2 vigencias
                                                                                                        and     statregt <> '4' and addsuini in ('1','3')), --variaci�n 3 reg. v�lido
                                                                                                        (   select  max(ctid)
                                                                                                        from	usinsug01.gen_cover
                                                                                                        where	usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = pol.product and currency = cov.currency --�ndice regular
                                                                                                        and     cover = cov.cover --variaci�n 1 sin modulec
                                                                                                        and     effecdate <= cla.occurdat and (nulldate is null or nulldate > cla.occurdat) --variaci�n 2 vigencias
                                                                                                        and     statregt <> '4' and addsuini in ('1','3'))), --variaci�n 3 reg. v�lido
                                                                                                coalesce((	select  max(ctid)
                                                                                                        from	usinsug01.gen_cover
                                                                                                        where	usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = pol.product and currency = cov.currency --�ndice regular
                                                                                                        and     modulec = cov.modulec and cover = cov.cover
                                                                                                        and     effecdate <= cla.occurdat and (nulldate is null or nulldate > cla.occurdat)
                                                                                                        and     statregt = '4' and addsuini in ('1','3')),
                                                                                                        (	select  max(ctid)
                                                                                                        from	usinsug01.gen_cover
                                                                                                        where	usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = pol.product and currency = cov.currency --�ndice regular
                                                                                                        and     cover = cov.cover
                                                                                                        and     effecdate <= cla.occurdat and (nulldate is null or nulldate > cla.occurdat)
                                                                                                        and     statregt = '4' and addsuini in ('1','3')))), --no est� cortado
                                                                                        coalesce(coalesce((	select  max(ctid)
                                                                                                        from	usinsug01.gen_cover
                                                                                                        where	usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = pol.product and currency = cov.currency --�ndice regular
                                                                                                        and     modulec = cov.modulec and cover = cov.cover
                                                                                                        and     effecdate <= cla.occurdat and nulldate <= cla.occurdat
                                                                                                        and     statregt <> '4' and addsuini in ('1','3')),
                                                                                                        (   select  max(ctid)
                                                                                                        from	usinsug01.gen_cover
                                                                                                        where	usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = pol.product and currency = cov.currency --�ndice regular
                                                                                                        and     cover = cov.cover
                                                                                                        and     effecdate <= cla.occurdat and nulldate <= cla.occurdat
                                                                                                        and     statregt <> '4' and addsuini in ('1','3'))), -- no est� cortado, pero fue anulado antes del efecto del registro
                                                                                        coalesce((	select  max(ctid)
                                                                                                        from	usinsug01.gen_cover
                                                                                                        where	usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = pol.product and currency = cov.currency --�ndice regular
                                                                                                        and     modulec = cov.modulec and cover = cov.cover
                                                                                                        and     effecdate <= cla.occurdat and nulldate <= cla.occurdat
                                                                                                        and     statregt = '4' and addsuini in ('1','3')),
                                                                                                (   select  max(ctid)
                                                                                                        from	usinsug01.gen_cover
                                                                                                        where 	usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = pol.product and currency = cov.currency --�ndice regular
                                                                                                        and     cover = cov.cover
                                                                                                        and     effecdate <= cla.occurdat and nulldate <= cla.occurdat
                                                                                                        and     statregt = '4' and addsuini in ('1','3'))))), --no est� cortado y aparte fue anulado antes del efecto del registro
                                                                                coalesce(coalesce((	select  max(ctid)
                                                                                                from	usinsug01.gen_cover
                                                                                                where 	usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = pol.product and currency = cov.currency --�ndice regular
                                                                                                and     modulec = cov.modulec and cover = cov.cover
                                                                                                and     effecdate > cla.occurdat
                                                                                                and     statregt <> '4' and addsuini in ('1','3')),
                                                                                                (   select  max(ctid)
                                                                                                from	usinsug01.gen_cover
                                                                                                where	usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = pol.product and currency = cov.currency --�ndice regular
                                                                                                and     cover = cov.cover
                                                                                                and     effecdate <= cla.occurdat and nulldate <= cla.occurdat
                                                                                                and     statregt <> '4' and addsuini in ('1','3'))), -- no est� cortado, pero tampoco al efecto de la tabla de datos particular
                                                                                coalesce((	select  max(ctid)
                                                                                                from	usinsug01.gen_cover
                                                                                                where	usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = pol.product and currency = cov.currency --�ndice regular
                                                                                                and     modulec = cov.modulec and cover = cov.cover
                                                                                                and     effecdate <= cla.occurdat and nulldate <= cla.occurdat
                                                                                                and     statregt = '4' and addsuini in ('1','3')),
                                                                                        (   select  max(ctid)
                                                                                                from	usinsug01.gen_cover
                                                                                                where	usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = pol.product and currency = cov.currency --�ndice regular
                                                                                                and     cover = cov.cover
                                                                                                and     effecdate > cla.occurdat
                                                                                                and     statregt = '4' and addsuini in ('1','3'))))) --est� cortado y no al efecto de la tabla de datos particular adem�s de estar cortado
                                                                                        where   cov.usercomp = cla.usercomp
                                                                                        and     cov.company = cla.company
                                                                                        and     cov.certype = pol.certype
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
                                                                                                else	1 end as numeric(14,2)) VCAPITAL,
                                                        '' VTXINDEM, --no disponible
                                                        '' VMTINDEM, --no disponible
                                                        '' TINICIND, --no disponible
                                                        '' DULTACTA, --excluido
                                                        coalesce(	case	trim((select distinct(lower(tabname)) from usinsug01.tab_name_b where branch = cla.branch))
                                                                                                when 	'accident'
                                                                                                then 	(	select	statusva
                                                                                                                        from	usinsug01.accident
                                                                                                                        where	ctid = 
                                                                                                                        (	coalesce(coalesce(coalesce(coalesce(coalesce(
                                                                                                                        (   select max(ctid) from usinsug01.accident
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate <= cla.occurdat
                                                                                                                                and (nulldate is null or nulldate > cla.occurdat)
                                                                                                                                and statusva not in ('2','3')),
                                                                                                                        (   select max(ctid) from usinsug01.accident
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate <= cla.occurdat
                                                                                                                                and (nulldate is null or nulldate >= cla.occurdat)
                                                                                                                                and statusva not in ('2','3'))),
                                                                                                                        (   select max(ctid) from usinsug01.accident
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate <= cla.occurdat
                                                                                                                                and (nulldate is null or nulldate < cla.occurdat)
                                                                                                                                and statusva not in ('2','3'))),
                                                                                                                        (   select min(ctid) from usinsug01.accident
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate > cla.occurdat
                                                                                                                                and (nulldate is null or nulldate > cla.occurdat)
                                                                                                                                and statusva not in ('2','3'))),
                                                                                                                        (   select min(ctid) from usinsug01.accident
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif
                                                                                                                                and statusva not in ('2','3'))),
                                                                                                                        (   select min(ctid) from usinsug01.accident
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif
                                                                                                                                and statusva in ('2','3')))))
                                                                                                when 	'auto_peru'
                                                                                                then 	(	select	statusva
                                                                                                                        from	usinsug01.auto_peru
                                                                                                                        where	ctid = 
                                                                                                                                        (	coalesce(coalesce(coalesce(coalesce(coalesce(
                                                                                                                        (   select max(ctid) from usinsug01.auto_peru
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate <= cla.occurdat
                                                                                                                                and (nulldate is null or nulldate > cla.occurdat)
                                                                                                                                and statusva not in ('2','3')),
                                                                                                                        (   select max(ctid) from usinsug01.auto_peru
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate <= cla.occurdat
                                                                                                                                and (nulldate is null or nulldate >= cla.occurdat)
                                                                                                                                and statusva not in ('2','3'))),
                                                                                                                        (   select max(ctid) from usinsug01.auto_peru
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate <= cla.occurdat
                                                                                                                                and (nulldate is null or nulldate < cla.occurdat)
                                                                                                                                and statusva not in ('2','3'))),
                                                                                                                        (   select min(ctid) from usinsug01.auto_peru
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate > cla.occurdat
                                                                                                                                and (nulldate is null or nulldate > cla.occurdat)
                                                                                                                                and statusva not in ('2','3'))),
                                                                                                                        (   select min(ctid) from usinsug01.auto_peru
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif
                                                                                                                                and statusva not in ('2','3'))),
                                                                                                                        (   select min(ctid) from usinsug01.auto_peru
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif
                                                                                                                                and statusva in ('2','3')))))
                                                                                                when 	'civil'
                                                                                                then 	(	select	statusva
                                                                                                                        from	usinsug01.civil
                                                                                                                        where	ctid = 
                                                                                                                        (	coalesce(coalesce(coalesce(coalesce(coalesce(
                                                                                                                        (   select max(ctid) from usinsug01.civil
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate <= cla.occurdat
                                                                                                                                and (nulldate is null or nulldate > cla.occurdat)
                                                                                                                                and statusva not in ('2','3')),
                                                                                                                        (   select max(ctid) from usinsug01.civil
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate <= cla.occurdat
                                                                                                                                and (nulldate is null or nulldate >= cla.occurdat)
                                                                                                                                and statusva not in ('2','3'))),
                                                                                                                        (   select max(ctid) from usinsug01.civil
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate <= cla.occurdat
                                                                                                                                and (nulldate is null or nulldate < cla.occurdat)
                                                                                                                                and statusva not in ('2','3'))),
                                                                                                                        (   select min(ctid) from usinsug01.civil
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate > cla.occurdat
                                                                                                                                and (nulldate is null or nulldate > cla.occurdat)
                                                                                                                                and statusva not in ('2','3'))),
                                                                                                                        (   select min(ctid) from usinsug01.civil
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif
                                                                                                                                and statusva not in ('2','3'))),
                                                                                                                        (   select min(ctid) from usinsug01.civil
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif
                                                                                                                                and statusva in ('2','3')))))
                                                                                                when 	'credit'
                                                                                                then 	(	select	statusva
                                                                                                                        from	usinsug01.credit
                                                                                                                        where	ctid = 
                                                                                                                        (	coalesce(coalesce(coalesce(coalesce(coalesce(
                                                                                                                        (   select max(ctid) from usinsug01.credit
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate <= cla.occurdat
                                                                                                                                and (nulldate is null or nulldate > cla.occurdat)
                                                                                                                                and statusva not in ('2','3')),
                                                                                                                        (   select max(ctid) from usinsug01.credit
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate <= cla.occurdat
                                                                                                                                and (nulldate is null or nulldate >= cla.occurdat)
                                                                                                                                and statusva not in ('2','3'))),
                                                                                                                        (   select max(ctid) from usinsug01.credit
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate <= cla.occurdat
                                                                                                                                and (nulldate is null or nulldate < cla.occurdat)
                                                                                                                                and statusva not in ('2','3'))),
                                                                                                                        (   select min(ctid) from usinsug01.credit
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate > cla.occurdat
                                                                                                                                and (nulldate is null or nulldate > cla.occurdat)
                                                                                                                                and statusva not in ('2','3'))),
                                                                                                                        (   select min(ctid) from usinsug01.credit
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif
                                                                                                                                and statusva not in ('2','3'))),
                                                                                                                        (   select min(ctid) from usinsug01.credit
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif
                                                                                                                                and statusva in ('2','3')))))
                                                                                                when 	'deshones'
                                                                                                then 	(	select	statusva
                                                                                                                        from	usinsug01.deshones
                                                                                                                        where	ctid = 
                                                                                                                        (	coalesce(coalesce(coalesce(coalesce(coalesce(
                                                                                                                        (   select max(ctid) from usinsug01.deshones
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate <= cla.occurdat
                                                                                                                                and (nulldate is null or nulldate > cla.occurdat)
                                                                                                                                and statusva not in ('2','3')),
                                                                                                                        (   select max(ctid) from usinsug01.deshones
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate <= cla.occurdat
                                                                                                                                and (nulldate is null or nulldate >= cla.occurdat)
                                                                                                                                and statusva not in ('2','3'))),
                                                                                                                        (   select max(ctid) from usinsug01.deshones
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate <= cla.occurdat
                                                                                                                                and (nulldate is null or nulldate < cla.occurdat)
                                                                                                                                and statusva not in ('2','3'))),
                                                                                                                        (   select min(ctid) from usinsug01.deshones
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate > cla.occurdat
                                                                                                                                and (nulldate is null or nulldate > cla.occurdat)
                                                                                                                                and statusva not in ('2','3'))),
                                                                                                                        (   select min(ctid) from usinsug01.deshones
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif
                                                                                                                                and statusva not in ('2','3'))),
                                                                                                                        (   select min(ctid) from usinsug01.deshones
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif
                                                                                                                                and statusva in ('2','3')))))
                                                                                                when 	'eqele_peru'
                                                                                                then 	(	select	statusva
                                                                                                                        from	usinsug01.eqele_peru
                                                                                                                        where	ctid = 
                                                                                                                        (	coalesce(coalesce(coalesce(coalesce(coalesce(
                                                                                                                        (   select max(ctid) from usinsug01.eqele_peru
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate <= cla.occurdat
                                                                                                                                and (nulldate is null or nulldate > cla.occurdat)
                                                                                                                                and statusva not in ('2','3')),
                                                                                                                        (   select max(ctid) from usinsug01.eqele_peru
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate <= cla.occurdat
                                                                                                                                and (nulldate is null or nulldate >= cla.occurdat)
                                                                                                                                and statusva not in ('2','3'))),
                                                                                                                        (   select max(ctid) from usinsug01.eqele_peru
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate <= cla.occurdat
                                                                                                                                and (nulldate is null or nulldate < cla.occurdat)
                                                                                                                                and statusva not in ('2','3'))),
                                                                                                                        (   select min(ctid) from usinsug01.eqele_peru
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate > cla.occurdat
                                                                                                                                and (nulldate is null or nulldate > cla.occurdat)
                                                                                                                                and statusva not in ('2','3'))),
                                                                                                                        (   select min(ctid) from usinsug01.eqele_peru
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif
                                                                                                                                and statusva not in ('2','3'))),
                                                                                                                        (   select min(ctid) from usinsug01.eqele_peru
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif
                                                                                                                                and statusva in ('2','3')))))
                                                                                                when 	'fire_lc'
                                                                                                then 	(	select	statusva
                                                                                                                        from	usinsug01.fire_lc
                                                                                                                        where	ctid = 
                                                                                                                        (	coalesce(coalesce(coalesce(coalesce(coalesce(
                                                                                                                        (   select max(ctid) from usinsug01.fire_lc
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate <= cla.occurdat
                                                                                                                                and (nulldate is null or nulldate > cla.occurdat)
                                                                                                                                and statusva not in ('2','3')),
                                                                                                                        (   select max(ctid) from usinsug01.fire_lc
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate <= cla.occurdat
                                                                                                                                and (nulldate is null or nulldate >= cla.occurdat)
                                                                                                                                and statusva not in ('2','3'))),
                                                                                                                        (   select max(ctid) from usinsug01.fire_lc
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate <= cla.occurdat
                                                                                                                                and (nulldate is null or nulldate < cla.occurdat)
                                                                                                                                and statusva not in ('2','3'))),
                                                                                                                        (   select min(ctid) from usinsug01.fire_lc
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate > cla.occurdat
                                                                                                                                and (nulldate is null or nulldate > cla.occurdat)
                                                                                                                                and statusva not in ('2','3'))),
                                                                                                                        (   select min(ctid) from usinsug01.fire_lc
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif
                                                                                                                                and statusva not in ('2','3'))),
                                                                                                                        (   select min(ctid) from usinsug01.fire_lc
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif
                                                                                                                                and statusva in ('2','3')))))
                                                                                                when 	'fire_peru'
                                                                                                then 	(	select	statusva
                                                                                                                        from	usinsug01.fire_peru
                                                                                                                        where	ctid = 
                                                                                                                        (	coalesce(coalesce(coalesce(coalesce(coalesce(
                                                                                                                        (   select max(ctid) from usinsug01.fire_peru
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate <= cla.occurdat
                                                                                                                                and (nulldate is null or nulldate > cla.occurdat)
                                                                                                                                and statusva not in ('2','3')),
                                                                                                                        (   select max(ctid) from usinsug01.fire_peru
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate <= cla.occurdat
                                                                                                                                and (nulldate is null or nulldate >= cla.occurdat)
                                                                                                                                and statusva not in ('2','3'))),
                                                                                                                        (   select max(ctid) from usinsug01.fire_peru
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate <= cla.occurdat
                                                                                                                                and (nulldate is null or nulldate < cla.occurdat)
                                                                                                                                and statusva not in ('2','3'))),
                                                                                                                        (   select min(ctid) from usinsug01.fire_peru
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate > cla.occurdat
                                                                                                                                and (nulldate is null or nulldate > cla.occurdat)
                                                                                                                                and statusva not in ('2','3'))),
                                                                                                                        (   select min(ctid) from usinsug01.fire_peru
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif
                                                                                                                                and statusva not in ('2','3'))),
                                                                                                                        (   select min(ctid) from usinsug01.fire_peru
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif
                                                                                                                                and statusva in ('2','3')))))
                                                                                                when 	'health'
                                                                                                then 	(	select	statusva
                                                                                                                        from	usinsug01.health
                                                                                                                        where	ctid = 
                                                                                                                        (	coalesce(coalesce(coalesce(coalesce(coalesce(
                                                                                                                        (   select max(ctid) from usinsug01.health
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate <= cla.occurdat
                                                                                                                                and (nulldate is null or nulldate > cla.occurdat)
                                                                                                                                and statusva not in ('2','3')),
                                                                                                                        (   select max(ctid) from usinsug01.health
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate <= cla.occurdat
                                                                                                                                and (nulldate is null or nulldate >= cla.occurdat)
                                                                                                                                and statusva not in ('2','3'))),
                                                                                                                        (   select max(ctid) from usinsug01.health
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate <= cla.occurdat
                                                                                                                                and (nulldate is null or nulldate < cla.occurdat)
                                                                                                                                and statusva not in ('2','3'))),
                                                                                                                        (   select min(ctid) from usinsug01.health
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate > cla.occurdat
                                                                                                                                and (nulldate is null or nulldate > cla.occurdat)
                                                                                                                                and statusva not in ('2','3'))),
                                                                                                                        (   select min(ctid) from usinsug01.health
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif
                                                                                                                                and statusva not in ('2','3'))),
                                                                                                                        (   select min(ctid) from usinsug01.health
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif
                                                                                                                                and statusva in ('2','3')))))
                                                                                                when 	'machine'
                                                                                                then 	(	select	statusva
                                                                                                                        from	usinsug01.machine
                                                                                                                        where	ctid = 
                                                                                                                        (	coalesce(coalesce(coalesce(coalesce(coalesce(
                                                                                                                        (   select max(ctid) from usinsug01.machine
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate <= cla.occurdat
                                                                                                                                and (nulldate is null or nulldate > cla.occurdat)
                                                                                                                                and statusva not in ('2','3')),
                                                                                                                        (   select max(ctid) from usinsug01.machine
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate <= cla.occurdat
                                                                                                                                and (nulldate is null or nulldate >= cla.occurdat)
                                                                                                                                and statusva not in ('2','3'))),
                                                                                                                        (   select max(ctid) from usinsug01.machine
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate <= cla.occurdat
                                                                                                                                and (nulldate is null or nulldate < cla.occurdat)
                                                                                                                                and statusva not in ('2','3'))),
                                                                                                                        (   select min(ctid) from usinsug01.machine
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate > cla.occurdat
                                                                                                                                and (nulldate is null or nulldate > cla.occurdat)
                                                                                                                                and statusva not in ('2','3'))),
                                                                                                                        (   select min(ctid) from usinsug01.machine
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif
                                                                                                                                and statusva not in ('2','3'))),
                                                                                                                        (   select min(ctid) from usinsug01.machine
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif
                                                                                                                                and statusva in ('2','3')))))
                                                                                                when 	'machine_lc'
                                                                                                then 	(	select	statusva
                                                                                                                        from	usinsug01.machine_lc
                                                                                                                        where	ctid = 
                                                                                                                        (	coalesce(coalesce(coalesce(coalesce(coalesce(
                                                                                                                        (   select max(ctid) from usinsug01.machine_lc
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate <= cla.occurdat
                                                                                                                                and (nulldate is null or nulldate > cla.occurdat)
                                                                                                                                and statusva not in ('2','3')),
                                                                                                                        (   select max(ctid) from usinsug01.machine_lc
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate <= cla.occurdat
                                                                                                                                and (nulldate is null or nulldate >= cla.occurdat)
                                                                                                                                and statusva not in ('2','3'))),
                                                                                                                        (   select max(ctid) from usinsug01.machine_lc
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate <= cla.occurdat
                                                                                                                                and (nulldate is null or nulldate < cla.occurdat)
                                                                                                                                and statusva not in ('2','3'))),
                                                                                                                        (   select min(ctid) from usinsug01.machine_lc
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate > cla.occurdat
                                                                                                                                and (nulldate is null or nulldate > cla.occurdat)
                                                                                                                                and statusva not in ('2','3'))),
                                                                                                                        (   select min(ctid) from usinsug01.machine_lc
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif
                                                                                                                                and statusva not in ('2','3'))),
                                                                                                                        (   select min(ctid) from usinsug01.machine_lc
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif
                                                                                                                                and statusva in ('2','3')))))
                                                                                                when 	'risk_3d'
                                                                                                then 	(	select	statusva
                                                                                                                        from	usinsug01.risk_3d
                                                                                                                        where	ctid = 
                                                                                                                        (	coalesce(coalesce(coalesce(coalesce(coalesce(
                                                                                                                        (   select max(ctid) from usinsug01.risk_3d
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate <= cla.occurdat
                                                                                                                                and (nulldate is null or nulldate > cla.occurdat)
                                                                                                                                and statusva not in ('2','3')),
                                                                                                                        (   select max(ctid) from usinsug01.risk_3d
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate <= cla.occurdat
                                                                                                                                and (nulldate is null or nulldate >= cla.occurdat)
                                                                                                                                and statusva not in ('2','3'))),
                                                                                                                        (   select max(ctid) from usinsug01.risk_3d
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate <= cla.occurdat
                                                                                                                                and (nulldate is null or nulldate < cla.occurdat)
                                                                                                                                and statusva not in ('2','3'))),
                                                                                                                        (   select min(ctid) from usinsug01.risk_3d
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate > cla.occurdat
                                                                                                                                and (nulldate is null or nulldate > cla.occurdat)
                                                                                                                                and statusva not in ('2','3'))),
                                                                                                                        (   select min(ctid) from usinsug01.risk_3d
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif
                                                                                                                                and statusva not in ('2','3'))),
                                                                                                                        (   select min(ctid) from usinsug01.risk_3d
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif
                                                                                                                                and statusva in ('2','3')))))
                                                                                                when 	'ship'
                                                                                                then 	(	select	statusva
                                                                                                                        from	usinsug01.ship
                                                                                                                        where	ctid = 
                                                                                                                        (	coalesce(coalesce(coalesce(coalesce(coalesce(
                                                                                                                        (   select max(ctid) from usinsug01.ship
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate <= cla.occurdat
                                                                                                                                and (nulldate is null or nulldate > cla.occurdat)
                                                                                                                                and statusva not in ('2','3')),
                                                                                                                        (   select max(ctid) from usinsug01.ship
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate <= cla.occurdat
                                                                                                                                and (nulldate is null or nulldate >= cla.occurdat)
                                                                                                                                and statusva not in ('2','3'))),
                                                                                                                        (   select max(ctid) from usinsug01.ship
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate <= cla.occurdat
                                                                                                                                and (nulldate is null or nulldate < cla.occurdat)
                                                                                                                                and statusva not in ('2','3'))),
                                                                                                                        (   select min(ctid) from usinsug01.ship
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate > cla.occurdat
                                                                                                                                and (nulldate is null or nulldate > cla.occurdat)
                                                                                                                                and statusva not in ('2','3'))),
                                                                                                                        (   select min(ctid) from usinsug01.ship
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif
                                                                                                                                and statusva not in ('2','3'))),
                                                                                                                        (   select min(ctid) from usinsug01.ship
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif
                                                                                                                                and statusva in ('2','3')))))
                                                                                                when 	'theft'
                                                                                                then 	(	select	statusva
                                                                                                                        from	usinsug01.theft
                                                                                                                        where	ctid = 
                                                                                                                        (	coalesce(coalesce(coalesce(coalesce(coalesce(
                                                                                                                        (   select max(ctid) from usinsug01.theft
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate <= cla.occurdat
                                                                                                                                and (nulldate is null or nulldate > cla.occurdat)
                                                                                                                                and statusva not in ('2','3')),
                                                                                                                        (   select max(ctid) from usinsug01.theft
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate <= cla.occurdat
                                                                                                                                and (nulldate is null or nulldate >= cla.occurdat)
                                                                                                                                and statusva not in ('2','3'))),
                                                                                                                        (   select max(ctid) from usinsug01.theft
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate <= cla.occurdat
                                                                                                                                and (nulldate is null or nulldate < cla.occurdat)
                                                                                                                                and statusva not in ('2','3'))),
                                                                                                                        (   select min(ctid) from usinsug01.theft
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate > cla.occurdat
                                                                                                                                and (nulldate is null or nulldate > cla.occurdat)
                                                                                                                                and statusva not in ('2','3'))),
                                                                                                                        (   select min(ctid) from usinsug01.theft
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif
                                                                                                                                and statusva not in ('2','3'))),
                                                                                                                        (   select min(ctid) from usinsug01.theft
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif
                                                                                                                                and statusva in ('2','3')))))
                                                                                                when 	'transport'
                                                                                                then 	(	select	statusva
                                                                                                                        from	usinsug01.transport
                                                                                                                        where	ctid = 
                                                                                                                        (	coalesce(coalesce(coalesce(coalesce(coalesce(
                                                                                                                        (   select max(ctid) from usinsug01.transport
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate <= cla.occurdat
                                                                                                                                and (nulldate is null or nulldate > cla.occurdat)
                                                                                                                                and statusva not in ('2','3')),
                                                                                                                        (   select max(ctid) from usinsug01.transport
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate <= cla.occurdat
                                                                                                                                and (nulldate is null or nulldate >= cla.occurdat)
                                                                                                                                and statusva not in ('2','3'))),
                                                                                                                        (   select max(ctid) from usinsug01.transport
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate <= cla.occurdat
                                                                                                                                and (nulldate is null or nulldate < cla.occurdat)
                                                                                                                                and statusva not in ('2','3'))),
                                                                                                                        (   select min(ctid) from usinsug01.transport
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate > cla.occurdat
                                                                                                                                and (nulldate is null or nulldate > cla.occurdat)
                                                                                                                                and statusva not in ('2','3'))),
                                                                                                                        (   select min(ctid) from usinsug01.transport
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif
                                                                                                                                and statusva not in ('2','3'))),
                                                                                                                        (   select min(ctid) from usinsug01.transport
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif
                                                                                                                                and statusva in ('2','3')))))
                                                                                                when 	'trec'
                                                                                                then 	(	select	statusva
                                                                                                                        from	usinsug01.trec
                                                                                                                        where	ctid = 
                                                                                                                        (	coalesce(coalesce(coalesce(coalesce(coalesce(
                                                                                                                        (   select max(ctid) from usinsug01.trec
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate <= cla.occurdat
                                                                                                                                and (nulldate is null or nulldate > cla.occurdat)
                                                                                                                                and statusva not in ('2','3')),
                                                                                                                        (   select max(ctid) from usinsug01.trec
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate <= cla.occurdat
                                                                                                                                and (nulldate is null or nulldate >= cla.occurdat)
                                                                                                                                and statusva not in ('2','3'))),
                                                                                                                        (   select max(ctid) from usinsug01.trec
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate <= cla.occurdat
                                                                                                                                and (nulldate is null or nulldate < cla.occurdat)
                                                                                                                                and statusva not in ('2','3'))),
                                                                                                                        (   select min(ctid) from usinsug01.trec
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate > cla.occurdat
                                                                                                                                and (nulldate is null or nulldate > cla.occurdat)
                                                                                                                                and statusva not in ('2','3'))),
                                                                                                                        (   select min(ctid) from usinsug01.trec
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif
                                                                                                                                and statusva not in ('2','3'))),
                                                                                                                        (   select min(ctid) from usinsug01.trec
                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif
                                                                                                                                and statusva in ('2','3')))))
                                                                                                else  '' end,
                                                                        coalesce(case	when	coalesce(cla.certif,0) = 0
                                                                                                        then	pol.status_pol
                                                                                                        else	coalesce((	select	cer.statusva
                                                                                                                                                from 	usinsug01.certificat cer
                                                                                                                                                where	cer.usercomp = cla.usercomp 
                                                                                                                                                and 	cer.company = cla.company
                                                                                                                                                and 	cer.certype = pol.certype
                                                                                                                                                and 	cer.branch = cla.branch 
                                                                                                                                                and 	cer.policy = cla.policy
                                                                                                                                                and		cer.certif = cla.certif),'')
                                                                                                        end), '') KACESTAP,
                                                        cast(case	coalesce(cla.certif,0)
                                                                                when	0
                                                                                then	case	when	coalesce(pol.payfreq,'0') = '1'
                                                                                                                then	1
                                                                                                                else	0 end
                                                                                else	case	when	coalesce((	select	cer.payfreq
                                                                                                                                                        from 	usinsug01.certificat cer
                                                                                                                                                        where	cer.usercomp = cla.usercomp 
                                                                                                                                                        and 	cer.company = cla.company
                                                                                                                                                        and 	cer.certype = pol.certype
                                                                                                                                                        and 	cer.branch = cla.branch 
                                                                                                                                                        and 	cer.policy = cla.policy
                                                                                                                                                        and		cer.certif = cla.certif),'0') = '1'
                                                                                                                then	1
                                                                                                                else	0 end
                                                                                end as varchar) DTERMO,
                                                        '' TULTALT, --excluido
                                                        '' DUSRUPD, --excluido
                                                        '' KABUNRIS, --SE DESCONOCE ABUNRIS.PK
                                                        '' DUSRENT, --excluido
                                                        '' DUSRSUP, --excluido
                                                        coalesce(cast(cla.staclaim as varchar),'') KSCESTSN,
                                                        '' KSCFMPTI, --excluido
                                                        '' KSCTPAUT, --excluido
                                                        '' KSCLOCSN, --excluido
                                                        '' KSCINBMT, --excluido
                                                        '' KSCINIDS, --excluido
                                                        '' DPROCIDS, --excluido
                                                        '' KSCETIDS, --excluido
                                                        'LPG' DCOMPA,
                                                        '' DMARCA, --excluido
                                                        '' KSCNATZA_IN, --excluido
                                                        '' KSCNATZA_FI, --excluido
                                                        '' KSCCDEST, --excluido
                                                        '' KACARGES, --excluido
                                                        '' DNUMOBJ, --excluido
                                                        '' DNUMOB2, --excluido
                                                        coalesce(	case	/*(	select	"RISKTYPEN"
                                                                                                        from	usbi01."IFRS170_T_RAMOS_POR_TIPO_RIESGO"
                                                                                                        where	"SOURCESCHEMA" = 'usinsug01'
                                                                                                        and		"BRANCHCOM" = cla.branch)*/
                                                                                                --when	3
                                                                                                when	cla.branch in (6,15,26,29,62,66,67)
                                                                                                then	coalesce((	select	case	when coalesce(trim(tnb.regist),'') <> '' and coalesce(trim(tnb.chassis),'') <> ''
                                                                                                                                                                        then coalesce(trim(tnb.regist),'') || '-' || coalesce(trim(tnb.chassis),'')
                                                                                                                                                                        when coalesce(trim(tnb.regist),'') <> '' and coalesce(trim(tnb.chassis),'') = ''
                                                                                                                                                                        then coalesce(trim(tnb.regist),'')
                                                                                                                                                                        when coalesce(trim(tnb.regist),'') = '' and coalesce(trim(tnb.chassis),'') <> ''
                                                                                                                                                                        then coalesce(trim(tnb.chassis),'')
                                                                                                                                                                        else '' end
                                                                                                                                        from	usinsug01.auto_peru tnb
                                                                                                                                        where	ctid = 
                                                                                                                                                        (	coalesce(coalesce(coalesce(coalesce(coalesce(
                                                                                                                                        (   select max(ctid) from usinsug01.auto_peru
                                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate <= cla.occurdat
                                                                                                                                                and (nulldate is null or nulldate > cla.occurdat)
                                                                                                                                                and statusva not in ('2','3')),
                                                                                                                                        (   select max(ctid) from usinsug01.auto_peru
                                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate <= cla.occurdat
                                                                                                                                                and (nulldate is null or nulldate >= cla.occurdat)
                                                                                                                                                and statusva not in ('2','3'))),
                                                                                                                                        (   select max(ctid) from usinsug01.auto_peru
                                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate <= cla.occurdat
                                                                                                                                                and (nulldate is null or nulldate < cla.occurdat)
                                                                                                                                                and statusva not in ('2','3'))),
                                                                                                                                        (   select min(ctid) from usinsug01.auto_peru
                                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate > cla.occurdat
                                                                                                                                                and (nulldate is null or nulldate > cla.occurdat)
                                                                                                                                                and statusva not in ('2','3'))),
                                                                                                                                        (   select min(ctid) from usinsug01.auto_peru
                                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif
                                                                                                                                                and statusva not in ('2','3'))),
                                                                                                                                        (   select min(ctid) from usinsug01.auto_peru
                                                                                                                                                where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                                and branch = cla.branch and policy = cla.policy and certif = cla.certif
                                                                                                                                                and statusva in ('2','3'))))),'')
                                                                                                --when	2
                                                                                                when	cla.branch in (1,2,3,4,7,8,9,10,11,12,13,14,16,17,18,19,28,30,38,39,55,57,58)
                                                                                                then	cla.branch || '-' || cla.policy || '-' || cla.certif
                                                                                                --when	1
                                                                                                when	cla.branch in (5,21,22,23,24,25,27,31,32,33,34,35,36,37,40,41,42,59,68,71,75,77,91,99)
                                                                                                then	coalesce(coalesce(
                                                                                                                                        (	select	max(sclient_vig)
                                                                                                                                                from	usinsug01.wbstblclidepequi
                                                                                                                                                where	sclient_old = 
                                                                                                                                                                (	select	evi.scod_vt
                                                                                                                                                                        from	usinsug01.equi_vt_inx evi 
                                                                                                                                                                        where	evi.scod_inx = cla.client)),
                                                                                                                                        (	select	evi.scod_vt
                                                                                                                                                from	usinsug01.equi_vt_inx evi 
                                                                                                                                                where	evi.scod_inx = cla.client)),
                                                                                                                        cla.client)
                                                                                                else 	'' end, '') DUNIDRISC,
                                                        coalesce((	select	cast(max(operdate) as varchar)
                                                                                from	usinsug01.claim_his
                                                                                where	claim = cla.claim
                                                                                and		oper_type = '16'),'') TDTREABE,
                                                        '' DEQREGUL, --excluido
                                                        '' KACMOEST, --excluido
                                                        '' KACCONCE, --excluido
                                                        '' KSCCONTE, --excluido
                                                        '' KSCSTREE, --no disponible
                                                        cast(coalesce(	case	when	pol.bussityp = '2'
                                                                                                        then	pol.leadshare
                                                                                                        else	cl0.share_coa
                                                                                                        end,0) as numeric(7,4)) VTXCOSEG,
                                                        '' KSCPAIS, --no disponible
                                                        '' KSCDEFRP, --excluido
                                                        '' KSCORPAR, --excluido
                                                        '' DTPRCAS, --excluido
                                                        '' TDTESTAD, --no disponible
                                                        '' KSBSIN_MP, --excluido
                                                        '' TMIGPARA, --excluido
                                                        '' KSBSIN_MD, --excluido
                                                        '' TMIGDE, --excluido
                                                        cla.branch || '-' || pol.product || '-' || cla.policy || '-' || cla.certif KABAPOL,
                                                        '' TPRENCER, --excluido
                                                        '' TDTREEMB, --no disponible
                                                        '' TENTPLAT, --excluido
                                                        '' DHENTPLA, --excluido
                                                        '' TENTCOMP, --excluido
                                                        '' DHENTCOM, --excluido
                                                        '' TPEDPART, --excluido
                                                        '' TDTRECLA, --excluido
                                                        '' TDECFIN, --excluido
                                                        '' TASSRESP, --excluido
                                                        '' DINDENCU, --excluido
                                                        '' KSCMTENC, --excluido
                                                        '' DQTDAAA, --excluido
                                                        '' DINFACTO, --excluido
                                                        '' TINISUSP, --excluido
                                                        '' TFIMSUSP, --excluido
                                                        '' DINSOPRE, --excluido
                                                        '' KSCTPDAN, --excluido
                                                        '' KABAPOL_EFT, --excluido
                                                        '' DARQUIVO, --excluido
                                                        '' TARQUIVO, --excluido
                                                        '' DLOCREF, --excluido
                                                        '' KACPARES, --no disponible
                                                        '' KGCRAMO_SAP, --excluido
                                                        '' DNUMPGRE, --no disponible
                                                        '' DINDSINTER, --FALTA C�LCULO
                                                        '' DQDREABER, --excluido
                                                        '' TPLANOCOSEG, --excluido
                                                        '' TPLANORESEG, --excluido
                                                        case	when pol.bussityp = '1' and cl0.share_coa = 100 then '0' --Paga todo
                                                                        when pol.bussityp = '1' and cl0.share_coa <> 100 then '1' --No paga todo
                                                                        when pol.bussityp = '2' then '0' --Paga todo
                                                                        else '0' end KSCPAGCSG,
                                                        '' KSCAPLGES, --excluido
                                                        '' DANUAGR, --excluido
                                                        '' DENTIDSO, --excluido
                                                        '' DNOFSIN, --excluido
                                                        '' DIMAGEM, --excluido
                                                        '' KEBENTID_GS, --excluido
                                                        '' KOCSCOPE, --excluido
                                                        '' DCDINTTRA --excluido
                                        from	(	select	clh.cla_id,
                                                                                clh.pol_id,
                                                                                clh.share_coa,
                                                                                clh.share_rea,
                                                                                sum(clh.monto_trans * case clh.tipo when 1 then 1 else 0 end ) reserva,
                                                                                sum(clh.monto_trans * case clh.tipo when 2 then 1 else 0 end ) ajustes,
                                                                                sum(clh.monto_trans * case clh.tipo when 3 then 1 else 0 end ) pagos
                                                                from	(	select 	cla.*,
                                                                                                        tcl.tipo,
                                                                                                        coalesce(	case	when	clh.currency = cla.moneda_cod
                                                                                                                                                then    clh.amount
                                                                                                                                                else	case	when	cla.moneda_cod = 1
                                                                                                                                                                                then	clh.amount *
                                                                                                                                                                                                case	when	clh.currency = 2
                                                                                                                                                                                                                then	(	select	max(clh0.exchange)
                                                                                                                                                                                                                                        from	usinsug01.claim_his clh0
                                                                                                                                                                                                                                        where	clh0.claim = clh.claim
                                                                                                                                                                                                                                        and		clh0.transac =
                                                                                                                                                                                                                                                        (	select	max(clh1.transac)
                                                                                                                                                                                                                                                                from	usinsug01.claim_his clh1
                                                                                                                                                                                                                                                                where 	clh1.claim = clh.claim
                                                                                                                                                                                                                                                                and		clh1.transac <= clh.transac
                                                                                                                                                                                                                                                                and		clh1.exchange not in (1,0)))
                                                                                                                                                                                                                else    1 end
                                                                                                                                                                                when	cla.moneda_cod = 2
                                                                                                                                                                                then	clh.amount /
                                                                                                                                                                                                case	when	clh.currency = 1
                                                                                                                                                                                                                then	(	select	max(clh0.exchange)
                                                                                                                                                                                                                                        from	usinsug01.claim_his clh0
                                                                                                                                                                                                                                        where	clh0.claim = clh.claim
                                                                                                                                                                                                                                        and		clh0.transac =
                                                                                                                                                                                                                                                        (	select	max(clh1.transac)
                                                                                                                                                                                                                                                                from	usinsug01.claim_his clh1
                                                                                                                                                                                                                                                                where 	clh1.claim = clh.claim
                                                                                                                                                                                                                                                                and		clh1.transac <= clh.transac
                                                                                                                                                                                                                                                                and		clh1.exchange not in (1,0)))
                                                                                                                                                                                                                else    1 end
                                                                                                                                                                                else	0 end end, 0) monto_trans
                                                                                        from 	(	select 	case 	when 	(cla.staclaim <> '6' or (cla.branch = 23 and cla.staclaim = '6')) and pol.certype = '2'
                                                                                                                                                                and 1 = (select t10b.company from usinsug01.table10b t10b where t10b.branch = cla.branch)
                                                                                                                                                then	cla.claim
                                                                                                                                                else 	null end claim,
                                                                                                                                case 	when 	(cla.staclaim <> '6' or (cla.branch = 23 and cla.staclaim = '6')) and pol.certype = '2'
                                                                                                                                                then	cla.ctid 
                                                                                                                                                else	null end cla_id,
                                                                                                                                case	when	pol.certype = '2'
                                                                                                                                                then	pol.ctid
                                                                                                                                                else	null end pol_id,
                                                                                                                                case 	when 	(cla.staclaim <> '6' or (cla.branch = 23 and cla.staclaim = '6')) and pol.certype = '2'
                                                                                                                                                                and 1 = (select t10b.company from usinsug01.table10b t10b where t10b.branch = cla.branch)
                                                                                                                                                then	case	pol.bussityp
                                                                                                                                                                                when	'2'
                                                                                                                                                                                then 	100 - coalesce(pol.leadshare,0)
                                                                                                                                                                                when	'3'
                                                                                                                                                                                then 	null
                                                                                                                                                                                else	coalesce((	select	coi.share
                                                                                                                                                                                                                        from	usinsug01.coinsuran coi
                                                                                                                                                                                                                        where   coi.usercomp = cla.usercomp
                                                                                                                                                                                                                        and     coi.company = cla.company
                                                                                                                                                                                                                        and     coi.certype = pol.certype
                                                                                                                                                                                                                        and     coi.branch = cla.branch
                                                                                                                                                                                                                        and     coi.policy = cla.policy
                                                                                                                                                                                                                        and     coi.effecdate <= cla.occurdat
                                                                                                                                                                                                                        and     (coi.nulldate is null or coi.nulldate > cla.occurdat)
                                                                                                                                                                                                                        and 	coalesce(coi.companyc,0) in (1,12)),100) end 
                                                                                                                                                else	0 end share_coa,
                                                                                                                                case 	when 	(cla.staclaim <> '6' or (cla.branch = 23 and cla.staclaim = '6')) and pol.certype = '2'
                                                                                                                                                                and 1 = (select t10b.company from usinsug01.table10b t10b where t10b.branch = cla.branch)
                                                                                                                                                then	coalesce((	select  sum(coalesce(rei.share,0))
                                                                                                                                                                                        from    usinsug01.reinsuran rei
                                                                                                                                                                                        where   rei.usercomp = cla.usercomp
                                                                                                                                                                                        and     rei.company = cla.company
                                                                                                                                                                                        and     rei.certype = pol.certype
                                                                                                                                                                                        and     rei.branch = cla.branch
                                                                                                                                                                                        and     rei.policy = cla.policy
                                                                                                                                                                                        and     rei.certif = cla.certif
                                                                                                                                                                                        and     rei.effecdate <= cla.occurdat
                                                                                                                                                                                        and     (rei.nulldate is null or rei.nulldate > cla.occurdat)
                                                                                                                                                                                        and     coalesce(rei.type,0) = 1),100)
                                                                                                                                                else	0 end share_rea,
                                                                                                                                case 	when 	(cla.staclaim <> '6' or (cla.branch = 23 and cla.staclaim = '6')) and pol.certype = '2'
                                                                                                                                                                and 1 = (select t10b.company from usinsug01.table10b t10b where t10b.branch = cla.branch)
                                                                                                                                                then	coalesce(	coalesce((	select	max(cpl.currency)
                                                                                                                                                                                                                from	usinsug01.curren_pol cpl
                                                                                                                                                                                                                where 	cpl.usercomp = cla.usercomp
                                                                                                                                                                                                                and 	cpl.company = cla.company
                                                                                                                                                                                                                and		cpl.certype = pol.certype
                                                                                                                                                                                                                and		cpl.branch = cla.branch
                                                                                                                                                                                                                and 	cpl.policy = cla.policy
                                                                                                                                                                                                                and		cpl.certif = cla.certif),
                                                                                                                                                                                                        (	select	max(cpl.currency)
                                                                                                                                                                                                                from	usinsug01.curren_pol cpl
                                                                                                                                                                                                                where 	cpl.usercomp = cla.usercomp
                                                                                                                                                                                                                and 	cpl.company = cla.company
                                                                                                                                                                                                                and		cpl.certype = pol.certype
                                                                                                                                                                                                                and		cpl.branch = cla.branch
                                                                                                                                                                                                                and 	cpl.policy = cla.policy)),0) 
                                                                                                                                                else	0 end moneda_cod
                                                                                                                from    usinsug01.policy pol
                                                                                                                join	(select cast('12/31/2015' as date) fecha) par on 1 = 1 --ejecutar al a�o 2021 (2015 es para pruebas)
                                                                                                                join	usinsug01.claim cla
                                                                                                                        on		cla.usercomp = pol.usercomp
                                                                                                                        and     cla.company = pol.company
                                                                                                                        and     cla.branch = pol.branch
                                                                                                                        and     cla.policy = pol.policy
                                                                                                                        and		cla.branch not in (23,66) --exclusi�n de estos ramos por el volumen de datos involucrados
                                                                                                                        and     exists
                                                                                                                                                (   select  1
                                                                                                                                                        from    (	select	cast(codigint as varchar(2)) operation
                                                                                                                                                                                from	usinsug01.table140
                                                                                                                                                                                where	codigint in 
                                                                                                                                                                                                (	select 	operation
                                                                                                                                                                                                        from	usinsug01.tab_cl_ope
                                                                                                                                                                                                        where	reserve = 1 or ajustes = 1 or pay_amount = 1)) tcl --solo pagos
                                                                                                                                                        join	usinsug01.claim_his clh on trim(clh.oper_type) = tcl.operation
                                                                                                                                                        where   coalesce (clh.claim,0) = cla.claim
                                                                                                                                                        and     clh.operdate >= par.fecha)) cla
                                                                                        join	usinsug01.claim_his clh
                                                                                                        on		clh.claim = cla.claim
                                                                                                        and		clh.operdate <= '12/31/2023' --en caso de ejecutar a una fecha posterior, alterar esta fecha a la de la ejecuci�n
                                                                                        join	(	select	case	when tcl.reserve = 1 then 1
                                                                                                                                                when tcl.ajustes = 1 then 2
                                                                                                                                                when tcl.pay_amount = 1 then 3
                                                                                                                                                else 0 end tipo,
                                                                                                                                cast(tcl.operation as varchar(2)) operation
                                                                                                                from	usinsug01.tab_cl_ope tcl
                                                                                                                where	(tcl.reserve = 1 or tcl.ajustes = 1 or tcl.pay_amount = 1)) tcl
                                                                                                        on		trim(clh.oper_type) = tcl.operation) clh
                                                                group	by 1,2,3,4) cl0
                                        join	usinsug01.claim cla on cla.ctid = cl0.cla_id
                                        join	usinsug01.policy pol on pol.ctid = cl0.pol_id
                                    ) AS TMP    
                                    '''
    L_DF_SBSIN_INSUNIX_LPG_OTROS = glue_context.read.format('jdbc').options(**connection).option("fetchsize",10000).option("dbtable",L_SBSIN_INSUNIX_LPG_OTROS).load()

    L_DF_SBSIN_INSUNIX_LPG = L_DF_SBSIN_INSUNIX_LPG_SOAT.union(L_DF_SBSIN_INSUNIX_LPG_AMF).union(L_DF_SBSIN_INSUNIX_LPG_OTROS)

    L_SBSIN_INSUNIX_LPV =  '''
                            (
                                select	        'D' INDDETREC,
                                                'SBSIN' TABLAIFRS17,
                                                '' PK,
                                                '' DTPREG, --excluido
                                                '' TIOCPROC, --no disponible
                                                coalesce(cast(cla.compdate as varchar),'') TIOCFRM,
                                                '' TIOCTO, --excluido
                                                'PIV' KGIORIGM,
                                                'LPV' KSCCOMPA,
                                                coalesce(cast(cla.branch as varchar),'') KGCRAMO,
                                                cla.branch || '-' || pol.product KABPRODT,
                                                coalesce(cast(cla.policy as varchar),'') DNUMAPO,
                                                coalesce(cast(cla.certif as varchar),'') DNMCERT,
                                                coalesce(cast(cla.claim as varchar),'') DNUMSIN,
                                                coalesce(cast(cla.occurdat as varchar),'') TOCURSIN,
                                                '' DHSINIST, --excluido
                                                coalesce(cast(cla.occurdat as varchar),'') TABERSIN,
                                                '' TPARTSIN, --excluido
                                                coalesce(
                                                        case	when	cla.staclaim in ('1','5','7')
                                                                        then	(	select	cast(max(operdate) as varchar)
                                                                                                from	usinsuv01.claim_his
                                                                                                where	claim = cla.claim
                                                                                                and		case	when	cla.staclaim in ('1') then oper_type in ('3','17')
                                                                                                                                when	cla.staclaim in ('5') then oper_type in ('11')
                                                                                                                                when	cla.staclaim in ('7') then oper_type in ('21')
                                                                                                                                else	oper_type is null end)
                                                                        else 	'' end, '') TFECHTEC,
                                                coalesce(
                                                        case	when	cla.staclaim in ('1','5','7')
                                                                        then	(	select	cast(max(operdate) as varchar)
                                                                                                from	usinsuv01.claim_his
                                                                                                where	claim = cla.claim
                                                                                                and		case	when	cla.staclaim in ('1') then oper_type in ('3','17')
                                                                                                                                when	cla.staclaim in ('5') then oper_type in ('52')
                                                                                                                                when	cla.staclaim in ('7') then oper_type in ('21')
                                                                                                                                else	oper_type is null end)
                                                                        else 	'' end, '') TFECHADM,
                                                coalesce(
                                                        (	select	cast(max(operdate) as varchar)
                                                                from	usinsuv01.claim_his
                                                                where	claim = cla.claim
                                                                and		oper_type in
                                                                                (	select	cast(codigint as varchar(2))
                                                                                        from	usinsug01.table140
                                                                                        where	(	codigint in 
                                                                                                                (	select	operation
                                                                                                                        from	usinsug01.tab_cl_ope
                                                                                                                        where	(reserve = 1 or ajustes = 1 or pay_amount in (1,3)))) 
                                                                                                        or codigint in (60))), '') TESTADO,
                                                coalesce(cast(cla.staclaim as varchar),'') KSCSITUA,
                                                '' KSCMOTSI, --excluido
                                                '' KSCTPSIN, --no disponible
                                                coalesce(cast(cla.causecod as varchar),'') KSCCAUSA,
                                                '' KSCARGES, --excluido
                                                '' KSCFMPGS, --no disponible
                                                '' KCBMED_DRA, --excluido
                                                '' KCBMED_PG, --excluido
                                                '' KCBMED_PD, --excluido
                                                '' KCBMED_P2, --excluido
                                                case	when pol.bussityp = '1' and cl0.share_coa = 100 then '1' --Sin coaseguro
                                                                when pol.bussityp = '1' and cl0.share_coa <> 100 then '2' --Con coaseguro, compa��a l�der
                                                                when pol.bussityp = '2' then '3' --Con coaseguro, compa��a no l�der
                                                                else '0' end KSCTPCSG,
                                                cast(coalesce(
                                                                coalesce((	select	max(cpl.currency)
                                                                                        from	usinsuv01.curren_pol cpl
                                                                                        where 	cpl.usercomp = cla.usercomp
                                                                                        and 	cpl.company = cla.company
                                                                                        and		cpl.certype = pol.certype
                                                                                        and		cpl.branch = cla.branch
                                                                                        and 	cpl.policy = cla.policy
                                                                                        and		cpl.certif = cla.certif),
                                                                                (	select	max(cpl.currency)
                                                                                        from	usinsuv01.curren_pol cpl
                                                                                        where 	cpl.usercomp = cla.usercomp
                                                                                        and 	cpl.company = cla.company
                                                                                        and		cpl.certype = pol.certype
                                                                                        and		cpl.branch = cla.branch
                                                                                        and 	cpl.policy = cla.policy)),0) as varchar) KSCMOEDA,
                                                '' VCAMBIO, --no disponible
                                                '' VTXRESPN, --no disponible
                                                cast(cl0.reserva + cl0.ajustes as numeric(14,2)) VMTPROVI,
                                                '' VMTPRVINI, --excluido
                                                cast((cl0.reserva + cl0.ajustes) * (case when pol.bussityp = '2' then 100 else share_coa end/100) as numeric(14,2)) VMTPRVRS, --no hay reaseguro en LPV INX
                                                cast((cl0.reserva + cl0.ajustes) * (case when pol.bussityp = '2' then 100 else share_coa end/100) as numeric(14,2)) VMTCOSEG,
                                                '' KSCNATUR, --excluido
                                                '' TALTENAT, --excluido
                                                '' DUSRREG, --excluido
                                                coalesce(coalesce(
                                                                        (	select	max(sclient_vig)
                                                                                from	usinsug01.wbstblclidepequi
                                                                                where	sclient_old = 
                                                                                                (	select	evi.scod_vt
                                                                                                        from	usinsug01.equi_vt_inx evi 
                                                                                                        where	evi.scod_inx = cla.client)),
                                                                        (	select	evi.scod_vt
                                                                                from	usinsug01.equi_vt_inx evi 
                                                                                where	evi.scod_inx = cla.client)),
                                                        cla.client) KEBENTID_TO,
                                                cast(coalesce(	case    when    coalesce((  select  distinct pro.brancht
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
                                                                                                                                        coalesce(coalesce(coalesce(coalesce(
                                                                                                                                                                                                (   select  max(ctid)
                                                                                                                                                                                                        from	usinsuv01.gen_cover
                                                                                                                                                                                                        where   usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = pol.product and currency = cov.currency --�ndice regular
                                                                                                                                                                                                        and		modulec = cov.modulec and cover = cov.cover --variaci�n 1 con modulec
                                                                                                                                                                                                        and		effecdate <= cla.occurdat and (nulldate is null or nulldate > cla.occurdat) --variaci�n 2 vigencias
                                                                                                                                                                                                        and		statregt <> '4' and addsuini = '1'), --variaci�n 3 reg. v�lido
                                                                                                                                                                                                (   select  max(ctid)
                                                                                                                                                                                                        from	usinsuv01.gen_cover
                                                                                                                                                                                                        where   usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = pol.product and currency = cov.currency --�ndice regular
                                                                                                                                                                                                        and		cover = cov.cover --variaci�n 1 sin modulec
                                                                                                                                                                                                        and		effecdate <= cla.occurdat and (nulldate is null or nulldate > cla.occurdat) --variaci�n 2 vigencias
                                                                                                                                                                                                        and		statregt <> '4' and addsuini = '1')), --variaci�n 3 reg. v�lido
                                                                                                                                                                                coalesce((  select  max(ctid)
                                                                                                                                                                                                        from	usinsuv01.gen_cover
                                                                                                                                                                                                        where   usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = pol.product and currency = cov.currency --�ndice regular
                                                                                                                                                                                                        and		modulec = cov.modulec and cover = cov.cover
                                                                                                                                                                                                        and		effecdate <= cla.occurdat and (nulldate is null or nulldate > cla.occurdat)
                                                                                                                                                                                                        and		statregt = '4' and addsuini = '1'),
                                                                                                                                                                                                (   select  max(ctid)
                                                                                                                                                                                                        from	usinsuv01.gen_cover
                                                                                                                                                                                                        where   usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = pol.product and currency = cov.currency --�ndice regular
                                                                                                                                                                                                        and		cover = cov.cover
                                                                                                                                                                                                        and		effecdate <= cla.occurdat and (nulldate is null or nulldate > cla.occurdat)
                                                                                                                                                                                                        and		statregt = '4' and addsuini = '1'))), --no est� cortado
                                                                                                                                                                coalesce(coalesce(( select  max(ctid)
                                                                                                                                                                                                        from	usinsuv01.gen_cover
                                                                                                                                                                                                        where   usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = pol.product and currency = cov.currency --�ndice regular
                                                                                                                                                                                                        and		modulec = cov.modulec and cover = cov.cover
                                                                                                                                                                                                        and		effecdate <= cla.occurdat and nulldate <= cla.occurdat
                                                                                                                                                                                                        and		statregt <> '4' and addsuini = '1'),
                                                                                                                                                                                                (   select  max(ctid)
                                                                                                                                                                                                        from	usinsuv01.gen_cover
                                                                                                                                                                                                        where   usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = pol.product and currency = cov.currency --�ndice regular
                                                                                                                                                                                                        and		cover = cov.cover
                                                                                                                                                                                                        and		effecdate <= cla.occurdat and nulldate <= cla.occurdat
                                                                                                                                                                                                        and		statregt <> '4' and addsuini = '1')), -- no est� cortado, pero fue anulado antes del efecto del registro
                                                                                                                                                                        coalesce((  select  max(ctid)
                                                                                                                                                                                                from	usinsuv01.gen_cover
                                                                                                                                                                                                where   usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = pol.product and currency = cov.currency --�ndice regular
                                                                                                                                                                                                and		modulec = cov.modulec and cover = cov.cover
                                                                                                                                                                                                and		effecdate <= cla.occurdat and nulldate <= cla.occurdat
                                                                                                                                                                                                and		statregt = '4' and addsuini = '1'),
                                                                                                                                                                                        (   select  max(ctid)
                                                                                                                                                                                                from	usinsuv01.gen_cover
                                                                                                                                                                                                where   usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = pol.product and currency = cov.currency --�ndice regular
                                                                                                                                                                                                and		cover = cov.cover
                                                                                                                                                                                                and		effecdate <= cla.occurdat and nulldate <= cla.occurdat
                                                                                                                                                                                                and		statregt = '4' and addsuini = '1')))), --no est� cortado y aparte fue anulado antes del efecto del registro
                                                                                                                                                coalesce(coalesce(( select  max(ctid)
                                                                                                                                                                                        from	usinsuv01.gen_cover
                                                                                                                                                                                        where   usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = pol.product and currency = cov.currency --�ndice regular
                                                                                                                                                                                        and		modulec = cov.modulec and cover = cov.cover
                                                                                                                                                                                        and		effecdate > cla.occurdat
                                                                                                                                                                                        and		statregt <> '4' and addsuini = '1'),
                                                                                                                                                                                (   select  max(ctid)
                                                                                                                                                                                        from	usinsuv01.gen_cover
                                                                                                                                                                                        where   usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = pol.product and currency = cov.currency --�ndice regular
                                                                                                                                                                                        and		cover = cov.cover
                                                                                                                                                                                        and		effecdate <= cla.occurdat and nulldate <= cla.occurdat
                                                                                                                                                                                        and		statregt <> '4' and addsuini = '1')), -- no est� cortado, pero tampoco al efecto de la tabla de datos particular
                                                                                                                                                        coalesce((  select  max(ctid)
                                                                                                                                                                                from	usinsuv01.gen_cover
                                                                                                                                                                                where   usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = pol.product and currency = cov.currency --�ndice regular
                                                                                                                                                                                and		modulec = cov.modulec and cover = cov.cover
                                                                                                                                                                                and		effecdate <= cla.occurdat and nulldate <= cla.occurdat
                                                                                                                                                                                and		statregt = '4' and addsuini = '1'),
                                                                                                                                                                        (   select  max(ctid)
                                                                                                                                                                                from	usinsuv01.gen_cover
                                                                                                                                                                                where   usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = pol.product and currency = cov.currency --�ndice regular
                                                                                                                                                                                and		cover = cov.cover
                                                                                                                                                                                and		effecdate > cla.occurdat
                                                                                                                                                                                and		statregt = '4' and addsuini = '1')))) --est� cortado y no al efecto de la tabla de datos particular adem�s de estar cortado
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
                                                                                                                                        coalesce(coalesce(coalesce(
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
                                                                                                                                                                                        and		statregt = '4'  and addcapii = '1')),--est� cortado
                                                                                                                                                                coalesce((  select  max(ctid)
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
                                                                                                                                                                                        and		statregt = '4' and addcapii = '1'))), --est� cortado y aparte fue anulado antes del efecto del registro
                                                                                                                                                coalesce((  select  max(ctid)
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
                                                                                                                                                        and		statregt = '4' and addcapii = '1')))
                                                                                                                        where	cov.usercomp = cla.usercomp
                                                                                                                        and		cov.company = cla.company
                                                                                                                        and		cov.certype = pol.certype
                                                                                                                        and		cov.branch = cla.branch
                                                                                                                        and		cov.policy = cla.policy
                                                                                                                        and		cov.certif = cla.certif
                                                                                                                        and		cov.effecdate <= cla.occurdat
                                                                                                                        and		(cov.nulldate is null or cov.nulldate > cla.occurdat))
                                                                                        end, 0) as numeric(14,2)) VCAPITAL,
                                                '' VTXINDEM, --no disponible
                                                '' VMTINDEM, --no disponible
                                                '' TINICIND, --no disponible
                                                '' DULTACTA, --excluido
                                /*
                                                coalesce(	case	trim((select distinct(tabname) from usinsug01.tab_name_b where branch = cla.branch))
                                                                                        when 	'life_prev'
                                                                                        then 	(	select	statusva
                                                                                                                from	usinsuv01.life_prev
                                                                                                                where	ctid = 
                                                                                                                (	coalesce(coalesce(coalesce(coalesce(coalesce(
                                                                                                                (   select max(ctid) from usinsuv01.life_prev
                                                                                                                        where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                        and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate <= cla.occurdat
                                                                                                                        and (nulldate is null or nulldate > cla.occurdat)
                                                                                                                        and statusva not in ('2','3')),
                                                                                                                (   select max(ctid) from usinsuv01.life_prev
                                                                                                                        where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                        and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate <= cla.occurdat
                                                                                                                        and (nulldate is null or nulldate >= cla.occurdat)
                                                                                                                        and statusva not in ('2','3'))),
                                                                                                                (   select max(ctid) from usinsuv01.life_prev
                                                                                                                        where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                        and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate <= cla.occurdat
                                                                                                                        and (nulldate is null or nulldate < cla.occurdat)
                                                                                                                        and statusva not in ('2','3'))),
                                                                                                                (   select min(ctid) from usinsuv01.life_prev
                                                                                                                        where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                        and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate > cla.occurdat
                                                                                                                        and (nulldate is null or nulldate > cla.occurdat)
                                                                                                                        and statusva not in ('2','3'))),
                                                                                                                (   select min(ctid) from usinsuv01.life_prev
                                                                                                                        where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                        and branch = cla.branch and policy = cla.policy and certif = cla.certif
                                                                                                                        and statusva not in ('2','3'))),
                                                                                                                (   select min(ctid) from usinsuv01.life_prev
                                                                                                                        where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                        and branch = cla.branch and policy = cla.policy and certif = cla.certif
                                                                                                                        and statusva in ('2','3')))))
                                                                                        when 	'life'
                                                                                        then 	(	select	statusva
                                                                                                                from	usinsuv01.life
                                                                                                                where	ctid = 
                                                                                                                (	coalesce(coalesce(coalesce(coalesce(coalesce(
                                                                                                                (   select max(ctid) from usinsuv01.life
                                                                                                                        where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                        and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate <= cla.occurdat
                                                                                                                        and (nulldate is null or nulldate > cla.occurdat)
                                                                                                                        and statusva not in ('2','3')),
                                                                                                                (   select max(ctid) from usinsuv01.life
                                                                                                                        where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                        and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate <= cla.occurdat
                                                                                                                        and (nulldate is null or nulldate >= cla.occurdat)
                                                                                                                        and statusva not in ('2','3'))),
                                                                                                                (   select max(ctid) from usinsuv01.life
                                                                                                                        where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                        and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate <= cla.occurdat
                                                                                                                        and (nulldate is null or nulldate < cla.occurdat)
                                                                                                                        and statusva not in ('2','3'))),
                                                                                                                (   select min(ctid) from usinsuv01.life
                                                                                                                        where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                        and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate > cla.occurdat
                                                                                                                        and (nulldate is null or nulldate > cla.occurdat)
                                                                                                                        and statusva not in ('2','3'))),
                                                                                                                (   select min(ctid) from usinsuv01.life
                                                                                                                        where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                        and branch = cla.branch and policy = cla.policy and certif = cla.certif
                                                                                                                        and statusva not in ('2','3'))),
                                                                                                                (   select min(ctid) from usinsuv01.life
                                                                                                                        where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                        and branch = cla.branch and policy = cla.policy and certif = cla.certif
                                                                                                                        and statusva in ('2','3')))))
                                                                                        when 	'health'
                                                                                        then 	(	select	statusva
                                                                                                                from	usinsuv01.health
                                                                                                                where	ctid = 
                                                                                                                (	coalesce(coalesce(coalesce(coalesce(coalesce(
                                                                                                                (   select max(ctid) from usinsuv01.health
                                                                                                                        where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                        and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate <= cla.occurdat
                                                                                                                        and (nulldate is null or nulldate > cla.occurdat)
                                                                                                                        and statusva not in ('2','3')),
                                                                                                                (   select max(ctid) from usinsuv01.health
                                                                                                                        where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                        and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate <= cla.occurdat
                                                                                                                        and (nulldate is null or nulldate >= cla.occurdat)
                                                                                                                        and statusva not in ('2','3'))),
                                                                                                                (   select max(ctid) from usinsuv01.health
                                                                                                                        where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                        and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate <= cla.occurdat
                                                                                                                        and (nulldate is null or nulldate < cla.occurdat)
                                                                                                                        and statusva not in ('2','3'))),
                                                                                                                (   select min(ctid) from usinsuv01.health
                                                                                                                        where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                        and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate > cla.occurdat
                                                                                                                        and (nulldate is null or nulldate > cla.occurdat)
                                                                                                                        and statusva not in ('2','3'))),
                                                                                                                (   select min(ctid) from usinsuv01.health
                                                                                                                        where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                        and branch = cla.branch and policy = cla.policy and certif = cla.certif
                                                                                                                        and statusva not in ('2','3'))),
                                                                                                                (   select min(ctid) from usinsuv01.health
                                                                                                                        where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                        and branch = cla.branch and policy = cla.policy and certif = cla.certif
                                                                                                                        and statusva in ('2','3')))))
                                                                                        else  '' end,
                                                                null
                                                                coalesce(case	when	coalesce(cla.certif,0) = 0
                                                                                                then	pol.status_pol
                                                                                                else	coalesce((	select	cer.statusva
                                                                                                                                        from 	usinsuv01.certificat cer
                                                                                                                                        where	cer.usercomp = cla.usercomp 
                                                                                                                                        and 	cer.company = cla.company
                                                                                                                                        and 	cer.certype = pol.certype
                                                                                                                                        and 	cer.branch = cla.branch 
                                                                                                                                        and 	cer.policy = cla.policy
                                                                                                                                        and		cer.certif = cla.certif),'')
                                                                                                end), '')*/ '' KACESTAP,
                                                cast(case	coalesce(cla.certif,0)
                                                                        when	0
                                                                        then	case	when	coalesce(pol.payfreq,'0') = '1'
                                                                                                        then	1
                                                                                                        else	0 end
                                                                        else	case	when	coalesce((	select	cer.payfreq
                                                                                                                                                from 	usinsuv01.certificat cer
                                                                                                                                                where	cer.usercomp = cla.usercomp 
                                                                                                                                                and 	cer.company = cla.company
                                                                                                                                                and 	cer.certype = pol.certype
                                                                                                                                                and 	cer.branch = cla.branch 
                                                                                                                                                and 	cer.policy = cla.policy
                                                                                                                                                and		cer.certif = cla.certif),'0') = '1'
                                                                                                        then	1
                                                                                                        else	0 end
                                                                        end as varchar) DTERMO,
                                                '' TULTALT, --excluido
                                                '' DUSRUPD, --excluido
                                                '' KABUNRIS, --SE DESCONOCE ABUNRIS.PK
                                                '' DUSRENT, --excluido
                                                '' DUSRSUP, --excluido
                                                coalesce(cast(cla.staclaim as varchar),'') KSCESTSN,
                                                '' KSCFMPTI, --excluido
                                                '' KSCTPAUT, --excluido
                                                '' KSCLOCSN, --excluido
                                                '' KSCINBMT, --excluido
                                                '' KSCINIDS, --excluido
                                                '' DPROCIDS, --excluido
                                                '' KSCETIDS, --excluido
                                                'LPV' DCOMPA,
                                                '' DMARCA, --excluido
                                                '' KSCNATZA_IN, --excluido
                                                '' KSCNATZA_FI, --excluido
                                                '' KSCCDEST, --excluido
                                                '' KACARGES, --excluido
                                                '' DNUMOBJ, --excluido
                                                '' DNUMOB2, --excluido
                                                coalesce(coalesce(
                                                                        (	select	max(sclient_vig)
                                                                                from	usinsug01.wbstblclidepequi
                                                                                where	sclient_old = 
                                                                                                (	select	evi.scod_vt
                                                                                                        from	usinsug01.equi_vt_inx evi 
                                                                                                        where	evi.scod_inx = cla.client)),
                                                                        (	select	evi.scod_vt
                                                                                from	usinsug01.equi_vt_inx evi 
                                                                                where	evi.scod_inx = cla.client)),
                                                        cla.client) DUNIDRISC,
                                                coalesce((	select	cast(max(operdate) as varchar)
                                                                        from	usinsuv01.claim_his
                                                                        where	claim = cla.claim
                                                                        and		oper_type = '16'),'') TDTREABE,
                                                '' DEQREGUL, --excluido
                                                '' KACMOEST, --excluido
                                                '' KACCONCE, --excluido
                                                '' KSCCONTE, --excluido
                                                '' KSCSTREE, --no disponible
                                                cast(coalesce(	case	when	pol.bussityp = '2'
                                                                                                then	pol.leadshare
                                                                                                else	cl0.share_coa
                                                                                                end,0) as numeric(7,4)) VTXCOSEG,
                                                '' KSCPAIS, --no disponible
                                                '' KSCDEFRP, --excluido
                                                '' KSCORPAR, --excluido
                                                '' DTPRCAS, --excluido
                                                '' TDTESTAD, --no disponible
                                                '' KSBSIN_MP, --excluido
                                                '' TMIGPARA, --excluido
                                                '' KSBSIN_MD, --excluido
                                                '' TMIGDE, --excluido
                                                cla.branch || '-' || cla.policy || '-' || cla.certif KABAPOL,
                                                '' TPRENCER, --excluido
                                                '' TDTREEMB, --no disponible
                                                '' TENTPLAT, --excluido
                                                '' DHENTPLA, --excluido
                                                '' TENTCOMP, --excluido
                                                '' DHENTCOM, --excluido
                                                '' TPEDPART, --excluido
                                                '' TDTRECLA, --excluido
                                                '' TDECFIN, --excluido
                                                '' TASSRESP, --excluido
                                                '' DINDENCU, --excluido
                                                '' KSCMTENC, --excluido
                                                '' DQTDAAA, --excluido
                                                '' DINFACTO, --excluido
                                                '' TINISUSP, --excluido
                                                '' TFIMSUSP, --excluido
                                                '' DINSOPRE, --excluido
                                                '' KSCTPDAN, --excluido
                                                '' KABAPOL_EFT, --excluido
                                                '' DARQUIVO, --excluido
                                                '' TARQUIVO, --excluido
                                                '' DLOCREF, --excluido
                                                '' KACPARES, --no disponible
                                                '' KGCRAMO_SAP, --excluido
                                                '' DNUMPGRE, --no disponible
                                                '' DINDSINTER, --FALTA C�LCULO
                                                '' DQDREABER, --excluido
                                                '' TPLANOCOSEG, --excluido
                                                '' TPLANORESEG, --excluido
                                                case	when pol.bussityp = '1' and cl0.share_coa = 100 then '0' --Paga todo
                                                                when pol.bussityp = '1' and cl0.share_coa <> 100 then '1' --No paga todo
                                                                when pol.bussityp = '2' then '0' --Paga todo
                                                                else '0' end KSCPAGCSG,
                                                '' KSCAPLGES, --excluido
                                                '' DANUAGR, --excluido
                                                '' DENTIDSO, --excluido
                                                '' DNOFSIN, --excluido
                                                '' DIMAGEM, --excluido
                                                '' KEBENTID_GS, --excluido
                                                '' KOCSCOPE, --excluido
                                                '' DCDINTTRA --excluido
                                from	(	select	clh.cla_id,
                                                                        clh.pol_id,
                                                                        clh.share_coa,
                                                                        sum(clh.monto_trans * case clh.tipo when 1 then 1 else 0 end ) reserva,
                                                                        sum(clh.monto_trans * case clh.tipo when 2 then 1 else 0 end ) ajustes,
                                                                        sum(clh.monto_trans * case clh.tipo when 3 then 1 else 0 end ) pagos
                                                        from	(	select	cla.cla_id,
                                                                                                cla.pol_id,
                                                                                                cla.share_coa,
                                                                                                tcl.tipo,
                                                                                                coalesce(	case	when	clh.currency = cla.moneda_cod
                                                                                                                                        then    clh.amount
                                                                                                                                        else	case	when	cla.moneda_cod = 1
                                                                                                                                                                        then	clh.amount *
                                                                                                                                                                                        case	when	clh.currency = 2
                                                                                                                                                                                                        then	(	select	max(clh0.exchange)
                                                                                                                                                                                                                                from	usinsuv01.claim_his clh0
                                                                                                                                                                                                                                where	clh0.claim = clh.claim
                                                                                                                                                                                                                                and		clh0.transac =
                                                                                                                                                                                                                                                (	select	max(clh1.transac)
                                                                                                                                                                                                                                                        from	usinsuv01.claim_his clh1
                                                                                                                                                                                                                                                        where 	clh1.claim = clh.claim
                                                                                                                                                                                                                                                        and		clh1.transac <= clh.transac
                                                                                                                                                                                                                                                        and		clh1.exchange not in (1,0)))
                                                                                                                                                                                                        else    1 end
                                                                                                                                                                        when	cla.moneda_cod = 2
                                                                                                                                                                        then	clh.amount /
                                                                                                                                                                                        case	when	clh.currency = 1
                                                                                                                                                                                                        then	(	select	max(clh0.exchange)
                                                                                                                                                                                                                                from	usinsuv01.claim_his clh0
                                                                                                                                                                                                                                where	clh0.claim = clh.claim
                                                                                                                                                                                                                                and		clh0.transac =
                                                                                                                                                                                                                                                (	select	max(clh1.transac)
                                                                                                                                                                                                                                                        from	usinsuv01.claim_his clh1
                                                                                                                                                                                                                                                        where 	clh1.claim = clh.claim
                                                                                                                                                                                                                                                        and		clh1.transac <= clh.transac
                                                                                                                                                                                                                                                        and		clh1.exchange not in (1,0)))
                                                                                                                                                                                                        else    1 end
                                                                                                                                                                        else	0 end end, 0) monto_trans
                                                                                from 	(	select 	case 	when 	cla.staclaim <> '6' and pol.certype = '2'
                                                                                                                                                        and 2 = (select t10b.company from usinsug01.table10b t10b where t10b.branch = cla.branch)
                                                                                                                                        then	cla.claim
                                                                                                                                        else 	null end claim,
                                                                                                                        case 	when 	cla.staclaim <> '6' and pol.certype = '2'
                                                                                                                                        then	cla.ctid 
                                                                                                                                        else	null end cla_id,
                                                                                                                        case	when	pol.certype = '2'
                                                                                                                                        then	pol.ctid
                                                                                                                                        else	null end pol_id,
                                                                                                                        case 	when 	cla.staclaim <> '6' and pol.certype = '2'
                                                                                                                                                        and 2 = (select t10b.company from usinsug01.table10b t10b where t10b.branch = cla.branch)
                                                                                                                                        then	case	pol.bussityp
                                                                                                                                                                        when	'2'
                                                                                                                                                                        then 	100 - coalesce(pol.leadshare,0)
                                                                                                                                                                        when	'3'
                                                                                                                                                                        then 	null
                                                                                                                                                                        else	coalesce((	select	coi.share
                                                                                                                                                                                                                from	usinsuv01.coinsuran coi
                                                                                                                                                                                                                where   coi.usercomp = cla.usercomp
                                                                                                                                                                                                                and     coi.company = cla.company
                                                                                                                                                                                                                and     coi.certype = pol.certype
                                                                                                                                                                                                                and     coi.branch = cla.branch
                                                                                                                                                                                                                and     coi.policy = cla.policy
                                                                                                                                                                                                                and     coi.effecdate <= cla.occurdat
                                                                                                                                                                                                                and     (coi.nulldate is null or coi.nulldate > cla.occurdat)
                                                                                                                                                                                                                and 	coalesce(coi.companyc,0) in (1,12)),100) end 
                                                                                                                                        else	0 end share_coa,
                                                                                                                        case 	when 	cla.staclaim <> '6' and pol.certype = '2'
                                                                                                                                                        and 2 = (select t10b.company from usinsug01.table10b t10b where t10b.branch = cla.branch)
                                                                                                                                        then	coalesce(	coalesce((	select	max(cpl.currency)
                                                                                                                                                                                                        from	usinsuv01.curren_pol cpl
                                                                                                                                                                                                        where 	cpl.usercomp = cla.usercomp
                                                                                                                                                                                                        and 	cpl.company = cla.company
                                                                                                                                                                                                        and		cpl.certype = pol.certype
                                                                                                                                                                                                        and		cpl.branch = cla.branch
                                                                                                                                                                                                        and 	cpl.policy = cla.policy
                                                                                                                                                                                                        and		cpl.certif = cla.certif),
                                                                                                                                                                                                (	select	max(cpl.currency)
                                                                                                                                                                                                        from	usinsuv01.curren_pol cpl
                                                                                                                                                                                                        where 	cpl.usercomp = cla.usercomp
                                                                                                                                                                                                        and 	cpl.company = cla.company
                                                                                                                                                                                                        and		cpl.certype = pol.certype
                                                                                                                                                                                                        and		cpl.branch = cla.branch
                                                                                                                                                                                                        and 	cpl.policy = cla.policy)),0) 
                                                                                                                                        else	0 end moneda_cod
                                                                                                        from    usinsuv01.policy pol
                                                                                                        join	usinsuv01.claim cla
                                                                                                                on		cla.usercomp = pol.usercomp
                                                                                                                and     cla.company = pol.company
                                                                                                                and     cla.branch = pol.branch
                                                                                                                and     cla.policy = pol.policy
                                                                                                                and     exists
                                                                                                                                        (   select  1
                                                                                                                        from    (	select	cast(codigint as varchar(2)) operation
                                                                                                                                                                        from	usinsug01.table140
                                                                                                                                                                        where	codigint in 
                                                                                                                                                                                        (	select 	operation
                                                                                                                                                                                                from	usinsug01.tab_cl_ope
                                                                                                                                                                                                where	reserve = 1 or ajustes = 1 or pay_amount = 1)) tcl --solo pagos
                                                                                                                                                join	usinsuv01.claim_his clh on trim(clh.oper_type) = tcl.operation
                                                                                                                        where   coalesce (clh.claim,0) = cla.claim
                                                                                                                        and     clh.operdate >= cast('12/31/2015' as date))) cla --ejecutar al a�o 2021 (2015 es para pruebas)
                                                                                join	usinsuv01.claim_his clh
                                                                                                on		clh.claim = cla.claim
                                                                                                and		clh.operdate <= '12/31/2023' --en caso de ejecutar a una fecha posterior, alterar esta fecha a la de la ejecuci�n
                                                                                join	(	select	case	when tcl.reserve = 1 then 1
                                                                                                                                        when tcl.ajustes = 1 then 2
                                                                                                                                        when tcl.pay_amount = 1 then 3
                                                                                                                                        else 0 end tipo,
                                                                                                                        cast(tcl.operation as varchar(2)) operation
                                                                                                        from	usinsug01.tab_cl_ope tcl
                                                                                                        where	(tcl.reserve = 1 or tcl.ajustes = 1 or tcl.pay_amount = 1)) tcl
                                                                                                on		trim(clh.oper_type) = tcl.operation) clh
                                                        group	by 1,2,3) cl0
                                join	usinsuv01.claim cla on cla.ctid = cl0.cla_id
                                join	usinsuv01.policy pol on pol.ctid = cl0.pol_id
                            ) AS TMP      
                            '''
    L_DF_SBSIN_INSUNIX_LPV = glue_context.read.format('jdbc').options(**connection).option("fetchsize",10000).option("dbtable",L_SBSIN_INSUNIX_LPV).load()
    
    L_DF_SBSIN_INSUNIX =  L_DF_SBSIN_INSUNIX_LPG.union(L_DF_SBSIN_INSUNIX_LPV)


    L_SBSIN_VTIME_LPG = '''
                            (
                                select	        'D' INDDETREC,
                                                'SBSIN' TABLAIFRS17,
                                                '' PK,
                                                '' DTPREG, --excluido
                                                '' TIOCPROC, --no disponible
                                                coalesce(cast(cast(cla."DCOMPDATE" as date) as varchar),'') TIOCFRM,
                                                '' TIOCTO, --excluido
                                                'PVG' KGIORIGM,
                                                'LPG' KSCCOMPA,
                                                coalesce(cast(cla."NBRANCH" as varchar),'') KGCRAMO,
                                                cla."NBRANCH" || '-' || pol."NPRODUCT" KABPRODT,
                                                coalesce(cast(cla."NPOLICY" as varchar),'') DNUMAPO,
                                                coalesce(cast(cla."NCERTIF" as varchar),'') DNMCERT,
                                                coalesce(cast(cla."NCLAIM" as varchar),'') DNUMSIN,
                                                coalesce(cast(cla."DOCCURDAT" as varchar),'') TOCURSIN,
                                                '' DHSINIST, --excluido
                                                coalesce(cast(cla."DOCCURDAT" as varchar),'') TABERSIN,
                                                '' TPARTSIN, --excluido
                                                coalesce(
                                                        case	when	trim(cla."SSTACLAIM") in ('1','5','7')
                                                                        then	(	select	cast(cast(max("DOPERDATE") as date) as varchar)
                                                                                                from	usvtimg01."CLAIM_HIS"
                                                                                                where	"NCLAIM" = cla."NCLAIM"
                                                                                                and		case	when	trim(cla."SSTACLAIM") in ('1') then "NOPER_TYPE" in (3,17)
                                                                                                                                when	trim(cla."SSTACLAIM") in ('5') then "NOPER_TYPE" in (11)
                                                                                                                                when	trim(cla."SSTACLAIM") in ('7') then "NOPER_TYPE" in (21)
                                                                                                                                else	"NOPER_TYPE" is null end)
                                                                        else '' end, '')  TFECHTEC,
                                                coalesce(
                                                        case	when	trim(cla."SSTACLAIM") in ('1','5','7')
                                                                        then	(	select	cast(cast(max("DOPERDATE") as date) as varchar)
                                                                                                from	usvtimg01."CLAIM_HIS"
                                                                                                where	"NCLAIM" = cla."NCLAIM"
                                                                                                and		case	when	trim(cla."SSTACLAIM") in ('1') then "NOPER_TYPE" in (3,17)
                                                                                                                                when	trim(cla."SSTACLAIM") in ('5') then "NOPER_TYPE" in (77)
                                                                                                                                when	trim(cla."SSTACLAIM") in ('7') then "NOPER_TYPE" in (21)
                                                                                                                                else	"NOPER_TYPE" is null end)
                                                                        else '' end, '')  TFECHADM,
                                                coalesce(
                                                        (	select	cast(cast(max("DOPERDATE") as date) as varchar)
                                                                from	usvtimg01."CLAIM_HIS"
                                                                where	"NCLAIM" = cla."NCLAIM"
                                                                and		"NOPER_TYPE" in
                                                                                (	select	"NOPER_TYPE"
                                                                                        from	usvtimg01."TABLE140"
                                                                                        where	"NOPER_TYPE" in
                                                                                                        (	select	cast("SVALUE" as INT4)
                                                                                                                from	usvtimg01."CONDITION_SERV" cs 
                                                                                                                where	"NCONDITION" in (73))
                                                                                                        or "NOPER_TYPE" in (75,76,77,78,79,80,83))),'') TESTADO,
                                                coalesce(cast(cla."SSTACLAIM" as varchar),'') KSCSITUA,
                                                '' KSCMOTSI, --excluido
                                                '' KSCTPSIN, --no disponible
                                                coalesce(cast(cla."NCAUSECOD" as varchar),'') KSCCAUSA,
                                                '' KSCARGES, --excluido
                                                '' KSCFMPGS, --no disponible
                                                '' KCBMED_DRA, --excluido
                                                '' KCBMED_PG, --excluido
                                                '' KCBMED_PD, --excluido
                                                '' KCBMED_P2, --excluido
                                                case	when pol."SBUSSITYP" = '1' and cl0.nshare_coa = 100 then 1 --Sin coaseguro
                                                                when pol."SBUSSITYP" = '1' and cl0.nshare_coa <> 100 then 2 --Con coaseguro, compa��a l�der
                                                                when pol."SBUSSITYP" = '2' then 3 --Con coaseguro, compa��a no l�der
                                                                else 0 end KSCTPCSG,
                                                cast(coalesce(
                                                                coalesce((  select  max(cpl."NCURRENCY")
                                                        from    usvtimg01."CURREN_POL" cpl
                                                        where 	cpl."SCERTYPE" = cla."SCERTYPE"
                                                        and		cpl."NBRANCH" = cla."NBRANCH"
                                                        and		cpl."NPRODUCT" = pol."NPRODUCT"
                                                        and     cpl."NPOLICY" = cla."NPOLICY"
                                                        and     cpl."NCERTIF" = cla."NCERTIF"),
                                                        (   select  max(cpl."NCURRENCY")
                                                        from    usvtimg01."CURREN_POL" cpl
                                                        where 	cpl."SCERTYPE" = cla."SCERTYPE"
                                                        and		cpl."NBRANCH" = cla."NBRANCH"
                                                        and		cpl."NPRODUCT" = pol."NPRODUCT"
                                                        and     cpl."NPOLICY" = cla."NPOLICY")),0) as varchar) KSCMOEDA,
                                                '' VCAMBIO, --no disponible
                                                '' VTXRESPN, --no disponible
                                                cast(cl0.reserva + cl0.ajustes as numeric(14,2)) VMTPROVI,
                                                '' VMTPRVINI, --excluido
                                                cast((cl0.reserva + cl0.ajustes)
                                                        * (case when pol."SBUSSITYP" = '2' then 100 else nshare_coa end/100)
                                        * (nshare_rea/100) as numeric(14,2)) VMTPRVRS,
                                                cast((cl0.reserva + cl0.ajustes)
                                                                * (case when pol."SBUSSITYP" = '2' then 100 else nshare_coa end/100) as numeric(14,2)) VMTCOSEG,
                                                '' KSCNATUR, --excluido
                                                '' TALTENAT, --excluido
                                                '' DUSRREG, --excluido
                                                coalesce((	select	max("SCLIENT_VGT")
                                                                        from	usvtimg01."WBSTBLCLIDEPEQUI" 
                                                                        where	"SCLIENT_OLD" = cla."SCLIENT"),
                                                                cla."SCLIENT") KEBENTID_TO,
                                                cast(	coalesce(	case	when	cla."NBRANCH" = 21 
                                                                                                        then	(	select	case	when	max(gen."SADDSUINI") = '3'
                                                                                                                                                                then	max(case when gen."SADDSUINI" = '3' then coalesce(cov."NCAPITAL",0) else 0 end)
                                                                                                                                                                else	sum(case when gen."SADDSUINI" = '1' then coalesce(cov."NCAPITAL",0) else 0 end) end
                                                                                                                                from    usvtimg01."COVER" cov
                                                                                                                                join	usvtimg01."LIFE_COVER" gen
                                                                                                                                                on		gen."NCOVER" = cov."NCOVER"
                                                                                                                                                and     gen."NPRODUCT" = cov."NPRODUCT"
                                                                                                                                                and     gen."NMODULEC" = cov."NMODULEC"
                                                                                                                                                and     gen."NBRANCH" = cov."NBRANCH"
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
                                                                                                                                                on		gen."NCOVER" = cov."NCOVER"
                                                                                                                                                and     gen."NPRODUCT" = cov."NPRODUCT"
                                                                                                                                                and     gen."NMODULEC" = cov."NMODULEC"
                                                                                                                                                and     gen."NBRANCH" = cov."NBRANCH"
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
                                                                                                        end, 0) as numeric(14,2)) VCAPITAL,
                                                '' VTXINDEM, --no disponible
                                                '' VMTINDEM, --no disponible
                                                '' TINICIND, --no disponible
                                                '' DULTACTA, --excluido
                                                coalesce(	case	coalesce(cla."NCERTIF",0)
                                                                                        when	0
                                                                                        then	pol."SSTATUS_POL"
                                                                                        else	(	select	cer."SSTATUSVA"
                                                                                                                from 	usvtimg01."CERTIFICAT" cer
                                                                                                                where	cer."SCERTYPE" = pol."SCERTYPE"
                                                                                                                and 	cer."NBRANCH" = cla."NBRANCH"
                                                                                                                and 	cer."NPOLICY" = cla."NPOLICY"
                                                                                                                and		cer."NCERTIF" = cla."NCERTIF"
                                                                                                                and		CER."NDIGIT" = 0)
                                                                                        end, '') KACESTAP,
                                                cast(	case	coalesce(cla."NCERTIF",0)
                                                                                when	0
                                                                                then	case	when	coalesce(pol."NPAYFREQ",'0') = '1'
                                                                                                                then	1
                                                                                                                else	0 end
                                                                                else	case	when	coalesce((	select	cer."NPAYFREQ"
                                                                                                                                                        from 	usvtimg01."CERTIFICAT" cer
                                                                                                                                                        where	cer."SCERTYPE" = pol."SCERTYPE"
                                                                                                                                                        and 	cer."NBRANCH" = cla."NBRANCH"
                                                                                                                                                        and 	cer."NPOLICY" = cla."NPOLICY"
                                                                                                                                                        and		cer."NCERTIF" = cla."NCERTIF"
                                                                                                                                                        and		CER."NDIGIT" = 0),'0') = '1'
                                                                                                                then	1
                                                                                                                else	0 end
                                                                                end as varchar) DTERMO,
                                                '' TULTALT, --excluido
                                                '' DUSRUPD, --excluido
                                                '' KABUNRIS, --SE DESCONOCE ABUNRIS.PK
                                                '' DUSRENT, --excluido
                                                '' DUSRSUP, --excluido
                                                coalesce(cla."SSTACLAIM",'') KSCESTSN,
                                                '' KSCFMPTI, --excluido
                                                '' KSCTPAUT, --excluido
                                                '' KSCLOCSN, --excluido
                                                '' KSCINBMT, --excluido
                                                '' KSCINIDS, --excluido
                                                '' DPROCIDS, --excluido
                                                '' KSCETIDS, --excluido
                                                'LPG' DCOMPA,
                                                '' DMARCA, --excluido
                                                '' KSCNATZA_IN, --excluido
                                                '' KSCNATZA_FI, --excluido
                                                '' KSCCDEST, --excluido
                                                '' KACARGES, --excluido
                                                '' DNUMOBJ, --excluido
                                                '' DNUMOB2, --excluido
                                                coalesce(	case	/*(	select	"RISKTYPEN"
                                                                                                from	usbi01."IFRS170_T_RAMOS_POR_TIPO_RIESGO"
                                                                                                where	"SOURCESCHEMA" = 'usvtimg01'
                                                                                                and		"BRANCHCOM" = cla."NBRANCH")*/
                                                                                        --when	3
                                                                                        when	cla."NBRANCH" in (6,62,66,67,69)
                                                                                        then	coalesce((	select	case	when coalesce(trim(tnb."SREGIST"),'') <> '' and coalesce(trim(tnb."SCHASSIS"),'') <> ''
                                                                                                                                                                then coalesce(trim(tnb."SREGIST"),'') || '-' || coalesce(trim(tnb."SCHASSIS"),'')
                                                                                                                                                                when coalesce(trim(tnb."SREGIST"),'') <> '' and coalesce(trim(tnb."SCHASSIS"),'') = ''
                                                                                                                                                                then coalesce(trim(tnb."SREGIST"),'')
                                                                                                                                                                when coalesce(trim(tnb."SREGIST"),'') = '' and coalesce(trim(tnb."SCHASSIS"),'') <> ''
                                                                                                                                                                then coalesce(trim(tnb."SCHASSIS"),'')
                                                                                                                                                                else '' end
                                                                                                                                from	usvtimg01."AUTO" tnb
                                                                                                                                where	ctid = 
                                                                                                                                                (	coalesce(coalesce(coalesce(
                                                                                                                                (   select max(ctid) from usvtimg01."AUTO" tn0
                                                                                                                                        where tn0."SCERTYPE" = pol."SCERTYPE" and tn0."NBRANCH" = cla."NBRANCH"
                                                                                                                                        and tn0."NPRODUCT" = cla."NPRODUCT" and tn0."NPOLICY" = cla."NPOLICY" and tn0."NCERTIF" = cla."NCERTIF"
                                                                                                                                        and cast(tn0."DEFFECDATE" as date) <= cast(cla."DOCCURDAT" as date)
                                                                                                                                        and (tn0."DNULLDATE" is null or cast(tn0."DNULLDATE" as date) > cast(cla."DOCCURDAT" as date))),
                                                                                                                                (   select max(ctid) from usvtimg01."AUTO" tn0
                                                                                                                                        where tn0."SCERTYPE" = pol."SCERTYPE" and tn0."NBRANCH" = cla."NBRANCH"
                                                                                                                                        and tn0."NPRODUCT" = cla."NPRODUCT" and tn0."NPOLICY" = cla."NPOLICY" and tn0."NCERTIF" = cla."NCERTIF"
                                                                                                                                        and cast(tn0."DEFFECDATE" as date) <= cast(cla."DOCCURDAT" as date)
                                                                                                                                        and (tn0."DNULLDATE" is null or cast(tn0."DNULLDATE" as date) >= cast(cla."DOCCURDAT" as date))),
                                                                                                                                (   select max(ctid) from usvtimg01."AUTO" tn0
                                                                                                                                        where tn0."SCERTYPE" = pol."SCERTYPE" and tn0."NBRANCH" = cla."NBRANCH"
                                                                                                                                        and tn0."NPRODUCT" = cla."NPRODUCT" and tn0."NPOLICY" = cla."NPOLICY" and tn0."NCERTIF" = cla."NCERTIF"
                                                                                                                                        and cast(tn0."DEFFECDATE" as date) <= cast(cla."DOCCURDAT" as date)
                                                                                                                                        and (tn0."DNULLDATE" is null or cast(tn0."DNULLDATE" as date) < cast(cla."DOCCURDAT" as date)))))))),'')
                                                                                        --when	2
                                                                                        when	cla."NBRANCH" in (1,2,3,4,5,7,8,9,10,11,12,13,14,17,18,19,28,29,30,38,39,45,55,57,58,59,60,61,63,921)
                                                                                        then	cla."NBRANCH" || '-' || cla."NPOLICY" || '-' || cla."NCERTIF"
                                                                                        --when	1
                                                                                        when	cla."NBRANCH" in (21,23,24,27,31,32,33,34,35,36,37,40,42,64,71,75,91)
                                                                                        then	coalesce((	select	max("SCLIENT_VGT")
                                                                                                                                from	usvtimg01."WBSTBLCLIDEPEQUI" 
                                                                                                                                where	"SCLIENT_OLD" = cla."SCLIENT"),
                                                                                                                        cla."SCLIENT")
                                                                                        else 	'' end, '') DUNIDRISC,
                                                coalesce((	select	cast(cast(max("DOPERDATE") as date) as varchar)
                                                                        from	usvtimg01."CLAIM_HIS"
                                                                        where	"NCLAIM" = cla."NCLAIM"
                                                                        and		"NOPER_TYPE" = 16),'') TDTREABE,
                                                '' DEQREGUL, --excluido
                                                '' KACMOEST, --excluido
                                                '' KACCONCE, --excluido
                                                '' KSCCONTE, --excluido
                                                '' KSCSTREE, --no disponible
                                                cast(coalesce(	case	when	pol."SBUSSITYP" = '2'
                                                                                                then	pol."NLEADSHARE"
                                                                                                else	cl0.nshare_coa
                                                                                                end,0) as numeric(7,4)) VTXCOSEG,
                                                '' KSCPAIS, --no disponible
                                                '' KSCDEFRP, --excluido
                                                '' KSCORPAR, --excluido
                                                '' DTPRCAS, --excluido
                                                '' TDTESTAD, --no disponible
                                                '' KSBSIN_MP, --excluido
                                                '' TMIGPARA, --excluido
                                                '' KSBSIN_MD, --excluido
                                                '' TMIGDE, --excluido
                                                cla."NBRANCH" || '-' || pol."NPRODUCT" || '-' || cla."NPOLICY" || '-' || cla."NCERTIF" KABAPOL,
                                                '' TPRENCER, --excluido
                                                '' TDTREEMB, --no disponible
                                                '' TENTPLAT, --excluido
                                                '' DHENTPLA, --excluido
                                                '' TENTCOMP, --excluido
                                                '' DHENTCOM, --excluido
                                                '' TPEDPART, --excluido
                                                '' TDTRECLA, --excluido
                                                '' TDECFIN, --excluido
                                                '' TASSRESP, --excluido
                                                '' DINDENCU, --excluido
                                                '' KSCMTENC, --excluido
                                                '' DQTDAAA, --excluido
                                                '' DINFACTO, --excluido
                                                '' TINISUSP, --excluido
                                                '' TFIMSUSP, --excluido
                                                '' DINSOPRE, --excluido
                                                '' KSCTPDAN, --excluido
                                                '' KABAPOL_EFT, --excluido
                                                '' DARQUIVO, --excluido
                                                '' TARQUIVO, --excluido
                                                '' DLOCREF, --excluido
                                                '' KACPARES, --no disponible
                                                '' KGCRAMO_SAP, --excluido
                                                '' DNUMPGRE, --no disponible
                                                '' DINDSINTER, --FALTA C�LCULO
                                                '' DQDREABER, --excluido
                                                '' TPLANOCOSEG, --excluido
                                                '' TPLANORESEG, --excluido
                                                case	when pol."SBUSSITYP" = '1' and cl0.nshare_coa = 100 then '0' --Paga todo
                                                                when pol."SBUSSITYP" = '1' and cl0.nshare_coa <> 100 then '1' --No paga todo
                                                                when pol."SBUSSITYP" = '2' then '0' --Paga todo
                                                                else '0' end KSCPAGCSG,
                                                '' KSCAPLGES, --excluido
                                                '' DANUAGR, --excluido
                                                '' DENTIDSO, --excluido
                                                '' DNOFSIN, --excluido
                                                '' DIMAGEM, --excluido
                                                '' KEBENTID_GS, --excluido
                                                '' KOCSCOPE, --excluido
                                                '' DCDINTTRA --excluido
                                from	(	select	clh.cla_id,
                                                                        clh.pol_id,
                                                                        clh.nshare_coa,
                                                                        clh.nshare_rea, --CALCULO PENDIENTE (PODR�A IMPLICAR MODIFICAR CUERPO DE QUERY A NIVEL DE CLM_M_COVER)
                                                                        sum(clh.monto_trans * case clh.tipo when 1 then 1 else 0 end ) reserva,
                                                                        sum(clh.monto_trans * case clh.tipo when 2 then 1 else 0 end ) ajustes,
                                                                        sum(clh.monto_trans * case clh.tipo when 3 then 1 else 0 end ) pagos
                                                        from 	(	select  cla.*,
                                                                                        csv.tipo,
                                                                                                case	when	cla.moneda_cod = 1
                                                                                                                then	case	when	clh."NCURRENCY" = 1
                                                                                                                                                then	clh."NAMOUNT"
                                                                                                                                                when	clh."NCURRENCY" = 2
                                                                                                                                                then	clh."NAMOUNT" * clh."NEXCHANGE"
                                                                                                                                                else	0 end
                                                                                                                when	cla.moneda_cod = 2
                                                                                                                then	case	when	clh."NCURRENCY" = 2
                                                                                                                                                then	clh."NAMOUNT"
                                                                                                                                                when	clh."NCURRENCY" = 1
                                                                                                                                                then	clh."NLOC_AMOUNT"
                                                                                                                                                else	0 end
                                                                                                                                else	0 end monto_trans
                                                                                from    (   select  case    when    cla."SSTACLAIM" <> '6' and pol."SCERTYPE" = '2'
                                                                                                        then    cla."NCLAIM"
                                                                                                        else    null end "NCLAIM",
                                                                                                                        case    when    cla."SSTACLAIM" <> '6' and pol."SCERTYPE" = '2'
                                                                                                        then    cla.ctid
                                                                                                        else    null end cla_id,
                                                                                                                        case    when    pol."SCERTYPE" = '2'
                                                                                                        then    pol.ctid
                                                                                                        else    null end pol_id,
                                                                                                case    when    cla."SSTACLAIM" <> '6' and pol."SCERTYPE"  = '2'
                                                                                                                                        then	case	pol."SBUSSITYP"
                                                                                                                                                                        when	'2'
                                                                                                                                                                        then 	100 - coalesce(pol."NLEADSHARE",0)
                                                                                                                                                                        when	'3'
                                                                                                                                                                        then 	null
                                                                                                                                                                        else	coalesce((  select 	"NSHARE"
                                                                                                                                                                                                                from	usvtimg01."COINSURAN" coi
                                                                                                                                                                                                                where	coi."SCERTYPE" = cla."SCERTYPE"
                                                                                                                                                                                                                and     coi."NBRANCH" = cla."NBRANCH"
                                                                                                                                                                                                                and     coi."NPRODUCT" = pol."NPRODUCT"
                                                                                                                                                                                                                and     coi."NPOLICY" = cla."NPOLICY"
                                                                                                                                                                                                                and 	coi."NCOMPANY" is not null
                                                                                                                                                                                                                and     cast(coi."DEFFECDATE" as date) <= cast(cla."DOCCURDAT" as date)
                                                                                                                                                                                                                and     (coi."DNULLDATE" is null or cast(coi."DNULLDATE" as date) > cast(cla."DOCCURDAT" as date))
                                                                                                                                                                                                                and		coi."NCOMPANY" in (1)),100) end 
                                                                                                                                        else	0 end nshare_coa,
                                                                                                                        coalesce(	case	when	cla."NBRANCH" = 21 
                                                                                                                                                                then	(	select 	coalesce(
                                                                                                                                                                                                                sum(case when flag_add = '3' then npremium_3 else npremium_1 end *
                                                                                                                                                                                                                ((	select	min(coalesce("NSHARE",0))
                                                                                                                                                                                                                        from	usvtimg01."REINSURAN" REI --* nivel revisado para el esquema de reaseguro asociado al registro
                                                                                                                                                                                                                        where	REI."SCERTYPE" = cla."SCERTYPE" AND REI."NBRANCH" = cla."NBRANCH"
                                                                                                                                                                                                                        and		REI."NPRODUCT" = pol."NPRODUCT" AND REI."NPOLICY" = cla."NPOLICY"
                                                                                                                                                                                                                        and		REI."NCERTIF" = case cov.flag_rea when 1 then coalesce(cla."NCERTIF",0) when 2 then 0 end
                                                                                                                                                                                                                        and		REI."NBRANCH_REI" = cov.nbranch_rei
                                                                                                                                                                                                                        and		REI."NTYPE_REIN" = 1
                                                                                                                                                                                                                        and		cast(REI."DEFFECDATE" as date) <= cast(cla."DOCCURDAT" as date)
                                                                                                                                                                                                                        and		(REI."DNULLDATE" IS NULL OR cast(REI."DNULLDATE" as date) > cast(cla."DOCCURDAT" as date))) / 100)) /
                                                                                                                                                                                                        nullif(sum(case when flag_add = '3' then npremium_3 else npremium_1 end),0),0)
                                                                                                                                                                                        from 	(	select	gen."NBRANCH_REI" nbranch_rei,
                                                                                                                                                                                                                                max(case when gen."SADDREINI" = '3' then coalesce(cov."NPREMIUM",0) else 0 end) npremium_3,
                                                                                                                                                                                                                                sum(case when gen."SADDREINI" = '1' then coalesce(cov."NPREMIUM",0) else 0 end) npremium_1,
                                                                                                                                                                                                                                max(gen."SADDREINI") flag_add,
                                                                                                                                                                                                                                max((	select	case	
                                                                                                                                                                                                                                                                when	NOT (pol."SPOLITYPE" = '2' AND cla."NBRANCH" = 57 AND cla."NPRODUCT" = 1)
                                                                                                                                                                                                                                                                then	case	when	EXISTS
                                                                                                                                                                                                                                                                                                                (	SELECT  1
                                                                                                                                                                                                                                                                                                                        from	usvtimg01."REINSURAN" REI
                                                                                                                                                                                                                                                                                                                        where	REI."SCERTYPE" = pol."SCERTYPE" AND REI."NBRANCH" = cla."NBRANCH"
                                                                                                                                                                                                                                                                                                                        and		REI."NPRODUCT" = pol."NPRODUCT" AND REI."NPOLICY" = cla."NPOLICY"
                                                                                                                                                                                                                                                                                                                        and		REI."NCERTIF" = case when "SPOLITYPE" = '3' then 0 else cla."NCERTIF" end
                                                                                                                                                                                                                                                                                                                        and		REI."NBRANCH_REI" = gen."NBRANCH_REI"
                                                                                                                                                                                                                                                                                                                        and		CAST(REI."DEFFECDATE" as DATE) <= cast(cla."DOCCURDAT" as date)
                                                                                                                                                                                                                                                                                                                        and		(REI."DNULLDATE" IS NULL OR CAST(REI."DNULLDATE" as DATE) > cast(cla."DOCCURDAT" as date)))
                                                                                                                                                                                                                                                                                                then	case	when	"SPOLITYPE" = '3'
                                                                                                                                                                                                                                                                                                        then	2
                                                                                                                                                                                                                                                                                                        else	1 end
                                                                                                                                                                                                                                                                                                else 	0 end 
                                                                                                                                                                                                                                                                when	(pol."SPOLITYPE" = '2' AND cla."NBRANCH" = 57 AND cla."NPRODUCT" = 1)
                                                                                                                                                                                                                                                                then 	case	when	EXISTS
                                                                                                                                                                                                                                                                                                                (	select	1
                                                                                                                                                                                                                                                                                                                        from	usvtimg01."REINSURAN" REI
                                                                                                                                                                                                                                                                                                                        where	REI."SCERTYPE" = pol."SCERTYPE" AND REI."NBRANCH" = cla."NBRANCH"
                                                                                                                                                                                                                                                                                                                        and		REI."NPRODUCT" = pol."NPRODUCT" AND REI."NPOLICY" = cla."NPOLICY"
                                                                                                                                                                                                                                                                                                                        and		REI."NCERTIF" = cla."NCERTIF" AND REI."NBRANCH_REI" = gen."NBRANCH_REI"
                                                                                                                                                                                                                                                                                                                        and		cast(REI."DEFFECDATE" as DATE) <= cast(cla."DOCCURDAT" as date)
                                                                                                                                                                                                                                                                                                                        and		(REI."DNULLDATE" IS NULL OR CAST(REI."DNULLDATE" as DATE) > cast(cla."DOCCURDAT" as date)))
                                                                                                                                                                                                                                                                                                then    1
                                                                                                                                                                                                                                                                                                else 	case 	when	EXISTS
                                                                                                                                                                                                                                                                                                                                                (   SELECT  1
                                                                                                                                                                                                                                                                                                                                                        from	usvtimg01."REINSURAN" REI
                                                                                                                                                                                                                                                                                                                                                        where	REI."SCERTYPE" = pol."SCERTYPE" AND REI."NBRANCH" = cla."NBRANCH"
                                                                                                                                                                                                                                                                                                                                                        and		REI."NPRODUCT" = pol."NPRODUCT" AND REI."NPOLICY" = cla."NPOLICY"
                                                                                                                                                                                                                                                                                                                                                        and		REI."NCERTIF" = 0 and REI."NBRANCH_REI" = gen."NBRANCH_REI"
                                                                                                                                                                                                                                                                                                                                                        and		cast(REI."DEFFECDATE" as DATE) <= cast(cla."DOCCURDAT" as date)
                                                                                                                                                                                                                                                                                                                                                        and		(REI."DNULLDATE" IS NULL OR CAST(REI."DNULLDATE" as DATE) > cast(cla."DOCCURDAT" as date)))
                                                                                                                                                                                                                                                                                                THEN    2
                                                                                                                                                                                                                                                                                                ELSE    0 END END
                                                                                                                                                                                                                                                                ELSE    0 END)) flag_rea
                                                                                                                                                                                                                from    usvtimg01."COVER" cov
                                                                                                                                                                                                                join	usvtimg01."LIFE_COVER" gen
                                                                                                                                                                                                                                on		gen."NCOVER" = cov."NCOVER"
                                                                                                                                                                                                                                and     gen."NPRODUCT" = cov."NPRODUCT"
                                                                                                                                                                                                                                and     gen."NMODULEC" = cov."NMODULEC"
                                                                                                                                                                                                                                and     gen."NBRANCH" = cov."NBRANCH"
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
                                                                                                                                                                                                                and     (cov."DNULLDATE" is null or cast(cov."DNULLDATE" as date) > cast(cla."DOCCURDAT" as date))
                                                                                                                                                                                                                group	by 1) cov)
                                                                                                                                                                else 	(	select 	coalesce(
                                                                                                                                                                                                                sum(case when flag_add = '3' then npremium_3 else npremium_1 end *
                                                                                                                                                                                                                ((	select	min(coalesce("NSHARE",0))
                                                                                                                                                                                                                        from	usvtimg01."REINSURAN" REI --* nivel revisado para el esquema de reaseguro asociado al registro
                                                                                                                                                                                                                        where	REI."SCERTYPE" = cla."SCERTYPE" AND REI."NBRANCH" = cla."NBRANCH"
                                                                                                                                                                                                                        and		REI."NPRODUCT" = pol."NPRODUCT" AND REI."NPOLICY" = cla."NPOLICY"
                                                                                                                                                                                                                        and		REI."NCERTIF" = case cov.flag_rea when 1 then coalesce(cla."NCERTIF",0) when 2 then 0 end
                                                                                                                                                                                                                        and		REI."NBRANCH_REI" = cov.nbranch_rei
                                                                                                                                                                                                                        and		REI."NTYPE_REIN" = 1
                                                                                                                                                                                                                        and		cast(REI."DEFFECDATE" as date) <= cast(cla."DOCCURDAT" as date)
                                                                                                                                                                                                                        and		(REI."DNULLDATE" IS NULL OR cast(REI."DNULLDATE" as date) > cast(cla."DOCCURDAT" as date))) / 100)) /
                                                                                                                                                                                                        nullif(sum(case when flag_add = '3' then npremium_3 else npremium_1 end),0),0)
                                                                                                                                                                                        from 	(	select	gen."NBRANCH_REI" nbranch_rei,
                                                                                                                                                                                                                                max(case when gen."SADDREINI" = '3' then coalesce(cov."NPREMIUM",0) else 0 end) npremium_3,
                                                                                                                                                                                                                                sum(case when gen."SADDREINI" = '1' then coalesce(cov."NPREMIUM",0) else 0 end) npremium_1,
                                                                                                                                                                                                                                max(gen."SADDREINI") flag_add,
                                                                                                                                                                                                                                max((	select	case	
                                                                                                                                                                                                                                                                when	NOT (pol."SPOLITYPE" = '2' AND cla."NBRANCH" = 57 AND cla."NPRODUCT" = 1)
                                                                                                                                                                                                                                                                then	case	when	EXISTS
                                                                                                                                                                                                                                                                                                                (	SELECT  1
                                                                                                                                                                                                                                                                                                                        from	usvtimg01."REINSURAN" REI
                                                                                                                                                                                                                                                                                                                        where	REI."SCERTYPE" = pol."SCERTYPE" AND REI."NBRANCH" = cla."NBRANCH"
                                                                                                                                                                                                                                                                                                                        and		REI."NPRODUCT" = pol."NPRODUCT" AND REI."NPOLICY" = cla."NPOLICY"
                                                                                                                                                                                                                                                                                                                        and		REI."NCERTIF" = case when "SPOLITYPE" = '3' then 0 else cla."NCERTIF" end
                                                                                                                                                                                                                                                                                                                        and		REI."NBRANCH_REI" = gen."NBRANCH_REI"
                                                                                                                                                                                                                                                                                                                        and		CAST(REI."DEFFECDATE" as DATE) <= cast(cla."DOCCURDAT" as date)
                                                                                                                                                                                                                                                                                                                        and		(REI."DNULLDATE" IS NULL OR CAST(REI."DNULLDATE" as DATE) > cast(cla."DOCCURDAT" as date)))
                                                                                                                                                                                                                                                                                                then	case	when	"SPOLITYPE" = '3'
                                                                                                                                                                                                                                                                                                        then	2
                                                                                                                                                                                                                                                                                                        else	1 end
                                                                                                                                                                                                                                                                                                else 	0 end 
                                                                                                                                                                                                                                                                when	(pol."SPOLITYPE" = '2' AND cla."NBRANCH" = 57 AND cla."NPRODUCT" = 1)
                                                                                                                                                                                                                                                                then 	case	when	EXISTS
                                                                                                                                                                                                                                                                                                                (	select	1
                                                                                                                                                                                                                                                                                                                        from	usvtimg01."REINSURAN" REI
                                                                                                                                                                                                                                                                                                                        where	REI."SCERTYPE" = pol."SCERTYPE" AND REI."NBRANCH" = cla."NBRANCH"
                                                                                                                                                                                                                                                                                                                        and		REI."NPRODUCT" = pol."NPRODUCT" AND REI."NPOLICY" = cla."NPOLICY"
                                                                                                                                                                                                                                                                                                                        and		REI."NCERTIF" = cla."NCERTIF" AND REI."NBRANCH_REI" = gen."NBRANCH_REI"
                                                                                                                                                                                                                                                                                                                        and		cast(REI."DEFFECDATE" as DATE) <= cast(cla."DOCCURDAT" as date)
                                                                                                                                                                                                                                                                                                                        and		(REI."DNULLDATE" IS NULL OR CAST(REI."DNULLDATE" as DATE) > cast(cla."DOCCURDAT" as date)))
                                                                                                                                                                                                                                                                                                then    1
                                                                                                                                                                                                                                                                                                else 	case 	when	EXISTS
                                                                                                                                                                                                                                                                                                                                                (   SELECT  1
                                                                                                                                                                                                                                                                                                                                                        from	usvtimg01."REINSURAN" REI
                                                                                                                                                                                                                                                                                                                                                        where	REI."SCERTYPE" = pol."SCERTYPE" AND REI."NBRANCH" = cla."NBRANCH"
                                                                                                                                                                                                                                                                                                                                                        and		REI."NPRODUCT" = pol."NPRODUCT" AND REI."NPOLICY" = cla."NPOLICY"
                                                                                                                                                                                                                                                                                                                                                        and		REI."NCERTIF" = 0 and REI."NBRANCH_REI" = gen."NBRANCH_REI"
                                                                                                                                                                                                                                                                                                                                                        and		cast(REI."DEFFECDATE" as DATE) <= cast(cla."DOCCURDAT" as date)
                                                                                                                                                                                                                                                                                                                                                        and		(REI."DNULLDATE" IS NULL OR CAST(REI."DNULLDATE" as DATE) > cast(cla."DOCCURDAT" as date)))
                                                                                                                                                                                                                                                                                                THEN    2
                                                                                                                                                                                                                                                                                                ELSE    0 END END
                                                                                                                                                                                                                                                                ELSE    0 END)) flag_rea
                                                                                                                                                                                                                from    usvtimg01."COVER" cov
                                                                                                                                                                                                                join	usvtimg01."GEN_COVER" gen
                                                                                                                                                                                                                                on		gen."NCOVER" = cov."NCOVER"
                                                                                                                                                                                                                                and     gen."NPRODUCT" = cov."NPRODUCT"
                                                                                                                                                                                                                                and     gen."NMODULEC" = cov."NMODULEC"
                                                                                                                                                                                                                                and     gen."NBRANCH" = cov."NBRANCH"
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
                                                                                                                                                                                                                and     (cov."DNULLDATE" is null or cast(cov."DNULLDATE" as date) > cast(cla."DOCCURDAT" as date))
                                                                                                                                                                                                                group	by 1) cov) --NO HAY '3' EN VTIME
                                                                                                                                                                end, 0) nshare_rea,
                                                                                                case    when    cla."SSTACLAIM" <> '6' and pol."SCERTYPE"  = '2'
                                                                                                        then    coalesce(   coalesce((  select  max(cpl."NCURRENCY")
                                                                                                                                                                                                        from    usvtimg01."CURREN_POL" cpl
                                                                                                                                                                                                        where 	cpl."SCERTYPE" = cla."SCERTYPE"
                                                                                                                                                                                                        and		cpl."NBRANCH" = cla."NBRANCH"
                                                                                                                                                                                                        and		cpl."NPRODUCT" = pol."NPRODUCT"
                                                                                                                                                                                                        and     cpl."NPOLICY" = cla."NPOLICY"
                                                                                                                                                                                                        and     cpl."NCERTIF" = cla."NCERTIF"),
                                                                                                                                        (   select  max(cpl."NCURRENCY")
                                                                                                                                                                                                        from    usvtimg01."CURREN_POL" cpl
                                                                                                                                                                                                        where 	cpl."SCERTYPE" = cla."SCERTYPE"
                                                                                                                                                                                                        and		cpl."NBRANCH" = cla."NBRANCH"
                                                                                                                                                                                                        and		cpl."NPRODUCT" = pol."NPRODUCT"
                                                                                                                                                                                                        and     cpl."NPOLICY" = cla."NPOLICY")),0)
                                                                                                        else    0 end moneda_cod
                                                                                                        from    usvtimg01."POLICY" pol
                                                                                                        join	usvtimg01."CLAIM" cla
                                                                                                        on		cla."SCERTYPE" = pol."SCERTYPE"
                                                                                                        and     cla."NPOLICY" = pol."NPOLICY"
                                                                                                        and     cla."NBRANCH" = pol."NBRANCH"
                                                                                                                        and     exists
                                                                                                                (   select  1
                                                                                                                        from    usvtimg01."CLAIM_HIS" clh
                                                                                                                                                join	(	select	cast("SVALUE" as INT4) "SVALUE"
                                                                                                                                                                        from	usvtimg01."CONDITION_SERV" cs 
                                                                                                                                                                        where	"NCONDITION" in (71,72,73)) csv --reservas, ajustes y pagos
                                                                                                                                                                on	clh."NOPER_TYPE" = csv."SVALUE"
                                                                                                                        where	coalesce(clh."NCLAIM",0) = cla."NCLAIM"
                                                                                                                        and		cast(clh."DOPERDATE" as date) >= '12/31/2015')) cla --ejecutar al a�o 2021 (2015 fue para pruebas por volumen de datos)
                                                                                join	usvtimg01."CLAIM_HIS" clh
                                                                                                on		clh."NCLAIM" = cla."NCLAIM"
                                                                                                and     cast(clh."DOPERDATE" as date) <= '12/31/2023' --en caso de ejecutar a una fecha posterior, alterar esta fecha a la de la ejecuci�n
                                                                                join	(	select	case 	when	"NCONDITION" = 71 then 1
                                                                                                                                        when	"NCONDITION" = 72 then 2
                                                                                                                                        when	"NCONDITION" = 73 then 3
                                                                                                                                        else	0 end tipo,
                                                                                                                        cast("SVALUE" as INT4) "SVALUE"
                                                                                                        from	usvtimg01."CONDITION_SERV" cs 
                                                                                                        where	"NCONDITION" in (71,72,73)) csv --solo reservas, ajustes y pagos
                                                                                                on	clh."NOPER_TYPE" = csv."SVALUE") clh
                                                        group by 1,2,3,4) cl0
                                join	usvtimg01."CLAIM" cla on cla.ctid = cl0.cla_id
                                join	usvtimg01."POLICY" pol on pol.ctid = cl0.pol_id
                            ) AS TMP       
                            '''

    L_DF_SBSIN_VTIME_LPG = glue_context.read.format('jdbc').options(**connection).option("fetchsize",10000).option("dbtable",L_SBSIN_VTIME_LPG).load()


    L_SBSIN_VTIME_LPV = '''
                            (
                               select	        'D' INDDETREC,
                                                'SBSIN' TABLAIFRS17,
                                                '' PK,
                                                '' DTPREG, --excluido
                                                '' TIOCPROC, --no disponible
                                                coalesce(cast(cast(cla."DCOMPDATE" as date) as varchar),'') TIOCFRM,
                                                '' TIOCTO, --excluido
                                                'PVV' KGIORIGM,
                                                'LPV' KSCCOMPA,
                                                coalesce(cast(cla."NBRANCH" as varchar),'') KGCRAMO,
                                                cla."NBRANCH" || '-' || pol."NPRODUCT" KABPRODT,
                                                coalesce(cast(cla."NPOLICY" as varchar),'') DNUMAPO,
                                                coalesce(cast(cla."NCERTIF" as varchar),'') DNMCERT,
                                                coalesce(cast(cla."NCLAIM" as varchar),'') DNUMSIN,
                                                coalesce(cast(cla."DOCCURDAT" as varchar),'') TOCURSIN,
                                                '' DHSINIST, --excluido
                                                coalesce(cast(cla."DOCCURDAT" as varchar),'') TABERSIN,
                                                '' TPARTSIN, --excluido
                                                coalesce(
                                                        case	when	trim(cla."SSTACLAIM") in ('1','5','7')
                                                                        then	(	select	cast(cast(max("DOPERDATE") as date) as varchar)
                                                                                                from	usvtimv01."CLAIM_HIS"
                                                                                                where	"NCLAIM" = cla."NCLAIM"
                                                                                                and		case	when	trim(cla."SSTACLAIM") in ('1') then "NOPER_TYPE" in (3,17)
                                                                                                                                when	trim(cla."SSTACLAIM") in ('5') then "NOPER_TYPE" in (11)
                                                                                                                                when	trim(cla."SSTACLAIM") in ('7') then "NOPER_TYPE" in (21)
                                                                                                                                else	"NOPER_TYPE" is null end)
                                                                        else '' end, '')  TFECHTEC,
                                                coalesce(
                                                        case	when	trim(cla."SSTACLAIM") in ('1','5','7')
                                                                        then	(	select	cast(cast(max("DOPERDATE") as date) as varchar)
                                                                                                from	usvtimv01."CLAIM_HIS"
                                                                                                where	"NCLAIM" = cla."NCLAIM"
                                                                                                and		case	when	trim(cla."SSTACLAIM") in ('1') then "NOPER_TYPE" in (3,17)
                                                                                                                                when	trim(cla."SSTACLAIM") in ('5') then "NOPER_TYPE" in (77)
                                                                                                                                when	trim(cla."SSTACLAIM") in ('7') then "NOPER_TYPE" in (21)
                                                                                                                                else	"NOPER_TYPE" is null end)
                                                                        else '' end, '')  TFECHADM,
                                                coalesce(
                                                        (	select	cast(cast(max("DOPERDATE") as date) as varchar)
                                                                from	usvtimv01."CLAIM_HIS"
                                                                where	"NCLAIM" = cla."NCLAIM"
                                                                and		"NOPER_TYPE" in
                                                                                (	select	"NOPER_TYPE"
                                                                                        from	usvtimv01."TABLE140"
                                                                                        where	"NOPER_TYPE" in
                                                                                                        (	select	cast("SVALUE" as INT4)
                                                                                                                from	usvtimv01."CONDITION_SERV" cs 
                                                                                                                where	"NCONDITION" in (73))
                                                                                                        or "NOPER_TYPE" in (75,76,77,78,79,80,83))),'') TESTADO,
                                                coalesce(cast(cla."SSTACLAIM" as varchar),'') KSCSITUA,
                                                '' KSCMOTSI, --excluido
                                                '' KSCTPSIN, --no disponible
                                                coalesce(cast(cla."NCAUSECOD" as varchar),'') KSCCAUSA,
                                                '' KSCARGES, --excluido
                                                '' KSCFMPGS, --no disponible
                                                '' KCBMED_DRA, --excluido
                                                '' KCBMED_PG, --excluido
                                                '' KCBMED_PD, --excluido
                                                '' KCBMED_P2, --excluido
                                                case	when pol."SBUSSITYP" = '1' and cl0.nshare_coa = 100 then 1 --Sin coaseguro
                                                                when pol."SBUSSITYP" = '1' and cl0.nshare_coa <> 100 then 2 --Con coaseguro, compa��a l�der
                                                                when pol."SBUSSITYP" = '2' then 3 --Con coaseguro, compa��a no l�der
                                                                else 0 end KSCTPCSG,
                                                cast(coalesce(
                                                                coalesce((  select  max(cpl."NCURRENCY")
                                                        from    usvtimv01."CURREN_POL" cpl
                                                        where 	cpl."SCERTYPE" = cla."SCERTYPE"
                                                        and		cpl."NBRANCH" = cla."NBRANCH"
                                                        and		cpl."NPRODUCT" = pol."NPRODUCT"
                                                        and     cpl."NPOLICY" = cla."NPOLICY"
                                                        and     cpl."NCERTIF" = cla."NCERTIF"),
                                                        (   select  max(cpl."NCURRENCY")
                                                        from    usvtimv01."CURREN_POL" cpl
                                                        where 	cpl."SCERTYPE" = cla."SCERTYPE"
                                                        and		cpl."NBRANCH" = cla."NBRANCH"
                                                        and		cpl."NPRODUCT" = pol."NPRODUCT"
                                                        and     cpl."NPOLICY" = cla."NPOLICY")),0) as varchar) KSCMOEDA,
                                                '' VCAMBIO, --no disponible
                                                '' VTXRESPN, --no disponible
                                                cast(cl0.reserva + cl0.ajustes as numeric(14,2)) VMTPROVI,
                                                '' VMTPRVINI, --excluido
                                                cast((cl0.reserva + cl0.ajustes) * (case when pol."SBUSSITYP" = '2' then 100 else nshare_coa end/100) as numeric(14,2)) VMTPRVRS, --no hay reaseguros en VT-LPV
                                                cast((cl0.reserva + cl0.ajustes) * (case when pol."SBUSSITYP" = '2' then 100 else nshare_coa end/100) as numeric(14,2)) VMTCOSEG,
                                                '' KSCNATUR, --excluido
                                                '' TALTENAT, --excluido
                                                '' DUSRREG, --excluido
                                                coalesce((	select	max("SCLIENT_VGT")
                                                                        from	usvtimg01."WBSTBLCLIDEPEQUI" 
                                                                        where	"SCLIENT_OLD" = cla."SCLIENT"),
                                                                cla."SCLIENT") KEBENTID_TO,
                                                cast(	coalesce((	select	case	when	max(gen."SADDSUINI") = '3'
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
                                                                                                        and     gen."SADDSUINI" in ('1','3')
                                                                                        where   cov."SCERTYPE" = pol."SCERTYPE" 
                                                                                        and     cov."NBRANCH" = cla."NBRANCH"
                                                                                        and     cov."NPRODUCT" = cla."NPRODUCT"
                                                                                        and     cov."NPOLICY" = cla."NPOLICY"
                                                                                        and     cov."NCERTIF" = cla."NCERTIF"
                                                                                        and     cast(cov."DEFFECDATE" as date) <= cast(cla."DOCCURDAT" as date)
                                                                                        and     (cov."DNULLDATE" is null or cast(cov."DNULLDATE" as date) > cast(cla."DOCCURDAT" as date))), 0) as numeric(14,2)) VCAPITAL,
                                                '' VTXINDEM, --no disponible
                                                '' VMTINDEM, --no disponible
                                                '' TINICIND, --no disponible
                                                '' DULTACTA, --excluido
                                                coalesce(	case	coalesce(cla."NCERTIF",0)
                                                                                        when	0
                                                                                        then	pol."SSTATUS_POL"
                                                                                        else	(	select	cer."SSTATUSVA"
                                                                                                                from 	usvtimv01."CERTIFICAT" cer
                                                                                                                where	cer."SCERTYPE" = pol."SCERTYPE"
                                                                                                                and 	cer."NBRANCH" = cla."NBRANCH"
                                                                                                                and 	cer."NPOLICY" = cla."NPOLICY"
                                                                                                                and		cer."NCERTIF" = cla."NCERTIF"
                                                                                                                and		CER."NDIGIT" = 0)
                                                                                        end, '') KACESTAP,
                                                cast(	case	coalesce(cla."NCERTIF",0)
                                                                                when	0
                                                                                then	case	when	coalesce(pol."NPAYFREQ",'0') = '1'
                                                                                                                then	1
                                                                                                                else	0 end
                                                                                else	case	when	coalesce((	select	cer."NPAYFREQ"
                                                                                                                                                        from 	usvtimv01."CERTIFICAT" cer
                                                                                                                                                        where	cer."SCERTYPE" = pol."SCERTYPE"
                                                                                                                                                        and 	cer."NBRANCH" = cla."NBRANCH"
                                                                                                                                                        and 	cer."NPOLICY" = cla."NPOLICY"
                                                                                                                                                        and		cer."NCERTIF" = cla."NCERTIF"
                                                                                                                                                        and		CER."NDIGIT" = 0),'0') = '1'
                                                                                                                then	1
                                                                                                                else	0 end
                                                                                end as varchar) DTERMO,
                                                '' TULTALT, --excluido
                                                '' DUSRUPD, --excluido
                                                '' KABUNRIS, --SE DESCONOCE ABUNRIS.PK
                                                '' DUSRENT, --excluido
                                                '' DUSRSUP, --excluido
                                                coalesce(cla."SSTACLAIM",'') KSCESTSN,
                                                '' KSCFMPTI, --excluido
                                                '' KSCTPAUT, --excluido
                                                '' KSCLOCSN, --excluido
                                                '' KSCINBMT, --excluido
                                                '' KSCINIDS, --excluido
                                                '' DPROCIDS, --excluido
                                                '' KSCETIDS, --excluido
                                                'LPV' DCOMPA,
                                                '' DMARCA, --excluido
                                                '' KSCNATZA_IN, --excluido
                                                '' KSCNATZA_FI, --excluido
                                                '' KSCCDEST, --excluido
                                                '' KACARGES, --excluido
                                                '' DNUMOBJ, --excluido
                                                '' DNUMOB2, --excluido
                                                coalesce((	select	max("SCLIENT_VGT")
                                                                        from	usvtimg01."WBSTBLCLIDEPEQUI" 
                                                                        where	"SCLIENT_OLD" = cla."SCLIENT"),
                                                                cla."SCLIENT") DUNIDRISC,
                                                coalesce((	select	cast(cast(max("DOPERDATE") as date) as varchar)
                                                                        from	usvtimv01."CLAIM_HIS"
                                                                        where	"NCLAIM" = cla."NCLAIM"
                                                                        and		"NOPER_TYPE" = 16),'') TDTREABE,
                                                '' DEQREGUL, --excluido
                                                '' KACMOEST, --excluido
                                                '' KACCONCE, --excluido
                                                '' KSCCONTE, --excluido
                                                '' KSCSTREE, --no disponible
                                                cast(coalesce(	case	when	pol."SBUSSITYP" = '2'
                                                                                                then	pol."NLEADSHARE"
                                                                                                else	cl0.nshare_coa
                                                                                                end,0) as numeric(7,4)) VTXCOSEG,
                                                '' KSCPAIS, --no disponible
                                                '' KSCDEFRP, --excluido
                                                '' KSCORPAR, --excluido
                                                '' DTPRCAS, --excluido
                                                '' TDTESTAD, --no disponible
                                                '' KSBSIN_MP, --excluido
                                                '' TMIGPARA, --excluido
                                                '' KSBSIN_MD, --excluido
                                                '' TMIGDE, --excluido
                                                cla."NBRANCH" || '-' || pol."NPRODUCT" || '-' || cla."NPOLICY" || '-' || cla."NCERTIF" KABAPOL,
                                                '' TPRENCER, --excluido
                                                '' TDTREEMB, --no disponible
                                                '' TENTPLAT, --excluido
                                                '' DHENTPLA, --excluido
                                                '' TENTCOMP, --excluido
                                                '' DHENTCOM, --excluido
                                                '' TPEDPART, --excluido
                                                '' TDTRECLA, --excluido
                                                '' TDECFIN, --excluido
                                                '' TASSRESP, --excluido
                                                '' DINDENCU, --excluido
                                                '' KSCMTENC, --excluido
                                                '' DQTDAAA, --excluido
                                                '' DINFACTO, --excluido
                                                '' TINISUSP, --excluido
                                                '' TFIMSUSP, --excluido
                                                '' DINSOPRE, --excluido
                                                '' KSCTPDAN, --excluido
                                                '' KABAPOL_EFT, --excluido
                                                '' DARQUIVO, --excluido
                                                '' TARQUIVO, --excluido
                                                '' DLOCREF, --excluido
                                                '' KACPARES, --no disponible
                                                '' KGCRAMO_SAP, --excluido
                                                '' DNUMPGRE, --no disponible
                                                '' DINDSINTER, --FALTA C�LCULO
                                                '' DQDREABER, --excluido
                                                '' TPLANOCOSEG, --excluido
                                                '' TPLANORESEG, --excluido
                                                case	when pol."SBUSSITYP" = '1' and cl0.nshare_coa = 100 then '0' --Paga todo
                                                                when pol."SBUSSITYP" = '1' and cl0.nshare_coa <> 100 then '1' --No paga todo
                                                                when pol."SBUSSITYP" = '2' then '0' --Paga todo
                                                                else '0' end KSCPAGCSG,
                                                '' KSCAPLGES, --excluido
                                                '' DANUAGR, --excluido
                                                '' DENTIDSO, --excluido
                                                '' DNOFSIN, --excluido
                                                '' DIMAGEM, --excluido
                                                '' KEBENTID_GS, --excluido
                                                '' KOCSCOPE, --excluido
                                                '' DCDINTTRA --excluido
                                from	(	select	clh.cla_id,
                                                                        clh.pol_id,
                                                                        clh.nshare_coa,
                                                                        sum(clh.monto_trans * case clh.tipo when 1 then 1 else 0 end ) reserva,
                                                                        sum(clh.monto_trans * case clh.tipo when 2 then 1 else 0 end ) ajustes,
                                                                        sum(clh.monto_trans * case clh.tipo when 3 then 1 else 0 end ) pagos
                                                        from 	(	select  cla.*,
                                                                                        csv.tipo,
                                                                                                case	when	cla.moneda_cod = 1
                                                                                                                then	case	when	clh."NCURRENCY" = 1
                                                                                                                                                then	clh."NAMOUNT"
                                                                                                                                                when	clh."NCURRENCY" = 2
                                                                                                                                                then	clh."NAMOUNT" * clh."NEXCHANGE"
                                                                                                                                                else	0 end
                                                                                                                when	cla.moneda_cod = 2
                                                                                                                then	case	when	clh."NCURRENCY" = 2
                                                                                                                                                then	clh."NAMOUNT"
                                                                                                                                                when	clh."NCURRENCY" = 1
                                                                                                                                                then	clh."NLOC_AMOUNT"
                                                                                                                                                else	0 end
                                                                                                                                else	0 end monto_trans
                                                                                from    (   select  case    when    cla."SSTACLAIM" <> '6' and pol."SCERTYPE" = '2'
                                                                                                        then    cla."NCLAIM"
                                                                                                        else    null end "NCLAIM",
                                                                                                                        case    when    cla."SSTACLAIM" <> '6' and pol."SCERTYPE" = '2'
                                                                                                        then    cla.ctid
                                                                                                        else    null end cla_id,
                                                                                                                        case    when    pol."SCERTYPE" = '2'
                                                                                                        then    pol.ctid
                                                                                                        else    null end pol_id,
                                                                                                case    when    cla."SSTACLAIM" <> '6' and pol."SCERTYPE"  = '2'
                                                                                                                                        then	case	pol."SBUSSITYP"
                                                                                                                                                                        when	'2'
                                                                                                                                                                        then 	100 - coalesce(pol."NLEADSHARE",0)
                                                                                                                                                                        when	'3'
                                                                                                                                                                        then 	null
                                                                                                                                                                        else	coalesce((  select 	"NSHARE"
                                                                                                                                                                                                                from	usvtimv01."COINSURAN" coi
                                                                                                                                                                                                                where	coi."SCERTYPE" = cla."SCERTYPE"
                                                                                                                                                                                                                and     coi."NBRANCH" = cla."NBRANCH"
                                                                                                                                                                                                                and     coi."NPRODUCT" = pol."NPRODUCT"
                                                                                                                                                                                                                and     coi."NPOLICY" = cla."NPOLICY"
                                                                                                                                                                                                                and 	coi."NCOMPANY" is not null
                                                                                                                                                                                                                and     cast(coi."DEFFECDATE" as date) <= cast(cla."DOCCURDAT" as date)
                                                                                                                                                                                                                and     (coi."DNULLDATE" is null or cast(coi."DNULLDATE" as date) > cast(cla."DOCCURDAT" as date))
                                                                                                                                                                                                                and		coi."NCOMPANY" in (1)),100) end 
                                                                                                                                        else	0 end nshare_coa,
                                                                                                case    when    cla."SSTACLAIM" <> '6' and pol."SCERTYPE"  = '2'
                                                                                                        then    coalesce(   coalesce((  select  max(cpl."NCURRENCY")
                                                                                                                                                                                                        from    usvtimv01."CURREN_POL" cpl
                                                                                                                                                                                                        where 	cpl."SCERTYPE" = cla."SCERTYPE"
                                                                                                                                                                                                        and		cpl."NBRANCH" = cla."NBRANCH"
                                                                                                                                                                                                        and		cpl."NPRODUCT" = pol."NPRODUCT"
                                                                                                                                                                                                        and     cpl."NPOLICY" = cla."NPOLICY"
                                                                                                                                                                                                        and     cpl."NCERTIF" = cla."NCERTIF"),
                                                                                                                                        (   select  max(cpl."NCURRENCY")
                                                                                                                                                                                                        from    usvtimv01."CURREN_POL" cpl
                                                                                                                                                                                                        where 	cpl."SCERTYPE" = cla."SCERTYPE"
                                                                                                                                                                                                        and		cpl."NBRANCH" = cla."NBRANCH"
                                                                                                                                                                                                        and		cpl."NPRODUCT" = pol."NPRODUCT"
                                                                                                                                                                                                        and     cpl."NPOLICY" = cla."NPOLICY")),0)
                                                                                                        else    0 end moneda_cod
                                                                                                        from    usvtimv01."POLICY" pol
                                                                                                        join	usvtimv01."CLAIM" cla
                                                                                                        on		cla."SCERTYPE" = pol."SCERTYPE"
                                                                                                        and     cla."NPOLICY" = pol."NPOLICY"
                                                                                                        and     cla."NBRANCH" = pol."NBRANCH"
                                                                                                                        and     exists
                                                                                                                (   select  1
                                                                                                                        from    usvtimv01."CLAIM_HIS" clh
                                                                                                                                                join	(	select	cast("SVALUE" as INT4) "SVALUE"
                                                                                                                                                                        from	usvtimv01."CONDITION_SERV" cs 
                                                                                                                                                                        where	"NCONDITION" in (71,72,73)) csv --reservas, ajustes y pagos
                                                                                                                                                                on	clh."NOPER_TYPE" = csv."SVALUE"
                                                                                                                        where	coalesce(clh."NCLAIM",0) = cla."NCLAIM"
                                                                                                                        and		cast(clh."DOPERDATE" as date) >= '12/31/2015')) cla --ejecutar al a�o 2021 (2015 fue para pruebas por volumen de datos)
                                                                                join	usvtimv01."CLAIM_HIS" clh
                                                                                                on		clh."NCLAIM" = cla."NCLAIM"
                                                                                                and     cast(clh."DOPERDATE" as date) <= '12/31/2023' --en caso de ejecutar a una fecha posterior, alterar esta fecha a la de la ejecuci�n
                                                                                join	(	select	case 	when	"NCONDITION" = 71 then 1
                                                                                                                                        when	"NCONDITION" = 72 then 2
                                                                                                                                        when	"NCONDITION" = 73 then 3
                                                                                                                                        else	0 end tipo,
                                                                                                                        cast("SVALUE" as INT4) "SVALUE"
                                                                                                        from	usvtimv01."CONDITION_SERV" cs 
                                                                                                        where	"NCONDITION" in (71,72,73)) csv --solo reservas, ajustes y pagos
                                                                                                on	clh."NOPER_TYPE" = csv."SVALUE") clh
                                                        group by 1,2,3) cl0
                                join	usvtimv01."CLAIM" cla on cla.ctid = cl0.cla_id
                                join	usvtimv01."POLICY" pol on pol.ctid = cl0.pol_id 
                            ) AS TMP     
                            '''

    L_DF_SBSIN_VTIME_LPV = glue_context.read.format('jdbc').options(**connection).option("fetchsize",10000).option("dbtable",L_SBSIN_VTIME_LPV).load()

    L_DF_SBSIN_VTIME = L_DF_SBSIN_VTIME_LPG.union(L_DF_SBSIN_VTIME_LPV)
    

    L_SBSIN_INSIS = '''
                            (
                                select	        'D' INDDETREC,
                                                'SBSIN' TABLAIFRS17,
                                                '' PK,
                                                '' DTPREG, --excluido
                                                '' TIOCPROC, --no disponible
                                                coalesce(cast(cast(cla."REGISTRATION_DATE" as date) as varchar),'') TIOCFRM,
                                                '' TIOCTO, --excluido
                                                'PNV' KGIORIGM,
                                                'LPV' KSCCOMPA,
                                                /*
                                                coalesce((	SELECT	DISTINCT ter.ramcom
                                                                        FROM	usinsiv01.CFGLPV_POLICY_TECHBRANCH_SBS tbs
                                                                        JOIN	usinsiv01.CUST_TBL_EQUIV_RAMCOM ter
                                                                                        ON		ter.insr_type = tbs.insr_type
                                                                                        AND		ter.as_is = tbs.as_is_product
                                                                                        AND		ter.tech_branch = tbs.technical_branch
                                                                        WHERE	tbs.insr_type = pol.insr_type
                                                                        AND		tbs.technical_branch = pol.attr1
                                                                        AND		tbs.sbs_code = pol.attr2),0)*/ '' KGCRAMO,
                                                pol."INSR_TYPE" KABPRODT,
                                                coalesce(	case 	when	cl0.pep_id is not null
                                                                                        then	(select "POLICY_NO" from usinsiv01."POLICY" where "POLICY_ID" = cl0.pep_id)
                                                                                        else	pol."POLICY_NO" end,'') DNUMAPO,
                                                coalesce(	case 	when	cl0.pep_id is not null
                                                                                        then	pol."POLICY_NO"
                                                                                        else	null end,'') DNMCERT,
                                                coalesce(cast(cla."CLAIM_REGID" as varchar),'') DNUMSIN,
                                                coalesce(cast(cast(cla."EVENT_DATE" as date) as varchar),'') TOCURSIN,
                                                '' DHSINIST, --excluido
                                                coalesce(cast(cast(cla."EVENT_DATE" as date) as varchar),'') TABERSIN,
                                                '' TPARTSIN, --excluido
                                                coalesce((	select	cast(cast(max(clh."REGISTRATION_DATE") as date) as varchar)
                                                                        from 	usinsiv01."CLAIM_OBJECTS" clo
                                                                        join	usinsiv01."CLAIM_RESERVE_HISTORY" clh
                                                                                        on		clh."CLAIM_ID" = clo."CLAIM_ID"
                                                                                        and		clh."REQUEST_ID" = clo."REQUEST_ID"
                                                                                        and		clh."OP_TYPE" IN ('PAYMCONF','PAYMINV')
                                                                        where	clo."CLAIM_ID" = cla."CLAIM_ID"
                                                                        and		clo."CLAIM_STATE" in (2,3)),'') TFECHTEC,
                                                coalesce((	select	cast(cast(max(clh."REGISTRATION_DATE") as date) as varchar)
                                                                        from 	usinsiv01."CLAIM_OBJECTS" clo
                                                                        join	usinsiv01."CLAIM_RESERVE_HISTORY" clh
                                                                                        on		clh."CLAIM_ID" = clo."CLAIM_ID"
                                                                                        and		clh."REQUEST_ID" = clo."REQUEST_ID"
                                                                                        and		clh."OP_TYPE" IN ('PAYMCONF','PAYMINV')
                                                                        where	clo."CLAIM_ID" = cla."CLAIM_ID"
                                                                        and		clo."CLAIM_STATE" in (2,3)),'') TFECHADM,
                                                coalesce((	select	cast(cast(max(clh."REGISTRATION_DATE") as date) as varchar)
                                                                        from 	usinsiv01."CLAIM_OBJECTS" clo
                                                                        join	usinsiv01."CLAIM_RESERVE_HISTORY" clh
                                                                                        on		clh."CLAIM_ID" = clo."CLAIM_ID"
                                                                                        and		clh."REQUEST_ID" = clo."REQUEST_ID"
                                                                                        and		clh."OP_TYPE" IN ('REG','EST','CLC','PAYMCONF','PAYMINV')
                                                                        where	clo."CLAIM_ID" = cla."CLAIM_ID"),'') TESTADO,
                                                coalesce((	select	cast(max(clo."CLAIM_STATE") as varchar)
                                                                        from 	usinsiv01."CLAIM_OBJECTS" clo
                                                                        join	usinsiv01."CLAIM_RESERVE_HISTORY" clh
                                                                                        on		clh."CLAIM_ID" = clo."CLAIM_ID"
                                                                                        and		clh."REQUEST_ID" = clo."REQUEST_ID"
                                                                                        and		clh."OP_TYPE" IN ('REG','EST','CLC','PAYMCONF','PAYMINV')
                                                                                        and		clh."REGISTRATION_DATE" = 
                                                                                                        (	select 	MAX("REGISTRATION_DATE")
                                                                                                                from	usinsiv01."CLAIM_RESERVE_HISTORY" ch0
                                                                                                                where	ch0."CLAIM_ID" = clo."CLAIM_ID"
                                                                                                                and		ch0."REQUEST_ID" = clo."REQUEST_ID"
                                                                                                                and		ch0."OP_TYPE" IN ('REG','EST','CLC','PAYMCONF','PAYMINV'))
                                                                        where	clo."CLAIM_ID" = cla."CLAIM_ID"),'') KSCSITUA,
                                                '' KSCMOTSI, --excluido
                                                '' KSCTPSIN, --no disponible
                                                cla."CAUSE_ID" KSCCAUSA, --PENDIENTE AUN (20231031)
                                                '' KSCARGES, --excluido
                                                '' KSCFMPGS, --no disponible
                                                '' KCBMED_DRA, --excluido
                                                '' KCBMED_PG, --excluido
                                                '' KCBMED_PD, --excluido
                                                '' KCBMED_P2, --excluido
                                                1 KSCTPCSG, --Sin coaseguro
                                                case	coalesce((	select	distinct "AV_CURRENCY"
                                                                                        from	usinsiv01."INSURED_OBJECT"
                                                                                        where	"POLICY_ID" = pol."POLICY_ID" limit 1),'')
                                                                when 'USD' then 2
                                                                when 'PEN' then 1
                                                                else 0 end KSCMOEDA,
                                                '' "VCAMBIO", --no disponible
                                                '' "VTXRESPN", --no disponible
                                                cast(cl0.reserva + cl0.ajustes as numeric (14,2)) VMTPROVI,
                                                '' VMTPRVINI, --excluido
                                                cast(cl0.reserva + cl0.ajustes as numeric (14,2)) VMTPRVRS, --no se est� aplicando reaseguros en INSIS
                                                cast(cl0.reserva + cl0.ajustes as numeric (14,2)) VMTCOSEG, --no hay coaseguros en INSIS
                                                '' KSCNATUR, --excluido
                                                '' TALTENAT, --excluido
                                                '' DUSRREG, --excluido
                                                coalesce((	select	lpi."LEGACY_ID"
                                                                        from	usinsiv01."INTRF_LPV_PEOPLE_IDS" lpi
                                                        where	lpi."MAN_ID" = 
                                                                        (	select	"MAN_ID"
                                                                                from 	usinsiv01."P_CLIENTS" cli 
                                                                                where 	cli."CLIENT_ID" = cla."CLIENT_ID")), cast(cla."CLIENT_ID" as VARCHAR)) KEBENTID_TO,
                                        cast(coalesce((select  sum("INSURED_VALUE")
                                                        from    usinsiv01."INSURED_OBJECT" obj
                                                        where   obj."POLICY_ID" = cla."POLICY_ID"
                                                        and     case  when  not exists
                                                                                (   select  1
                                                                                from    usinsiv01."GEN_ANNEX" ann
                                                                                where   ann."POLICY_ID" = cla."POLICY_ID"
                                                                                and     ann."ANNEX_TYPE" = '17'
                                                                                and     ann."ANNEX_STATE" = '0')
                                                                        then  case  when  cast(pol."INSR_END" as date) > cast(cla."EVENT_DATE" as date) --se busca la prima a la fecha vigente solicitada
                                                                                then  cast(cla."EVENT_DATE" as date) --se busca la prima a la fecha vigente solicitada
                                                                                else  cast(pol."INSR_END" as date) end
                                                                        else  (   select  cast("INSR_BEGIN" as date) -1 --se busca la prima a la fecha de operaci�n anterior a su anulaci�n
                                                                                from    usinsiv01."GEN_ANNEX" ann
                                                                                where   ann."POLICY_ID" = cla."POLICY_ID"
                                                                                and     ann."ANNEX_TYPE" = '17'
                                                                                and     ann."ANNEX_STATE" = '0'
                                                                                and     (ann."INSR_BEGIN" is null or cast(ann."INSR_BEGIN" as date) > cast(cla."EVENT_DATE" as date))) end --se busca la vigencia acorde a la fecha que se solicita
                                                between cast(obj."INSR_BEGIN" as date) and cast(obj."INSR_END" as date)),0) as float) VCAPITAL,
                                                '' VTXINDEM, --no disponible
                                                '' VMTINDEM, --no disponible
                                                '' TINICIND, --no disponible
                                                '' DULTACTA, --excluido
                                                coalesce(	case 	when	cl0.pep_id is not null
                                                                                        then	(select "POLICY_STATE" from usinsiv01."POLICY" where "POLICY_ID" = cl0.pep_id)
                                                                                        else	pol."POLICY_STATE" end,0) KACESTAP,
                                                coalesce(	case 	when	cl0.pep_id is not null
                                                                                        then	(select "ATTR5" from usinsiv01."POLICY" where "POLICY_ID" = cl0.pep_id)
                                                                                        else	pol."ATTR5" end,'') DTERMO,
                                                '' TULTALT, --excluido
                                                '' DUSRUPD, --excluido
                                                '' KABUNRIS, --SE DESCONOCE ABUNRIS.PK
                                                '' DUSRENT, --excluido
                                                '' DUSRSUP, --excluido
                                                (	select	max(clo."CLAIM_STATE")
                                                        from 	usinsiv01."CLAIM_OBJECTS" clo
                                                        join	usinsiv01."CLAIM_RESERVE_HISTORY" clh
                                                                        on		clh."CLAIM_ID" = clo."CLAIM_ID"
                                                                        and		clh."REQUEST_ID" = clo."REQUEST_ID"
                                                                        and		clh."OP_TYPE" IN ('REG','EST','CLC','PAYMCONF','PAYMINV')
                                                                        and		clh."REGISTRATION_DATE" = 
                                                                                        (	select 	MAX("REGISTRATION_DATE")
                                                                                                from	usinsiv01."CLAIM_RESERVE_HISTORY" ch0
                                                                                                where	ch0."CLAIM_ID" = clo."CLAIM_ID"
                                                                                                and		ch0."REQUEST_ID" = clo."REQUEST_ID"
                                                                                                and		ch0."OP_TYPE" IN ('REG','EST','CLC','PAYMCONF','PAYMINV'))
                                                        where	clo."CLAIM_ID" = cla."CLAIM_ID") KSCESTSN, 
                                                '' KSCFMPTI, --excluido
                                                '' KSCTPAUT, --excluido
                                                '' KSCLOCSN, --excluido
                                                '' KSCINBMT, --excluido
                                                '' KSCINIDS, --excluido
                                                '' DPROCIDS, --excluido
                                                '' KSCETIDS, --excluido
                                                'LPV' "DCOMPA",
                                                '' DMARCA, --excluido
                                                '' KSCNATZA_IN, --excluido
                                                '' KSCNATZA_FI, --excluido
                                                '' KSCCDEST, --excluido
                                                '' KACARGES, --excluido
                                                '' DNUMOBJ, --excluido
                                                '' DNUMOB2, --excluido
                                                coalesce((	select	lpi."LEGACY_ID"
                                                                        from	usinsiv01."INTRF_LPV_PEOPLE_IDS" lpi
                                                        where	lpi."MAN_ID" = 
                                                                        (	select	"MAN_ID"
                                                                                from 	usinsiv01."P_CLIENTS" cli 
                                                                                where 	cli."CLIENT_ID" = cla."CLIENT_ID")), cast(cla."CLIENT_ID" as VARCHAR)) DUNIDRISC,
                                                '' TDTREABE, --no se maneja en INSIS el concepto de reapertura para los siniestros
                                                '' DEQREGUL, --excluido
                                                '' KACMOEST, --excluido
                                                '' KACCONCE, --excluido
                                                '' KSCCONTE, --excluido
                                                '' KSCSTREE, --no disponible
                                                100 VTXCOSEG, --se considera el 100% como reteci�n
                                                '' KSCPAIS, --agregar c�lculo
                                                '' KSCDEFRP, --excluido
                                                '' KSCORPAR, --excluido
                                                '' DTPRCAS, --excluido
                                                '' TDTESTAD, --no disponible
                                                '' KSBSIN_MP, --excluido
                                                '' TMIGPARA, --excluido
                                                '' KSBSIN_MD, --excluido
                                                '' TMIGDE, --excluido
                                                cast(pol."POLICY_ID" as varchar) KABAPOL,
                                                '' TPRENCER, --excluido
                                                '' TDTREEMB, --no disponible
                                                '' TENTPLAT, --excluido
                                                '' DHENTPLA, --excluido
                                                '' TENTCOMP, --excluido
                                                '' DHENTCOM, --excluido
                                                '' TPEDPART, --excluido
                                                '' TDTRECLA, --excluido
                                                '' TDECFIN, --excluido
                                                '' TASSRESP, --excluido
                                                '' DINDENCU, --excluido
                                                '' KSCMTENC, --excluido
                                                '' DQTDAAA, --excluido
                                                '' DINFACTO, --excluido
                                                '' TINISUSP, --excluido
                                                '' TFIMSUSP, --excluido
                                                '' DINSOPRE, --excluido
                                                '' KSCTPDAN, --excluido
                                                '' KABAPOL_EFT, --excluido
                                                '' DARQUIVO, --excluido
                                                '' TARQUIVO, --excluido
                                                '' DLOCREF, --excluido
                                                '' KACPARES, --no disponible
                                                '' KGCRAMO_SAP, --excluido
                                                '' DNUMPGRE, --no disponible
                                                '' DINDSINTER, --FALTA C�LCULO
                                                '' DQDREABER, --excluido
                                                '' TPLANOCOSEG, --excluido
                                                '' TPLANORESEG, --excluido
                                                1 KSCPAGCSG, --Sin coaseguro
                                                '' KSCAPLGES, --excluido
                                                '' DANUAGR, --excluido
                                                '' DENTIDSO, --excluido
                                                '' DNOFSIN, --excluido
                                                '' DIMAGEM, --excluido
                                                '' KEBENTID_GS, --excluido
                                                '' KOCSCOPE, --excluido
                                                '' DCDINTTRA --excluido
                                from	(	select	cla.cla_id,
                                                                        cla.pol_id,
                                                                        cla.pep_id,
                                                                        sum(case when clh."OP_TYPE" in ('REG') then "RESERV_CHANGE" else 0 end) reserva, --REV.
                                                                        sum(case when clh."OP_TYPE" in ('EST','CLC') then "RESERV_CHANGE" else 0 end) ajustes, --REV.
                                                                        sum(case when clh."OP_TYPE" in ('PAYMCONF','PAYMINV') then "RESERV_CHANGE" else 0 end) pagos --REV.
                                                                        
                                                        from	(	select	cla."CLAIM_ID",
                                                                                                (select "MASTER_POLICY_ID" from usinsiv01."POLICY_ENG_POLICIES" where "POLICY_ID" = CLA."POLICY_ID") pep_id,
                                                                                                cla.ctid cla_id,
                                                                                                pol.ctid pol_id,
                                                                                                case	coalesce((	select	distinct "AV_CURRENCY"
                                                                                                                                        from	usinsiv01."INSURED_OBJECT"
                                                                                                                                        where	"POLICY_ID" = pol."POLICY_ID" limit 1),'')
                                                                                                                when 'USD' then 2
                                                                                                                when 'PEN' then 1
                                                                                                                else 0 end moneda_cod
                                                                                from	usinsiv01."CLAIM" cla
                                                                                join	usinsiv01."POLICY" pol on pol."POLICY_ID" = cla."POLICY_ID"
                                                                                where	exists 
                                                                                                (	select	1 
                                                                                                        from	usinsiv01."CLAIM_RESERVE_HISTORY" crh
                                                                                                        where	crh."CLAIM_ID" = cla."CLAIM_ID"
                                                                                                        and		crh."OP_TYPE" IN ('REG','EST','CLC','PAYMCONF','PAYMINV')
                                                                                                        and		cast(crh."REGISTRATION_DATE" as date) >= '12/31/2015')) cla --ejecutar al a�o 2021 (2015 es para pruebas)
                                                        join	usinsiv01."CLAIM_RESERVE_HISTORY" clh
                                                                        on		clh."CLAIM_ID" = cla."CLAIM_ID"
                                                                        and		clh."OP_TYPE" IN ('REG','EST','CLC','PAYMCONF','PAYMINV')
                                                                        and		cast(clh."REGISTRATION_DATE" as date) <= '12/31/2023' --en caso de ejecutar a una fecha posterior, alterar esta fecha a la de la ejecuci�n
                                                        group	by 1,2,3) cl0
                                join	usinsiv01."CLAIM" cla on cla.ctid = cl0.cla_id
                                join	usinsiv01."POLICY" pol on pol.ctid = cl0.pol_id
                            ) AS TMP     
                            '''
                            
    L_DF_SBSIN_INSIS = glue_context.read.format('jdbc').options(**connection).option("fetchsize",10000).option("dbtable",L_SBSIN_INSIS).load()
  
    L_DF_SBSIN = L_DF_SBSIN_INSUNIX.union(L_DF_SBSIN_VTIME).union(L_DF_SBSIN_INSIS)
    
    return L_DF_SBSIN                          
