def get_data(glue_context, connection, p_fecha_inicio, p_fecha_fin):

    INSUNIX_LPG_AMF  =  f'''
                            (select	
                             'D' INDDETREC,
                             'SBSIN' TABLAIFRS17,
		             '' PK,
		             '' DTPREG, --excluido
		             '' TIOCPROC, --no disponible
		             cast(cl0.xxxxdate as varchar) TIOCFRM,
		             '' TIOCTO, --excluido
		             'PIG' KGIORIGM,
		             'LPG' KSCCOMPA,
		             coalesce(cast(cla.branch as varchar),'') KGCRAMO,
		             cla.branch || '-' || pol.product || '-0' KABPRODT, --solo AMF tiene subproducto <> 0 en tabla usinsug01.pol_subproduct
		             coalesce(cast(cla.policy as varchar),'') DNUMAPO,
		             coalesce(cast(cla.certif as varchar),'') DNMCERT,
		             coalesce(cast(cla.claim as varchar),'') DNUMSIN,
		             coalesce(cast(cla.occurdat as varchar),'') TOCURSIN,
		             '' DHSINIST, --excluido
		             coalesce(cast(cla.occurdat as varchar),'') TABERSIN,
		             '' TPARTSIN, --excluido
		             coalesce(cast(
		             	case	when	cla.staclaim in ('1','5','7')
		             			then	(	select 	max(case	when	cla.staclaim in ('1') and oper_type in ('3','17') then clh.operdate
		             											when	cla.staclaim in ('5') and oper_type in ('11') then clh.operdate
		             											when	cla.staclaim in ('7') and oper_type in ('21') then clh.operdate
		             											else	null end)
		             						from	usinsug01.claim_his clh
		             						where	clh.claim = cla.claim
		             						and		trim(oper_type) in ('3','17','11','21')
		             						and		operdate <= cl0.dat_lim)
		             			else 	null end as varchar),'') TFECHTEC,
		             coalesce(cast(
		             	case	when	cla.staclaim in ('1','5','7')
		             			then	(	select 	max(case	when	cla.staclaim in ('1') and oper_type in ('3','17') then clh.operdate
		             											when	cla.staclaim in ('5') and oper_type in ('52') then clh.operdate
		             											when	cla.staclaim in ('7') and oper_type in ('21') then clh.operdate
		             											else	null end)
		             						from	usinsug01.claim_his clh
		             						where	clh.claim = cla.claim
		             						and		trim(oper_type) in ('3','17','52','21')
		             						and		operdate <= cl0.dat_lim)
		             			else 	null end as varchar),'') TFECHADM,
		             coalesce(cast(
		             	(	select 	max(clh.operdate)
		             		from	usinsug01.claim_his clh
		             		where	clh.claim = cla.claim
		             		and		(	trim(oper_type) in 
		             					(select cast(operation as varchar(2)) from usinsug01.tab_cl_ope where reserve = 1 or ajustes = 1 or pay_amount in (1,3))
		             					or trim(oper_type) = '60')
		             		and		operdate <= cl0.dat_lim) as varchar),'') TESTADO,
		             coalesce(cast(cla.staclaim as varchar),'') KSCSITUA,
		             '' KSCMOTSI, --excluido
		             '' KSCTPSIN, --no disponible
		             coalesce(cla.branch || '-' || cla.causecod,'') KSCCAUSA,
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
		             cast(	coalesce(
		             			(	select	max(cpl.currency)
		             				from	usinsug01.curren_pol cpl
		             				where 	cpl.usercomp = cla.usercomp and cpl.company = cla.company
		             				and		cpl.certype = pol.certype and cpl.branch = cla.branch
		             				and 	cpl.policy = cla.policy and cpl.certif = cla.certif),
		             			(	select	max(cpl.currency)
		             				from	usinsug01.curren_pol cpl
		             				where 	cpl.usercomp = cla.usercomp and cpl.company = cla.company
		             				and		cpl.certype = pol.certype and cpl.branch = cla.branch
		             				and 	cpl.policy = cla.policy),0) as varchar) KSCMOEDA,
		             '' VCAMBIO, --no disponible
		             '' VTXRESPN, --no disponible
		             cast(cl0.monto_trans as numeric(14,2)) VMTPROVI,
		             '' VMTPRVINI, --excluido
		             cast(cl0.monto_trans
		             	* (case when pol.bussityp = '2' then 100 else share_coa end/100)
                               * case	when	not exists (	select  1
		             									from    usinsug01.reinsuran rei
		             									where   rei.usercomp = cla.usercomp
		             									and     rei.company = cla.company
		             									and     rei.certype = pol.certype
		             									and     rei.branch = cla.branch
		             									and     rei.policy = cla.policy
		             									and     rei.certif = cla.certif)
            	             	then	1
            	             	else	(	coalesce((	select  sum(coalesce(rei.share,0))
		             									from    usinsug01.reinsuran rei
		             									where   rei.usercomp = cla.usercomp
		             									and     rei.company = cla.company
		             									and     rei.certype = pol.certype
		             									and     rei.branch = cla.branch
		             									and     rei.policy = cla.policy
		             									and     rei.certif = cla.certif
		             									and     rei.effecdate <= cla.occurdat
		             									and     (rei.nulldate is null or rei.nulldate > cla.occurdat)
		             									and     coalesce(rei.type,0) = 1),100)/100) end as numeric(14,2)) VMTPRVRS,
		             cast(cl0.monto_trans * (case when pol.bussityp = '2' then 100 else share_coa end/100) as numeric(14,2)) VMTCOSEG,
		             '' KSCNATUR, --excluido
		             '' TALTENAT, --excluido
		             '' DUSRREG, --excluido
		             cl0.client KEBENTID_TO,
		             cast(coalesce((	select  sum(case when gco.addsuini = '1' then coalesce(cov.capital,0) else 0 end)
		             				from    usinsug01.cover cov
		             				join	usinsug01.gen_cover gco on gco.ctid =
	                                         coalesce(
		             							(   select  max(ctid)
                                                     from	usinsug01.gen_cover
                                                     where	usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = pol.product and coalesce(sub_product,0) = cl0.sub_product and currency = cov.currency --con subproducto
                                                     and     modulec = cov.modulec and cover = cov.cover --con m�dulo
                                                     and     effecdate <= cla.occurdat and (nulldate is null or nulldate > cla.occurdat)
                                                     and     statregt <> '4'),
                                                 (   select  max(ctid)
                                                     from	usinsug01.gen_cover
                                                     where	usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = pol.product and coalesce(sub_product,0) = cl0.sub_product and currency = cov.currency --con subproducto
                                                     and     coalesce(modulec,0) = 0 and cover = cov.cover --sin m�dulo
                                                     and     effecdate <= cla.occurdat and (nulldate is null or nulldate > cla.occurdat)
                                                     and     statregt <> '4'),
		             							(   select  max(ctid)
                                                     from	usinsug01.gen_cover
                                                     where	usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = pol.product and coalesce(sub_product,0) = 0 and currency = cov.currency --sin subproducto
                                                     and     modulec = cov.modulec and cover = cov.cover --con m�dulo
                                                     and     effecdate <= cla.occurdat and (nulldate is null or nulldate > cla.occurdat) --variaci�n 2 vigencias
                                                     and     statregt <> '4'), --variaci�n 3 reg. v�lido
                                                 (   select  max(ctid)
                                                     from	usinsug01.gen_cover
                                                     where	usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = pol.product and coalesce(sub_product,0) = 0 and currency = cov.currency --sin subproducto
                                                     and     coalesce(modulec,0) = 0 and cover = cov.cover --sin m�dulo
                                                     and     effecdate <= cla.occurdat and (nulldate is null or nulldate > cla.occurdat) --variaci�n 2 vigencias
                                                     and     statregt <> '4'), --variaci�n 3 reg. v�lido
		             							(	select  max(ctid)
                                                     from	usinsug01.gen_cover
                                                     where	usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = pol.product and coalesce(sub_product,0) = cl0.sub_product and currency = cov.currency --con subproducto
                                                     and     modulec = cov.modulec and cover = cov.cover --con m�dulo
                                                     and     effecdate <= cla.occurdat and (nulldate is null or nulldate > cla.occurdat)
                                                     and     statregt = '4'),
                                                 (	select  max(ctid)
                                                     from	usinsug01.gen_cover
                                                     where	usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = pol.product and coalesce(sub_product,0) = cl0.sub_product and currency = cov.currency --con subproducto
                                                     and     coalesce(modulec,0) = 0 and cover = cov.cover --sin m�dulo
                                                     and     effecdate <= cla.occurdat and (nulldate is null or nulldate > cla.occurdat)
                                                     and     statregt = '4'),
		             							(	select  max(ctid)
                                                     from	usinsug01.gen_cover
                                                     where	usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = pol.product and coalesce(sub_product,0) = 0 and currency = cov.currency --sin subproducto
                                                     and     modulec = cov.modulec and cover = cov.cover --con m�dulo
                                                     and     effecdate <= cla.occurdat and (nulldate is null or nulldate > cla.occurdat)
                                                     and     statregt = '4'),
                                                 (	select  max(ctid)
                                                     from	usinsug01.gen_cover
                                                     where	usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = pol.product and coalesce(sub_product,0) = 0 and currency = cov.currency --sin subproducto
                                                     and     coalesce(modulec,0) = 0 and cover = cov.cover --sin m�dulo
                                                     and     effecdate <= cla.occurdat and (nulldate is null or nulldate > cla.occurdat)
                                                     and     statregt = '4'), --no est� cortado
		             							(	select  max(ctid)
                                                     from	usinsug01.gen_cover
                                                     where	usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = pol.product and coalesce(sub_product,0) = cl0.sub_product and currency = cov.currency --con subproducto
                                                     and     modulec = cov.modulec and cover = cov.cover --con m�dulo
                                                     and     effecdate <= cla.occurdat and nulldate <= cla.occurdat
                                                     and     statregt <> '4'),
                                                 (   select  max(ctid)
                                                     from	usinsug01.gen_cover
                                                     where	usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = pol.product and coalesce(sub_product,0) = cl0.sub_product and currency = cov.currency --con subproducto
                                                     and     coalesce(modulec,0) = 0 and cover = cov.cover --sin m�dulo
                                                     and     effecdate <= cla.occurdat and nulldate <= cla.occurdat
                                                     and     statregt <> '4'),
		             							(	select  max(ctid)
                                                     from	usinsug01.gen_cover
                                                     where	usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = pol.product and coalesce(sub_product,0) = 0 and currency = cov.currency --sin subproducto
                                                     and     modulec = cov.modulec and cover = cov.cover --con m�dulo
                                                     and     effecdate <= cla.occurdat and nulldate <= cla.occurdat
                                                     and     statregt <> '4'),
                                                 (   select  max(ctid)
                                                     from	usinsug01.gen_cover
                                                     where	usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = pol.product and coalesce(sub_product,0) = 0 and currency = cov.currency --sin subproducto
                                                     and     coalesce(modulec,0) = 0 and cover = cov.cover --sin m�dulo
                                                     and     effecdate <= cla.occurdat and nulldate <= cla.occurdat
                                                     and     statregt <> '4'), -- no est� cortado, pero fue anulado antes del efecto del registro
		             							(	select  max(ctid)
                                                     from	usinsug01.gen_cover
                                                     where	usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = pol.product and coalesce(sub_product,0) = cl0.sub_product and currency = cov.currency --con subproducto
                                                     and     modulec = cov.modulec and cover = cov.cover --con m�dulo
                                                     and     effecdate <= cla.occurdat and nulldate <= cla.occurdat
                                                     and     statregt = '4'),
                                                 (   select  max(ctid)
                                                     from	usinsug01.gen_cover
                                                     where 	usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = pol.product and coalesce(sub_product,0) = cl0.sub_product and currency = cov.currency --con subproducto
                                                     and     coalesce(modulec,0) = 0 and cover = cov.cover --sin m�dulo
                                                     and     effecdate <= cla.occurdat and nulldate <= cla.occurdat
                                                     and     statregt = '4'),
		             							(	select  max(ctid)
                                                     from	usinsug01.gen_cover
                                                     where	usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = pol.product and coalesce(sub_product,0) = 0 and currency = cov.currency --sin subproducto
                                                     and     modulec = cov.modulec and cover = cov.cover --con m�dulo
                                                     and     effecdate <= cla.occurdat and nulldate <= cla.occurdat
                                                     and     statregt = '4'),
                                                 (   select  max(ctid)
                                                     from	usinsug01.gen_cover
                                                     where 	usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = pol.product and coalesce(sub_product,0) = 0 and currency = cov.currency --sin subproducto
                                                     and     coalesce(modulec,0) = 0 and cover = cov.cover --con m�dulo
                                                     and     effecdate <= cla.occurdat and nulldate <= cla.occurdat
                                                     and     statregt = '4'), --no est� cortado y aparte fue anulado antes del efecto del registro
		             							(	select  max(ctid)
                                                     from	usinsug01.gen_cover
                                                     where 	usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = pol.product and coalesce(sub_product,0) = cl0.sub_product and currency = cov.currency --con subproducto
                                                     and     modulec = cov.modulec and cover = cov.cover --con m�dulo
                                                     and     effecdate > cla.occurdat
                                                     and     statregt <> '4'),
                                                 (   select  max(ctid)
                                                     from	usinsug01.gen_cover
                                                     where	usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = pol.product and coalesce(sub_product,0) = cl0.sub_product and currency = cov.currency --con subproducto
                                                     and     coalesce(modulec,0) = 0 and cover = cov.cover --sin m�dulo
                                                     and     effecdate <= cla.occurdat and nulldate <= cla.occurdat
                                                     and     statregt <> '4'), -- no est� cortado, pero tampoco al efecto de la tabla de datos particular
                                 				(	select  max(ctid)
                                                     from	usinsug01.gen_cover
                                                     where 	usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = pol.product and coalesce(sub_product,0) = 0 and currency = cov.currency --sin subproducto
                                                     and     modulec = cov.modulec and cover = cov.cover --con m�dulo
                                                     and     effecdate > cla.occurdat
                                                     and     statregt <> '4'),
                                                 (   select  max(ctid)
                                                     from	usinsug01.gen_cover
                                                     where	usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = pol.product and coalesce(sub_product,0) = 0 and currency = cov.currency --sin subproducto
                                                     and     coalesce(modulec,0) = 0 and cover = cov.cover --sin m�dulo
                                                     and     effecdate <= cla.occurdat and nulldate <= cla.occurdat
                                                     and     statregt <> '4'), -- no est� cortado, pero tampoco al efecto de la tabla de datos particular
		             							(	select  max(ctid)
                                                     from	usinsug01.gen_cover
                                                     where	usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = pol.product and coalesce(sub_product,0) = cl0.sub_product and currency = cov.currency --con sub_producto
                                                     and     modulec = cov.modulec and cover = cov.cover --con m�dulo
                                                     and     effecdate <= cla.occurdat and nulldate <= cla.occurdat
                                                     and     statregt = '4'),
                                                 (   select  max(ctid)
                                                     from	usinsug01.gen_cover
                                                     where	usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = pol.product and coalesce(sub_product,0) = cl0.sub_product and currency = cov.currency --con sub_producto
                                                     and     coalesce(modulec,0) = 0 and cover = cov.cover --sin m�dulo
                                                     and     effecdate > cla.occurdat
                                                     and     statregt = '4'),
		             							(	select  max(ctid)
                                                     from	usinsug01.gen_cover
                                                     where	usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = pol.product and coalesce(sub_product,0) = 0 and currency = cov.currency --sin sub_producto
                                                     and     modulec = cov.modulec and cover = cov.cover --con m�dulo
                                                     and     effecdate <= cla.occurdat and nulldate <= cla.occurdat
                                                     and     statregt = '4'),
                                                 (   select  max(ctid)
                                                     from	usinsug01.gen_cover
                                                     where	usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = pol.product and coalesce(sub_product,0) = 0 and currency = cov.currency --sin sub_producto
                                                     and     coalesce(modulec,0) = 0 and cover = cov.cover --sin m�dulo
                                                     and     effecdate > cla.occurdat
                                                     and     statregt = '4')) --est� cortado y no al efecto de la tabla de datos particular adem�s de estar cortado
		             				where   cov.usercomp = cla.usercomp
		             				and     cov.company = cla.company
		             				and     cov.certype = pol.certype
		             				and     cov.branch = cla.branch
		             				and     cov.policy = cla.policy
		             				and     cov.certif = cla.certif
		             				and     cov.effecdate <= cla.occurdat
		             				and     (cov.nulldate is null or cov.nulldate > cla.occurdat)),0) as numeric(14,2)) VCAPITAL,
		             '' VTXINDEM, --no disponible
		             '' VMTINDEM, --no disponible
		             '' TINICIND, --no disponible
		             '' DULTACTA, --excluido
		             coalesce(	case	trim((select distinct(lower(tabname)) from usinsug01.tab_name_b where branch = cla.branch))
		             					when 	'health'
		             					then 	(	select	statusva
		             								from	usinsug01.health
		             								where	ctid = 
		             							            coalesce(
		             						                    (   select max(ctid) from usinsug01.health
		             						                        where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
		             						                        and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate <= cla.occurdat
		             						                        and (nulldate is null or nulldate > cla.occurdat)
		             						                        and statusva not in ('2','3')),
		             						                    (   select max(ctid) from usinsug01.health
		             						                        where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
		             						                        and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate <= cla.occurdat
		             						                        and (nulldate is null or nulldate >= cla.occurdat)
		             						                        and statusva not in ('2','3')),
		             						                    (   select max(ctid) from usinsug01.health
		             						                        where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
		             						                        and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate <= cla.occurdat
		             						                        and (nulldate is null or nulldate < cla.occurdat)
		             						                        and statusva not in ('2','3')),
		             						                    (   select min(ctid) from usinsug01.health
		             						                        where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
		             						                        and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate > cla.occurdat
		             						                        and (nulldate is null or nulldate > cla.occurdat)
		             						                        and statusva not in ('2','3')),
		             						                    (   select min(ctid) from usinsug01.health
		             						                        where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
		             						                        and branch = cla.branch and policy = cla.policy and certif = cla.certif
		             						                        and statusva not in ('2','3')),
		             						                    (   select min(ctid) from usinsug01.health
		             						                        where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
		             						                        and branch = cla.branch and policy = cla.policy and certif = cla.certif
		             						                        and statusva in ('2','3'))))
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
		             						and		"BRANCHCOM" = cl0.branch)*/
		             					--when	3
		             					when	cla.branch in (6,15,26,29,62,66,67)
		             					then	coalesce((	select	coalesce(	case	when	trim(coalesce(tnb.regist,'')) <> ''
		             																	then	trim(tnb.regist)
		             																	else 	case	when	trim(coalesce(tnb.chassis,'')) <> ''
		             																					then	trim(tnb.chassis)
		             																					else	''
		             																					end
		             																	end,'')
		             										from	usinsug01.auto_peru tnb
		             										where	ctid = 
		             												coalesce(
		             								                    (   select max(ctid) from usinsug01.auto_peru
		             								                        where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
		             								                        and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate <= cla.occurdat
		             								                        and (nulldate is null or nulldate > cla.occurdat)
		             								                        and statusva not in ('2','3')),
		             								                    (   select max(ctid) from usinsug01.auto_peru
		             								                        where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
		             								                        and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate <= cla.occurdat
		             								                        and (nulldate is null or nulldate >= cla.occurdat)
		             								                        and statusva not in ('2','3')),
		             								                    (   select max(ctid) from usinsug01.auto_peru
		             								                        where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
		             								                        and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate <= cla.occurdat
		             								                        and (nulldate is null or nulldate < cla.occurdat)
		             								                        and statusva not in ('2','3')),
		             								                    (   select min(ctid) from usinsug01.auto_peru
		             								                        where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
		             								                        and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate > cla.occurdat
		             								                        and (nulldate is null or nulldate > cla.occurdat)
		             								                        and statusva not in ('2','3')),
		             								                    (   select min(ctid) from usinsug01.auto_peru
		             								                        where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
		             								                        and branch = cla.branch and policy = cla.policy and certif = cla.certif
		             								                        and statusva not in ('2','3')),
		             								                    (   select min(ctid) from usinsug01.auto_peru
		             								                        where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
		             								                        and branch = cla.branch and policy = cla.policy and certif = cla.certif
		             								                        and statusva in ('2','3')))),'')
		             					--when	2
		             					when	cla.branch in (1,2,3,4,7,8,9,10,11,12,13,14,16,17,18,19,28,30,38,39,55,57,58)
		             					then	cla.branch || '-' || cla.policy || '-' || cla.certif
		             					--when	1
		             					when	cla.branch in (5,21,22,23,24,25,27,31,32,33,34,35,36,37,40,41,42,59,68,71,75,77,91,99)
		             					then	cl0.client
		             					else 	'' end, '') DUNIDRISC,
		             coalesce((	select	cast(max(operdate) as varchar)
		             			from	usinsug01.claim_his
		             			where	claim = cla.claim
		             			and		oper_type = '16'
		             			and		operdate <= cl0.dat_lim),'') TDTREABE,
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
                             from	(	select 	cla.ctid cla_id,
                                                                     pol.ctid pol_id,
                                                                     case	pol.bussityp
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
                                                                                                                             and 	coalesce(coi.companyc,0) in (1,12)),100) end share_coa,
                                                                     coalesce((	select	sum(coalesce(
                                                                                                                             case	coalesce(
                                                                                                                                                     (	select	max(cpl.currency)
                                                                                                                                                             from	usinsug01.curren_pol cpl
                                                                                                                                                             where 	cpl.usercomp = cla.usercomp and cpl.company = cla.company and cpl.certype = pol.certype
                                                                                                                                                             and		cpl.branch = cla.branch and cpl.policy = cla.policy and	cpl.certif = cla.certif),
                                                                                                                                                     (	select	max(cpl.currency)
                                                                                                                                                             from	usinsug01.curren_pol cpl
                                                                                                                                                             where 	cpl.usercomp = cla.usercomp and cpl.company = cla.company and cpl.certype = pol.certype
                                                                                                                                                             and		cpl.branch = cla.branch and cpl.policy = cla.policy),0)
                                                                                                                                             when	clh.currency-- = cl0.moneda_cod
                                                                                                                                             then    coalesce(clh.amount,0)
                                                                                                                                             else	coalesce(clh.amount,0) *
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
                                                                                                                                                                             else	1 end end, 0))
                                                                                             from	usinsug01.claim_his clh
                                                                                             where	clh.claim = cla.claim
                                                                                             and		trim(clh.oper_type) in (select cast(operation as varchar(2)) from usinsug01.tab_cl_ope where reserve = 1 or ajustes = 1)
                                                                                             and		clh.operdate <= dat.dat_fin),0) monto_trans, --evita tomar transacciones actualizadas posteriormente (carga incremental)
                                                                     coalesce((	select	evi.scod_vt
                                                                                             from	usinsug01.equi_vt_inx evi 
                                                                                             where	evi.scod_inx = cla.client),
                                                                                     cla.client) client,
                                                                     coalesce((	select	sub_product
                                                                                             from	usinsug01.pol_subproduct
                                                                                             where	usercomp = cla.usercomp
                                                                                             and 	company = cla.company
                                                                                             and		certype = pol.certype
                                                                                             and		branch = cla.branch
                                                                                             and		policy = cla.policy
                                                                                             and		product = pol.product),0) sub_product,
                                                                     (	select 	max(greatest(
                                                                                                     case when clm.compdate <= dat.his_fin then clm.compdate else '0001-01-01'::date end,
                                                                                                     case when clh.compdate <= dat.his_fin then clh.compdate else '0001-01-01'::date end,
                                                                                                     case when clh.operdate <= dat.his_fin then clh.operdate else '0001-01-01'::date end,
                                                                                                     case when cla.compdate <= dat.his_fin then cla.compdate else '0001-01-01'::date end))
                                                                             from	usinsug01.claim_his clh
                                                                                             join	usinsug01.cl_m_cover clm
                                                                                             on		clm.usercomp = clh.usercomp
                                                                                             and		clm.company = clh.company
                                                                                             and		clm.claim = clh.claim
                                                                                             and		clm.movement = clh.transac
                                                                             where	clh.claim = cla.claim
                                                                             and		trim(clh.oper_type) in 
                                                                                             (	select	cast(operation as varchar(2))
                                                                                                     from	usinsug01.tab_cl_ope
                                                                                                     where	reserve = 1 or ajustes = 1 or pay_amount = 1)) xxxxdate, --se busca la mayor fecha a la nueva fecha de quiebre
                                                                     dat.his_fin dat_lim
                                                     from 	usinsug01.policy pol
                                                     join	(	select	cast('{p_fecha_inicio}' as date) his_ini, --rango inicial carga hist�rica (variable a elecci�n)
                                                                                cast('{p_fecha_fin}' as date) his_fin --rango final carga hist�rica (variable a elecci�n)
                                                                                             ) dat on 1 = 1
                                                     join	usinsug01.claim cla
                                                             on		cla.usercomp = pol.usercomp
                                                             and     cla.company = pol.company
                                                             and     cla.branch = pol.branch
                                                             and     cla.policy = pol.policy
                                                             and		(cla.compdate between dat.his_ini and dat.his_fin --se consideran siniestros con modificaciones entre los rangos (limitaci�n carga inicial)
                                                                             or	exists --a nivel de transacciones
                                                                                     (   select  1
                                                                                             from	usinsug01.claim_his clh
                                                                                             where	clh.claim = cla.claim
                                                                                             and		trim(clh.oper_type) in 
                                                                                                             (select cast(operation as varchar(2)) from usinsug01.tab_cl_ope where reserve = 1 or ajustes = 1 or pay_amount = 1)
                                                                                             and     (clh.operdate between dat.his_ini and dat.his_fin --se consideran siniestros con operaciones entre los rangos (limitaci�n carga inicial)
                                                                                                             or clh.compdate between dat.his_ini and dat.his_fin)) --se consideran siniestros con modificaciones entre los rangos (limitaci�n carga inicial)								
                                                                                     or exists --a nivel de coberturas
                                                                                     (   select  1
                                                                                             from	usinsug01.claim_his clh
                                                                                                             join	usinsug01.cl_m_cover clm
                                                                                                             on		clm.usercomp = clh.usercomp
                                                                                                             and		clm.company = clh.company
                                                                                                             and		clm.claim = clh.claim
                                                                                                             and		clm.movement = clh.transac
                                                                                                             and		clm.compdate between dat.his_ini and dat.his_fin --se consideran coberturas con modificaciones entre los rangos (limitaci�n carga inicial)
                                                                                             where	clh.claim = cla.claim
                                                                                             and		trim(clh.oper_type) in 
                                                                                                             (select cast(operation as varchar(2)) from usinsug01.tab_cl_ope where reserve = 1 or ajustes = 1 or pay_amount = 1)))
                                                             and		not exists
                                                                                     (   select  1
                                                                                             from    usinsug01.claim_his clh
                                                                                             where   clh.claim = cla.claim
                                                                                             and		trim(clh.oper_type) in 
                                                                                                             (select cast(operation as varchar(2)) from usinsug01.tab_cl_ope where reserve = 1 or ajustes = 1 or pay_amount = 1)
                                                                                             and     (clh.operdate > dat.his_fin --no existan operaciones posteriores al corte
                                                                                                             or clh.compdate > dat.his_fin)) --no existan modificaciones posteriores al corte
                                                             and		not exists
                                                                                     (   select  1
                                                                                             from	usinsug01.claim_his clh
                                                                                                             join	usinsug01.cl_m_cover clm
                                                                                                             on		clm.usercomp = clh.usercomp
                                                                                                             and		clm.company = clh.company
                                                                                                             and		clm.claim = clh.claim
                                                                                                             and		clm.movement = clh.transac
                                                                                                             and		clm.compdate > dat.his_fin --se consideran coberturas con modificaciones entre los rangos (limitaci�n carga inicial)
                                                                                             where	clh.claim = cla.claim
                                                                                             and		trim(clh.oper_type) in 
                                                                                                             (select cast(operation as varchar(2)) from usinsug01.tab_cl_ope where reserve = 1 or ajustes = 1 or pay_amount = 1))
                                                     where	pol.usercomp = 1
                                                     and		pol.company = 1
                                                     and		pol.certype = '2'
                                                     and		pol.branch = 23) cl0  --se excluyen transacciones sin montos
                             join	usinsug01.claim cla on cla.ctid = cl0.cla_id
                             join	usinsug01.policy pol on pol.ctid = cl0.pol_id
                             where	cl0.monto_trans <> 0
                            ) AS TMP    
                         '''
                            
    DF_INSUNIX_LPG_AMF = glue_context.read.format('jdbc').options(**connection).option("fetchsize",10000).option("dbtable",INSUNIX_LPG_AMF).load()

    INSUNIX_LPG_OTROS = f'''
                            (select	
                             'D' INDDETREC,
                             'SBSIN' TABLAIFRS17,
		             '' PK,
		             '' DTPREG, --excluido
		             '' TIOCPROC, --no disponible
		             cast(cl0.xxxxdate as varchar) TIOCFRM,
		             '' TIOCTO, --excluido
		             'PIG' KGIORIGM,
		             'LPG' KSCCOMPA,
		             coalesce(cast(cla.branch as varchar),'') KGCRAMO,
		             cla.branch || '-' || pol.product || '-0' KABPRODT, --solo AMF tiene subproducto <> 0 en tabla usinsug01.pol_subproduct
		             coalesce(cast(cla.policy as varchar),'') DNUMAPO,
		             coalesce(cast(cla.certif as varchar),'') DNMCERT,
		             coalesce(cast(cla.claim as varchar),'') DNUMSIN,
		             coalesce(cast(cla.occurdat as varchar),'') TOCURSIN,
		             '' DHSINIST, --excluido
		             coalesce(cast(cla.occurdat as varchar),'') TABERSIN,
		             '' TPARTSIN, --excluido
		             coalesce(cast(
		             	case	when	cla.staclaim in ('1','5','7')
		             			then	(	select 	max(case	when	cla.staclaim in ('1') and oper_type in ('3','17') then clh.operdate
		             											when	cla.staclaim in ('5') and oper_type in ('11') then clh.operdate
		             											when	cla.staclaim in ('7') and oper_type in ('21') then clh.operdate
		             											else	null end)
		             						from	usinsug01.claim_his clh
		             						where	clh.claim = cla.claim
		             						and		trim(oper_type) in ('3','17','11','21')
		             						and		operdate <= cl0.dat_lim)
		             			else 	null end as varchar),'') TFECHTEC,
		             coalesce(cast(
		             	case	when	cla.staclaim in ('1','5','7')
		             			then	(	select 	max(case	when	cla.staclaim in ('1') and oper_type in ('3','17') then clh.operdate
		             											when	cla.staclaim in ('5') and oper_type in ('52') then clh.operdate
		             											when	cla.staclaim in ('7') and oper_type in ('21') then clh.operdate
		             											else	null end)
		             						from	usinsug01.claim_his clh
		             						where	clh.claim = cla.claim
		             						and		trim(oper_type) in ('3','17','52','21')
		             						and		operdate <= cl0.dat_lim)
		             			else 	null end as varchar),'') TFECHADM,
		             coalesce(cast(
		             	(	select 	max(clh.operdate)
		             		from	usinsug01.claim_his clh
		             		where	clh.claim = cla.claim
		             		and		(	trim(oper_type) in 
		             					(select cast(operation as varchar(2)) from usinsug01.tab_cl_ope where reserve = 1 or ajustes = 1 or pay_amount in (1,3))
		             					or trim(oper_type) = '60')
		             		and		operdate <= cl0.dat_lim) as varchar),'') TESTADO,
		             coalesce(cast(cla.staclaim as varchar),'') KSCSITUA,
		             '' KSCMOTSI, --excluido
		             '' KSCTPSIN, --no disponible
		             coalesce(cla.branch || '-' || cla.causecod,'') KSCCAUSA,
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
		             cast(	coalesce((	select	max(cpl.currency)
		             					from	usinsug01.curren_pol cpl
		             					where 	cpl.usercomp = cla.usercomp and cpl.company = cla.company
		             					and		cpl.certype = pol.certype and cpl.branch = cla.branch
		             					and 	cpl.policy = cla.policy and cpl.certif = cla.certif),
		             				(	select	max(cpl.currency)
		             					from	usinsug01.curren_pol cpl
		             					where 	cpl.usercomp = cla.usercomp and cpl.company = cla.company
		             					and		cpl.certype = pol.certype and cpl.branch = cla.branch
		             					and 	cpl.policy = cla.policy),0) as varchar) KSCMOEDA,
		             '' VCAMBIO, --no disponible
		             '' VTXRESPN, --no disponible
		             cast(cl0.monto_trans as numeric(14,2)) VMTPROVI,
		             '' VMTPRVINI, --excluido
		             cast(cl0.monto_trans
		             	* (case when pol.bussityp = '2' then 100 else share_coa end/100)
                               * case	when	not exists (	select  1
		             									from    usinsug01.reinsuran rei
		             									where   rei.usercomp = cla.usercomp
		             									and     rei.company = cla.company
		             									and     rei.certype = pol.certype
		             									and     rei.branch = cla.branch
		             									and     rei.policy = cla.policy
		             									and     rei.certif = cla.certif)
            	             	then	1
            	             	else	(	coalesce((	select  sum(coalesce(rei.share,0))
		             									from    usinsug01.reinsuran rei
		             									where   rei.usercomp = cla.usercomp
		             									and     rei.company = cla.company
		             									and     rei.certype = pol.certype
		             									and     rei.branch = cla.branch
		             									and     rei.policy = cla.policy
		             									and     rei.certif = cla.certif
		             									and     rei.effecdate <= cla.occurdat
		             									and     (rei.nulldate is null or rei.nulldate > cla.occurdat)
		             									and     coalesce(rei.type,0) = 1),100)/100) end as numeric(14,2)) VMTPRVRS,
		             cast(cl0.monto_trans * (case when pol.bussityp = '2' then 100 else share_coa end/100) as numeric(14,2)) VMTCOSEG,
		             '' KSCNATUR, --excluido
		             '' TALTENAT, --excluido
		             '' DUSRREG, --excluido
		             cl0.client KEBENTID_TO,
		             cast(coalesce((	select  case	when	max(gco.addsuini) = '3'
		             								then	max(case when gco.addsuini = '3' then coalesce(cov.capital,0) else 0 end)
		             								else	sum(case when gco.addsuini = '1' then coalesce(cov.capital,0) else 0 end) end
		             				from    usinsug01.cover cov
		             				join	usinsug01.gen_cover gco on gco.ctid =
	                                         coalesce(
		             							(   select  max(ctid)
                                                     from	usinsug01.gen_cover
                                                     where	usercomp = cla.usercomp and company = cla.company and branch = cla.branch --�ndice regular (1)
                                                     and		product = pol.product and coalesce(sub_product,0) = 0 and currency = cov.currency --�ndice regular (2)
                                                     and     modulec = cov.modulec and cover = cov.cover --variaci�n 1 con modulec
                                                     and     effecdate <= cla.occurdat and (nulldate is null or nulldate > cla.occurdat) --variaci�n 2 vigencias
                                                     and     statregt <> '4'), --variaci�n 3 reg. v�lido
                                                 (   select  max(ctid)
                                                     from	usinsug01.gen_cover
                                                     where	usercomp = cla.usercomp and company = cla.company and branch = cla.branch --�ndice regular (1)
                                                     and		product = pol.product and coalesce(sub_product,0) = 0 and currency = cov.currency --�ndice regular (2)
                                                     and     coalesce(modulec,0) = 0 and cover = cov.cover --variaci�n 1 sin modulec
                                                     and     effecdate <= cla.occurdat and (nulldate is null or nulldate > cla.occurdat) --variaci�n 2 vigencias
                                                     and     statregt <> '4'), --variaci�n 3 reg. v�lido
		             							(	select  max(ctid)
                                                     from	usinsug01.gen_cover
                                                     where	usercomp = cla.usercomp and company = cla.company and branch = cla.branch --�ndice regular (1)
                                                     and		product = pol.product and coalesce(sub_product,0) = 0 and currency = cov.currency --�ndice regular (2)
                                                     and     modulec = cov.modulec and cover = cov.cover
                                                     and     effecdate <= cla.occurdat and (nulldate is null or nulldate > cla.occurdat)
                                                     and     statregt = '4'),
                                                 (	select  max(ctid)
                                                     from	usinsug01.gen_cover
                                                     where	usercomp = cla.usercomp and company = cla.company and branch = cla.branch --�ndice regular (1)
                                                     and		product = pol.product and coalesce(sub_product,0) = 0 and currency = cov.currency --�ndice regular (2)
                                                     and     coalesce(modulec,0) = 0 and cover = cov.cover
                                                     and     effecdate <= cla.occurdat and (nulldate is null or nulldate > cla.occurdat)
                                                     and     statregt = '4'), --no est� cortado
		             							(	select  max(ctid)
                                                     from	usinsug01.gen_cover
                                                     where	usercomp = cla.usercomp and company = cla.company and branch = cla.branch --�ndice regular (1)
                                                     and		product = pol.product and coalesce(sub_product,0) = 0 and currency = cov.currency --�ndice regular (2)
                                                     and     modulec = cov.modulec and cover = cov.cover
                                                     and     effecdate <= cla.occurdat and nulldate <= cla.occurdat
                                                     and     statregt <> '4'),
                                                 (   select  max(ctid)
                                                     from	usinsug01.gen_cover
                                                     where	usercomp = cla.usercomp and company = cla.company and branch = cla.branch --�ndice regular (1)
                                                     and		product = pol.product and coalesce(sub_product,0) = 0 and currency = cov.currency --�ndice regular (2)
                                                     and     coalesce(modulec,0) = 0 and cover = cov.cover
                                                     and     effecdate <= cla.occurdat and nulldate <= cla.occurdat
                                                     and     statregt <> '4'), -- no est� cortado, pero fue anulado antes del efecto del registro
		             							(	select  max(ctid)
                                                     from	usinsug01.gen_cover
                                                     where	usercomp = cla.usercomp and company = cla.company and branch = cla.branch --�ndice regular (1)
                                                     and		product = pol.product and coalesce(sub_product,0) = 0 and currency = cov.currency --�ndice regular (2)
                                                     and     modulec = cov.modulec and cover = cov.cover
                                                     and     effecdate <= cla.occurdat and nulldate <= cla.occurdat
                                                     and     statregt = '4'),
                                                 (   select  max(ctid)
                                                     from	usinsug01.gen_cover
                                                     where	usercomp = cla.usercomp and company = cla.company and branch = cla.branch --�ndice regular (1)
                                                     and		product = pol.product and coalesce(sub_product,0) = 0 and currency = cov.currency --�ndice regular (2)
                                                     and     coalesce(modulec,0) = 0 and cover = cov.cover
                                                     and     effecdate <= cla.occurdat and nulldate <= cla.occurdat
                                                     and     statregt = '4'), --no est� cortado y aparte fue anulado antes del efecto del registro
		             							(	select  max(ctid)
                                                     from	usinsug01.gen_cover
                                                     where	usercomp = cla.usercomp and company = cla.company and branch = cla.branch --�ndice regular (1)
                                                     and		product = pol.product and coalesce(sub_product,0) = 0 and currency = cov.currency --�ndice regular (2)
                                                     and     modulec = cov.modulec and cover = cov.cover
                                                     and     effecdate > cla.occurdat
                                                     and     statregt <> '4'),
                                                 (   select  max(ctid)
                                                     from	usinsug01.gen_cover
                                                     where	usercomp = cla.usercomp and company = cla.company and branch = cla.branch --�ndice regular (1)
                                                     and		product = pol.product and coalesce(sub_product,0) = 0 and currency = cov.currency --�ndice regular (2)
                                                     and     coalesce(modulec,0) = 0 and cover = cov.cover
                                                     and     effecdate <= cla.occurdat and nulldate <= cla.occurdat
                                                     and     statregt <> '4'), -- no est� cortado, pero tampoco al efecto de la tabla de datos particular
		             							(	select  max(ctid)
                                                     from	usinsug01.gen_cover
                                                     where	usercomp = cla.usercomp and company = cla.company and branch = cla.branch --�ndice regular (1)
                                                     and		product = pol.product and coalesce(sub_product,0) = 0 and currency = cov.currency --�ndice regular (2)
                                                     and     modulec = cov.modulec and cover = cov.cover
                                                     and     effecdate <= cla.occurdat and nulldate <= cla.occurdat
                                                     and     statregt = '4'),
                                                 (   select  max(ctid)
                                                     from	usinsug01.gen_cover
                                                     where	usercomp = cla.usercomp and company = cla.company and branch = cla.branch --�ndice regular (1)
                                                     and		product = pol.product and coalesce(sub_product,0) = 0 and currency = cov.currency --�ndice regular (2)
                                                     and     coalesce(modulec,0) = 0 and cover = cov.cover
                                                     and     effecdate > cla.occurdat
                                                     and     statregt = '4')) --est� cortado y no al efecto de la tabla de datos particular adem�s de estar cortado
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
		             							            coalesce(
		             						                    (   select max(ctid) from usinsug01.accident
		             						                        where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
		             						                        and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate <= cla.occurdat
		             						                        and (nulldate is null or nulldate > cla.occurdat)
		             						                        and statusva not in ('2','3')),
		             						                    (   select max(ctid) from usinsug01.accident
		             						                        where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
		             						                        and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate <= cla.occurdat
		             						                        and (nulldate is null or nulldate >= cla.occurdat)
		             						                        and statusva not in ('2','3')),
		             						                    (   select max(ctid) from usinsug01.accident
		             						                        where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
		             						                        and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate <= cla.occurdat
		             						                        and (nulldate is null or nulldate < cla.occurdat)
		             						                        and statusva not in ('2','3')),
		             						                    (   select min(ctid) from usinsug01.accident
		             						                        where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
		             						                        and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate > cla.occurdat
		             						                        and (nulldate is null or nulldate > cla.occurdat)
		             						                        and statusva not in ('2','3')),
		             						                    (   select min(ctid) from usinsug01.accident
		             						                        where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
		             						                        and branch = cla.branch and policy = cla.policy and certif = cla.certif
		             						                        and statusva not in ('2','3')),
		             						                    (   select min(ctid) from usinsug01.accident
		             						                        where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
		             						                        and branch = cla.branch and policy = cla.policy and certif = cla.certif
		             						                        and statusva in ('2','3'))))
		             					when 	'auto_peru'
		             					then 	(	select	statusva
		             								from	usinsug01.auto_peru
		             								where	ctid = 
		             							            coalesce(
		             						                    (   select max(ctid) from usinsug01.auto_peru
		             						                        where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
		             						                        and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate <= cla.occurdat
		             						                        and (nulldate is null or nulldate > cla.occurdat)
		             						                        and statusva not in ('2','3')),
		             						                    (   select max(ctid) from usinsug01.auto_peru
		             						                        where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
		             						                        and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate <= cla.occurdat
		             						                        and (nulldate is null or nulldate >= cla.occurdat)
		             						                        and statusva not in ('2','3')),
		             						                    (   select max(ctid) from usinsug01.auto_peru
		             						                        where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
		             						                        and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate <= cla.occurdat
		             						                        and (nulldate is null or nulldate < cla.occurdat)
		             						                        and statusva not in ('2','3')),
		             						                    (   select min(ctid) from usinsug01.auto_peru
		             						                        where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
		             						                        and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate > cla.occurdat
		             						                        and (nulldate is null or nulldate > cla.occurdat)
		             						                        and statusva not in ('2','3')),
		             						                    (   select min(ctid) from usinsug01.auto_peru
		             						                        where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
		             						                        and branch = cla.branch and policy = cla.policy and certif = cla.certif
		             						                        and statusva not in ('2','3')),
		             						                    (   select min(ctid) from usinsug01.auto_peru
		             						                        where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
		             						                        and branch = cla.branch and policy = cla.policy and certif = cla.certif
		             						                        and statusva in ('2','3'))))
		             					when 	'civil'
		             					then 	(	select	statusva
		             								from	usinsug01.civil
		             								where	ctid = 
		             							            coalesce(
		             						                    (   select max(ctid) from usinsug01.civil
		             						                        where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
		             						                        and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate <= cla.occurdat
		             						                        and (nulldate is null or nulldate > cla.occurdat)
		             						                        and statusva not in ('2','3')),
		             						                    (   select max(ctid) from usinsug01.civil
		             						                        where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
		             						                        and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate <= cla.occurdat
		             						                        and (nulldate is null or nulldate >= cla.occurdat)
		             						                        and statusva not in ('2','3')),
		             						                    (   select max(ctid) from usinsug01.civil
		             						                        where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
		             						                        and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate <= cla.occurdat
		             						                        and (nulldate is null or nulldate < cla.occurdat)
		             						                        and statusva not in ('2','3')),
		             						                    (   select min(ctid) from usinsug01.civil
		             						                        where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
		             						                        and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate > cla.occurdat
		             						                        and (nulldate is null or nulldate > cla.occurdat)
		             						                        and statusva not in ('2','3')),
		             						                    (   select min(ctid) from usinsug01.civil
		             						                        where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
		             						                        and branch = cla.branch and policy = cla.policy and certif = cla.certif
		             						                        and statusva not in ('2','3')),
		             						                    (   select min(ctid) from usinsug01.civil
		             						                        where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
		             						                        and branch = cla.branch and policy = cla.policy and certif = cla.certif
		             						                        and statusva in ('2','3'))))
		             					when 	'credit'
		             					then 	(	select	statusva
		             								from	usinsug01.credit
		             								where	ctid = 
		             							            coalesce(
		             						                    (   select max(ctid) from usinsug01.credit
		             						                        where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
		             						                        and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate <= cla.occurdat
		             						                        and (nulldate is null or nulldate > cla.occurdat)
		             						                        and statusva not in ('2','3')),
		             						                    (   select max(ctid) from usinsug01.credit
		             						                        where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
		             						                        and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate <= cla.occurdat
		             						                        and (nulldate is null or nulldate >= cla.occurdat)
		             						                        and statusva not in ('2','3')),
		             						                    (   select max(ctid) from usinsug01.credit
		             						                        where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
		             						                        and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate <= cla.occurdat
		             						                        and (nulldate is null or nulldate < cla.occurdat)
		             						                        and statusva not in ('2','3')),
		             						                    (   select min(ctid) from usinsug01.credit
		             						                        where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
		             						                        and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate > cla.occurdat
		             						                        and (nulldate is null or nulldate > cla.occurdat)
		             						                        and statusva not in ('2','3')),
		             						                    (   select min(ctid) from usinsug01.credit
		             						                        where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
		             						                        and branch = cla.branch and policy = cla.policy and certif = cla.certif
		             						                        and statusva not in ('2','3')),
		             						                    (   select min(ctid) from usinsug01.credit
		             						                        where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
		             						                        and branch = cla.branch and policy = cla.policy and certif = cla.certif
		             						                        and statusva in ('2','3'))))
		             					when 	'deshones'
		             					then 	(	select	statusva
		             								from	usinsug01.deshones
		             								where	ctid = 
		             							            coalesce(
		             						                    (   select max(ctid) from usinsug01.deshones
		             						                        where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
		             						                        and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate <= cla.occurdat
		             						                        and (nulldate is null or nulldate > cla.occurdat)
		             						                        and statusva not in ('2','3')),
		             						                    (   select max(ctid) from usinsug01.deshones
		             						                        where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
		             						                        and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate <= cla.occurdat
		             						                        and (nulldate is null or nulldate >= cla.occurdat)
		             						                        and statusva not in ('2','3')),
		             						                    (   select max(ctid) from usinsug01.deshones
		             						                        where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
		             						                        and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate <= cla.occurdat
		             						                        and (nulldate is null or nulldate < cla.occurdat)
		             						                        and statusva not in ('2','3')),
		             						                    (   select min(ctid) from usinsug01.deshones
		             						                        where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
		             						                        and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate > cla.occurdat
		             						                        and (nulldate is null or nulldate > cla.occurdat)
		             						                        and statusva not in ('2','3')),
		             						                    (   select min(ctid) from usinsug01.deshones
		             						                        where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
		             						                        and branch = cla.branch and policy = cla.policy and certif = cla.certif
		             						                        and statusva not in ('2','3')),
		             						                    (   select min(ctid) from usinsug01.deshones
		             						                        where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
		             						                        and branch = cla.branch and policy = cla.policy and certif = cla.certif
		             						                        and statusva in ('2','3'))))
		             					when 	'eqele_peru'
		             					then 	(	select	statusva
		             								from	usinsug01.eqele_peru
		             								where	ctid = 
		             							            coalesce(
		             						                    (   select max(ctid) from usinsug01.eqele_peru
		             						                        where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
		             						                        and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate <= cla.occurdat
		             						                        and (nulldate is null or nulldate > cla.occurdat)
		             						                        and statusva not in ('2','3')),
		             						                    (   select max(ctid) from usinsug01.eqele_peru
		             						                        where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
		             						                        and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate <= cla.occurdat
		             						                        and (nulldate is null or nulldate >= cla.occurdat)
		             						                        and statusva not in ('2','3')),
		             						                    (   select max(ctid) from usinsug01.eqele_peru
		             						                        where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
		             						                        and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate <= cla.occurdat
		             						                        and (nulldate is null or nulldate < cla.occurdat)
		             						                        and statusva not in ('2','3')),
		             						                    (   select min(ctid) from usinsug01.eqele_peru
		             						                        where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
		             						                        and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate > cla.occurdat
		             						                        and (nulldate is null or nulldate > cla.occurdat)
		             						                        and statusva not in ('2','3')),
		             						                    (   select min(ctid) from usinsug01.eqele_peru
		             						                        where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
		             						                        and branch = cla.branch and policy = cla.policy and certif = cla.certif
		             						                        and statusva not in ('2','3')),
		             						                    (   select min(ctid) from usinsug01.eqele_peru
		             						                        where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
		             						                        and branch = cla.branch and policy = cla.policy and certif = cla.certif
		             						                        and statusva in ('2','3'))))
		             					when 	'fire_lc'
		             					then 	(	select	statusva
		             								from	usinsug01.fire_lc
		             								where	ctid = 
		             							            coalesce(
		             						                    (   select max(ctid) from usinsug01.fire_lc
		             						                        where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
		             						                        and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate <= cla.occurdat
		             						                        and (nulldate is null or nulldate > cla.occurdat)
		             						                        and statusva not in ('2','3')),
		             						                    (   select max(ctid) from usinsug01.fire_lc
		             						                        where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
		             						                        and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate <= cla.occurdat
		             						                        and (nulldate is null or nulldate >= cla.occurdat)
		             						                        and statusva not in ('2','3')),
		             						                    (   select max(ctid) from usinsug01.fire_lc
		             						                        where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
		             						                        and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate <= cla.occurdat
		             						                        and (nulldate is null or nulldate < cla.occurdat)
		             						                        and statusva not in ('2','3')),
		             						                    (   select min(ctid) from usinsug01.fire_lc
		             						                        where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
		             						                        and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate > cla.occurdat
		             						                        and (nulldate is null or nulldate > cla.occurdat)
		             						                        and statusva not in ('2','3')),
		             						                    (   select min(ctid) from usinsug01.fire_lc
		             						                        where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
		             						                        and branch = cla.branch and policy = cla.policy and certif = cla.certif
		             						                        and statusva not in ('2','3')),
		             						                    (   select min(ctid) from usinsug01.fire_lc
		             						                        where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
		             						                        and branch = cla.branch and policy = cla.policy and certif = cla.certif
		             						                        and statusva in ('2','3'))))
		             					when 	'fire_peru'
		             					then 	(	select	statusva
		             								from	usinsug01.fire_peru
		             								where	ctid = 
		             							            coalesce(
		             						                    (   select max(ctid) from usinsug01.fire_peru
		             						                        where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
		             						                        and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate <= cla.occurdat
		             						                        and (nulldate is null or nulldate > cla.occurdat)
		             						                        and statusva not in ('2','3')),
		             						                    (   select max(ctid) from usinsug01.fire_peru
		             						                        where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
		             						                        and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate <= cla.occurdat
		             						                        and (nulldate is null or nulldate >= cla.occurdat)
		             						                        and statusva not in ('2','3')),
		             						                    (   select max(ctid) from usinsug01.fire_peru
		             						                        where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
		             						                        and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate <= cla.occurdat
		             						                        and (nulldate is null or nulldate < cla.occurdat)
		             						                        and statusva not in ('2','3')),
		             						                    (   select min(ctid) from usinsug01.fire_peru
		             						                        where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
		             						                        and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate > cla.occurdat
		             						                        and (nulldate is null or nulldate > cla.occurdat)
		             						                        and statusva not in ('2','3')),
		             						                    (   select min(ctid) from usinsug01.fire_peru
		             						                        where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
		             						                        and branch = cla.branch and policy = cla.policy and certif = cla.certif
		             						                        and statusva not in ('2','3')),
		             						                    (   select min(ctid) from usinsug01.fire_peru
		             						                        where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
		             						                        and branch = cla.branch and policy = cla.policy and certif = cla.certif
		             						                        and statusva in ('2','3'))))
		             					when 	'health'
		             					then 	(	select	statusva
		             								from	usinsug01.health
		             								where	ctid = 
		             							            coalesce(
		             						                    (   select max(ctid) from usinsug01.health
		             						                        where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
		             						                        and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate <= cla.occurdat
		             						                        and (nulldate is null or nulldate > cla.occurdat)
		             						                        and statusva not in ('2','3')),
		             						                    (   select max(ctid) from usinsug01.health
		             						                        where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
		             						                        and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate <= cla.occurdat
		             						                        and (nulldate is null or nulldate >= cla.occurdat)
		             						                        and statusva not in ('2','3')),
		             						                    (   select max(ctid) from usinsug01.health
		             						                        where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
		             						                        and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate <= cla.occurdat
		             						                        and (nulldate is null or nulldate < cla.occurdat)
		             						                        and statusva not in ('2','3')),
		             						                    (   select min(ctid) from usinsug01.health
		             						                        where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
		             						                        and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate > cla.occurdat
		             						                        and (nulldate is null or nulldate > cla.occurdat)
		             						                        and statusva not in ('2','3')),
		             						                    (   select min(ctid) from usinsug01.health
		             						                        where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
		             						                        and branch = cla.branch and policy = cla.policy and certif = cla.certif
		             						                        and statusva not in ('2','3')),
		             						                    (   select min(ctid) from usinsug01.health
		             						                        where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
		             						                        and branch = cla.branch and policy = cla.policy and certif = cla.certif
		             						                        and statusva in ('2','3'))))
		             					when 	'machine'
		             					then 	(	select	statusva
		             								from	usinsug01.machine
		             								where	ctid = 
		             							            coalesce(
		             						                    (   select max(ctid) from usinsug01.machine
		             						                        where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
		             						                        and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate <= cla.occurdat
		             						                        and (nulldate is null or nulldate > cla.occurdat)
		             						                        and statusva not in ('2','3')),
		             						                    (   select max(ctid) from usinsug01.machine
		             						                        where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
		             						                        and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate <= cla.occurdat
		             						                        and (nulldate is null or nulldate >= cla.occurdat)
		             						                        and statusva not in ('2','3')),
		             						                    (   select max(ctid) from usinsug01.machine
		             						                        where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
		             						                        and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate <= cla.occurdat
		             						                        and (nulldate is null or nulldate < cla.occurdat)
		             						                        and statusva not in ('2','3')),
		             						                    (   select min(ctid) from usinsug01.machine
		             						                        where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
		             						                        and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate > cla.occurdat
		             						                        and (nulldate is null or nulldate > cla.occurdat)
		             						                        and statusva not in ('2','3')),
		             						                    (   select min(ctid) from usinsug01.machine
		             						                        where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
		             						                        and branch = cla.branch and policy = cla.policy and certif = cla.certif
		             						                        and statusva not in ('2','3')),
		             						                    (   select min(ctid) from usinsug01.machine
		             						                        where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
		             						                        and branch = cla.branch and policy = cla.policy and certif = cla.certif
		             						                        and statusva in ('2','3'))))
		             					when 	'machine_lc'
		             					then 	(	select	statusva
		             								from	usinsug01.machine_lc
		             								where	ctid = 
		             							            coalesce(
		             						                    (   select max(ctid) from usinsug01.machine_lc
		             						                        where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
		             						                        and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate <= cla.occurdat
		             						                        and (nulldate is null or nulldate > cla.occurdat)
		             						                        and statusva not in ('2','3')),
		             						                    (   select max(ctid) from usinsug01.machine_lc
		             						                        where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
		             						                        and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate <= cla.occurdat
		             						                        and (nulldate is null or nulldate >= cla.occurdat)
		             						                        and statusva not in ('2','3')),
		             						                    (   select max(ctid) from usinsug01.machine_lc
		             						                        where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
		             						                        and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate <= cla.occurdat
		             						                        and (nulldate is null or nulldate < cla.occurdat)
		             						                        and statusva not in ('2','3')),
		             						                    (   select min(ctid) from usinsug01.machine_lc
		             						                        where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
		             						                        and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate > cla.occurdat
		             						                        and (nulldate is null or nulldate > cla.occurdat)
		             						                        and statusva not in ('2','3')),
		             						                    (   select min(ctid) from usinsug01.machine_lc
		             						                        where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
		             						                        and branch = cla.branch and policy = cla.policy and certif = cla.certif
		             						                        and statusva not in ('2','3')),
		             						                    (   select min(ctid) from usinsug01.machine_lc
		             						                        where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
		             						                        and branch = cla.branch and policy = cla.policy and certif = cla.certif
		             						                        and statusva in ('2','3'))))
		             					when 	'risk_3d'
		             					then 	(	select	statusva
		             								from	usinsug01.risk_3d
		             								where	ctid = 
		             							            coalesce(
		             						                    (   select max(ctid) from usinsug01.risk_3d
		             						                        where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
		             						                        and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate <= cla.occurdat
		             						                        and (nulldate is null or nulldate > cla.occurdat)
		             						                        and statusva not in ('2','3')),
		             						                    (   select max(ctid) from usinsug01.risk_3d
		             						                        where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
		             						                        and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate <= cla.occurdat
		             						                        and (nulldate is null or nulldate >= cla.occurdat)
		             						                        and statusva not in ('2','3')),
		             						                    (   select max(ctid) from usinsug01.risk_3d
		             						                        where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
		             						                        and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate <= cla.occurdat
		             						                        and (nulldate is null or nulldate < cla.occurdat)
		             						                        and statusva not in ('2','3')),
		             						                    (   select min(ctid) from usinsug01.risk_3d
		             						                        where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
		             						                        and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate > cla.occurdat
		             						                        and (nulldate is null or nulldate > cla.occurdat)
		             						                        and statusva not in ('2','3')),
		             						                    (   select min(ctid) from usinsug01.risk_3d
		             						                        where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
		             						                        and branch = cla.branch and policy = cla.policy and certif = cla.certif
		             						                        and statusva not in ('2','3')),
		             						                    (   select min(ctid) from usinsug01.risk_3d
		             						                        where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
		             						                        and branch = cla.branch and policy = cla.policy and certif = cla.certif
		             						                        and statusva in ('2','3'))))
		             					when 	'ship'
		             					then 	(	select	statusva
		             								from	usinsug01.ship
		             								where	ctid = 
		             							            coalesce(
		             						                    (   select max(ctid) from usinsug01.ship
		             						                        where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
		             						                        and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate <= cla.occurdat
		             						                        and (nulldate is null or nulldate > cla.occurdat)
		             						                        and statusva not in ('2','3')),
		             						                    (   select max(ctid) from usinsug01.ship
		             						                        where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
		             						                        and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate <= cla.occurdat
		             						                        and (nulldate is null or nulldate >= cla.occurdat)
		             						                        and statusva not in ('2','3')),
		             						                    (   select max(ctid) from usinsug01.ship
		             						                        where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
		             						                        and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate <= cla.occurdat
		             						                        and (nulldate is null or nulldate < cla.occurdat)
		             						                        and statusva not in ('2','3')),
		             						                    (   select min(ctid) from usinsug01.ship
		             						                        where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
		             						                        and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate > cla.occurdat
		             						                        and (nulldate is null or nulldate > cla.occurdat)
		             						                        and statusva not in ('2','3')),
		             						                    (   select min(ctid) from usinsug01.ship
		             						                        where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
		             						                        and branch = cla.branch and policy = cla.policy and certif = cla.certif
		             						                        and statusva not in ('2','3')),
		             						                    (   select min(ctid) from usinsug01.ship
		             						                        where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
		             						                        and branch = cla.branch and policy = cla.policy and certif = cla.certif
		             						                        and statusva in ('2','3'))))
		             					when 	'theft'
		             					then 	(	select	statusva
		             								from	usinsug01.theft
		             								where	ctid = 
		             							            coalesce(
		             						                    (   select max(ctid) from usinsug01.theft
		             						                        where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
		             						                        and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate <= cla.occurdat
		             						                        and (nulldate is null or nulldate > cla.occurdat)
		             						                        and statusva not in ('2','3')),
		             						                    (   select max(ctid) from usinsug01.theft
		             						                        where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
		             						                        and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate <= cla.occurdat
		             						                        and (nulldate is null or nulldate >= cla.occurdat)
		             						                        and statusva not in ('2','3')),
		             						                    (   select max(ctid) from usinsug01.theft
		             						                        where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
		             						                        and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate <= cla.occurdat
		             						                        and (nulldate is null or nulldate < cla.occurdat)
		             						                        and statusva not in ('2','3')),
		             						                    (   select min(ctid) from usinsug01.theft
		             						                        where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
		             						                        and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate > cla.occurdat
		             						                        and (nulldate is null or nulldate > cla.occurdat)
		             						                        and statusva not in ('2','3')),
		             						                    (   select min(ctid) from usinsug01.theft
		             						                        where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
		             						                        and branch = cla.branch and policy = cla.policy and certif = cla.certif
		             						                        and statusva not in ('2','3')),
		             						                    (   select min(ctid) from usinsug01.theft
		             						                        where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
		             						                        and branch = cla.branch and policy = cla.policy and certif = cla.certif
		             						                        and statusva in ('2','3'))))
		             					when 	'transport'
		             					then 	(	select	statusva
		             								from	usinsug01.transport
		             								where	ctid = 
		             							            coalesce(
		             						                    (   select max(ctid) from usinsug01.transport
		             						                        where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
		             						                        and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate <= cla.occurdat
		             						                        and (nulldate is null or nulldate > cla.occurdat)
		             						                        and statusva not in ('2','3')),
		             						                    (   select max(ctid) from usinsug01.transport
		             						                        where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
		             						                        and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate <= cla.occurdat
		             						                        and (nulldate is null or nulldate >= cla.occurdat)
		             						                        and statusva not in ('2','3')),
		             						                    (   select max(ctid) from usinsug01.transport
		             						                        where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
		             						                        and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate <= cla.occurdat
		             						                        and (nulldate is null or nulldate < cla.occurdat)
		             						                        and statusva not in ('2','3')),
		             						                    (   select min(ctid) from usinsug01.transport
		             						                        where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
		             						                        and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate > cla.occurdat
		             						                        and (nulldate is null or nulldate > cla.occurdat)
		             						                        and statusva not in ('2','3')),
		             						                    (   select min(ctid) from usinsug01.transport
		             						                        where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
		             						                        and branch = cla.branch and policy = cla.policy and certif = cla.certif
		             						                        and statusva not in ('2','3')),
		             						                    (   select min(ctid) from usinsug01.transport
		             						                        where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
		             						                        and branch = cla.branch and policy = cla.policy and certif = cla.certif
		             						                        and statusva in ('2','3'))))
		             					when 	'trec'
		             					then 	(	select	statusva
		             								from	usinsug01.trec
		             								where	ctid = 
		             							            coalesce(
		             						                    (   select max(ctid) from usinsug01.trec
		             						                        where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
		             						                        and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate <= cla.occurdat
		             						                        and (nulldate is null or nulldate > cla.occurdat)
		             						                        and statusva not in ('2','3')),
		             						                    (   select max(ctid) from usinsug01.trec
		             						                        where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
		             						                        and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate <= cla.occurdat
		             						                        and (nulldate is null or nulldate >= cla.occurdat)
		             						                        and statusva not in ('2','3')),
		             						                    (   select max(ctid) from usinsug01.trec
		             						                        where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
		             						                        and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate <= cla.occurdat
		             						                        and (nulldate is null or nulldate < cla.occurdat)
		             						                        and statusva not in ('2','3')),
		             						                    (   select min(ctid) from usinsug01.trec
		             						                        where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
		             						                        and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate > cla.occurdat
		             						                        and (nulldate is null or nulldate > cla.occurdat)
		             						                        and statusva not in ('2','3')),
		             						                    (   select min(ctid) from usinsug01.trec
		             						                        where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
		             						                        and branch = cla.branch and policy = cla.policy and certif = cla.certif
		             						                        and statusva not in ('2','3')),
		             						                    (   select min(ctid) from usinsug01.trec
		             						                        where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
		             						                        and branch = cla.branch and policy = cla.policy and certif = cla.certif
		             						                        and statusva in ('2','3'))))
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
		             					then	coalesce((	select	coalesce(	case	when	trim(coalesce(tnb.regist,'')) <> ''
		             																	then	trim(tnb.regist)
		             																	else 	case	when	trim(coalesce(tnb.chassis,'')) <> ''
		             																					then	trim(tnb.chassis)
		             																					else	''
		             																					end
		             																	end,'')
		             										from	usinsug01.auto_peru tnb
		             										where	ctid = 
		             												coalesce(
		             								                    (   select max(ctid) from usinsug01.auto_peru
		             								                        where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
		             								                        and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate <= cla.occurdat
		             								                        and (nulldate is null or nulldate > cla.occurdat)
		             								                        and statusva not in ('2','3')),
		             								                    (   select max(ctid) from usinsug01.auto_peru
		             								                        where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
		             								                        and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate <= cla.occurdat
		             								                        and (nulldate is null or nulldate >= cla.occurdat)
		             								                        and statusva not in ('2','3')),
		             								                    (   select max(ctid) from usinsug01.auto_peru
		             								                        where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
		             								                        and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate <= cla.occurdat
		             								                        and (nulldate is null or nulldate < cla.occurdat)
		             								                        and statusva not in ('2','3')),
		             								                    (   select min(ctid) from usinsug01.auto_peru
		             								                        where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
		             								                        and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate > cla.occurdat
		             								                        and (nulldate is null or nulldate > cla.occurdat)
		             								                        and statusva not in ('2','3')),
		             								                    (   select min(ctid) from usinsug01.auto_peru
		             								                        where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
		             								                        and branch = cla.branch and policy = cla.policy and certif = cla.certif
		             								                        and statusva not in ('2','3')),
		             								                    (   select min(ctid) from usinsug01.auto_peru
		             								                        where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
		             								                        and branch = cla.branch and policy = cla.policy and certif = cla.certif
		             								                        and statusva in ('2','3')))),'')
		             					--when	2
		             					when	cla.branch in (1,2,3,4,7,8,9,10,11,12,13,14,16,17,18,19,28,30,38,39,55,57,58)
		             					then	cla.branch || '-' || cla.policy || '-' || cla.certif
		             					--when	1
		             					when	cla.branch in (5,21,22,23,24,25,27,31,32,33,34,35,36,37,40,41,42,59,68,71,75,77,91,99)
		             					then	cl0.client
		             					else 	'' end, '') DUNIDRISC,
		             coalesce((	select	cast(max(operdate) as varchar)
		             			from	usinsug01.claim_his
		             			where	claim = cla.claim
		             			and		oper_type = '16'
		             			and		operdate <= cl0.dat_lim),'') TDTREABE,
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
                             from	(	select 	cla.ctid cla_id,
                                                                     pol.ctid pol_id,
                                                                     case	pol.bussityp
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
                                                                                                                             and 	coalesce(coi.companyc,0) in (1,12)),100) end share_coa,
                                                                     coalesce((	select	sum(coalesce(
                                                                                                                             case	coalesce(
                                                                                                                                                     (	select	max(cpl.currency)
                                                                                                                                                             from	usinsug01.curren_pol cpl
                                                                                                                                                             where 	cpl.usercomp = cla.usercomp and cpl.company = cla.company and cpl.certype = pol.certype
                                                                                                                                                             and		cpl.branch = cla.branch and cpl.policy = cla.policy and	cpl.certif = cla.certif),
                                                                                                                                                     (	select	max(cpl.currency)
                                                                                                                                                             from	usinsug01.curren_pol cpl
                                                                                                                                                             where 	cpl.usercomp = cla.usercomp and cpl.company = cla.company and cpl.certype = pol.certype
                                                                                                                                                             and		cpl.branch = cla.branch and cpl.policy = cla.policy),0)
                                                                                                                                             when	clh.currency-- = cl0.moneda_cod
                                                                                                                                             then    coalesce(clh.amount,0)
                                                                                                                                             else	coalesce(clh.amount,0) *
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
                                                                                                                                                                             else	1 end end, 0))
                                                                                             from	usinsug01.claim_his clh
                                                                                             where	clh.claim = cla.claim
                                                                                             and		trim(clh.oper_type) in (select cast(operation as varchar(2)) from usinsug01.tab_cl_ope where reserve = 1 or ajustes = 1)
                                                                                             and		clh.operdate <= dat.his_fin),0) monto_trans, --se considera la reserva ajustada a la fecha de quiebre
                                                                     coalesce((	select	evi.scod_vt
                                                                                             from	usinsug01.equi_vt_inx evi 
                                                                                             where	evi.scod_inx = cla.client),
                                                                                     cla.client) client,
                                                                     (	select 	max(greatest(
                                                                                                     case when clm.compdate <= dat.his_fin then clm.compdate else '0001-01-01'::date end,
                                                                                                     case when clh.compdate <= dat.his_fin then clh.compdate else '0001-01-01'::date end,
                                                                                                     case when clh.operdate <= dat.his_fin then clh.operdate else '0001-01-01'::date end,
                                                                                                     case when cla.compdate <= dat.his_fin then cla.compdate else '0001-01-01'::date end))
                                                                             from	usinsug01.claim_his clh
                                                                                             join	usinsug01.cl_m_cover clm
                                                                                             on		clm.usercomp = clh.usercomp
                                                                                             and		clm.company = clh.company
                                                                                             and		clm.claim = clh.claim
                                                                                             and		clm.movement = clh.transac
                                                                             where	clh.claim = cla.claim
                                                                             and		trim(clh.oper_type) in 
                                                                                             (	select	cast(operation as varchar(2))
                                                                                                     from	usinsug01.tab_cl_ope
                                                                                                     where	reserve = 1 or ajustes = 1 or pay_amount = 1)) xxxxdate, --se busca la mayor fecha a la nueva fecha de quiebre
                                                                     dat.his_fin dat_lim
                                                     from 	usinsug01.policy pol
                                                     join	(	select	cast('{p_fecha_inicio}' as date) his_ini, --rango inicial carga hist�rica (variable a elecci�n)
                                                                                cast('{p_fecha_fin}' as date) his_fin --rango final carga hist�rica (variable a elecci�n)
                                                                                             ) dat on 1 = 1
                                                     join	usinsug01.claim cla
                                                             on		cla.usercomp = pol.usercomp
                                                             and     cla.company = pol.company
                                                             and     cla.branch = pol.branch
                                                             and     cla.policy = pol.policy
                                                             and     cla.staclaim <> '6'
                                                             and		(cla.compdate between dat.his_ini and dat.his_fin --se consideran siniestros con modificaciones entre los rangos (limitaci�n carga inicial)
                                                                             or	exists --a nivel de transacciones
                                                                                     (   select  1
                                                                                             from	usinsug01.claim_his clh
                                                                                             where	clh.claim = cla.claim
                                                                                             and		trim(clh.oper_type) in 
                                                                                                             (select cast(operation as varchar(2)) from usinsug01.tab_cl_ope where reserve = 1 or ajustes = 1 or pay_amount = 1)
                                                                                             and     (clh.operdate between dat.his_ini and dat.his_fin --se consideran siniestros con operaciones entre los rangos (limitaci�n carga inicial)
                                                                                                             or clh.compdate between dat.his_ini and dat.his_fin)) --se consideran siniestros con modificaciones entre los rangos (limitaci�n carga inicial)								
                                                                                     or exists --a nivel de coberturas
                                                                                     (   select  1
                                                                                             from	usinsug01.claim_his clh
                                                                                                             join	usinsug01.cl_m_cover clm
                                                                                                             on		clm.usercomp = clh.usercomp
                                                                                                             and		clm.company = clh.company
                                                                                                             and		clm.claim = clh.claim
                                                                                                             and		clm.movement = clh.transac
                                                                                                             and		clm.compdate between dat.his_ini and dat.his_fin --se consideran coberturas con modificaciones entre los rangos (limitaci�n carga inicial)
                                                                                             where	clh.claim = cla.claim
                                                                                             and		trim(clh.oper_type) in 
                                                                                                             (select cast(operation as varchar(2)) from usinsug01.tab_cl_ope where reserve = 1 or ajustes = 1 or pay_amount = 1)))
                                                             and		not exists
                                                                                     (   select  1
                                                                                             from    usinsug01.claim_his clh
                                                                                             where   clh.claim = cla.claim
                                                                                             and		trim(clh.oper_type) in 
                                                                                                             (select cast(operation as varchar(2)) from usinsug01.tab_cl_ope where reserve = 1 or ajustes = 1 or pay_amount = 1)
                                                                                             and     (clh.operdate > dat.his_fin --no existan operaciones posteriores al corte
                                                                                                             or clh.compdate > dat.his_fin)) --no existan modificaciones posteriores al corte
                                                             and		not exists
                                                                                     (   select  1
                                                                                             from	usinsug01.claim_his clh
                                                                                                             join	usinsug01.cl_m_cover clm
                                                                                                             on		clm.usercomp = clh.usercomp
                                                                                                             and		clm.company = clh.company
                                                                                                             and		clm.claim = clh.claim
                                                                                                             and		clm.movement = clh.transac
                                                                                                             and		clm.compdate > dat.his_fin --se consideran coberturas con modificaciones entre los rangos (limitaci�n carga inicial)
                                                                                             where	clh.claim = cla.claim
                                                                                             and		trim(clh.oper_type) in 
                                                                                                             (select cast(operation as varchar(2)) from usinsug01.tab_cl_ope where reserve = 1 or ajustes = 1 or pay_amount = 1))
                                                     where	pol.usercomp = 1
                                                     and		pol.company = 1
                                                     and		pol.certype = '2'
                                                     and		pol.branch in (select branch from usinsug01.table10b where company = 1)
                                                     and		pol.branch <> 23) cl0
                             join	usinsug01.claim cla on cla.ctid = cla_id
                             join	usinsug01.policy pol on pol.ctid = pol_id
                             where	cl0.monto_trans <> 0
                            ) AS TMP       
                         '''

    DF_INSUNIX_LPG_OTROS = glue_context.read.format('jdbc').options(**connection).option("fetchsize",10000).option("dbtable",INSUNIX_LPG_OTROS).load()

    DF_INSUNIX_LPG = DF_INSUNIX_LPG_AMF.union(DF_INSUNIX_LPG_OTROS)

    INSUNIX_LPV = f'''
                      (select	
                       'D' INDDETREC,
                       'SBSIN' TABLAIFRS17,
		       '' PK,
		       '' DTPREG, --excluido
		       '' TIOCPROC, --no disponible
		       cast(cl0.xxxxdate as varchar) TIOCFRM,
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
		       coalesce(cast(
		       	case	when	cla.staclaim in ('1','5','7')
		       			then	(	select 	max(case	when	cla.staclaim in ('1') and oper_type in ('3','17') then clh.operdate
		       											when	cla.staclaim in ('5') and oper_type in ('11') then clh.operdate
		       											when	cla.staclaim in ('7') and oper_type in ('21') then clh.operdate
		       											else	null end)
		       						from	usinsuv01.claim_his clh
		       						where	clh.claim = cla.claim
		       						and		trim(oper_type) in ('3','17','11','21')
		       						and		operdate <= cl0.dat_lim)
		       			else 	null end as varchar),'') TFECHTEC,
		       coalesce(cast(
		       	case	when	cla.staclaim in ('1','5','7')
		       			then	(	select 	max(case	when	cla.staclaim in ('1') and oper_type in ('3','17') then clh.operdate
		       											when	cla.staclaim in ('5') and oper_type in ('52') then clh.operdate
		       											when	cla.staclaim in ('7') and oper_type in ('21') then clh.operdate
		       											else	null end)
		       						from	usinsuv01.claim_his clh
		       						where	clh.claim = cla.claim
		       						and		trim(oper_type) in ('3','17','52','21')
		       						and		operdate <= cl0.dat_lim)
		       			else 	null end as varchar),'') TFECHADM,
		       coalesce(cast(
		       	(	select 	max(clh.operdate)
		       		from	usinsuv01.claim_his clh
		       		where	clh.claim = cla.claim
		       		and		(	trim(oper_type) in 
		       					(select cast(operation as varchar(2)) from usinsug01.tab_cl_ope where reserve = 1 or ajustes = 1 or pay_amount in (1,3))
		       					or trim(oper_type) = '60')
		       		and		operdate <= cl0.dat_lim) as varchar),'') TESTADO,
		       coalesce(cast(cla.staclaim as varchar),'') KSCSITUA,
		       '' KSCMOTSI, --excluido
		       '' KSCTPSIN, --no disponible
		       coalesce(cla.branch || '-' || cla.causecod,'') KSCCAUSA,
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
		       cast(	coalesce((	select	max(cpl.currency)
		       					from	usinsuv01.curren_pol cpl
		       					where 	cpl.usercomp = cla.usercomp and cpl.company = cla.company
		       					and		cpl.certype = pol.certype and cpl.branch = cla.branch
		       					and 	cpl.policy = cla.policy and cpl.certif = cla.certif),
		       				(	select	max(cpl.currency)
		       					from	usinsuv01.curren_pol cpl
		       					where 	cpl.usercomp = cla.usercomp and cpl.company = cla.company
		       					and		cpl.certype = pol.certype and cpl.branch = cla.branch
		       					and 	cpl.policy = cla.policy),0) as varchar) KSCMOEDA,
		       '' VCAMBIO, --no disponible
		       '' VTXRESPN, --no disponible
		       cast(cl0.monto_trans as numeric(14,2)) VMTPROVI,
		       '' VMTPRVINI, --excluido
		       cast(cl0.monto_trans * (case when pol.bussityp = '2' then 100 else share_coa end/100) as numeric(14,2)) VMTPRVRS, --no hay reaseguro en LPV INX
		       cast(cl0.monto_trans * (case when pol.bussityp = '2' then 100 else share_coa end/100) as numeric(14,2)) VMTCOSEG,
		       '' KSCNATUR, --excluido
		       '' TALTENAT, --excluido
		       '' DUSRREG, --excluido
		       cl0.client KEBENTID_TO,
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
		       		           		end, 0) as numeric(14,2)) VCAPITAL,
		       '' VTXINDEM, --no disponible
		       '' VMTINDEM, --no disponible
		       '' TINICIND, --no disponible
		       '' DULTACTA, --excluido
		       coalesce(	case	trim((select distinct(tabname) from usinsug01.tab_name_b where branch = cla.branch))
		       					when 	'life_prev'
		       					then 	(	select	statusva
		       								from	usinsuv01.life_prev
		       								where	ctid = 
		       							            coalesce(
		       						                    (   select max(ctid) from usinsuv01.life_prev
		       						                        where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
		       						                        and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate <= cla.occurdat
		       						                        and (nulldate is null or nulldate > cla.occurdat)
		       						                        and statusva not in ('2','3')),
		       						                    (   select max(ctid) from usinsuv01.life_prev
		       						                        where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
		       						                        and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate <= cla.occurdat
		       						                        and (nulldate is null or nulldate >= cla.occurdat)
		       						                        and statusva not in ('2','3')),
		       						                    (   select max(ctid) from usinsuv01.life_prev
		       						                        where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
		       						                        and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate <= cla.occurdat
		       						                        and (nulldate is null or nulldate < cla.occurdat)
		       						                        and statusva not in ('2','3')),
		       						                    (   select min(ctid) from usinsuv01.life_prev
		       						                        where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
		       						                        and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate > cla.occurdat
		       						                        and (nulldate is null or nulldate > cla.occurdat)
		       						                        and statusva not in ('2','3')),
		       						                    (   select min(ctid) from usinsuv01.life_prev
		       						                        where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
		       						                        and branch = cla.branch and policy = cla.policy and certif = cla.certif
		       						                        and statusva not in ('2','3')),
		       						                    (   select min(ctid) from usinsuv01.life_prev
		       						                        where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
		       						                        and branch = cla.branch and policy = cla.policy and certif = cla.certif
		       						                        and statusva in ('2','3'))))/*
		       					when 	'life'
		       					then 	(	select	statusva
		       								from	usinsuv01.life
		       								where	ctid = 
		       							            coalesce(
		       						                    (   select max(ctid) from usinsuv01.life
		       						                        where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
		       						                        and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate <= cla.occurdat
		       						                        and (nulldate is null or nulldate > cla.occurdat)
		       						                        and statusva not in ('2','3')),
		       						                    (   select max(ctid) from usinsuv01.life
		       						                        where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
		       						                        and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate <= cla.occurdat
		       						                        and (nulldate is null or nulldate >= cla.occurdat)
		       						                        and statusva not in ('2','3')),
		       						                    (   select max(ctid) from usinsuv01.life
		       						                        where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
		       						                        and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate <= cla.occurdat
		       						                        and (nulldate is null or nulldate < cla.occurdat)
		       						                        and statusva not in ('2','3')),
		       						                    (   select min(ctid) from usinsuv01.life
		       						                        where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
		       						                        and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate > cla.occurdat
		       						                        and (nulldate is null or nulldate > cla.occurdat)
		       						                        and statusva not in ('2','3')),
		       						                    (   select min(ctid) from usinsuv01.life
		       						                        where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
		       						                        and branch = cla.branch and policy = cla.policy and certif = cla.certif
		       						                        and statusva not in ('2','3')),
		       						                    (   select min(ctid) from usinsuv01.life
		       						                        where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
		       						                        and branch = cla.branch and policy = cla.policy and certif = cla.certif
		       						                        and statusva in ('2','3'))))*/
		       					when 	'health'
		       					then 	(	select	statusva
		       								from	usinsuv01.health
		       								where	ctid = 
		       							            coalesce(
		       						                    (   select max(ctid) from usinsuv01.health
		       						                        where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
		       						                        and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate <= cla.occurdat
		       						                        and (nulldate is null or nulldate > cla.occurdat)
		       						                        and statusva not in ('2','3')),
		       						                    (   select max(ctid) from usinsuv01.health
		       						                        where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
		       						                        and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate <= cla.occurdat
		       						                        and (nulldate is null or nulldate >= cla.occurdat)
		       						                        and statusva not in ('2','3')),
		       						                    (   select max(ctid) from usinsuv01.health
		       						                        where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
		       						                        and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate <= cla.occurdat
		       						                        and (nulldate is null or nulldate < cla.occurdat)
		       						                        and statusva not in ('2','3')),
		       						                    (   select min(ctid) from usinsuv01.health
		       						                        where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
		       						                        and branch = cla.branch and policy = cla.policy and certif = cla.certif and effecdate > cla.occurdat
		       						                        and (nulldate is null or nulldate > cla.occurdat)
		       						                        and statusva not in ('2','3')),
		       						                    (   select min(ctid) from usinsuv01.health
		       						                        where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
		       						                        and branch = cla.branch and policy = cla.policy and certif = cla.certif
		       						                        and statusva not in ('2','3')),
		       						                    (   select min(ctid) from usinsuv01.health
		       						                        where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
		       						                        and branch = cla.branch and policy = cla.policy and certif = cla.certif
		       						                        and statusva in ('2','3'))))
		       					else  '' end,
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
		       						end), '') KACESTAP,
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
		       right(coalesce(cl0.client,''),10) DUNIDRISC,
		       coalesce((	select	cast(max(operdate) as varchar)
		       			from	usinsuv01.claim_his
		       			where	claim = cla.claim
		       			and		oper_type = '16'
		       			and		operdate <= cl0.dat_lim),'') TDTREABE,
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
                       from 	(	select 	case	pol.bussityp
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
                                                                                                                       and 	coalesce(coi.companyc,0) in (1,12)),100) end share_coa,
                                                               coalesce((	select	sum(coalesce(
                                                                                                                       case	coalesce(
                                                                                                                                               coalesce((	select	max(cpl.currency)
                                                                                                                                                                       from	usinsuv01.curren_pol cpl
                                                                                                                                                                       where 	cpl.usercomp = cla.usercomp and cpl.company = cla.company and cpl.certype = pol.certype
                                                                                                                                                                       and		cpl.branch = cla.branch and cpl.policy = cla.policy and	cpl.certif = cla.certif),
                                                                                                                                                               (	select	max(cpl.currency)
                                                                                                                                                                       from	usinsuv01.curren_pol cpl
                                                                                                                                                                       where 	cpl.usercomp = cla.usercomp and cpl.company = cla.company and cpl.certype = pol.certype
                                                                                                                                                                       and		cpl.branch = cla.branch and cpl.policy = cla.policy)),0)
                                                                                                                                       when	clh.currency-- = cl0.moneda_cod
                                                                                                                                       then    coalesce(clh.amount,0)
                                                                                                                                       else	coalesce(clh.amount,0) *
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
                                                                                                                                                                       else	1 end end, 0))
                                                                                       from	usinsuv01.claim_his clh
                                                                                       where	clh.claim = cla.claim
                                                                                       and		trim(clh.oper_type) in (select cast(operation as varchar(2)) from usinsug01.tab_cl_ope where reserve = 1 or ajustes = 1)
                                                                                       and		clh.operdate <= dat.his_fin),0) monto_trans, --evita tomar transacciones actualizadas posteriormente (carga incremental)
                                                               coalesce((	select	evi.scod_vt
                                                                                       from	usinsug01.equi_vt_inx evi 
                                                                                       where	evi.scod_inx = cla.client),
                                                                               cla.client) client,
                                                               (	select 	max(greatest(
                                                                                               case when clm.compdate <= dat.his_fin then clm.compdate else '0001-01-01'::date end,
                                                                                               case when clh.compdate <= dat.his_fin then clh.compdate else '0001-01-01'::date end,
                                                                                               case when clh.operdate <= dat.his_fin then clh.operdate else '0001-01-01'::date end,
                                                                                               case when cla.compdate <= dat.his_fin then cla.compdate else '0001-01-01'::date end))
                                                                       from	usinsuv01.claim_his clh
                                                                                       join	usinsuv01.cl_m_cover clm
                                                                                       on		clm.usercomp = clh.usercomp
                                                                                       and		clm.company = clh.company
                                                                                       and		clm.claim = clh.claim
                                                                                       and		clm.movement = clh.transac
                                                                       where	clh.claim = cla.claim
                                                                       and		trim(clh.oper_type) in 
                                                                                       (	select	cast(operation as varchar(2))
                                                                                               from	usinsug01.tab_cl_ope
                                                                                               where	reserve = 1 or ajustes = 1 or pay_amount = 1)) xxxxdate, --se busca la mayor fecha a la nueva fecha de quiebre
                                                               dat.his_fin dat_lim, 
                                                               pol.ctid pol_id,
                                                               cla.ctid cla_id
                                               from    (	select	ctid pol_id
                                                                       from	usinsuv01.policy
                                                                       where	usercomp = 1
                                                                       and		company = 1
                                                                       and		certype = '2'
                                                                       and		branch in (select branch from usinsug01.table10b where company = 2)) po0
                                               join	(	select	cast('{p_fecha_inicio}' as date) his_ini, --rango inicial carga hist�rica (variable a elecci�n)
                                                                        cast('{p_fecha_fin}' as date) his_fin --rango final carga hist�rica (variable a elecci�n)
                                                                                       ) dat on 1 = 1
                                               join	usinsuv01.policy pol on pol.ctid = po0.pol_id
                                               join	usinsuv01.claim cla
                                                       on		cla.usercomp = pol.usercomp
                                                       and     cla.company = pol.company
                                                       and     cla.branch = pol.branch
                                                       and     cla.policy = pol.policy
                                                       and		cla.staclaim <> '6'
                                                       and		(cla.compdate between dat.his_ini and dat.his_fin --se consideran siniestros con modificaciones entre los rangos (limitaci�n carga inicial)
                                                                       or	exists --a nivel de transacciones
                                                                               (   select  1
                                                                                       from	usinsuv01.claim_his clh
                                                                                       where	clh.claim = cla.claim
                                                                                       and		trim(clh.oper_type) in 
                                                                                                       (select cast(operation as varchar(2)) from usinsug01.tab_cl_ope where reserve = 1 or ajustes = 1 or pay_amount = 1)
                                                                                       and     (clh.operdate between dat.his_ini and dat.his_fin --se consideran siniestros con operaciones entre los rangos (limitaci�n carga inicial)
                                                                                                       or clh.compdate between dat.his_ini and dat.his_fin)) --se consideran siniestros con modificaciones entre los rangos (limitaci�n carga inicial)								
                                                                               or exists --a nivel de coberturas
                                                                               (   select  1
                                                                                       from	usinsuv01.claim_his clh
                                                                                                       join	usinsuv01.cl_m_cover clm
                                                                                                       on		clm.usercomp = clh.usercomp
                                                                                                       and		clm.company = clh.company
                                                                                                       and		clm.claim = clh.claim
                                                                                                       and		clm.movement = clh.transac
                                                                                                       and		clm.compdate between dat.his_ini and dat.his_fin --se consideran coberturas con modificaciones entre los rangos (limitaci�n carga inicial)
                                                                                       where	clh.claim = cla.claim
                                                                                       and		trim(clh.oper_type) in 
                                                                                                       (select cast(operation as varchar(2)) from usinsug01.tab_cl_ope where reserve = 1 or ajustes = 1 or pay_amount = 1)))
                                                       and		not exists
                                                                               (   select  1
                                                                                       from    usinsuv01.claim_his clh
                                                                                       where   clh.claim = cla.claim
                                                                                       and		trim(clh.oper_type) in 
                                                                                                       (select cast(operation as varchar(2)) from usinsug01.tab_cl_ope where reserve = 1 or ajustes = 1 or pay_amount = 1)
                                                                                       and     (clh.operdate > dat.his_fin --no existan operaciones posteriores al corte
                                                                                                       or clh.compdate > dat.his_fin)) --no existan modificaciones posteriores al corte
                                                       and		not exists
                                                                               (   select  1
                                                                                       from	usinsuv01.claim_his clh
                                                                                                       join	usinsuv01.cl_m_cover clm
                                                                                                       on		clm.usercomp = clh.usercomp
                                                                                                       and		clm.company = clh.company
                                                                                                       and		clm.claim = clh.claim
                                                                                                       and		clm.movement = clh.transac
                                                                                                       and		clm.compdate > dat.his_fin --se consideran coberturas con modificaciones entre los rangos (limitaci�n carga inicial)
                                                                                       where	clh.claim = cla.claim
                                                                                       and		trim(clh.oper_type) in 
                                                                                                       (select cast(operation as varchar(2)) from usinsug01.tab_cl_ope where reserve = 1 or ajustes = 1 or pay_amount = 1))) cl0 --se excluyen transacciones sin montos
                       join	usinsuv01.claim cla on cla.ctid = cl0.cla_id
                       join	usinsuv01.policy pol on pol.ctid = cl0.pol_id
                       where	cl0.monto_trans <> 0
                      ) AS TMP
                   '''

    DF_INSUNIX_LPV = glue_context.read.format('jdbc').options(**connection).option("fetchsize",10000).option("dbtable",INSUNIX_LPV).load()    

    VTIME_LPG = f'''
                    (select	
                     'D' INDDETREC,
                     'SBSIN' TABLAIFRS17,
		     '' PK,
		     '' DTPREG, --excluido
		     '' TIOCPROC, --no disponible
		     cast(cl0.xxxxdate as varchar) TIOCFRM,
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
		     										else	"NOPER_TYPE" is null end
		     						and		cast("DOPERDATE" as date) <= cl0.dat_lim)
		     			else '' end, '')  TFECHTEC,
		     coalesce(
		     	case	when	trim(cla."SSTACLAIM") in ('1','5','7')
		     			then	(	select	cast(cast(max("DOPERDATE") as date) as varchar)
		     						from	usvtimg01."CLAIM_HIS"
		     						where	"NCLAIM" = cla."NCLAIM"
		     						and		case	when	trim(cla."SSTACLAIM") in ('1') then "NOPER_TYPE" in (3,17)
		     										when	trim(cla."SSTACLAIM") in ('5') then "NOPER_TYPE" in (77)
		     										when	trim(cla."SSTACLAIM") in ('7') then "NOPER_TYPE" in (21)
		     										else	"NOPER_TYPE" is null end
		     						and		cast("DOPERDATE" as date) <= cl0.dat_lim)
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
		     							or "NOPER_TYPE" in (75,76,77,78,79,80,83))
		     					and		cast("DOPERDATE" as date) <= cl0.dat_lim),'') TESTADO,
		     coalesce(cast(cla."SSTACLAIM" as varchar),'') KSCSITUA,
		     '' KSCMOTSI, --excluido
		     '' KSCTPSIN, --no disponible
		     case	when	exists
		     				(	select 	1
		     					from	usvtimg01."CLAIM_CAUS" 
		     					where	"NBRANCH" = cla."NBRANCH"
		     					and		"NPRODUCT" = pol."NPRODUCT"
		     					and		"NCAUSECOD" = cla."NCAUSECOD")
		     		then	coalesce(cla."NBRANCH" || '-' || pol."NPRODUCT" || '-' || cla."NCAUSECOD",'')
		     		else	coalesce(cla."NBRANCH" || '-0-' || cla."NCAUSECOD",'')
		     		end KSCCAUSA,
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
		     cast(cl0.moneda_cod as varchar) KSCMOEDA,
		     '' VCAMBIO, --no disponible
		     '' VTXRESPN, --no disponible
		     cast(cl0.monto_trans as numeric(14,2)) VMTPROVI,
		     '' VMTPRVINI, --excluido
		     cast(	cl0.monto_trans *
		     		(case when pol."SBUSSITYP" = '2' then 100 else nshare_coa end / 100) *
		     		case 	when	not(cl0.monto_trans <> 0 and cl0.flag_rea <> 0)
		     				then	1
		     				else	(	select	sum(ratio_rea)
		     							from	(	select	coalesce((	select	sum(cl1.monto_trans * coalesce("NSHARE",0)/100)
		     															from	usvtimg01."REINSURAN" REI --* nivel revisado para el esquema de reaseguro asociado al registro
		     															where	REI."SCERTYPE" = cla."SCERTYPE" AND REI."NBRANCH" = cla."NBRANCH"
		     															and		REI."NPRODUCT" = pol."NPRODUCT" AND REI."NPOLICY" = cla."NPOLICY"
		     															and		REI."NCERTIF" = case cl1.flag_rea when 1 then coalesce(cla."NCERTIF",0) when 2 then 0 end
		     															and		REI."NTYPE_REIN" = 1 and REI."NBRANCH_REI" = cl1.nbranch_rei
		     															and		cast(REI."DEFFECDATE" as date) <= cast(cla."DOCCURDAT" as date)
		     															and		(REI."DNULLDATE" IS NULL OR cast(REI."DNULLDATE" as date) > cast(cla."DOCCURDAT" as date))),0) /
		     															cl0.monto_trans ratio_rea
		     										from 	(	select	cl1.nbranch_rei,
		     															cl1.monto_trans,
		     															case	when	NOT (pol."SPOLITYPE" = '2' AND cla."NBRANCH" = 57 AND cla."NPRODUCT" = 1)
		     																	then	case	when	EXISTS
		     																							(	SELECT  1
		     																								from	usvtimg01."REINSURAN" REI
		     																								where	REI."SCERTYPE" = pol."SCERTYPE" AND REI."NBRANCH" = cla."NBRANCH"
		     																								and		REI."NPRODUCT" = pol."NPRODUCT" AND REI."NPOLICY" = cla."NPOLICY"
		     																								and		REI."NCERTIF" = case when "SPOLITYPE" = '3' then 0 else cla."NCERTIF" end
		     																								and		REI."NBRANCH_REI" = cl1.nbranch_rei
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
		     																								and		REI."NCERTIF" = cla."NCERTIF" AND REI."NBRANCH_REI" = cl1.nbranch_rei
		     																								and		cast(REI."DEFFECDATE" as DATE) <= cast(cla."DOCCURDAT" as date)
		     																								and		(REI."DNULLDATE" IS NULL OR CAST(REI."DNULLDATE" as DATE) > cast(cla."DOCCURDAT" as date)))
		     																					then    1
		     																					else 	case 	when	EXISTS
		     																											(   SELECT  1
		     																												from	usvtimg01."REINSURAN" REI
		     																												where	REI."SCERTYPE" = pol."SCERTYPE" AND REI."NBRANCH" = cla."NBRANCH"
		     																												and		REI."NPRODUCT" = pol."NPRODUCT" AND REI."NPOLICY" = cla."NPOLICY"
		     																												and		REI."NCERTIF" = 0 and REI."NBRANCH_REI" = cl1.nbranch_rei
		     																												and		cast(REI."DEFFECDATE" as DATE) <= cast(cla."DOCCURDAT" as date)
		     																												and		(REI."DNULLDATE" IS NULL OR CAST(REI."DNULLDATE" as DATE) > cast(cla."DOCCURDAT" as date)))
		     															                                                   THEN    2
		     															                                                   ELSE    0 END END
		     															                   ELSE    0 END flag_rea
		     													from 	(	select	case	when	cla."NBRANCH" = 21
		     																        		then	(   select  gco."NBRANCH_REI"
		     																		                    from    usvtimg01."LIFE_COVER" gco
		     																		                    where   gco."NCOVER" = clm."NCOVER"
		     																		                    and     gco."NPRODUCT" = pol."NPRODUCT"
		     																		                    and     gco."NMODULEC" = clm."NMODULEC"
		     																		                    and     gco."NBRANCH" = cla."NBRANCH"
		     																		                    and     cast(gco."DEFFECDATE" as date) <= cast(cla."DOCCURDAT" as date)
		     																		                    and     (gco."DNULLDATE" is null or cast(gco."DNULLDATE" as date) > cast(cla."DOCCURDAT" as date))
		     																		                    and     gco."SSTATREGT" <> '4')
		     																				else	(   select  gco."NBRANCH_REI"
		     																		                    from    usvtimg01."GEN_COVER" gco
		     																		                    where   gco."NCOVER" = clm."NCOVER"
		     																		                    and     gco."NPRODUCT" = pol."NPRODUCT"
		     																		                    and     gco."NMODULEC" = clm."NMODULEC"
		     																		                    and     gco."NBRANCH" = cla."NBRANCH"
		     																		                    and     cast(gco."DEFFECDATE" as date) <= cast(cla."DOCCURDAT" as date)
		     																		                    and     (gco."DNULLDATE" is null or cast(gco."DNULLDATE" as date) > cast(cla."DOCCURDAT" as date))
		     																		                    and     gco."SSTATREGT" <> '4') end nbranch_rei,
		     																		sum(case	cl0.moneda_cod
		     																					when	1
		     																					then	case	when	clm."NCURRENCY" = 1
		     																									then	clm."NAMOUNT"
		     																									when	clm."NCURRENCY" = 2
		     																									then	clm."NAMOUNT" * clm."NEXCHANGE"
		     																									else	0 end
		     																					when	2
		     																					then	case	when	clm."NCURRENCY" = 2
		     																									then	clm."NAMOUNT"
		     																									when	clm."NCURRENCY" = 1
		     																									then	clm."NLOC_AMOUNT"
		     																									else	0 end
		     																					else	0 end) monto_trans
		     																from 	usvtimg01."CLAIM_HIS" clh,
		     																		usvtimg01."CL_M_COVER" clm
		     																where	clh."NCLAIM" = cla."NCLAIM"
		     																and		clh."NOPER_TYPE" in (select cast("SVALUE" as INT4) from usvtimg01."CONDITION_SERV" where "NCONDITION" in (71,72)) 
		     																and     cast(clh."DOPERDATE" as date) <= cl0.dat_lim
		     																and		cast(clh."DCOMPDATE" as date) <= cl0.dat_lim
		     																and		clm."NCLAIM" = clh."NCLAIM"
		     																and 	clm."NCASE_NUM" = clh."NCASE_NUM"
		     																and 	clm."NDEMAN_TYPE" = clh."NDEMAN_TYPE"
		     																and 	clm."NTRANSAC" = clh."NTRANSAC"
		     																group	by 1) cl1
		     													where	coalesce(cl1.monto_trans,0) <> 0) cl1) rea) end
		     		as numeric(14,2)) VMTPRVRS,
		     cast(cl0.monto_trans * (case when pol."SBUSSITYP" = '2' then 100 else nshare_coa end/100) as numeric(14,2)) VMTCOSEG,
		     '' KSCNATUR, --excluido
		     '' TALTENAT, --excluido
		     '' DUSRREG, --excluido
		     cla."SCLIENT" KEBENTID_TO,
		     cast(	coalesce(	case	when	cla."NBRANCH" = 21 
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
		     								and		cer."NPRODUCT" = pol."NPRODUCT"
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
		     													and		cer."NPRODUCT" = pol."NPRODUCT"
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
		     					then	coalesce((	select	coalesce(	case	when	trim(coalesce(tnb."SREGIST",'')) <> ''
		     																	then	trim(tnb."SREGIST")
		     																	else 	case	when	trim(coalesce(tnb."SCHASSIS",'')) <> ''
		     																					then	trim(tnb."SCHASSIS")
		     																					else	''
		     																					end
		     																	end,'')
		     										from	usvtimg01."AUTO" tnb
		     										where	ctid = 
		     												coalesce(
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
		     								                        and (tn0."DNULLDATE" is null or cast(tn0."DNULLDATE" as date) < cast(cla."DOCCURDAT" as date))))),'')
		     					--when	2
		     					when	cla."NBRANCH" in (1,2,3,4,5,7,8,9,10,11,12,13,14,17,18,19,28,29,30,38,39,45,55,57,58,59,60,61,63,921)
		     					then	cla."NBRANCH" || '-' || cla."NPOLICY" || '-' || cla."NCERTIF"
		     					--when	1
		     					when	cla."NBRANCH" in (21,23,24,27,31,32,33,34,35,36,37,40,42,64,71,75,91)
		     					then	cla."SCLIENT"
		     					else 	'' end, '') DUNIDRISC,
		     coalesce((	select	cast(cast(max("DOPERDATE") as date) as varchar)
		     			from	usvtimg01."CLAIM_HIS"
		     			where	"NCLAIM" = cla."NCLAIM"
		     			and		"NOPER_TYPE" = 16
		     			and		cast("DOPERDATE" as date) <= cl0.dat_lim),'') TDTREABE,
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
                     from 	(	select  cla.ctid cla_id,
                                                             pol.ctid pol_id,
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
                                                             coalesce((	select	sum(case	coalesce(
                                                                                                                                     (	select  max(cpl."NCURRENCY")
                                                                                                                                             from    usvtimg01."CURREN_POL" cpl
                                                                                                                                             where 	cpl."SCERTYPE" = cla."SCERTYPE" and	cpl."NBRANCH" = cla."NBRANCH" 
                                                                                                                                             and		cpl."NPRODUCT" = pol."NPRODUCT" and cpl."NPOLICY" = cla."NPOLICY"
                                                                                                                                             and		cpl."NCERTIF" = cla."NCERTIF"),
                                                                                                             (   select  max(cpl."NCURRENCY")
                                                                                                                                             from    usvtimg01."CURREN_POL" cpl
                                                                                                                                             where 	cpl."SCERTYPE" = cla."SCERTYPE" and	cpl."NBRANCH" = cla."NBRANCH" 
                                                                                                                                             and		cpl."NPRODUCT" = pol."NPRODUCT" and cpl."NPOLICY" = cla."NPOLICY"),0)
                                                                                                                             when	1
                                                                                                                             then	case	when	clh."NCURRENCY" = 1
                                                                                                                                                             then	clh."NAMOUNT"
                                                                                                                                                             when	clh."NCURRENCY" = 2
                                                                                                                                                             then	clh."NAMOUNT" * clh."NEXCHANGE"
                                                                                                                                                             else	0 end
                                                                                                                             when	2
                                                                                                                             then	case	when	clh."NCURRENCY" = 2
                                                                                                                                                             then	clh."NAMOUNT"
                                                                                                                                                             when	clh."NCURRENCY" = 1
                                                                                                                                                             then	clh."NLOC_AMOUNT"
                                                                                                                                                             else	0 end
                                                                                                                             else	0 end) 
                                                                                     from 	usvtimg01."CLAIM_HIS" clh
                                                                                     where	clh."NCLAIM" = cla."NCLAIM"
                                                                                     and		clh."NOPER_TYPE" in (select cast("SVALUE" as INT4) from usvtimg01."CONDITION_SERV" where "NCONDITION" in (71,72)) 
                                                                                     and     cast(clh."DOPERDATE" as date) <= dat.his_fin),0) monto_trans,
                                                             case	pol."SBUSSITYP"
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
                                                                                                                     and		coi."NCOMPANY" in (1)),100) end nshare_coa,
                                                     case	when	NOT (pol."SPOLITYPE" = '2' AND cla."NBRANCH" = 57 AND cla."NPRODUCT" = 1)
                                                                     then	case	when	EXISTS
                                                                                                                     (	SELECT  1
                                                                                                                             from	usvtimg01."REINSURAN" REI
                                                                                                                             where	REI."SCERTYPE" = pol."SCERTYPE" AND REI."NBRANCH" = cla."NBRANCH"
                                                                                                                             and		REI."NPRODUCT" = pol."NPRODUCT" AND REI."NPOLICY" = cla."NPOLICY"
                                                                                                                             and		REI."NCERTIF" = case when "SPOLITYPE" = '3' then 0 else cla."NCERTIF" end
                                                                                                                             --and		REI."NBRANCH_REI" = gen."NBRANCH_REI"
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
                                                                                                                             and		REI."NCERTIF" = cla."NCERTIF" --AND REI."NBRANCH_REI" = gen."NBRANCH_REI"
                                                                                                                             and		cast(REI."DEFFECDATE" as DATE) <= cast(cla."DOCCURDAT" as date)
                                                                                                                             and		(REI."DNULLDATE" IS NULL OR CAST(REI."DNULLDATE" as DATE) > cast(cla."DOCCURDAT" as date)))
                                                                                                     then    1
                                                                                                     else 	case 	when	EXISTS
                                                                                                                                                     (   SELECT  1
                                                                                                                                                             from	usvtimg01."REINSURAN" REI
                                                                                                                                                             where	REI."SCERTYPE" = pol."SCERTYPE" AND REI."NBRANCH" = cla."NBRANCH"
                                                                                                                                                             and		REI."NPRODUCT" = pol."NPRODUCT" AND REI."NPOLICY" = cla."NPOLICY"
                                                                                                                                                             and		REI."NCERTIF" = 0 --and REI."NBRANCH_REI" = gen."NBRANCH_REI"
                                                                                                                                                             and		cast(REI."DEFFECDATE" as DATE) <= cast(cla."DOCCURDAT" as date)
                                                                                                                                                             and		(REI."DNULLDATE" IS NULL OR CAST(REI."DNULLDATE" as DATE) > cast(cla."DOCCURDAT" as date)))
                                                                                                     THEN    2
                                                                                                     ELSE    0 END END
                                                                     ELSE    0 END flag_rea,
                                                             (	select 	max(greatest(
                                                                                             case when cast(clm."DCOMPDATE" as date) <= dat.his_fin then cast(clm."DCOMPDATE" as date) else '0001-01-01'::date end,
                                                                                             case when cast(clh."DCOMPDATE" as date) <= dat.his_fin then cast(clh."DCOMPDATE" as date) else '0001-01-01'::date end,
                                                                                             case when cast(clh."DOPERDATE" as date) <= dat.his_fin then cast(clh."DOPERDATE" as date) else '0001-01-01'::date end,
                                                                                             case when cast(cla."DCOMPDATE" as date) <= dat.his_fin then cast(cla."DCOMPDATE" as date) else '0001-01-01'::date end))
                                                                     from	usvtimg01."CLAIM_HIS" clh
                                                             join	usvtimg01."CL_M_COVER" clm
                                                             on		clm."NCLAIM" = clh."NCLAIM"
                                                             and		clm."NCASE_NUM" = clH."NCASE_NUM"
                                                             and		clm."NDEMAN_TYPE" = clh."NDEMAN_TYPE"
                                                             and		clm."NTRANSAC" = clh."NTRANSAC"
                                                                     where	clh."NCLAIM" = cla."NCLAIM"
                                                                     and		clh."NOPER_TYPE" in (select	cast("SVALUE" as INT4) from usvtimg01."CONDITION_SERV" where "NCONDITION" in (71,72,73))
                                                                     and		(clh."DOPERDATE" <= dat.his_fin or clh."DCOMPDATE" <= dat.his_fin) --evita tomar operaciones posteriores (carga incremental)
                                                                     ) xxxxdate,
                                                             dat.his_fin dat_lim
                                             from    (	select	ctid pol_id
                                                                     from	usvtimg01."POLICY"
                                                                     where	"SCERTYPE" = '2') po0
                                             join	(	select	cast('{p_fecha_inicio}' as date) his_ini, --rango inicial carga hist�rica (variable a elecci�n)
                                                                        cast('{p_fecha_fin}' as date) his_fin --rango final carga hist�rica (variable a elecci�n)
                                                             ) dat on 1 = 1		
                                             join	usvtimg01."POLICY" pol on pol.ctid = po0.pol_id
                                             join	usvtimg01."CLAIM" cla
                                                             on		cla."SCERTYPE" = pol."SCERTYPE"
                                                     and     cla."NPOLICY" = pol."NPOLICY"
                                                     and     cla."NBRANCH" = pol."NBRANCH"
                                                     and     cla."SSTACLAIM" <> '6'
                                                     and		(cast(cla."DCOMPDATE" as date) between dat.his_ini and dat.his_fin
                                                                             or	exists
                                                                     (   select  1
                                                                     from    usvtimg01."CLAIM_HIS" clh
                                                                     where	clh."NCLAIM" = cla."NCLAIM"
                                                                     and		coalesce(clh."NCASE_NUM",0) >= 0
                                                                                             and     (cast(clh."DOPERDATE" as date) between dat.his_ini and dat.his_fin --se consideran siniestros con operaciones entre los rangos (limitaci�n carga inicial)
                                                                                                             or cast(clh."DCOMPDATE" as date) between dat.his_ini and dat.his_fin) --se consideran siniestros con modificaciones entre los rangos (limitaci�n carga inicial)
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
                                                                                     and		cast(clm."DCOMPDATE" as date) between dat.his_ini and dat.his_fin --se consideran coberturas con modificaciones entre los rangos (limitaci�n carga inicial)
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
                                                                                     and     (cast(clh."DOPERDATE" as date) > dat.his_fin --no existan operaciones posteriores al corte
                                                                                                     or cast(clh."DOPERDATE" as date) > dat.his_fin) --no existan modificaciones posteriores al corte
                                                                                     and		coalesce(clh."NAMOUNT",0) <> 0)
                                                             and		not exists
                                                             (   select  1
                                                             from    usvtimg01."CLAIM_HIS" clh
                                                                             join	usvtimg01."CL_M_COVER" clm
                                                                             on		clm."NCLAIM" = clh."NCLAIM"
                                                                             and		clm."NCASE_NUM" = clH."NCASE_NUM"
                                                                             and		clm."NDEMAN_TYPE" = clh."NDEMAN_TYPE"
                                                                             and		clm."NTRANSAC" = clh."NTRANSAC"
                                                                             and		cast(clm."DCOMPDATE" as date) > dat.his_fin --se consideran coberturas con modificaciones entre los rangos (limitaci�n carga inicial)
                                                             where	clh."NCLAIM" = cla."NCLAIM"
                                                             and		coalesce(clh."NCASE_NUM",0) >= 0
                                                                                     and		clh."NOPER_TYPE" in 
                                                                                                     (select	cast("SVALUE" as INT4) from	usvtimg01."CONDITION_SERV" cs  where "NCONDITION" in (71,72,73)))) cl0
                     join	usvtimg01."CLAIM" cla on cla.ctid = cl0.cla_id
                     join	usvtimg01."POLICY" pol on pol.ctid = cl0.pol_id
                     where	cl0.monto_trans <> 0
                    ) AS TMP
                 '''
                            
    DF_VTIME_LPG = glue_context.read.format('jdbc').options(**connection).option("fetchsize",10000).option("dbtable",VTIME_LPG).load()

    VTIME_LPV = f'''
                    (select	
                     'D' INDDETREC,
                     'SBSIN' TABLAIFRS17,
		     '' PK,
		     '' DTPREG, --excluido
		     '' TIOCPROC, --no disponible
		     cast(cl0.xxxxdate as varchar) TIOCFRM,
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
		     										else	"NOPER_TYPE" is null end
		     						and		cast("DOPERDATE" as date) <= cl0.dat_lim)
		     			else '' end, '')  TFECHTEC,
		     coalesce(
		     	case	when	trim(cla."SSTACLAIM") in ('1','5','7')
		     			then	(	select	cast(cast(max("DOPERDATE") as date) as varchar)
		     						from	usvtimv01."CLAIM_HIS"
		     						where	"NCLAIM" = cla."NCLAIM"
		     						and		case	when	trim(cla."SSTACLAIM") in ('1') then "NOPER_TYPE" in (3,17)
		     										when	trim(cla."SSTACLAIM") in ('5') then "NOPER_TYPE" in (77)
		     										when	trim(cla."SSTACLAIM") in ('7') then "NOPER_TYPE" in (21)
		     										else	"NOPER_TYPE" is null end
		     						and		cast("DOPERDATE" as date) <= cl0.dat_lim)
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
		     							or "NOPER_TYPE" in (75,76,77,78,79,80,83))
		     		and		cast("DOPERDATE" as date) <= cl0.dat_lim),'') TESTADO,
		     coalesce(cast(cla."SSTACLAIM" as varchar),'') KSCSITUA,
		     '' KSCMOTSI, --excluido
		     '' KSCTPSIN, --no disponible
		     /*
		     case	when	exists
		     				(	select 	1
		     					from	usvtimv01."CLAIM_CAUS" 
		     					where	"NBRANCH" = cla."NBRANCH"
		     					and		"NPRODUCT" = pol."NPRODUCT"
		     					and		"NCAUSECOD" = cla."NCAUSECOD")
		     		then	coalesce(cla."NBRANCH" || '-' || pol."NPRODUCT" || '-' || cla."NCAUSECOD",'')
		     		else	coalesce(cla."NBRANCH" || '-0-' || cla."NCAUSECOD",'')
		     		end*/ '' KSCCAUSA,
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
		     cast(cl0.moneda_cod as varchar) KSCMOEDA,
		     '' VCAMBIO, --no disponible
		     '' VTXRESPN, --no disponible
		     cast(cl0.monto_trans as numeric(14,2)) VMTPROVI,
		     '' VMTPRVINI, --excluido
		     cast(cl0.monto_trans * (case when pol."SBUSSITYP" = '2' then 100 else nshare_coa end/100) as numeric(14,2)) VMTPRVRS, --no HAY REASEGURO EN LPV-VT
		     cast(cl0.monto_trans * (case when pol."SBUSSITYP" = '2' then 100 else nshare_coa end/100) as numeric(14,2)) VMTCOSEG,
		     '' KSCNATUR, --excluido
		     '' TALTENAT, --excluido
		     '' DUSRREG, --excluido
		     cla."SCLIENT" KEBENTID_TO,
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
		     								and		cer."NPRODUCT" = pol."NPRODUCT"
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
		     													and		cer."NPRODUCT" = pol."NPRODUCT"
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
		     right(cla."SCLIENT",10) DUNIDRISC,
		     coalesce((	select	cast(cast(max("DOPERDATE") as date) as varchar)
		     			from	usvtimv01."CLAIM_HIS"
		     			where	"NCLAIM" = cla."NCLAIM"
		     			and		"NOPER_TYPE" = 16
		     			and		cast("DOPERDATE" as date) <= cl0.dat_lim),'') TDTREABE,
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
                     from 	(	select  cla.ctid cla_id,
                                                             pol.ctid pol_id,
                                                             coalesce((  select  max(cpl."NCURRENCY")
                                                                                     from    usvtimv01."CURREN_POL" cpl
                                                                                     where 	cpl."SCERTYPE" = cla."SCERTYPE" and	cpl."NBRANCH" = cla."NBRANCH" 
                                                                                     and		cpl."NPRODUCT" = pol."NPRODUCT" and cpl."NPOLICY" = cla."NPOLICY"
                                                                                     and		cpl."NCERTIF" = cla."NCERTIF"),
                                                     (   select  max(cpl."NCURRENCY")
                                                                                     from    usvtimv01."CURREN_POL" cpl
                                                                                     where 	cpl."SCERTYPE" = cla."SCERTYPE" and	cpl."NBRANCH" = cla."NBRANCH" 
                                                                                     and		cpl."NPRODUCT" = pol."NPRODUCT" and cpl."NPOLICY" = cla."NPOLICY"),0) moneda_cod,
                                                             coalesce((	select	sum(case	coalesce((  select  max(cpl."NCURRENCY")
                                                                                                                                                     from    usvtimv01."CURREN_POL" cpl
                                                                                                                                                     where 	cpl."SCERTYPE" = cla."SCERTYPE" and	cpl."NBRANCH" = cla."NBRANCH" 
                                                                                                                                                     and		cpl."NPRODUCT" = pol."NPRODUCT" and cpl."NPOLICY" = cla."NPOLICY"
                                                                                                                                                     and		cpl."NCERTIF" = cla."NCERTIF"),
                                                                                                                     (   select  max(cpl."NCURRENCY")
                                                                                                                                                     from    usvtimv01."CURREN_POL" cpl
                                                                                                                                                     where 	cpl."SCERTYPE" = cla."SCERTYPE" and	cpl."NBRANCH" = cla."NBRANCH" 
                                                                                                                                                     and		cpl."NPRODUCT" = pol."NPRODUCT" and cpl."NPOLICY" = cla."NPOLICY"),0)
                                                                                                                             when	1
                                                                                                                             then	case	when	clh."NCURRENCY" = 1
                                                                                                                                                             then	clh."NAMOUNT"
                                                                                                                                                             when	clh."NCURRENCY" = 2
                                                                                                                                                             then	clh."NAMOUNT" * clh."NEXCHANGE"
                                                                                                                                                             else	0 end
                                                                                                                             when	2
                                                                                                                             then	case	when	clh."NCURRENCY" = 2
                                                                                                                                                             then	clh."NAMOUNT"
                                                                                                                                                             when	clh."NCURRENCY" = 1
                                                                                                                                                             then	clh."NLOC_AMOUNT"
                                                                                                                                                             else	0 end
                                                                                                                             else	0 end) 
                                                                                     from 	usvtimv01."CLAIM_HIS" clh
                                                                                     where	clh."NCLAIM" = cla."NCLAIM"
                                                                                     and		clh."NOPER_TYPE" in (select cast("SVALUE" as INT4) from usvtimv01."CONDITION_SERV" where "NCONDITION" in (71,72)) 
                                                                                     and     cast(clh."DOPERDATE" as date) <= dat.his_fin),0) monto_trans,
                                                             case	pol."SBUSSITYP"
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
                                                                                                                     and		coi."NCOMPANY" in (1)),100) end nshare_coa,
                                                             (	select 	max(greatest(
                                                                                             case when cast(clm."DCOMPDATE" as date) <= dat.his_fin then cast(clm."DCOMPDATE" as date) else '0001-01-01'::date end,
                                                                                             case when cast(clh."DCOMPDATE" as date) <= dat.his_fin then cast(clh."DCOMPDATE" as date) else '0001-01-01'::date end,
                                                                                             case when cast(clh."DOPERDATE" as date) <= dat.his_fin then cast(clh."DOPERDATE" as date) else '0001-01-01'::date end,
                                                                                             case when cast(cla."DCOMPDATE" as date) <= dat.his_fin then cast(cla."DCOMPDATE" as date) else '0001-01-01'::date end))
                                                                     from	usvtimv01."CLAIM_HIS" clh
                                                             join	usvtimv01."CL_M_COVER" clm
                                                             on		clm."NCLAIM" = clh."NCLAIM"
                                                             and		clm."NCASE_NUM" = clH."NCASE_NUM"
                                                             and		clm."NDEMAN_TYPE" = clh."NDEMAN_TYPE"
                                                             and		clm."NTRANSAC" = clh."NTRANSAC"
                                                                     where	clh."NCLAIM" = cla."NCLAIM"
                                                                     and		clh."NOPER_TYPE" in (select	cast("SVALUE" as INT4) from usvtimv01."CONDITION_SERV" where "NCONDITION" in (71,72,73))
                                                                     and		(clh."DOPERDATE" <= dat.his_fin or clh."DCOMPDATE" <= dat.his_fin) --evita tomar operaciones posteriores (carga incremental)
                                                                     ) xxxxdate,
                                                             dat.his_fin dat_lim
                                             from    (	select	ctid pol_id
                                                                     from	usvtimv01."POLICY"
                                                                     where	"SCERTYPE" = '2') po0
                                             join	(	select	cast('{p_fecha_inicio}' as date) his_ini, --rango inicial carga hist�rica (2017 para pruebas)
                                                                        cast('{p_fecha_fin}' as date) his_fin --rango final carga hist�rica (d�a anterior al rango de la carga inicial)
                                                             ) dat on 1 = 1
                                             join	usvtimv01."POLICY" pol on pol.ctid = po0.pol_id
                                             join	usvtimv01."CLAIM" cla
                                                             on		cla."SCERTYPE" = pol."SCERTYPE"
                                                     and     cla."NPOLICY" = pol."NPOLICY"
                                                     and     cla."NBRANCH" = pol."NBRANCH"
                                                     and     cla."SSTACLAIM" <> '6'
                                                     and		(cast(cla."DCOMPDATE" as date) between dat.his_ini and dat.his_fin
                                                                             or	exists
                                                                     (   select  1
                                                                     from    usvtimv01."CLAIM_HIS" clh
                                                                     where	clh."NCLAIM" = cla."NCLAIM"
                                                                     and		coalesce(clh."NCASE_NUM",0) >= 0
                                                                                             and     (cast(clh."DOPERDATE" as date) between dat.his_ini and dat.his_fin --se consideran siniestros con operaciones entre los rangos (limitaci�n carga inicial)
                                                                                                             or cast(clh."DCOMPDATE" as date) between dat.his_ini and dat.his_fin) --se consideran siniestros con modificaciones entre los rangos (limitaci�n carga inicial)
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
                                                                                     and		cast(clm."DCOMPDATE" as date) between dat.his_ini and dat.his_fin --se consideran coberturas con modificaciones entre los rangos (limitaci�n carga inicial)
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
                                                                                     and     (cast(clh."DOPERDATE" as date) > dat.his_fin --no existan operaciones posteriores al corte
                                                                                                     or cast(clh."DOPERDATE" as date) > dat.his_fin) --no existan modificaciones posteriores al corte
                                                                                     and		coalesce(clh."NAMOUNT",0) <> 0)
                                                             and		not exists
                                                             (   select  1
                                                             from    usvtimv01."CLAIM_HIS" clh
                                                                             join	usvtimv01."CL_M_COVER" clm
                                                                             on		clm."NCLAIM" = clh."NCLAIM"
                                                                             and		clm."NCASE_NUM" = clH."NCASE_NUM"
                                                                             and		clm."NDEMAN_TYPE" = clh."NDEMAN_TYPE"
                                                                             and		clm."NTRANSAC" = clh."NTRANSAC"
                                                                             and		cast(clm."DCOMPDATE" as date) > dat.his_fin --se consideran coberturas con modificaciones entre los rangos (limitaci�n carga inicial)
                                                             where	clh."NCLAIM" = cla."NCLAIM"
                                                             and		coalesce(clh."NCASE_NUM",0) >= 0
                                                                                     and		clh."NOPER_TYPE" in 
                                                                                                     (select	cast("SVALUE" as INT4) from	usvtimv01."CONDITION_SERV" cs  where "NCONDITION" in (71,72,73)))) cl0
                     join	usvtimv01."CLAIM" cla on cla.ctid = cl0.cla_id
                     join	usvtimv01."POLICY" pol on pol.ctid = cl0.pol_id
                     where	cl0.monto_trans <> 0
                    ) AS TMP
                 '''
                            
    DF_VTIME_LPV = glue_context.read.format('jdbc').options(**connection).option("fetchsize",10000).option("dbtable",VTIME_LPV).load()

    INSIS_LPV = f'''
                    (select	
                     'D' INDDETREC,
                     'SBSIN' TABLAIFRS17,
		     '' PK,
		     '' DTPREG, --excluido
		     '' TIOCPROC, --no disponible
		     cast(cl0.xxxxdate as varchar) TIOCFRM,
		     '' TIOCTO, --excluido
		     'PNV' KGIORIGM,
		     'LPV' KSCCOMPA,
		     /*
		     coalesce((	SELECT	DISTINCT ter."RAMCOM"
		     			FROM	usinsiv01."CFGLPV_POLICY_TECHBRANCH_SBS" tbs
		     			JOIN	usinsiv01."CUST_TBL_EQUIV_RAMCOM" ter
		     					ON		ter."INSR_TYPE" = tbs."INSR_TYPE"
		     					AND		ter."AS_IS" = tbs."AS_IS_PRODUCT"
		     					AND		trim(ter."TECH_BRANCH") = cast(tbs."TECHNICAL_BRANCH" as varchar)
		     			WHERE	tbs."INSR_TYPE" = pol."INSR_TYPE"
		     			AND		cast(tbs."TECHNICAL_BRANCH" as varchar) = pol."ATTR1"
		     			AND		tbs."SBS_CODE" = pol."ATTR2"),0)*/ '' KGCRAMO,
		     cast(pol."INSR_TYPE" as varchar) KABPRODT,
		     substring(cast(coalesce(cl0.pep_id,pol."POLICY_ID") as varchar),6) DNUMAPO,
		     coalesce(substring(cast(case when cl0.pep_id is not null then pol."POLICY_ID" else null end as varchar),6),'0') DNMCERT,
		     coalesce(cast(cla."CLAIM_ID" as varchar),'') DNUMSIN,
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
		     					and		cast(clh."CHANGE_DATE" as date) <= cl0.dat_lim
		     			where	clo."CLAIM_ID" = cla."CLAIM_ID"
		     			and		clo."CLAIM_STATE" in (2,3)),'') TFECHTEC,
		     coalesce((	select	cast(cast(max(clh."REGISTRATION_DATE") as date) as varchar)
		     			from 	usinsiv01."CLAIM_OBJECTS" clo
		     			join	usinsiv01."CLAIM_RESERVE_HISTORY" clh
		     					on		clh."CLAIM_ID" = clo."CLAIM_ID"
		     					and		clh."REQUEST_ID" = clo."REQUEST_ID"
		     					and		clh."OP_TYPE" IN ('PAYMCONF','PAYMINV')
		     					and		cast(clh."CHANGE_DATE" as date) <= cl0.dat_lim
		     			where	clo."CLAIM_ID" = cla."CLAIM_ID"
		     			and		clo."CLAIM_STATE" in (2,3)),'') TFECHADM,
		     coalesce((	select	cast(cast(max(clh."REGISTRATION_DATE") as date) as varchar)
		     			from 	usinsiv01."CLAIM_OBJECTS" clo
		     			join	usinsiv01."CLAIM_RESERVE_HISTORY" clh
		     					on		clh."CLAIM_ID" = clo."CLAIM_ID"
		     					and		clh."REQUEST_ID" = clo."REQUEST_ID"
		     					and		clh."OP_TYPE" IN ('REG','EST','CLC','PAYMCONF','PAYMINV')
		     					and		cast(clh."CHANGE_DATE" as date) <= cl0.dat_lim
		     			where	clo."CLAIM_ID" = cla."CLAIM_ID"),'') TESTADO,
		     coalesce((	select	cast(max(clo."CLAIM_STATE") as varchar)
		     			from 	usinsiv01."CLAIM_OBJECTS" clo
		     			join	usinsiv01."CLAIM_RESERVE_HISTORY" clh
		     					on		clh."CLAIM_ID" = clo."CLAIM_ID"
		     					and		clh."REQUEST_ID" = clo."REQUEST_ID"
		     					and		clh."OP_TYPE" IN ('REG','EST','CLC','PAYMCONF','PAYMINV')
		     					and		cast(clh."CHANGE_DATE" as date) <= cl0.dat_lim
		     					and		clh."REGISTRATION_DATE" = 
		     							(	select 	MAX("REGISTRATION_DATE")
		     								from	usinsiv01."CLAIM_RESERVE_HISTORY" ch0
		     								where	ch0."CLAIM_ID" = clo."CLAIM_ID"
		     								and		ch0."REQUEST_ID" = clo."REQUEST_ID"
		     								and		ch0."OP_TYPE" IN ('REG','EST','CLC','PAYMCONF','PAYMINV')
		     								and		cast(clh."CHANGE_DATE" as date) <= cl0.dat_lim)
		     			where	clo."CLAIM_ID" = cla."CLAIM_ID"),'') KSCSITUA,
		     '' KSCMOTSI, --excluido
		     '' KSCTPSIN, --no disponible
		     cast(cla."CAUSE_ID" as varchar) KSCCAUSA, --PENDIENTE AUN (20231031)
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
		     cast(cl0.monto_trans as numeric (14,2)) VMTPROVI,
		     '' VMTPRVINI, --excluido
		     cast(cl0.monto_trans as numeric (14,2)) VMTPRVRS, --no se est� aplicando reaseguros en INSIS
		     cast(cl0.monto_trans as numeric (14,2)) VMTCOSEG, --no hay coaseguros en INSIS
		     '' KSCNATUR, --excluido
		     '' TALTENAT, --excluido
		     '' DUSRREG, --excluido
		     coalesce((	select	lpi."LEGACY_ID"
		     			from	usinsiv01."INTRF_LPV_PEOPLE_IDS" lpi
                     	where	lpi."MAN_ID" = 
		                     	(	select	"MAN_ID"
		                     		from 	usinsiv01."P_CLIENTS" cli 
		                     		where 	cli."CLIENT_ID" = cla."CLIENT_ID")), cast(cla."CLIENT_ID" as VARCHAR)) KEBENTID_TO,
                     cast(coalesce((	select  sum("INSURED_VALUE")
	                         from    usinsiv01."INSURED_OBJECT" obj
	                         where   obj."POLICY_ID" = cla."POLICY_ID"
	                         and     cast(cla."CLAIM_STARTED" as date)
                         	between	cast(obj."INSR_BEGIN" as date) and cast(obj."INSR_END" as date)),0) as float) VCAPITAL,
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
		     coalesce((	select	clo."CLAIM_STATE" || '-' || clo."CLAIM_STATE_AUX"
		     			from	usinsiv01."CLAIM_OBJECTS" clo
		     			where	clo."CLAIM_ID" = cla."CLAIM_ID"
		     			and		clo."CLAIM_OBJ_SEQ" =
		     					(	select	max(cl1."CLAIM_OBJ_SEQ")
		     						from 	usinsiv01."CLAIM_OBJECTS"  cl1
		     						where	cl1."CLAIM_ID" = cla."CLAIM_ID"
		     						and		cl1."CLAIM_STATE" <> -1)),'') KSCESTSN, 
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
		     right(coalesce((	select	lpi."LEGACY_ID"
		     					from	usinsiv01."INTRF_LPV_PEOPLE_IDS" lpi
		                     	where	lpi."MAN_ID" = 
		     		                	(	select	"MAN_ID"
		     		                		from 	usinsiv01."P_CLIENTS" cli 
		     		                		where 	cli."CLIENT_ID" = cla."CLIENT_ID")), cast(cla."CLIENT_ID" as VARCHAR)),10) DUNIDRISC,
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
		     substring(cast(pol."POLICY_ID" as varchar),6) KABAPOL,
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
                                                             max(cla.dat_lim) dat_lim,
                                                             max(cla.xxxxdate) xxxxdate, --valor �nico, se evita hacer group by por este campo
                                                             sum(case when clh."OP_TYPE" in ('PAYMCONF','PAYMINV') then 0 else "RESERV_CHANGE" end) monto_trans
                                             from	(	select	cla."CLAIM_ID",
                                                                                     (select "MASTER_POLICY_ID" from usinsiv01."POLICY_ENG_POLICIES" where "POLICY_ID" = CLA."POLICY_ID") pep_id,
                                                                                     cla.ctid cla_id,
                                                                                     pol.ctid pol_id,
                                                                                     case	coalesce((	select	distinct "AV_CURRENCY"
                                                                                                                             from	usinsiv01."INSURED_OBJECT"
                                                                                                                             where	"POLICY_ID" = pol."POLICY_ID" limit 1),'')
                                                                                                     when 'USD' then 2
                                                                                                     when 'PEN' then 1
                                                                                                     else 0 end moneda_cod,
                                                                                     (	select	max(greatest(
                                                                                                                     case when cast(cla."REGISTRATION_DATE" as date) <= dat.his_fin then cast(cla."REGISTRATION_DATE" as date) else '0001-01-01'::date end,
                                                                                                                     case when cast(clh."REGISTRATION_DATE" as date) <= dat.his_fin then cast(clh."REGISTRATION_DATE" as date) else '0001-01-01'::date end,
                                                                                                                     case when cast(clh."CHANGE_DATE" as date) <= dat.his_fin then cast(clh."CHANGE_DATE" as date) else '0001-01-01'::date end))
                                                                                             from	usinsiv01."CLAIM_RESERVE_HISTORY" clh
                                                                                             join	usinsiv01."CLAIM_OBJECTS" clo
                                                                                                             on		clo."CLAIM_ID" = clh."CLAIM_ID"
                                                                                                             and		clo."CLAIM_OBJ_SEQ" = clh."CLAIM_OBJECT_SEQ"
                                                                                                             and		clo."REQUEST_ID" = clh."REQUEST_ID"
                                                                                                             and		clo."CLAIM_STATE" <> -1
                                                                                             where	clh."CLAIM_ID" = cla."CLAIM_ID"
                                                                                             and		clh."OP_TYPE" IN ('REG','EST','CLC','PAYMCONF','PAYMINV')) xxxxdate,
                                                                                     dat.his_fin dat_lim
                                                                     from	usinsiv01."CLAIM" cla
                                                                     join	(	select	cast('{p_fecha_inicio}' as date) his_ini, --rango inicial carga hist�rica (variable a elecci�n)
                                                                                                cast('{p_fecha_fin}' as date) his_fin --rango final carga hist�rica (variable a elecci�n)
                                                                                                             ) dat on 1 = 1
                                                                     join	usinsiv01."POLICY" pol
                                                                                     on		pol."POLICY_ID" = cla."POLICY_ID"
                                                                                     and		pol."POLICY_STATE" >= 0
                                                                     where	(cast(cla."REGISTRATION_DATE" as date) between dat.his_ini and dat.his_fin
                                                                                     or	exists 
                                                                                             (	select	1 
                                                                                                     from	usinsiv01."CLAIM_RESERVE_HISTORY" clh
                                                                                                     join	usinsiv01."CLAIM_OBJECTS" clo
                                                                                                                     on		clo."CLAIM_ID" = clh."CLAIM_ID"
                                                                                                                     and		clo."CLAIM_OBJ_SEQ" = clh."CLAIM_OBJECT_SEQ"
                                                                                                                     and		clo."REQUEST_ID" = clh."REQUEST_ID"
                                                                                                                     and		clo."CLAIM_STATE" <> -1
                                                                                                     where	clh."CLAIM_ID" = cla."CLAIM_ID"
                                                                                                     and		clh."OP_TYPE" IN ('REG','EST','CLC','PAYMCONF','PAYMINV')
                                                                                                     and		(cast(clh."REGISTRATION_DATE" as date) between dat.his_ini and dat.his_fin --ejecutar al a�o 2021 (2018 es para pruebas)
                                                                                                                     or cast(clh."CHANGE_DATE" as date) between dat.his_ini and dat.his_fin)))
                                                                     and 	not exists 
                                                                                     (	select	1 
                                                                                             from	usinsiv01."CLAIM_RESERVE_HISTORY" clh
                                                                                             join	usinsiv01."CLAIM_OBJECTS" clo
                                                                                                             on		clo."CLAIM_ID" = clh."CLAIM_ID"
                                                                                                             and		clo."CLAIM_OBJ_SEQ" = clh."CLAIM_OBJECT_SEQ"
                                                                                                             and		clo."REQUEST_ID" = clh."REQUEST_ID"
                                                                                                             and		clo."CLAIM_STATE" <> -1
                                                                                             where	clh."CLAIM_ID" = cla."CLAIM_ID"
                                                                                             and		clh."OP_TYPE" IN ('REG','EST','CLC','PAYMCONF','PAYMINV')
                                                                                             and		(cast(clh."REGISTRATION_DATE" as date) > dat.his_fin --no existan operaciones posteriores al corte
                                                                                                             or cast(clh."CHANGE_DATE" as date) > dat.his_fin))) cla --no existan modificaciones posteriores al corte
                                             join	usinsiv01."CLAIM_RESERVE_HISTORY" clh
                                                             on		clh."CLAIM_ID" = cla."CLAIM_ID"
                                                             and		clh."OP_TYPE" IN ('REG','EST','CLC')
                                                             and		(cast(clh."REGISTRATION_DATE" as date) <= cla.dat_lim --evita tomar operaciones posteriores (carga incremental)
                                                                             or cast(clh."CHANGE_DATE" as date) <= cla.dat_lim) --evita tomar transacciones actualizadas posteriormente (carga incremental)
                                             join	usinsiv01."CLAIM_OBJECTS" clo
                                                             on		clo."CLAIM_ID" = clh."CLAIM_ID"
                                                             and		clo."CLAIM_OBJ_SEQ" = clh."CLAIM_OBJECT_SEQ"
                                                             and		clo."REQUEST_ID" = clh."REQUEST_ID"
                                                             and		cast(clh."REGISTRATION_DATE" as date) <= cla.dat_lim
                                                             and		clo."CLAIM_STATE" <> -1
                                             group	by 1,2,3) cl0
                     join usinsiv01."CLAIM" cla on cla.ctid = cl0.cla_id
                     join usinsiv01."POLICY" pol on pol.ctid = cl0.pol_id
                     where cl0.monto_trans <> 0
                    ) AS TMP
                 '''
                            
    DF_INSIS_LPV = glue_context.read.format('jdbc').options(**connection).option("fetchsize",10000).option("dbtable",INSIS_LPV).load()

    L_DF_SBSIN = DF_INSUNIX_LPG.union(DF_INSUNIX_LPV).union(DF_VTIME_LPG).union(DF_VTIME_LPV).union(DF_INSIS_LPV)
    
    return L_DF_SBSIN