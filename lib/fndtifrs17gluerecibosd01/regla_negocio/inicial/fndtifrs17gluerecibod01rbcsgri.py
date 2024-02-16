def get_data(glue_context, connection):

    L_RBCSGRI_INSUNIX_LPG_NEGO1 = '''
                                      (
                                               select 	'D' as INDDETREC,
                                                        'RBCSGRI' as TABLAIFRS17,
                                                        '' PK,
                                                        '' DTPREG,
                                                        '' TIOCPROC,
                                                        coi.compdate TIOCFRM,
                                                        '' TIOCTO,
                                                        'PIG' KGIORIGM,
                                                        clh.claim || '-' || clh.transac KRBRECIN,
                                                        '' DORDCSG,
                                                        case 	when	coalesce(coi.companyc,0) in (1,12)
                                                                then	'1'
                                                                else	'2'end DPLANO,
                                                        cast (coalesce(coi.share,100) as numeric(7,4)) VTXCSGI,
                                                        abs(cast (coalesce  (	case	when    clm.currency = clh.moneda_cod
                                                                                                                                then    clm.amount
                                                                                                                                else	case    when    clh.moneda_cod = 1
                                                                                                                                                then    clm.amount *
                                                                                                                                                        case    when    clm.currency = 2
                                                                                                                                                                then    clh.clh_exchange
                                                                                                                                                                else    1 end
                                                                                                                                                when	clh.moneda_cod = 2
                                                                                                                                                then    clm.amount /
                                                                                                                                                        case    when    clm.currency = 1
                                                                                                                                                                then    clh.clh_exchange
                                                                                                                                                                else	1 end
                                                                                                                                                else    0 end
                                                                                                                                end, 0) * coalesce(coi.share,100) / 100  as numeric (12,2))) VMTCSGI,
                                                                coalesce((	select  gco.covergen || '-' || gco.currency
                                                                                        from    usinsug01.gen_cover gco
                                                                                        where	gco.ctid =
                                                                        coalesce(coalesce(coalesce(coalesce(
                                                                                                                                        (   select  max(ctid)
                                                                                                        from	usinsug01.gen_cover
                                                                                                        where	usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = cla.product and currency = clh.moneda_cod -- ndice regular
                                                                                                        and     modulec = clh.cla_modulec and cover = clm.cover --variaci n 1 con modulec
                                                                                                        and     effecdate <= cla.occurdat and (nulldate is null or nulldate > cla.occurdat) --variaci n 2 vigencias
                                                                                                        and     statregt <> '4'), --variaci n 3 reg. v lido
                                                                                                        (   select  max(ctid)
                                                                                                        from	usinsug01.gen_cover
                                                                                                        where	usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = cla.product and currency = clh.moneda_cod -- ndice regular
                                                                                                        and     cover = clm.cover --variaci n 1 sin modulec
                                                                                                        and     effecdate <= cla.occurdat and (nulldate is null or nulldate > cla.occurdat) --variaci n 2 vigencias
                                                                                                        and     statregt <> '4')), --variaci n 3 reg. v lido
                                                                                                coalesce((	select  max(ctid)
                                                                                                        from	usinsug01.gen_cover
                                                                                                        where	usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = cla.product and currency = clh.moneda_cod -- ndice regular
                                                                                                        and     modulec = clh.cla_modulec and cover = clm.cover
                                                                                                        and     effecdate <= cla.occurdat and (nulldate is null or nulldate > cla.occurdat)
                                                                                                        and     statregt = '4'),
                                                                                                        (	select  max(ctid)
                                                                                                        from	usinsug01.gen_cover
                                                                                                        where	usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = cla.product and currency = clh.moneda_cod -- ndice regular
                                                                                                        and     cover = clm.cover
                                                                                                        and     effecdate <= cla.occurdat and (nulldate is null or nulldate > cla.occurdat)
                                                                                                        and     statregt = '4'))), --no est  cortado
                                                                                        coalesce(coalesce((	select  max(ctid)
                                                                                                        from	usinsug01.gen_cover
                                                                                                        where	usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = cla.product and currency = clh.moneda_cod -- ndice regular
                                                                                                        and     modulec = clh.cla_modulec and cover = clm.cover
                                                                                                        and     effecdate <= cla.occurdat and nulldate <= cla.occurdat
                                                                                                        and     statregt <> '4'),
                                                                                                        (   select  max(ctid)
                                                                                                        from	usinsug01.gen_cover
                                                                                                        where	usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = cla.product and currency = clh.moneda_cod -- ndice regular
                                                                                                        and     cover = clm.cover
                                                                                                        and     effecdate <= cla.occurdat and nulldate <= cla.occurdat
                                                                                                        and     statregt <> '4')), -- no est  cortado, pero fue anulado antes del efecto del registro
                                                                                        coalesce((	select  max(ctid)
                                                                                                        from	usinsug01.gen_cover
                                                                                                        where	usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = cla.product and currency = clh.moneda_cod -- ndice regular
                                                                                                        and     modulec = clh.cla_modulec and cover = clm.cover
                                                                                                        and     effecdate <= cla.occurdat and nulldate <= cla.occurdat
                                                                                                        and     statregt = '4'),
                                                                                                (   select  max(ctid)
                                                                                                        from	usinsug01.gen_cover
                                                                                                        where 	usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = cla.product and currency = clh.moneda_cod -- ndice regular
                                                                                                        and     cover = clm.cover
                                                                                                        and     effecdate <= cla.occurdat and nulldate <= cla.occurdat
                                                                                                        and     statregt = '4')))), --no est  cortado y aparte fue anulado antes del efecto del registro
                                                                                coalesce(coalesce((	select  max(ctid)
                                                                                                from	usinsug01.gen_cover
                                                                                                where 	usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = cla.product and currency = clh.moneda_cod -- ndice regular
                                                                                                and     modulec = clh.cla_modulec and cover = clm.cover
                                                                                                and     effecdate > cla.occurdat
                                                                                                and     statregt <> '4'),
                                                                                                (   select  max(ctid)
                                                                                                from	usinsug01.gen_cover
                                                                                                where	usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = cla.product and currency = clh.moneda_cod -- ndice regular
                                                                                                and     cover = clm.cover
                                                                                                and     effecdate <= cla.occurdat and nulldate <= cla.occurdat
                                                                                                and     statregt <> '4')), -- no est  cortado, pero tampoco al efecto de la tabla de datos particular
                                                                                coalesce((	select  max(ctid)
                                                                                                from	usinsug01.gen_cover
                                                                                                where	usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = cla.product and currency = clh.moneda_cod -- ndice regular
                                                                                                and     modulec = clh.cla_modulec and cover = clm.cover
                                                                                                and     effecdate <= cla.occurdat and nulldate <= cla.occurdat
                                                                                                and     statregt = '4'),
                                                                                        (   select  max(ctid)
                                                                                                from	usinsug01.gen_cover
                                                                                                where	usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = cla.product and currency = clh.moneda_cod -- ndice regular
                                                                                                and     cover = clm.cover
                                                                                                and     effecdate > cla.occurdat
                                                                                                and     statregt = '4'))))), --est  cortado y no al efecto de la tabla de datos particular adem s de estar cortado
                                                                        cast(coalesce(clm.cover,0)* -1 as varchar) || '-' || clh.moneda_cod) KRCTPCBT,
                                                        '' KRCTPNDI, --no disponible
                                                        coalesce('PAE' || '-' || coalesce((select scod_vt from usinsug01.equi_vt_inx where scod_inx = clh.bene_code),clh.bene_code),'') KRBPENS,
                                                        '' KRBRENDA,
                                                        cast (coi.companyc as varchar) DCODCSG,
                                                        '' DORDDSPIN,
                                                        'LPG' DCOMPA,
                                                        '' DMARCA,
                                                        '' DINDDESD,
                                                                case 	when	coalesce(coi.companyc,0) in (1,12)
                                                                                then	'1'
                                                                                else	'2' end KRCTPQTP
                                                from	(	select	cla.cla_id,
                                                                                        cla.usercomp,
                                                                                        cla.company,
                                                                                        cla.claim,
                                                                                        cla.certype,
                                                                                        cla.branch,
                                                                                        cla.policy,
                                                                                        cla.occurdat,
                                                                                        cla.moneda_cod,
                                                                                        cla.cla_modulec,
                                                                                        clh.transac,
                                                                                        clh.oper_type,
                                                                                        clh.operdate,
                                                                                        clh.bene_code,
                                                                                        coalesce(clh.amount,0) amount,
                                                                case    when    moneda_cod = 1
                                                                        then    case    when    clh.currency = 2
                                                                                        then    (   select  max(clh0.exchange)
                                                                                                        from    usinsug01.claim_his clh0
                                                                                                        where	clh0.claim = clh.claim
                                                                                                        and     clh0.transac =
                                                                                                                (   select  max(clh1.transac)
                                                                                                                from    usinsug01.claim_his clh1
                                                                                                                where	clh1.claim = clh.claim
                                                                                                                and     clh1.transac <= clh.transac
                                                                                                                and     clh1.exchange not in (1,0)))
                                                                                        else 1 end
                                                                        when 	moneda_cod = 2
                                                                        then    case    when    clh.currency = 1
                                                                                        then    (   select  max(clh0.exchange)
                                                                                                        from    usinsug01.claim_his clh0
                                                                                                        where 	clh0.claim = clh.claim
                                                                                                        and     clh0.transac =
                                                                                                                (   select  max(clh1.transac)
                                                                                                                from    usinsug01.claim_his clh1
                                                                                                                where	clh1.claim = clh.claim
                                                                                                                and     clh1.transac <= clh.transac
                                                                                                                and     clh1.exchange not in (1,0)))
                                                                                        else    1 end
                                                                        else    0 end clh_exchange
                                                                from    (   select	case	when	cla.staclaim <> '6' or (cla.branch = 23 and cla.staclaim = '6')
                                                                                                                                then	1
                                                                                                                                else	0 end flag_reg,
                                                                                                                cla.claim,
                                                                                cla.usercomp,
                                                                                cla.company,
                                                                                pol.certype,
                                                                                cla.branch,
                                                                                cla.policy,
                                                                                cla.occurdat,
                                                                                cla.ctid cla_id,
                                                                                coalesce(   coalesce((  select  max(cpl.currency)
                                                                                                        from    usinsug01.curren_pol cpl
                                                                                                        where 	cpl.usercomp = cla.usercomp and cpl.company = cla.company and cpl.certype = pol.certype
                                                                                                        and     cpl.branch = cla.branch and cpl.policy = cla.policy and cpl.certif = cla.certif),
                                                                                                (   select  max(cpl.currency)
                                                                                                        from    usinsug01.curren_pol cpl
                                                                                                        where 	cpl.usercomp = cla.usercomp and cpl.company = cla.company and cpl.certype = pol.certype
                                                                                                        and     cpl.branch = cla.branch and cpl.policy = cla.policy)),0) moneda_cod,
                                                                                case	when	cla.branch <> 23
                                                                                                                                then	coalesce((	select	max(coalesce(modulec,0))
                                                                                                                                                                        from	usinsug01.modules
                                                                                                                                                                        where	usercomp = cla.usercomp
                                                                                                                                                                        and		company = cla.company
                                                                                                                                                                        and		certype = pol.certype
                                                                                                                                                                        and		branch = cla.branch
                                                                                                                                                                        and		policy = cla.policy
                                                                                                                                                                        and		certif = cla.certif
                                                                                                                                                                        and		effecdate <= cla.occurdat
                                                                                                                                                                        and		(nulldate is null or nulldate > cla.occurdat)),0)
                                                                                                                                else	(	select	tariff
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
                                                                                                                                end cla_modulec
                                                                                                from    (	select	pol.ctid pol_id
                                                                                                                        from	usinsug01.policy pol
                                                                                                                        where	usercomp = 1
                                                                                                                        and		company = 1
                                                                                                                        and		certype = '2'
                                                                                                                        and		branch in (select branch from usinsug01.table10b where company = 1)
                                                                                                                        and		bussityp = '1'
                                                                                                                        and		exists
                                                                                                                                        (	select 	1
                                                                                                                                                from	usinsug01.coinsuran coi
                                                                                                                                                where	coi.usercomp = pol.usercomp
                                                                                                                                                and     coi.company = pol.company
                                                                                                                                                and     coi.certype = pol.certype
                                                                                                                                                and     coi.branch = pol.branch
                                                                                                                                                and     coi.policy = pol.policy)) po0
                                                                                                join	usinsug01.policy pol on pol.ctid = po0.pol_id
                                                                                                join	usinsug01.claim cla
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
                                                                                                                                                                                        where	reserve = 1 or ajustes = 1 or pay_amount = 1)) tcl
                                                                                                                                        join	usinsug01.claim_his clh on trim(clh.oper_type) = tcl.operation
                                                                                                                                        where   clh.claim = cla.claim
                                                                                                                                        and     clh.operdate >= '12/31/2018')) cla --ejecutar al a o 2021 (2018 es para pruebas)
                                                                join	usinsug01.claim_his clh
                                                                                on		clh.claim = cla.claim
                                                                                        and		trim(clh.oper_type) in (select cast(operation as varchar(2)) from usinsug01.tab_cl_ope where pay_amount = 1)
                                                                                and     clh.operdate <= '12/31/2023'
                                                                where	cla.flag_reg = 1) clh --en caso de ejecutar a una fecha posterior, alterar esta fecha a la de la ejecuci n
                                                join	usinsug01.claim cla on cla.ctid = clh.cla_id
                                                join 	usinsug01.cl_m_cover clm
                                                                on 		clm.usercomp = clh.usercomp
                                                        and		clm.company = clh.company
                                                        and		clm.claim = clh.claim
                                                        and 	clm.movement = clh.transac
                                                join 	usinsug01.coinsuran coi
                                                                on 	coi.usercomp = clh.usercomp 
                                                        and     coi.company = clh.company
                                                        and     coi.certype = clh.certype
                                                        and     coi.branch = clh.branch
                                                        and     coi.policy = clh.policy
                                                        and     coi.effecdate <= clh.occurdat
                                                        and     (coi.nulldate is null or coi.nulldate > clh.occurdat)
                                                where	coalesce(clh.amount,0) <> 0
                                      ) AS TMP  
                                      '''
    
    DF_LPG_INSUNIX_NEGO1 = glue_context.read.format('jdbc').options(**connection).option("fetchsize",10000).option("dbtable",L_RBCSGRI_INSUNIX_LPG_NEGO1).load()
    
    L_RBCSGRI_INSUNIX_LPG_NEGO2 = '''
                                      (
                                               select 	'D' as INDDETREC,
                                                        'RBCSGRI' as TABLAIFRS17,
                                                        '' PK,
                                                        '' DTPREG,
                                                        '' TIOCPROC,
                                                        clh.pol_compdate TIOCFRM,
                                                        '' TIOCTO,
                                                        'PIG' KGIORIGM,
                                                        clh.claim || '-' || clh.transac KRBRECIN,
                                                        '' DORDCSG,
                                                        '2' DPLANO,
                                                        cast(100 - coalesce(clh.leadshare) as numeric (7,4)) VTXCSGI,
                                                        abs(cast (coalesce  (	case	when    clm.currency = clh.moneda_cod
                                                                                                                                then    clm.amount
                                                                                                                                else	case    when    clh.moneda_cod = 1
                                                                                                                                                then    clm.amount *
                                                                                                                                                        case    when    clm.currency = 2
                                                                                                                                                                then    clh.clh_exchange
                                                                                                                                                                else    1 end
                                                                                                                                                when	clh.moneda_cod = 2
                                                                                                                                                then    clm.amount /
                                                                                                                                                        case    when    clm.currency = 1
                                                                                                                                                                then    clh.clh_exchange
                                                                                                                                                                else	1 end
                                                                                                                                                else    0 end
                                                                                                                                end, 0) as numeric (12,2))) VMTCSGI,
                                                                coalesce((	select  gco.covergen || '-' || gco.currency
                                                                                        from    usinsug01.gen_cover gco
                                                                                        where	gco.ctid =
                                                                        coalesce(coalesce(coalesce(coalesce(
                                                                                                                                        (   select  max(ctid)
                                                                                                        from	usinsug01.gen_cover
                                                                                                        where	usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = cla.product and currency = clh.moneda_cod -- ndice regular
                                                                                                        and     modulec = clh.cla_modulec and cover = clm.cover --variaci n 1 con modulec
                                                                                                        and     effecdate <= cla.occurdat and (nulldate is null or nulldate > cla.occurdat) --variaci n 2 vigencias
                                                                                                        and     statregt <> '4'), --variaci n 3 reg. v lido
                                                                                                        (   select  max(ctid)
                                                                                                        from	usinsug01.gen_cover
                                                                                                        where	usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = cla.product and currency = clh.moneda_cod -- ndice regular
                                                                                                        and     cover = clm.cover --variaci n 1 sin modulec
                                                                                                        and     effecdate <= cla.occurdat and (nulldate is null or nulldate > cla.occurdat) --variaci n 2 vigencias
                                                                                                        and     statregt <> '4')), --variaci n 3 reg. v lido
                                                                                                coalesce((	select  max(ctid)
                                                                                                        from	usinsug01.gen_cover
                                                                                                        where	usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = cla.product and currency = clh.moneda_cod -- ndice regular
                                                                                                        and     modulec = clh.cla_modulec and cover = clm.cover
                                                                                                        and     effecdate <= cla.occurdat and (nulldate is null or nulldate > cla.occurdat)
                                                                                                        and     statregt = '4'),
                                                                                                        (	select  max(ctid)
                                                                                                        from	usinsug01.gen_cover
                                                                                                        where	usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = cla.product and currency = clh.moneda_cod -- ndice regular
                                                                                                        and     cover = clm.cover
                                                                                                        and     effecdate <= cla.occurdat and (nulldate is null or nulldate > cla.occurdat)
                                                                                                        and     statregt = '4'))), --no est  cortado
                                                                                        coalesce(coalesce((	select  max(ctid)
                                                                                                        from	usinsug01.gen_cover
                                                                                                        where	usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = cla.product and currency = clh.moneda_cod -- ndice regular
                                                                                                        and     modulec = clh.cla_modulec and cover = clm.cover
                                                                                                        and     effecdate <= cla.occurdat and nulldate <= cla.occurdat
                                                                                                        and     statregt <> '4'),
                                                                                                        (   select  max(ctid)
                                                                                                        from	usinsug01.gen_cover
                                                                                                        where	usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = cla.product and currency = clh.moneda_cod -- ndice regular
                                                                                                        and     cover = clm.cover
                                                                                                        and     effecdate <= cla.occurdat and nulldate <= cla.occurdat
                                                                                                        and     statregt <> '4')), -- no est  cortado, pero fue anulado antes del efecto del registro
                                                                                        coalesce((	select  max(ctid)
                                                                                                        from	usinsug01.gen_cover
                                                                                                        where	usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = cla.product and currency = clh.moneda_cod -- ndice regular
                                                                                                        and     modulec = clh.cla_modulec and cover = clm.cover
                                                                                                        and     effecdate <= cla.occurdat and nulldate <= cla.occurdat
                                                                                                        and     statregt = '4'),
                                                                                                (   select  max(ctid)
                                                                                                        from	usinsug01.gen_cover
                                                                                                        where 	usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = cla.product and currency = clh.moneda_cod -- ndice regular
                                                                                                        and     cover = clm.cover
                                                                                                        and     effecdate <= cla.occurdat and nulldate <= cla.occurdat
                                                                                                        and     statregt = '4')))), --no est  cortado y aparte fue anulado antes del efecto del registro
                                                                                coalesce(coalesce((	select  max(ctid)
                                                                                                from	usinsug01.gen_cover
                                                                                                where 	usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = cla.product and currency = clh.moneda_cod -- ndice regular
                                                                                                and     modulec = clh.cla_modulec and cover = clm.cover
                                                                                                and     effecdate > cla.occurdat
                                                                                                and     statregt <> '4'),
                                                                                                (   select  max(ctid)
                                                                                                from	usinsug01.gen_cover
                                                                                                where	usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = cla.product and currency = clh.moneda_cod -- ndice regular
                                                                                                and     cover = clm.cover
                                                                                                and     effecdate <= cla.occurdat and nulldate <= cla.occurdat
                                                                                                and     statregt <> '4')), -- no est  cortado, pero tampoco al efecto de la tabla de datos particular
                                                                                coalesce((	select  max(ctid)
                                                                                                from	usinsug01.gen_cover
                                                                                                where	usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = cla.product and currency = clh.moneda_cod -- ndice regular
                                                                                                and     modulec = clh.cla_modulec and cover = clm.cover
                                                                                                and     effecdate <= cla.occurdat and nulldate <= cla.occurdat
                                                                                                and     statregt = '4'),
                                                                                        (   select  max(ctid)
                                                                                                from	usinsug01.gen_cover
                                                                                                where	usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = cla.product and currency = clh.moneda_cod -- ndice regular
                                                                                                and     cover = clm.cover
                                                                                                and     effecdate > cla.occurdat
                                                                                                and     statregt = '4'))))), --est  cortado y no al efecto de la tabla de datos particular adem s de estar cortado
                                                                        cast(coalesce(clm.cover,0)* -1 as varchar) || '-' || clh.moneda_cod) KRCTPCBT,
                                                        '' KRCTPNDI, --no disponible
                                                        coalesce('PAE' || '-' || coalesce((select scod_vt from usinsug01.equi_vt_inx where scod_inx = clh.bene_code),clh.bene_code),'') KRBPENS,
                                                        '' KRBRENDA,
                                                        '1' DCODCSG,
                                                        '' DORDDSPIN,
                                                        'LPG' DCOMPA,
                                                        '' DMARCA,
                                                        '' DINDDESD,
                                                                '1' KRCTPQTP
                                                from	(	select	cla.cla_id,
                                                                                        cla.usercomp,
                                                                                        cla.company,
                                                                                        cla.claim,
                                                                                        cla.certype,
                                                                                        cla.pol_compdate,
                                                                                        cla.leadshare,
                                                                                        cla.branch,
                                                                                        cla.policy,
                                                                                        cla.occurdat,
                                                                                        cla.moneda_cod,
                                                                                        cla.cla_modulec,
                                                                                        clh.transac,
                                                                                        clh.oper_type,
                                                                                        clh.operdate,
                                                                                        clh.bene_code,
                                                                                        coalesce(clh.amount,0) amount,
                                                                case    when    moneda_cod = 1
                                                                        then    case    when    clh.currency = 2
                                                                                        then    (   select  max(clh0.exchange)
                                                                                                        from    usinsug01.claim_his clh0
                                                                                                        where	clh0.claim = clh.claim
                                                                                                        and     clh0.transac =
                                                                                                                (   select  max(clh1.transac)
                                                                                                                from    usinsug01.claim_his clh1
                                                                                                                where	clh1.claim = clh.claim
                                                                                                                and     clh1.transac <= clh.transac
                                                                                                                and     clh1.exchange not in (1,0)))
                                                                                        else 1 end
                                                                        when 	moneda_cod = 2
                                                                        then    case    when    clh.currency = 1
                                                                                        then    (   select  max(clh0.exchange)
                                                                                                        from    usinsug01.claim_his clh0
                                                                                                        where 	clh0.claim = clh.claim
                                                                                                        and     clh0.transac =
                                                                                                                (   select  max(clh1.transac)
                                                                                                                from    usinsug01.claim_his clh1
                                                                                                                where	clh1.claim = clh.claim
                                                                                                                and     clh1.transac <= clh.transac
                                                                                                                and     clh1.exchange not in (1,0)))
                                                                                        else    1 end
                                                                        else    0 end clh_exchange
                                                                from    (   select	case	when	cla.staclaim <> '6' or (cla.branch = 23 and cla.staclaim = '6')
                                                                                                                                then	1
                                                                                                                                else	0 end flag_reg,
                                                                                                                cla.claim,
                                                                                cla.usercomp,
                                                                                cla.company,
                                                                                pol.certype,
                                                                                pol.compdate pol_compdate,
                                                                                pol.leadshare,
                                                                                cla.branch,
                                                                                cla.policy,
                                                                                cla.occurdat,
                                                                                cla.ctid cla_id,
                                                                                coalesce(   coalesce((  select  max(cpl.currency)
                                                                                                        from    usinsug01.curren_pol cpl
                                                                                                        where 	cpl.usercomp = cla.usercomp and cpl.company = cla.company and cpl.certype = pol.certype
                                                                                                        and     cpl.branch = cla.branch and cpl.policy = cla.policy and cpl.certif = cla.certif),
                                                                                                (   select  max(cpl.currency)
                                                                                                        from    usinsug01.curren_pol cpl
                                                                                                        where 	cpl.usercomp = cla.usercomp and cpl.company = cla.company and cpl.certype = pol.certype
                                                                                                        and     cpl.branch = cla.branch and cpl.policy = cla.policy)),0) moneda_cod,
                                                                                case	when	cla.branch <> 23
                                                                                                                                then	coalesce((	select	max(coalesce(modulec,0))
                                                                                                                                                                        from	usinsug01.modules
                                                                                                                                                                        where	usercomp = cla.usercomp
                                                                                                                                                                        and		company = cla.company
                                                                                                                                                                        and		certype = pol.certype
                                                                                                                                                                        and		branch = cla.branch
                                                                                                                                                                        and		policy = cla.policy
                                                                                                                                                                        and		certif = cla.certif
                                                                                                                                                                        and		effecdate <= cla.occurdat
                                                                                                                                                                        and		(nulldate is null or nulldate > cla.occurdat)),0)
                                                                                                                                else	(	select	tariff
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
                                                                                                                                end cla_modulec
                                                                                                from    (	select	pol.ctid pol_id
                                                                                                                        from	usinsug01.policy pol
                                                                                                                        where	usercomp = 1
                                                                                                                        and		company = 1
                                                                                                                        and		certype = '2'
                                                                                                                        and		branch in (select branch from usinsug01.table10b where company = 1)
                                                                                                                        and		bussityp = '2') po0
                                                                                                join	usinsug01.policy pol on pol.ctid = po0.pol_id
                                                                                                join	usinsug01.claim cla
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
                                                                                                                                                                                        where	reserve = 1 or ajustes = 1 or pay_amount = 1)) tcl
                                                                                                                                        join	usinsug01.claim_his clh on trim(clh.oper_type) = tcl.operation
                                                                                                                                        where   clh.claim = cla.claim
                                                                                                                                        and     clh.operdate >= '12/31/2018')) cla --ejecutar al a o 2021 (2018 es para pruebas)
                                                                join	usinsug01.claim_his clh
                                                                                on		clh.claim = cla.claim
                                                                                        and		trim(clh.oper_type) in (select cast(operation as varchar(2)) from usinsug01.tab_cl_ope where pay_amount = 1)
                                                                                and     clh.operdate <= '12/31/2023'
                                                                where	cla.flag_reg = 1) clh --en caso de ejecutar a una fecha posterior, alterar esta fecha a la de la ejecuci n
                                                join	usinsug01.claim cla on cla.ctid = clh.cla_id
                                                join 	usinsug01.cl_m_cover clm
                                                                on 		clm.usercomp = clh.usercomp
                                                        and		clm.company = clh.company
                                                        and		clm.claim = clh.claim
                                                        and 	clm.movement = clh.transac
                                                where	coalesce(clh.amount,0) <> 0
                                      ) AS TMP  
                                      '''
    
    DF_LPG_INSUNIX_NEGO2 = glue_context.read.format('jdbc').options(**connection).option("fetchsize",10000).option("dbtable",L_RBCSGRI_INSUNIX_LPG_NEGO2).load()
    
    L_RBCSGRI_INSUNIX_LPV_NEGO1 = '''
                                      (
                                                select 	'D' as INDDETREC,
                                                        'RBCSGRI' as TABLAIFRS17,
                                                        '' PK,
                                                        '' DTPREG,
                                                        '' TIOCPROC,
                                                        coi.compdate TIOCFRM,
                                                        '' TIOCTO,
                                                        'PIV' KGIORIGM,
                                                        clh.claim || '-' || clh.transac KRBRECIN,
                                                        '' DORDCSG,
                                                        case 	when	coalesce(coi.companyc,0) in (1,12)
                                                                then	'1'
                                                                else	'2'end DPLANO,
                                                        cast (coalesce(coi.share,100) as numeric(7,4)) VTXCSGI,
                                                        abs(cast (coalesce  (	case	when    clm.currency = clh.moneda_cod
                                                                                                                                then    clm.amount
                                                                                                                                else	case    when    clh.moneda_cod = 1
                                                                                                                                                then    clm.amount *
                                                                                                                                                        case    when    clm.currency = 2
                                                                                                                                                                then    clh.clh_exchange
                                                                                                                                                                else    1 end
                                                                                                                                                when	clh.moneda_cod = 2
                                                                                                                                                then    clm.amount /
                                                                                                                                                        case    when    clm.currency = 1
                                                                                                                                                                then    clh.clh_exchange
                                                                                                                                                                else	1 end
                                                                                                                                                else    0 end
                                                                                                                                end, 0) * coalesce(coi.share,100) / 100  as numeric (12,2))) VMTCSGI,
                                                                coalesce(	case    when    coalesce((  select  distinct pro.brancht
                                                                                                                                                from    usinsuv01.product pro
                                                                                                                                                where	pro.usercomp = cla.usercomp
                                                                                                                                                and		pro.company = cla.company
                                                                                                                                                and		pro.branch = cla.branch
                                                                                                                                                and		pro.product = clh.pol_product
                                                                                                                                                and		pro.effecdate <= cla.occurdat
                                                                                                                                                and		(pro.nulldate is null or pro.nulldate > cla.occurdat)),'0') not in ('1','5')
                                                                                                        then	coalesce((	select  gco.covergen || '-' || gco.currency || '-' || 'g'
                                                                                                                                                from    usinsuv01.gen_cover gco
                                                                                                                                                where	gco.ctid =
                                                                                                                                coalesce(coalesce(coalesce(coalesce(
                                                                                                                                                                                                (   select  max(ctid)
                                                                                                                                                                from	usinsuv01.gen_cover gco
                                                                                                                                                                where	gco.usercomp = cla.usercomp and gco.company = cla.company and gco.branch = cla.branch and gco.product = clh.pol_product and gco.currency = clh.moneda_cod -- ndice regular
                                                                                                                                                                and     gco.modulec = clh.cla_modulec and gco.cover = clm.cover --variaci n 1 con modulec
                                                                                                                                                                and     gco.effecdate <= cla.occurdat and (gco.nulldate is null or gco.nulldate > cla.occurdat) --variaci n 2 vigencias
                                                                                                                                                                and     gco.statregt <> '4'), --variaci n 3 reg. v lido
                                                                                                                                                                (   select  max(ctid)
                                                                                                                                                                from	usinsuv01.gen_cover gco
                                                                                                                                                                where	gco.usercomp = cla.usercomp and gco.company = cla.company and gco.branch = cla.branch and gco.product = clh.pol_product and gco.currency = clh.moneda_cod -- ndice regular
                                                                                                                                                                and     gco.cover = clm.cover --variaci n 1 sin modulec
                                                                                                                                                                and     gco.effecdate <= cla.occurdat and (gco.nulldate is null or gco.nulldate > cla.occurdat) --variaci n 2 vigencias
                                                                                                                                                                and     gco.statregt <> '4')), --variaci n 3 reg. v lido
                                                                                                                                                        coalesce((	select  max(ctid)
                                                                                                                                                                from	usinsuv01.gen_cover gco
                                                                                                                                                                where	gco.usercomp = cla.usercomp and gco.company = cla.company and gco.branch = cla.branch and gco.product = clh.pol_product and gco.currency = clh.moneda_cod -- ndice regular
                                                                                                                                                                and     gco.modulec = clh.cla_modulec and gco.cover = clm.cover
                                                                                                                                                                and     gco.effecdate <= cla.occurdat and (gco.nulldate is null or gco.nulldate > cla.occurdat)
                                                                                                                                                                and     gco.statregt = '4'),
                                                                                                                                                                (	select  max(ctid)
                                                                                                                                                                from	usinsuv01.gen_cover gco
                                                                                                                                                                where	gco.usercomp = cla.usercomp and gco.company = cla.company and gco.branch = cla.branch and gco.product = clh.pol_product and gco.currency = clh.moneda_cod -- ndice regular
                                                                                                                                                                and     gco.cover = clm.cover
                                                                                                                                                                and     gco.effecdate <= cla.occurdat and (gco.nulldate is null or gco.nulldate > cla.occurdat)
                                                                                                                                                                and     gco.statregt = '4'))), --no est  cortado
                                                                                                                                                coalesce(coalesce((	select  max(ctid)
                                                                                                                                                                from	usinsuv01.gen_cover gco
                                                                                                                                                                where	gco.usercomp = cla.usercomp and gco.company = cla.company and gco.branch = cla.branch and gco.product = clh.pol_product and gco.currency = clh.moneda_cod -- ndice regular
                                                                                                                                                                and     gco.modulec = clh.cla_modulec and gco.cover = clm.cover
                                                                                                                                                                and     gco.effecdate <= cla.occurdat and gco.nulldate <= cla.occurdat
                                                                                                                                                                and     gco.statregt <> '4'),
                                                                                                                                                                (   select  max(ctid)
                                                                                                                                                                from	usinsuv01.gen_cover gco
                                                                                                                                                                where	gco.usercomp = cla.usercomp and gco.company = cla.company and gco.branch = cla.branch and gco.product = clh.pol_product and gco.currency = clh.moneda_cod -- ndice regular
                                                                                                                                                                and     gco.cover = clm.cover
                                                                                                                                                                and     gco.effecdate <= cla.occurdat and gco.nulldate <= cla.occurdat
                                                                                                                                                                and     gco.statregt <> '4')), -- no est  cortado, pero fue anulado antes del efecto del registro
                                                                                                                                                coalesce((	select  max(ctid)
                                                                                                                                                                from	usinsuv01.gen_cover gco
                                                                                                                                                                where	gco.usercomp = cla.usercomp and gco.company = cla.company and gco.branch = cla.branch and gco.product = clh.pol_product and gco.currency = clh.moneda_cod -- ndice regular
                                                                                                                                                                and     gco.modulec = clh.cla_modulec and gco.cover = clm.cover
                                                                                                                                                                and     gco.effecdate <= cla.occurdat and gco.nulldate <= cla.occurdat
                                                                                                                                                                and     gco.statregt = '4'),
                                                                                                                                                        (   select  max(ctid)
                                                                                                                                                                from	usinsuv01.gen_cover gco
                                                                                                                                                                where	gco.usercomp = cla.usercomp and gco.company = cla.company and gco.branch = cla.branch and gco.product = clh.pol_product and gco.currency = clh.moneda_cod -- ndice regular
                                                                                                                                                                and     gco.cover = clm.cover
                                                                                                                                                                and     gco.effecdate <= cla.occurdat and gco.nulldate <= cla.occurdat
                                                                                                                                                                and     statregt = '4')))), --no est  cortado y aparte fue anulado antes del efecto del registro
                                                                                                                                        coalesce(coalesce((	select  max(ctid)
                                                                                                                                                        from	usinsuv01.gen_cover gco
                                                                                                                                                        where	gco.usercomp = cla.usercomp and gco.company = cla.company and gco.branch = cla.branch and gco.product = clh.pol_product and gco.currency = clh.moneda_cod -- ndice regular
                                                                                                                                                        and     gco.modulec = clh.cla_modulec and gco.cover = clm.cover
                                                                                                                                                        and     gco.effecdate > cla.occurdat
                                                                                                                                                        and     gco.statregt <> '4'),
                                                                                                                                                        (   select  max(ctid)
                                                                                                                                                        from	usinsuv01.gen_cover gco
                                                                                                                                                        where	gco.usercomp = cla.usercomp and gco.company = cla.company and gco.branch = cla.branch and gco.product = clh.pol_product and gco.currency = clh.moneda_cod -- ndice regular
                                                                                                                                                        and     gco.cover = clm.cover
                                                                                                                                                        and     gco.effecdate <= cla.occurdat and gco.nulldate <= cla.occurdat
                                                                                                                                                        and     gco.statregt <> '4')), -- no est  cortado, pero tampoco al efecto de la tabla de datos particular
                                                                                                                                        coalesce((	select  max(ctid)
                                                                                                                                                        from	usinsuv01.gen_cover gco
                                                                                                                                                        where	gco.usercomp = cla.usercomp and gco.company = cla.company and gco.branch = cla.branch and gco.product = clh.pol_product and gco.currency = clh.moneda_cod -- ndice regular
                                                                                                                                                        and     gco.modulec = clh.cla_modulec and gco.cover = clm.cover
                                                                                                                                                        and     gco.effecdate <= cla.occurdat and gco.nulldate <= cla.occurdat
                                                                                                                                                        and     gco.statregt = '4'),
                                                                                                                                                (   select  max(ctid)
                                                                                                                                                        from	usinsuv01.gen_cover gco
                                                                                                                                                        where	gco.usercomp = cla.usercomp and gco.company = cla.company and gco.branch = cla.branch and gco.product = clh.pol_product and gco.currency = clh.moneda_cod -- ndice regular
                                                                                                                                                        and     gco.cover = clm.cover
                                                                                                                                                        and     gco.effecdate > cla.occurdat
                                                                                                                                                        and     gco.statregt = '4'))))), --est  cortado y no al efecto de la tabla de datos particular adem s de estar cortado
                                                                                                                        cast(coalesce(clm.cover,0)* -1 as varchar) || '-' || clh.moneda_cod || '-' || 'g')
                                                                                                        else    coalesce((   select  gco.covergen || '-' || gco.currency || '-' || 'l'
                                                                                                                                        from    usinsuv01.life_cover gco
                                                                                                                                        where	gco.ctid =
                                                                                                                                                        coalesce(coalesce(coalesce(
                                                                                                                                                                                                (	select  max(ctid)
                                                                                                                                                                                                        from 	usinsuv01.life_cover
                                                                                                                                                                                                        where   usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = clh.pol_product and currency = clh.moneda_cod -- ndice regular
                                                                                                                                                                                                        and		cover = clm.cover
                                                                                                                                                                                                        and		effecdate <= cla.occurdat and (nulldate is null or nulldate > cla.occurdat)
                                                                                                                                                                                                        and		statregt <> '4' ), --que no est  cortado
                                                                                                                                                                                                (   select  max(ctid)
                                                                                                                                                                                                        from 	usinsuv01.life_cover
                                                                                                                                                                                                        where   usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = clh.pol_product and currency = clh.moneda_cod -- ndice regular
                                                                                                                                                                                                        and		cover = clm.cover
                                                                                                                                                                                                        and		effecdate <= cla.occurdat and (nulldate is null or nulldate > cla.occurdat)
                                                                                                                                                                                                        and		statregt = '4' )),--est  cortado
                                                                                                                                                                                coalesce((  select  max(ctid)
                                                                                                                                                                                                        from 	usinsuv01.life_cover
                                                                                                                                                                                                        where   usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = clh.pol_product and currency = clh.moneda_cod -- ndice regular
                                                                                                                                                                                                        and		cover = clm.cover
                                                                                                                                                                                                        and		effecdate <= cla.occurdat and nulldate <= cla.occurdat
                                                                                                                                                                                                        and		statregt <> '4'),--no est  cortado pero fue anulado antes del efecto del registro
                                                                                                                                                                                                (   select  max(ctid)
                                                                                                                                                                                                        from 	usinsuv01.life_cover
                                                                                                                                                                                                        where   usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = clh.pol_product and currency = clh.moneda_cod -- ndice regular
                                                                                                                                                                                                        and		cover = clm.cover
                                                                                                                                                                                                        and		effecdate <= cla.occurdat and nulldate <= cla.occurdat
                                                                                                                                                                                                        and		statregt = '4'))), --est  cortado y aparte fue anulado antes del efecto del registro
                                                                                                                                                                coalesce((  select  max(ctid)
                                                                                                                                                                                        from 	usinsuv01.life_cover
                                                                                                                                                                                        where   usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = clh.pol_product and currency = clh.moneda_cod -- ndice regular
                                                                                                                                                                                        and		cover = clm.cover
                                                                                                                                                                                        and		effecdate > cla.occurdat
                                                                                                                                                                                        and		statregt <> '4'),
                                                                                                                                                                        (   select  max(ctid)
                                                                                                                                                                                from 	usinsuv01.life_cover
                                                                                                                                                                                where   usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = clh.pol_product and currency = clh.moneda_cod -- ndice regular
                                                                                                                                                                                and		cover = clm.cover
                                                                                                                                                                                and		effecdate > cla.occurdat --est  cortado pero no al efecto de la tabla de datos particular
                                                                                                                                                                        and		statregt = '4')))),
                                                                                                                                cast(coalesce(clm.cover,0)* -1 as varchar) || '-' || clh.moneda_cod || '-' || 'l')
                                                                                                end, 
                                                                                                cast(coalesce(clm.cover,0)* -1 as varchar) || '-' || clh.moneda_cod || '-' ||
                                                                                        case    when    coalesce((  select  distinct pro.brancht --, select * from usinsug01.table37
                                                                                                                                                from    usinsuv01.product pro
                                                                                                                                                where	pro.usercomp = cla.usercomp
                                                                                                                                                and		pro.company = cla.company
                                                                                                                                                and		pro.branch = cla.branch
                                                                                                                                                and		pro.product = clh.pol_product
                                                                                                                                                and		pro.effecdate <= cla.occurdat
                                                                                                                                                and		(pro.nulldate is null or pro.nulldate > cla.occurdat)),'0') not in ('1','5')
                                                                                                        then	'g'
                                                                                                        else	'l' end) KRCTPCBT,
                                                        '' KRCTPNDI, --no disponible
                                                        coalesce('PAE' || '-' || coalesce((select scod_vt from usinsug01.equi_vt_inx where scod_inx = clh.bene_code),clh.bene_code),'') KRBPENS,
                                                        '' KRBRENDA,
                                                        cast (coi.companyc as varchar) DCODCSG,
                                                        '' DORDDSPIN,
                                                        'LPV' DCOMPA,
                                                        '' DMARCA,
                                                        '' DINDDESD,
                                                                case 	when	coalesce(coi.companyc,0) in (1,12)
                                                                                then	'1'
                                                                                else	'2' end KRCTPQTP
                                                from	(	select	cla.cla_id,
                                                                                        cla.usercomp,
                                                                                        cla.company,
                                                                                        cla.claim,
                                                                                        cla.certype,
                                                                                        cla.branch,
                                                                                        cla.policy,
                                                                                        cla.occurdat,
                                                                                        cla.moneda_cod,
                                                                                        cla.pol_product,
                                                                                        cla.cla_modulec,
                                                                                        clh.transac,
                                                                                        clh.oper_type,
                                                                                        clh.operdate,
                                                                                        clh.bene_code,
                                                                                        coalesce(clh.amount,0) amount,
                                                                case    when    moneda_cod = 1
                                                                        then    case    when    clh.currency = 2
                                                                                        then    (   select  max(clh0.exchange)
                                                                                                        from    usinsuv01.claim_his clh0
                                                                                                        where	clh0.claim = clh.claim
                                                                                                        and     clh0.transac =
                                                                                                                (   select  max(clh1.transac)
                                                                                                                from    usinsuv01.claim_his clh1
                                                                                                                where	clh1.claim = clh.claim
                                                                                                                and     clh1.transac <= clh.transac
                                                                                                                and     clh1.exchange not in (1,0)))
                                                                                        else 1 end
                                                                        when 	moneda_cod = 2
                                                                        then    case    when    clh.currency = 1
                                                                                        then    (   select  max(clh0.exchange)
                                                                                                        from    usinsuv01.claim_his clh0
                                                                                                        where 	clh0.claim = clh.claim
                                                                                                        and     clh0.transac =
                                                                                                                (   select  max(clh1.transac)
                                                                                                                from    usinsuv01.claim_his clh1
                                                                                                                where	clh1.claim = clh.claim
                                                                                                                and     clh1.transac <= clh.transac
                                                                                                                and     clh1.exchange not in (1,0)))
                                                                                        else    1 end
                                                                        else    0 end clh_exchange
                                                                from    (   select	case	when	cla.staclaim <> '6' or (cla.branch = 23 and cla.staclaim = '6')
                                                                                                                                then	1
                                                                                                                                else	0 end flag_reg,
                                                                                                                cla.claim,
                                                                                cla.usercomp,
                                                                                cla.company,
                                                                                pol.certype,
                                                                                                                pol.product pol_product,
                                                                                cla.branch,
                                                                                cla.policy,
                                                                                cla.occurdat,
                                                                                cla.ctid cla_id,
                                                                                coalesce(   coalesce((  select  max(cpl.currency)
                                                                                                        from    usinsuv01.curren_pol cpl
                                                                                                        where 	cpl.usercomp = cla.usercomp and cpl.company = cla.company and cpl.certype = pol.certype
                                                                                                        and     cpl.branch = cla.branch and cpl.policy = cla.policy and cpl.certif = cla.certif),
                                                                                                (   select  max(cpl.currency)
                                                                                                        from    usinsuv01.curren_pol cpl
                                                                                                        where 	cpl.usercomp = cla.usercomp and cpl.company = cla.company and cpl.certype = pol.certype
                                                                                                        and     cpl.branch = cla.branch and cpl.policy = cla.policy)),0) moneda_cod,
                                                                                                                coalesce((	select	distinct(coalesce(modulec,0))
                                                                                                                                                                                                        from	usinsuv01.modules
                                                                                                                                                                                                        where	usercomp = cla.usercomp
                                                                                                                                                                                                        and		company = cla.company
                                                                                                                                                                                                        and		certype = pol.certype
                                                                                                                                                                                                        and		branch = cla.branch
                                                                                                                                                                                                        and		policy = cla.policy
                                                                                                                                                                                                        and		certif = cla.certif
                                                                                                                                                                                                        and		effecdate <= cla.occurdat
                                                                                                                                                                                                        and		(nulldate is null or nulldate > cla.occurdat)),0) cla_modulec
                                                                                                from    (	select	pol.ctid pol_id
                                                                                                                        from	usinsuv01.policy pol
                                                                                                                        where	usercomp = 1
                                                                                                                        and		company = 1
                                                                                                                        and		certype = '2'
                                                                                                                        and		branch in (select branch from usinsug01.table10b where company = 2)
                                                                                                                        and		bussityp = '1'
                                                                                                                        and		exists
                                                                                                                                        (	select 	1
                                                                                                                                                from	usinsuv01.coinsuran coi
                                                                                                                                                where	coi.usercomp = pol.usercomp
                                                                                                                                                and     coi.company = pol.company
                                                                                                                                                and     coi.certype = pol.certype
                                                                                                                                                and     coi.branch = pol.branch
                                                                                                                                                and     coi.policy = pol.policy)) po0
                                                                                                join	usinsuv01.policy pol on pol.ctid = po0.pol_id
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
                                                                                                                                                                                        where	reserve = 1 or ajustes = 1 or pay_amount = 1)) tcl
                                                                                                                                        join	usinsuv01.claim_his clh on trim(clh.oper_type) = tcl.operation
                                                                                                                                        where   clh.claim = cla.claim
                                                                                                                                        and     clh.operdate >= '12/31/2018')) cla --ejecutar al a o 2021 (2018 es para pruebas)
                                                                join	usinsuv01.claim_his clh
                                                                                on		clh.claim = cla.claim
                                                                                        and		trim(clh.oper_type) in (select cast(operation as varchar(2)) from usinsug01.tab_cl_ope where pay_amount = 1)
                                                                                and     clh.operdate <= '12/31/2023'
                                                                where	cla.flag_reg = 1) clh --en caso de ejecutar a una fecha posterior, alterar esta fecha a la de la ejecuci n
                                                join	usinsuv01.claim cla on cla.ctid = clh.cla_id
                                                join 	usinsuv01.cl_m_cover clm
                                                                on 		clm.usercomp = clh.usercomp
                                                        and		clm.company = clh.company
                                                        and		clm.claim = clh.claim
                                                        and 	clm.movement = clh.transac
                                                join 	usinsuv01.coinsuran coi
                                                                on 	coi.usercomp = clh.usercomp 
                                                        and     coi.company = clh.company
                                                        and     coi.certype = clh.certype
                                                        and     coi.branch = clh.branch
                                                        and     coi.policy = clh.policy
                                                        and     coi.effecdate <= clh.occurdat
                                                        and     (coi.nulldate is null or coi.nulldate > clh.occurdat)
                                                where	coalesce(clh.amount,0) <> 0                                                                                
                                      ) AS TMP  
                                      '''
    
    DF_LPV_INSUNIX_NEGO1 = glue_context.read.format('jdbc').options(**connection).option("fetchsize",10000).option("dbtable",L_RBCSGRI_INSUNIX_LPV_NEGO1).load()
    
    L_RBCSGRI_INSUNIX_LPV_NEGO2 = '''
                                      (
                                                select 	'D' as INDDETREC,
                                                        'RBCSGRI' as TABLAIFRS17,
                                                        '' PK,
                                                        '' DTPREG,
                                                        '' TIOCPROC,
                                                        clh.pol_compdate TIOCFRM,
                                                        '' TIOCTO,
                                                        'PIV' KGIORIGM,
                                                        clh.claim || '-' || clh.transac KRBRECIN,
                                                        '' DORDCSG,
                                                        '2' DPLANO,
                                                        cast(100 - coalesce(clh.leadshare) as numeric (7,4)) VTXCSGI,
                                                        abs(cast (coalesce  (	case	when    clm.currency = clh.moneda_cod
                                                                                                                                then    clm.amount
                                                                                                                                else	case    when    clh.moneda_cod = 1
                                                                                                                                                then    clm.amount *
                                                                                                                                                        case    when    clm.currency = 2
                                                                                                                                                                then    clh.clh_exchange
                                                                                                                                                                else    1 end
                                                                                                                                                when	clh.moneda_cod = 2
                                                                                                                                                then    clm.amount /
                                                                                                                                                        case    when    clm.currency = 1
                                                                                                                                                                then    clh.clh_exchange
                                                                                                                                                                else	1 end
                                                                                                                                                else    0 end
                                                                                                                                end, 0) as numeric (12,2))) VMTCSGI,
                                                                coalesce(	case    when    coalesce((  select  distinct pro.brancht
                                                                                                                                                from    usinsuv01.product pro
                                                                                                                                                where	pro.usercomp = cla.usercomp
                                                                                                                                                and		pro.company = cla.company
                                                                                                                                                and		pro.branch = cla.branch
                                                                                                                                                and		pro.product = clh.pol_product
                                                                                                                                                and		pro.effecdate <= cla.occurdat
                                                                                                                                                and		(pro.nulldate is null or pro.nulldate > cla.occurdat)),'0') not in ('1','5')
                                                                                                        then	coalesce((	select  gco.covergen || '-' || gco.currency || '-' || 'g'
                                                                                                                                                from    usinsuv01.gen_cover gco
                                                                                                                                                where	gco.ctid =
                                                                                                                                coalesce(coalesce(coalesce(coalesce(
                                                                                                                                                                                                (   select  max(ctid)
                                                                                                                                                                from	usinsuv01.gen_cover gco
                                                                                                                                                                where	gco.usercomp = cla.usercomp and gco.company = cla.company and gco.branch = cla.branch and gco.product = clh.pol_product and gco.currency = clh.moneda_cod -- ndice regular
                                                                                                                                                                and     gco.modulec = clh.cla_modulec and gco.cover = clm.cover --variaci n 1 con modulec
                                                                                                                                                                and     gco.effecdate <= cla.occurdat and (gco.nulldate is null or gco.nulldate > cla.occurdat) --variaci n 2 vigencias
                                                                                                                                                                and     gco.statregt <> '4'), --variaci n 3 reg. v lido
                                                                                                                                                                (   select  max(ctid)
                                                                                                                                                                from	usinsuv01.gen_cover gco
                                                                                                                                                                where	gco.usercomp = cla.usercomp and gco.company = cla.company and gco.branch = cla.branch and gco.product = clh.pol_product and gco.currency = clh.moneda_cod -- ndice regular
                                                                                                                                                                and     gco.cover = clm.cover --variaci n 1 sin modulec
                                                                                                                                                                and     gco.effecdate <= cla.occurdat and (gco.nulldate is null or gco.nulldate > cla.occurdat) --variaci n 2 vigencias
                                                                                                                                                                and     gco.statregt <> '4')), --variaci n 3 reg. v lido
                                                                                                                                                        coalesce((	select  max(ctid)
                                                                                                                                                                from	usinsuv01.gen_cover gco
                                                                                                                                                                where	gco.usercomp = cla.usercomp and gco.company = cla.company and gco.branch = cla.branch and gco.product = clh.pol_product and gco.currency = clh.moneda_cod -- ndice regular
                                                                                                                                                                and     gco.modulec = clh.cla_modulec and gco.cover = clm.cover
                                                                                                                                                                and     gco.effecdate <= cla.occurdat and (gco.nulldate is null or gco.nulldate > cla.occurdat)
                                                                                                                                                                and     gco.statregt = '4'),
                                                                                                                                                                (	select  max(ctid)
                                                                                                                                                                from	usinsuv01.gen_cover gco
                                                                                                                                                                where	gco.usercomp = cla.usercomp and gco.company = cla.company and gco.branch = cla.branch and gco.product = clh.pol_product and gco.currency = clh.moneda_cod -- ndice regular
                                                                                                                                                                and     gco.cover = clm.cover
                                                                                                                                                                and     gco.effecdate <= cla.occurdat and (gco.nulldate is null or gco.nulldate > cla.occurdat)
                                                                                                                                                                and     gco.statregt = '4'))), --no est  cortado
                                                                                                                                                coalesce(coalesce((	select  max(ctid)
                                                                                                                                                                from	usinsuv01.gen_cover gco
                                                                                                                                                                where	gco.usercomp = cla.usercomp and gco.company = cla.company and gco.branch = cla.branch and gco.product = clh.pol_product and gco.currency = clh.moneda_cod -- ndice regular
                                                                                                                                                                and     gco.modulec = clh.cla_modulec and gco.cover = clm.cover
                                                                                                                                                                and     gco.effecdate <= cla.occurdat and gco.nulldate <= cla.occurdat
                                                                                                                                                                and     gco.statregt <> '4'),
                                                                                                                                                                (   select  max(ctid)
                                                                                                                                                                from	usinsuv01.gen_cover gco
                                                                                                                                                                where	gco.usercomp = cla.usercomp and gco.company = cla.company and gco.branch = cla.branch and gco.product = clh.pol_product and gco.currency = clh.moneda_cod -- ndice regular
                                                                                                                                                                and     gco.cover = clm.cover
                                                                                                                                                                and     gco.effecdate <= cla.occurdat and gco.nulldate <= cla.occurdat
                                                                                                                                                                and     gco.statregt <> '4')), -- no est  cortado, pero fue anulado antes del efecto del registro
                                                                                                                                                coalesce((	select  max(ctid)
                                                                                                                                                                from	usinsuv01.gen_cover gco
                                                                                                                                                                where	gco.usercomp = cla.usercomp and gco.company = cla.company and gco.branch = cla.branch and gco.product = clh.pol_product and gco.currency = clh.moneda_cod -- ndice regular
                                                                                                                                                                and     gco.modulec = clh.cla_modulec and gco.cover = clm.cover
                                                                                                                                                                and     gco.effecdate <= cla.occurdat and gco.nulldate <= cla.occurdat
                                                                                                                                                                and     gco.statregt = '4'),
                                                                                                                                                        (   select  max(ctid)
                                                                                                                                                                from	usinsuv01.gen_cover gco
                                                                                                                                                                where	gco.usercomp = cla.usercomp and gco.company = cla.company and gco.branch = cla.branch and gco.product = clh.pol_product and gco.currency = clh.moneda_cod -- ndice regular
                                                                                                                                                                and     gco.cover = clm.cover
                                                                                                                                                                and     gco.effecdate <= cla.occurdat and gco.nulldate <= cla.occurdat
                                                                                                                                                                and     statregt = '4')))), --no est  cortado y aparte fue anulado antes del efecto del registro
                                                                                                                                        coalesce(coalesce((	select  max(ctid)
                                                                                                                                                        from	usinsuv01.gen_cover gco
                                                                                                                                                        where	gco.usercomp = cla.usercomp and gco.company = cla.company and gco.branch = cla.branch and gco.product = clh.pol_product and gco.currency = clh.moneda_cod -- ndice regular
                                                                                                                                                        and     gco.modulec = clh.cla_modulec and gco.cover = clm.cover
                                                                                                                                                        and     gco.effecdate > cla.occurdat
                                                                                                                                                        and     gco.statregt <> '4'),
                                                                                                                                                        (   select  max(ctid)
                                                                                                                                                        from	usinsuv01.gen_cover gco
                                                                                                                                                        where	gco.usercomp = cla.usercomp and gco.company = cla.company and gco.branch = cla.branch and gco.product = clh.pol_product and gco.currency = clh.moneda_cod -- ndice regular
                                                                                                                                                        and     gco.cover = clm.cover
                                                                                                                                                        and     gco.effecdate <= cla.occurdat and gco.nulldate <= cla.occurdat
                                                                                                                                                        and     gco.statregt <> '4')), -- no est  cortado, pero tampoco al efecto de la tabla de datos particular
                                                                                                                                        coalesce((	select  max(ctid)
                                                                                                                                                        from	usinsuv01.gen_cover gco
                                                                                                                                                        where	gco.usercomp = cla.usercomp and gco.company = cla.company and gco.branch = cla.branch and gco.product = clh.pol_product and gco.currency = clh.moneda_cod -- ndice regular
                                                                                                                                                        and     gco.modulec = clh.cla_modulec and gco.cover = clm.cover
                                                                                                                                                        and     gco.effecdate <= cla.occurdat and gco.nulldate <= cla.occurdat
                                                                                                                                                        and     gco.statregt = '4'),
                                                                                                                                                (   select  max(ctid)
                                                                                                                                                        from	usinsuv01.gen_cover gco
                                                                                                                                                        where	gco.usercomp = cla.usercomp and gco.company = cla.company and gco.branch = cla.branch and gco.product = clh.pol_product and gco.currency = clh.moneda_cod -- ndice regular
                                                                                                                                                        and     gco.cover = clm.cover
                                                                                                                                                        and     gco.effecdate > cla.occurdat
                                                                                                                                                        and     gco.statregt = '4'))))), --est  cortado y no al efecto de la tabla de datos particular adem s de estar cortado
                                                                                                                        cast(coalesce(clm.cover,0)* -1 as varchar) || '-' || clh.moneda_cod || '-' || 'g')
                                                                                                        else    coalesce((   select  gco.covergen || '-' || gco.currency || '-' || 'l'
                                                                                                                                        from    usinsuv01.life_cover gco
                                                                                                                                        where	gco.ctid =
                                                                                                                                                        coalesce(coalesce(coalesce(
                                                                                                                                                                                                (	select  max(ctid)
                                                                                                                                                                                                        from 	usinsuv01.life_cover
                                                                                                                                                                                                        where   usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = clh.pol_product and currency = clh.moneda_cod -- ndice regular
                                                                                                                                                                                                        and		cover = clm.cover
                                                                                                                                                                                                        and		effecdate <= cla.occurdat and (nulldate is null or nulldate > cla.occurdat)
                                                                                                                                                                                                        and		statregt <> '4' ), --que no est  cortado
                                                                                                                                                                                                (   select  max(ctid)
                                                                                                                                                                                                        from 	usinsuv01.life_cover
                                                                                                                                                                                                        where   usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = clh.pol_product and currency = clh.moneda_cod -- ndice regular
                                                                                                                                                                                                        and		cover = clm.cover
                                                                                                                                                                                                        and		effecdate <= cla.occurdat and (nulldate is null or nulldate > cla.occurdat)
                                                                                                                                                                                                        and		statregt = '4' )),--est  cortado
                                                                                                                                                                                coalesce((  select  max(ctid)
                                                                                                                                                                                                        from 	usinsuv01.life_cover
                                                                                                                                                                                                        where   usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = clh.pol_product and currency = clh.moneda_cod -- ndice regular
                                                                                                                                                                                                        and		cover = clm.cover
                                                                                                                                                                                                        and		effecdate <= cla.occurdat and nulldate <= cla.occurdat
                                                                                                                                                                                                        and		statregt <> '4'),--no est  cortado pero fue anulado antes del efecto del registro
                                                                                                                                                                                                (   select  max(ctid)
                                                                                                                                                                                                        from 	usinsuv01.life_cover
                                                                                                                                                                                                        where   usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = clh.pol_product and currency = clh.moneda_cod -- ndice regular
                                                                                                                                                                                                        and		cover = clm.cover
                                                                                                                                                                                                        and		effecdate <= cla.occurdat and nulldate <= cla.occurdat
                                                                                                                                                                                                        and		statregt = '4'))), --est  cortado y aparte fue anulado antes del efecto del registro
                                                                                                                                                                coalesce((  select  max(ctid)
                                                                                                                                                                                        from 	usinsuv01.life_cover
                                                                                                                                                                                        where   usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = clh.pol_product and currency = clh.moneda_cod -- ndice regular
                                                                                                                                                                                        and		cover = clm.cover
                                                                                                                                                                                        and		effecdate > cla.occurdat
                                                                                                                                                                                        and		statregt <> '4'),
                                                                                                                                                                        (   select  max(ctid)
                                                                                                                                                                                from 	usinsuv01.life_cover
                                                                                                                                                                                where   usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = clh.pol_product and currency = clh.moneda_cod -- ndice regular
                                                                                                                                                                                and		cover = clm.cover
                                                                                                                                                                                and		effecdate > cla.occurdat --est  cortado pero no al efecto de la tabla de datos particular
                                                                                                                                                                        and		statregt = '4')))),
                                                                                                                                cast(coalesce(clm.cover,0)* -1 as varchar) || '-' || clh.moneda_cod || '-' || 'l')
                                                                                                end, 
                                                                                                cast(coalesce(clm.cover,0)* -1 as varchar) || '-' || clh.moneda_cod || '-' ||
                                                                                        case    when    coalesce((  select  distinct pro.brancht --, select * from usinsug01.table37
                                                                                                                                                from    usinsuv01.product pro
                                                                                                                                                where	pro.usercomp = cla.usercomp
                                                                                                                                                and		pro.company = cla.company
                                                                                                                                                and		pro.branch = cla.branch
                                                                                                                                                and		pro.product = clh.pol_product
                                                                                                                                                and		pro.effecdate <= cla.occurdat
                                                                                                                                                and		(pro.nulldate is null or pro.nulldate > cla.occurdat)),'0') not in ('1','5')
                                                                                                        then	'g'
                                                                                                        else	'l' end) KRCTPCBT,
                                                        '' KRCTPNDI, --no disponible
                                                        coalesce('PAE' || '-' || coalesce((select scod_vt from usinsug01.equi_vt_inx where scod_inx = clh.bene_code),clh.bene_code),'') KRBPENS,
                                                        '' KRBRENDA,
                                                        '1' DCODCSG,
                                                        '' DORDDSPIN,
                                                        'LPV' DCOMPA,
                                                        '' DMARCA,
                                                        '' DINDDESD,
                                                                '1' KRCTPQTP
                                                from	(	select	cla.cla_id,
                                                                                        cla.usercomp,
                                                                                        cla.company,
                                                                                        cla.claim,
                                                                                        cla.certype,
                                                                                        cla.pol_compdate,
                                                                                        cla.leadshare,
                                                                                        cla.branch,
                                                                                        cla.policy,
                                                                                        cla.occurdat,
                                                                                        cla.moneda_cod,
                                                                                        cla.pol_product,
                                                                                        cla.cla_modulec,
                                                                                        clh.transac,
                                                                                        clh.oper_type,
                                                                                        clh.operdate,
                                                                                        clh.bene_code,
                                                                                        coalesce(clh.amount,0) amount,
                                                                case    when    moneda_cod = 1
                                                                        then    case    when    clh.currency = 2
                                                                                        then    (   select  max(clh0.exchange)
                                                                                                        from    usinsuv01.claim_his clh0
                                                                                                        where	clh0.claim = clh.claim
                                                                                                        and     clh0.transac =
                                                                                                                (   select  max(clh1.transac)
                                                                                                                from    usinsuv01.claim_his clh1
                                                                                                                where	clh1.claim = clh.claim
                                                                                                                and     clh1.transac <= clh.transac
                                                                                                                and     clh1.exchange not in (1,0)))
                                                                                        else 1 end
                                                                        when 	moneda_cod = 2
                                                                        then    case    when    clh.currency = 1
                                                                                        then    (   select  max(clh0.exchange)
                                                                                                        from    usinsuv01.claim_his clh0
                                                                                                        where 	clh0.claim = clh.claim
                                                                                                        and     clh0.transac =
                                                                                                                (   select  max(clh1.transac)
                                                                                                                from    usinsuv01.claim_his clh1
                                                                                                                where	clh1.claim = clh.claim
                                                                                                                and     clh1.transac <= clh.transac
                                                                                                                and     clh1.exchange not in (1,0)))
                                                                                        else    1 end
                                                                        else    0 end clh_exchange
                                                                from    (   select	case	when	cla.staclaim <> '6' or (cla.branch = 23 and cla.staclaim = '6')
                                                                                                                                then	1
                                                                                                                                else	0 end flag_reg,
                                                                                                                cla.claim,
                                                                                cla.usercomp,
                                                                                cla.company,
                                                                                pol.certype,
                                                                                pol.compdate pol_compdate,
                                                                                pol.leadshare,
                                                                                pol.product pol_product,
                                                                                cla.branch,
                                                                                cla.policy,
                                                                                cla.occurdat,
                                                                                cla.ctid cla_id,
                                                                                coalesce(   coalesce((  select  max(cpl.currency)
                                                                                                        from    usinsuv01.curren_pol cpl
                                                                                                        where 	cpl.usercomp = cla.usercomp and cpl.company = cla.company and cpl.certype = pol.certype
                                                                                                        and     cpl.branch = cla.branch and cpl.policy = cla.policy and cpl.certif = cla.certif),
                                                                                                (   select  max(cpl.currency)
                                                                                                        from    usinsuv01.curren_pol cpl
                                                                                                        where 	cpl.usercomp = cla.usercomp and cpl.company = cla.company and cpl.certype = pol.certype
                                                                                                        and     cpl.branch = cla.branch and cpl.policy = cla.policy)),0) moneda_cod,
                                                                                                                coalesce((	select	distinct(coalesce(modulec,0))
                                                                                                                                                                                                        from	usinsuv01.modules
                                                                                                                                                                                                        where	usercomp = cla.usercomp
                                                                                                                                                                                                        and		company = cla.company
                                                                                                                                                                                                        and		certype = pol.certype
                                                                                                                                                                                                        and		branch = cla.branch
                                                                                                                                                                                                        and		policy = cla.policy
                                                                                                                                                                                                        and		certif = cla.certif
                                                                                                                                                                                                        and		effecdate <= cla.occurdat
                                                                                                                                                                                                        and		(nulldate is null or nulldate > cla.occurdat)),0) cla_modulec
                                                                                                from    (	select	pol.ctid pol_id
                                                                                                                        from	usinsuv01.policy pol
                                                                                                                        where	usercomp = 1
                                                                                                                        and		company = 1
                                                                                                                        and		certype = '2'
                                                                                                                        and		branch in (select branch from usinsug01.table10b where company = 2)
                                                                                                                        and		bussityp = '2') po0
                                                                                                join	usinsuv01.policy pol on pol.ctid = po0.pol_id
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
                                                                                                                                                                                        where	reserve = 1 or ajustes = 1 or pay_amount = 1)) tcl
                                                                                                                                        join	usinsuv01.claim_his clh on trim(clh.oper_type) = tcl.operation
                                                                                                                                        where   clh.claim = cla.claim
                                                                                                                                        and     clh.operdate >= '12/31/2018')) cla --ejecutar al a o 2021 (2018 es para pruebas)
                                                                join	usinsuv01.claim_his clh
                                                                                on		clh.claim = cla.claim
                                                                                        and		trim(clh.oper_type) in (select cast(operation as varchar(2)) from usinsug01.tab_cl_ope where pay_amount = 1)
                                                                                and     clh.operdate <= '12/31/2023'
                                                                where	cla.flag_reg = 1) clh --en caso de ejecutar a una fecha posterior, alterar esta fecha a la de la ejecuci n
                                                join	usinsuv01.claim cla on cla.ctid = clh.cla_id
                                                join 	usinsuv01.cl_m_cover clm
                                                                on 		clm.usercomp = clh.usercomp
                                                        and		clm.company = clh.company
                                                        and		clm.claim = clh.claim
                                                        and 	clm.movement = clh.transac
                                                where	coalesce(clh.amount,0) <> 0
                                      ) AS TMP  
                                      '''
    
    DF_LPV_INSUNIX_NEGO2 = glue_context.read.format('jdbc').options(**connection).option("fetchsize",10000).option("dbtable",L_RBCSGRI_INSUNIX_LPV_NEGO2).load()
    

    L_DF_RBCSGRI_INSUNIX = DF_LPG_INSUNIX_NEGO1.union(DF_LPG_INSUNIX_NEGO2).union(DF_LPV_INSUNIX_NEGO1).union(DF_LPV_INSUNIX_NEGO2)

    L_RBCSGRI_VTIME_LPG_NEGO1_Agrario  = '''
                                     (
                                                select 	'D' as INDDETREC,
                                                        'RBCSGRI' as TABLAIFRS17,
                                                        '' PK,
                                                        '' DTPREG,
                                                        '' TIOCPROC,
                                                        cast (coi."DCOMPDATE" as date) TIOCFRM,
                                                        '' TIOCTO,
                                                        'PVG' KGIORIGM,
                                                        cla."NCLAIM" || '-' || clh."NCASE_NUM" || '-' || clh."NTRANSAC" KRBRECIN,
                                                        '' DORDCSG,
                                                        case 	when	coalesce(coi."NCOMPANY",0) in (1,12)
                                                                        then	'1'
                                                                        else	'2' end DPLANO,
                                                        abs(cast (coalesce(coi."NSHARE",100) as numeric (7,4))) VTXCSGI,
                                                        abs(cast(cl0.namount * coalesce(coi."NSHARE",100) / 100 as numeric (12,2))) VMTCSGI,
                                                                cl0.ncover KRCTPCBT,
                                                        '' KRCTPNDI, --no disponible
                                                        coalesce('PAE' || '-' || clh."SCLIENT",'') KRBPENS,
                                                        '' KRBRENDA,
                                                        cast (coi."NCOMPANY" as varchar) DCODCSG,
                                                        '' DORDDSPIN,
                                                        'LPG' DCOMPA,
                                                        '' DMARCA,
                                                        '' DINDDESD,
                                                        case 	when	coalesce(coi."NCOMPANY",0) in (1,12)
                                                                then	'1'
                                                                else	'2' end KRCTPQTP
                                                from 	(	select	cla.cla_id,
                                                                        (   select  clh.ctid
                                                                        from    usvtimg01."CLAIM_HIS" clh
                                                                        where	clh."NCLAIM" = clm."NCLAIM"
                                                                                                and 	clh."NCASE_NUM" = clm."NCASE_NUM"
                                                                                                and 	clh."NDEMAN_TYPE" = clm."NDEMAN_TYPE"
                                                                                                and 	clh."NTRANSAC" = clm."NTRANSAC") clh_id,
                                                                                        coalesce(	case	when	cla.nbranch = 21 
                                                                                                                                then	(	select	gen."NCOVERGEN" || '-l'
                                                                                                                                                        from    usvtimg01."LIFE_COVER" gen
                                                                                                                                                        where	gen."NCOVER" = clm."NCOVER"
                                                                                                                                                        and     gen."NPRODUCT" = cla.pol_nproduct
                                                                                                                                                        and     gen."NMODULEC" = clm."NMODULEC"
                                                                                                                                                        and     gen."NBRANCH" = cla.nbranch
                                                                                                                                                        and     cast(gen."DEFFECDATE" as date) <= cla.doccurdat
                                                                                                                                                        and     (gen."DNULLDATE" is null or gen."DNULLDATE" > cla.doccurdat)
                                                                                                                                                        and     gen."SSTATREGT" <> '4')
                                                                                                                                else 	(	select	gen."NCOVERGEN" || '-g'
                                                                                                                                                        from    usvtimg01."GEN_COVER" gen
                                                                                                                                                        where	gen."NCOVER" = clm."NCOVER"
                                                                                                                                                        and     gen."NPRODUCT" = cla.pol_nproduct
                                                                                                                                                        and     gen."NMODULEC" = clm."NMODULEC"
                                                                                                                                                        and     gen."NBRANCH" = cla.nbranch
                                                                                                                                                        and     cast(gen."DEFFECDATE" as date) <= cla.doccurdat
                                                                                                                                                        and     (gen."DNULLDATE" is null or gen."DNULLDATE" > cla.doccurdat)
                                                                                                                                                        and     gen."SSTATREGT" <> '4')
                                                                                                                                end, clm."NCOVER" * -1 || '-' || case when cla.nbranch = 21 then 'l' else 'g' end) ncover,
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
                                                                                                                                        else	0 end, 0)) namount
                                                                        from	(	select	cla."NCLAIM" nclaim,
                                                                                                                pol."SCERTYPE" scertype,
                                                                                                                cla."NBRANCH" nbranch, 
                                                                                                                pol."NPRODUCT" pol_nproduct,
                                                                                                                cast(cla."DOCCURDAT" as date) doccurdat,
                                                                                                                cla.ctid cla_id,
                                                                                                                coalesce(
                                                                                                                        coalesce((	select  max(cpl."NCURRENCY")
                                                                                                                                                from    usvtimg01."CURREN_POL" cpl
                                                                                                                                                where 	cpl."SCERTYPE" = cla."SCERTYPE" and cpl."NBRANCH" = cla."NBRANCH"
                                                                                                                                                and		cpl."NPRODUCT" = pol."NPRODUCT" and cpl."NPOLICY" = cla."NPOLICY"
                                                                                                                                                and     cpl."NCERTIF" = cla."NCERTIF"),
                                                                                                (	select  max(cpl."NCURRENCY")
                                                                                                                                                from    usvtimg01."CURREN_POL" cpl
                                                                                                                                                where 	cpl."SCERTYPE" = cla."SCERTYPE" and cpl."NBRANCH" = cla."NBRANCH"
                                                                                                                                                and		cpl."NPRODUCT" = pol."NPRODUCT" and cpl."NPOLICY" = cla."NPOLICY")),0) moneda_cod
                                                                                                from	usvtimg01."POLICY" pol
                                                                                                join	usvtimg01."CLAIM" cla
                                                                                                                on		cla."SCERTYPE" = pol."SCERTYPE"
                                                                                                                and     cla."NPOLICY" = pol."NPOLICY"
                                                                                                                and     cla."NBRANCH" = pol."NBRANCH"
                                                                                                                and		cla."SSTACLAIM" <> '6'
                                                                                                                and		exists
                                                                                                                        (   select  1
                                                                                                                        from    usvtimg01."CLAIM_HIS" clh
                                                                                                                        where   coalesce (clh."NCLAIM",0) = clh."NCLAIM"
                                                                                                                        and     clh."NOPER_TYPE" in
                                                                                                                                                        (   select  cast("SVALUE" as INT4) "SVALUE"
                                                                                                                                        from    usvtimg01."CONDITION_SERV"
                                                                                                                                        where   "NCONDITION" in (71,72,73))--operaciones de reserva, ajustes o pagos
                                                                                                                        and     cast(clh."DOPERDATE" as date) >= '12/31/2018') --ejecutar al a o 2021 (2018 es para pruebas)
                                                                                                where   pol."SCERTYPE" = '2'
                                                                                                and 	pol."SBUSSITYP" = '1'
                                                                                                and		pol."NBRANCH" in (58) --solo est  este ramo por el volumen de datos que posee
                                                                                                and		exists
                                                                                                                (	select	distinct "NBRANCH", "NPOLICY"
                                                                                                                        from	usvtimg01."COINSURAN" coi
                                                                                                                        where	coi."SCERTYPE" = pol."SCERTYPE"
                                                                                                                        and     coi."NBRANCH" = pol."NBRANCH"
                                                                                                                        and     coi."NPRODUCT" = pol."NPRODUCT"
                                                                                                                        and     coi."NPOLICY" = pol."NPOLICY")) cla
                                                                        join	usvtimg01."CL_M_COVER" clm on clm."NCLAIM" = cla.nclaim
                                                                        and		exists
                                                                        (   select  1
                                                                        from    usvtimg01."CLAIM_HIS" clh
                                                                                                join	(	select	cast("SVALUE" as INT4) "SVALUE"
                                                                                                                        from	usvtimg01."CONDITION_SERV" cs 
                                                                                                                        where	"NCONDITION" in (73)) csv
                                                                                                                on	clh."NOPER_TYPE" = csv."SVALUE"
                                                                        where	clh."NCLAIM" = clm."NCLAIM"
                                                                                                and 	clh."NCASE_NUM" = clm."NCASE_NUM"
                                                                                                and 	clh."NDEMAN_TYPE" = clm."NDEMAN_TYPE"
                                                                                                and 	clh."NTRANSAC" = clm."NTRANSAC"
                                                                        and		cast(clh."DOPERDATE" as date) <= cast('12/31/2023' as date)) --en caso de ejecutar a una fecha posterior, alterar esta fecha a la de la ejecuci n
                                                                        group 	by 1,2,3) cl0
                                                join	usvtimg01."CLAIM" cla on cla.ctid = cl0.cla_id
                                                join	usvtimg01."CLAIM_HIS" clh on clh.ctid = cl0.clh_id
                                                join	usvtimg01."COINSURAN" coi
                                                                on		coi."SCERTYPE" = cla."SCERTYPE"
                                                                and     coi."NBRANCH" = cla."NBRANCH"
                                                                and     coi."NPRODUCT" = cla."NPRODUCT"
                                                                and     coi."NPOLICY" = cla."NPOLICY"
                                                                and 	coi."NCOMPANY" is not null
                                                                and     cast(coi."DEFFECDATE" as date) <= cast("DOCCURDAT" as date)
                                                                and     (coi."DNULLDATE" is null or cast(coi."DNULLDATE" as date) > cast("DOCCURDAT" as date))
                                                where 	coalesce(cl0.namount,0) <> 0
                                     ) AS TMP        
                                     '''

    DF_LPG_VTIME_NEGO1_Agrario = glue_context.read.format('jdbc').options(**connection).option("fetchsize",10000).option("dbtable",L_RBCSGRI_VTIME_LPG_NEGO1_Agrario).load()

    L_RBCSGRI_VTIME_LPG_NEGO1_OtrosRamos  = '''
                                     (
                                               select 	'D' as INDDETREC,
                                                        'RBCSGRI' as TABLAIFRS17,
                                                        '' PK,
                                                        '' DTPREG,
                                                        '' TIOCPROC,
                                                        cast (coi."DCOMPDATE" as date) TIOCFRM,
                                                        '' TIOCTO,
                                                        'PVG' KGIORIGM,
                                                        cla."NCLAIM" || '-' || clh."NCASE_NUM" || '-' || clh."NTRANSAC" KRBRECIN,
                                                        '' DORDCSG,
                                                        case 	when	coalesce(coi."NCOMPANY",0) in (1,12)
                                                                        then	'1'
                                                                        else	'2' end DPLANO,
                                                        abs(cast (coalesce(coi."NSHARE",100) as numeric (7,4))) VTXCSGI,
                                                        abs(cast(cl0.namount * coalesce(coi."NSHARE",100) / 100 as numeric (12,2))) VMTCSGI,
                                                                cl0.ncover KRCTPCBT,
                                                        '' KRCTPNDI, --no disponible
                                                        coalesce('PAE' || '-' || clh."SCLIENT",'') KRBPENS,
                                                        '' KRBRENDA,
                                                        cast (coi."NCOMPANY" as varchar) DCODCSG,
                                                        '' DORDDSPIN,
                                                        'LPG' DCOMPA,
                                                        '' DMARCA,
                                                        '' DINDDESD,
                                                        case 	when	coalesce(coi."NCOMPANY",0) in (1,12)
                                                                then	'1'
                                                                else	'2' end KRCTPQTP
                                                from 	(	select	cla.cla_id,
                                                                        (   select  clh.ctid
                                                                        from    usvtimg01."CLAIM_HIS" clh
                                                                        where	clh."NCLAIM" = clm."NCLAIM"
                                                                                                and 	clh."NCASE_NUM" = clm."NCASE_NUM"
                                                                                                and 	clh."NDEMAN_TYPE" = clm."NDEMAN_TYPE"
                                                                                                and 	clh."NTRANSAC" = clm."NTRANSAC") clh_id,
                                                                                        coalesce(	case	when	cla.nbranch = 21 
                                                                                                                                then	(	select	gen."NCOVERGEN" || '-l'
                                                                                                                                                        from    usvtimg01."LIFE_COVER" gen
                                                                                                                                                        where	gen."NCOVER" = clm."NCOVER"
                                                                                                                                                        and     gen."NPRODUCT" = cla.pol_nproduct
                                                                                                                                                        and     gen."NMODULEC" = clm."NMODULEC"
                                                                                                                                                        and     gen."NBRANCH" = cla.nbranch
                                                                                                                                                        and     cast(gen."DEFFECDATE" as date) <= cla.doccurdat
                                                                                                                                                        and     (gen."DNULLDATE" is null or gen."DNULLDATE" > cla.doccurdat)
                                                                                                                                                        and     gen."SSTATREGT" <> '4')
                                                                                                                                else 	(	select	gen."NCOVERGEN" || '-g'
                                                                                                                                                        from    usvtimg01."GEN_COVER" gen
                                                                                                                                                        where	gen."NCOVER" = clm."NCOVER"
                                                                                                                                                        and     gen."NPRODUCT" = cla.pol_nproduct
                                                                                                                                                        and     gen."NMODULEC" = clm."NMODULEC"
                                                                                                                                                        and     gen."NBRANCH" = cla.nbranch
                                                                                                                                                        and     cast(gen."DEFFECDATE" as date) <= cla.doccurdat
                                                                                                                                                        and     (gen."DNULLDATE" is null or gen."DNULLDATE" > cla.doccurdat)
                                                                                                                                                        and     gen."SSTATREGT" <> '4')
                                                                                                                                end, clm."NCOVER" * -1 || '-' || case when cla.nbranch = 21 then 'l' else 'g' end) ncover,
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
                                                                                                                                        else	0 end, 0)) namount
                                                                        from	(	select	cla."NCLAIM" nclaim,
                                                                                                                pol."SCERTYPE" scertype,
                                                                                                                cla."NBRANCH" nbranch, 
                                                                                                                pol."NPRODUCT" pol_nproduct,
                                                                                                                cast(cla."DOCCURDAT" as date) doccurdat,
                                                                                                                cla.ctid cla_id,
                                                                                                                coalesce(
                                                                                                                        coalesce((	select  max(cpl."NCURRENCY")
                                                                                                                                                from    usvtimg01."CURREN_POL" cpl
                                                                                                                                                where 	cpl."SCERTYPE" = cla."SCERTYPE" and cpl."NBRANCH" = cla."NBRANCH"
                                                                                                                                                and		cpl."NPRODUCT" = pol."NPRODUCT" and cpl."NPOLICY" = cla."NPOLICY"
                                                                                                                                                and     cpl."NCERTIF" = cla."NCERTIF"),
                                                                                                (	select  max(cpl."NCURRENCY")
                                                                                                                                                from    usvtimg01."CURREN_POL" cpl
                                                                                                                                                where 	cpl."SCERTYPE" = cla."SCERTYPE" and cpl."NBRANCH" = cla."NBRANCH"
                                                                                                                                                and		cpl."NPRODUCT" = pol."NPRODUCT" and cpl."NPOLICY" = cla."NPOLICY")),0) moneda_cod
                                                                                                from	usvtimg01."POLICY" pol
                                                                                                join	usvtimg01."CLAIM" cla
                                                                                                                on		cla."SCERTYPE" = pol."SCERTYPE"
                                                                                                                and     cla."NPOLICY" = pol."NPOLICY"
                                                                                                                and     cla."NBRANCH" = pol."NBRANCH"
                                                                                                                and		cla."SSTACLAIM" <> '6'
                                                                                                                and		exists
                                                                                                                        (   select  1
                                                                                                                        from    usvtimg01."CLAIM_HIS" clh
                                                                                                                        where   coalesce (clh."NCLAIM",0) = clh."NCLAIM"
                                                                                                                        and     clh."NOPER_TYPE" in
                                                                                                                                                        (   select  cast("SVALUE" as INT4) "SVALUE"
                                                                                                                                        from    usvtimg01."CONDITION_SERV"
                                                                                                                                        where   "NCONDITION" in (71,72,73))--operaciones de reserva, ajustes o pagos
                                                                                                                        and     cast(clh."DOPERDATE" as date) >= '12/31/2018') --ejecutar al a o 2021 (2018 es para pruebas)
                                                                                                where   pol."SCERTYPE" = '2'
                                                                                                and 	pol."SBUSSITYP" = '1'
                                                                                                and		pol."NBRANCH" not in (58) --se excluye este ramo por el volumen de datos que posee
                                                                                                and		exists
                                                                                                                (	select	distinct "NBRANCH", "NPOLICY"
                                                                                                                        from	usvtimg01."COINSURAN" coi
                                                                                                                        where	coi."SCERTYPE" = pol."SCERTYPE"
                                                                                                                        and     coi."NBRANCH" = pol."NBRANCH"
                                                                                                                        and     coi."NPRODUCT" = pol."NPRODUCT"
                                                                                                                        and     coi."NPOLICY" = pol."NPOLICY")) cla
                                                                        join	usvtimg01."CL_M_COVER" clm on clm."NCLAIM" = cla.nclaim
                                                                        and		exists
                                                                        (   select  1
                                                                        from    usvtimg01."CLAIM_HIS" clh
                                                                                                join	(	select	cast("SVALUE" as INT4) "SVALUE"
                                                                                                                        from	usvtimg01."CONDITION_SERV" cs 
                                                                                                                        where	"NCONDITION" in (73)) csv
                                                                                                                on	clh."NOPER_TYPE" = csv."SVALUE"
                                                                        where	clh."NCLAIM" = clm."NCLAIM"
                                                                                                and 	clh."NCASE_NUM" = clm."NCASE_NUM"
                                                                                                and 	clh."NDEMAN_TYPE" = clm."NDEMAN_TYPE"
                                                                                                and 	clh."NTRANSAC" = clm."NTRANSAC"
                                                                        and		cast(clh."DOPERDATE" as date) <= cast('12/31/2023' as date)) --en caso de ejecutar a una fecha posterior, alterar esta fecha a la de la ejecuci n
                                                                        group 	by 1,2,3) cl0
                                                join	usvtimg01."CLAIM" cla on cla.ctid = cl0.cla_id
                                                join	usvtimg01."CLAIM_HIS" clh on clh.ctid = cl0.clh_id
                                                join	usvtimg01."COINSURAN" coi
                                                                on		coi."SCERTYPE" = cla."SCERTYPE"
                                                                and     coi."NBRANCH" = cla."NBRANCH"
                                                                and     coi."NPRODUCT" = cla."NPRODUCT"
                                                                and     coi."NPOLICY" = cla."NPOLICY"
                                                                and 	coi."NCOMPANY" is not null
                                                                and     cast(coi."DEFFECDATE" as date) <= cast("DOCCURDAT" as date)
                                                                and     (coi."DNULLDATE" is null or cast(coi."DNULLDATE" as date) > cast("DOCCURDAT" as date))
                                                where 	coalesce(cl0.namount,0) <> 0
                                     ) AS TMP        
                                     '''

    DF_LPG_VTIME_NEGO1_OtrosRamos = glue_context.read.format('jdbc').options(**connection).option("fetchsize",10000).option("dbtable",L_RBCSGRI_VTIME_LPG_NEGO1_OtrosRamos).load()

    L_RBCSGRI_VTIME_LPG_NEGO2  = '''
                                     (
                                        select 	'D' as INDDETREC,
                                                'RBCSGRI' as TABLAIFRS17,
                                                '' PK,
                                                '' DTPREG,
                                                '' TIOCPROC,
                                                cast (pol."DCOMPDATE" as date) TIOCFRM,
                                                '' TIOCTO,
                                                'PVG' KGIORIGM,
                                                cla."NCLAIM" || '-' || clh."NCASE_NUM" || '-' || clh."NTRANSAC" KRBRECIN,
                                                '' DORDCSG,
                                                '2' DPLANO,
                                                cast (100 - coalesce(pol."NLEADSHARE",0) as numeric(7,4)) VTXCSGI,
                                                abs(cast(cl0.namount as numeric (12,2))) VMTCSGI,
                                                        cl0.ncover KRCTPCBT,
                                                '' KRCTPNDI, --no disponible
                                                coalesce('PAE' || '-' || clh."SCLIENT",'') KRBPENS,
                                                '' KRBRENDA,
                                                '1' DCODCSG,
                                                '' DORDDSPIN,
                                                'LPG' DCOMPA,
                                                '' DMARCA,
                                                '' DINDDESD,
                                                '1' KRCTPQTP
                                        from 	(	select	cla.cla_id,
                                                                                cla.pol_id,
                                                                (   select  clh.ctid
                                                                from    usvtimg01."CLAIM_HIS" clh
                                                                where	clh."NCLAIM" = clm."NCLAIM"
                                                                                        and 	clh."NCASE_NUM" = clm."NCASE_NUM"
                                                                                        and 	clh."NDEMAN_TYPE" = clm."NDEMAN_TYPE"
                                                                                        and 	clh."NTRANSAC" = clm."NTRANSAC") clh_id,
                                                                                coalesce(	case	when	cla.nbranch = 21 
                                                                                                                        then	(	select	gen."NCOVERGEN" || '-l'
                                                                                                                                                from    usvtimg01."LIFE_COVER" gen
                                                                                                                                                where	gen."NCOVER" = clm."NCOVER"
                                                                                                                                                and     gen."NPRODUCT" = cla.pol_nproduct
                                                                                                                                                and     gen."NMODULEC" = clm."NMODULEC"
                                                                                                                                                and     gen."NBRANCH" = cla.nbranch
                                                                                                                                                and     cast(gen."DEFFECDATE" as date) <= cla.doccurdat
                                                                                                                                                and     (gen."DNULLDATE" is null or gen."DNULLDATE" > cla.doccurdat)
                                                                                                                                                and     gen."SSTATREGT" <> '4')
                                                                                                                        else 	(	select	gen."NCOVERGEN" || '-g'
                                                                                                                                                from    usvtimg01."GEN_COVER" gen
                                                                                                                                                where	gen."NCOVER" = clm."NCOVER"
                                                                                                                                                and     gen."NPRODUCT" = cla.pol_nproduct
                                                                                                                                                and     gen."NMODULEC" = clm."NMODULEC"
                                                                                                                                                and     gen."NBRANCH" = cla.nbranch
                                                                                                                                                and     cast(gen."DEFFECDATE" as date) <= cla.doccurdat
                                                                                                                                                and     (gen."DNULLDATE" is null or gen."DNULLDATE" > cla.doccurdat)
                                                                                                                                                and     gen."SSTATREGT" <> '4')
                                                                                                                        end, clm."NCOVER" * -1 || '-' || case when cla.nbranch = 21 then 'l' else 'g' end) ncover,
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
                                                                                                                                else	0 end, 0)) namount
                                                                from	(	select	cla."NCLAIM" nclaim,
                                                                                                        pol."SCERTYPE" scertype,
                                                                                                        cla."NBRANCH" nbranch, 
                                                                                                        pol."NPRODUCT" pol_nproduct,
                                                                                                        cast(cla."DOCCURDAT" as date) doccurdat,
                                                                                                        cla.ctid cla_id,
                                                                                                        pol.ctid pol_id,
                                                                                                        coalesce(
                                                                                                                coalesce((	select  max(cpl."NCURRENCY")
                                                                                                                                        from    usvtimg01."CURREN_POL" cpl
                                                                                                                                        where 	cpl."SCERTYPE" = cla."SCERTYPE" and cpl."NBRANCH" = cla."NBRANCH"
                                                                                                                                        and		cpl."NPRODUCT" = pol."NPRODUCT" and cpl."NPOLICY" = cla."NPOLICY"
                                                                                                                                        and     cpl."NCERTIF" = cla."NCERTIF"),
                                                                                        (	select  max(cpl."NCURRENCY")
                                                                                                                                        from    usvtimg01."CURREN_POL" cpl
                                                                                                                                        where 	cpl."SCERTYPE" = cla."SCERTYPE" and cpl."NBRANCH" = cla."NBRANCH"
                                                                                                                                        and		cpl."NPRODUCT" = pol."NPRODUCT" and cpl."NPOLICY" = cla."NPOLICY")),0) moneda_cod
                                                                                        from	usvtimg01."POLICY" pol
                                                                                        join	usvtimg01."CLAIM" cla
                                                                                                        on		cla."SCERTYPE" = pol."SCERTYPE"
                                                                                                        and     cla."NPOLICY" = pol."NPOLICY"
                                                                                                        and     cla."NBRANCH" = pol."NBRANCH"
                                                                                                        and		cla."SSTACLAIM" <> '6'
                                                                                                        and		exists
                                                                                                                (   select  1
                                                                                                                from    usvtimg01."CLAIM_HIS" clh
                                                                                                                where   coalesce (clh."NCLAIM",0) = clh."NCLAIM"
                                                                                                                and     clh."NOPER_TYPE" in
                                                                                                                                                (   select  cast("SVALUE" as INT4) "SVALUE"
                                                                                                                                from    usvtimg01."CONDITION_SERV"
                                                                                                                                where   "NCONDITION" in (71,72,73))--operaciones de reserva, ajustes o pagos
                                                                                                                and     cast(clh."DOPERDATE" as date) >= '12/31/2018') --ejecutar al a o 2021 (2018 es para pruebas)
                                                                                        where   pol."SCERTYPE" = '2'
                                                                                        and 	pol."SBUSSITYP" = '2') cla
                                                                join	usvtimg01."CL_M_COVER" clm on clm."NCLAIM" = cla.nclaim
                                                                and		exists
                                                                (   select  1
                                                                from    usvtimg01."CLAIM_HIS" clh
                                                                                        join	(	select	cast("SVALUE" as INT4) "SVALUE"
                                                                                                                from	usvtimg01."CONDITION_SERV" cs 
                                                                                                                where	"NCONDITION" in (73)) csv
                                                                                                        on	clh."NOPER_TYPE" = csv."SVALUE"
                                                                where	clh."NCLAIM" = clm."NCLAIM"
                                                                                        and 	clh."NCASE_NUM" = clm."NCASE_NUM"
                                                                                        and 	clh."NDEMAN_TYPE" = clm."NDEMAN_TYPE"
                                                                                        and 	clh."NTRANSAC" = clm."NTRANSAC"
                                                                and		cast(clh."DOPERDATE" as date) <= cast('12/31/2023' as date)) --en caso de ejecutar a una fecha posterior, alterar esta fecha a la de la ejecuci n
                                                                group 	by 1,2,3,4) cl0
                                        join	usvtimg01."CLAIM" cla on cla.ctid = cl0.cla_id
                                        join	usvtimg01."POLICY" pol on pol.ctid = cl0.pol_id
                                        join	usvtimg01."CLAIM_HIS" clh on clh.ctid = cl0.clh_id
                                        where 	coalesce(cl0.namount,0) <> 0
                                     ) AS TMP     
                                     '''

    DF_LPG_VTIME_NEGO2 = glue_context.read.format('jdbc').options(**connection).option("fetchsize",10000).option("dbtable",L_RBCSGRI_VTIME_LPG_NEGO2).load()

    
    L_RBCSGRI_VTIME_LPV_NEGO1  = '''
                                    (
                                        select 	        'D' as INDDETREC,
                                                        'RBCSGRI' as TABLAIFRS17,
                                                        '' PK,
                                                        '' DTPREG,
                                                        '' TIOCPROC,
                                                        cast (coi."DCOMPDATE" as date) TIOCFRM,
                                                        '' TIOCTO,
                                                        'PVV' KGIORIGM,
                                                        cla."NCLAIM" || '-' || clh."NCASE_NUM" || '-' || clh."NTRANSAC" KRBRECIN,
                                                        '' DORDCSG,
                                                        case 	when	coalesce(coi."NCOMPANY",0) in (1,12)
                                                                        then	'1'
                                                                        else	'2' end DPLANO,
                                                        abs(cast (coalesce(coi."NSHARE",100) as numeric (7,4))) VTXCSGI,
                                                        abs(cast(cl0.namount * coalesce(coi."NSHARE",100) / 100 as numeric (12,2))) VMTCSGI,
                                                                cl0.ncover KRCTPCBT,
                                                        '' KRCTPNDI, --no disponible
                                                        coalesce('PAE' || '-' || clh."SCLIENT",'') KRBPENS,
                                                        '' KRBRENDA,
                                                        cast (coi."NCOMPANY" as varchar) DCODCSG,
                                                        '' DORDDSPIN,
                                                        'LPV' DCOMPA,
                                                        '' DMARCA,
                                                        '' DINDDESD,
                                                        case 	when	coalesce(coi."NCOMPANY",0) in (1,12)
                                                                then	'1'
                                                                else	'2' end KRCTPQTP
                                                from 	(	select	cla.cla_id,
                                                                        (   select  clh.ctid
                                                                        from    usvtimv01."CLAIM_HIS" clh
                                                                        where	clh."NCLAIM" = clm."NCLAIM"
                                                                                                and 	clh."NCASE_NUM" = clm."NCASE_NUM"
                                                                                                and 	clh."NDEMAN_TYPE" = clm."NDEMAN_TYPE"
                                                                                                and 	clh."NTRANSAC" = clm."NTRANSAC") clh_id,
                                                                                        cast(	coalesce((	select	gen."NCOVERGEN"
                                                                                                                                from    usvtimv01."LIFE_COVER" gen
                                                                                                                                where	gen."NCOVER" = clm."NCOVER"
                                                                                                                                and     gen."NPRODUCT" = cla.pol_nproduct
                                                                                                                                and     gen."NMODULEC" = clm."NMODULEC"
                                                                                                                                and     gen."NBRANCH" = cla.nbranch
                                                                                                                                and     cast(gen."DEFFECDATE" as date) <= cla.doccurdat
                                                                                                                                and     (gen."DNULLDATE" is null or gen."DNULLDATE" > cla.doccurdat)
                                                                                                                                and     gen."SSTATREGT" <> '4'),
                                                                                                                clm."NCOVER" * -1) as varchar) ncover,
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
                                                                                                                                        else	0 end, 0)) namount
                                                                        from	(	select	cla."NCLAIM" nclaim,
                                                                                                                pol."SCERTYPE" scertype,
                                                                                                                cla."NBRANCH" nbranch, 
                                                                                                                pol."NPRODUCT" pol_nproduct,
                                                                                                                cast(cla."DOCCURDAT" as date) doccurdat,
                                                                                                                cla.ctid cla_id,
                                                                                                                coalesce(
                                                                                                                        coalesce((	select  max(cpl."NCURRENCY")
                                                                                                                                                from    usvtimv01."CURREN_POL" cpl
                                                                                                                                                where 	cpl."SCERTYPE" = cla."SCERTYPE" and cpl."NBRANCH" = cla."NBRANCH"
                                                                                                                                                and		cpl."NPRODUCT" = pol."NPRODUCT" and cpl."NPOLICY" = cla."NPOLICY"
                                                                                                                                                and     cpl."NCERTIF" = cla."NCERTIF"),
                                                                                                (	select  max(cpl."NCURRENCY")
                                                                                                                                                from    usvtimv01."CURREN_POL" cpl
                                                                                                                                                where 	cpl."SCERTYPE" = cla."SCERTYPE" and cpl."NBRANCH" = cla."NBRANCH"
                                                                                                                                                and		cpl."NPRODUCT" = pol."NPRODUCT" and cpl."NPOLICY" = cla."NPOLICY")),0) moneda_cod
                                                                                                from	usvtimv01."POLICY" pol
                                                                                                join	usvtimv01."CLAIM" cla
                                                                                                                on		cla."SCERTYPE" = pol."SCERTYPE"
                                                                                                                and     cla."NPOLICY" = pol."NPOLICY"
                                                                                                                and     cla."NBRANCH" = pol."NBRANCH"
                                                                                                                and		cla."SSTACLAIM" <> '6'
                                                                                                                and		exists
                                                                                                                        (   select  1
                                                                                                                        from    usvtimv01."CLAIM_HIS" clh
                                                                                                                        where   coalesce (clh."NCLAIM",0) = clh."NCLAIM"
                                                                                                                        and     clh."NOPER_TYPE" in
                                                                                                                                                        (   select  cast("SVALUE" as INT4) "SVALUE"
                                                                                                                                        from    usvtimv01."CONDITION_SERV"
                                                                                                                                        where   "NCONDITION" in (71,72,73))--operaciones de reserva, ajustes o pagos
                                                                                                                        and     cast(clh."DOPERDATE" as date) >= '12/31/2015') --ejecutar al a o 2021 (2015 es para pruebas)
                                                                                                where   pol."SCERTYPE" = '2'
                                                                                                and 	pol."SBUSSITYP" = '1'
                                                                                                and		exists
                                                                                                                (	select	distinct "NBRANCH", "NPOLICY"
                                                                                                                        from	usvtimv01."COINSURAN" coi
                                                                                                                        where	coi."SCERTYPE" = pol."SCERTYPE"
                                                                                                                        and     coi."NBRANCH" = pol."NBRANCH"
                                                                                                                        and     coi."NPRODUCT" = pol."NPRODUCT"
                                                                                                                        and     coi."NPOLICY" = pol."NPOLICY")) cla
                                                                        join	usvtimv01."CL_M_COVER" clm on clm."NCLAIM" = cla.nclaim
                                                                        and		exists
                                                                        (   select  1
                                                                        from    usvtimv01."CLAIM_HIS" clh
                                                                                                join	(	select	cast("SVALUE" as INT4) "SVALUE"
                                                                                                                        from	usvtimv01."CONDITION_SERV" cs 
                                                                                                                        where	"NCONDITION" in (73)) csv
                                                                                                                on	clh."NOPER_TYPE" = csv."SVALUE"
                                                                        where	clh."NCLAIM" = clm."NCLAIM"
                                                                                                and 	clh."NCASE_NUM" = clm."NCASE_NUM"
                                                                                                and 	clh."NDEMAN_TYPE" = clm."NDEMAN_TYPE"
                                                                                                and 	clh."NTRANSAC" = clm."NTRANSAC"
                                                                        and		cast(clh."DOPERDATE" as date) <= cast('12/31/2023' as date)) --en caso de ejecutar a una fecha posterior, alterar esta fecha a la de la ejecuci n
                                                                        group 	by 1,2,3) cl0
                                                join	usvtimv01."CLAIM" cla on cla.ctid = cl0.cla_id
                                                join	usvtimv01."CLAIM_HIS" clh on clh.ctid = cl0.clh_id
                                                join	usvtimv01."COINSURAN" coi
                                                                on		coi."SCERTYPE" = cla."SCERTYPE"
                                                                and     coi."NBRANCH" = cla."NBRANCH"
                                                                and     coi."NPRODUCT" = cla."NPRODUCT"
                                                                and     coi."NPOLICY" = cla."NPOLICY"
                                                                and 	coi."NCOMPANY" is not null
                                                                and     cast(coi."DEFFECDATE" as date) <= cast("DOCCURDAT" as date)
                                                                and     (coi."DNULLDATE" is null or cast(coi."DNULLDATE" as date) > cast("DOCCURDAT" as date))
                                                where 	coalesce(cl0.namount,0) <> 0
                                    ) AS TMP       
                                    '''

    DF_LPV_VTIME_NEGO1 = glue_context.read.format('jdbc').options(**connection).option("fetchsize",10000).option("dbtable",L_RBCSGRI_VTIME_LPV_NEGO1).load()

    L_RBCSGRI_VTIME_LPV_NEGO2  = '''
                                    (
                                       select 	'D' as INDDETREC,
                                                'RBCSGRI' as TABLAIFRS17,
                                                '' PK,
                                                '' DTPREG,
                                                '' TIOCPROC,
                                                cast (pol."DCOMPDATE" as date) TIOCFRM,
                                                '' TIOCTO,
                                                'PVV' KGIORIGM,
                                                cla."NCLAIM" || '-' || clh."NCASE_NUM" || '-' || clh."NTRANSAC" KRBRECIN,
                                                '' DORDCSG,
                                                '2' DPLANO,
                                                cast (100 - coalesce(pol."NLEADSHARE",0) as numeric(7,4)) VTXCSGI,
                                                abs(cast(cl0.namount as numeric (12,2))) VMTCSGI,
                                                        cl0.ncover KRCTPCBT,
                                                '' KRCTPNDI, --no disponible
                                                coalesce('PAE' || '-' || clh."SCLIENT",'') KRBPENS,
                                                '' KRBRENDA,
                                                '1' DCODCSG,
                                                '' DORDDSPIN,
                                                'LPV' DCOMPA,
                                                '' DMARCA,
                                                '' DINDDESD,
                                                '1' KRCTPQTP
                                        from 	(	select	cla.cla_id,
                                                                                cla.pol_id,
                                                                (   select  clh.ctid
                                                                from    usvtimv01."CLAIM_HIS" clh
                                                                where	clh."NCLAIM" = clm."NCLAIM"
                                                                                        and 	clh."NCASE_NUM" = clm."NCASE_NUM"
                                                                                        and 	clh."NDEMAN_TYPE" = clm."NDEMAN_TYPE"
                                                                                        and 	clh."NTRANSAC" = clm."NTRANSAC") clh_id,
                                                                                cast(	coalesce((	select	gen."NCOVERGEN"
                                                                                                                        from    usvtimv01."LIFE_COVER" gen
                                                                                                                        where	gen."NCOVER" = clm."NCOVER"
                                                                                                                        and     gen."NPRODUCT" = cla.pol_nproduct
                                                                                                                        and     gen."NMODULEC" = clm."NMODULEC"
                                                                                                                        and     gen."NBRANCH" = cla.nbranch
                                                                                                                        and     cast(gen."DEFFECDATE" as date) <= cla.doccurdat
                                                                                                                        and     (gen."DNULLDATE" is null or gen."DNULLDATE" > cla.doccurdat)
                                                                                                                        and     gen."SSTATREGT" <> '4'),
                                                                                                        clm."NCOVER" * -1) as varchar) ncover,
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
                                                                                                                                else	0 end, 0)) namount
                                                                from	(	select	cla."NCLAIM" nclaim,
                                                                                                        pol."SCERTYPE" scertype,
                                                                                                        cla."NBRANCH" nbranch, 
                                                                                                        pol."NPRODUCT" pol_nproduct,
                                                                                                        cast(cla."DOCCURDAT" as date) doccurdat,
                                                                                                        cla.ctid cla_id,
                                                                                                        pol.ctid pol_id,
                                                                                                        coalesce(
                                                                                                                coalesce((	select  max(cpl."NCURRENCY")
                                                                                                                                        from    usvtimv01."CURREN_POL" cpl
                                                                                                                                        where 	cpl."SCERTYPE" = cla."SCERTYPE" and cpl."NBRANCH" = cla."NBRANCH"
                                                                                                                                        and		cpl."NPRODUCT" = pol."NPRODUCT" and cpl."NPOLICY" = cla."NPOLICY"
                                                                                                                                        and     cpl."NCERTIF" = cla."NCERTIF"),
                                                                                        (	select  max(cpl."NCURRENCY")
                                                                                                                                        from    usvtimv01."CURREN_POL" cpl
                                                                                                                                        where 	cpl."SCERTYPE" = cla."SCERTYPE" and cpl."NBRANCH" = cla."NBRANCH"
                                                                                                                                        and		cpl."NPRODUCT" = pol."NPRODUCT" and cpl."NPOLICY" = cla."NPOLICY")),0) moneda_cod
                                                                                        from	usvtimv01."POLICY" pol
                                                                                        join	usvtimv01."CLAIM" cla
                                                                                                        on		cla."SCERTYPE" = pol."SCERTYPE"
                                                                                                        and     cla."NPOLICY" = pol."NPOLICY"
                                                                                                        and     cla."NBRANCH" = pol."NBRANCH"
                                                                                                        and		cla."SSTACLAIM" <> '6'
                                                                                                        and		exists
                                                                                                                (   select  1
                                                                                                                from    usvtimv01."CLAIM_HIS" clh
                                                                                                                where   coalesce (clh."NCLAIM",0) = clh."NCLAIM"
                                                                                                                and     clh."NOPER_TYPE" in
                                                                                                                                                (   select  cast("SVALUE" as INT4) "SVALUE"
                                                                                                                                from    usvtimv01."CONDITION_SERV"
                                                                                                                                where   "NCONDITION" in (71,72,73))--operaciones de reserva, ajustes o pagos
                                                                                                                and     cast(clh."DOPERDATE" as date) >= '12/31/2018') --ejecutar al a o 2021 (2018 es para pruebas)
                                                                                        where   pol."SCERTYPE" = '2'
                                                                                        and 	pol."SBUSSITYP" = '2') cla
                                                                join	usvtimv01."CL_M_COVER" clm on clm."NCLAIM" = cla.nclaim
                                                                and		exists
                                                                (   select  1
                                                                from    usvtimv01."CLAIM_HIS" clh
                                                                                        join	(	select	cast("SVALUE" as INT4) "SVALUE"
                                                                                                                from	usvtimv01."CONDITION_SERV" cs 
                                                                                                                where	"NCONDITION" in (73)) csv
                                                                                                        on	clh."NOPER_TYPE" = csv."SVALUE"
                                                                where	clh."NCLAIM" = clm."NCLAIM"
                                                                                        and 	clh."NCASE_NUM" = clm."NCASE_NUM"
                                                                                        and 	clh."NDEMAN_TYPE" = clm."NDEMAN_TYPE"
                                                                                        and 	clh."NTRANSAC" = clm."NTRANSAC"
                                                                and		cast(clh."DOPERDATE" as date) <= cast('12/31/2023' as date)) --en caso de ejecutar a una fecha posterior, alterar esta fecha a la de la ejecuci n
                                                                group 	by 1,2,3,4) cl0
                                        join	usvtimv01."CLAIM" cla on cla.ctid = cl0.cla_id
                                        join	usvtimv01."POLICY" pol on pol.ctid = cl0.pol_id
                                        join	usvtimv01."CLAIM_HIS" clh on clh.ctid = cl0.clh_id
                                        where 	coalesce(cl0.namount,0) <> 0
                                    ) AS TMP      
                                    '''

    DF_LPV_VTIME_NEGO2 = glue_context.read.format('jdbc').options(**connection).option("fetchsize",10000).option("dbtable",L_RBCSGRI_VTIME_LPV_NEGO2).load()

    L_DF_RBCSGRI_VTIME = DF_LPG_VTIME_NEGO1_Agrario.union(DF_LPG_VTIME_NEGO1_OtrosRamos).union(DF_LPG_VTIME_NEGO2).union(DF_LPV_VTIME_NEGO1).union(DF_LPV_VTIME_NEGO2)


#     L_RBCSGRI_INSIS = f'''
#                             (
#                             ) AS TMP
#                              '''
#     L_DF_RBCSGRI_INSIS = glue_context.read.format('jdbc').options(**connection).option("dbtable",L_RBCSGRI_INSIS).load()

    L_DF_RBCSGRI = L_DF_RBCSGRI_INSUNIX.union(L_DF_RBCSGRI_VTIME)#.union(L_DF_RBCSGRI_INSIS)

    return L_DF_RBCSGRI  
