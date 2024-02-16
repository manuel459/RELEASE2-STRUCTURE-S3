def get_data(glue_context, connection):

  L_RBRECIN_INSUNIX_LPG_OTROS_RAMOS = '''
                                        (
                                               select	'D' INDDETREC,
                                                        'RBRECIN' TABLAIFRS17,
                                                        '' PK,
                                                        '' DTPREG, --excluido
                                                        '' TIOCPROC, --excluido
                                                        clh.operdate TIOCFRM,
                                                        '' TIOCTO, --excluido
                                                        '' KRITPREG, --indeterminado (pendiente coordinar con FID/LP)
                                                        '' KRITPRGI, --indeterminado (pendiente coordinar con FID/LP)
                                                        case	when coalesce(clh.amount,0) <= 0
                                                                        then 1
                                                                        else 2 end KRCTPRCI,
                                                        'PIG' KGIORIGM, --excluido
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
                                                                                from	usinsug01.claim_his cl0
                                                                                where 	cl0.claim = clh.claim
                                                                                and		cl0.transac = 	
                                                                                                (	select 	max(csp.transac)
                                                                                                        from	usinsug01.claim_pay_sap csp
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
                                                                                                                                                                        from	usinsug01.claim_his clh0
                                                                                                                                                                        where	clh0.claim = clh.claim
                                                                                                                                                                        and		clh0.transac =
                                                                                                                                                                                        (	select	max(clh1.transac)
                                                                                                                                                                                                from	usinsug01.claim_his clh1
                                                                                                                                                                                                where 	clh1.claim = clh.claim
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
                                                                                                                                                                        from	usinsug01.claim_his clh0
                                                                                                                                                                        where	clh0.claim = clh.claim
                                                                                                                                                                        and		clh0.transac =
                                                                                                                                                                                        (	select	max(clh1.transac)
                                                                                                                                                                                                from	usinsug01.claim_his clh1
                                                                                                                                                                                                where 	clh1.claim = clh.claim
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
                                                                                                                                                                        from	usinsug01.claim_his clh0
                                                                                                                                                                        where	clh0.claim = clh.claim
                                                                                                                                                                        and		clh0.transac =
                                                                                                                                                                                        (	select	max(clh1.transac)
                                                                                                                                                                                                from	usinsug01.claim_his clh1
                                                                                                                                                                                                where 	clh1.claim = clh.claim
                                                                                                                                                                                                and		clh1.transac <= clh.transac
                                                                                                                                                                                                and		clh1.exchange not in (1,0)))
                                                                                                                                                else	1 end end, 0)
                                                                        * (case when cla.bussityp = '2' then 100 else cla.share_coa end/100)
                                                                        * (cla.share_rea/100) as numeric(12,2))) VMTRESSG,
                                                        cla.branch || '-' || cla.product || '-' || coalesce(cla.sub_product,0) KABPRODT,
                                                        '' KRCFMPGI, --excluido
                                                        '' KMEDCB, --excluido
                                                        '' KMEDPG, --excluido
                                                        '' DUSRORI, --excluido
                                                        '' DUSRAPR, --excluido
                                                        case	when cla.bussityp = '1' and cla.share_coa = 100 then '1' --Sin coaseguro
                                                                        when cla.bussityp = '1' and cla.share_coa <> 100 then '2' --Con coaseguro
                                                                        when cla.bussityp = '2' then '3' --LP compa  a no l der
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
                                                        cast(coalesce(cla.branch_bal,0) as varchar) KGCRAMO_SAP,
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
                                        from    (   select	cla.claim,
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
                                                                                                                        from 	usinsug01.certificat cer
                                                                                                                        where	cer.usercomp = cla.usercomp 
                                                                                                                        and 	cer.company = cla.company
                                                                                                                        and 	cer.certype = pol.certype
                                                                                                                        and 	cer.branch = cla.branch 
                                                                                                                        and 	cer.policy = cla.policy
                                                                                                                        and		cer.certif = cla.certif) end date_origi,
                                                                                coalesce(   coalesce((  select  max(cpl.currency)
                                                                                from    usinsug01.curren_pol cpl
                                                                                where 	cpl.usercomp = cla.usercomp and cpl.company = cla.company and cpl.certype = pol.certype
                                                                                and     cpl.branch = cla.branch and cpl.policy = cla.policy and cpl.certif = cla.certif),
                                                                                (   select  max(cpl.currency)
                                                                                from    usinsug01.curren_pol cpl
                                                                                where 	cpl.usercomp = cla.usercomp and cpl.company = cla.company and cpl.certype = pol.certype
                                                                                and     cpl.branch = cla.branch and cpl.policy = cla.policy)),0) moneda_cod,
                                                                                coalesce((	select	coi.share
                                                                                                        from	usinsug01.coinsuran coi
                                                                                                        where   coi.usercomp = cla.usercomp and coi.company = cla.company and coi.certype = pol.certype
                                                                                                        and     coi.branch = cla.branch and coi.policy = cla.policy and coi.effecdate <= cla.occurdat
                                                                                                        and     (coi.nulldate is null or coi.nulldate > cla.occurdat)
                                                                                                        and 	coalesce(coi.companyc,0) in (1,12)),100) share_coa,
                                                                                coalesce((	select  sum(coalesce(rei.share,0))
                                                                                                        from    usinsug01.reinsuran rei
                                                                                                        where   rei.usercomp = cla.usercomp and rei.company = cla.company and rei.certype = pol.certype
                                                                                                        and		rei.branch = cla.branch and rei.policy = cla.policy and rei.certif = cla.certif
                                                                                                        and		rei.effecdate <= cla.occurdat and (rei.nulldate is null or rei.nulldate > cla.occurdat)
                                                                                                        and     coalesce(rei.type,0) = 1),100) share_rea,
                                                                                coalesce((	select 	acc.branch_bal
                                                                                                        from	usinsug01.acc_autom2 acc
                                                                                                        where	ctid =
                                                                                                                        coalesce(
                                                                                                                (   select  min(ctid) --b squeda normal en acc_autom2(ramo;producto;concepto 1 para todos, excepto incendio)
                                                                                                                from    usinsug01.acc_autom2 abe
                                                                                                                where   abe.usercomp = cla.usercomp
                                                                                                                and		abe.company = cla.company
                                                                                                                and		abe.branch = cla.branch
                                                                                                                and 	abe.product = pol.product
                                                                                                                and 	abe.concept_fac = 1),
                                                                                                                (   select  min(ctid) --b squeda igual a anterior, pero en caso el producto no exista en acc_autom2 (error LP)
                                                                                                                from    usinsug01.acc_autom2 abe
                                                                                                                where   abe.usercomp = cla.usercomp
                                                                                                                and		abe.company = cla.company
                                                                                                                and		abe.branch = cla.branch
                                                                                                                and 	abe.concept_fac = 1))),0) branch_bal,
                                                                                (	select	sub_product
                                                                                        from	usinsug01.pol_subproduct
                                                                                        where	usercomp = cla.usercomp
                                                                                        and 	company = cla.company
                                                                                        and		certype = pol.certype
                                                                                        and		branch = cla.branch
                                                                                        and		policy = cla.policy
                                                                                        and		product = pol.product) sub_product
                                                                from    (	select	ctid pol_id
                                                                                        from	usinsug01.policy
                                                                                        where	usercomp = 1
                                                                                        and		company = 1
                                                                                        and		certype = '2'
                                                                                        and		branch in (select branch from usinsug01.table10b where company = 1)
                                                                                        and		branch not in (1,23,66)) po0 --exclusi n de estos ramos por el volumen de datos involucrados (23,66) y la metodolog a de obtenci n (1)
                                                                join	usinsug01.policy pol on pol.ctid = po0.pol_id
                                                                join	usinsug01.claim cla
                                                                        on		cla.usercomp = pol.usercomp
                                                                        and     cla.company = pol.company
                                                                        and     cla.branch = pol.branch
                                                                        and     cla.policy = pol.policy
                                                                        and		cla.staclaim <> '6'
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
                                        join 	usinsug01.claim_his clh
                                                        on		clh.claim = cla.claim
                                                        and		trim(clh.oper_type) in (select cast(operation as varchar(2)) from usinsug01.tab_cl_ope where pay_amount in (1))
                                                        and		clh.operdate <= '12/31/2023' --en caso de ejecutar a una fecha posterior, alterar esta fecha a la de la ejecuci n
                                                        and		coalesce(clh.amount,0) <> 0
                                        ) AS TMP               
                                        '''   
   
  DF_LPG_INSUNIX_OTROS_RAMOS = glue_context.read.format('jdbc').options(**connection).option("fetchsize",10000).option("dbtable",L_RBRECIN_INSUNIX_LPG_OTROS_RAMOS).load()
  
  L_RBRECIN_INSUNIX_LPG_INCENDIO = '''
                                        (
                                                select	'D' INDDETREC,
                                                        'RBRECIN' TABLAIFRS17,
                                                        '' PK,
                                                        '' DTPREG, --excluido
                                                        '' TIOCPROC, --excluido
                                                        clh.operdate TIOCFRM,
                                                        '' TIOCTO, --excluido
                                                        '' KRITPREG, --indeterminado (pendiente coordinar con FID/LP)
                                                        '' KRITPRGI, --indeterminado (pendiente coordinar con FID/LP)
                                                        case	when coalesce(coalesce(cl0.monto_clm,0),0) <= 0
                                                                        then 1
                                                                        else 2 end KRCTPRCI,
                                                        'PIG' KGIORIGM, --excluido
                                                        cast(cla.branch as varchar) KGCRAMO,
                                                        cast(cla.policy as varchar) DNUMAPO,
                                                        cast(cla.certif as varchar) DNMCERT,
                                                        cast(clh.claim as varchar) DNUMSIN,
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
                                                        coalesce((	select	cast(max(cl0.operdate) as varchar)
                                                                                from	usinsug01.claim_his cl0
                                                                                where 	cl0.claim = cla.claim
                                                                                and		cl0.transac = 	
                                                                                                (	select 	max(csp.transac)
                                                                                                        from	usinsug01.claim_pay_sap csp
                                                                                                        where	csp.claim = cl0.claim 
                                                                                                        and 	csp.transac_pay = clh.transac)),'') TPGCOB,
                                                        '' TANSUSP, --PENDIENTE CONOCER SI ES POSIBLE/VIALBE
                                                        '' KRCESPRI, --no disponible
                                                        '' KRCESTRI, --no disponible
                                                        '' KRCMOSTI, --excluido
                                                        cast(	coalesce(   coalesce((  select  max(cpl.currency)
                                                                                from    usinsug01.curren_pol cpl
                                                                                where 	cpl.usercomp = cla.usercomp and cpl.company = cla.company and cpl.certype = pol.certype
                                                                                and     cpl.branch = cla.branch and cpl.policy = cla.policy and cpl.certif = cla.certif),
                                                                                (   select  max(cpl.currency)
                                                                                from    usinsug01.curren_pol cpl
                                                                                where 	cpl.usercomp = cla.usercomp and cpl.company = cla.company and cpl.certype = pol.certype
                                                                                and     cpl.branch = cla.branch and cpl.policy = cla.policy)),0) as varchar) KRCMOEDA,
                                                        '' VCAMBIO, --no disponible
                                                        cast(abs(cl0.monto_clm) as numeric (12,2)) VMTTOTRI,
                                                        '' VMTIVA, --no disponible
                                                        '' VMTIRSRT, --no disponible
                                                        '' VTXIRSRT, --no disponible
                                                        '' VMTLIQRC, --excluido
                                                        abs(cast(cl0.monto_clm * (case when pol.bussityp = '2' then 100 else cl0.share_coa end/100) as numeric(12,2))) VMTCOSEG,
                                                        abs(cast(cl0.monto_clm * (case when pol.bussityp = '2' then 100 else cl0.share_coa end/100) * (cl0.share_rea/100) as numeric(12,2))) VMTRESSG,
                                                        cla.branch || '-' || cla.product || '- 0' KABPRODT,
                                                        '' KRCFMPGI, --excluido
                                                        '' KMEDCB, --excluido
                                                        '' KMEDPG, --excluido
                                                        '' DUSRORI, --excluido
                                                        '' DUSRAPR, --excluido
                                                        case	when pol.bussityp = '1' and cl0.share_coa = 100 then '1' --Sin coaseguro
                                                                        when pol.bussityp = '1' and cl0.share_coa <> 100 then '2' --Con coaseguro
                                                                        when pol.bussityp = '2' then '3' --LP compa  a no l der
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
                                                        cast(clh.claim as varchar) KSBSIN,
                                                        cla.branch || '-' || pol.product || '-' || cla.policy || '-' || cla.certif KABAPOL,
                                                        '' KABAPOL_EFT, --excluido
                                                        coalesce(cast(	case	coalesce(cla.certif,0)
                                                                                                        when	0
                                                                                                        then	pol.date_origi
                                                                                                        else	(	select	cer.date_origi
                                                                                                                                from 	usinsug01.certificat cer
                                                                                                                                where	cer.usercomp = cla.usercomp 
                                                                                                                                and 	cer.company = cla.company
                                                                                                                                and 	cer.certype = pol.certype
                                                                                                                                and 	cer.branch = cla.branch 
                                                                                                                                and 	cer.policy = cla.policy
                                                                                                                                and		cer.certif = cla.certif)
                                                                                                        end	as varchar),'') TINICIOA,
                                                        cl0.branch_led KGCRAMO_SAP,
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
                                                        coalesce((	select	evi.scod_vt
                                                                                from	usinsug01.equi_vt_inx evi 
                                                                                where	evi.scod_inx = cla.client),
                                                                coalesce(cla.client,'')) KEBENTID_TO,
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
                                        from	(	select	clh.cla_id,
                                                                                clh.clh_id,
                                                                                clh.pol_id,
                                                                                clh.share_coa,
                                                                                clh.share_rea,
                                                                                coalesce((	select	cast(gco.bill_item as varchar)
                                                                                                        from	usinsug01.gen_cover gco
                                                                                                        where	gco.ctid = 
                                                                                                                        coalesce((	select  max(ctid)
                                                                                                                                from	usinsug01.gen_cover
                                                                                                                                where	usercomp = clh.usercomp and company = clh.company and branch = clh.branch
                                                                                                                                and		product = clh.product and currency = clh.moneda_cod and modulec = clh.modulec_cla
                                                                                                                                and		cover =	clm.cover and effecdate <= clh.occurdat and (nulldate is null or nulldate > clh.occurdat)
                                                                                                                                and     statregt <> '4'), --variaci n 3 reg. v lido
                                                                                                                                (   select  max(ctid)
                                                                                                                                from	usinsug01.gen_cover
                                                                                                                                where	usercomp = clh.usercomp and company = clh.company and branch = clh.branch
                                                                                                                                and		product = clh.product and currency = clh.moneda_cod
                                                                                                                                and		cover = clm.cover and effecdate <= clh.occurdat and (nulldate is null or nulldate > clh.occurdat)
                                                                                                                                and     statregt <> '4'))),'0') branch_led,
                                                                                sum(coalesce(	case	clh.moneda_cod
                                                                                                                when	clm.currency
                                                                                                                then    coalesce(clm.amount,0)
                                                                                                                else	coalesce(clm.amount,0) *
                                                                                                                                case	when	clm.currency in (1,2)
                                                                                                                                                then	case	when clm.currency = 2
                                                                                                                                                                                then clh.clh_exchange
                                                                                                                                                                                else 1 / clh.clh_exchange end
                                                                                                                                                else	0 end end, 0)) monto_clm
                                                                from    (   select  cla.claim,
                                                                                                        cla.usercomp,
                                                                                                        cla.company,
                                                                                                        cla.branch,
                                                                                                        cla.policy,
                                                                                                        cla.certif,
                                                                                                        cla.occurdat,
                                                                                                        cla.product,
                                                                                                        cla.bussityp,
                                                                                                        cla.moneda_cod,
                                                                                                        cla.share_coa,
                                                                                                        cla.share_rea,
                                                                                                        cla.modulec_cla,
                                                                                clh.transac,
                                                                                clh.oper_type,
                                                                                clh.operdate,
                                                                                cla.cla_id,
                                                                                cla.pol_id,
                                                                                clh.ctid clh_id,
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
                                                                        from    (   select  cla.claim,
                                                                                                                                cla.usercomp,
                                                                                                                                cla.company,
                                                                                                                                cla.branch,
                                                                                                                                cla.policy,
                                                                                                                                cla.certif,
                                                                                                                                cla.occurdat,
                                                                                                                                cla.ctid cla_id,
                                                                                                                                pol.ctid pol_id,
                                                                                                                                pol.product,
                                                                                                                                pol.bussityp,
                                                                                                coalesce(   coalesce((  select  max(cpl.currency)
                                                                                                                from    usinsug01.curren_pol cpl
                                                                                                                where 	cpl.usercomp = cla.usercomp and cpl.company = cla.company and cpl.certype = pol.certype
                                                                                                                and     cpl.branch = cla.branch and cpl.policy = cla.policy and cpl.certif = cla.certif),
                                                                                                        (   select  max(cpl.currency)
                                                                                                                from    usinsug01.curren_pol cpl
                                                                                                                where 	cpl.usercomp = cla.usercomp and cpl.company = cla.company and cpl.certype = pol.certype
                                                                                                                and     cpl.branch = cla.branch and cpl.policy = cla.policy)),0) moneda_cod,
                                                                                                                                case	pol.bussityp
                                                                                                                                                when	'2'
                                                                                                                                                then 	100 - coalesce(pol.leadshare,0)
                                                                                                                                                when	'3'
                                                                                                                                                then 	null
                                                                                                                                                else	coalesce((	select	coi.share
                                                                                                                                                                                        from	usinsug01.coinsuran coi
                                                                                                                                                                                        where   coi.usercomp = cla.usercomp and coi.company = cla.company and coi.certype = pol.certype
                                                                                                                                                                                        and     coi.branch = cla.branch and coi.policy = cla.policy and coi.effecdate <= cla.occurdat
                                                                                                                                                                                        and     (coi.nulldate is null or coi.nulldate > cla.occurdat)
                                                                                                                                                                                        and 	coalesce(coi.companyc,0) in (1,12)),100) end share_coa,
                                                                                                                                coalesce((	select  sum(coalesce(rei.share,0))
                                                                                                                                                        from    usinsug01.reinsuran rei
                                                                                                                                                        where   rei.usercomp = cla.usercomp and rei.company = cla.company and rei.certype = pol.certype
                                                                                                                                                        and		rei.branch = cla.branch and rei.policy = cla.policy and rei.certif = cla.certif
                                                                                                                                                        and		rei.effecdate <= cla.occurdat and (rei.nulldate is null or rei.nulldate > cla.occurdat)
                                                                                                                                                        and     coalesce(rei.type,0) = 1),100) share_rea,
                                                                                                                                coalesce((	select  min(coalesce(modulec,0))
                                                                                                                                        from    usinsug01.modules
                                                                                                                                        where	usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                        and		branch = cla.branch and policy = cla.policy and certif = cla.certif
                                                                                                                                        and     effecdate <= cla.occurdat and (nulldate is null or nulldate > cla.occurdat)
                                                                                                                                        and     coalesce(modulec,0) > 0),0) modulec_cla
                                                                                                                from    (	select	ctid pol_id
                                                                                                                                        from	usinsug01.policy
                                                                                                                                        where	usercomp = 1
                                                                                                                                        and		company = 1
                                                                                                                                        and		certype = '2'
                                                                                                                                        and		branch = 1) po0 --consideraci n solo de este ramo por el volumen y reglas aplicadas
                                                                                                                join	usinsug01.policy pol on pol.ctid = po0.pol_id
                                                                                                                join	usinsug01.claim cla
                                                                                                                        on		cla.usercomp = pol.usercomp
                                                                                                                        and     cla.company = pol.company
                                                                                                                        and     cla.branch = pol.branch
                                                                                                                        and     cla.policy = pol.policy
                                                                                                                        and		cla.staclaim <> '6'
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
                                                                                join 	usinsug01.claim_his clh
                                                                                                on		clh.claim = cla.claim
                                                                                                and		trim(clh.oper_type) in (select cast(operation as varchar(2)) from usinsug01.tab_cl_ope where pay_amount in (1))
                                                                                                and		clh.operdate <= '12/31/2023') clh --en caso de ejecutar a una fecha posterior, alterar esta fecha a la de la ejecuci n
                                                                join 	usinsug01.cl_m_cover clm
                                                                on		clm.usercomp = clh.usercomp
                                                                and 	clm.company = clh.company
                                                                and 	clm.claim = clh.claim
                                                                and 	clm.movement = clh.transac
                                                                group	by 1,2,3,4,5,6) cl0
                                        join	usinsug01.claim cla on cla.ctid = cl0.cla_id
                                        join	usinsug01.policy pol on pol.ctid = cl0.pol_id
                                        join	usinsug01.claim_his clh on clh.ctid = cl0.clh_id
                                        where 	coalesce(cl0.monto_clm,0) <> 0
                                        ) AS TMP
                                        '''
  
  DF_LPG_INSUNIX_INCENDIO = glue_context.read.format('jdbc').options(**connection).option("fetchsize",10000).option("dbtable",L_RBRECIN_INSUNIX_LPG_INCENDIO).load()
  
  L_RBRECIN_INSUNIX_LPG_AMF = '''
                                        (
                                                select	        'D' INDDETREC,
                                                                'RBRECIN' TABLAIFRS17,
                                                                '' PK,
                                                                '' DTPREG, --excluido
                                                                '' TIOCPROC, --excluido
                                                                clh.operdate TIOCFRM,
                                                                '' TIOCTO, --excluido
                                                                '' KRITPREG, --indeterminado (pendiente coordinar con FID/LP)
                                                                '' KRITPRGI, --indeterminado (pendiente coordinar con FID/LP)
                                                                case	when coalesce(coalesce(clh.amount,0),0) <= 0
                                                                                then 1
                                                                                else 2 end KRCTPRCI,
                                                                'PIG' KGIORIGM,
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
                                                                coalesce((	select	cast(cast(max(cl0.operdate) as date) as varchar)
                                                                                        from	usinsug01.claim_his cl0
                                                                                        where 	cl0.claim = clh.claim
                                                                                        and		cl0.transac = 	
                                                                                                        (	select 	max(csp.transac)
                                                                                                                from	usinsug01.claim_pay_sap csp
                                                                                                                where	csp.claim = cl0.claim 
                                                                                                                and 	csp.transac_pay = clh.transac)),'') TPGCOB,
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
                                                                                                                                                                                from	usinsug01.claim_his clh0
                                                                                                                                                                                where	clh0.claim = clh.claim
                                                                                                                                                                                and		clh0.transac =
                                                                                                                                                                                                (	select	max(clh1.transac)
                                                                                                                                                                                                        from	usinsug01.claim_his clh1
                                                                                                                                                                                                        where 	clh1.claim = clh.claim
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
                                                                                                                                                                                from	usinsug01.claim_his clh0
                                                                                                                                                                                where	clh0.claim = clh.claim
                                                                                                                                                                                and		clh0.transac =
                                                                                                                                                                                                (	select	max(clh1.transac)
                                                                                                                                                                                                        from	usinsug01.claim_his clh1
                                                                                                                                                                                                        where 	clh1.claim = clh.claim
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
                                                                                                                                                                                from	usinsug01.claim_his clh0
                                                                                                                                                                                where	clh0.claim = clh.claim
                                                                                                                                                                                and		clh0.transac =
                                                                                                                                                                                                (	select	max(clh1.transac)
                                                                                                                                                                                                        from	usinsug01.claim_his clh1
                                                                                                                                                                                                        where 	clh1.claim = clh.claim
                                                                                                                                                                                                        and		clh1.transac <= clh.transac
                                                                                                                                                                                                        and		clh1.exchange not in (1,0)))
                                                                                                                                                        else	1 end end, 0)
                                                                                * (case when cla.bussityp = '2' then 100 else cla.share_coa end/100)
                                                                                * (cla.share_rea/100) as numeric(12,2))) VMTRESSG,
                                                                cla.branch || '-' || cla.product || '-' || coalesce(cla.sub_product,0) KABPRODT,
                                                                '' KRCFMPGI, --excluido
                                                                '' KMEDCB, --excluido
                                                                '' KMEDPG, --excluido
                                                                '' DUSRORI, --excluido
                                                                '' DUSRAPR, --excluido
                                                                case	when cla.bussityp = '1' and cla.share_coa = 100 then '1' --Sin coaseguro
                                                                                when cla.bussityp = '1' and cla.share_coa <> 100 then '2' --Con coaseguro
                                                                                when cla.bussityp = '2' then '3' --LP compa  a no l der
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
                                                                cast(coalesce(cla.branch_bal,0) as varchar) KGCRAMO_SAP,
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
                                                from    (   select	cla.claim,
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
                                                                                                                                from 	usinsug01.certificat cer
                                                                                                                                where	cer.usercomp = cla.usercomp 
                                                                                                                                and 	cer.company = cla.company
                                                                                                                                and 	cer.certype = pol.certype
                                                                                                                                and 	cer.branch = cla.branch 
                                                                                                                                and 	cer.policy = cla.policy
                                                                                                                                and		cer.certif = cla.certif) end date_origi,
                                                                                        coalesce(   coalesce((  select  max(cpl.currency)
                                                                                        from    usinsug01.curren_pol cpl
                                                                                        where 	cpl.usercomp = cla.usercomp and cpl.company = cla.company and cpl.certype = pol.certype
                                                                                        and     cpl.branch = cla.branch and cpl.policy = cla.policy and cpl.certif = cla.certif),
                                                                                        (   select  max(cpl.currency)
                                                                                        from    usinsug01.curren_pol cpl
                                                                                        where 	cpl.usercomp = cla.usercomp and cpl.company = cla.company and cpl.certype = pol.certype
                                                                                        and     cpl.branch = cla.branch and cpl.policy = cla.policy)),0) moneda_cod,
                                                                                        coalesce((	select	coi.share
                                                                                                                from	usinsug01.coinsuran coi
                                                                                                                where   coi.usercomp = cla.usercomp and coi.company = cla.company and coi.certype = pol.certype
                                                                                                                and     coi.branch = cla.branch and coi.policy = cla.policy and coi.effecdate <= cla.occurdat
                                                                                                                and     (coi.nulldate is null or coi.nulldate > cla.occurdat)
                                                                                                                and 	coalesce(coi.companyc,0) in (1,12)),100) share_coa,
                                                                                        coalesce((	select  sum(coalesce(rei.share,0))
                                                                                                                from    usinsug01.reinsuran rei
                                                                                                                where   rei.usercomp = cla.usercomp and rei.company = cla.company and rei.certype = pol.certype
                                                                                                                and		rei.branch = cla.branch and rei.policy = cla.policy and rei.certif = cla.certif
                                                                                                                and		rei.effecdate <= cla.occurdat and (rei.nulldate is null or rei.nulldate > cla.occurdat)
                                                                                                                and     coalesce(rei.type,0) = 1),100) share_rea,
                                                                                        coalesce((	select 	acc.branch_bal
                                                                                                                from	usinsug01.acc_autom2 acc
                                                                                                                where	ctid =
                                                                                                                                coalesce(
                                                                                                                        (   select  min(ctid) --b squeda normal en acc_autom2(ramo;producto;concepto 1 para todos, excepto incendio)
                                                                                                                        from    usinsug01.acc_autom2 abe
                                                                                                                        where   abe.usercomp = cla.usercomp
                                                                                                                        and		abe.company = cla.company
                                                                                                                        and		abe.branch = cla.branch
                                                                                                                        and 	abe.product = pol.product
                                                                                                                        and 	abe.concept_fac = 1),
                                                                                                                        (   select  min(ctid) --b squeda igual a anterior, pero en caso el producto no exista en acc_autom2 (error LP)
                                                                                                                        from    usinsug01.acc_autom2 abe
                                                                                                                        where   abe.usercomp = cla.usercomp
                                                                                                                        and		abe.company = cla.company
                                                                                                                        and		abe.branch = cla.branch
                                                                                                                        and 	abe.concept_fac = 1))),0) branch_bal,
                                                                                        (	select	sub_product
                                                                                                from	usinsug01.pol_subproduct
                                                                                                where	usercomp = cla.usercomp
                                                                                                and 	company = cla.company
                                                                                                and		certype = pol.certype
                                                                                                and		branch = cla.branch
                                                                                                and		policy = cla.policy
                                                                                                and		product = pol.product) sub_product
                                                                        from    (	select	ctid pol_id
                                                                                                from	usinsug01.policy
                                                                                                where	usercomp = 1
                                                                                                and		company = 1
                                                                                                and		certype = '2'
                                                                                                and		branch = 23) po0 --consideraci n solo de este ramo por el volumen y reglas aplicadas
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
                                                join 	usinsug01.claim_his clh
                                                                on		clh.claim = cla.claim
                                                                and		trim(clh.oper_type) in (select cast(operation as varchar(2)) from usinsug01.tab_cl_ope where pay_amount in (1))
                                                                and		clh.operdate <= '12/31/2023' --en caso de ejecutar a una fecha posterior, alterar esta fecha a la de la ejecuci n
                                                                and		coalesce(clh.amount,0) <> 0
                                        ) AS TMP
                                        '''
  
  DF_LPG_INSUNIX_AMF = glue_context.read.format('jdbc').options(**connection).option("fetchsize",10000).option("dbtable",L_RBRECIN_INSUNIX_LPG_AMF).load()

  L_RBRECIN_INSUNIX_LPG_SOAT = '''
                                        (
                                                select	'D' INDDETREC,
                                                'RBRECIN' TABLAIFRS17,
                                                '' PK,
                                                '' DTPREG, --excluido
                                                '' TIOCPROC, --excluido
                                                clh.operdate TIOCFRM,
                                                '' TIOCTO, --excluido
                                                '' KRITPREG, --indeterminado (pendiente coordinar con FID/LP)
                                                '' KRITPRGI, --indeterminado (pendiente coordinar con FID/LP)
                                                case	when coalesce(clh.amount,0) <= 0
                                                                then 1
                                                                else 2 end KRCTPRCI,
                                                'PIG' KGIORIGM, --excluido
                                                cast(cla.branch as varchar) KGCRAMO,
                                                cast(cla.policy as varchar) DNUMAPO,
                                                cast(cla.certif as varchar) DNMCERT,
                                                cast(cla.claim as varchar) DNUMSIN,
                                                cast(cla.num_case as varchar) DNUMSSIN,
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
                                                                        from	usinsug01.claim_his cl0
                                                                        where 	cl0.claim = clh.claim
                                                                        and		cl0.transac = 	
                                                                                        (	select 	max(csp.transac)
                                                                                                from	usinsug01.claim_pay_sap csp
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
                                                                                                                                                                from	usinsug01.claim_his clh0
                                                                                                                                                                where	clh0.claim = clh.claim
                                                                                                                                                                and		clh0.transac =
                                                                                                                                                                                (	select	max(clh1.transac)
                                                                                                                                                                                        from	usinsug01.claim_his clh1
                                                                                                                                                                                        where 	clh1.claim = clh.claim
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
                                                                                                                                                                from	usinsug01.claim_his clh0
                                                                                                                                                                where	clh0.claim = clh.claim
                                                                                                                                                                and		clh0.transac =
                                                                                                                                                                                (	select	max(clh1.transac)
                                                                                                                                                                                        from	usinsug01.claim_his clh1
                                                                                                                                                                                        where 	clh1.claim = clh.claim
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
                                                                                                                                                                from	usinsug01.claim_his clh0
                                                                                                                                                                where	clh0.claim = clh.claim
                                                                                                                                                                and		clh0.transac =
                                                                                                                                                                                (	select	max(clh1.transac)
                                                                                                                                                                                        from	usinsug01.claim_his clh1
                                                                                                                                                                                        where 	clh1.claim = clh.claim
                                                                                                                                                                                        and		clh1.transac <= clh.transac
                                                                                                                                                                                        and		clh1.exchange not in (1,0)))
                                                                                                                                        else	1 end end, 0)
                                                                * (case when cla.bussityp = '2' then 100 else cla.share_coa end/100)
                                                                * (cla.share_rea/100) as numeric(12,2))) VMTRESSG,
                                                cla.branch || '-' || cla.product || '-' || coalesce(cla.sub_product,0) KABPRODT,
                                                '' KRCFMPGI, --excluido
                                                '' KMEDCB, --excluido
                                                '' KMEDPG, --excluido
                                                '' DUSRORI, --excluido
                                                '' DUSRAPR, --excluido
                                                case	when cla.bussityp = '1' and cla.share_coa = 100 then '1' --Sin coaseguro
                                                                when cla.bussityp = '1' and cla.share_coa <> 100 then '2' --Con coaseguro
                                                                when cla.bussityp = '2' then '3' --LP compa  a no l der
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
                                                cast(coalesce(cla.branch_bal,0) as varchar) KGCRAMO_SAP,
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
                                from    (   select	cla.claim,
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
                                                                                                                from 	usinsug01.certificat cer
                                                                                                                where	cer.usercomp = cla.usercomp 
                                                                                                                and 	cer.company = cla.company
                                                                                                                and 	cer.certype = pol.certype
                                                                                                                and 	cer.branch = cla.branch 
                                                                                                                and 	cer.policy = cla.policy
                                                                                                                and		cer.certif = cla.certif) end date_origi,
                                                                        coalesce(   coalesce((  select  max(cpl.currency)
                                                                        from    usinsug01.curren_pol cpl
                                                                        where 	cpl.usercomp = cla.usercomp and cpl.company = cla.company and cpl.certype = pol.certype
                                                                        and     cpl.branch = cla.branch and cpl.policy = cla.policy and cpl.certif = cla.certif),
                                                                        (   select  max(cpl.currency)
                                                                        from    usinsug01.curren_pol cpl
                                                                        where 	cpl.usercomp = cla.usercomp and cpl.company = cla.company and cpl.certype = pol.certype
                                                                        and     cpl.branch = cla.branch and cpl.policy = cla.policy)),0) moneda_cod,
                                                                        coalesce((	select	coi.share
                                                                                                from	usinsug01.coinsuran coi
                                                                                                where   coi.usercomp = cla.usercomp and coi.company = cla.company and coi.certype = pol.certype
                                                                                                and     coi.branch = cla.branch and coi.policy = cla.policy and coi.effecdate <= cla.occurdat
                                                                                                and     (coi.nulldate is null or coi.nulldate > cla.occurdat)
                                                                                                and 	coalesce(coi.companyc,0) in (1,12)),100) share_coa,
                                                                        coalesce((	select  sum(coalesce(rei.share,0))
                                                                                                from    usinsug01.reinsuran rei
                                                                                                where   rei.usercomp = cla.usercomp and rei.company = cla.company and rei.certype = pol.certype
                                                                                                and		rei.branch = cla.branch and rei.policy = cla.policy and rei.certif = cla.certif
                                                                                                and		rei.effecdate <= cla.occurdat and (rei.nulldate is null or rei.nulldate > cla.occurdat)
                                                                                                and     coalesce(rei.type,0) = 1),100) share_rea,
                                                                        coalesce((	select 	acc.branch_bal
                                                                                                from	usinsug01.acc_autom2 acc
                                                                                                where	ctid =
                                                                                                                coalesce(
                                                                                                        (   select  min(ctid) --b squeda normal en acc_autom2(ramo;producto;concepto 1 para todos, excepto incendio)
                                                                                                        from    usinsug01.acc_autom2 abe
                                                                                                        where   abe.usercomp = cla.usercomp
                                                                                                        and		abe.company = cla.company
                                                                                                        and		abe.branch = cla.branch
                                                                                                        and 	abe.product = pol.product
                                                                                                        and 	abe.concept_fac = 1),
                                                                                                        (   select  min(ctid) --b squeda igual a anterior, pero en caso el producto no exista en acc_autom2 (error LP)
                                                                                                        from    usinsug01.acc_autom2 abe
                                                                                                        where   abe.usercomp = cla.usercomp
                                                                                                        and		abe.company = cla.company
                                                                                                        and		abe.branch = cla.branch
                                                                                                        and 	abe.concept_fac = 1))),0) branch_bal,
                                                                        (	select	sub_product
                                                                                from	usinsug01.pol_subproduct
                                                                                where	usercomp = cla.usercomp
                                                                                and 	company = cla.company
                                                                                and		certype = pol.certype
                                                                                and		branch = cla.branch
                                                                                and		policy = cla.policy
                                                                                and		product = pol.product) sub_product,
                                                                        (	select 	min(num_case)
                                                                                from	usinsug01.claim_case
                                                                                where 	usercomp = cla.usercomp
                                                                                and 	company = cla.company
                                                                                and 	claim = cla.claim) num_case
                                                        from    (	select	ctid pol_id
                                                                                from	usinsug01.policy
                                                                                where	usercomp = 1
                                                                                and		company = 1
                                                                                and		certype = '2'
                                                                                and		branch = 66) po0 --consideraci n solo de este ramo por el volumen y reglas aplicadas
                                                        join	usinsug01.policy pol on pol.ctid = po0.pol_id
                                                        join	usinsug01.claim cla
                                                                on		cla.usercomp = pol.usercomp
                                                                and     cla.company = pol.company
                                                                and     cla.branch = pol.branch
                                                                and     cla.policy = pol.policy
                                                                and		cla.staclaim <> '6'
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
                                join 	usinsug01.claim_his clh
                                                on		clh.claim = cla.claim
                                                and		trim(clh.oper_type) in (select cast(operation as varchar(2)) from usinsug01.tab_cl_ope where pay_amount in (1))
                                                and		clh.operdate <= '12/31/2023' --en caso de ejecutar a una fecha posterior, alterar esta fecha a la de la ejecuci n
                                                and		coalesce(clh.amount,0) <> 0
                                        ) AS TMP
                                        '''
  
  DF_LPG_INSUNIX_SOAT = glue_context.read.format('jdbc').options(**connection).option("fetchsize",10000).option("dbtable",L_RBRECIN_INSUNIX_LPG_SOAT).load()


  DF_LPG_INSUNIX = DF_LPG_INSUNIX_OTROS_RAMOS.union(DF_LPG_INSUNIX_INCENDIO).union(DF_LPG_INSUNIX_AMF).union(DF_LPG_INSUNIX_SOAT)
         
  L_RBRECIN_INSUNIX_LPV = '''
                             (
                                        select	'D' INDDETREC,
                                                'RBRECIN' TABLAIFRS17,
                                                '' PK,
                                                '' DTPREG, --excluido
                                                '' TIOCPROC, --excluido
                                                clh.operdate TIOCFRM,
                                                '' TIOCTO, --excluido
                                                '' KRITPREG, --indeterminado (pendiente coordinar con FID/LP)
                                                '' KRITPRGI, --indeterminado (pendiente coordinar con FID/LP)
                                                case	when coalesce(coalesce(clh.amount,0),0) <= 0
                                                                then 1
                                                                else 2 end KRCTPRCI,
                                                'PIV' KGIORIGM,
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
                                                                                                                                                                where	clh0.claim = clh.claim
                                                                                                                                                                and		clh0.transac =
                                                                                                                                                                                (	select	max(clh1.transac)
                                                                                                                                                                                        from	usinsuv01.claim_his clh1
                                                                                                                                                                                        where 	clh1.claim = clh.claim
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
                                                                                                                                                                where	clh0.claim = clh.claim
                                                                                                                                                                and		clh0.transac =
                                                                                                                                                                                (	select	max(clh1.transac)
                                                                                                                                                                                        from	usinsuv01.claim_his clh1
                                                                                                                                                                                        where 	clh1.claim = clh.claim
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
                                                                                                                                                                where	clh0.claim = clh.claim
                                                                                                                                                                and		clh0.transac =
                                                                                                                                                                                (	select	max(clh1.transac)
                                                                                                                                                                                        from	usinsuv01.claim_his clh1
                                                                                                                                                                                        where 	clh1.claim = clh.claim
                                                                                                                                                                                        and		clh1.transac <= clh.transac
                                                                                                                                                                                        and		clh1.exchange not in (1,0)))
                                                                                                                                        else	1 end end, 0)
                                                                * (case when cla.bussityp = '2' then 100 else cla.share_coa end/100) as numeric(12,2))) VMTRESSG, --no hay reaseguros en INX-LPV
                                                cla.branch || '-' || cla.product KABPRODT,
                                                '' KRCFMPGI, --excluido
                                                '' KMEDCB, --excluido
                                                '' KMEDPG, --excluido
                                                '' DUSRORI, --excluido
                                                '' DUSRAPR, --excluido
                                                case	when cla.bussityp = '1' and cla.share_coa = 100 then '1' --Sin coaseguro
                                                                when cla.bussityp = '1' and cla.share_coa <> 100 then '2' --Con coaseguro
                                                                when cla.bussityp = '2' then '3' --LP compa  a no l der
                                                                else '0' end KRCTPCSG,
                                                '' DCODENPG, --excluido
                                                '' DCODENRC, --excluido
                                                '' VMFAT, --no disponible
                                                'LPV' DCOMPA,
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
                                from    (   select	cla.claim,
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
                                                                        coalesce(   coalesce((  select  max(cpl.currency)
                                                                        from    usinsuv01.curren_pol cpl
                                                                        where 	cpl.usercomp = cla.usercomp and cpl.company = cla.company and cpl.certype = pol.certype
                                                                        and     cpl.branch = cla.branch and cpl.policy = cla.policy and cpl.certif = cla.certif),
                                                                        (   select  max(cpl.currency)
                                                                        from    usinsuv01.curren_pol cpl
                                                                        where 	cpl.usercomp = cla.usercomp and cpl.company = cla.company and cpl.certype = pol.certype
                                                                        and     cpl.branch = cla.branch and cpl.policy = cla.policy)),0) moneda_cod,
                                                                        coalesce((	select	coi.share
                                                                                                from	usinsuv01.coinsuran coi
                                                                                                where   coi.usercomp = cla.usercomp and coi.company = cla.company and coi.certype = pol.certype
                                                                                                and     coi.branch = cla.branch and coi.policy = cla.policy and coi.effecdate <= cla.occurdat
                                                                                                and     (coi.nulldate is null or coi.nulldate > cla.occurdat)
                                                                                                and 	coalesce(coi.companyc,0) in (1,12)),100) share_coa,
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
                                                                                                                                (select coalesce(coalesce(coalesce(coalesce(coalesce(
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
                                                                                                                                        and statusva not in ('2','3'))),
                                                                                                                                (   select max(ctid) from usinsuv01.life_prev
                                                                                                                                        where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                        and branch = cla.branch and policy = cla.policy and certif = cla.certif
                                                                                                                                        and effecdate <= cla.occurdat
                                                                                                                                        and (nulldate is null or nulldate < cla.occurdat)
                                                                                                                                        and statusva not in ('2','3'))),
                                                                                                                                (   select min(ctid) from usinsuv01.life_prev
                                                                                                                                        where usercomp = cla.usercomp and company = cla.company and certype = pol.certype
                                                                                                                                        and branch = cla.branch and policy = cla.policy and certif = cla.certif
                                                                                                                                        and effecdate > cla.occurdat
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
                                                                                                                                and     sbs.cod_sbs_gyp = tnb.cod_sbs_gyp),0) end branch_gyp
                                                        from    (	select	ctid pol_id
                                                                                from	usinsuv01.policy
                                                                                where	usercomp = 1
                                                                                and		company = 1
                                                                                and		certype = '2'
                                                                                and		branch in (select branch from usinsug01.table10b where company = 2)) po0
                                                        join	usinsuv01.policy pol on pol.ctid = po0.pol_id
                                                        join	usinsuv01.claim cla
                                                                on		cla.usercomp = pol.usercomp
                                                                and     cla.company = pol.company
                                                                and     cla.branch = pol.branch
                                                                and     cla.policy = pol.policy
                                                                and		cla.staclaim <> '6'
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
                                join 	usinsuv01.claim_his clh
                                                on		clh.claim = cla.claim
                                                and		trim(clh.oper_type) in (select cast(operation as varchar(2)) from usinsug01.tab_cl_ope where pay_amount in (1))
                                                and		clh.operdate <= '12/31/2023' --en caso de ejecutar a una fecha posterior, alterar esta fecha a la de la ejecuci n
                                                and		coalesce(clh.amount,0) <> 0
                             ) AS TMP       
                             '''   
        
  DF_LPV_INSUNIX = glue_context.read.format('jdbc').options(**connection).option("fetchsize",10000).option("dbtable",L_RBRECIN_INSUNIX_LPV).load()

  L_DF_RBRECIN_INSUNIX = DF_LPG_INSUNIX.union(DF_LPV_INSUNIX)

  L_RBRECIN_VTIME_LPG_AGRARIO = '''
                        (
                            select	'D' INDDETREC,
                                        'RBRECIN' TABLAIFRS17,
                                        '' PK,
                                        '' DTPREG, --excluido
                                        '' TIOCPROC, --excluido
                                        cast(clh."DOPERDATE" as date) TIOCFRM,
                                        '' TIOCTO, --excluido
                                        '' KRITPREG, --indeterminado (pendiente coordinar con FID/LP)
                                        '' KRITPRGI, --indeterminado (pendiente coordinar con FID/LP)
                                        case	when coalesce(cl0.namount,0) <= 0
                                                        then 1
                                                        else 2 end KRCTPRCI,
                                        'PVG' KGIORIGM,
                                        cast(cla."NBRANCH" as varchar) KGCRAMO,
                                        cast(cla."NPOLICY" as varchar) DNUMAPO,
                                        cast(cla."NCERTIF" as varchar) DNMCERT,
                                        cast(cla."NCLAIM" as varchar) DNUMSIN,
                                        cast(clh."NCASE_NUM" as varchar) DNUMSSIN,
                                        '' DNUMPENS, --excluido
                                        clh."NCLAIM" || '-' || clh."NCASE_NUM" || '-' || clh."NTRANSAC" DNUMREC,
                                        '' DNMAGRE, --no disponible
                                        '' NSAGREG, --excluido
                                        '' NSEQSIN, --excluido
                                        coalesce(cast(extract(year from cla."DOCCURDAT") as varchar),'') DANOSIN,
                                        coalesce(cast(cast(clh."DOPERDATE" as date) as varchar),'') TEMISSAO,
                                        '' TINICIO, --no disponible
                                        '' TTERMO, --no disponible
                                        '' TESTADO, --no disponible
                                        /*
                                        coalesce((	select	cast(cast(max(cl1."DOPERDATE") as date) as varchar)
                                                                from	usvtimg01."CLAIM_HIS" cl1
                                                                where 	cl1."NCLAIM" = clh."NCLAIM"
                                                                and		cl1."NCASE_NUM" = clh."NCASE_NUM"
                                                                and		cl1."NDEMAN_TYPE" = clh."NDEMAN_TYPE"
                                                                and		cl1."NTRANSAC" = 
                                                                                (	select 	max(csp."NTRANSAC")
                                                                                        from	usvtimg01."WBSTBLCLAIM_DOC_SAP"  csp
                                                                                        where	csp."NCLAIM" = cl1."NCLAIM"
                                                                                        and		csp."NCASE_NUM" = cl1."NCASE_NUM"
                                                                                        and		csp."NDEMAN_TYPE" = cl1."NDEMAN_TYPE"
                                                                                        and 	csp."NTRANSAC" = cl1."NTRANSCLAIMREV")),'')*/ '' TPGCOB, --pendiente evaluar si est  bien aplicado
                                        '' TANSUSP, --PENDIENTE CONOCER SI ES POSIBLE/VIABLE
                                        '' KRCESPRI, --no disponible
                                        '' KRCESTRI, --no disponible
                                        '' KRCMOSTI, --excluido
                                        cast(	coalesce(
                                                coalesce((  select  max(cpl."NCURRENCY")
                                                        from    usvtimg01."CURREN_POL" cpl
                                                        where 	cpl."SCERTYPE" = cla."SCERTYPE" and	cpl."NBRANCH" = cla."NBRANCH"
                                                        and		cpl."NPRODUCT" = pol."NPRODUCT" and cpl."NPOLICY" = cla."NPOLICY"
                                                        and     cpl."NCERTIF" = cla."NCERTIF"),
                                                        (   select  max(cpl."NCURRENCY")
                                                        from    usvtimg01."CURREN_POL" cpl
                                                        where 	cpl."SCERTYPE" = cla."SCERTYPE" and	cpl."NBRANCH" = cla."NBRANCH"
                                                        and		cpl."NPRODUCT" = pol."NPRODUCT" and cpl."NPOLICY" = cla."NPOLICY")),0) as varchar) KRCMOEDA,
                                        '' VCAMBIO, --no disponible
                                        abs(cast(cl0.namount as numeric(12,2))) VMTTOTRI,
                                        '' VMTIVA, --no disponible
                                        '' VMTIRSRT, --no disponible
                                        '' VTXIRSRT, --no disponible
                                        '' VMTLIQRC, --excluido
                                        abs(cast(coalesce(cl0.namount, 0) * (case when pol."SBUSSITYP" = '2' then 100 else cl0.nshare_coa end/100) as numeric(12,2))) VMTCOSEG,
                                        abs(cast(cl0.namount *
                                                        (case when pol."SBUSSITYP" = '2' then 100 else nshare_coa end / 100) *
                                                        case 	when	not(cl0.namount <> 0 and cl0.flag_rea <> 0)
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
                                                                                                                                                                cl0.namount ratio_rea
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
                                                                                                                                                                                                        where   gco."NCOVER" = cm0."NCOVER"
                                                                                                                                                                                                        and     gco."NPRODUCT" = pol."NPRODUCT"
                                                                                                                                                                                                        and     gco."NMODULEC" = cm0."NMODULEC"
                                                                                                                                                                                                        and     gco."NBRANCH" = cla."NBRANCH"
                                                                                                                                                                                                        and     cast(gco."DEFFECDATE" as date) <= cast(cla."DOCCURDAT" as date)
                                                                                                                                                                                                        and     (gco."DNULLDATE" is null or cast(gco."DNULLDATE" as date) > cast(cla."DOCCURDAT" as date))
                                                                                                                                                                                                        and     gco."SSTATREGT" <> '4'
                                                                                                                                                                                                        and		gco."NBRANCH_LED" = 58)
                                                                                                                                                                                                        else	(   select  gco."NBRANCH_REI"
                                                                                                                                                                                                        from    usvtimg01."GEN_COVER" gco
                                                                                                                                                                                                        where   gco."NCOVER" = cm0."NCOVER"
                                                                                                                                                                                                        and     gco."NPRODUCT" = pol."NPRODUCT"
                                                                                                                                                                                                        and     gco."NMODULEC" = cm0."NMODULEC"
                                                                                                                                                                                                        and     gco."NBRANCH" = cla."NBRANCH"
                                                                                                                                                                                                        and     cast(gco."DEFFECDATE" as date) <= cast(cla."DOCCURDAT" as date)
                                                                                                                                                                                                        and     (gco."DNULLDATE" is null or cast(gco."DNULLDATE" as date) > cast(cla."DOCCURDAT" as date))
                                                                                                                                                                                                        and     gco."SSTATREGT" <> '4'
                                                                                                                                                                                                        and		gco."NBRANCH_LED" = 58) end nbranch_rei,
                                                                                                                                                                                        sum(case	coalesce(
                                                                                                                                                                                                coalesce((  select  max(cpl."NCURRENCY")
                                                                                                                                                                                                from    usvtimg01."CURREN_POL" cpl
                                                                                                                                                                                                where 	cpl."SCERTYPE" = cla."SCERTYPE" and	cpl."NBRANCH" = cla."NBRANCH"
                                                                                                                                                                                                and		cpl."NPRODUCT" = pol."NPRODUCT" and cpl."NPOLICY" = cla."NPOLICY"
                                                                                                                                                                                                and     cpl."NCERTIF" = cla."NCERTIF"),
                                                                                                                                                                                        (   select  max(cpl."NCURRENCY")
                                                                                                                                                                                                from    usvtimg01."CURREN_POL" cpl
                                                                                                                                                                                                where 	cpl."SCERTYPE" = cla."SCERTYPE" and	cpl."NBRANCH" = cla."NBRANCH"
                                                                                                                                                                                                and		cpl."NPRODUCT" = pol."NPRODUCT" and cpl."NPOLICY" = cla."NPOLICY")),0)
                                                                                                                                                                                                                when	1
                                                                                                                                                                                                                then	case	when	cm0."NCURRENCY" = 1
                                                                                                                                                                                                                                                then	cm0."NAMOUNT"
                                                                                                                                                                                                                                                when	cm0."NCURRENCY" = 2
                                                                                                                                                                                                                                                then	cm0."NAMOUNT" * cm0."NEXCHANGE"
                                                                                                                                                                                                                                                else	0 end
                                                                                                                                                                                                                when	2
                                                                                                                                                                                                                then	case	when	cm0."NCURRENCY" = 2
                                                                                                                                                                                                                                                then	cm0."NAMOUNT"
                                                                                                                                                                                                                                                when	cm0."NCURRENCY" = 1
                                                                                                                                                                                                                                                then	cm0."NLOC_AMOUNT"
                                                                                                                                                                                                                                                else	0 end
                                                                                                                                                                                                                else	0 end) monto_trans
                                                                                                                                                                        from 	usvtimg01."CLAIM_HIS" ch0,
                                                                                                                                                                                        usvtimg01."CL_M_COVER" cm0
                                                                                                                                                                        where	ch0.ctid = cl0.clh_id
                                                                                                                                                                        and		cm0."NCLAIM" = ch0."NCLAIM"
                                                                                                                                                                        and 	cm0."NCASE_NUM" = ch0."NCASE_NUM"
                                                                                                                                                                        and 	cm0."NDEMAN_TYPE" = ch0."NDEMAN_TYPE"
                                                                                                                                                                        and 	cm0."NTRANSAC" = ch0."NTRANSAC"
                                                                                                                                                                        group	by 1) cl1
                                                                                                                                                where	nbranch_rei is not null
                                                                                                                                                and		coalesce(cl1.monto_trans,0) <> 0) cl1) rea) end as numeric(12,2))) VMTRESSG,
                                        cla."NBRANCH" || '-' || pol."NPRODUCT" KABPRODT,
                                        '' KRCFMPGI, --excluido
                                        '' KMEDCB, --excluido
                                        '' KMEDPG, --excluido
                                        '' DUSRORI, --excluido
                                        '' DUSRAPR, --excluido
                                        case	when pol."SBUSSITYP" = '1' and cl0.nshare_coa = 100 then '1' --Sin coaseguro 
                                                        when pol."SBUSSITYP" = '1' and cl0.nshare_coa <> 100 then '2' --Con coaseguro, LP compa  a l der
                                                        when pol."SBUSSITYP" = '2' then '3' --LP compa  a no l der
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
                                        cast(cla."NCLAIM" as varchar) KSBSIN,
                                        cla."NBRANCH" || '-' || pol."NPRODUCT" || '-' || cla."NPOLICY" || '-' || cla."NCERTIF" KABAPOL,
                                        '' KABAPOL_EFT, --excluido
                                        coalesce(cast(	case	coalesce(cla."NCERTIF",0)
                                                                                        when	0
                                                                                        then	cast(pol."DDATE_ORIGI" as date) 
                                                                                        else	(	select	cast(cer."DDATE_ORIGI" as date)
                                                                                                                from 	usvtimg01."CERTIFICAT" cer
                                                                                                                where	cer."SCERTYPE" = pol."SCERTYPE"
                                                                                                                and 	cer."NBRANCH" = cla."NBRANCH"
                                                                                                                and 	cer."NPOLICY" = cla."NPOLICY"
                                                                                                                and		cer."NCERTIF" = cla."NCERTIF"
                                                                                                                and		cer."NDIGIT" = 0) 
                                                                                        end as varchar),'') TINICIOA,
                                        '58' KGCRAMO_SAP, -- nico ramo contable conocido para el ramo comercial 58
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
                                        coalesce(cla."SCLIENT",'') KEBENTID_TO,
                                        '' TCONTAB, --excluido
                                        coalesce(cast(extract(year from cla."DOCCURDAT") as varchar),'') DANOABER,
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
                        from    (   select  cla.cla_id,
                                                                cla.pol_id,
                                                                cla.nshare_coa,
                                                                cla.flag_rea,
                                                                clh.ctid clh_id,					
                                                                sum(case	when	cla.moneda_cod = 1 
                                                                                        then	coalesce(clh."NAMOUNT",0) 
                                                                                                        *	case	when	clh."NCURRENCY" = 2
                                                                                                                                then	clh."NEXCHANGE"
                                                                                                                                else	1 end
                                                                                        when	cla.moneda_cod = 2
                                                                                        then	case	when	clh."NCURRENCY" = 2
                                                                                                                        then	coalesce(clh."NAMOUNT",0)
                                                                                                                        else	coalesce(clh."NLOC_AMOUNT",0) end end) namount
                                from    (   select  cla."NCLAIM" nclaim,
                                                                                        cla.ctid cla_id,
                                                                                        pol.ctid pol_id,
                                                                coalesce(
                                                                        coalesce((  select  max(cpl."NCURRENCY")
                                                                        from    usvtimg01."CURREN_POL" cpl
                                                                        where 	cpl."SCERTYPE" = cla."SCERTYPE" and	cpl."NBRANCH" = cla."NBRANCH"
                                                                        and		cpl."NPRODUCT" = pol."NPRODUCT" and cpl."NPOLICY" = cla."NPOLICY"
                                                                        and     cpl."NCERTIF" = cla."NCERTIF"),
                                                                (   select  max(cpl."NCURRENCY")
                                                                        from    usvtimg01."CURREN_POL" cpl
                                                                        where 	cpl."SCERTYPE" = cla."SCERTYPE" and	cpl."NBRANCH" = cla."NBRANCH"
                                                                        and		cpl."NPRODUCT" = pol."NPRODUCT" and cpl."NPOLICY" = cla."NPOLICY")),0) moneda_cod,
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
                                                                                        case	when	EXISTS
                                                                                                                        (	SELECT  1
                                                                                                                                from	usvtimg01."REINSURAN" REI
                                                                                                                                where	REI."SCERTYPE" = pol."SCERTYPE" AND REI."NBRANCH" = cla."NBRANCH"
                                                                                                                                and		REI."NPRODUCT" = pol."NPRODUCT" AND REI."NPOLICY" = cla."NPOLICY"
                                                                                                                                and		REI."NCERTIF" = case when "SPOLITYPE" = '3' then 0 else cla."NCERTIF" end
                                                                                                                                and		CAST(REI."DEFFECDATE" as DATE) <= cast(cla."DOCCURDAT" as date)
                                                                                                                                and		(REI."DNULLDATE" IS NULL OR CAST(REI."DNULLDATE" as DATE) > cast(cla."DOCCURDAT" as date)))
                                                                                                        then	case	when	"SPOLITYPE" = '3'
                                                                                                                then	2
                                                                                                                else	1 end
                                                                                                        else 	0 end flag_rea
                                                                        from    (	select	ctid pol_id
                                                                                                from	usvtimg01."POLICY"
                                                                                                where	"SCERTYPE" = '2'
                                                                                                and		"NBRANCH" in (58)) po0 --solo se consideran estos ramos por tener un solo ramo contable y la cantidad de registros asociados
                                                                        join	usvtimg01."POLICY" pol on pol.ctid = po0.pol_id
                                                                        join	usvtimg01."CLAIM" cla
                                                                                        on		cla."SCERTYPE" = pol."SCERTYPE"
                                                                                and     cla."NPOLICY" = pol."NPOLICY"
                                                                                and     cla."NBRANCH" = pol."NBRANCH"
                                                                                and     cla."SSTACLAIM" <> '6'
                                                                                        and     exists
                                                                                        (   select  1
                                                                                        from    usvtimg01."CLAIM_HIS" clh
                                                                                                                join	(	select	cast("SVALUE" as INT4) "SVALUE"
                                                                                                                                        from	usvtimg01."CONDITION_SERV" cs 
                                                                                                                                        where	"NCONDITION" in (71,72,73)) csv
                                                                                                                                on	clh."NOPER_TYPE" = csv."SVALUE"
                                                                                        where	coalesce(clh."NCLAIM",0) = cla."NCLAIM"
                                                                                        and		cast(clh."DOPERDATE" as date) >= '12/31/2018')) cla --ejecutar al a o 2021 (2018 es para pruebas)
                                                join	usvtimg01."CLAIM_HIS" clh
                                                                on		clh."NCLAIM" = cla.nclaim
                                                                and 	clh."NOPER_TYPE" = 
                                                                                (	select	cast("SVALUE" as INT4) svalue
                                                                                        from	usvtimg01."CONDITION_SERV"
                                                                                        where	cast("SVALUE" as INT4) = clh."NOPER_TYPE"
                                                                                        and		"NCONDITION" in (73))
                                                                and     cast(clh."DOPERDATE" as date) <= cast('12/31/2023' as date) --en caso de ejecutar a una fecha posterior, alterar esta fecha a la de la ejecuci n
                                                group 	by 1,2,3,4,5) cl0
                        join	usvtimg01."CLAIM" cla on cla.ctid = cl0.cla_id
                        join	usvtimg01."POLICY" pol on pol.ctid = cl0.pol_id
                        join	usvtimg01."CLAIM_HIS" clh on clh.ctid = cl0.clh_id
                        where	coalesce(cl0.namount,0) <> 0
                        ) AS TMP                         
                        '''

  DF_LPG_VTIME_AGRARIO = glue_context.read.format('jdbc').options(**connection).option("fetchsize",10000).option("dbtable",L_RBRECIN_VTIME_LPG_AGRARIO).load()

  L_RBRECIN_VTIME_LPG_OTROS_RAMOS = '''
                        (
                                       select	        'D' INDDETREC,
                                                        'RBRECIN' TABLAIFRS17,
                                                        '' PK,
                                                        '' DTPREG, --excluido
                                                        '' TIOCPROC, --excluido
                                                        cast(clh."DOPERDATE" as date) TIOCFRM,
                                                        '' TIOCTO, --excluido
                                                        '' KRITPREG, --indeterminado (pendiente coordinar con FID/LP)
                                                        '' KRITPRGI, --indeterminado (pendiente coordinar con FID/LP)
                                                        case	when coalesce(clm.namount,0) <= 0
                                                                        then 1
                                                                        else 2 end KRCTPRCI,
                                                        'PVG' KGIORIGM,
                                                        cast(cla."NBRANCH" as varchar) KGCRAMO,
                                                        cast(cla."NPOLICY" as varchar) DNUMAPO,
                                                        cast(cla."NCERTIF" as varchar) DNMCERT,
                                                        cast(cla."NCLAIM" as varchar) DNUMSIN,
                                                        cast(clh."NCASE_NUM" as varchar) DNUMSSIN,
                                                        '' DNUMPENS, --excluido
                                                        clh."NCLAIM" || '-' || clh."NCASE_NUM" || '-' || clh."NTRANSAC" DNUMREC,
                                                        '' DNMAGRE, --no disponible
                                                        '' NSAGREG, --excluido
                                                        '' NSEQSIN, --excluido
                                                        coalesce(cast(extract(year from cla."DOCCURDAT") as varchar),'') DANOSIN,
                                                        coalesce(cast(cast(clh."DOPERDATE" as date) as varchar),'') TEMISSAO,
                                                        '' TINICIO, --no disponible
                                                        '' TTERMO, --no disponible
                                                        '' TESTADO, --no disponible
                                                        /*
                                                        coalesce((	select	cast(cast(max(cl0."DOPERDATE") as date) as varchar)
                                                                                from	usvtimg01."CLAIM_HIS"  cl0
                                                                                where 	cl0."NCLAIM" = clh."NCLAIM"
                                                                                and		cl0."NTRANSAC" = 
                                                                                                (	select 	max(csp."NTRANSAC")
                                                                                                        from	usvtimg01."WBSTBLCLAIM_DOC_SAP"  csp
                                                                                                        where	csp."NCLAIM" = cl0."NCLAIM"
                                                                                                        and		csp."NCASE_NUM" = cl0."NCASE_NUM"
                                                                                                        and 	csp."NTRANSAC" = cl0."NTRANSCLAIMREV")),'')*/ '' TPGCOB, --pendiente evaluar si est  bien aplicado
                                                        '' TANSUSP, --PENDIENTE CONOCER SI ES POSIBLE/VIABLE
                                                        '' KRCESPRI, --no disponible
                                                        '' KRCESTRI, --no disponible
                                                        '' KRCMOSTI, --excluido
                                                        cast(clm.moneda_cod as varchar) KRCMOEDA,
                                                        '' VCAMBIO, --no disponible
                                                        abs(cast(clm.namount as numeric(12,2))) VMTTOTRI,
                                                        '' VMTIVA, --no disponible
                                                        '' VMTIRSRT, --no disponible
                                                        '' VTXIRSRT, --no disponible
                                                        '' VMTLIQRC, --excluido
                                                        abs(cast(coalesce(clm.namount, 0) * (case when pol."SBUSSITYP" = '2' then 100 else clm.nshare_coa end/100) as numeric(12,2))) VMTCOSEG,
                                                        abs(cast(clm.namount *
                                                                        (case when pol."SBUSSITYP" = '2' then 100 else nshare_coa end / 100) *
                                                                        case 	when	not(clm.namount <> 0 and clm.flag_rea <> 0)
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
                                                                                                                                                                                clm.namount ratio_rea
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
                                                                                                                                                                                                                        where   gco."NCOVER" = cm0."NCOVER"
                                                                                                                                                                                                                        and     gco."NPRODUCT" = pol."NPRODUCT"
                                                                                                                                                                                                                        and     gco."NMODULEC" = cm0."NMODULEC"
                                                                                                                                                                                                                        and     gco."NBRANCH" = cla."NBRANCH"
                                                                                                                                                                                                                        and     cast(gco."DEFFECDATE" as date) <= cast(cla."DOCCURDAT" as date)
                                                                                                                                                                                                                        and     (gco."DNULLDATE" is null or cast(gco."DNULLDATE" as date) > cast(cla."DOCCURDAT" as date))
                                                                                                                                                                                                                        and     gco."SSTATREGT" <> '4'
                                                                                                                                                                                                                        and		gco."NBRANCH_LED" = clm.nbranch_led)
                                                                                                                                                                                                                        else	(   select  gco."NBRANCH_REI"
                                                                                                                                                                                                                        from    usvtimg01."GEN_COVER" gco
                                                                                                                                                                                                                        where   gco."NCOVER" = cm0."NCOVER"
                                                                                                                                                                                                                        and     gco."NPRODUCT" = pol."NPRODUCT"
                                                                                                                                                                                                                        and     gco."NMODULEC" = cm0."NMODULEC"
                                                                                                                                                                                                                        and     gco."NBRANCH" = cla."NBRANCH"
                                                                                                                                                                                                                        and     cast(gco."DEFFECDATE" as date) <= cast(cla."DOCCURDAT" as date)
                                                                                                                                                                                                                        and     (gco."DNULLDATE" is null or cast(gco."DNULLDATE" as date) > cast(cla."DOCCURDAT" as date))
                                                                                                                                                                                                                        and     gco."SSTATREGT" <> '4'
                                                                                                                                                                                                                        and		gco."NBRANCH_LED" = clm.nbranch_led) end nbranch_rei,
                                                                                                                                                                                                        sum(case	clm.moneda_cod
                                                                                                                                                                                                                                when	1
                                                                                                                                                                                                                                then	case	when	cm0."NCURRENCY" = 1
                                                                                                                                                                                                                                                                then	cm0."NAMOUNT"
                                                                                                                                                                                                                                                                when	cm0."NCURRENCY" = 2
                                                                                                                                                                                                                                                                then	cm0."NAMOUNT" * cm0."NEXCHANGE"
                                                                                                                                                                                                                                                                else	0 end
                                                                                                                                                                                                                                when	2
                                                                                                                                                                                                                                then	case	when	cm0."NCURRENCY" = 2
                                                                                                                                                                                                                                                                then	cm0."NAMOUNT"
                                                                                                                                                                                                                                                                when	cm0."NCURRENCY" = 1
                                                                                                                                                                                                                                                                then	cm0."NLOC_AMOUNT"
                                                                                                                                                                                                                                                                else	0 end
                                                                                                                                                                                                                                else	0 end) monto_trans
                                                                                                                                                                                        from 	usvtimg01."CLAIM_HIS" ch0,
                                                                                                                                                                                                        usvtimg01."CL_M_COVER" cm0
                                                                                                                                                                                        where	ch0.ctid = clm.clh_id
                                                                                                                                                                                        and		cm0."NCLAIM" = ch0."NCLAIM"
                                                                                                                                                                                        and 	cm0."NCASE_NUM" = ch0."NCASE_NUM"
                                                                                                                                                                                        and 	cm0."NDEMAN_TYPE" = ch0."NDEMAN_TYPE"
                                                                                                                                                                                        and 	cm0."NTRANSAC" = ch0."NTRANSAC"
                                                                                                                                                                                        group	by 1) cl1
                                                                                                                                                                where	nbranch_rei is not null
                                                                                                                                                                and		coalesce(cl1.monto_trans,0) <> 0) cl1) rea) end as numeric(12,2))) VMTRESSG,
                                                        cla."NBRANCH" || '-' || pol."NPRODUCT" KABPRODT,
                                                        '' KRCFMPGI, --excluido
                                                        '' KMEDCB, --excluido
                                                        '' KMEDPG, --excluido
                                                        '' DUSRORI, --excluido
                                                        '' DUSRAPR, --excluido
                                                        case	when pol."SBUSSITYP" = '1' and clm.nshare_coa = 100 then '1' --Sin coaseguro 
                                                                        when pol."SBUSSITYP" = '1' and clm.nshare_coa <> 100 then '2' --Con coaseguro, LP compa  a l der
                                                                        when pol."SBUSSITYP" = '2' then '3' --LP compa  a no l der
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
                                                        cast(cla."NCLAIM" as varchar) KSBSIN,
                                                        cla."NBRANCH" || '-' || pol."NPRODUCT" || '-' || cla."NPOLICY" || '-' || cla."NCERTIF" KABAPOL,
                                                        '' KABAPOL_EFT, --excluido
                                                        coalesce(cast(	case	coalesce(cla."NCERTIF",0)
                                                                                                        when	0
                                                                                                        then	cast(pol."DDATE_ORIGI" as date) 
                                                                                                        else	(	select	cast(cer."DDATE_ORIGI" as date)
                                                                                                                                from 	usvtimg01."CERTIFICAT" cer
                                                                                                                                where	cer."SCERTYPE" = pol."SCERTYPE"
                                                                                                                                and 	cer."NBRANCH" = cla."NBRANCH"
                                                                                                                                and 	cer."NPOLICY" = cla."NPOLICY"
                                                                                                                                and		cer."NCERTIF" = cla."NCERTIF"
                                                                                                                                and		cer."NDIGIT" = 0) 
                                                                                                        end as varchar),'') TINICIOA,
                                                        coalesce(cast(clm.nbranch_led as varchar),'0') KGCRAMO_SAP,
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
                                                        coalesce(cla."SCLIENT",'') KEBENTID_TO,
                                                        '' TCONTAB, --excluido
                                                        coalesce(cast(extract(year from cla."DOCCURDAT") as varchar),'') DANOABER,
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
                                        from 	(	select	cla.cla_id,
                                                                (   select  clh.ctid
                                                                from    usvtimg01."CLAIM_HIS" clh
                                                                where	clh."NCLAIM" = clm."NCLAIM"
                                                                                        and 	clh."NCASE_NUM" = clm."NCASE_NUM"
                                                                                        and 	clh."NDEMAN_TYPE" = clm."NDEMAN_TYPE"
                                                                                        and 	clh."NTRANSAC" = clm."NTRANSAC") clh_id,
                                                                                cla.pol_id,
                                                                                cla.nshare_coa,
                                                                                cla.flag_rea,
                                                                                cla.moneda_cod,
                                                                                case	when	cla.nbranch = 21
                                                                                        then	(   select  gco."NBRANCH_LED"
                                                                                                from    usvtimg01."LIFE_COVER" gco
                                                                                                where   gco."NCOVER" = clm."NCOVER"
                                                                                                and     gco."NPRODUCT" = cla.nproduct
                                                                                                and     gco."NMODULEC" = clm."NMODULEC"
                                                                                                and     gco."NBRANCH" = cla.nbranch
                                                                                                and     cast(gco."DEFFECDATE" as date) <= cla.doccurdat
                                                                                                and     (gco."DNULLDATE" is null or cast(gco."DNULLDATE" as date) > cla.doccurdat)
                                                                                                and     gco."SSTATREGT" <> '4')
                                                                                                else	(   select  gco."NBRANCH_LED"
                                                                                                from    usvtimg01."GEN_COVER" gco
                                                                                                where   gco."NCOVER" = clm."NCOVER"
                                                                                                and     gco."NPRODUCT" = cla.nproduct
                                                                                                and     gco."NMODULEC" = clm."NMODULEC"
                                                                                                and     gco."NBRANCH" = cla.nbranch
                                                                                                and     cast(gco."DEFFECDATE" as date) <= cla.doccurdat
                                                                                                and     (gco."DNULLDATE" is null or cast(gco."DNULLDATE" as date) > cla.doccurdat)
                                                                                                and     gco."SSTATREGT" <> '4') end nbranch_led,
                                                                                sum(coalesce(	case	when	cla.moneda_cod = 1
                                                                                                                                then	case	when	clm."NCURRENCY" = 1
                                                                                                                                                                then	clm."NAMOUNT"
                                                                                                                                                                when	clm."NCURRENCY" = 2
                                                                                                                                                                then	clm."NAMOUNT" * clm."NEXCHANGE"
                                                                                                                                                                else	0 end
                                                                                                                                when	cla.moneda_cod = 2
                                                                                                                                then	case	when	clm."NCURRENCY" = 2
                                                                                                                                                                then	clm."NAMOUNT"
                                                                                                                                                                when	clm."NCURRENCY" = 1
                                                                                                                                                                then	clm."NLOC_AMOUNT"
                                                                                                                                                                else	0 end
                                                                                                                                else	0 end, 0)) namount
                                                                from	(	select  cla."NCLAIM" nclaim,
                                                                                                        cla.ctid cla_id,
                                                                                                        pol.ctid pol_id,
                                                                                cla."NBRANCH" nbranch,
                                                                                cla."NPOLICY" npolicy,
                                                                                cla."NCERTIF" ncertif,
                                                                                pol."NPRODUCT" nproduct,
                                                                                pol."SBUSSITYP" sbussityp,
                                                                                cast(cla."DOCCURDAT" as date) doccurdat,
                                                                                coalesce(
                                                                                        coalesce((  select  max(cpl."NCURRENCY")
                                                                                        from    usvtimg01."CURREN_POL" cpl
                                                                                        where 	cpl."SCERTYPE" = cla."SCERTYPE" and	cpl."NBRANCH" = cla."NBRANCH"
                                                                                        and		cpl."NPRODUCT" = pol."NPRODUCT" and cpl."NPOLICY" = cla."NPOLICY"
                                                                                        and     cpl."NCERTIF" = cla."NCERTIF"),
                                                                                (   select  max(cpl."NCURRENCY")
                                                                                        from    usvtimg01."CURREN_POL" cpl
                                                                                        where 	cpl."SCERTYPE" = cla."SCERTYPE" and	cpl."NBRANCH" = cla."NBRANCH"
                                                                                        and		cpl."NPRODUCT" = pol."NPRODUCT" and cpl."NPOLICY" = cla."NPOLICY")),0) moneda_cod,
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
                                                                                                                        ELSE    0 END flag_rea
                                                                                        from    (	select	ctid pol_id
                                                                                                                from	usvtimg01."POLICY"
                                                                                                                where	"SCERTYPE" = '2'
                                                                                                                and		"NBRANCH" <> 58) po0 --ramo excluido por el volumen de datos asociados
                                                                                        join	usvtimg01."POLICY" pol on pol.ctid = po0.pol_id
                                                                                        join	usvtimg01."CLAIM" cla
                                                                                                        on		cla."SCERTYPE" = pol."SCERTYPE"
                                                                                                and     cla."NPOLICY" = pol."NPOLICY"
                                                                                                and     cla."NBRANCH" = pol."NBRANCH"
                                                                                                and     cla."SSTACLAIM" <> '6'
                                                                                                        and     exists
                                                                                                        (   select  1
                                                                                                        from    usvtimg01."CLAIM_HIS" clh
                                                                                                                                join	(	select	cast("SVALUE" as INT4) "SVALUE"
                                                                                                                                                        from	usvtimg01."CONDITION_SERV" cs 
                                                                                                                                                        where	"NCONDITION" in (71,72,73)) csv --reservas, ajustes o pagos
                                                                                                                                                on	clh."NOPER_TYPE" = csv."SVALUE"
                                                                                                        where	coalesce(clh."NCLAIM",0) = cla."NCLAIM"
                                                                                                        and		cast(clh."DOPERDATE" as date) >= '12/31/2018')) cla --ejecutar al a o 2021 (2018 es para pruebas)
                                                                join	usvtimg01."CL_M_COVER" clm on clm."NCLAIM" = cla.nclaim
                                                                and		exists
                                                                (   select  1
                                                                from    usvtimg01."CLAIM_HIS" clh
                                                                                        join	(	select	cast("SVALUE" as INT4) "SVALUE"
                                                                                                                from	usvtimg01."CONDITION_SERV" cs 
                                                                                                                where	"NCONDITION" in (73)) csv --pagos
                                                                                                        on	clh."NOPER_TYPE" = csv."SVALUE"
                                                                where	clh."NCLAIM" = clm."NCLAIM"
                                                                                        and 	clh."NCASE_NUM" = clm."NCASE_NUM"
                                                                                        and 	clh."NDEMAN_TYPE" = clm."NDEMAN_TYPE"
                                                                                        and 	clh."NTRANSAC" = clm."NTRANSAC"
                                                                and		cast(clh."DOPERDATE" as date) <= cast('12/31/2023' as date)) --en caso de ejecutar a una fecha posterior, alterar esta fecha a la de la ejecuci n
                                                                group 	by 1,2,3,4,5,6,7) clm
                                        join	usvtimg01."POLICY" pol on pol.ctid = clm.pol_id
                                        join	usvtimg01."CLAIM" cla on cla.ctid = clm.cla_id
                                        join	usvtimg01."CLAIM_HIS" clh on clh.ctid = clm.clh_id
                                        where	coalesce(clm.namount,0) <> 0
                        ) AS TMP                         
                        '''

  DF_LPG_VTIME_OTROS_RAMOS = glue_context.read.format('jdbc').options(**connection).option("fetchsize",10000).option("dbtable",L_RBRECIN_VTIME_LPG_OTROS_RAMOS).load()

  L_RBRECIN_VTIME_LPV = '''
                        (
                           select	'D' INDDETREC,
                                        'RBRECIN' TABLAIFRS17,
                                        '' PK,
                                        '' DTPREG, --excluido
                                        '' TIOCPROC, --excluido
                                        cast(clh."DOPERDATE" as date) TIOCFRM,
                                        '' TIOCTO, --excluido
                                        '' KRITPREG, --indeterminado (pendiente coordinar con FID/LP)
                                        '' KRITPRGI, --indeterminado (pendiente coordinar con FID/LP)
                                        case	when coalesce(clm.namount,0) <= 0
                                                        then 1
                                                        else 2 end KRCTPRCI,
                                        'PVV' KGIORIGM,
                                        cast(cla."NBRANCH" as varchar) KGCRAMO,
                                        cast(cla."NPOLICY" as varchar) DNUMAPO,
                                        cast(cla."NCERTIF" as varchar) DNMCERT,
                                        cast(cla."NCLAIM" as varchar) DNUMSIN,
                                        cast(clh."NCASE_NUM" as varchar) DNUMSSIN,
                                        '' DNUMPENS, --excluido
                                        clh."NCLAIM" || '-' || clh."NCASE_NUM" || '-' || clh."NTRANSAC" DNUMREC,
                                        '' DNMAGRE, --no disponible
                                        '' NSAGREG, --excluido
                                        '' NSEQSIN, --excluido
                                        coalesce(cast(extract(year from cla."DOCCURDAT") as varchar),'') DANOSIN,
                                        coalesce(cast(cast(clh."DOPERDATE" as date) as varchar),'') TEMISSAO,
                                        '' TINICIO, --no disponible
                                        '' TTERMO, --no disponible
                                        '' TESTADO, --no disponible
                                        /*
                                        coalesce((	select	cast(cast(max(cl0."DOPERDATE") as date) as varchar)
                                                                from	usvtimv01."CLAIM_HIS"  cl0
                                                                where 	cl0."NCLAIM" = clh."NCLAIM"
                                                                and		cl0."NTRANSAC" = 
                                                                                (	select 	max(csp."NTRANSAC")
                                                                                        from	usvtimv01."WBSTBLCLAIM_DOC_SAP"  csp
                                                                                        where	csp."NCLAIM" = cl0."NCLAIM"
                                                                                        and		csp."NCASE_NUM" = cl0."NCASE_NUM"
                                                                                        and 	csp."NTRANSAC" = cl0."NTRANSCLAIMREV")),'')*/'' TPGCOB, --pendiente evaluar si est  bien aplicado
                                        '' TANSUSP, --PENDIENTE CONOCER SI ES POSIBLE/VIABLE
                                        '' KRCESPRI, --no disponible
                                        '' KRCESTRI, --no disponible
                                        '' KRCMOSTI, --excluido
                                        cast(	coalesce(
                                                                coalesce((	select	max(cpl."NCURRENCY")
                                                        from	usvtimv01."CURREN_POL" cpl
                                                        where 	cpl."SCERTYPE" = cla."SCERTYPE" and cpl."NBRANCH" = cla."NBRANCH"
                                                                                        and		cpl."NPRODUCT" = pol."NPRODUCT" and	cpl."NPOLICY" = cla."NPOLICY"
                                                                                        and		cpl."NCERTIF" = cla."NCERTIF"),
                                                (   select  max(cpl."NCURRENCY")
                                                        from    usvtimv01."CURREN_POL" cpl
                                                        where 	cpl."SCERTYPE" = cla."SCERTYPE" and cpl."NBRANCH" = cla."NBRANCH"
                                                                                        and		cpl."NPRODUCT" = pol."NPRODUCT" and	cpl."NPOLICY" = cla."NPOLICY")),0) as varchar) KRCMOEDA,
                                        '' VCAMBIO, --no disponible
                                        abs(cast(clm.namount as numeric(12,2))) VMTTOTRI,
                                        '' VMTIVA, --no disponible
                                        '' VMTIRSRT, --no disponible
                                        '' VTXIRSRT, --no disponible
                                        '' VMTLIQRC, --excluido
                                        abs(cast(coalesce(clm.namount, 0) * (case when pol."SBUSSITYP" = '2' then 100 else clm.nshare_coa end/100) as numeric(12,2))) VMTCOSEG,
                                        abs(cast(coalesce(clm.namount, 0) * (case when pol."SBUSSITYP" = '2' then 100 else clm.nshare_coa end/100) as numeric(12,2))) VMTRESSG, --no hay reaseguro en VT-LPV
                                        cla."NBRANCH" || '-' || pol."NPRODUCT" KABPRODT,
                                        '' KRCFMPGI, --excluido
                                        '' KMEDCB, --excluido
                                        '' KMEDPG, --excluido
                                        '' DUSRORI, --excluido
                                        '' DUSRAPR, --excluido
                                        case	when pol."SBUSSITYP" = '1' and clm.nshare_coa = 100 then '1' --Sin coaseguro 
                                                        when pol."SBUSSITYP" = '1' and clm.nshare_coa <> 100 then '2' --Con coaseguro, LP compa  a l der
                                                        when pol."SBUSSITYP" = '2' then '3' --LP compa  a no l der
                                                        else '0' end KRCTPCSG,
                                        '' DCODENPG, --excluido
                                        '' DCODENRC, --excluido
                                        '' VMFAT, --no disponible
                                        'LPV' DCOMPA,
                                        '' DMARCA, --excluido
                                        '' TDAPROV, --excluido
                                        '' DNMACJUD, --excluido
                                        '' KRCTPERP, --excluido
                                        '' KRBRECIN_MP, --excluido
                                        '' TMIGPARA, --excluido
                                        '' KRBRECIN_MD, --excluido
                                        '' TMIGDE, --excluido
                                        cast(cla."NCLAIM" as varchar) KSBSIN,
                                        cla."NBRANCH" || '-' || pol."NPRODUCT" || '-' || cla."NPOLICY" || '-' || cla."NCERTIF" KABAPOL,
                                        '' KABAPOL_EFT, --excluido
                                        coalesce(cast(	case	coalesce(cla."NCERTIF",0)
                                                                                        when	0
                                                                                        then	cast(pol."DDATE_ORIGI" as date) 
                                                                                        else	(	select	cast(cer."DDATE_ORIGI" as date)
                                                                                                                from 	usvtimv01."CERTIFICAT" cer
                                                                                                                where	cer."SCERTYPE" = pol."SCERTYPE"
                                                                                                                and 	cer."NBRANCH" = cla."NBRANCH"
                                                                                                                and 	cer."NPOLICY" = cla."NPOLICY"
                                                                                                                and		cer."NCERTIF" = cla."NCERTIF"
                                                                                                                and		cer."NDIGIT" = 0) 
                                                                                        end as varchar),'') TINICIOA,
                                        coalesce(cast(clm.nbranch_led as varchar),'0') KGCRAMO_SAP,
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
                                        coalesce(cla."SCLIENT",'') KEBENTID_TO,
                                        '' TCONTAB, --excluido
                                        coalesce(cast(extract(year from cla."DOCCURDAT") as varchar),'') DANOABER,
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
                        from 	(	select	clh.cla_id,
                                                                clh.clh_id,
                                                                clh.pol_id,
                                                                clh.nshare_coa,
                                                                (   select  gco."NBRANCH_LED"
                                                from    usvtimv01."LIFE_COVER" gco
                                                where   gco."NCOVER" = clm."NCOVER"
                                                and     gco."NPRODUCT" = clh.nproduct
                                                and     gco."NMODULEC" = clm."NMODULEC"
                                                and     gco."NBRANCH" = clh.nbranch
                                                and     cast(gco."DEFFECDATE" as date) <= doccurdat
                                                and     (gco."DNULLDATE" is null or cast(gco."DNULLDATE" as date) > doccurdat)
                                                and     gco."SSTATREGT" <> '4') nbranch_led,
                                                                sum(coalesce(	case	when	clh.moneda_cod = 1
                                                                                                                then	case	when	clm."NCURRENCY" = 1
                                                                                                                                                then	coalesce(clm."NAMOUNT",0)
                                                                                                                                                when	clm."NCURRENCY" = 2
                                                                                                                                                then	coalesce(clm."NAMOUNT",0) * clm."NEXCHANGE"
                                                                                                                                                else	0 end
                                                                                                                when	clh.moneda_cod = 2
                                                                                                                then	case	when	clm."NCURRENCY" = 2
                                                                                                                                                then	coalesce(clm."NAMOUNT",0)
                                                                                                                                                when	clm."NCURRENCY" = 1
                                                                                                                                                then	coalesce(clm."NLOC_AMOUNT",0)
                                                                                                                                                else	0 end
                                                                                                                else	0 end, 0)) namount
                                                from    (   select  cla.nclaim,
                                                                                        cla.cla_id,
                                                                                        cla.pol_id,
                                                                                        cla.nbranch,
                                                                                        cla.npolicy,
                                                                                        cla.ncertif,
                                                                                        cla.nproduct,
                                                                                        cla.doccurdat,
                                                                                        cla.moneda_cod,
                                                                                        cla.nshare_coa,
                                                                                        clh."NCASE_NUM" ncase_num,
                                                                                        clh."NDEMAN_TYPE" ndeman_type,
                                                                clh."NTRANSAC" ntransac,
                                                                                        clh.ctid clh_id
                                                        from    (   select  case    when    cla."SSTACLAIM" <> '6' and pol."SCERTYPE" = '2'
                                                                                        then    cla."NCLAIM"
                                                                                        else    null end nclaim,
                                                                                                                case    when    cla."SSTACLAIM" <> '6' and pol."SCERTYPE" = '2'
                                                                                        then    cla.ctid
                                                                                        else    null end cla_id,
                                                                                                                case    when    cla."SSTACLAIM" <> '6' and pol."SCERTYPE" = '2'
                                                                                        then    pol.ctid
                                                                                        else    null end pol_id,
                                                                                cla."NBRANCH" nbranch,
                                                                                cla."NPOLICY" npolicy,
                                                                                cla."NCERTIF" ncertif,
                                                                                pol."NPRODUCT" nproduct,
                                                                                cast(cla."DOCCURDAT" as date) doccurdat,
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
                                                                                        else    0 end moneda_cod,
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
                                                                                                                                                                                                        and     coi."DEFFECDATE" <= cla."DOCCURDAT"
                                                                                                                                                                                                        and     (coi."DNULLDATE" is null or coi."DNULLDATE" > cla."DOCCURDAT")
                                                                                                                                                                                                        and		coi."NCOMPANY" in (1)),100) end 
                                                                                                                                else	0 end nshare_coa
                                                                                        from    (	select	ctid pol_id
                                                                                                                from	usvtimv01."POLICY"
                                                                                                                where	"SCERTYPE" = '2') po0 --ramo excluido por el volumen de datos asociados
                                                                                        join	usvtimv01."POLICY" pol on pol.ctid = po0.pol_id
                                                                                        join	usvtimv01."CLAIM" cla
                                                                                                        on		cla."SCERTYPE" = pol."SCERTYPE"
                                                                                                and     cla."NPOLICY" = pol."NPOLICY"
                                                                                                and     cla."NBRANCH" = pol."NBRANCH"
                                                                                                and     cla."SSTACLAIM" <> '6'
                                                                                                        and     exists
                                                                                                        (   select  1
                                                                                                        from    usvtimv01."CLAIM_HIS" clh
                                                                                                                                join	(	select	cast("SVALUE" as INT4) "SVALUE"
                                                                                                                                                        from	usvtimv01."CONDITION_SERV" cs 
                                                                                                                                                        where	"NCONDITION" in (71,72,73)) csv --reservas, ajustes o pagos
                                                                                                                                                on	clh."NOPER_TYPE" = csv."SVALUE"
                                                                                                        where	coalesce(clh."NCLAIM",0) = cla."NCLAIM"
                                                                                                        and		cast(clh."DOPERDATE" as date) >= '12/31/2015')) cla --ejecutar al a o 2021 (2015 es para pruebas)
                                                                        join	usvtimv01."CLAIM_HIS" clh
                                                                                        on		clh."NCLAIM" = cla.nclaim
                                                                                        and     cast(clh."DOPERDATE" as date) <= cast('12/31/2023' as date) --en caso de ejecutar a una fecha posterior, alterar esta fecha a la de la ejecuci n
                                                                        join	(	select	cast("SVALUE" as INT4) svalue
                                                                                                from	usvtimv01."CONDITION_SERV"
                                                                                                where	"NCONDITION" in (73)) csv --solo reservas, ajustes y pagos
                                                                                        on	clh."NOPER_TYPE" = csv.svalue) clh
                                                join	usvtimv01."CL_M_COVER" clm
                                                                on		clm."NCLAIM" = clh.nclaim
                                                                and 	clm."NCASE_NUM" = clh.ncase_num
                                                                and 	clm."NDEMAN_TYPE" = clh.ndeman_type
                                                                and 	clm."NTRANSAC" = clh.ntransac
                                                group 	by 1,2,3,4,5) clm
                        join	usvtimv01."POLICY" pol on pol.ctid = clm.pol_id
                        join	usvtimv01."CLAIM" cla on cla.ctid = clm.cla_id
                        join	usvtimv01."CLAIM_HIS" clh on clh.ctid = clm.clh_id
                        where	coalesce(clm.namount,0) <> 0
                        ) AS TMP             
                        '''        

  DF_LPV_VTIME = glue_context.read.format('jdbc').options(**connection).option("fetchsize",10000).option("dbtable",L_RBRECIN_VTIME_LPV).load()

  L_DF_RBRECIN_VTIME = DF_LPG_VTIME_AGRARIO.union(DF_LPG_VTIME_OTROS_RAMOS).union(DF_LPV_VTIME)

  L_RBRECIN_INSIS = '''
                        (
                           select	'D' INDDETREC,
                                        'RBRECIN' TABLAIFRS17,
                                        '' PK,
                                        '' DTPREG, --excluido
                                        '' TIOCPROC, --excluido
                                        --cast(round(clh."REGISTRATION_DATE",0) as date) TIOCFRM,
                                        cast(clh."REGISTRATION_DATE",0) as date) TIOCFRM,
                                        '' TIOCTO, --excluido
                                        '' KRITPREG, --indeterminado (pendiente coordinar con FID/LP)
                                        '' KRITPRGI, --indeterminado (pendiente coordinar con FID/LP)
                                        case	when coalesce(clh."RESERV_CHANGE",0) <= 0
                                                        then 1
                                                        else 2 end KRCTPRCI,
                                        'PNV' KGIORIGM, --excluido
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
                                        substring(cast(coalesce(cl0.pep_id,pol."POLICY_ID") as varchar),6) DNUMAPO,
                                        coalesce(substring(cast(case when cl0.pep_id is not null then pol."POLICY_ID" else null end as varchar),6),'0') DNMCERT,
                                        coalesce(cast(cl0.claim_id as varchar),'') DNUMSIN,
                                        '' DNUMSSIN, --no disponible
                                        '' DNUMPENS, --excluido
                                        cast(round(clh."RESERV_SEQ",0) as varchar) DNUMREC,
                                        '' DNMAGRE, --no disponible
                                        '' NSAGREG, --excluido
                                        '' NSEQSIN, --excluido
                                        coalesce(cast(extract(year from cla."EVENT_DATE") as varchar),'') DANOSIN,
                                        coalesce(cast(cast(clh."REGISTRATION_DATE" as date) as varchar),'') TEMISSAO,
                                        '' TINICIO, --no disponible
                                        '' TTERMO, --no disponible
                                        '' TESTADO, --no disponible
                                        '' TPGCOB, --no se dispone de accesos a la tabla INTERFAZ_INSUDB.WBSTBLCLAIM_DOC_SAP
                                        '' TANSUSP, --PENDIENTE CONOCER SI ES POSIBLE/VIABLE
                                        '' KRCESPRI, --no disponible
                                        '' KRCESTRI, --no disponible
                                        '' KRCMOSTI, --excluido
                                        cast(coalesce(cl0.moneda_cod,0) as varchar) KRCMOEDA,
                                        '' VCAMBIO, --no disponible
                                        abs(cast(coalesce(clh."RESERV_AMNT",0) as numeric(12,2))) VMTTOTRI,
                                        '' VMTIVA, --no disponible
                                        '' VMTIRSRT, --no disponible
                                        '' VTXIRSRT, --no disponible
                                        '' VMTLIQRC, --excluido
                                        abs(cast(coalesce(clh."RESERV_AMNT",0) as numeric(12,2))) VMTCOSEG,
                                        abs(cast(coalesce(clh."RESERV_AMNT",0) as numeric(12,2))) VMTRESSG, --pendiente determina reglas en RI_CEDED_CLAIMS
                                        pol."ATTR1"  || '-' || pol."ATTR4" KABPRODT,
                                        '' KRCFMPGI, --excluido
                                        '' KMEDCB, --excluido
                                        '' KMEDPG, --excluido
                                        '' DUSRORI, --excluido
                                        '' DUSRAPR, --excluido
                                        '1' KRCTPCSG, --no hay coaseguros en INSIS (100% retenci n = sin coaseguro)
                                        '' DCODENPG, --excluido
                                        '' DCODENRC, --excluido
                                        '' VMFAT, --no disponible
                                        'LPV' DCOMPA,
                                        '' DMARCA, --excluido
                                        '' TDAPROV, --excluido
                                        '' DNMACJUD, --excluido
                                        '' KRCTPERP, --excluido
                                        '' KRBRECIN_MP, --excluido
                                        '' TMIGPARA, --excluido
                                        '' KRBRECIN_MD, --excluido
                                        '' TMIGDE, --excluido
                                        cast(clh."CLAIM_ID" as varchar) KSBSIN,
                                        substring(cast(pol."POLICY_ID" as varchar),6) KABAPOL,
                                        '' KABAPOL_EFT, --excluido
                                        coalesce(cast(cast(cla."EVENT_DATE" as date) as varchar),'') TINICIOA,
                                pol."ATTR1" KGCRAMO_SAP,
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
                                        coalesce(
                                                coalesce((	select	lpi."LEGACY_ID"
                                                                        from	usinsiv01."INTRF_LPV_PEOPLE_IDS" lpi
                                                        where	lpi."MAN_ID" = clo."MAN_ID"),
                                                                cast(clo."MAN_ID" as varchar)),'') KEBENTID_TO,
                                        '' TCONTAB, --excluido
                                        coalesce(cast(extract(year from cla."EVENT_DATE") as varchar),'') DANOABER,
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
                        from	(	select	cla."CLAIM_ID" claim_id,
                                                                (select "MASTER_POLICY_ID" from usinsiv01."POLICY_ENG_POLICIES" where "POLICY_ID" = CLA."POLICY_ID") pep_id,
                                                                pol.ctid pol_id,
                                                                case	coalesce((	select	distinct "AV_CURRENCY"
                                                                                                        from	usinsiv01."INSURED_OBJECT"
                                                                                                        where	"POLICY_ID" = pol."POLICY_ID" limit 1),'')
                                                                                when 'USD' then 2
                                                                                when 'PEN' then 1
                                                                                else 0 end MONEDA_COD
                                                from	usinsiv01."CLAIM" cla
                                                join	usinsiv01."POLICY" pol
                                                                on 		pol."POLICY_ID" = cla."POLICY_ID"
                                                                and		pol."POLICY_STATE" >= 0
                                                where	exists 
                                                                (	select	1 
                                                                        from	usinsiv01."CLAIM_RESERVE_HISTORY" crh
                                                                        join	usinsiv01."CLAIM_OBJECTS" clo
                                                                                        on		clo."CLAIM_ID" = crh."CLAIM_ID"
                                                                                        and		clo."CLAIM_OBJ_SEQ" = crh."CLAIM_OBJECT_SEQ"
                                                                                        and		clo."REQUEST_ID" = crh."REQUEST_ID"
                                                                                        and		clo."CLAIM_STATE" <> -1
                                                                        where	crh."CLAIM_ID" = cla."CLAIM_ID"
                                                                        and		crh."OP_TYPE" IN ('PAYMCONF','PAYMINV')
                                                                        and		cast(crh."REGISTRATION_DATE" as date) >= '12/31/2018')) cl0 --ejecutar al a o 2021 (2018 es para pruebas)
                        join	usinsiv01."CLAIM_OBJECTS"  clo
                                        on		clo."CLAIM_ID" = cl0.claim_id
                                        and		clo."CLAIM_STATE" <> -1
                        join	usinsiv01."CLAIM_RESERVE_HISTORY" clh
                                        on		clh."CLAIM_ID" = clo."CLAIM_ID"
                                        and		clh."CLAIM_OBJECT_SEQ" = clo."CLAIM_OBJ_SEQ"
                                        and		clh."REQUEST_ID" = clo."REQUEST_ID"
                                        and		clh."OP_TYPE" IN ('PAYMCONF','PAYMINV')
                                        and		cast(clh."REGISTRATION_DATE" as date) <= '12/31/2023' --en caso de ejecutar a una fecha posterior, alterar esta fecha a la de la ejecuci n
                                        and		coalesce(clh."RESERV_CHANGE",0) <> 0
                        join	usinsiv01."CLAIM" cla on cla."CLAIM_ID" = cl0.claim_id
                        join	usinsiv01."POLICY" pol on pol.ctid = cl0.pol_id
                        ) AS TMP
                        '''
   
  L_DF_RBRECIN_INSIS= glue_context.read.format('jdbc').options(**connection).option("dbtable",L_RBRECIN_INSIS).load()
 
  L_DF_RBRECIN = L_DF_RBRECIN_INSUNIX.union(L_DF_RBRECIN_VTIME).union(L_DF_RBRECIN_INSIS)

  return L_DF_RBRECIN
