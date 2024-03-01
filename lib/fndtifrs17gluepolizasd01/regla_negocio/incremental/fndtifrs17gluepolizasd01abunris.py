from pyspark.sql.types import DecimalType
from pyspark.sql.functions import col

def get_data(glue_context, connection, p_fecha_inicio, p_fecha_fin):
      L_ABUNRIS_INSUNIX_G_PES = f'''
                             (
                               select 
                                'D' AS INDDETREC,
                                'ABUNRIS' AS TABLAIFRS17,
                                '' PK,                                                          -- Clave compuesta
                                '' as DTPREG,
                                '' as TIOCPROC,
                                coalesce(cast(rol.effecdate as varchar),'') TIOCFRM,            -- Fecha de inicio de validez del registro
                                '' as TIOCTO,
                                'PIG' KGIORIGM,                                                  -- Indicador
                                coalesce(cast(rol.branch as varchar),'') || '-' || coalesce(cast(P.PRODUCT as varchar),'')|| '-' || coalesce(cast(psp.SUB_PRODUCT as varchar),'') || '-' || coalesce(cast(rol.policy as varchar),'') ||  '-' || coalesce(cast(rol.certif as varchar),'') KABAPOL,  -- Numero de Poliza
                                'PES' KACTPRIS,           -- Codigo del Tipo de riesgo
                                (select evi.scod_vt  FROM usinsug01.equi_vt_inx evi  WHERE evi.scod_inx  = rol.client)  DUNIRIS,                                                          -- Codigo de unidad de riesgo 
                                coalesce(cast(rol.EFFECDATE as varchar),'')TINCRIS,             -- Fecha de inicio de riesgo
                                coalesce(cast(rol.NULLDATE  as varchar),'') TVENCRI,            -- Fecha de fin de riesgo
                                '' TSITRIS,                                                     -- Fecha de estado de la unidad del riesgo
                                '' KACSITUR,                                                    -- Codigo de estad de la unidad del riesgo
                                '' as KACESQM,
                                'LPG' as DCOMPA,                                                    -- Empresa a la que pertenece la informacion
                                '' as DMARCA,
                                '' as DCREHIP,
                                '' as DCDLCRIS,
                                '' as DSUBPOST,
                                '' as KACTPOPS,                                                     -- Codigo del tipo de persona asegurada
                                '' as KACTPAGR,
                                '' as KACTPBON,
                                '' as KACTPDES,
                                '' as DDEUNRIS,
                                '' as TDACTRIS,
                                '' as TDCANRIS,
                                '' as TDGRARIS,
                                '' as TDRENOVA,
                                '' as TDVENTRA,
                                '' as DHORAINI,
                                1  as DQOBJSEG,                                                     -- Numero de objetos asegurados 
                                0  as VCAPITAL,                                                     -- Importe de capital
                                '' as VMTPRABP,
                                0  as VMTPRMBR,                                                     -- Importe de Prima Bruta
                                0  as VMTCOMR,                                                      -- Importe de Prima Comercial
                                '' as VMTPRLIQ,
                                '' as VMTPREMC,                                                     -- Comision de la prima
                                '' as VMTPRMTR,
                                '' as VMTBOMAT,                                                     -- Monto de bonificacion
                                '' as VTXBOMAT,
                                '' as VMTBOCOM,                                                     -- Monto de bonificacion comercial
                                '' as VTXBOCOM,
                                '' as VMTDECOM,
                                '' as VTXDECOM,
                                '' as VMTDETEC,
                                '' as VTXDETEC,
                                '' as VMTAGRAV,
                                '' as VTXAGRAV,
                                '' as VMIBOMAT,
                                '' as VMIBOCOM,
                                '' as VMIDECOM,
                                '' as VMIDETEC,
                                '' as VMIPRMBR,
                                '' as VMICOMR,
                                '' as VMIPRLIQ,
                                '' as VMIPRMTR,
                                '' as VMIAGRAV,
                                '' as VMIRPMSP,
                                '' as VMICMNQP,
                                '' as DNUMOBPR,
                                '' as DNUMOBSE,
                                '' as KACINDRE,                                                     -- Indicador de Reaseguro
                                '' as DINOBJCB,
                                '' as DINVADIV,
                                '' as DINSINAN,
                                '' as DINSNANC,
                                '' as DINREGFL
                                from usinsug01.policy p
                                join usinsug01.certificat cert
                                on p.usercomp = cert.usercomp
                                and p.company = cert.company
                                and p.certype = cert.certype
                                and p.branch = cert.branch
                                and p.policy = cert.policy
                                join usinsug01.pol_subproduct psp
                                ON  PSP.USERCOMP = P.USERCOMP
                                AND PSP.COMPANY  = P.COMPANY
                                AND PSP.CERTYPE  = P.CERTYPE
                                AND PSP.BRANCH   = P.BRANCH		   
                                AND PSP.PRODUCT  = P.PRODUCT
                                AND PSP.POLICY   = P.policy
                                JOIN USBI01."IFRS170_T_RAMOS_POR_TIPO_RIESGO" RTR
                                ON RTR."BRANCHCOM" = P.BRANCH 
                                AND  RTR."RISKTYPEN" = 1 
                                AND RTR."SOURCESCHEMA" = 'usinsug01'
                                join usinsug01.roles rol
                                on ROL.USERCOMP = P.USERCOMP
                                      AND ROL.COMPANY  = P.COMPANY 
                                      AND ROL.CERTYPE  = P.CERTYPE
                                      AND ROL.BRANCH   = P.BRANCH 
                                      AND ROL.POLICY   = P.POLICY 
                                      AND ROL.CERTIF   = cert.CERTIF  
                                      AND ROL.EFFECDATE <= cert.EFFECDATE 
                                      AND (ROL.NULLDATE IS NULL OR ROL.NULLDATE > cert.EFFECDATE)
                                      AND ROL.ROLE IN (2,8) -- Asegurado , Asegurado adicional
                                where P.CERTYPE = '2' 
                                AND P.STATUS_POL NOT IN ('2','3')
                                and CAST(rol.compdate AS DATE) between '{p_fecha_inicio}' and '{p_fecha_fin}'
                             ) AS TMP
                             '''

      L_DF_ABUNRIS_INSUNIX_G_PES = glue_context.read.format('jdbc').options(**connection).option("dbtable",L_ABUNRIS_INSUNIX_G_PES).load()
      print("ABUNRIS INSUNIX PES")

      L_ABUNRIS_INSUNIX_G_PAT = f'''
                             (
                               select 
                                    'D' AS INDDETREC,
                                    'ABUNRIS' AS TABLAIFRS17,
                                    '' PK,                                                          -- Clave compuesta
                                    '' as DTPREG,
                                    '' as TIOCPROC,
                                    coalesce(cast(PC.EFFECDATE as varchar),'') TIOCFRM,            -- Fecha de inicio de validez del registro
                                    '' as TIOCTO,
                                    'PIG' KGIORIGM,                                                  -- Indicador
                                    coalesce(cast(ad.branch as varchar),'') || '-' || coalesce(cast(PC.PRODUCT as varchar),'')|| '-' || coalesce(cast(PC.SUB_PRODUCT as varchar),'') || '-' || coalesce(cast(ad.policy as varchar),'') ||  '-' || coalesce(cast(ad.certif as varchar),'') KABAPOL,  -- Numero de Poliza
                                    /*(select "RISKTYPEL"  from USBI01."IFRS170_T_RAMOS_POR_TIPO_RIESGO" where "BRANCHCOM" = ad.branch  and "SOURCESCHEMA" = 'usinsug01')*/'PAT' KACTPRIS ,           -- Codigo del Tipo de riesgo
                                    coalesce(cast(ad.branch as varchar),'') || '-' || coalesce(cast(ad.policy as varchar),'') ||  '-' || coalesce(cast(ad.certif as varchar),'')  DUNIRIS,                                                          -- Codigo de unidad de riesgo 
                                    coalesce(cast(PC.EFFECDATE as varchar),'')TINCRIS,             -- Fecha de inicio de riesgo
                                    coalesce(cast(PC.NULLDATE  as varchar),'') TVENCRI,            -- Fecha de fin de riesgo
                                    '' TSITRIS,                                                     -- Fecha de estado de la unidad del riesgo
                                    '' KACSITUR,                                                    -- Codigo de estad de la unidad del riesgo
                                    '' as KACESQM,
                                    'LPG' as DCOMPA,                                                    -- Empresa a la que pertenece la informacion
                                    '' as DMARCA,
                                    '' as DCREHIP,
                                    '' as DCDLCRIS,
                                    '' as DSUBPOST,
                                    '' as KACTPOPS,                                                     -- Codigo del tipo de persona asegurada
                                    '' as KACTPAGR,
                                    '' as KACTPBON,
                                    '' as KACTPDES,
                                    '' as DDEUNRIS,
                                    '' as TDACTRIS,
                                    '' as TDCANRIS,
                                    '' as TDGRARIS,
                                    '' as TDRENOVA,
                                    '' as TDVENTRA,
                                    '' as DHORAINI,
                                    1  as DQOBJSEG,                                                     -- Numero de objetos asegurados 
                                    0  as VCAPITAL,                                                     -- Importe de capital
                                    '' as VMTPRABP,
                                    0  as VMTPRMBR,                                                     -- Importe de Prima Bruta
                                    0  as VMTCOMR,                                                      -- Importe de Prima Comercial
                                    '' as VMTPRLIQ,
                                    '' as VMTPREMC,                                                     -- Comision de la prima
                                    '' as VMTPRMTR,
                                    '' as VMTBOMAT,                                                     -- Monto de bonificacion
                                    '' as VTXBOMAT,
                                    '' as VMTBOCOM,                                                     -- Monto de bonificacion comercial
                                    '' as VTXBOCOM,
                                    '' as VMTDECOM,
                                    '' as VTXDECOM,
                                    '' as VMTDETEC,
                                    '' as VTXDETEC,
                                    '' as VMTAGRAV,
                                    '' as VTXAGRAV,
                                    '' as VMIBOMAT,
                                    '' as VMIBOCOM,
                                    '' as VMIDECOM,
                                    '' as VMIDETEC,
                                    '' as VMIPRMBR,
                                    '' as VMICOMR,
                                    '' as VMIPRLIQ,
                                    '' as VMIPRMTR,
                                    '' as VMIAGRAV,
                                    '' as VMIRPMSP,
                                    '' as VMICMNQP,
                                    '' as DNUMOBPR,
                                    '' as DNUMOBSE,
                                    '' as KACINDRE,                                                     -- Indicador de Reaseguro
                                    '' as DINOBJCB,
                                    '' as DINVADIV,
                                    '' as DINSINAN,
                                    '' as DINSNANC,
                                    '' as DINREGFL
                                    from USINSUG01.POLICY P 
                                    LEFT JOIN USINSUG01.CERTIFICAT CERT 
                                    ON P.USERCOMP = CERT.USERCOMP 
                                    AND P.COMPANY = CERT.COMPANY 
                                    AND P.CERTYPE = CERT.CERTYPE 
                                    AND P.BRANCH  = CERT.BRANCH 
                                    AND P.POLICY  = CERT.policy
                                    JOIN USINSUG01.POL_SUBPRODUCT PSP
                                    ON  PSP.USERCOMP = P.USERCOMP
                                    AND PSP.COMPANY  = P.COMPANY
                                    AND PSP.CERTYPE  = P.CERTYPE
                                    AND PSP.BRANCH   = P.BRANCH		   
                                    AND PSP.PRODUCT  = P.PRODUCT
                                    AND PSP.POLICY   = P.policy
                                    join usinsug01.address ad
                                    ON  AD.USERCOMP = P.USERCOMP 
                                    AND AD.COMPANY  = P.COMPANY 
                                    AND AD.CERTYPE  = P.CERTYPE
                                    AND AD.BRANCH   = P.BRANCH 
                                    AND AD.POLICY   = PC.POLICY 
                                    AND AD.CERTIF   = PC.CERTIF
                                    where CAST(ad.compdate AS DATE) between '{p_fecha_inicio}' and '{p_fecha_fin}'
                             ) AS TMP
                             '''

      L_DF_ABUNRIS_INSUNIX_G_PAT = glue_context.read.format('jdbc').options(**connection).option("dbtable",L_ABUNRIS_INSUNIX_G_PAT).load()
      print("ABUNRIS INSUNIX PAT")

      L_ABUNRIS_INSUNIX_G_AUT = f'''
                             (
                               select 
                                'D' AS INDDETREC,
                                'ABUNRIS' AS TABLAIFRS17,
                                '' PK,                                                          -- Clave compuesta
                                '' as DTPREG,
                                '' as TIOCPROC,
                                coalesce(cast(tnb.effecdate as varchar),'') TIOCFRM,            -- Fecha de inicio de validez del registro
                                '' as TIOCTO,
                                'PIG' KGIORIGM,                                                  -- Indicador
                                coalesce(cast(tnb.branch as varchar),'')|| '-' || coalesce(cast(P.PRODUCT as varchar),'')|| '-' || coalesce(cast(PSP.SUB_PRODUCT as varchar),'') || '-' || coalesce(cast(p.policy as varchar),'') ||  '-' || coalesce(cast(cert.certif as varchar),'') KABAPOL,  --Numero de Poliza
                                /*(select "RISKTYPEL"  from USBI01."IFRS170_T_RAMOS_POR_TIPO_RIESGO" where "BRANCHCOM" = tnb.branch  and "SOURCESCHEMA" = 'usinsug01')*/'AUT' KACTPRIS ,     -- Codigo del Tipo de riesgo 
                                trim(TNB.REGIST)|| '-' || trim(TNB.CHASSIS)  DUNIRIS,           -- Codigo de Unidad de riesgo,                                                          -- Codigo de unidad de riesgo 
                                coalesce(cast(TNB.STARTDATE as varchar),'') TINCRIS,            -- Fecha de inicio del riesgo
                                coalesce(cast(TNB.EXPIRDAT as varchar),'')TVENCRI,              -- Fecha de vencimiento del riesgo
                                '' as TSITRIS,                                                  -- Fecha de estado de la unidad del riesgo
                                '' as KACSITUR,                                                 -- Codigo de estad de la unidad del riesgo
                                '' as KACESQM,
                                'LPG' as DCOMPA,                                                -- Empresa a la que pertenece la informacion
                                '' as DMARCA,
                                '' as DCREHIP,
                                '' as DCDLCRIS,
                                '' as DSUBPOST,
                                '' as KACTPOPS,                                                 -- Codigo del tipo de persona asegurada
                                '' as KACTPAGR,
                                '' as KACTPBON,
                                '' as KACTPDES,
                                '' as DDEUNRIS,
                                '' as TDACTRIS,
                                '' as TDCANRIS,
                                '' as TDGRARIS,
                                '' as TDRENOVA,
                                '' as TDVENTRA,
                                '' as DHORAINI,
                                1  as DQOBJSEG,                                                 -- Numero de objetos asegurados 
                                coalesce(cast(tnb.CAPITAL as numeric(14,2)),0) VCAPITAL,        -- Importe Capital asegurado
                                '' as VMTPRABP,
                                coalesce(cast(tnb.PREMIUM as numeric(12,2)),0) VMTPRMBR,        -- Importe de Prima Bruta
                                coalesce(cast(tnb.PREMIUM as numeric(12,2)),0) VMTCOMR,         -- Importe de Prima Comercial
                                '' as VMTPRLIQ,
                                '' as VMTPREMC,                                                 -- Comision de la prima
                                '' as VMTPRMTR,
                                '' as VMTBOMAT,                                                 -- Monto de bonificacion
                                '' as VTXBOMAT,
                                '' as VMTBOCOM,                                                 -- Monto de bonificacion comercial
                                '' as VTXBOCOM,
                                '' as VMTDECOM,
                                '' as VTXDECOM,
                                '' as VMTDETEC,
                                '' as VTXDETEC,
                                '' as VMTAGRAV,
                                '' as VTXAGRAV,
                                '' as VMIBOMAT,
                                '' as VMIBOCOM,
                                '' as VMIDECOM,
                                '' as VMIDETEC,
                                '' as VMIPRMBR,
                                '' as VMICOMR,
                                '' as VMIPRLIQ,
                                '' as VMIPRMTR,
                                '' as VMIAGRAV,
                                '' as VMIRPMSP,
                                '' as VMICMNQP,
                                '' as DNUMOBPR,
                                '' as DNUMOBSE,
                                '' as KACINDRE,                                                 -- Indicador de Reaseguro
                                '' as DINOBJCB,
                                '' as DINVADIV,
                                '' as DINSINAN,
                                '' as DINSNANC,
                                '' as DINREGFL
                                from USINSUG01.POLICY P 
                                LEFT JOIN USINSUG01.CERTIFICAT CERT 
                                ON P.USERCOMP = CERT.USERCOMP 
                                AND P.COMPANY = CERT.COMPANY 
                                AND P.CERTYPE = CERT.CERTYPE 
                                AND P.BRANCH  = CERT.BRANCH 
                                AND P.POLICY  = CERT.policy
                                JOIN USINSUG01.POL_SUBPRODUCT PSP
                                ON  PSP.USERCOMP = P.USERCOMP
                                AND PSP.COMPANY  = P.COMPANY
                                AND PSP.CERTYPE  = P.CERTYPE
                                AND PSP.BRANCH   = P.BRANCH		   
                                AND PSP.PRODUCT  = P.PRODUCT
                                AND PSP.POLICY   = P.POLICY	
                                JOIN USBI01."IFRS170_T_RAMOS_POR_TIPO_RIESGO" RTR 
                                ON RTR."BRANCHCOM" = P.BRANCH 
                                AND  RTR."RISKTYPEN" = 3
                                AND RTR."SOURCESCHEMA" = 'usinsug01'
                                join usinsug01.auto_peru tnb
                                ON  TNB.USERCOMP = P.USERCOMP
                                      AND TNB.COMPANY  = P.COMPANY 
                                      AND TNB.CERTYPE  = P.CERTYPE
                                      AND TNB.BRANCH   = P.BRANCH 
                                      AND TNB.POLICY   = P.POLICY 
                                      AND TNB.CERTIF   = cert.CERTIF
                                      AND TNB.EFFECDATE <= cert.EFFECDATE 
                                      AND (TNB.NULLDATE IS NULL OR TNB.NULLDATE > cert.EFFECDATE)
                                WHERE P.CERTYPE = '2'
                                AND P.STATUS_POL NOT IN ('2','3')
                                and CAST(tnb.compdate AS DATE) between '{p_fecha_inicio}' and '{p_fecha_fin}'
                             ) AS TMP
                             '''

    
      L_DF_ABUNRIS_INSUNIX_G_AUT = glue_context.read.format('jdbc').options(**connection).option("dbtable",L_ABUNRIS_INSUNIX_G_AUT).load()
      print("ABUNRIS INSUNIX AUT")

      #UNION DE INSUNIX GENERAL
      L_DF_ABUNRIS_INX_G = L_DF_ABUNRIS_INSUNIX_G_PES.union(L_DF_ABUNRIS_INSUNIX_G_PAT).union(L_DF_ABUNRIS_INSUNIX_G_AUT)


      L_ABUNRIS_INSUNIX_V_PES = f'''
                            (
                              select 
                              'D' AS INDDETREC,
                              'ABUNRIS' AS TABLAIFRS17,
                              '' PK,                                                          -- Clave compuesta
                              '' as DTPREG,
                              '' as TIOCPROC,
                              coalesce(cast(rol.effecdate as varchar),'') TIOCFRM,            -- Fecha de inicio de validez del registro
                              '' as TIOCTO,
                              'PIV' KGIORIGM,                                                  -- Indicador
                              coalesce(cast(rol.branch as varchar),'') || '-' || coalesce(cast(P.PRODUCT as varchar),'')|| '-' || coalesce(cast(rol.policy as varchar),'') ||  '-' || coalesce(cast(rol.certif as varchar),'') KABAPOL,  -- Numero de Poliza
                              /*(select "RISKTYPEL"  from USBI01."IFRS170_T_RAMOS_POR_TIPO_RIESGO" where "BRANCHCOM" = rol.branch  and "SOURCESCHEMA" = 'usinsug01')*/'PES' KACTPRIS ,           -- Codigo del Tipo de riesgo 
                              (select evi.scod_vt  FROM usinsug01.equi_vt_inx evi  WHERE evi.scod_inx  = rol.client)  DUNIRIS,                                                          -- Codigo de unidad de riesgo 
                              coalesce(cast(rol.effecdate as varchar),'') TINCRIS,            -- Fecha de inicio de riesgo
                              coalesce(cast(rol.NULLDATE  as varchar),'') TVENCRI,            -- Fecha de fin de riesgo
                              '' as TSITRIS,                                                  -- Fecha de estado de la unidad del riesgo
                              '' as KACSITUR,                                                 -- Codigo de estad de la unidad del riesgo
                              '' as KACESQM,
                              'LPV' as DCOMPA,                                                -- Empresa a la que pertenece la informacion
                              '' as DMARCA,
                              '' as DCREHIP,
                              '' as DCDLCRIS,
                              '' as DSUBPOST,
                              '' as KACTPOPS,                                                 -- Codigo del tipo de persona asegurada
                              '' as KACTPAGR,
                              '' as KACTPBON,
                              '' as KACTPDES,
                              '' as DDEUNRIS,
                              '' as TDACTRIS,
                              '' as TDCANRIS,
                              '' as TDGRARIS,
                              '' as TDRENOVA,
                              '' as TDVENTRA,
                              '' as DHORAINI,
                              1  as DQOBJSEG,                                                 -- Numero de objetos asegurados 
                              0 VCAPITAL,                                                     -- Importe Capital asegurado
                              '' as VMTPRABP,
                              0 VMTPRMBR,                                                     -- Importe de Prima Bruta
                              0 VMTCOMR,                                                      -- Importe de Prima Comercial
                              '' as VMTPRLIQ,
                              '' as VMTPREMC,                                                 -- Comision de la prima
                              '' as VMTPRMTR,
                              '' as VMTBOMAT,                                                 -- Monto de bonificacion
                              '' as VTXBOMAT,
                              '' as VMTBOCOM,                                                 -- Monto de bonificacion comercial
                              '' as VTXBOCOM,
                              '' as VMTDECOM,
                              '' as VTXDECOM,
                              '' as VMTDETEC,
                              '' as VTXDETEC,
                              '' as VMTAGRAV,
                              '' as VTXAGRAV,
                              '' as VMIBOMAT,
                              '' as VMIBOCOM,
                              '' as VMIDECOM,
                              '' as VMIDETEC,
                              '' as VMIPRMBR,
                              '' as VMICOMR,
                              '' as VMIPRLIQ,
                              '' as VMIPRMTR,
                              '' as VMIAGRAV,
                              '' as VMIRPMSP,
                              '' as VMICMNQP,
                              '' as DNUMOBPR,
                              '' as DNUMOBSE,
                              '' as KACINDRE,                                                 -- Indicador de Reaseguro
                              '' as DINOBJCB,
                              '' as DINVADIV,
                              '' as DINSINAN,
                              '' as DINSNANC,
                              '' as DINREGFL
                              from USINSUV01.POLICY P 
                              LEFT JOIN USINSUV01.CERTIFICAT CERT 
                              ON P.USERCOMP = CERT.USERCOMP 
                              AND P.COMPANY = CERT.COMPANY 
                              AND P.CERTYPE = CERT.CERTYPE 
                              AND P.BRANCH  = CERT.BRANCH 
                              AND P.POLICY  = CERT.policy	
                              JOIN USBI01."IFRS170_T_RAMOS_POR_TIPO_RIESGO" RTR 
                              ON RTR."BRANCHCOM" = P.BRANCH 
                              AND  RTR."RISKTYPEN" = 1 
                              AND RTR."SOURCESCHEMA" = 'usinsuv01'
                              join usinsuv01.roles rol
                              ON  ROL.USERCOMP = P.USERCOMP 
                              AND ROL.COMPANY  = P.COMPANY 
                              AND ROL.CERTYPE  = P.CERTYPE
                              AND ROL.BRANCH   = P.BRANCH 
                              AND ROL.POLICY   = P.POLICY 
                              AND ROL.CERTIF   = cert.CERTIF  
                              AND ROL.EFFECDATE <= cert.EFFECDATE 
                              AND (ROL.NULLDATE IS NULL OR ROL.NULLDATE > cert.EFFECDATE)
                              AND ROL.ROLE IN (2,8)
                              WHERE P.CERTYPE = '2' 
                              AND P.STATUS_POL NOT IN ('2','3')
                              and CAST(rol.compdate AS DATE) between '{p_fecha_inicio}' and '{p_fecha_fin}'
                            ) AS TMP
                           '''
    
    #EJECUTAR CONSULTA

      L_DF_ABUNRIS_INSUNIX_V_PES = glue_context.read.format('jdbc').options(**connection).option("dbtable",L_ABUNRIS_INSUNIX_V_PES).load()
      print("ABUNRIS INSUNIX V PES")
    
      L_ABUNRIS_VTIME_G_PES = f'''
                            (
                              select 
                              'D' AS INDDETREC,
                              'ABUNRIS' AS TABLAIFRS17,
                              '' PK,                                                          -- Clave compuesta
                              '' as DTPREG,
                              '' as TIOCPROC,
                              coalesce(cast(cast(rol."DEFFECDATE" as date) as varchar),'') TIOCFRM,                          -- Fecha de inicio de validez del registro
                              '' as TIOCTO,
                              'PVG' KGIORIGM,                                                                                -- Indicador
                              rol."NBRANCH" || '-' || P."NPRODUCT" || '-' || rol."NPOLICY"  ||  '-' || rol."NCERTIF"  KABAPOL,                      -- Numero de Poliza
                              /*(select "RISKTYPEL"  from USBI01."IFRS170_T_RAMOS_POR_TIPO_RIESGO" where "BRANCHCOM" = rol."NBRANCH" and "SOURCESCHEMA" = 'usvtimg01')*/'PES' KACTPRIS ,     -- Codigo del Tipo de riesgo 
                              rol."SCLIENT" DUNIRIS,                                                                         -- Codigo de unidad de riesgo 
                              coalesce(cast(cast(rol."DEFFECDATE" as date) as varchar),'')  TINCRIS,                         -- Fecha de inicio de riesgo
                              coalesce(cast(cast(rol."DNULLDATE" as date) as VARCHAR),'')TVENCRI,                            -- Fecha de fin de riesgo
                              '' as TSITRIS,                                                  -- Fecha de estado de la unidad del riesgo
                              '' as KACSITUR,                                                 -- Codigo de estad de la unidad del riesgo
                              '' as KACESQM,
                              'LPG' as DCOMPA,                                                -- Empresa a la que pertenece la informacion
                              '' as DMARCA,
                              '' as DCREHIP,
                              '' as DCDLCRIS,
                              '' as DSUBPOST,
                              '' as KACTPOPS,                                                 -- Codigo del tipo de persona asegurada
                              '' as KACTPAGR,
                              '' as KACTPBON,
                              '' as KACTPDES,
                              '' as DDEUNRIS,
                              '' as TDACTRIS,
                              '' as TDCANRIS,
                              '' as TDGRARIS,
                              '' as TDRENOVA,
                              '' as TDVENTRA,
                              '' as DHORAINI,
                              1  as DQOBJSEG,                                                 -- Numero de objetos asegurados
                              0 VCAPITA,                                                      -- Importe de capital Asegurado
                              '' as VMTPRABP,
                              0 VMTPRMBR,                                                     -- Importe de Prima Bruta
                              0 VMTCOMR,                                                      -- Importe de Prima Comercial
                              '' as VMTPRLIQ,
                              '' as VMTPREMC,                                                 -- Comision de la prima
                              '' as VMTPRMTR,
                              '' as VMTBOMAT,                                                 -- Monto de bonificacion
                              '' as VTXBOMAT,
                              '' as VMTBOCOM,                                                 -- Monto de bonificacion comercial
                              '' as VTXBOCOM,
                              '' as VMTDECOM,
                              '' as VTXDECOM,
                              '' as VMTDETEC,
                              '' as VTXDETEC,
                              '' as VMTAGRAV,
                              '' as VTXAGRAV,
                              '' as VMIBOMAT,
                              '' as VMIBOCOM,
                              '' as VMIDECOM,
                              '' as VMIDETEC,
                              '' as VMIPRMBR,
                              '' as VMICOMR,
                              '' as VMIPRLIQ,
                              '' as VMIPRMTR,
                              '' as VMIAGRAV,
                              '' as VMIRPMSP,
                              '' as VMICMNQP,
                              '' as DNUMOBPR,
                              '' as DNUMOBSE,
                              '' as KACINDRE,                                                 -- Indicador de Reaseguro
                              '' as DINOBJCB,
                              '' as DINVADIV,
                              '' as DINSINAN,
                              '' as DINSNANC,
                              '' as DINREGFL
                              FROM USVTIMG01."POLICY" P 
                              LEFT JOIN USVTIMG01."CERTIFICAT" CERT 
                              ON  P."SCERTYPE" = CERT."SCERTYPE" 
                              AND P."NBRANCH"  = CERT."NBRANCH"
                              AND P."NPRODUCT" = CERT."NPRODUCT"
                              AND P."NPOLICY"  = CERT."NPOLICY"
                              JOIN USBI01."IFRS170_T_RAMOS_POR_TIPO_RIESGO" RTR 
                              ON RTR."BRANCHCOM" = P."NBRANCH" 
                              AND  RTR."RISKTYPEN" = 1 
                              AND RTR."SOURCESCHEMA" = 'usvtimg01'
                              join usvtimg01."ROLES" rol
                              ON  ROL."SCERTYPE"  = P."SCERTYPE"
                              AND ROL."NBRANCH"   = P."NBRANCH"  
                              AND ROL."NPRODUCT"  = P."NPRODUCT"
                              AND ROL."NPOLICY"   = P."NPOLICY" 
                              AND ROL."NCERTIF"   = CERT."NCERTIF"  
                              AND ROL."DEFFECDATE" <= P."DSTARTDATE" 
                              AND (ROL."DNULLDATE" IS NULL OR ROL."DNULLDATE" > P."DSTARTDATE")
                              and ROL."NROLE" IN (2,8)
                              WHERE P."SCERTYPE" = '2' 
                              AND P."SSTATUS_POL" NOT IN ('2','3')
                              and CAST(ROL."DCOMPDATE" AS DATE) between '{p_fecha_inicio}' and '{p_fecha_fin}'
                            ) AS TMP
                           '''
    
      #EJECUTAR CONSULTA
      L_DF_ABUNRIS_VTIME_G_PES = glue_context.read.format('jdbc').options(**connection).option("dbtable",L_ABUNRIS_VTIME_G_PES).load()
      print("ABUNRIS VTIME LPG PES")                            

      L_ABUNRIS_VTIME_G_PAT = f'''
                            (
                              select 
                              'D' AS INDDETREC,
                              'ABUNRIS' AS TABLAIFRS17,
                              '' PK,                                                          -- Clave compuesta
                              '' as DTPREG,
                              '' as TIOCPROC,
                              coalesce(cast(cast(ad."DEFFECDATE" as date) as varchar),'')  TIOCFRM,                         -- Fecha de inicio de validez del registro
                              '' as TIOCTO,
                              'PVG' KGIORIGM,                                                                                -- Indicador
                              ad."NBRANCH" || '-' || P."NPRODUCT" || '-' || ad."NPOLICY" ||  '-' || ad."NCERTIF"  KABAPOL,                          -- Numero de Poliza
                              /*(select "RISKTYPEL"  from USBI01."IFRS170_T_RAMOS_POR_TIPO_RIESGO" where "BRANCHCOM" = ad."NBRANCH" and "SOURCESCHEMA" = 'usvtimg01' )*/'PAT' KACTPRIS ,     -- Codigo del Tipo de riesgo 
                              ad."SKEYADDRESS" DUNIRIS,                                                                      -- Codigo de unidad de riesgo  
                              coalesce(cast(cast(ad."DEFFECDATE" as date) as varchar),'')  TINCRIS,                          -- Fecha de Inicio del riesgo
                              coalesce(cast(ad."DNULLDATE" as VARCHAR),'')  TVENCRI,                                         -- Fecha de vencimiento del riesgo 
                              '' as TSITRIS,                                                  -- Fecha de estado de la unidad del riesgo
                              '' as KACSITUR,                                                 -- Codigo de estad de la unidad del riesgo
                              '' as KACESQM,
                              'LPG' as DCOMPA,                                                -- Empresa a la que pertenece la informacion
                              '' as DMARCA,
                              '' as DCREHIP,
                              '' as DCDLCRIS,
                              '' as DSUBPOST,
                              '' as KACTPOPS,                                                 -- Codigo del tipo de persona asegurada
                              '' as KACTPAGR,
                              '' as KACTPBON,
                              '' as KACTPDES,
                              '' as DDEUNRIS,
                              '' as TDACTRIS,
                              '' as TDCANRIS,
                              '' as TDGRARIS,
                              '' as TDRENOVA,
                              '' as TDVENTRA,
                              '' as DHORAINI,
                              1  as DQOBJSEG,                                                 -- Numero de objetos asegurados
                              0 VCAPITA,                                                      -- Importe de capital Asegurado
                              '' as VMTPRABP,
                              0 VMTPRMBR,                                                     -- Importe de Prima Bruta
                              0 VMTCOMR,                                                      -- Importe de Prima Comercial
                              '' as VMTPRLIQ,
                              '' as VMTPREMC,                                                 -- Comision de la prima
                              '' as VMTPRMTR,
                              '' as VMTBOMAT,                                                 -- Monto de bonificacion
                              '' as VTXBOMAT,
                              '' as VMTBOCOM,                                                 -- Monto de bonificacion comercial
                              '' as VTXBOCOM,
                              '' as VMTDECOM,
                              '' as VTXDECOM,
                              '' as VMTDETEC,
                              '' as VTXDETEC,
                              '' as VMTAGRAV,
                              '' as VTXAGRAV,
                              '' as VMIBOMAT,
                              '' as VMIBOCOM,
                              '' as VMIDECOM,
                              '' as VMIDETEC,
                              '' as VMIPRMBR,
                              '' as VMICOMR,
                              '' as VMIPRLIQ,
                              '' as VMIPRMTR,
                              '' as VMIAGRAV,
                              '' as VMIRPMSP,
                              '' as VMICMNQP,
                              '' as DNUMOBPR,
                              '' as DNUMOBSE,
                              '' as KACINDRE,                                                 -- Indicador de Reaseguro
                              '' as DINOBJCB,
                              '' as DINVADIV,
                              '' as DINSINAN,
                              '' as DINSNANC,
                              '' as DINREGFL
                              FROM USVTIMG01."POLICY" P 
                              LEFT JOIN USVTIMG01."CERTIFICAT" CERT 
                              ON  P."SCERTYPE" = CERT."SCERTYPE" 
                              AND P."NBRANCH"  = CERT."NBRANCH"
                              AND P."NPRODUCT" = CERT."NPRODUCT"
                              AND P."NPOLICY"  = CERT."NPOLICY"
                              JOIN USBI01."IFRS170_T_RAMOS_POR_TIPO_RIESGO" RTR 
                              ON RTR."BRANCHCOM" = P."NBRANCH" 
                              AND  RTR."RISKTYPEN" = 2 
                              AND RTR."SOURCESCHEMA" = 'usvtimg01'
                              join usvtimg01."ADDRESS" ad
                              ON  AD."SCERTYPE"  = P."SCERTYPE"
                              AND AD."NBRANCH"   = P."NBRANCH" 
                              AND AD."NPRODUCT"  = P."NPRODUCT" 
                              AND AD."NPOLICY"   = P."NPOLICY" 
                              AND AD."NCERTIF"   = cert."NCERTIF" 
                              WHERE P."SCERTYPE" = '2' 
                              AND P."SSTATUS_POL" NOT IN ('2','3')
                              and CAST(ad."DCOMPDATE" AS DATE) between '{p_fecha_inicio}' and '{p_fecha_fin}'
                            ) AS TMP
                           '''
    

      L_DF_ABUNRIS_VTIME_G_PAT = glue_context.read.format('jdbc').options(**connection).option("dbtable",L_ABUNRIS_VTIME_G_PAT).load()
      print("ABUNRIS VTIME LPG PAT")  

      L_ABUNRIS_VTIME_G_AUT = f'''
                            (
                              select 
                              'D' AS INDDETREC,
                              'ABUNRIS' AS TABLAIFRS17,
                              '' PK,                                                          -- Clave compuesta
                              '' as DTPREG,
                              '' as TIOCPROC,
                              coalesce(cast(cast(aut."DEFFECDATE" as date) as varchar),'')  TIOCFRM,                        -- Fecha de inicio de validez de registro
                              '' as TIOCTO,
                              'PVG' KGIORIGM,                                                                               -- Indicador
                              aut."NBRANCH" || '-' ||  P."NPRODUCT" || '-' || aut."NPOLICY" ||  '-' || aut."NCERTIF" KABAPOL,                      -- Numero de Poliza
                              /*(select "RISKTYPEL"  from USBI01."IFRS170_T_RAMOS_POR_TIPO_RIESGO" where "BRANCHCOM" = aut."NBRANCH" and "SOURCESCHEMA" = 'usvtimg01')*/ 'AUT' KACTPRIS ,     -- Codigo del Tipo de riesgo
                              coalesce(trim(aut."SREGIST"),'') || '-' || coalesce(trim(aut."SCHASSIS"),'')  DUNIRIS,        -- Codigo de Unidad de riesgo
                              coalesce(cast(cast(aut."DSTARTDATE"as date)as varchar),'')  TINCRIS,                          -- Fecha de inicio del riesgo
                              coalesce(cast(aut."DEXPIRDAT" as varchar),'')  TVENCRI,                                       -- Fecha de vencimiento del riesgo
                              '' as TSITRIS,                                                  -- Fecha de estado de la unidad del riesgo
                              '' as KACSITUR,                                                 -- Codigo de estad de la unidad del riesgo
                              '' as KACESQM,
                              'LPG' as DCOMPA,                                                -- Empresa a la que pertenece la informacion
                              '' as DMARCA,
                              '' as DCREHIP,
                              '' as DCDLCRIS,
                              '' as DSUBPOST,
                              '' as KACTPOPS,                                                 -- Codigo del tipo de persona asegurada
                              '' as KACTPAGR,
                              '' as KACTPBON,
                              '' as KACTPDES,
                              '' as DDEUNRIS,
                              '' as TDACTRIS,
                              '' as TDCANRIS,
                              '' as TDGRARIS,
                              '' as TDRENOVA,
                              '' as TDVENTRA,
                              '' as DHORAINI,
                              1  as DQOBJSEG,                                                 -- Numero de objetos asegurados
                              coalesce(cast(aut."NCAPITAL" as NUMERIC(14,2)),0) VCAPITAL,     -- Importe Capital asegurado
                              '' as VMTPRABP,
                              coalesce(cast(aut."NPREMIUM" as NUMERIC(12,2)),0) VMTPRMBR,     -- Importe de Prima Bruta
                              coalesce(cast(aut."NPREMIUM" as NUMERIC(12,2)),0) VMTCOMR,      -- Importe de Prima Comercial
                              '' as VMTPRLIQ,
                              '' as VMTPREMC,                                                 -- Comision de la prima
                              '' as VMTPRMTR,
                              '' as VMTBOMAT,                                                 -- Monto de bonificacion
                              '' as VTXBOMAT,
                              '' as VMTBOCOM,                                                 -- Monto de bonificacion comercial
                              '' as VTXBOCOM,
                              '' as VMTDECOM,
                              '' as VTXDECOM,
                              '' as VMTDETEC,
                              '' as VTXDETEC,
                              '' as VMTAGRAV,
                              '' as VTXAGRAV,
                              '' as VMIBOMAT,
                              '' as VMIBOCOM,
                              '' as VMIDECOM,
                              '' as VMIDETEC,
                              '' as VMIPRMBR,
                              '' as VMICOMR,
                              '' as VMIPRLIQ,
                              '' as VMIPRMTR,
                              '' as VMIAGRAV,
                              '' as VMIRPMSP,
                              '' as VMICMNQP,
                              '' as DNUMOBPR,
                              '' as DNUMOBSE,
                              '' as KACINDRE,                                                 -- Indicador de Reaseguro
                              '' as DINOBJCB,
                              '' as DINVADIV,
                              '' as DINSINAN,
                              '' as DINSNANC,
                              '' as DINREGFL
                              FROM USVTIMG01."POLICY" P 
                              LEFT JOIN USVTIMG01."CERTIFICAT" CERT 
                              ON  P."SCERTYPE" = CERT."SCERTYPE" 
                              AND P."NBRANCH"  = CERT."NBRANCH"
                              AND P."NPRODUCT" = CERT."NPRODUCT"
                              AND P."NPOLICY"  = CERT."NPOLICY"
                              JOIN USBI01."IFRS170_T_RAMOS_POR_TIPO_RIESGO" RTR 
                              ON RTR."BRANCHCOM" = P."NBRANCH" 
                              AND  RTR."RISKTYPEN" = 3
                              AND RTR."SOURCESCHEMA" = 'usvtimg01'
                              join usvtimg01."AUTO" aut
                              ON  AUT."SCERTYPE"  = P."SCERTYPE"
                              AND AUT."NBRANCH"   = P."NBRANCH" 
                              AND AUT."NPRODUCT"  = P."NPRODUCT"
                              AND AUT."NPOLICY"   = P."NPOLICY" 
                              AND AUT."NCERTIF"   = cert."NCERTIF"
                              AND AUT."DEFFECDATE" <= P."DSTARTDATE"
                              AND (AUT."DNULLDATE" IS NULL OR AUT."DNULLDATE" > P."DSTARTDATE") 
                              WHERE P."SCERTYPE" = '2' 
                              AND P."SSTATUS_POL" NOT IN ('2','3') 
                              and CAST(p."DCOMPDATE" AS DATE) between '{p_fecha_inicio}' and '{p_fecha_fin}'
                            ) AS TMP
                           '''
    

      L_DF_ABUNRIS_VTIME_G_AUT = glue_context.read.format('jdbc').options(**connection).option("dbtable",L_ABUNRIS_VTIME_G_AUT).load()
      print("ABUNRIS VTIME LPG AUT")  


      L_DF_ABUNRIS_VTIME_G = L_DF_ABUNRIS_VTIME_G_PES.union(L_DF_ABUNRIS_VTIME_G_PAT).union(L_DF_ABUNRIS_VTIME_G_AUT)

      L_ABUNRIS_VTIME_V_PES = f'''
                            (
                              select 
                              'D' AS INDDETREC,
                              'ABUNRIS' AS TABLAIFRS17,
                              '' PK,                                                          -- Clave compuesta
                              '' as DTPREG,
                              '' as TIOCPROC,
                              coalesce(cast(cast(rol."DEFFECDATE" as date) as varchar),'')  TIOCFRM,                        -- Fecha de inicio de validez de registro
                              '' as TIOCTO,
                              'PVV' KGIORIGM,                                                                               -- Indicador
                              rol."NBRANCH" || '-' ||  P."NPRODUCT" || '-' || rol."NPOLICY" ||  '-' || rol."NCERTIF" KABAPOL,                      -- Numero de Poliza
                              /*(select "RISKTYPEL"  from USBI01."IFRS170_T_RAMOS_POR_TIPO_RIESGO" where "BRANCHCOM" = rol."NBRANCH" and "SOURCESCHEMA" = 'usvtimv01' )*/ 'PES' KACTPRIS ,     -- Codigo del Tipo de riesgo  
                              rol."SCLIENT"    DUNIRIS,                                                                     -- Codigo de unidad de riesgo 
                              coalesce(cast(cast(rol."DEFFECDATE" as date) as varchar),'') TINCRIS,                         -- Fecha de inicio de riesgo
                              coalesce(cast(rol."DNULLDATE" as VARCHAR),'')  TVENCRI,                                       -- Fecha de vencimiento del riesgo
                              '' as TSITRIS,                                                  -- Fecha de estado de la unidad del riesgo
                              '' as KACSITUR,                                                 -- Codigo de estad de la unidad del riesgo
                              '' as KACESQM,
                              'LPV' as DCOMPA,                                                -- Empresa a la que pertenece la informacion
                              '' as DMARCA,
                              '' as DCREHIP,
                              '' as DCDLCRIS,
                              '' as DSUBPOST,
                              '' as KACTPOPS,                                                 -- Codigo del tipo de persona asegurada
                              '' as KACTPAGR,
                              '' as KACTPBON,
                              '' as KACTPDES,
                              '' as DDEUNRIS,
                              '' as TDACTRIS,
                              '' as TDCANRIS,
                              '' as TDGRARIS,
                              '' as TDRENOVA,
                              '' as TDVENTRA,
                              '' as DHORAINI,
                              1  as DQOBJSEG,                                                 -- Numero de objetos asegurados
                              0 VCAPITA,                                                      -- Importe de capital
                              '' as VMTPRABP,
                              0 VMTPRMBR,                                                     -- Importe de Prima Bruta
                              0 VMTCOMR,                                                      -- Importe de Prima Comercial 
                              '' as VMTPRLIQ,
                              '' as VMTPREMC,                                                 -- Comision de la prima
                              '' as VMTPRMTR,
                              '' as VMTBOMAT,                                                 -- Monto de bonificacion
                              '' as VTXBOMAT,
                              '' as VMTBOCOM,                                                 -- Monto de bonificacion comercial
                              '' as VTXBOCOM,
                              '' as VMTDECOM,
                              '' as VTXDECOM,
                              '' as VMTDETEC,
                              '' as VTXDETEC,
                              '' as VMTAGRAV,
                              '' as VTXAGRAV,
                              '' as VMIBOMAT,
                              '' as VMIBOCOM,
                              '' as VMIDECOM,
                              '' as VMIDETEC,
                              '' as VMIPRMBR,
                              '' as VMICOMR,
                              '' as VMIPRLIQ,
                              '' as VMIPRMTR,
                              '' as VMIAGRAV,
                              '' as VMIRPMSP,
                              '' as VMICMNQP,
                              '' as DNUMOBPR,
                              '' as DNUMOBSE,
                              '' as KACINDRE,                                                 -- Indicador de Reaseguro
                              '' as DINOBJCB,
                              '' as DINVADIV,
                              '' as DINSINAN,
                              '' as DINSNANC,
                              '' as DINREGFL
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
                              join USVTIMV01."ROLES" ROL
                              ON  ROL."SCERTYPE"  = P."SCERTYPE"
                              AND ROL."NBRANCH"   = P."NBRANCH" 
                              AND ROL."NPRODUCT"  = P."NPRODUCT"
                              AND ROL."NPOLICY"   = P."NPOLICY" 
                              AND ROL."NCERTIF"   = CERT."NCERTIF"  
                              AND ROL."DEFFECDATE" <= P."DSTARTDATE" 
                              AND (ROL."DNULLDATE" IS NULL OR ROL."DNULLDATE" > P."DSTARTDATE")
                              AND ROL."NROLE" IN (2,8) 
                              WHERE P."SCERTYPE" = '2' 
                              AND P."SSTATUS_POL" NOT IN ('2','3')
                              and CAST(rol."DCOMPDATE" AS DATE) between '{p_fecha_inicio}' and '{p_fecha_fin}'
                            ) AS TMP
                           '''
    

      L_DF_ABUNRIS_VTIME_V_PES = glue_context.read.format('jdbc').options(**connection).option("dbtable",L_ABUNRIS_VTIME_V_PES).load()
      print("ABUNRIS VTIME LPV PES")

      L_ABUNRIS_INSIS_V = f'''
                            (select 
                              'D' AS INDDETREC,
                              'ABUNRIS' AS TABLAIFRS17,
                              '' PK,                                                          
                              '' as DTPREG,
                              '' as TIOCPROC,
                              cast(cast(io."INSR_BEGIN" as date)as varchar) as TIOCFRM,                        
                              '' as TIOCTO,
                              'PNV' KGIORIGM,                                      
                              SUBSTRING(cast(io."POLICY_ID" as varchar),6,12) KABAPOL,                      
                              '' KACTPRIS,     
                              io."INSURED_OBJ_ID"  DUNIRIS,                                                                     
                              cast(cast(io."INSR_BEGIN" as date)as varchar) TINCRIS,                         
                              cast(cast(io."INSR_END" as date)as varchar) TVENCRI,      
                              '' as TSITRIS,                                                  
                              io."INSURED_OBJ_ID"  as KACSITUR,                                                 
                              '' as KACESQM,
                              'LPV' as DCOMPA,                                                
                              '' as DMARCA,
                              '' as DCREHIP,
                              '' as DCDLCRIS,
                              '' as DSUBPOST,
                              '' as KACTPOPS,                                                 
                              '' as KACTPAGR,
                              '' as KACTPBON,
                              '' as KACTPDES,
                              '' as DDEUNRIS,
                              '' as TDACTRIS,
                              '' as TDCANRIS,
                              '' as TDGRARIS,
                              cast(cast(io."INSR_BEGIN" as date)as varchar) as TDRENOVA,
                              '' as TDVENTRA,
                              '' as DHORAINI,
                              1  as DQOBJSEG,                                                 
                              io."INSURED_VALUE"  VCAPITA,                                                      
                              '' as VMTPRABP,
                              0 VMTPRMBR,                                                     
                              0 VMTCOMR,                                                      
                              '' as VMTPRLIQ,
                              '' as VMTPREMC,                                                 
                              '' as VMTPRMTR,
                              '' as VMTBOMAT,                                                 
                              '' as VTXBOMAT,
                              '' as VMTBOCOM,                                                 
                              '' as VTXBOCOM,
                              '' as VMTDECOM,
                              '' as VTXDECOM,
                              '' as VMTDETEC,
                              '' as VTXDETEC,
                              '' as VMTAGRAV,
                              '' as VTXAGRAV,
                              '' as VMIBOMAT,
                              '' as VMIBOCOM,
                              '' as VMIDECOM,
                              '' as VMIDETEC,
                              '' as VMIPRMBR,
                              '' as VMICOMR,
                              '' as VMIPRLIQ,
                              '' as VMIPRMTR,
                              '' as VMIAGRAV,
                              '' as VMIRPMSP,
                              '' as VMICMNQP,
                              '' as DNUMOBPR,
                              '' as DNUMOBSE,
                              '' as KACINDRE,                                                 
                              '' as DINOBJCB,
                              '' as DINVADIV,
                              '' as DINSINAN,
                              '' as DINSNANC,
                              '' as DINREGFL                              
                             from  usinsiv01."INSURED_OBJECT" io 
                             where cast(io."REGISTRATION_DATE" as date)  between  '{p_fecha_inicio}' AND '{p_fecha_fin}'
                             limit 100 -- AQUI LA OBSERVACION SERIA QUE SE VE MAS BIEN LA CARGA INCREMENTAL Y NO LA CARGA INICIAL. GABRIEL 
                            ) AS TMP
                           '''
    

      L_DF_ABUNRIS_INSIS_V = glue_context.read.format('jdbc').options(**connection).option("dbtable",L_ABUNRIS_INSIS_V).load()
      print("ABUNRIS INSIS")

      #PERFORM THE UNION OPERATION
      L_DF_ABUNRIS = L_DF_ABUNRIS_INX_G.union(L_DF_ABUNRIS_INSUNIX_V_PES).union(L_DF_ABUNRIS_VTIME_G).union(L_DF_ABUNRIS_VTIME_V_PES).union(L_DF_ABUNRIS_INSIS_V)
    
      L_DF_ABUNRIS = L_DF_ABUNRIS.withColumn("DQOBJSEG",col("DQOBJSEG").cast(DecimalType(10,0))).withColumn("VCAPITAL",col("VCAPITAL").cast(DecimalType(14,2))).withColumn("VMTPRMBR",col("VMTPRMBR").cast(DecimalType(12,2))).withColumn("VMTCOMR",col("VMTCOMR").cast(DecimalType(12,2)))

      return L_DF_ABUNRIS
