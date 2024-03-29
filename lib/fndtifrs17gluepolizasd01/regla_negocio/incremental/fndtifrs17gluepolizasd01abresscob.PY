def get_data(glue_context,connection,p_fecha_inicio, p_fecha_fin):


    L_ABRSSCOB_INSIS = f'''
                        (SELECT
                        '' PK,	                                      --Chave Composta gerada pelo IOC
                        ABR.TIOCFRM,	                                  --Data de Início de validade do registo   --OJO
                        '' TIOCTO,                                    --Data de fim de validade do registo
                        'PNV'                                         --KGIORIGM,	Código do Sistema de ORIGEM
                        KABAPOL,                                      --Chave da Tabela de Apólices             -- OJO 
                        ABR.DNUMAPO,                                      --Numero de Apolice
                        ABR.DNMCERT,                                      --Número de Certificado / Aderente
                        ABR.KABUNRIS,                                     --Chave da tabela de Unidades de Risco
                        ABR.DENTIDSO,	                                  --Número Interno da Entidade no Sistema Operacional.
                        ABR.KEBENTID_PS,                                  --Entidade da Pessoa Segura
                        ABR.KABPRODT,                                     --Chave primária composta da tabela de Produtos
                        ABR.KGCTPCBT,                                     --Código da Cobertura                  -- OJO
                        ABR.QS_PORCE VTXSBPRE,                            --Percentagem sobre Premios
                        CASE WHEN ABR.TIPO_REASEGURO_CARTERA = 'F' THEN 0 ELSE (ABR.qs_sum_reins - ABR.qs_sum_placed) END VMTCAPRE,                                    --Capital Retido
                        CASE WHEN ABR.TIPO_REASEGURO_CARTERA = 'F' THEN 0 ELSE ABR.qs_sum_placed END VMTCAPCQP,                                                    --Capital Cedido Quota Parte
                        CASE WHEN ABR.TIPO_REASEGURO_CARTERA = 'F' THEN 0 ELSE ABR.sp_sum_placed END VMTCAPCEXC,                                                   --Capital Cedido Excedente
                        0 VMTCAPCFAC,                                                                                                                      --Capital Cedido Facultativo
                        CASE WHEN ABR.TIPO_REASEGURO_CARTERA = 'F' THEN 0 ELSE (ABR.qs_sum_reins - ABR.qs_sum_placed) + qs_sum_placed + sp_sum_placed END VMTCAPCUM,   --Capital Cumulativo
                        CASE WHEN ABR.TIPO_REASEGURO_CARTERA = 'F' THEN 0 ELSE ABR.qs_pri END VMTPRMARCQP,                                                         --Premio Anual Resseguro Cedido Quota Parte
                        CASE WHEN ABR.TIPO_REASEGURO_CARTERA = 'F' THEN 0 ELSE ABR.sp_pri END VMTPRMARCEXC,                                                        --Premio Anual Resseguro Cedido Excedente
                        0 VMTPRMARCFAC,                                                                                                                    --Premio Anual Resseguro Cedido Facultativo
                        CASE WHEN ABR.TIPO_REASEGURO_CARTERA = 'F' THEN 0 ELSE ABR.qs_sum_comms END VMTCOMARCED,                                                   --Comissão Anual Resseguro Cedido
                        CASE WHEN ABR.TIPO_REASEGURO_CARTERA = 'F' THEN 0 ELSE ABR.sp_sum_comms END VMTCOMARCEXC,                                                  --Comissão Anual Resseguro Cedido Excedente
                        0 VMTCOMARCFAC,                                                                                                                    --Comissão Anual Resseguro Cedido Facultativo
                        CASE WHEN ABR.TIPO_REASEGURO_CARTERA = 'F' THEN 0 ELSE (ABR.qs_pri + ABR.sp_pri) END VMTPRMPAARES,                                             --Premio Processado Acumulado Ano Resseguro
                        CASE WHEN ABR.TIPO_REASEGURO_CARTERA = 'F' THEN 0 ELSE (ABR.qs_sum_comms + ABR.sp_sum_comms) END VMTCOMPAARES,                                 --Comissão Processada Acumulada Ano Resseguro
                        'LPV' DCOMPA,                                                                                                                      --Companhia ao que pertence a informação
                        CASE WHEN TIPO_REASEGURO_CARTERA = 'A' THEN 0 ELSE ABR.qs_sum_placed END VMTCAPCFACQP,                                                 --Capital_Cedido_Facultativo_Quota_Parte        Se debe ver en Facultativos
                        CASE WHEN TIPO_REASEGURO_CARTERA = 'A' THEN 0 ELSE ABR.sp_sum_placed END VMTCAPCFACEX,                                                 --Capital_Cedido_Facultativo_Excedente          Se debe ver en Facultativos
                        CASE WHEN TIPO_REASEGURO_CARTERA = 'A' THEN 0 ELSE ABR.qs_pri END VMTPRARCFAQP,                                                        --Premio_Anual_Resseguro_Cedido_Facultativo_Quota_Parte Se debe ver en Facultativos
                        CASE WHEN TIPO_REASEGURO_CARTERA = 'A' THEN 0 ELSE ABR.sp_pri END VMTPRARCFAEX                                                        --Premio_Anual_Resseguro_Cedido_Facultativo_Excedente   Se debe ver en Facultativos
                        FROM 
                        (
                        SELECT 	'A' TIPO_REASEGURO_CARTERA,
                                RCP."POLICY_ID" KABAPOL,
                                CASE WHEN PEP."ENG_POL_TYPE" = 'MASTER' THEN    -- Es una poliza matriz de un colectivo
                                          RCP."POLICY_ID"                           -- Es el numero de poliza
                                     WHEN PEP."ENG_POL_TYPE" = 'POLICY' THEN    -- Es una poliza individual
                                          RCP."POLICY_ID"                           -- Es el numero de poliza 
                                     WHEN PEP."ENG_POL_TYPE" = 'DEPENDENT' THEN -- Es una certificado de un colectivo    
                                          PEP."MASTER_POLICY_ID"              
                                END DNUMAPO, 
                                CASE WHEN PEP."ENG_POL_TYPE" = 'MASTER' THEN    -- Es una poliza matriz de un colectivo
                                          0                                   -- Es cero el certificado
                                     WHEN PEP."ENG_POL_TYPE" = 'POLICY' THEN    -- Es una poliza individual
                                          0                                   -- Es cero el certificado
                                     WHEN PEP."ENG_POL_TYPE" = 'DEPENDENT' THEN -- Es una certificado de un colectivo 
                                          RCP."POLICY_ID" 
                                     ELSE 0       
                                END DNMCERT,
                                POL."INSR_TYPE" AS KABPRODT,
                                RCP."INSURED_OBJ_ID"  KABUNRIS,
                                ILPI."LEGACY_ID" KEBENTID_PS,
                                ILPI."MAN_ID" DENTIDSO,
                        		RCP."INSTALL_DATE" TIOCFRM,
                        		(SELECT SUBSTR(CAST("COVER_CPR_ID" AS VARCHAR(15)), 5, 10)
                        		   FROM usinsiv01."CPR_COVER" CC 
                        		  WHERE CC."COVER_TYPE" = RCP."COVER_TYPE" ) AS KGCTPCBT,
                        		MAX(CASE WHEN "RI_CLAUSE_TYPE" = 'QS' THEN "RI_SHARE" ELSE 0 END) QS_PORCE,
                        		SUM(CASE WHEN "RI_CLAUSE_TYPE" = 'QS' THEN "RI_SUM_PLACED" ELSE 0 END) qs_sum_placed,
                        		SUM(CASE WHEN "RI_CLAUSE_TYPE" = 'SP' THEN "RI_SUM_PLACED" ELSE 0 END) sp_sum_Placed,
                        		SUM(CASE WHEN "RI_CLAUSE_TYPE" = 'QS' THEN "RI_PREMIUM" ELSE 0 END) qs_pri,
                        		SUM(CASE WHEN "RI_CLAUSE_TYPE" = 'SP' THEN "RI_PREMIUM" ELSE 0 END) sp_pri,
                        		SUM(CASE WHEN "RI_CLAUSE_TYPE" = 'QS' THEN "RI_SUM_REINSURED" ELSE 0 END) qs_sum_reins,
                        		SUM(CASE WHEN "RI_CLAUSE_TYPE" = 'SP' THEN "RI_SUM_REINSURED" ELSE 0 END) sp_sum_reins,
                        		SUM(CASE WHEN "RI_CLAUSE_TYPE" = 'QS' THEN "RI_COMMISSION" ELSE 0 END) qs_sum_comms,
                        		SUM(CASE WHEN "RI_CLAUSE_TYPE" = 'SP' THEN "RI_COMMISSION" ELSE 0 END) sp_sum_comms
                          FROM usinsiv01."RI_CEDED_PREMIUMS" RCP
                          JOIN usinsiv01."POLICY" POL 
                          ON POL."POLICY_ID" = RCP."POLICY_ID"
                          JOIN usinsiv01."POLICY_ENG_POLICIES" PEP  
                          ON PEP."POLICY_ID" = RCP."POLICY_ID"     -- usinsiv01.POLICY
                          JOIN usinsiv01."INSURED_OBJECT" IO 
                          ON IO."INSURED_OBJ_ID" =  RCP."INSURED_OBJ_ID"
                          JOIN usinsiv01."O_ACCINSURED" OA 
                          ON OA."OBJECT_ID" = IO."OBJECT_ID"
                          JOIN usinsiv01."INTRF_LPV_PEOPLE_IDS" ILPI 
                          ON ILPI."MAN_ID" = OA."MAN_ID"
                          JOIN usinsiv01."P_PEOPLE" PP 
                          ON PP."MAN_ID" = OA."MAN_ID"
                          WHERE RCP."PREMIUM_TYPE" = 'DUE'
                          AND RCP."RI_TREATY_PREM_ID" IS NOT NULL
                          AND (RCP."REGISTRATION_DATE" BETWEEN  '{p_fecha_inicio}' AND '{p_fecha_fin}')
                        GROUP BY
                                 RCP."POLICY_ID",
                                 CASE WHEN PEP."ENG_POL_TYPE" = 'MASTER' THEN    -- Es una poliza matriz de un colectivo
                                          RCP."POLICY_ID"                           -- Es el numero de poliza
                                     WHEN PEP."ENG_POL_TYPE" = 'POLICY' THEN    -- Es una poliza individual
                                          RCP."POLICY_ID"                           -- Es el numero de poliza 
                                     WHEN PEP."ENG_POL_TYPE" = 'DEPENDENT' THEN -- Es una certificado de un colectivo    
                                          PEP."MASTER_POLICY_ID"              
                                 END, 
                                 CASE WHEN PEP."ENG_POL_TYPE" = 'MASTER' THEN    -- Es una poliza matriz de un colectivo
                                          0                                   -- Es cero el certificado
                                     WHEN PEP."ENG_POL_TYPE" = 'POLICY' THEN    -- Es una poliza individual
                                          0                                   -- Es cero el certificado
                                     WHEN PEP."ENG_POL_TYPE" = 'DEPENDENT' THEN -- Es una certificado de un colectivo 
                                          RCP."POLICY_ID"
                                     ELSE 0       
                                 END,
                                 POL."INSR_TYPE",
                                 RCP."INSURED_OBJ_ID",
                                 ILPI."LEGACY_ID" ,
                                 ILPI."MAN_ID" ,
                                 RCP."INSTALL_DATE", 
                                 RCP."COVER_TYPE"

                        --CONTRATOS AUTOMATICOS PROPORCIONALES (INSIS)

                        UNION ALL

                        SELECT 	'F' TIPO_REASEGURO_CARTERA,
                                RCP."POLICY_ID" KABAPOL,
                                CASE WHEN PEP."ENG_POL_TYPE" = 'MASTER' THEN    -- Es una poliza matriz de un colectivo
                                          RCP."POLICY_ID"                       -- Es el numero de poliza
                                     WHEN PEP."ENG_POL_TYPE" = 'POLICY' THEN    -- Es una poliza individual
                                          RCP."POLICY_ID"                       -- Es el numero de poliza 
                                     WHEN PEP."ENG_POL_TYPE" = 'DEPENDENT' THEN -- Es una certificado de un colectivo    
                                          PEP."MASTER_POLICY_ID"              
                                END DNUMAPO, 
                                CASE WHEN PEP."ENG_POL_TYPE" = 'MASTER' THEN    -- Es una poliza matriz de un colectivo
                                          0                                     -- Es cero el certificado
                                     WHEN PEP."ENG_POL_TYPE" = 'POLICY' THEN    -- Es una poliza individual
                                          0                                     -- Es cero el certificado
                                     WHEN PEP."ENG_POL_TYPE" = 'DEPENDENT' THEN -- Es una certificado de un colectivo 
                                          RCP."POLICY_ID" 
                                     ELSE 0       
                                END DNMCERT,
                                POL."INSR_TYPE" AS KABPRODT,
                                RCP."INSURED_OBJ_ID"  KABUNRIS,
                                ILPI."LEGACY_ID" KEBENTID_PS,
                                ILPI."MAN_ID" DENTIDSO,
                        		RCP."INSTALL_DATE" TIOCFRM,
                        	    (SELECT SUBSTR(CAST("COVER_CPR_ID" AS VARCHAR(15)), 5, 10)
                        		   FROM usinsiv01."CPR_COVER" CC 
                        		  WHERE CC."COVER_TYPE" = RCP."COVER_TYPE" ) AS KGCTPCBT,
                        		MAX(CASE WHEN "RI_CLAUSE_TYPE" = 'QS' THEN "RI_SHARE" ELSE 0 END) QS_PORCE,
                        		SUM(CASE WHEN "RI_CLAUSE_TYPE" = 'QS' THEN "RI_SUM_PLACED" ELSE 0 END) qs_sum_placed,
                        		SUM(CASE WHEN "RI_CLAUSE_TYPE" = 'SP' THEN "RI_SUM_PLACED" ELSE 0 END) sp_sum_Placed,
                        		SUM(CASE WHEN "RI_CLAUSE_TYPE" = 'QS' THEN "RI_PREMIUM" ELSE 0 END) qs_pri,
                        		SUM(CASE WHEN "RI_CLAUSE_TYPE" = 'SP' THEN "RI_PREMIUM" ELSE 0 END) sp_pri,
                        		SUM(CASE WHEN "RI_CLAUSE_TYPE" = 'QS' THEN "RI_SUM_REINSURED" ELSE 0 END) qs_sum_reins,
                        		SUM(CASE WHEN "RI_CLAUSE_TYPE" = 'SP' THEN "RI_SUM_REINSURED" ELSE 0 END) sp_sum_reins,
                        		SUM(CASE WHEN "RI_CLAUSE_TYPE" = 'QS' THEN "RI_COMMISSION" ELSE 0 END) qs_sum_comms,
                        		SUM(CASE WHEN "RI_CLAUSE_TYPE" = 'SP' THEN "RI_COMMISSION" ELSE 0 END) sp_sum_comms
                          FROM usinsiv01."RI_CEDED_PREMIUMS" RCP      -- CESIONES DE PRIMA
                          JOIN usinsiv01."POLICY" POL 
                          ON POL."POLICY_ID" = RCP."POLICY_ID"
                          JOIN usinsiv01."POLICY_ENG_POLICIES" PEP  
                          ON PEP."POLICY_ID" = RCP."POLICY_ID"    
                          JOIN usinsiv01."INSURED_OBJECT" IO 
                          ON IO."INSURED_OBJ_ID" =  RCP."INSURED_OBJ_ID"
                          JOIN usinsiv01."O_ACCINSURED" OA 
                          ON OA."OBJECT_ID" = IO."OBJECT_ID"
                          JOIN usinsiv01."INTRF_LPV_PEOPLE_IDS" ILPI 
                          ON ILPI."MAN_ID" = OA."MAN_ID"
                          JOIN usinsiv01."P_PEOPLE" PP 
                          ON PP."MAN_ID" = OA."MAN_ID"
                        WHERE RCP."PREMIUM_TYPE" = 'DUE'
                          AND RCP."RI_FAC_PREM_ID" IS NOT NULL
                          AND (RCP."REGISTRATION_DATE" BETWEEN  '{p_fecha_inicio}' AND '{p_fecha_fin}')
                        GROUP BY
                        RCP."POLICY_ID",
                        CASE WHEN PEP."ENG_POL_TYPE" = 'MASTER' THEN    -- Es una poliza matriz de un colectivo
                                          RCP."POLICY_ID"                        -- Es el numero de poliza
                                     WHEN PEP."ENG_POL_TYPE" = 'POLICY' THEN     -- Es una poliza individual
                                          RCP."POLICY_ID"                        -- Es el numero de poliza 
                                     WHEN PEP."ENG_POL_TYPE" = 'DEPENDENT' THEN  -- Es una certificado de un colectivo    
                                          PEP."MASTER_POLICY_ID"             
                                 END, 
                                 CASE WHEN PEP."ENG_POL_TYPE" = 'MASTER' THEN    -- Es una poliza matriz de un colectivo
                                          0                                      -- Es cero el certificado
                                     WHEN PEP."ENG_POL_TYPE" = 'POLICY' THEN     -- Es una poliza individual
                                          0                                   	 -- Es cero el certificado
                                     WHEN PEP."ENG_POL_TYPE" = 'DEPENDENT' THEN  -- Es una certificado de un colectivo 
                                          RCP."POLICY_ID" 
                                     ELSE 0       
                                 END,
                                 POL."INSR_TYPE",
                                 RCP."INSURED_OBJ_ID",
                                 ILPI."LEGACY_ID",
                                 ILPI."MAN_ID",
                                 RCP."INSTALL_DATE", 
                                 RCP."COVER_TYPE"
                        --CONTRATOS FACULTATIVOS (INSIS)          
                        ) ABR
                        ) AS TMP'''

    L_DF_ABRSSCOB_INSIS = glue_context.read.format('jdbc').options(**connection).option("dbtable",L_ABRSSCOB_INSIS).load()
    
    return L_DF_ABRSSCOB_INSIS
              