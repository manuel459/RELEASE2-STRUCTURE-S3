
def get_data(glue_context, bucket ,tablas):
 
 l_fecha_carga_inicial = '2021-12-31'

 l_obressegura_insunix_lpg = f'''
                                  SELECT 
                                  'D' AS INDDETREC,
                                  'OBRESSEGURA' AS TABLAIFRS17,
                                  '' AS PK,
                                  '' AS DTPREG,
                                  '' AS TIOCPROC,
                                  COALESCE(CAST(CC.EFFECDATE AS STRING) , '')AS TIOCFRM,
                                  '' AS TIOCTO,
                                  'PIG' AS KGIORIGM,
                                  CC.NUMBER  ||'-'|| CC.BRANCH   AS DCDINTTRA,
                                  CC.CURRENCY ||''|| CC.YEAR_CONTR  ||''|| CC.TYPE AS KOCSBTRT,
                                  '' AS DDESCDSBTRT,
                                  '' AS KOCPOOL,
                                  COALESCE(CC.COMPANYC,0) AS KOCCDRESS,
                                  COALESCE(CC.COMPANYC,0) AS DCDRESS,
                                  COALESCE(CAST(CC.SUPERVIS AS CHAR(4)),'') AS DCDCORR,
                                  COALESCE((
                                            SELECT MAX(EVI.SCOD_VT)
                                            FROM EQUI_VT_INX EVI
                                            LEFT JOIN  COMPANY C
                                            ON C.CLIENT = EVI.SCOD_INX
                                            WHERE CC.COMPANYC = C.CODE
                                  ),'')  AS KEBENTID_RSS,
                                  COALESCE(CAST(CC.SHARE AS NUMERIC(7,4)),0)  AS VTXPART,
                                  'LPG' AS DCOMPA,
                                  '' AS DMARCA
                                  FROM CONTR_COMP CC
                                  WHERE (CC.EFFECDATE >= '{l_fecha_carga_inicial}'
                                  OR (CC.NULLDATE IS NULL OR CC.NULLDATE > '{l_fecha_carga_inicial}'))
                                  
                               '''

#--------------------------------------------------------------------------------------------------------------------------# 
 l_obressegura_insunix_lpv = f'''
                                SELECT
                                'D' AS INDDETREC,
                                'OBRESSEGURA' AS TABLAIFRS17,
                                '' AS PK,
                                '' AS DTPREG,
                                '' AS TIOCPROC,
                                CAST(CC.EFFECDATE AS STRING)AS TIOCFRM,
                                '' AS TIOCTO,
                                'PIV' AS KGIORIGM,
                                CC.NUMBER  ||'-'|| CC.BRANCH   AS DCDINTTRA,
                                CC.CURRENCY ||''|| CC.YEAR_CONTR  ||''|| CC.TYPE AS KOCSBTRT,
                                '' AS DDESCDSBTRT,
                                '' AS KOCPOOL,
                                COALESCE(CC.COMPANYC,0) AS KOCCDRESS,
                                COALESCE(CC.COMPANYC,0) AS DCDRESS,
                                COALESCE(CAST(CC.SUPERVIS AS CHAR(4)),'') AS DCDCORR,
                                COALESCE((
                                          SELECT MAX(EVI.SCOD_VT)
                                          FROM EQUI_VT_INX EVI
                                          LEFT JOIN  COMPANY C  
                                          ON C.CLIENT = EVI.SCOD_INX 
                                          WHERE CC.COMPANYC = C.CODE 
                                ),'')  AS KEBENTID_RSS,
                                COALESCE(CAST(CC.SHARE AS NUMERIC(7,4)),0)  AS VTXPART,
                                'LPV' AS DCOMPA,
                                '' AS DMARCA
                                FROM CONTR_COMP CC 
                                WHERE (CC.EFFECDATE >= '{l_fecha_carga_inicial}'
                                  OR (CC.NULLDATE IS NULL OR CC.NULLDATE > '{l_fecha_carga_inicial}'))
                             '''
#--------------------------------------------------------------------------------------------------------------------------# 
 l_obressegura_vtime_lpg = f'''
                               SELECT
                               'D' AS INDDETREC,
                               'OBRESSEGURA' AS TABLAIFRS17,
                               '' AS PK,
                               '' AS DTPREG,
                               '' AS TIOCPROC,
                               COALESCE(CAST(CAST(PC.DEFFECDATE AS DATE) AS STRING),'') AS TIOCFRM,
                               '' AS TIOCTO,
                               'PVG' AS KGIORIGM,
                               PC.NNUMBER  ||'-'|| PC.NBRANCH  AS DCDINTTRA,
                               '' AS KOCSBTRT,
                               '' AS DDESCDSBTRT,
                               '' AS KOCPOOL,
                               PC.NCOMPANY   AS KOCCDRESS,
                               PC.NCOMPANY  AS DCDRESS,
                               COALESCE(CAST(PC.NCORREDOR AS CHAR(4)),'')  AS DCDCORR,
                               COALESCE((
                                         SELECT MAX(C.SCLIENT)
                                         FROM COMPANY C
                                         WHERE PC.NCOMPANY = C.NCOMPANY
                               ),'')  AS KEBENTID_RSS,
                               COALESCE(CAST(PC.NSHARE AS NUMERIC(7,4)),0)  AS VTXPART,
                               'LPG' AS DCOMPA,
                               '' AS DMARCA
                               FROM PART_CONTR PC
                               WHERE (CAST(PC.DEFFECDATE AS DATE) >= '{l_fecha_carga_inicial}'
                                    OR (PC.DNULLDATE IS NULL OR CAST(PC.DNULLDATE AS DATE) > '{l_fecha_carga_inicial}'))
                             '''
#--------------------------------------------------------------------------------------------------------------------------# 
 l_obressegura_vtime_lpv = f'''
                               SELECT
                               'D' AS INDDETREC,
                               'OBRESSEGURA' AS TABLAIFRS17,
                               '' AS PK,
                               '' AS DTPREG,
                               '' AS TIOCPROC,
                               CAST(CAST(PC.DEFFECDATE AS DATE) AS STRING) AS TIOCFRM,
                               '' AS TIOCTO,
                               'PVV' AS KGIORIGM,
                               PC.NNUMBER  ||'-'|| PC.NBRANCH  AS DCDINTTRA,
                               '' AS KOCSBTRT,
                               '' AS DDESCDSBTRT,
                               '' AS KOCPOOL,
                               PC.NCOMPANY  AS KOCCDRESS,
                               PC.NCOMPANY AS DCDRESS,
                               COALESCE(CAST(PC.NCORREDOR AS CHAR(4)),'')  AS DCDCORR,
                               COALESCE((
                                         SELECT MAX(C.SCLIENT)
                                         FROM COMPANY C
                                         WHERE PC.NCOMPANY = C.NCOMPANY 
                               ),'')  AS KEBENTID_RSS,
                               COALESCE(CAST(PC.NSHARE AS NUMERIC(7,4)),0)  AS VTXPART,
                               'LPV' AS DCOMPA,
                               '' AS DMARCA
                               FROM PART_CONTR PC 
                               WHERE (CAST(PC.DEFFECDATE AS DATE) >= '{l_fecha_carga_inicial}'
                                    OR (PC.DNULLDATE IS NULL OR CAST(PC.DNULLDATE AS DATE) > '{l_fecha_carga_inicial}'))
                            '''
#--------------------------------------------------------------------------------------------------------------------------#
 l_obressegura_insis_lpv = f'''
                                SELECT
                                'D' AS INDDETREC,
                                'OBRESSEGURA' AS TABLAIFRS17,
                                '' AS PK,
                                '' AS DTPREG,
                                '' AS TIOCPROC,
                                COALESCE(CAST(CAST(RTR.ACTIVE_FROM AS DATE) AS STRING),'') AS TIOCFRM,
                                '' AS TIOCTO,
                                'PNV' AS KGIORIGM,
                                '' AS DCDINTTRA,
                                '' AS KOCSBTRT,
                                '' AS DDESCDSBTRT,
                                '' AS KOCPOOL,
                                RTR.REINSURER_ID AS KOCCDRESS,
                                '' AS DCDRESS,
                                ''  AS DCDCORR,
                                RTR.REINSURER_ID  AS KEBENTID_RSS,
                                CAST(RTR.REINRINSR_SHARE AS NUMERIC(7,4)) AS VTXPART,
                                'LPV' AS DCOMPA,
                                '' AS DMARCA
                                FROM RI_TREATY_REINSURERS RTR
                                WHERE (CAST(RTR.ACTIVE_FROM AS DATE) >= '{l_fecha_carga_inicial}'
                                    OR (RTR.ACTIVE_TO IS NULL OR CAST(RTR.ACTIVE_TO AS DATE) > '{l_fecha_carga_inicial}'))
                            '''
  #--------------------------------------------------------------------------------------------------------------------------#

 spark = glue_context.spark_session
 l_df_obresegura = None
  
 print(tablas)
  
 for tabla in tablas:
    print('Aqui esta la lista de tablas:',tabla['lista'])
    
    for item in tabla['lista']:
      view_name = item['vista']
      file_path = item['path']
      
      print('la vista : ', view_name)
      print('el path origen: ',file_path)
      
      # Leer datos desde Parquet usando pandas
      pandas_df = spark.read.parquet('s3://'+bucket+'/'+file_path)

      pandas_df.createOrReplaceTempView(view_name)
      
    current_df = spark.sql(locals()[tabla['var']])
    print('la variable a ejecutar', tabla['var'])
    
    if l_df_obresegura is None:
      l_df_obresegura = current_df
    else: 
      # Ejecutar la consulta final
      l_df_obresegura = l_df_obresegura.union(current_df)
    current_df.show()
  
 print('Proceso Final')
 l_df_obresegura.show()
    
 return l_df_obresegura