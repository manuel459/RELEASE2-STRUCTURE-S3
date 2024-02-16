
def get_data(glue_context, bucket ,tablas, p_fecha_inicio, p_fecha_fin):

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
                                  WHERE CC.COMPDATE BETWEEN '{p_fecha_inicio}' AND '{p_fecha_fin}'
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
                                WHERE CC.COMPDATE BETWEEN '{p_fecha_inicio}' AND '{p_fecha_fin}' --'1999-10-06' '2017-02-22'
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
                               WHERE CAST(PC.DCOMPDATE AS DATE) BETWEEN '{p_fecha_inicio}' AND '{p_fecha_fin}' --2008-12-05 - 2023-08-31
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
                               WHERE CAST(PC.DCOMPDATE AS DATE) BETWEEN '{p_fecha_inicio}' AND '{p_fecha_fin}' --'2007-11-20'	'2016-04-15' 
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
                                WHERE CAST(RTR.ACTIVE_FROM AS DATE) BETWEEN '{p_fecha_inicio}' and '{p_fecha_fin}'
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