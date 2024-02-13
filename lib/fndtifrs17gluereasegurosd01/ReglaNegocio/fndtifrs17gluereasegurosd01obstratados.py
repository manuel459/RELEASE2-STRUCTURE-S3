def get_data(glue_context, bucket ,tablas, p_fecha_inicio, p_fecha_fin):
    
 l_obstratados_insunix_lpg = f'''
                                 select
                                 'D' AS INDDETREC,
                                 'OBTRATADOS' AS TABLAIFRS17,
                                 '' AS PK,
                                 '' AS DTPREG,
                                 '' AS TIOCPROC,
                                 coalesce(CAST(C.EFFECDATE AS STRING),'') AS TIOCFRM,
                                 '' AS TIOCTO,
                                 'PIG' AS KGIORIGM,
                                 CAST(C.NUMBER AS STRING) ||'-'|| C.BRANCH  AS DCDINTTRA,
                                 C.CURRENCY ||'-'|| C.TYPE AS DCDTRAT_SO,
                                 '' AS DDESCDTRA,
                                 '' AS DDESABRTRA,
                                 coalesce(CAST(C.STARTDAT AS STRING),'') AS TINICIO,
                                 coalesce(CAST(C.EXPIRDAT AS STRING),'') AS TTERMO,
                                 coalesce(CAST(C.YEAR_CONTR AS STRING),'') AS DANOTRAT,
                                 coalesce(CAST(C.TYPE AS STRING),'') AS KOCTPRESS,
                                 '' AS KOCTPFRC,
                                 '' AS KOCVLDFRC,
                                 '' AS KOCTPTRT,
                                 '' AS KOCTPDUR,
                                 '' AS KOCTPOBJ,
                                 '' AS KOCSIT,
                                 '' AS DCDTRAT,
                                 'LPG' AS DCOMPA,
                                 '' AS DMARCA,
                                 '' AS KOCSCOPE,
                                 '' AS KOCIDFAC,
                                 '' AS KOCTPRNP,
                                 '' AS KACSEGM,
                                 '' AS KOCMOEDA,
                                 '' AS DINDPMAX,
                                 '' AS VMTMAXTR,
                                 '' AS VMTPLENO,
                                 '' AS VMTPDEDT,
                                 '' AS KOCGRCBT,
                                 '' AS VMTPRUN,
                                 '' AS KGCRAMO_SAP
                                 FROM CONTRPROC C --1995-08-01 - 2023-07-31
                                 WHERE C.COMPDATE BETWEEN '{p_fecha_inicio}' AND '{p_fecha_fin}'
                              '''
#--------------------------------------------------------------------------------------------------------------------------# 
 l_obstratados_insunix_lpv = f'''
                             select
                             'D' AS INDDETREC,
                             'OBTRATADOS' AS TABLAIFRS17,
                             '' AS PK,
                             '' AS DTPREG,
                             '' AS TIOCPROC,
                             coalesce(CAST(C.EFFECDATE AS STRING),'') AS TIOCFRM,
                             '' AS TIOCTO,
                             'PIV' AS KGIORIGM,
                             CAST(C.NUMBER AS STRING) ||'-'|| C.branch  AS DCDINTTRA,
                             '' AS DCDTRAT_SO,
                             '' AS DDESCDTRA,
                             '' AS DDESABRTRA,
                             coalesce(CAST(C.STARTDAT AS STRING),'') AS TINICIO,
                             coalesce(CAST(C.EXPIRDAT AS STRING),'') AS TTERMO,
                             coalesce(CAST(C.YEAR_CONTR AS STRING),'') AS DANOTRAT,
                             coalesce(CAST(C.TYPE AS STRING),'') AS KOCTPRESS,
                             '' AS KOCTPFRC,
                             '' AS KOCVLDFRC,
                             '' AS KOCTPTRT,
                             '' AS KOCTPDUR,
                             '' AS KOCTPOBJ,
                             '' AS KOCSIT,
                             '' AS DCDTRAT,
                             'LPV' AS DCOMPA,
                             '' AS DMARCA,
                             '' AS KOCSCOPE,
                             '' AS KOCIDFAC,
                             '' AS KOCTPRNP,
                             '' AS KACSEGM,
                             '' AS KOCMOEDA,
                             '' AS DINDPMAX,
                             '' AS VMTMAXTR,
                             '' AS VMTPLENO,
                             '' AS VMTPDEDT,
                             '' AS KOCGRCBT,
                             '' AS VMTPRUN,
                             '' AS KGCRAMO_SAP
                             FROM CONTRPROC C
                             WHERE C.COMPDATE BETWEEN '{p_fecha_inicio}' AND '{p_fecha_fin}' --1995-08-01 - 2022-12-30
                             '''
#--------------------------------------------------------------------------------------------------------------------------# 
 l_obstratados_vtime_lpg = f'''
                               select
                               'D' AS INDDETREC,
                               'OBTRATADOS' AS TABLAIFRS17,
                               '' AS PK,
                               '' AS DTPREG,
                               '' AS TIOCPROC,
                               CAST(C.DEFFECDATE AS DATE)  AS TIOCFRM,
                               '' AS TIOCTO,
                               'PVG' AS KGIORIGM,
                               CAST(C.NNUMBER AS STRING) ||'-'|| C.NBRANCH AS DCDINTTRA,
                               '' AS DCDTRAT_SO,
                               '' AS DDESCDTRA,
                               '' AS DDESABRTRA,
                               CAST( CAST(C.DEFFECDATE AS date) AS STRING) AS TINICIO,
                               coalesce(CAST( CAST(C.DNULLDATE AS date) AS STRING),'') AS TTERMO,
                               coalesce(CAST(C.NYEAR_BEGIN AS STRING),'') AS DANOTRAT,
                               CAST(C.NTYPE AS STRING)  AS KOCTPRESS,
                               '' AS KOCTPFRC,
                               '' AS KOCVLDFRC,
                               '' AS KOCTPTRT,
                               '' AS KOCTPDUR,
                               '' AS KOCTPOBJ,
                               '' AS KOCSIT,
                               '' AS DCDTRAT,
                               'LPG' AS DCOMPA,
                               '' AS DMARCA,
                               '' AS KOCSCOPE,
                               '' AS KOCIDFAC,
                               '' AS KOCTPRNP,
                               '' AS KACSEGM,
                               '' AS KOCMOEDA,
                               '' AS DINDPMAX,
                               '' AS VMTMAXTR,
                               '' AS VMTPLENO,
                               '' AS VMTPDEDT,
                               '' AS KOCGRCBT,
                               '' AS VMTPRUN,
                               '' AS KGCRAMO_SAP
                               FROM CONTRPROC C --2009-01-17 - 2023-03-30
                               WHERE CAST(C.DCOMPDATE AS DATE) BETWEEN '{p_fecha_inicio}' AND '{p_fecha_fin}'
                            '''
#--------------------------------------------------------------------------------------------------------------------------#
 l_obstratados_vtime_lpv = f'''
                               select 
                               'D' AS INDDETREC,
                               'OBTRATADOS' AS TABLAIFRS17,
                               '' AS PK,
                               '' AS DTPREG,
                               '' AS TIOCPROC,
                               CAST(C.DEFFECDATE AS DATE)  AS TIOCFRM,
                               '' AS TIOCTO,
                               'PVV' AS KGIORIGM,
                               CAST(C.NNUMBER AS STRING) ||'-'|| C.NBRANCH AS DCDINTTRA,
                               '' AS DCDTRAT_SO,
                               '' AS DDESCDTRA,
                               '' AS DDESABRTRA,
                               CAST(CAST(C.DEFFECDATE AS date) AS STRING) AS TINICIO,
                               coalesce(CAST( CAST(C.DNULLDATE AS date) AS STRING),'') AS TTERMO,
                               coalesce(CAST(C.NYEAR_BEGIN AS STRING),'')  AS DANOTRAT,
                               CAST(C.NTYPE AS STRING)  AS KOCTPRESS,
                               '' AS KOCTPFRC,
                               '' AS KOCVLDFRC,
                               '' AS KOCTPTRT,
                               '' AS KOCTPDUR,
                               '' AS KOCTPOBJ,
                               '' AS KOCSIT,
                               '' AS DCDTRAT,
                               'LPV' AS DCOMPA,
                               '' AS DMARCA,
                               '' AS KOCSCOPE,
                               '' AS KOCIDFAC,
                               '' AS KOCTPRNP,
                               '' AS KACSEGM,
                               '' AS KOCMOEDA,
                               '' AS DINDPMAX,
                               '' AS VMTMAXTR,
                               '' AS VMTPLENO,
                               '' AS VMTPDEDT,
                               '' AS KOCGRCBT,
                               '' AS VMTPRUN,
                               '' AS KGCRAMO_SAP
                               FROM CONTRPROC C --2006-06-02 - 2017-08-14 
                               WHERE CAST(C.DCOMPDATE AS DATE) BETWEEN '{p_fecha_inicio}' AND '{p_fecha_fin}'
                            '''
#--------------------------------------------------------------------------------------------------------------------------#
 l_obstratados_insis_lpv = f'''
                            select 
                            'D' AS INDDETREC,
                            'OBTRATADOS' AS TABLAIFRS17,
                            '' AS PK,
                            '' AS DTPREG,
                            '' AS TIOCPROC,
                            CAST(RT.START_DATE AS DATE) AS TIOCFRM,
                            '' AS TIOCTO,
                            'PNV' AS KGIORIGM,
                            RT.TREATY_ID  AS DCDINTTRA,
                            '' AS DCDTRAT_SO,
                            '' AS DDESCDTRA,
                            '' AS DDESABRTRA,
                            CAST(RT.START_DATE AS DATE) AS TINICIO,
                            CAST(RT.END_DATE   AS DATE) AS TTERMO,
                            '' AS DANOTRAT,
                            RT.TREATY_SUBTYPE AS KOCTPRESS,
                            '' AS KOCTPFRC,
                            '' AS KOCVLDFRC,
                            '' AS KOCTPTRT,
                            '' AS KOCTPDUR,
                            '' AS KOCTPOBJ,
                            '' AS KOCSIT,
                            '' AS DCDTRAT,
                            'LPV' AS DCOMPA,
                            '' AS DMARCA,
                            '' AS KOCSCOPE,
                            '' AS KOCIDFAC,
                            '' AS KOCTPRNP,
                            '' AS KACSEGM,
                            '' AS KOCMOEDA,
                            '' AS DINDPMAX,
                            '' AS VMTMAXTR,
                            '' AS VMTPLENO,
                            '' AS VMTPDEDT,
                            '' AS KOCGRCBT,
                            '' AS VMTPRUN,
                            '' AS KGCRAMO_SAP
                            FROM RI_TREATY RT --1995-01-01 - 2021-01-01
                            WHERE CAST(RT.START_DATE AS DATE) BETWEEN '{p_fecha_inicio}' AND '{p_fecha_fin}'
                        '''
  #--------------------------------------------------------------------------------------------------------------------------#

 spark = glue_context.spark_session
 l_df_obstratados = None
  
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
    
    if l_df_obstratados is None:
      l_df_obstratados = current_df
    else: 
      # Ejecutar la consulta final
      l_df_obstratados = l_df_obstratados.union(current_df)
    current_df.show()
  
 print('Proceso Final')
 l_df_obstratados.show()
    
 return l_df_obstratados