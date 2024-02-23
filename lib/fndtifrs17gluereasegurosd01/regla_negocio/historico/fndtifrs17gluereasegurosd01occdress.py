def get_data(glue_context, bucket ,tablas, p_fecha_inicio, p_fecha_fin):

 l_occdress_insunix_lpg = '''
                         select
                         'D' as INDDETREC,
                         'OCCDRESS' as TABLAIFRS17,
                         '' as PK,
                         '' as DTPREG,
                         '' as TIOCPROC,
                         coalesce(cast(c.EFFECDATE as STRING) ,'') as TIOCFRM,
                         '' as TIOCTO,
                         'PIG' as KGIORIGM,
                         coalesce((SELECT  MAX(EVI.SCOD_VT)
                                   FROM EQUI_VT_INX EVI
                                   WHERE c.CLIENT = EVI.SCOD_INX),'') as DCODIGO,
                         '' as DDESC,
                         '' as KOICDRESS
                         FROM COMPANY C --1994-02-16 - 2023-08-04 {'p_fecha_inicio}'}
                      '''
#--------------------------------------------------------------------------------------------------------------------------# 
 l_occdress_insunix_lpv = '''
                         select
                         'D' as INDDETREC,
                         'OCCDRESS' as TABLAIFRS17,
                         '' as PK,
                         '' as DTPREG,
                         '' as TIOCPROC,
                         coalesce(cast(c.EFFECDATE as STRING) ,'') as TIOCFRM,
                         '' as TIOCTO,
                         'PIV' as KGIORIGM,
                         coalesce((SELECT  MAX(EVI.SCOD_VT)
                                   FROM EQUI_VT_INX EVI
                                   WHERE c.CLIENT = EVI.SCOD_INX
                         ),'') as DCODIGO,
                         '' as DDESC,
                         '' as KOICDRESS
                         FROM COMPANY C --1994-02-16 - 2023-08-04
                      '''
#--------------------------------------------------------------------------------------------------------------------------#
 l_occdress_vtime_lpg = '''
                           select
                           'D' as INDDETREC,
                           'OCCDRESS' as TABLAIFRS17,
                           '' as PK,
                           '' as DTPREG,
                           '' as TIOCPROC,
                           cast(c.DCOMPDATE as date ) as TIOCFRM,
                           '' as TIOCTO,
                           'PVG' as KGIORIGM,
                           coalesce(c.SCLIENT,'')  as DCODIGO,
                           '' as DDESC,
                           '' as KOICDRESS
                           FROM COMPANY C --2007-12-03 - 2023-08-04 
                        '''
#--------------------------------------------------------------------------------------------------------------------------#
 l_occdress_vtime_lpv = '''
                           select
                           'D' as INDDETREC,
                           'OCCDRESS' as TABLAIFRS17,
                           '' as PK,
                           '' as DTPREG,
                           '' as TIOCPROC,
                           cast(C.DCOMPDATE as date) as TIOCFRM,
                           '' as TIOCTO,
                           'PVV' as KGIORIGM,
                           coalesce(C.SCLIENT,'')  as DCODIGO,
                           '' as DDESC,
                           '' as KOICDRESS
                           FROM COMPANY C --2007-12-03 - 2023-08-04 
                        '''
#--------------------------------------------------------------------------------------------------------------------------#
 l_occdress_insis_lpv = '''
                           select
                           'D' as INDDETREC,
                           'OCCDRESS' as TABLAIFRS17,
                           '' as PK,
                           '' as DTPREG,
                           '' as TIOCPROC,
                           cast(PIN.fecha_replicacion_positiva  as date) as TIOCFRM,
                           '' as TIOCTO,
                           'PNV' as KGIORIGM,
                           PIN.MAN_ID as DCODIGO,
                           '' as DDESC,
                           '' as KOICDRESS
                           FROM P_INSURERS PIN --2023-11-06 - 2023-11-06 
                           --LA TABLA ORIGINAL NO TIENE FECHAS 
                        '''
  #--------------------------------------------------------------------------------------------------------------------------#

 spark = glue_context.spark_session
 l_df_occdress = None
  
 print(tablas)
 print(p_fecha_inicio)
 print(p_fecha_fin)
  
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
    
    if l_df_occdress is None:
      l_df_occdress = current_df
    else: 
      # Ejecutar la consulta final
      l_df_occdress = l_df_occdress.union(current_df)
    current_df.show()
  
 print('Proceso Final')
 l_df_occdress.show()
    
 return l_df_occdress