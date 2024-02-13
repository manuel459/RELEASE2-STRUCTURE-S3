
def get_data(glue_context, bucket ,tablas, p_fecha_inicio, p_fecha_fin):
  
  l_ebentid_insis = f'''
                          SELECT 
                          'D' AS INDDETREC,
                          'EBENTID' AS TABLAIFRS17, 
                          ILPI.LEGACY_ID AS PK,
                          '' AS DTPREG,
                          '' AS TIOCPROC,
                          COALESCE(CAST(PX.VALID_FROM AS DATE), CAST(PP.REGISTRATION_DATE AS DATE)) AS TIOCFRM,
                          '' AS TIOCTO,
                          'PAE' AS KGIORIGM,								 
                          ILPI.LEGACY_ID DCODIGO,
                          CASE
                          WHEN (PP.COMP_TYPE NOT IN ('IN', 'IS') AND PP.NATIONALITY <> 'PT') THEN PP.NAME
                          ELSE ''
                          END DNOME,
                          COALESCE(CAST(CAST(PP.BIRTH_DATE AS DATE) AS STRING), '')  AS TNASCIAC,
                          '' AS DBI,
                          '' AS DNIF,
                          '' AS KECTITLO,
                          '' AS KECNAC,
                          COALESCE(PP.COMP_TYPE, '') AS KECTPENT,
                          '' AS KECPROF,
                          '' AS KECESTCV,
                          COALESCE(CAST(PP.SEX AS STRING), '')  AS KECSEXO,
                          '' AS KECCAE,
                          'LPV' AS DCOMPA,
                          '' AS DLEICODE,
                          '' AS DRSRCODE,
                          '' AS VMTMAXCAP,
                          '' AS KECMOEDA
                          FROM P_PEOPLE PP
                          JOIN INTRF_LPV_PEOPLE_IDS ILPI
                          ON ILPI.MAN_ID = PP.MAN_ID
                          left JOIN  
                          (
                            (SELECT  --CLIENTES CON REGISTROS VALIDOS
                            DISTINCT PPC.MAN_ID,
                            CAST(PPC.VALID_FROM AS DATE) VALID_FROM                       
                              FROM P_PEOPLE_CHANGES PPC  
                              WHERE PPC.VALID_TO IS NULL)
                            
                            UNION
                                                                                  
                            (SELECT --CLIENTES CON REGISTROS NO VALIDOS
                              PPC.MAN_ID,
                              CAST(PPC.VALID_FROM AS DATE) VALID_FROM
                              FROM P_PEOPLE_CHANGES PPC 
                              WHERE NOT EXISTS (SELECT 1 FROM P_PEOPLE_CHANGES PPC2  
                                          WHERE PPC2.VALID_TO IS NULL AND PPC2.MAN_ID = PPC.MAN_ID ))
                          ) PX
                          ON PP.MAN_ID = PX.MAN_ID
                          WHERE PP.REGISTRATION_DATE BETWEEN '{p_fecha_inicio}' AND '{p_fecha_fin}'
                        '''
    
    #EJECUTAR CONSULTA
      
  #--------------------------------------------------------------------------------------------------------------------------#
  
  spark = glue_context.spark_session
  l_df_ebentid = None
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
    
    if l_df_ebentid is None:
      l_df_ebentid = current_df
    else: 
      # Ejecutar la consulta final
      l_df_ebentid = l_df_ebentid.union(current_df)
    current_df.show()
  
  print('Proceso Final')
  l_df_ebentid.show()

  return l_df_ebentid