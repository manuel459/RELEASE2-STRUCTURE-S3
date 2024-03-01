def get_data(glue_context,connection,p_fecha_inicio, p_fecha_fin):

    L_ABRSSCOB_INSIS = '''
                        (

                        ) AS TMP
                        '''

    L_DF_ABRSSCOB_INSIS = glue_context.read.format('jdbc').options(**connection).option("dbtable",L_ABRSSCOB_INSIS).load()
    
    return L_DF_ABRSSCOB_INSIS
              