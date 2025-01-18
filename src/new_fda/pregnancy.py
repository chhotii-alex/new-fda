

admissions_query = """        
SELECT Enc_num            
   ,adm_dt       
   ,delivery_type         
   ,delivery_type_full       
   ,hosp_svc            
   ,diagnosis           
   ,mrn         
   ,baby_deliver_tm        
 FROM vwADT_Admissions        
 where          
 baby_deliver_tm is not null   
"""

limit_clause = db.make_limit_clause(20) # TODO remove
columns = ', '.join(['Enc_num',
           'adm_dt',  
           'delivery_type',
           'delivery_type_full',
           'hosp_svc', 
           'diagnosis',
           'mrn',
           'baby_deliver_tm'
           ])
table = 'vwADT_Admissions'
join = None
where = 'baby_deliver_tm is not null'

def make_query(db):
    return db.order_select_query(
        limit_clause,
        columns,
        table,
        join,
        where)

