
# coding: utf-8

import pyspark
from pyspark.context import SparkContext
from pyspark.sql import SQLContext, HiveContext
from pyspark.storagelevel import StorageLevel
import os
import pyspark.sql.functions as F
import pyspark.sql.types  as T
from pyspark.sql.types import DatetimeConverter
from pyspark.sql import Row
from pyspark.sql import DataFrame
from pyspark.sql.window import Window 
import sys
from dateutil.parser import parse
import datetime
from ConfigParser import ConfigParser

#
## In[2]:
#
sc = SparkContext()
try:
    # Try to access HiveConf, it will raise exception if Hive is not added
    sc._jvm.org.apache.hadoop.hive.conf.HiveConf()
    sqlContext = HiveContext(sc)
    sqlContext.setConf("hive.exec.dynamic.partition", "true")
    sqlContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")
except py4j.protocol.Py4JError:
    sqlContext = SQLContext(sc)
except TypeError:
    sqlContext = SQLContext(sc)


# In[3]:

# # Function to read string and numeric variables

# In[23]:


def getDataList(config,section, attrib, data_type = 'string'):
    raw_data = config.get(section,attrib).split(',')
    if data_type=='int':
        return [int(x) for x in raw_data]
    if data_type=='string':
        return raw_data
    if data_type=='float':
        return [float(x) for x in raw_data]


# In[4]:

sqlContext.sql('set hive.exec.dynamic.partition.mode=nonstrict')
sqlContext.sql('set hive.exec.dynamic.partition=true')


# In[5]:

# # InI file set up
configFile = sys.argv[1]
#configFile = 'loy_casa_event_dc_domestic_spends.ini'
config = ConfigParser()
config.read(configFile)


# In[6]:

#Input DB Names

INP_DB_NM_1 = config.get('default', 'INP_DB_NM_1')
INP_DB_NM_2 = config.get('default', 'INP_DB_NM_2')
INP_DB_NM_3 = config.get('default', 'INP_DB_NM_3')
INP_DB_NM_4 = config.get('default', 'INP_DB_NM_4')


#Output DB Name
OUT_DB_NM_1 = config.get('default', 'OUT_DB_NM_1')


#Output Table Names
OUT_TBL_NM_1 = config.get('default', 'OUT_TBL_NM_1')


#Input Table Names

ods_fcr_ci_cust_acct_card_xref = config.get('default', 'INP_TBL_NM_1') #stg_fcr_fcrlive_1_cm_custcard_acct_xref  of stage
mv_ch_acct_mast = config.get('default', 'INP_TBL_NM_4') #db_gold_ch_acct_mast 
loy_event_cust_master = config.get('default','INP_TBL_NM_3') #loy_event_cust_master
vw_hdm_debit_card_data = config.get('default','INP_TBL_NM_2')#stg_dc_hdm_vw_hdm_debit_card_data

#Read latest data_date
data_date = config.get('default','MASTER_DATA_DATE_KEY').replace('"', "").replace("'","")

batchidcustxref = getDataList(config,'default','END_BATCH_ID_1','string')[0]

batchid_debit = getDataList(config,'default','END_BATCH_ID_2','string')[0]


SA_Prod_Codes=getDataList(config,'default','SA_Prod_Codes','int')
SA_YCOP_Prod_Codes=getDataList(config,'default','SA_YCOP_Prod_Codes','int')
CA_Prod_Codes = getDataList(config,'default','CA_Prod_Codes','int')


flg_cust_typ =getDataList(config,'default','FLG_CUST_TYP','string')

cod_merch_typ=getDataList(config,'default','cod_merch_typ','string')

Pcode=getDataList(config,'default','Pcode','string')


Event_Code=config.get('default','EVENT_CODE')
Event_Name=config.get('default','EVENT_NAME')

AMOUNT_THRESHOLD=float(config.get('default','AMOUNT_THRESHOLD'))


# In[8]:




create_event_table = """CREATE TABLE IF NOT EXISTS %s.%s (event_code string,event_name string, customer_id bigint, txn_acct_no string,txn_card_no string,txn_date timestamp,txn_amt string,txn_count int,txn_ref_no string,process_date timestamp,ref_open_date timestamp) PARTITIONED BY (data_dt string)
""" % (OUT_DB_NM_1, OUT_TBL_NM_1)


# In[29]:

sqlContext.sql(create_event_table)



# In[9]:

out_cols=['event_code','event_name','customer_id','txn_acct_no','txn_card_no','txn_date','txn_amt','txn_count','txn_ref_no','process_date',
         'ref_open_date','data_dt']






# In[10]:

#read account master
df_acct_mast = (sqlContext.table(INP_DB_NM_4+"."+mv_ch_acct_mast)
                .filter((F.col('data_dt')==data_date)
                        &(~F.col('cod_acct_stat').isin('13'))
                        &(F.col('flg_mnt_status')=='A')
                       &(F.col('cod_prod').cast('int').isin(SA_Prod_Codes+SA_YCOP_Prod_Codes+CA_Prod_Codes)))
   ).drop_duplicates()


#Read cust mast table
cust_mast=(sqlContext.table(INP_DB_NM_3+'.'+loy_event_cust_master).filter(
                                                             (F.col('flg_cust_typ').isin(flg_cust_typ)))
 )


# In[11]:

#Select columns from acct_mast table
acct_mast_cols = df_acct_mast.select(F.trim(F.col('cod_acct_no')).cast('string').alias('TXN_ACCT_NO')
                     ,F.col('cod_cust_id').cast('int').alias("CUSTOMER_ID")
                     ,F.col('cod_prod')
                   #  ,F.to_timestamp('dat_acct_open').alias('REF_OPEN_DATE') #Filter this line for customer level event/rule
                     )
#Comments shriya- comment the ref_open_date in acct_mast-will give issue while joining acct mast and cust mast
#Select columns from cust_mast table
cust_mast_cols=cust_mast.select(F.trim(F.col('cod_cust_id')).cast('int').alias("CUSTOMER_ID"),
                   F.to_timestamp(F.col('dat_cust_open')).alias('REF_OPEN_DATE') #Uncomment this line for customer level event/rule
                   )


# In[12]:

# In[31]:

#read acct card xref
df_cust_acct_card_xref = (sqlContext.table(INP_DB_NM_1+"."+ods_fcr_ci_cust_acct_card_xref)
              .filter((F.col('batch_id')==(batchidcustxref))
                       &(F.col('card_no_correct')=='Y')
                      )
                      .select(F.trim(F.col('cod_card_no')).cast("string").alias('TXN_CARD_NO')
                              ,F.trim(F.col("cod_card_no_hashed")).alias("CARD_NO_HASH")
                              ,F.trim(F.col('cod_acct_no')).cast('string').alias('TXN_ACCT_NO')
                      ,F.col('cod_cust_id').cast('int').alias('CUSTOMER_ID')
                      )
                      )
#Comments shriya-batch_id filer


# In[13]:

#New additon
df_cust_acct_card_xref_acc_join= acct_mast_cols.join(df_cust_acct_card_xref.drop(F.col('CUSTOMER_ID')),on=['TXN_ACCT_NO'],how ='inner')




debit_card_data=(sqlContext.table(INP_DB_NM_2+"."+vw_hdm_debit_card_data)
             .filter((F.col("batch_id")==batchid_debit)
                     &(F.to_date(F.col("local_date"))==data_date)
                     &(F.col("Msgtype").isin('210','110'))
                     & (~F.col("Merchant_type").isin(cod_merch_typ))
                     & (F.col("respcode").isin('0','00'))
                     & F.col('pcode').isin(Pcode)
                     &(F.col ("ACQ_CURRENCY_CODE")=='356')
                     &(F.col('acceptorname').like('%IN')
                     |F.col('acceptorname').like('%IND'))
                     )
            .select(F.substring(F.trim(F.col('acctnum')),-15,15).cast("string").alias("TXN_ACCT_NO")
                    ,F.sha2(F.trim(F.col("Pan")),256).alias("CARD_NO_HASH")
                    ,F.col("local_date").cast('timestamp').alias('TXN_DATE')
                    ,F.trim(F.col("refnum")).alias("TXN_REF_NO")
                    ,F.col("ch_amount").cast(T.DecimalType(18,2)).alias('TXN_AMT')
                   )
             )




# In[34]:
#New additon in join (TXN_ACCT_NO)
postxn_xref_join = debit_card_data.join(df_cust_acct_card_xref_acc_join,on=['CARD_NO_HASH','TXN_ACCT_NO'],
                                      how='left').persist(StorageLevel.MEMORY_AND_DISK)


#where join was successful
debit_card_txn_1 = (postxn_xref_join
                     .filter(F.col('CUSTOMER_ID').isNotNull())
                     .select(F.lit(Event_Code).alias('EVENT_CODE')
                            ,(F.lit(Event_Name)).alias('EVENT_NAME')
                             ,F.col('CUSTOMER_ID')
                             ,F.col('TXN_ACCT_NO')
                             ,F.col('TXN_CARD_NO')
                             ,F.col('TXN_DATE')
                             ,F.col('TXN_AMT')
                             ,F.lit(1).alias('TXN_COUNT')
                             ,F.col('TXN_REF_NO')
                             ,F.current_timestamp().alias('PROCESS_DATE')
                             ,F.lit(data_date).alias('data_dt'))
                    )



# In[19]:

cols_1=debit_card_txn_1.columns


# In[20]:

# In[36]:

#where join was not successful, pick cust id from acct mast
debit_card_txn_2 = (postxn_xref_join.filter(F.col('CUSTOMER_ID').isNull())).drop(F.col('CUSTOMER_ID'))


# In[21]:


# In[37]:

debit_card_txn_acct_mast = debit_card_txn_2.join(acct_mast_cols,'TXN_ACCT_NO','inner')


# In[22]:


# In[38]:
debit_card_txn_3 = (debit_card_txn_acct_mast
                         .select(F.lit(Event_Code).alias('EVENT_CODE')
                                 ,(F.lit(Event_Name)).alias('EVENT_NAME')
                                 ,F.col('CUSTOMER_ID')
                                 ,F.col('TXN_ACCT_NO')
                                 ,F.col('TXN_CARD_NO')
                                 ,F.col('TXN_DATE')
                                 ,F.col('TXN_AMT')
                                 ,F.lit(1).alias('TXN_COUNT')
                                 ,F.col('TXN_REF_NO')
                                 ,F.current_timestamp().alias('PROCESS_DATE')
                                 ,F.lit(data_date).alias('data_dt'))
                        )
#Comments Shriya- change the event code according to the event code mentioned in the sheet against the rules


# In[23]:

cols_3=debit_card_txn_3.columns


# In[24]:


# In[39]:

Debit_card_txn_union = debit_card_txn_1.select(cols_1).unionAll(debit_card_txn_3.select(cols_3))



# In[25]:

#Join above table with cust_mast
Debit_card_cust_acct_join=Debit_card_txn_union.join(cust_mast_cols,'CUSTOMER_ID','inner')



#Apply threshold filter
DBIT_CARD_FINAL = Debit_card_cust_acct_join.filter((F.col('TXN_AMT').cast('int')>=AMOUNT_THRESHOLD))



DBIT_CARD_FINAL.select(out_cols).repartition('data_dt').write.insertInto(OUT_DB_NM_1+'.'+OUT_TBL_NM_1, True)

