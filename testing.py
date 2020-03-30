table_name = 'table1'

#create spark dataframe
df = spark.read.csv(r'C:\Users\luisj\Documents\Project Folders\Pandas_profiling\profiling_df.csv')

#create pandas dataframe
pd_describe = pd.DataFrame({'table_name':[table_name]})

#describe function into pandas
df_describe_output = df.describe().toPandas().T
df_describe_output.columns = ['count', 'mean', 'stddev', 'min', 'max']
df_describe_output = df_describe_output.iloc[1:,:].reset_index()

#add in pandas dataframe
pd_describe['columnx_count'] = df_describe_output.iloc[0 , 1]
pd_describe['columnx_max'] = df_describe_output.iloc[0, 5]
pd_describe['columnx_min'] = df_describe_output.iloc[0, 4]
pd_describe['columny_count'] = df_describe_output.iloc[1 , 1]
pd_describe['columny_max'] = df_describe_output.iloc[1, 5]
pd_describe['columny_min'] = df_describe_output.iloc[1, 4]

#pandas to hive context
df = hive_context.createDataframe(pd_describe)
