import pandas as pd
# import sidetable
from Kleffy import kleffy_utility as ku

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)

data_dir = 'data'
doctors_fn = 'doctors.csv'
hospitals_fn = 'hospital_visits.csv'
patients_fn = 'patients.csv'

doctors = ku.read_file(filename=doctors_fn, path=data_dir)
hospital_visits = ku.read_file(filename=hospitals_fn, path=data_dir)
patients = ku.read_file(filename=patients_fn, path=data_dir)

hvp =  ku.merge_dataframe(df1=hospital_visits, df2=patients, left_on='patient_id', right_on='id')

sdf =  ku.merge_dataframe(df1=hvp, df2=doctors, left_on='doctor_id', right_on='id', 
                                right_suffix='doctor', 
                                drop_rcolumns=['created_at'])


single_df = ku.create_year_month_day_column(dataframe=sdf, date_column='created_at')

# method 2
gdf = single_df.groupby(['doctor_id','month', 'name'])

container = []
for label, df in gdf:
    _df = pd.DataFrame(df['name'].value_counts())
    df_con = _df.reset_index().merge(df, left_on='index', right_on='name')[['index', 'name_x']].fillna('')
    df_con.loc[f'Doctor ID: {label[0]} , Month: {label[1]} Subtotal'] = df_con[['name_x']].sum()
    df_con.fillna(' ', inplace=True)
    container.append(df_con)



df_summary = pd.concat(container)
df_summary['name_x'] = df_summary['name_x'].astype(int)
df_summary.rename(columns={'index': 'patient name', 'name_x':'patient visit count'}, inplace=True)
df_summary.head(20)