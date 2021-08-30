# import necessary libraries
import pandas as pd
import sidetable

# import user defined utilities
from Kleffy import kleffy_utility as ku

# set pandas dataframe global options
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)

# initialise variables
data_dir = 'data'
doctors_fn = 'doctors.csv'
hospitals_fn = 'hospital_visits.csv'
patients_fn = 'patients.csv'

# read/load csv files
doctors = ku.read_file(filename=doctors_fn, path=data_dir)
hospital_visits = ku.read_file(filename=hospitals_fn, path=data_dir)
patients = ku.read_file(filename=patients_fn, path=data_dir)

# merge hospital visits dataframe and paients dataframe into one - intermediate step
hvp =  ku.merge_dataframe(df1=hospital_visits, df2=patients, left_on='patient_id', right_on='id')

# merge hvp dataframe and doctors dataframe into one - final step
sdf =  ku.merge_dataframe(df1=hvp, df2=doctors, left_on='doctor_id', right_on='id', 
                                right_suffix='doctor', 
                                drop_rcolumns=['created_at'])


# Feature engineering: create year, month, and day_name columns from created_at
single_df = ku.create_year_month_day_column(dataframe=sdf, date_column='created_at')

# save final single dataframe
ku.save_as_csv(df=single_df, filename='single_records', path='Output')

## Number of hospital visits per day
hvd = pd.DataFrame(single_df.groupby('created_at').size(), 
                    columns=['Number of visits per day'])
hvd.index.name = 'date of visit'
print(hvd.head(10))

## Number of patients attended to per doctor per month showing their names
# method 1
df1 = single_df.groupby(['doctor_id','month', 'name']).agg(dict(patient_id='count')).stb.subtotal().rename(columns={'patient_id':'patient count'})
df1['patient count'] = df1['patient count'].astype(int)
df1.head(20)

## The ratio of female to male patient visits per month
gender_count=single_df.groupby(['month', 'sex'])['sex'].count()
gender_total=single_df.groupby(['month'])['sex'].count()
ratio=((gender_count/gender_total)*100).round(2)
ratio = pd.DataFrame(ratio).rename(columns={'sex':'ratio'})
print(ratio)


## Number of patients attended to per doctor per month showing their names
# # method 2
# gdf = single_df.groupby(['doctor_id','month', 'name'])

# container = []
# for label, df in gdf:
#     _df = pd.DataFrame(df['name'].value_counts())
#     df_con = _df.reset_index().merge(df, left_on='index', right_on='name')[['index', 'name_x']].fillna('')
#     df_con.loc[f'Doctor ID: {label[0]} , Month: {label[1]} Subtotal'] = df_con[['name_x']].sum()
#     df_con.fillna(' ', inplace=True)
#     container.append(df_con)

# df_summary = pd.concat(container)
# df_summary['name_x'] = df_summary['name_x'].astype(int)
# df_summary.rename(columns={'index': 'patient name', 'name_x':'patient visit count'}, inplace=True)
# df_summary.head(20)