# To add a new cell, type '# %%'
# To add a new markdown cell, type '# %% [markdown]'

# %%



# %%
import os
import gzip
import shutil
import pandas as pd
from IPython.display import clear_output
import warnings

warnings.filterwarnings('ignore')
pd.options.display.max_columns = None


# %%


# %% [markdown]
# read data directory from config file

# %%
with open('config') as f:
    path = f.readline()

# %% [markdown]
# ### unzip a file from network path and copy to local path

# %%
if os.path.isfile('.\\data\\unpacked.csv'):
    unpacked = pd.read_csv('.\\data\\unpacked.csv')
else:
    unpacked = pd.DataFrame(columns=['filename', 'total', 'sentToMTA', 'clicked', 'rendered', 'bounced', 'skipped', 'feedback', 'unsubscribed'])
    unpacked = unpacked.set_index('filename')


# %%
chunk = pd.read_csv('.\\data\\chunk.csv')
chunk.describe()


# %%
chunk['record.Timestamp'].str[:4]


# %%
tmp_file = '.\\data\\tmp.csv'

total = len(os.listdir(path))
for i, filename in enumerate(os.listdir(path)):
    if 235 <= i <= 235:
        clear_output()
        print("processing file nr {}".format(i))
        print(path + filename)

        unpacked.loc[filename] = [0,0,0,0,0,0,0,0]

        try:
           # unzip to tmp_file
            with gzip.open(os.path.join(path,filename)) as f_in:
                with open(tmp_file, 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)
        except:
            print('Error unpacking file nr ' + str(i) + ' ', filename)
            continue

        df_clicked = pd.DataFrame()
        reader = pd.read_csv(tmp_file, sep=';', chunksize=100000)   
        for chunk in reader:
            unpacked.loc[filename, 'total'] += len(chunk)
            unpacked.loc[filename, 'clicked'] += chunk['click.Timestamp'].count()
            unpacked.loc[filename, 'rendered'] += chunk['render.Timestamp'].count()
            unpacked.loc[filename, 'bounced'] += chunk['bounce.Timestamp'].count()
            unpacked.loc[filename, 'skipped'] += chunk['bounce.Timestamp'].count()
            unpacked.loc[filename, 'sentToMTA'] += chunk['sentToMTA.Timestamp'].count()
            unpacked.loc[filename, 'feedback'] += chunk['feedback.Timestamp'].count()
            unpacked.loc[filename, 'unsubscribed'] += chunk['unsubscribe.Timestamp'].count()
            

            chunk['Year'] = chunk['record.Timestamp'].str[:4]
            for year in chunk['Year'].unique():
                df = chunk[chunk['Year']==year] 
                df = df.dropna(subset=['click.Timestamp'])  
                #df.to_csv('.\\data\\' + year + '\\' + filename + '_clicked.csv', mode='a', header=False) 
          
                df_clicked = pd.concat([df_clicked, df]) 
       
        unpacked.to_csv('.\\data\\unpacked.csv')
        df_clicked.to_csv('.\\data\\' + year + '\\' + filename + '_clicked.csv')


# %%
df_clicked = pd.DataFrame()
reader = pd.read_csv('.\\data\\tmp_236.csv', sep=';', chunksize=1)   
for chunk in reader:
    df = chunk.dropna(subset=['click.Timestamp'])
    df.to_csv('.\\data\\clicked.csv', mode='a', header=False)


# %%
reader = pd.read_csv("sample_data.csv", sep=';', chunksize=1)

df_clicked = pd.DataFrame()
for chunk in reader:
    df = chunk.dropna(subset=['click.Timestamp'])
    df.to_csv('.\\data\\clicked.csv', mode='a', header=False)

