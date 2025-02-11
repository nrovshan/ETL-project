import pandas as pd
import psycopg2
from sqlalchemy import create_engine


db_config = {
    'host': 'localhost',
    'database': 'bank',
    'user': 'postgres',
    'password': 'narmin',
    'port': 5432  
}

# Extract, transform and load for customer_id and last_name columns

# extract

csv_file_path='customer;10000;id,tenure,gender,surname.csv'

header_name=['RowNumber', 'CustomerId', 'Surname', 'CreditScore', 'Geography', 'Gender', 'Age', 'Tenure', 'Balance', 'NumOfProducts', 'HasCrCard', 'IsActiveMember', 'EstimatedSalary', 'Exited']
df=pd.read_csv(csv_file_path,sep=',',header=None,names=header_name)
print(df.head())

print(df.columns)

col_delete= [0,3,4,6,8,9,10,11,12,13]
df.drop(df.columns[col_delete],axis=1,inplace=True)
print(df.head())

df.drop_duplicates(inplace=True)
print(df.describe())

df_subset=df.head(500)
print(df_subset.head())

df_subset.columns=df_subset.columns.str.strip()
print(df_subset.columns)

df_subset=df_subset.drop(index=0)
print(df_subset)

#load to postgresql

try:
    conn = psycopg2.connect(**db_config)
    cursor = conn.cursor()

    for index, row in df_subset.iterrows():
    
        last_name = row['Surname']
        last_name=last_name.replace("'","''")  
        insert_query = f"INSERT INTO bank.customers (last_name) VALUES ('{last_name}')"
       
        cursor.execute(insert_query)
    
    conn.commit()
    print(f"Successfully inserted data into PostgreSQL table.")

except psycopg2.Error as e:
    print(f"Error inserting data into PostgreSQL: {e}")

finally:
    if conn:
        conn.close()


# Extract, transform and load for first_name column

# extract

csv_file_path1='customers;first_name.csv'
df1=pd.read_csv(csv_file_path1)
print(df1.head())

# transform

print(df1.columns)

col_delete=[1,2,3,4,5]

df1.drop(df1.columns[col_delete],axis=1,inplace=True)
print(df1.head())

df1[['first_name','last_name']]=df1['Name'].str.split(n=1,expand=True)
print(df1.head())

col_delete=[0,2]
df1.drop(df1.columns[col_delete],axis=1,inplace=True)
print(df1.head())

print(df1.isna().sum())

df1.drop_duplicates(inplace=True)
print(df1.head())

df1= df1.head(500)
print(df1.head())

df1['first_name']=df1['first_name'].str.title()

# load

try:
    conn=psycopg2.connect(**db_config)
    cursor=conn.cursor()

    cursor.execute("SET search_path TO bank, public;")

    cursor.execute("""
        SELECT pg_get_serial_sequence('bank.customers', 'customer_id');
    """)
    sequence_name = cursor.fetchone()[0]
    print(f"Sequence name: {sequence_name}")

    if not sequence_name:
        raise Exception("No sequence found for bank.customer.customer_id")

    cursor.execute(sql.SQL("ALTER SEQUENCE bank.customers_customer_id_seq RESTART WITH 1").format(sql.Identifier(sequence_name)))
    conn.commit()

    cursor.execute("SELECT customer_id FROM bank.customers ORDER BY customer_id;")
    customer_ids = cursor.fetchall()

    for i, (customer_id,) in enumerate(customer_ids, start=1):
        cursor.execute("""
            UPDATE bank.customers
            SET customer_id = %s
            WHERE customer_id = %s;
        """, (i, customer_id))
    
    conn.commit()

    for i, first_name in enumerate(df1['first_name'], start=1):
        cursor.execute("""
        UPDATE bank.customers
        SET first_name = %s
        WHERE customer_id = %s;
    """, (first_name, i))
    
    conn.commit()
    print(f"Successfully inserted data into PostgreSQL table.")

except psycopg2.Error as e:
    print(f"Error inserting data into PostgreSQL: {e}")

finally:
    if conn:
        conn.close()


# Extract, transform and load for dateofbirth column

# extract

csv_file_path='customers;birth_date.csv'

df2=pd.read_csv(csv_file_path)
print(df2.head())

# transform
print(df2.dtypes)
print(df2.info())
print(df2.describe())

col_delete=[0,2,3]
df2.drop(df2.columns[col_delete],axis=1,inplace=True)
print(df2.head(500))

df2['DOB'] = pd.to_datetime(df2['DOB'],utc=True)
df2['DOB'] = df2['DOB'].dt.date
print(df2.dtypes)

print(df2['DOB'].apply(type).unique())



# Load data into PostgreSQL

conn= psycopg2.connect(**db_config)
cursor=conn.cursor()


# Iterate over the DataFrame and update the database
for i, birth_date in enumerate(df2['DOB']):
    query = f"""
    UPDATE bank.customers
    SET dateofbirth = '{birth_date}'
    WHERE customer_id = {i + 1};
    """
    cursor.execute(query)

# Commit the changes
conn.commit()



# Extract, transform and load for tenure, gender, last_name columns

# extract

csv_file_path='customer;10000;id,tenure,gender,surname.csv'
df3=pd.read_csv(csv_file_path)
print(df3.head())


# transform 

print(df3.columns)

col_delete= [0,1,2,3,4,6,8,9,10,11,12,13]
df3.drop(df3.columns[col_delete],axis=1,inplace=True)
print(df3.head())

print(df3['Gender'].unique)

print(df3.head(500))


valid_genders = ['Male', 'Female']
df3 = df3[df3['Gender'].isin(valid_genders)]

print(df3.dtypes)

# load data into postgresql

db_config = {
   'host': 'localhost',
    'database': 'bank',
    'user': 'postgres',
    'password': 'narmin',
    'port': 5432  
}

conn=psycopg2.connect(**db_config)
cursor=conn.cursor()

for i in range(len(df3)):
    tenure = df3.iloc[i]['Tenure'].item()
    gender = df3.iloc[i]['Gender']
    
    query = f"""
    UPDATE bank.customers
    SET tenure = %s, gender = %s
    WHERE customer_id = %s;
    """
    cursor.execute(query, (tenure, gender, i + 1))

# Commit the changes
conn.commit()


# Extract, transform and load for job, marital, education columns

# extract

csv_file_path='customer;4522;job,marital,education.csv'
df4=pd.read_csv(csv_file_path,sep=';')
print(df4.head())


# transform
df4.drop_duplicates(inplace=True)

print(df4['marital'].unique())
print(df4['marital'].isna().sum())

print(df4['education'].unique())
print(df4['education'].isna().sum())

print(df4['job'].unique())
print(df4['job'].isna().sum())

print(df4.columns)

col_delete=[0,4,5,6,7,8,9,10,11,12,13,14,15,16]
df4.drop(df4.columns[col_delete],axis=1,inplace=True)
print(df4.head(500))


# To change the first letter of each value in an entire column to uppercase

def capitalize_first_letter(s):
    return s[0].upper() + s[1:] if isinstance(s,str) else s

df4['job']=df4['job'].apply(capitalize_first_letter)
df4['marital']=df4['marital'].apply(capitalize_first_letter)
df4['education']=df4['education'].apply(capitalize_first_letter)
print(df4.head())



# load data into postgresql

valid_maritals = ['Married', 'Single', 'Divorced']
df = df4[df4['marital'].isin(valid_maritals)]

db_config = {
   'host': 'localhost',
    'database': 'bank',
    'user': 'postgres',
    'password': 'narmin',
    'port': 5432  
}

conn=psycopg2.connect(**db_config)
cursor=conn.cursor()

for i in range(len(df4)):
    job = df.iloc[i]['job']
    marital = df.iloc[i]['marital']
    education = df.iloc[i]['education']

    query = """
    UPDATE bank.customers
    SET job = %s, marital = %s, education = %s
    WHERE customer_id = %s;
    """
    cursor.execute(query, (job, marital, education, i + 1))

# Commit the changes
conn.commit()

