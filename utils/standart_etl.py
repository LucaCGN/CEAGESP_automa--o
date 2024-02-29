import pandas as pd
import re

# Define the path to your CSV file
csv_file_path = r'C:\Users\lnonino\OneDrive - DATAGRO\Documentos\GitHub\CEAGESP_automação\master_CEAGESP.csv'

# Read only the first row to get the headers
headers = pd.read_csv(csv_file_path, nrows=1).columns.tolist()

# Now read the data starting from row  380762, using the headers obtained
df = pd.read_csv(csv_file_path, skiprows=range(1,  380762), names=headers)

# Fill missing values with 'N/A' before concatenating using .loc
df.loc[:, 'Produto'] = df['Produto'].fillna('N/A')
df.loc[:, 'Variedade'] = df['Variedade'].fillna('N/A')
df.loc[:, 'Classificação'] = df['Classificação'].fillna('N/A')

# Convert 'Guerra' to string and remove trailing .0 using .loc
df.loc[:, 'Guerra'] = df['Guerra'].astype(str).str.replace(r'\.0$', '', regex=True)

# Keep only the specified columns
df = df[['Produto', 'Guerra', 'Variedade', 'Classificação', 'Guerra Produto + Classificação']]

# Create a new column 'Concat(Produto,Variedade,Classificação)'
df['Concat(Produto,Variedade,Classificação)'] = df['Produto'] + ' ' + df['Variedade'] + ' ' + df['Classificação']

# Replace spaces with underscores in the concatenated column
df['Concat(Produto,Variedade,Classificação)'] = df['Concat(Produto,Variedade,Classificação)'].str.replace(' ', '_')

# Drop the original columns 'Variedade' and 'Classificação' as they are now concatenated
df = df.drop(columns=['Variedade', 'Classificação'])

# Remove duplicates based on the new concatenated column
df = df.drop_duplicates(subset=['Produto', 'Guerra', 'Concat(Produto,Variedade,Classificação)', 'Guerra Produto + Classificação'])

# Save the filtered DataFrame to a new CSV file
df.to_csv(r'C:\Users\lnonino\OneDrive - DATAGRO\Documentos\GitHub\CEAGESP_automação\filtered_master_CEAGESP.csv', index=False)
