import pandas as pd

# Read the master CEAGESP CSV file
df_master = pd.read_csv('C:/Users/lnonino/OneDrive - DATAGRO/Documentos/GitHub/CEAGESP_automação/master_CEAGESP.csv')

# Select the required columns and remove duplicates
df_ref = df_master[['Guerra Produto + Classificação', 'Grupo', 'Produto', 'Variedade', 'Classificação']].drop_duplicates()

# Create a new column 'CONCAT' by concatenating 'Produto' and 'Classificação' columns
df_ref['CONCAT'] = df_ref['Produto'] + "_" + df_ref['Classificação']

# Save the processed DataFrame to a new CSV file
output_path = 'C:/Users/lnonino/OneDrive - DATAGRO/Documentos/GitHub/CEAGESP_automação/data/ids_table.csv'
df_ref.to_csv(output_path, index=False)

print(f'Reference table saved to {output_path}')
