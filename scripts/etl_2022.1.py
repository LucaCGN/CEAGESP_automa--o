import pandas as pd

def read_2022_1_sheet(file_path):
    # Read the 2022.1 sheet
    df_2022_1 = pd.read_excel(file_path, sheet_name='2022.1')
    return df_2022_1

def transform_2022_1_data(df_2022_1):
    # Apply the transformations
    df_2022_1['Guerra Produto + Classificação'] = ''  # Empty for now
    df_2022_1['Guerra'] = ''  # Empty for now, to be filled later
    df_2022_1['Grupo'] = ''  # Empty for now
    df_2022_1['Produto'] = df_2022_1['DESCRICAO']
    df_2022_1.drop(['PRODUTO', 'DESCRICAO', 'MÊS'], axis=1, inplace=True)

    # Rename and reorder columns
    df_2022_1.rename(columns={'DATA': 'Data', 'VARIEDADE': 'Variedade', 'CLASSIFICA': 'Classificação', 
                              'MENOR': 'Menor', 'COMUM': 'Comum', 'MAIOR': 'Maior', 'UNIDADE': 'Unidade', 'PESO': 'Peso'}, inplace=True)
    column_order = ['Data', 'Guerra Produto + Classificação', 'Guerra', 'Grupo', 'Produto', 'Variedade', 'Classificação', 'Menor', 'Comum', 'Maior', 'Unidade', 'Peso']
    df_2022_1 = df_2022_1[column_order]
    return df_2022_1

def merge_with_reference_table(df, ref_table_path):
    # Create a copy of df to avoid SettingWithCopyWarning
    df_copy = df.copy()

    # Read the reference table
    ref_df = pd.read_csv(ref_table_path)

    # Create a temporary 'CONCAT' column in df_copy
    df_copy['CONCAT'] = df_copy['Produto'] + "_" + df_copy['Classificação']

    # Perform a merge (lookup) with the reference table
    merged_df = df_copy.merge(ref_df[['Guerra Produto + Classificação', 'Grupo', 'CONCAT']],
                              on='CONCAT', 
                              how='left')

    # Extract 'Guerra' ID from 'Guerra Produto + Classificação' column and fill in 'Guerra Produto + Classificação' and 'Grupo'
    df_copy['Guerra Produto + Classificação'] = merged_df['Guerra Produto + Classificação_y'].fillna('N/A')
    df_copy['Grupo'] = merged_df['Grupo_y'].fillna('N/A')
    df_copy['Guerra'] = merged_df['Guerra Produto + Classificação_y'].str.split('@').str[0].fillna('N/A')

    # Drop the temporary 'CONCAT' column
    df_copy.drop('CONCAT', axis=1, inplace=True)

    return df_copy

# File paths and ETL process
ceagesp_file_path = 'data/raw/CEAGESP_5Y_dados_DataAgro.xlsx'
ref_table_path = 'utils/data/ids_table.csv'
output_file_path = 'master_CEAGESP.csv'

df_2022_1 = read_2022_1_sheet(ceagesp_file_path)
df_2022_1_transformed = transform_2022_1_data(df_2022_1)
df_2022_1_final = merge_with_reference_table(df_2022_1_transformed, ref_table_path)
df_2022_1_final.to_csv(output_file_path, mode='a', index=False, header=False)
