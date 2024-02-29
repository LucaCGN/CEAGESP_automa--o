import pandas as pd

def read_2021_sheet(file_path):
    # Read the 2021 sheet
    df_2021 = pd.read_excel(file_path, sheet_name='2021')
    return df_2021

def transform_2021_data(df_2021):
    # Apply the transformations
    df_2021['Guerra'] = df_2021['PSEUDOCOD']
    df_2021['Produto'] = df_2021['DESCRICAO']
    df_2021['Guerra Produto + Classificação'] = ''  # Empty for now
    df_2021['Grupo'] = ''  # Empty for now
    df_2021.drop(['PSEUDOCOD', 'PRODUTO', 'DESCRICAO', 'mês'], axis=1, inplace=True)

    # Rename and reorder columns
    df_2021.rename(columns={'DATA': 'Data', 'VARIEDADE': 'Variedade', 'CLASSIFICA': 'Classificação', 
                            'MENOR': 'Menor', 'COMUM': 'Comum', 'MAIOR': 'Maior', 'UNIDADE': 'Unidade', 'PESO': 'Peso'}, inplace=True)
    column_order = ['Data', 'Guerra Produto + Classificação', 'Guerra', 'Grupo', 'Produto', 'Variedade', 'Classificação', 'Menor', 'Comum', 'Maior', 'Unidade', 'Peso']
    df_2021 = df_2021[column_order]
    return df_2021

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

    # Assign 'Guerra Produto + Classificação' and 'Grupo' from merged_df to df_copy using the correct suffixed columns
    df_copy['Guerra Produto + Classificação'] = merged_df['Guerra Produto + Classificação_y'].fillna('N/A')
    df_copy['Grupo'] = merged_df['Grupo_y'].fillna('N/A')

    # Corrected: Drop the temporary 'CONCAT' column before returning the DataFrame
    df_copy.drop('CONCAT', axis=1, inplace=True)

    return df_copy

# File paths and ETL process
ceagesp_file_path = 'data/raw/CEAGESP_5Y_dados_DataAgro.xlsx'
ref_table_path = 'utils/data/ids_table.csv'
output_file_path = 'master_CEAGESP.csv'

df_2021 = read_2021_sheet(ceagesp_file_path)
df_2021_transformed = transform_2021_data(df_2021)
df_2021_final = merge_with_reference_table(df_2021_transformed, ref_table_path)
df_2021_final.to_csv(output_file_path, mode='a', index=False, header=False)
