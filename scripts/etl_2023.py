import pandas as pd

def read_2023_sheet(file_path):
    # Read the 2023 sheet
    df_2023 = pd.read_excel(file_path, sheet_name='2023')
    return df_2023

def transform_2023_data(df_2023):
    # Apply the transformations
    df_2023['Guerra Produto + Classificação'] = ''  # Empty for now
    df_2023.drop(['Boletim', 'MÊS'], axis=1, inplace=True)  # Drop 'Boletim' and 'MÊS' columns

    # Rename columns to match the target format
    df_2023.rename(columns={'Data': 'Data', 'Guerra': 'Guerra', 'Grupo': 'Grupo', 
                            'Produto': 'Produto', 'Variedade': 'Variedade', 
                            'Classificação': 'Classificação', 'Menor': 'Menor', 
                            'Comum': 'Comum', 'Maior': 'Maior', 'Unidade': 'Unidade', 'Peso': 'Peso'}, inplace=True)

    # Reorder columns
    column_order = ['Data', 'Guerra Produto + Classificação', 'Guerra', 'Grupo', 'Produto', 'Variedade', 'Classificação', 'Menor', 'Comum', 'Maior', 'Unidade', 'Peso']
    df_2023 = df_2023[column_order]
    return df_2023

def merge_with_reference_table(df, ref_table_path):
    # Create a copy of df to avoid SettingWithCopyWarning
    df_copy = df.copy()

    # Read the reference table
    ref_df = pd.read_csv(ref_table_path)

    # Create a temporary 'CONCAT' column in df_copy
    df_copy['CONCAT'] = df_copy['Produto'] + "_" + df_copy['Classificação']

    # Perform a merge (lookup) with the reference table
    merged_df = df_copy.merge(ref_df[['Guerra Produto + Classificação', 'CONCAT']],
                              on='CONCAT', 
                              how='left')

    # Assign 'Guerra Produto + Classificação' from merged_df to df_copy
    df_copy['Guerra Produto + Classificação'] = merged_df['Guerra Produto + Classificação_y'].fillna('N/A')

    # Drop the temporary 'CONCAT' column
    df_copy.drop('CONCAT', axis=1, inplace=True)

    return df_copy

# File paths and ETL process
ceagesp_file_path = 'data/raw/CEAGESP_5Y_dados_DataAgro.xlsx'
ref_table_path = 'utils/data/ids_table.csv'
output_file_path = 'master_CEAGESP.csv'

df_2023 = read_2023_sheet(ceagesp_file_path)
df_2023_transformed = transform_2023_data(df_2023)
df_2023_final = merge_with_reference_table(df_2023_transformed, ref_table_path)
df_2023_final.to_csv(output_file_path, mode='a', index=False, header=False)
