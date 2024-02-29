import pandas as pd

def read_2022_2_sheet(file_path):
    # Read the 2022.2 sheet
    df_2022_2 = pd.read_excel(file_path, sheet_name='2022.2')
    return df_2022_2

def transform_2022_2_data(df_2022_2):
    # Apply the transformations
    df_2022_2['Guerra Produto + Classificação'] = ''  # Empty for now
    df_2022_2.drop(['MÊS'], axis=1, inplace=True)  # Drop 'MÊS' column

    # Rename columns to match the target format
    df_2022_2.rename(columns={'Data': 'Data', 'Grupo': 'Grupo', 'Guerra': 'Guerra', 'Produto': 'Produto',
                              'Variedade': 'Variedade', 'Classificação': 'Classificação', 'Menor': 'Menor',
                              'Comum': 'Comum', 'Maior': 'Maior', 'Unidade': 'Unidade', 'Peso': 'Peso'}, inplace=True)

    # Reorder columns
    column_order = ['Data', 'Guerra Produto + Classificação', 'Guerra', 'Grupo', 'Produto', 'Variedade', 'Classificação', 'Menor', 'Comum', 'Maior', 'Unidade', 'Peso']
    df_2022_2 = df_2022_2[column_order]
    return df_2022_2

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

df_2022_2 = read_2022_2_sheet(ceagesp_file_path)
df_2022_2_transformed = transform_2022_2_data(df_2022_2)
df_2022_2_final = merge_with_reference_table(df_2022_2_transformed, ref_table_path)
df_2022_2_final.to_csv(output_file_path, mode='a', index=False, header=False)
