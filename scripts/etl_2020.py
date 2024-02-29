import pandas as pd

def read_2020_sheet(file_path):
    # Read the 2020 sheet
    df_2020 = pd.read_excel(file_path, sheet_name='2020')
    return df_2020

def transform_2020_data(df_2020):
    # Apply the transformations
    df_2020['Guerra'] = df_2020['pseudocod']
    df_2020['Produto'] = df_2020['descricao']
    df_2020['Guerra Produto + Classificação'] = ''  # Empty for now
    df_2020['Grupo'] = ''  # Empty for now
    df_2020.drop(['pseudocod', 'produto', 'descricao', 'mês'], axis=1, inplace=True)

    # Rename and reorder columns
    df_2020.rename(columns={'data': 'Data', 'variedade': 'Variedade', 'classifica': 'Classificação', 
                            'menor': 'Menor', 'comum': 'Comum', 'maior': 'Maior', 'unidade': 'Unidade', 'peso': 'Peso'}, inplace=True)
    column_order = ['Data', 'Guerra Produto + Classificação', 'Guerra', 'Grupo', 'Produto', 'Variedade', 'Classificação', 'Menor', 'Comum', 'Maior', 'Unidade', 'Peso']
    df_2020 = df_2020[column_order]
    return df_2020
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

    # Drop the temporary 'CONCAT' column before returning the DataFrame
    df_copy.drop('CONCAT', axis=1, inplace=True)

    return df_copy


# File paths and ETL process
ceagesp_file_path = 'data/raw/CEAGESP_5Y_dados_DataAgro.xlsx'
ref_table_path = 'utils/data/ids_table.csv'
output_file_path = 'master_CEAGESP.csv'

df_2020 = read_2020_sheet(ceagesp_file_path)
df_2020_transformed = transform_2020_data(df_2020)
df_2020_final = merge_with_reference_table(df_2020_transformed, ref_table_path)
df_2020_final.to_csv(output_file_path, mode='a', index=False, header=False)
