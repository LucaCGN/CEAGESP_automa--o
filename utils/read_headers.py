import pandas as pd
import os

def consolidate_cotacao_files(directory_path, output_file):
    # List to store DataFrames from each file
    df_list = []
    
    # Iterate through each Cotação file in the directory
    for file_name in os.listdir(directory_path):
        if file_name.startswith('Cotação') and file_name.endswith('.xlsx'):
            file_path = os.path.join(directory_path, file_name)
            try:
                # Read the Excel file
                df = pd.read_excel(file_path)
                
                # Remove 'Boletim' and 'Tolerância' columns
                df.drop(['Boletim', 'Tolerância'], axis=1, inplace=True)
                
                # Append the DataFrame to the list
                df_list.append(df)
            except Exception as e:
                print(f"Error processing file {file_name}: {e}")
    
    # Concatenate all DataFrames into a single one
    consolidated_df = pd.concat(df_list, ignore_index=True)
    
    # Save the consolidated DataFrame to a CSV file
    consolidated_df.to_csv(output_file, index=False)

# Set the directory path where the Cotação files are located
directory_path = 'C:/Users/lnonino/OneDrive - DATAGRO/Documentos/GitHub/CEAGESP_automação/data/raw'

# Set the path for the output master file
output_file = 'C:/Users/lnonino/OneDrive - DATAGRO/Documentos/GitHub/CEAGESP_automação/master_CEAGESP.csv'

# Consolidate the Cotação files
consolidate_cotacao_files(directory_path, output_file)
