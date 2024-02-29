import pandas as pd
import os
import datetime

def log_error(message, log_directory):
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log_message = f"{timestamp} - {message}\n"
    with open(os.path.join(log_directory, 'error_log.txt'), 'a') as log_file:
        log_file.write(log_message)

def validate_headers(df, expected_headers):
    return all(header in df.columns for header in expected_headers)

def consolidate_cotacao_files(directory_path, output_file, log_directory):
    expected_headers = ['Data','Guerra Produto + Classificação', 'Guerra', 'Grupo', 'Produto', 'Variedade', 'Classificação', 'Menor', 'Comum', 'Maior', 'Unidade', 'Peso']
    df_list = []

    for file_name in os.listdir(directory_path):
        if file_name.startswith('Cotação') and file_name.endswith('.xlsx'):
            file_path = os.path.join(directory_path, file_name)
            try:
                df = pd.read_excel(file_path)
                if not validate_headers(df, expected_headers):
                    log_error(f"Header mismatch in file {file_name}", log_directory)
                    continue
                
                # Drop the first and last columns
                df = df.drop(df.columns[[0, -1]], axis=1)
                
                df_list.append(df)
            except Exception as e:
                log_error(f"Error processing file {file_name}: {e}", log_directory)

    if df_list:
        consolidated_df = pd.concat(df_list, ignore_index=True)
        
        if not os.path.exists(output_file) or os.stat(output_file).st_size ==  0:
            consolidated_df.to_csv(output_file, index=False, header=True)
        else:
            consolidated_df.to_csv(output_file, mode='a', index=False, header=False)

directory_path = 'data/raw'
log_directory = 'utils/logs'
output_file = 'master_CEAGESP.csv'

consolidate_cotacao_files(directory_path, output_file, log_directory)
