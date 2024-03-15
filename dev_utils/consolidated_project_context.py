#
# consolidated_project_context.py
#


#
# gen_context.py
#
import os
import re

project_dir = '.'
consolidated_file = 'consolidated_project_context.py'

with open(consolidated_file, 'w') as outfile:
  for root, dirs, files in os.walk(project_dir):
    for file in files:
      if file.endswith('.py'):
        filepath = os.path.join(root, file)
        relpath = os.path.relpath(filepath, project_dir)

        # Write header with relative path
        outfile.write(f'#\n# {relpath}\n#\n')

        # Read and write content, removing any trailing newline
        with open(filepath) as infile:
          content = infile.read().rstrip('\n')  # Remove trailing newline
          outfile.write(content)
          outfile.write('\n\n')  # Add blank lines for separation

#
# main.py
#
import subprocess
import sys

# Paths to the scripts
script_paths = [
    "scripts/email_sync.py",
    "scripts/check_completion.py",
    "scripts/add_entry_reference.py",
    "scripts/file_conversion.py",
    "scripts/check_cod_ipv.py",
    "scripts/clear_data.py"
]

# Executing each script sequentially
for script_path in script_paths:
    print(f"Executando {script_path}...")
    try:
        output = subprocess.check_output(["python", script_path], text=True)
        print(output)
        if script_path == "scripts/check_completion.py" and "All files processed successfully." in output:
            print("Pipeline concluído com sucesso.")
            break # Stop execution if all files are processed
    except subprocess.CalledProcessError as e:
        print(f"{script_path} failed with exit status {e.returncode}. Continuing with the next script.")

if script_path != "scripts/check_completion.py":
    print("Pipeline concluído com sucesso.")

#
# scripts\add_entry_reference.py
#
import pandas as pd
import os

def load_cotacao_files():
    # Dynamically construct the path to the 'data/raw' directory
    cotacao_folder = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'data', 'raw')
    cotacao_files = [f for f in os.listdir(cotacao_folder) if f.startswith('Cotacao')]
    dfs = []
    for file in cotacao_files:
        file_path = os.path.join(cotacao_folder, file)
        df = pd.read_excel(file_path, engine='openpyxl')
        # Ensure the column is renamed to '<cod>' before proceeding
        df.rename(columns={'Guerra Produto + Classificação': '<cod>'}, inplace=True)
        # Replace '@' with '_' in the '<cod>' column
        df['<cod>'] = df['<cod>'].str.replace('@', '_')
        # Data Preparation for 'UNIQUE_CONCAT'
        for column in ['Produto', 'Variedade', 'Classificação']:
            df[column] = df[column].fillna('N/A')
            df[column] = df[column].str.replace(' ', '_')
        df['UNIQUE_CONCAT'] = df['Produto'] + df['Variedade'] + df['Classificação']
        dfs.append(df)
    return pd.concat(dfs, ignore_index=True)

def update_id_reference(df):
    # Dynamically construct the path to the 'data/processed/id_reference.csv' file
    id_reference_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'data', 'processed', 'id_reference.csv')
    id_reference_df = pd.read_csv(id_reference_path, delimiter=';')
    
    # Ensure the '<cod>' column exists before filtering
    if '<cod>' not in df.columns:
        print("Error: '<cod>' column does not exist in the DataFrame.")
        return
    
    # Filter rows in cotacao DataFrame that have a matching cod in id_reference.csv
    filtered_df = df[df['<cod>'].isin(id_reference_df['cod'])]
    
    # Find new entries in cotacao DataFrame that are not in id_reference.csv
    new_entries = filtered_df[~filtered_df['UNIQUE_CONCAT'].isin(id_reference_df['UNIQUE_CONCAT'])]
    
    if not new_entries.empty:
        # Ensure we only add new entries with existing cod values
        new_entries = new_entries[new_entries['<cod>'].notna()]
        
        # Add new entries to id_reference.csv
        # Ensure the new entries DataFrame has the same columns as the existing one
        new_entries = new_entries[['<cod>', 'UNIQUE_CONCAT']].rename(columns={'<cod>': 'cod'})
        id_reference_df = pd.concat([id_reference_df, new_entries], ignore_index=True)
        id_reference_df.to_csv(id_reference_path, index=False, sep=';')
        print("New entries added to id_reference.csv")
    else:
        print("No new entries found.")

if __name__ == "__main__":
    cotacao_df = load_cotacao_files()
    update_id_reference(cotacao_df)

#
# scripts\check_cod_ipv.py
#
import pandas as pd
import os

def load_ipvs_files():
    # Adjust the ipvs_files path to be relative to the script's location
    ipvs_folder = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'data', 'processed', 'IPVS')
    ipvs_files = [f for f in os.listdir(ipvs_folder) if f.startswith('IPVS')]
    dfs = []
    for file in ipvs_files:
        df = pd.read_csv(os.path.join(ipvs_folder, file), delimiter=',')
        df.rename(columns={df.columns[0]: 'cod'}, inplace=True)
        dfs.append(df)
    return pd.concat(dfs, ignore_index=True)

def verify_cod_values(id_reference_df, ipvs_df):
    missing_cods = id_reference_df[~id_reference_df['cod'].isin(ipvs_df['cod'])]
    if not missing_cods.empty:
        print("Missing cod values in IPVS files:")
        print(missing_cods[['cod', 'UNIQUE_CONCAT']])
    else:
        print("All cod values from id_reference.csv are present in IPVS files.")

if __name__ == "__main__":
    # Adjust the id_reference_df path to be relative to the script's location
    id_reference_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'data', 'processed', 'id_reference.csv')
    id_reference_df = pd.read_csv(id_reference_path, delimiter=';')
    ipvs_df = load_ipvs_files()
    verify_cod_values(id_reference_df, ipvs_df)

#
# scripts\check_completion.py
#
import json
import sys

def main():
    try:
        with open('data/logs/processed_list.json', 'r') as f:
            processed_list = json.load(f)
    except FileNotFoundError:
        print("Error: processed_list.json not found.")
        sys.exit(1)

    unprocessed_files = [file for file in processed_list['files'] if file['status'] != 'PROCESSED']

    if unprocessed_files:
        print(f"Error: {len(unprocessed_files)} file(s) not processed successfully.")
        for file in unprocessed_files:
            print(f"- {file['name']}")
        sys.exit(1) # Return a non-zero exit status if there are unprocessed files
    else:
        print("All files processed successfully.")
        sys.exit(0) # Return a zero exit status if all files are processed

if __name__ == '__main__':
    main()

#
# scripts\clear_data.py
#
# scripts/clear_data.py
import os
import shutil

def clear_data_folder():
    folder_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'data', 'raw')
    for filename in os.listdir(folder_path):
        file_path = os.path.join(folder_path, filename)
        try:
            if os.path.isfile(file_path) or os.path.islink(file_path):
                os.unlink(file_path)
            elif os.path.isdir(file_path):
                shutil.rmtree(file_path)
        except Exception as e:
            print(f'Failed to delete {file_path}. Reason: {e}')

if __name__ == "__main__":
    clear_data_folder()

#
# scripts\email_sync.py
#
import imaplib
from pathlib import Path
import email
import os
import json
from email.header import decode_header



# Determine the path to the config.json file relative to the script's location
script_dir = os.path.dirname(os.path.abspath(__file__))
root_dir = os.path.dirname(script_dir) # Go up one directory level
config_path = os.path.join(root_dir, 'config.json')

# Load the configuration from the config.json file
with open(config_path) as f:
    config = json.load(f)

EMAIL = config["EMAIL"]
PASSWORD = config["PASSWORD"]
SERVER = config["SERVER"]



def save_attachment(msg, save_folder, processed_list_path):
    for part in msg.walk():
        if part.get_content_maintype() == 'multipart' or part.get('Content-Disposition') is None:
            continue

        filename = part.get_filename()
        if filename:
            decoded_string, encoding = decode_header(filename)[0]
            filename = decoded_string.decode(encoding) if encoding else decoded_string

            filename = filename.replace("Cotação", "Cotacao")

            if not filename.startswith("Cotacao"):
                continue

            # Check if the file is already processed or exists in the processed list
            if is_file_processed(processed_list_path, filename):
                print(f"File {filename} is already processed. Skipping...")
                continue

            print(f"Saving attachment: {filename}")

            os.makedirs(save_folder, exist_ok=True)

            filepath = os.path.join(save_folder, filename)
            with open(filepath, 'wb') as f:
                f.write(part.get_payload(decode=True))
            print(f"Attachment saved: {filename}")

            # Mark the file as 'UNPROCESSED' only if it's not already in the list
            update_processed_list(processed_list_path, filename)

        else:
            print("No filename found or file doesn't need processing.")

def process_emails(save_folder, processed_list_path):
    print(f"Connecting to server {SERVER}")
    mail = imaplib.IMAP4_SSL(SERVER)
    mail.login(EMAIL, PASSWORD)
    print("Logged in, selecting inbox.")
    mail.select('inbox')

    result, data = mail.uid('search', None, '(HEADER Subject "Ceagesp")')
    if result == 'OK':
        print("Found emails, processing...")
        for num in data[0].split():
            result, data = mail.uid('fetch', num, '(RFC822)')
            raw_email = data[0][1]
            email_message = email.message_from_bytes(raw_email)
            save_attachment(email_message, save_folder, processed_list_path)

        print("Email processing complete.")
    else:
        print("No emails found matching criteria.")
    mail.logout()

def update_processed_list(processed_list_path, filename):
    processed_list_path.parent.mkdir(parents=True, exist_ok=True)

    if not processed_list_path.exists():
        with processed_list_path.open('w') as file:
            json.dump({"files": []}, file, indent=4)

    try:
        with processed_list_path.open('r') as file:
            processed_list = json.load(file)

        # Check if the file is already in the list before appending
        if not any(entry['name'] == filename for entry in processed_list['files']):
            processed_list['files'].append({"name": filename, "status": "UNPROCESSED"})

            with processed_list_path.open('w') as file:
                json.dump(processed_list, file, indent=4)

            print(f"Updated processed_list with {filename}.")

    except Exception as e:
        print(f"Error updating processed list: {e}")
        raise

def is_file_processed(processed_list_path, filename):
    try:
        with open(processed_list_path, 'r') as file:
            processed_list = json.load(file)
            return any(entry['name'] == filename for entry in processed_list['files'])
    except Exception as e:
        print(f"Error checking if file is processed: {e}")
    return False

if __name__ == "__main__":
    save_folder = Path("data/raw")
    processed_list_path = Path("data/logs/processed_list.json")

    print("Script start.")
    process_emails(save_folder, processed_list_path)
    print("Script end.")

#
# scripts\file_conversion.py
#
# -*- coding: utf-8 -*-
from pathlib import Path
import pandas as pd
import os
import openpyxl
from openpyxl import load_workbook
import datetime
import json
import unicodedata
import re

# Function to convert files to IPVS format
import os
import pandas as pd  # Assuming you're using pandas for DataFrame operations

def convert_to_ipvs(file_paths):
    for file_path in file_paths:
        df = load_file(file_path)  # Assuming load_file is a predefined function
        
        # Data Preparation
        df.rename(columns={'Guerra Produto + Classificação': '<cod>'}, inplace=True)
        for column in ['Produto', 'Variedade', 'Classificação']:
            df[column] = df[column].fillna('N/A')
            df[column] = df[column].str.replace(' ', '_')
        
        # Reference Linking
        df['<concat>'] = df['Produto'] + df['Variedade'] + df['Classificação']
        
        # Adjust the path to the id_reference.csv file
        id_reference_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'data', 'processed', 'id_reference.csv')
        df = update_cod_using_reference(df, id_reference_path)  # Assuming update_cod_using_reference is a predefined function
        
        # Calculating '<min>', '<med>', '<max>'
        for index, row in df.iterrows():
            if row['Unidade'] == 'KG' or row['Unidade'] == 'MC':
                df.at[index, '<min>'] = row['Menor']
                df.at[index, '<med>'] = row['Comum']
                df.at[index, '<max>'] = row['Maior']
            elif row['Unidade'] == 'ENG':
                df.at[index, '<min>'] = row['Menor'] / row['Peso']
                df.at[index, '<med>'] = row['Comum'] / row['Peso']
                df.at[index, '<max>'] = row['Maior'] / row['Peso']
            elif row['Unidade'] == 'DZMC':
                df.at[index, '<min>'] = row['Menor'] / 12
                df.at[index, '<med>'] = row['Comum'] / 12
                df.at[index, '<max>'] = row['Maior'] / 12
        
        # Filter out rows with 'Unknown' in the '<cod>' column
        df = df[df['<cod>'] != 'Unknown']
        
        # File Saving
        df.rename(columns={'Data': '<data>'}, inplace=True)
        df = df[['<cod>', '<data>', '<min>', '<med>', '<max>']]
        save_as_ipvs(df, file_path)  # Assuming save_as_ipvs is a predefined function




def normalize_path(file_path):
    """
    Normalize the file path to ensure compatibility with file system encoding.

    Args:
        file_path (str): The original file path.

    Returns:
        str: The normalized file path.
    """
    # Directly return the absolute path of the file without normalization
    return os.path.abspath(file_path)


def load_file(file_path):
    """
    Load the specified Excel file into a pandas DataFrame using openpyxl.

    Args:
        file_path (str): Path to the Excel file to be loaded.

    Returns:
        DataFrame: The loaded file as a pandas DataFrame.
    """
    try:
        # Normalize the file path to handle encoding issues
        file_path = normalize_path(file_path)
        
        # Ensure the file path is correctly resolved
        file_path = os.path.abspath(file_path)
        
        # Check if the file exists
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"The file {file_path} does not exist.")
        
        # Load the workbook using openpyxl
        workbook = load_workbook(file_path)
        # Select the active sheet
        sheet = workbook.active
        
        # Convert the sheet to a DataFrame
        df = pd.DataFrame(sheet.values)
        
        # Set the column names using the first row of the DataFrame
        df.columns = df.iloc[0]
        df = df.iloc[1:]
        
        return df
    except Exception as e:
        print(f"Error loading {file_path}: {e}")
        raise


def update_cod_using_reference(df, reference_path):
    """
    Update the '<cod>' column in the DataFrame using the ID reference mapping.

    Args:
        df (DataFrame): The DataFrame to be updated.
        reference_path (str): Path to the reference CSV file containing the mapping.

    Returns:
        DataFrame: The updated DataFrame.
    """
    try:
        reference_df = pd.read_csv(reference_path, delimiter=';')
        # Creating a dictionary from the reference DataFrame for efficient lookup
        cod_mapping = dict(zip(reference_df['UNIQUE_CONCAT'], reference_df['cod']))
        # Updating the '<cod>' column based on the '<concat>' column using the mapping
        df['<cod>'] = df['<concat>'].apply(lambda x: cod_mapping.get(x, 'Unknown'))
    except Exception as e:
        print(f"Error updating cod using reference: {e}")
        raise
    return df



def save_as_ipvs(df, original_file_path):
    """
    Save the DataFrame as an IPVS file in the designated directory.

    Args:
        df (DataFrame): The DataFrame to be saved.
        original_file_path (str): The original file path to derive the save folder from.

    Returns:
        None
    """
    try:
        # Adjust the save_folder path to be relative to the script's location
        save_folder = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'data', 'processed', 'IPVS')
        if not os.path.exists(save_folder):
            os.makedirs(save_folder)
        
        # Extract the date from the original file name
        date_pattern = r"\d{2}\.\d{2}\.\d{4}"
        match = re.search(date_pattern, original_file_path)
        if match:
            date_str = match.group(0).replace('.', '') # Convert to YYYYMMDD format
        else:
            raise ValueError(f"Date not found in file name: {original_file_path}")
        
        filename = f"IPVS_{date_str}.csv"
        save_path = os.path.join(save_folder, filename)

        print(f"Saving file to: {save_path}") # Debugging line
        df.to_csv(save_path, index=False, header=['<cod>', '<data>', '<min>', '<med>', '<max>'])
        mark_file_as_processed(os.path.basename(original_file_path))
    except Exception as e:
        print(f"Error saving IPVS file: {e}")
        raise



def mark_file_as_processed(filename):
    """
    Update the processed_list.json to mark a file as processed.

    Args:
        filename (str): The name of the file to mark as processed.
    """
    # Navigate up one directory from the script location, then to 'data/logs'
    processed_list_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'data', 'logs', 'processed_list.json')
    try:
        # Read the current processed list
        with open(processed_list_path, 'r') as file:
            processed_list = json.load(file)

        # Update the status of the file
        for item in processed_list['files']:
            if item['name'] == filename:
                item['status'] = 'PROCESSED'
                break

        # Write the updated list back to the JSON file
        with open(processed_list_path, 'w') as file:
            json.dump(processed_list, file, indent=4)
    except Exception as e:
        print(f"Error updating processed list: {e}")
        raise

def get_unprocessed_file_paths():
    """
    Fetch the paths of unprocessed files based on the processed_list.json.

    Returns:
        list: A list of file paths that have not been processed yet.
    """
    # Navigate up one directory from the script location, then to 'data/logs'
    processed_list_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'data', 'logs', 'processed_list.json')
    try:
        with open(processed_list_path, 'r') as file:
            processed_list = json.load(file)
            unprocessed_files = [os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'data', 'raw', item['name'])
                                 for item in processed_list['files']
                                 if item['status'] == 'UNPROCESSED']
        print(f"Unprocessed files: {unprocessed_files}") # Debugging line
    except Exception as e:
        print(f"Error fetching unprocessed file paths: {e}")
        raise
    return unprocessed_files


# Main execution logic
if __name__ == "__main__":
    # Assuming a function that fetches unprocessed file paths
    unprocessed_files = get_unprocessed_file_paths()
    # Normalize paths of unprocessed files
    normalized_paths = [normalize_path(path) for path in unprocessed_files]
    convert_to_ipvs(normalized_paths)

