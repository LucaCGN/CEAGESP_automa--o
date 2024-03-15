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