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
