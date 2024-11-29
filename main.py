import os
from dotenv import load_dotenv
import streamlit as st
import boto3
import pandas as pd

# Load environment variables from the .env file
load_dotenv()

# Get AWS credentials from environment variables
aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')
aws_region = os.getenv('AWS_REGION')

# Set up S3 client
s3_client = boto3.client(
    's3',
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key,
    region_name=aws_region
)

# Fixed bucket name
BUCKET_NAME = "prod-tc-pdf-files-bucket"


# Function to list folders (prefixes) in the S3 bucket up to a fixed depth
def list_folders_in_s3(bucket_name, prefix="PDFS", max_depth=2, current_depth=0):
    if current_depth >= max_depth:
        return []

    result = s3_client.list_objects_v2(
        Bucket=bucket_name,
        Prefix=prefix,
        Delimiter='/'
    )
    folders = []
    if 'CommonPrefixes' in result:
        for prefix in result['CommonPrefixes']:
            folders.append(prefix['Prefix'])
            # Recursively list subfolders (limit to max_depth)
            folders.extend(list_folders_in_s3(bucket_name, prefix=prefix['Prefix'], max_depth=max_depth,
                                              current_depth=current_depth + 1))
    return folders


# Function to list files in a folder
def list_files_in_folder(bucket_name, folder_name, max_keys=50):

    # result = s3_client.list_objects_v2(
    #     Bucket=bucket_name,
    #     Prefix=folder_name,
    #     MaxKeys=max_keys
    # )
    files = []
    continuation_token = None
    while True:
        list_params = {
            'Bucket': bucket_name,
            'Prefix': folder_name,
            'MaxKeys': max_keys,
        }
        if continuation_token:
            list_params['ContinuationToken'] = continuation_token

        # Make the API call to S3
        result = s3_client.list_objects_v2(**list_params)

        if 'Contents' in result:
            for obj in result['Contents']:
                file_size_mb = obj['Size'] / (1024 * 1024)  # Convert size to MB
                files.append({
                    'File Name': obj['Key'],
                    'Size (MB)': round(file_size_mb, 2),  # Round to 2 decimal places
                    'Last Modified': obj['LastModified']
                })

        # Check if there are more objects to fetch (pagination)
        if result.get('IsTruncated'):  # If true, more objects are available
            continuation_token = result.get('NextContinuationToken')
        else:
            break
    return files

# Function to download a file from S3
def download_file_from_s3(bucket_name, file_key):
    file_obj = s3_client.get_object(Bucket=bucket_name, Key=file_key)
    file_content = file_obj['Body'].read()
    return file_content

# Streamlit UI
st.title('S3 File Search App')

# List folders in the fixed bucket
folders = list_folders_in_s3(BUCKET_NAME)
folders = sorted(folders)  # Sort folders for easier navigation

if folders:
    # Display folder structure with selection
    selected_folder = st.selectbox('Select a folder:', folders)

    if selected_folder:
        query = st.text_input(f'Search for a file in "{selected_folder}" folder:')
        if query:
            with st.spinner('Searching...'):
                selected_folder = selected_folder + query.lower()
                # print(selected_folder)
                files = list_files_in_folder(BUCKET_NAME, selected_folder)
                # Filter files based on query
                filtered_files = [file for file in files if query.lower() in file['File Name'].lower()]

                if filtered_files:
                    # Display the filtered files in a table
                    df = pd.DataFrame(filtered_files)
                    st.write(df)
                    # Provide a download button for each file in the list
                    for file in filtered_files:
                        file_name = file['File Name']
                        st.download_button(
                            label=f"Download {file_name}",
                            data=download_file_from_s3(BUCKET_NAME, file_name),
                            file_name=file_name,
                            mime="application/octet-stream"  # Use the appropriate MIME type for the file
                        )
                else:
                    st.warning(f'No files found matching "{query}".')
else:
    st.warning('No folders found in the bucket.')
