import google.auth
from google.cloud import storage

def test(file, _):
    storage_client = storage.Client()
    name_file = file['name']    
    bucket_name = file['bucket']
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.get_blob(name_file)
    # lines = blob.download_as_string().decode('utf-8')
    x = blob.download_as_string()
    x = x.decode('utf-8')
    n_lines = len(x.split('\n'))
    # print(f'File {name_file} uploaded to {bucket_name} with {n_lines} lines.')
    print('File Name: ',name_file)
    print('Number of lines in file: ',n_lines)
