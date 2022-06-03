from gcloud import storage

# Setting credentials using the downloaded JSON file
client = storage.Client.from_service_account_json(json_credentials_path='credentials-python-storage.json')
# Bucket access
bucket = client.get_bucket('buckaruckabuckaboo17284739492912389rss2820q8fhao028rhvhw8wb')
# File to be uploaded
test1 = bucket.blob('compressed.txt')
# Uploading file
test1.upload_from_filename('compressed.txt')
