from gcloud import helper as gh
import time



bucket_name = 'bucket_andersonbraz'

gh.create_bucket(bucket_name)

print("First list of files ::::::::::::::::::::")
gh.list_files(bucket_name)

gh.create_folder(bucket_name, '2020')
gh.create_folder(bucket_name, '2021')
gh.create_folder(bucket_name, '2022')
gh.create_folder(bucket_name, '2023')
gh.create_folder(bucket_name, '2024')
gh.create_folder(bucket_name, '2025')

print("Last list of files ::::::::::::::::::::")
gh.list_files(bucket_name)

time.sleep(60)
gh.delete_bucket(bucket_name)