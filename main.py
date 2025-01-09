from gcloud import helper as gch
import time



bucket_name = 'bucket_andersonbraz'

gch.create_bucket(bucket_name)

print("First list of files ::::::::::::::::::::")
gch.list_files(bucket_name)

gch.create_folder(bucket_name, '2020')
gch.create_folder(bucket_name, '2021')
gch.create_folder(bucket_name, '2022')
gch.create_folder(bucket_name, '2023')
gch.create_folder(bucket_name, '2024')
gch.create_folder(bucket_name, '2025')

print("Last list of files ::::::::::::::::::::")
gch.list_files(bucket_name)

time.sleep(60)

gch.delete_bucket(bucket_name)