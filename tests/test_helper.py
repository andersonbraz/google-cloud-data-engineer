import pytest
from unittest import mock
from google.cloud import exceptions
from gcloud.helper import create_bucket, delete_bucket, list_files_bucket, create_folder_bucket, create_dataset_bigquery, delete_dataset_bigquery

@mock.patch('helper.storage.Client')
def test_create_bucket(mock_storage_client):
    mock_bucket = mock.Mock()
    mock_storage_client.return_value.bucket.return_value = mock_bucket
    mock_storage_client.return_value.create_bucket.return_value = mock_bucket

    create_bucket("test-bucket")
    mock_storage_client.return_value.create_bucket.assert_called_once_with(mock_bucket, location="US")

@mock.patch('helper.storage.Client')
def test_delete_bucket(mock_storage_client):
    mock_bucket = mock.Mock()
    mock_storage_client.return_value.bucket.return_value = mock_bucket
    mock_bucket.list_blobs.return_value = []

    delete_bucket("test-bucket")
    mock_bucket.delete.assert_called_once()

@mock.patch('helper.storage.Client')
def test_list_files_bucket(mock_storage_client):
    mock_bucket = mock.Mock()
    mock_blob = mock.Mock()
    mock_blob.name = "test-file"
    mock_storage_client.return_value.bucket.return_value = mock_bucket
    mock_bucket.list_blobs.return_value = [mock_blob]

    list_files_bucket("test-bucket")
    mock_bucket.list_blobs.assert_called_once()

@mock.patch('helper.storage.Client')
def test_create_folder_bucket(mock_storage_client):
    mock_bucket = mock.Mock()
    mock_blob = mock.Mock()
    mock_storage_client.return_value.bucket.return_value = mock_bucket
    mock_bucket.blob.return_value = mock_blob

    create_folder_bucket("test-bucket", "test-folder")
    mock_blob.upload_from_string.assert_called_once_with("")

@mock.patch('helper.bigquery.Client')
def test_create_dataset_bigquery(mock_bigquery_client):
    mock_dataset = mock.Mock()
    mock_bigquery_client.return_value.create_dataset.return_value = mock_dataset

    create_dataset_bigquery("test-dataset")
    mock_bigquery_client.return_value.create_dataset.assert_called_once()

@mock.patch('helper.bigquery.Client')
def test_delete_dataset_bigquery(mock_bigquery_client):
    delete_dataset_bigquery("test-dataset")
    mock_bigquery_client.return_value.delete_dataset.assert_called_once_with("test-project.test-dataset", delete_contents=True, not_found_ok=True)