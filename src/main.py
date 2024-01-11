import boto3
from loguru import logger
from botocore.exceptions import NoCredentialsError


class S3FileManager:
    def __init__(self, aws_access_key, aws_secret_key, endpoint_url):
        self.s3 = boto3.client('s3',
                               endpoint_url=endpoint_url,
                               aws_access_key_id=aws_access_key,
                               aws_secret_access_key=aws_secret_key)

    def create_bucket(self, bucket_name):
        try:
            self.s3.create_bucket(Bucket=bucket_name)
            logger.success(f"Bucket '{bucket_name}' created successfully.")
        except Exception as e:
            logger.error(f"Error creating bucket '{bucket_name}': {str(e)}")

    def create_folder(self, bucket_name, folder_name):
        try:
            self.s3.put_object(Bucket=bucket_name, Key=(folder_name + '/'))
            logger.success(f"Folder '{folder_name}' created successfully in bucket '{bucket_name}'.")
        except Exception as e:
            logger.error(f"Error creating folder '{folder_name}' in bucket '{bucket_name}': {str(e)}")

    def upload_file(self, file_path, bucket_name, object_key):
        try:
            self.s3.upload_file(file_path, bucket_name, object_key)
            object_url = f"https://{bucket_name}.s3.amazonaws.com/{object_key}"
            logger.success(f"File '{file_path}' uploaded successfully. URL: {object_url}")
            return object_url
        except NoCredentialsError:
            logger.error("Credentials not available.")
            return None
        except Exception as e:
            logger.error(f"Error uploading file '{file_path}' to bucket '{bucket_name}': {str(e)}")
            return None

    def download_file(self, bucket_name, object_key, local_path):
        try:
            self.s3.download_file(bucket_name, object_key, local_path)
            logger.success(f"File '{object_key}' downloaded successfully to '{local_path}'.")
        except NoCredentialsError:
            logger.error("Credentials not available.")
        except Exception as e:
            logger.error(f"Error downloading file '{object_key}' from bucket '{bucket_name}': {str(e)}")

    def delete_file(self, bucket_name, object_key):
        try:
            self.s3.delete_object(Bucket=bucket_name, Key=object_key)
            logger.success(f"File '{object_key}' deleted successfully from bucket '{bucket_name}'.")
        except NoCredentialsError:
            logger.error("Credentials not available.")
        except Exception as e:
            logger.error(f"Error deleting file '{object_key}' from bucket '{bucket_name}': {str(e)}")


if __name__ == "__main__":
    # Пример использования:
    endpoint_url = 'your_endpoint_url'
    aws_access_key = 'your_access_key'
    aws_secret_key = 'your_secret_key'

    bucket_name = 'your_bucket_name'
    folder_name = 'your_folder_name'

    file_path = 'path/to/your/file.txt'
    object_key = 'custom/object/key.txt'
    local_download_path = 'path/to/local/downloaded/file.txt'

    s3_manager = S3FileManager(aws_access_key, aws_secret_key, endpoint_url)
    s3_manager.create_bucket(bucket_name)
    # s3_manager.create_folder(bucket_name, folder_name)
    # uploaded_file_url = s3_manager.upload_file(file_path, bucket_name, object_key)
    # s3_manager.download_file(bucket_name, object_key, local_download_path)
    # s3_manager.delete_file(bucket_name, object_key)
