from azure.storage.blob import BlobServiceClient, ContentSettings
from azure.core.exceptions import ResourceExistsError
import time


def write_to_ADLS(connection_string, container_name, local_path, city) -> None:
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)

    try:
        container_client = blob_service_client.create_container(container_name)
    except ResourceExistsError:
        print('Specified container already exists. Using the existing one.')
        container_client = blob_service_client.get_container_client(container_name)

    blob_name = f'{city}-raw/apartments-{time.strftime("%d%m%Y-%H%M%S")}.csv'

    with open(local_path, 'rb') as file:
        content_settings = ContentSettings(content_type='text/csv')
        blob_client = container_client.get_blob_client(blob_name)
        blob_client.upload_blob(file, overwrite=True, content_settings=content_settings)
    return


if __name__ == '__main__':
    pass
