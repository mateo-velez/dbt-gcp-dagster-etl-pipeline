import fnmatch
from typing import List, Union
from dagster import ConfigurableResource
from google.cloud import storage
import os
import tempfile
from ..config.constants import BRONZE_BUCKET_NAME

class ObjectStorageResource(ConfigurableResource):
    """Provides functions for interacting with object storage buckets."""

    bucket_name: str

    def _download_many_blobs_with_transfer_manager(
        self, bucket_name, blob_names, destination_directory="", workers=4
    ):
        """Download blobs in a list by name, concurrently in a process pool.

        The filename of each blob once downloaded is derived from the blob name and
        the `destination_directory `parameter. For complete control of the filename
        of each blob, use transfer_manager.download_many() instead.

        Directories will be created automatically as needed to accommodate blob
        names that include slashes.
        """

        # The ID of your GCS bucket
        # bucket_name = "your-bucket-name"

        # The list of blob names to download. The names of each blobs will also
        # be the name of each destination file (use transfer_manager.download_many()
        # instead to control each destination file name). If there is a "/" in the
        # blob name, then corresponding directories will be created on download.
        # blob_names = ["myblob", "myblob2"]

        # The directory on your computer to which to download all of the files. This
        # string is prepended (with os.path.join()) to the name of each blob to form
        # the full path. Relative paths and absolute paths are both accepted. An
        # empty string means "the current working directory". Note that this
        # parameter allows accepts directory traversal ("../" etc.) and is not
        # intended for unsanitized end user input.
        # destination_directory = ""

        # The maximum number of processes to use for the operation. The performance
        # impact of this value depends on the use case, but smaller files usually
        # benefit from a higher number of processes. Each additional process occupies
        # some CPU and memory resources until finished. Threads can be used instead
        # of processes by passing `worker_type=transfer_manager.THREAD`.
        # workers=8

        from google.cloud.storage import Client, transfer_manager

        storage_client = Client()
        bucket = storage_client.bucket(bucket_name)

        results = transfer_manager.download_many_to_path(
            bucket,
            blob_names,
            destination_directory=destination_directory,
            max_workers=workers,
        )

        for name, result in zip(blob_names, results):
            # The results list is either `None` or an exception for each blob in
            # the input list, in order.

            if isinstance(result, Exception):
                print("Failed to download {} due to exception: {}".format(name, result))
            else:
                print("Downloaded {} to {}.".format(name, destination_directory + name))

    def get(self, uris: Union[str,List[str]], destination_directory: str) -> None:
            """
            Downloads the blobs specified by the URIs to the specified destination directory.

            Args:
                uris (Union[str, List[str]]): The URIs of the blobs to download. It can be a single URI as a string
                    or a list of URIs as strings.
                destination_directory (str): The directory where the blobs will be downloaded.

            Raises:
                ValueError: If the uris parameter is not a string or a list of strings.

            Examples:
                >>> storage.get("data/*/*.parquet","./local/")

                >>> storage.get(["table1.csv","table2.csv"],"./local/")
            """

            if isinstance(uris, str):
                blob_names = self.list(uris)
            elif isinstance(uris, list) and all(isinstance(item, str) for item in uris):
                blob_names = uris
            else:
                raise ValueError("The uris parameter must be a string or a list of strings.")
            
            self._download_many_blobs_with_transfer_manager(
                self.bucket_name, blob_names, destination_directory=destination_directory
            )

            ...

    def _filter_paths(self, paths, pattern):
        return [path for path in paths if fnmatch.fnmatch(path, pattern)]

    def _longest_prefix(self, pattern, stop_char="*"):
        prefix = ""
        for char in pattern:
            if char == stop_char:
                break
            prefix += char
        return prefix

    def _list_blobs(self, bucket_name, prefix):
        # Initialize a GCP Storage client
        client = storage.Client()
        # Get the bucket
        bucket = client.get_bucket(bucket_name)
        # List blobs with the given prefix
        blobs = bucket.list_blobs(prefix=prefix)
        # Extract blob names
        blob_names = [blob.name for blob in blobs]

        return blob_names

    def list(self, pattern: str) -> List[str]:
        """Lists objects in the specified bucket, optionally filtered by a prefix.

        Args:
            pattern (str): An optional prefix to filter the listed objects.

        Returns:
            List[str]: A list of object keys in the bucket.

        Examples:
            List all objects in the bucket:
            ```python
            objects = resource.list("")
            ```

            List objects in a specific folder:
            ```python
            objects = resource.list("my-folder/")
            ```
        """
        prefix = self._longest_prefix(pattern)
        blob_names = self._list_blobs(self.bucket_name, prefix)
        filtered_blob_names = self._filter_paths(blob_names, pattern)
        return filtered_blob_names

        ...

    def delete(self, pattern: Union[str,List[str]]) -> None:
        """Deletes objects from the specified bucket with matching keys or patterns.

        Args:
            pattern (str): A pattern or key matching the objects to delete.
                       Supports standard object storage wildcards.

        Examples:
            Delete all objects with the `.log` extension:
            ```python
            resource.delete("*.log")
            ```

            Delete all objects in a specific folder:
            ```python
            resource.delete("my-folder/*")
            ```
        """
        if not isinstance(pattern,list):
            blob_names = self.list(pattern)
        else:
            blob_names = pattern

        storage_client = storage.Client()
        bucket = storage_client.bucket(self.bucket_name)
        for blob_name in blob_names:
            blob = bucket.blob(blob_name)
            blob.delete()

    def _upload_directory_with_transfer_manager(self,bucket_name, source_directory, workers=4):
        """Upload every file in a directory, including all files in subdirectories.

        Each blob name is derived from the filename, not including the `directory`
        parameter itself. For complete control of the blob name for each file (and
        other aspects of individual blob metadata), use
        transfer_manager.upload_many() instead.
        """

        # The ID of your GCS bucket
        # bucket_name = "your-bucket-name"

        # The directory on your computer to upload. Files in the directory and its
        # subdirectories will be uploaded. An empty string means "the current
        # working directory".
        # source_directory=""

        # The maximum number of processes to use for the operation. The performance
        # impact of this value depends on the use case, but smaller files usually
        # benefit from a higher number of processes. Each additional process occupies
        # some CPU and memory resources until finished. Threads can be used instead
        # of processes by passing `worker_type=transfer_manager.THREAD`.
        # workers=8

        from pathlib import Path

        from google.cloud.storage import Client, transfer_manager

        storage_client = Client()
        bucket = storage_client.bucket(bucket_name)

        # Generate a list of paths (in string form) relative to the `directory`.
        # This can be done in a single list comprehension, but is expanded into
        # multiple lines here for clarity.

        # First, recursively get all files in `directory` as Path objects.
        directory_as_path_obj = Path(source_directory)
        paths = directory_as_path_obj.rglob("*")

        # Filter so the list only includes files, not directories themselves.
        file_paths = [path for path in paths if path.is_file()]

        # These paths are relative to the current working directory. Next, make them
        # relative to `directory`
        relative_paths = [path.relative_to(source_directory) for path in file_paths]

        # Finally, convert them all to strings.
        string_paths = [str(path) for path in relative_paths]

        print("Found {} files.".format(len(string_paths)))

        # Start the upload.
        results = transfer_manager.upload_many_from_filenames(
            bucket, string_paths, source_directory=source_directory, max_workers=workers
        )

        for name, result in zip(string_paths, results):
            # The results list is either `None` or an exception for each filename in
            # the input list, in order.

            if isinstance(result, Exception):
                print("Failed to upload {} due to exception: {}".format(name, result))
            else:
                print("Uploaded {} to {}.".format(name, bucket.name))




    def put(self, source_directory: str) -> None:
            """
            Uploads the contents of a directory to the specified bucket in the object storage.

            Args:
                source_directory (str): The path to the directory containing the files to be uploaded.

            Returns:
                None
            
            Examples:
                >>> storage.put("local/")
            """

            self._upload_directory_with_transfer_manager(self.bucket_name, source_directory)

    def get_bucket_name(self) -> str:
        """Returns the name of the current bucket."""
        return self.bucket_name


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.DEBUG)

    # Create a temporary directory to store dummy files
    with tempfile.TemporaryDirectory() as temp_dir:
        # Create dummy files
        file1 = os.path.join(temp_dir, "file1.txt")
        file2 = os.path.join(temp_dir, "file2.txt")

        logging.debug(f"Creating files: {file1}, {file2}")

        with open(file1, "w") as f:
            f.write("This is file 1")
        with open(file2, "w") as f:
            f.write("This is file 2")

        # Initialize the ObjectStorageResource
        resource = ObjectStorageResource(bucket_name=BRONZE_BUCKET_NAME)

        # Upload the files to the bucket
        logging.debug(f"Uploading files to bucket: {BRONZE_BUCKET_NAME}")
        resource.put(temp_dir)

        # List the objects in the bucket
        objects = resource.list("*.txt")


        logging.debug(f"Objects in the bucket: {objects}")

        # Download the files from the bucket
        destination_dir = os.path.join(temp_dir, "downloaded")
        os.mkdir(destination_dir)

        logging.debug(f"Downloading files to: {destination_dir}")
        resource.get(objects, destination_dir)

        # Verify the downloaded files
        downloaded_files = os.listdir(destination_dir)
        logging.debug(f"Downloaded files: {downloaded_files}")

        # Delete the objects from the bucket
        resource.delete(objects)

        # List the objects in the bucket after deletion
        objects = resource.list("*.txt")
        logging.debug(f"Objects in the bucket after deletion: {objects}")