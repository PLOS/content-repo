import os
import sys

from shared import make_bucket_map, \
    QueueWorkerThread, make_generator_from_args, chunked
import boto3


class VerifyThread(QueueWorkerThread):
    """Thread worker for verifying transfer."""

    def __init__(self, *args, **kwargs):
        """Initialize this thread."""
        super().__init__(*args, **kwargs)
        self.s3_resource = boto3.resource('s3')
        self.bucket_map = make_bucket_map(os.environ["BUCKETS"])
        self.dynamo_db = boto3.resource('dynamodb')
        self.table_name = os.environ["DYNAMODB_TABLE"]

    def dowork(self, mogile_file_list):
        """Verify the transfer."""
        sys.stderr.write('*')
        sys.stderr.flush()
        query = {self.table_name: {'Keys': [{"fid": f.fid}
                                            for f in mogile_file_list]}}
        response = self.dynamo_db.batch_get_item(RequestItems=query)
        items = {item['fid']: item
                 for item in response["Responses"][self.table_name]}
        for mogile_file in mogile_file_list:
            db_entry = items[mogile_file.fid]
            mogile_file.verify(
                sha1=db_entry['sha1'],
                md5=db_entry['md5'],
                fid=int(db_entry['fid']),
                bucket=db_entry['bucket'],
                bucket_map=self.bucket_map)


def main():
    """Verify that all items were migrated correctly."""
    generator = chunked(make_generator_from_args(sys.argv[1:]), size=100)
    VerifyThread.process_generator(100, generator)


if __name__ == "__main__":
    main()
