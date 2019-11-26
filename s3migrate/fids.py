import os
import threading

import boto3


class FidThread(threading.Thread):
    """Thread worker for scanning fids from DynamoDB."""

    def __init__(self, fids, lock, segment, total_segments, *args, **kwargs):
        """Initialize this thread."""
        super(FidThread, self).__init__(*args, **kwargs)
        self.client = boto3.client("dynamodb")
        self.fids = fids
        self.lock = lock
        self.segment = segment
        self.total_segments = total_segments

    def run(self):
        """Run this thread."""
        kwargs = {"TableName": os.environ["DYNAMODB_TABLE"],
                  "AttributesToGet": ["fid"],
                  "Segment": self.segment,
                  "TotalSegments": self.total_segments}
        response = self.client.scan(**kwargs)
        while "LastEvaluatedKey" in response:
            values = [int(item["fid"]["N"])
                      for item in response["Items"]]
            with self.lock:
                self.fids.update(values)
            response = self.client.scan(
                ExclusiveStartKey=response["LastEvaluatedKey"], **kwargs)

    @classmethod
    def start_pool(cls, fids):
        """Run threads on the data provided in the queue.

        Returns a list of threads to pass to `finish_pool` later.
        """
        lock = threading.Lock()
        total_segments = 100
        threads = []
        for segment in range(total_segments):
            thread = cls(fids=fids, lock=lock, segment=segment,
                         total_segments=total_segments)
            threads.append(thread)
            thread.start()
        return threads

    @staticmethod
    def finish_pool(threads):
        """Wait until queue is empty and threads have finished processing."""
        # stop workers
        for thread in threads:
            thread.join()


def main():
    """Print all processed fids to stdout."""
    fids = set()
    threads = FidThread.start_pool(fids=fids)
    FidThread.finish_pool(threads)
    for fid in fids:
        print(fid)


if __name__ == "__main__":
    main()
