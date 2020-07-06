import tempfile
from unittest.mock import Mock

import pytest
from botocore.exceptions import ClientError

from .shared import MogileFile, make_bucket_map,\
    md5_fileobj_hex, sha1_fileobj_hex, md5_fileobj_b64, sha1_fileobj_b64, \
    QueueWorkerThread, chunked, future_waiter

from queue import Queue


# pylint: disable=C0115,C0116,R0201
class TestMigrate():
    MD5_HEX = "5eb63bbbe01eeed093cb22bb8f5acdc3"
    SHA1_HEX = "2aae6c35c94fcfb415dbe95f408b9ce91ee846ed"
    MD5_B64 = "XrY7u+Ae7tCTyyK7j1rNww=="
    SHA1_B64 = "Kq5sNclPz7QV2+lfQIuc6R7oRu0="

    @pytest.fixture
    def mogile_client(self):
        mogile_client = Mock()
        paths = Mock()
        mogile_client.get_paths.return_value = paths
        paths.data = {
            'paths': {
                1: 'http://example.org/1',
                2: 'http://example.org/2'
            }
        }
        return mogile_client

    @pytest.fixture
    def gcs_client(self):
        gcs_client = Mock()
        bucket = Mock()
        blob = Mock()
        gcs_client.get_bucket.return_value = bucket
        bucket.get_blob.return_value = blob
        return gcs_client
    
    @pytest.fixture
    def row(self):
        # hello world sha1sum: 2aae6c35c94fcfb415dbe95f408b9ce91ee846ed
        # (fid, dmid, dkey, length, classid, devcount)
        return [1, 1, f"{self.SHA1_HEX}-mogilefs-prod-repo", 1593790, 0, 2]

    @pytest.yield_fixture
    def my_tempfile(self):
        with tempfile.TemporaryFile() as tmp:
            tmp.write(b"hello world")
            yield tmp

    @pytest.fixture
    def mogile_file(self):
        return MogileFile(
            dkey=f"{self.SHA1_HEX}-repo",
            fid=564879786,
            length=1)

    def test_parse_row(self, row):
        file = MogileFile.parse_row(row)
        assert file.sha1sum == self.SHA1_HEX
        assert file.fid == 1
        assert file.length == 1593790
        assert (file.dkey ==
                f"{self.SHA1_HEX}-mogilefs-prod-repo")
        assert file.temp is False
        assert file.mogile_bucket == 'mogilefs-prod-repo'

    def test_parse_bad_dmid(self, row):
        row[1] = 2
        with pytest.raises(AssertionError, match='Bad domain'):
            MogileFile.parse_row(row)

    def test_parse_temp_file(self, row):
        row[2] = '8d26b4da-bd3e-47eb-888a-13bb3579c7e9.tmp'
        file = MogileFile.parse_row(row)
        assert file.temp is True
        assert file.mogile_bucket is None
        assert file.sha1sum is None

    def test_parse_bad_class(self, row):
        row[4] = 1
        with pytest.raises(AssertionError, match='Bad class'):
            MogileFile.parse_row(row)

    def test_make_intermediary_key(self, mogile_file):
        assert(mogile_file.make_intermediary_key() ==
               '0/564/879/0564879786.fid')

    def test_make_contentrepo_key(self, mogile_file):
        assert mogile_file.make_contentrepo_key() == self.SHA1_HEX

    def test_exists_in_bucket(self, mogile_file, gcs_client):
        blob = gcs_client.get_bucket().get_blob()
        blob.size = 1
        blob.md5_hash = self.MD5_HEX
        assert(mogile_file.exists_in_bucket(gcs_client, 'my-bucket', 'my-key')
               == self.MD5_HEX)

    def test_does_not_exist_in_bucket(self, gcs_client, mogile_file):
        gcs_client.get_bucket().get_blob.return_value = None
        assert(mogile_file.exists_in_bucket(
            gcs_client, 'my-bucket', 'my-file') is False)

    def test_mogile_file_to_json(self, mogile_file):
        assert mogile_file == MogileFile.from_json(mogile_file.to_json())

    def test_md5_fileobj(self, my_tempfile):
        assert md5_fileobj_hex(my_tempfile) == self.MD5_HEX
        assert md5_fileobj_b64(my_tempfile) == self.MD5_B64

    def test_sha1_fileobj(self, my_tempfile):
        assert sha1_fileobj_hex(my_tempfile) == self.SHA1_HEX
        assert sha1_fileobj_b64(my_tempfile) == self.SHA1_B64

    def test_put(self, mogile_file: MogileFile,
                 mogile_client, gcs_client,
                 requests_mock):
        requests_mock.get('http://example.org/1', content=b'hello world')
        requests_mock.get('http://example.org/2', content=b'hello world')
        blob = gcs_client.get_bucket().get_blob()
        blob.upload_from_file.return_value = 'xyz'
        blob.md5_hash = self.MD5_B64
        md5 = mogile_file.put(mogile_client, gcs_client, 'my-bucket')
        assert md5 == self.MD5_B64

    def test_make_bucket_map(self):
        assert make_bucket_map("a:b,c:d") == {"a": "b", "c": "d"}

    def test_chunked(self):
        assert list(chunked(range(0, 15), size=10)) == [
            list(range(0, 10)),
            list(range(10, 15))]

    def test_future_waiter(self):
        futures_list = []
        called_times = {}
        for i in range(0, 100):
            future = Mock()

            def mk_done(counter):
                called_times[counter] = 0
                return_after = counter % 3

                def done():
                    called_times[counter] += 1
                    return called_times[counter] > return_after
                return done

            future.done = mk_done(i)
            future.result.return_value = None
            futures_list.append(future)
        passthrough = future_waiter((f for f in futures_list), 10)
        leftovers = list(iter(passthrough))
        assert leftovers == [None]*100

    def test_queue_worker(self):
        class MyThread(QueueWorkerThread):
            def __init__(self, queue: Queue, result, *args, **kwargs):
                super().__init__(queue, *args, **kwargs)
                self.result = result

            def dowork(self, what):
                self.result['result'] += what

        q = Queue()
        for i in range(1000):
            q.put(i)
        # Wrapper for int since we cannot modify an int.
        result = {'result': 0}
        threads = MyThread.start_pool(2, q, result)
        MyThread.finish_pool(q, threads)
        assert result['result'] == sum(range(1000))

    def test_queue_worker_except(self):
        class MyThread(QueueWorkerThread):
            def dowork(self, item):
                # Fail in one thread, on one item only.
                assert item != 5

        q = Queue()
        for i in range(10):
            q.put(i)
        threads = MyThread.start_pool(2, q)
        with pytest.raises(AssertionError):
            MyThread.finish_pool(q, threads)

    def test_queue_process_generator(self):
        class MyThread(QueueWorkerThread):
            def __init__(self, queue: Queue, result, *args, **kwargs):
                super().__init__(queue, *args, **kwargs)
                self.result = result

            def dowork(self, item):
                self.result['result'] += item

        # Wrapper for int since we cannot modify an int.
        result = {'result': 0}
        MyThread.process_generator(2, range(1000), result)
        assert result['result'] == sum(range(1000))
