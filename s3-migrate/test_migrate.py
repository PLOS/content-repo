import tempfile
from unittest.mock import Mock

import pytest
from botocore.exceptions import ClientError

from migrate import MogileFile, make_bucket_hash,\
    md5_fileobj_hex, sha1_fileobj_hex, md5_fileobj_b64, sha1_fileobj_b64


# pylint: disable=C0115,C0116,R0201
class TestMigrate():
    @pytest.fixture
    def s3_client(self):
        s3_client = Mock()
        obj = Mock()
        s3_client.Object.return_value = obj
        obj.load.return_value = {}
        return s3_client

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
    def row(self):
        # hello world sha1sum: 2aae6c35c94fcfb415dbe95f408b9ce91ee846ed
        return {
            'fid': 1,
            'dmid': 1,
            'dkey':
            '2aae6c35c94fcfb415dbe95f408b9ce91ee846ed-mogilefs-prod-repo',
            'length': 1593790,
            'classid': 0,
            'devcount': 2}

    @pytest.yield_fixture
    def my_tempfile(self):
        with tempfile.TemporaryFile() as tmp:
            tmp.write(b"hello world")
            yield tmp

    @pytest.fixture
    def mogile_file(self):
        return MogileFile(
            dkey='2aae6c35c94fcfb415dbe95f408b9ce91ee846ed-repo',
            fid=564879786,
            length=1)

    def test_parse_row(self, row):
        file = MogileFile.parse_row(row)
        assert file.sha1sum == '2aae6c35c94fcfb415dbe95f408b9ce91ee846ed'
        assert file.fid == 1
        assert file.length == 1593790
        assert (file.dkey ==
                '2aae6c35c94fcfb415dbe95f408b9ce91ee846ed-mogilefs-prod-repo')

    def test_parse_bad_dmid(self, row):
        row['dmid'] = 2
        with pytest.raises(AssertionError, match='Bad domain'):
            MogileFile.parse_row(row)

    def test_parse_bad_class(self, row):
        row['classid'] = 1
        with pytest.raises(AssertionError, match='Bad class'):
            MogileFile.parse_row(row)

    def test_make_intermediary_key(self, mogile_file):
        assert(mogile_file.make_intermediary_key() ==
               '0/564/879/0564879786.fid')

    def test_make_contentrepo_key(self, mogile_file):
        assert mogile_file.make_contentrepo_key() == \
            "2aae6c35c94fcfb415dbe95f408b9ce91ee846ed"

    def test_exists_in_bucket(self, mogile_file, s3_client):
        s3_client.Object.return_value.content_length = mogile_file.length
        assert(mogile_file.exists_in_bucket(s3_client, 'my-bucket', 'my-key')
               is True)

    def test_does_not_exist_in_bucket(self, s3_client, mogile_file):
        ex = ClientError({'Error': {'Code': '404'}}, 'Head')
        s3_client.Object.return_value.load.side_effect = ex
        assert(mogile_file.exists_in_bucket(s3_client, 'my-bucket', 'my-file')
               is False)

    def test_exists_in_bucket_raises_exception(self, s3_client, mogile_file):
        ex = ClientError({'Error': {'Code': '500'}}, 'Head')
        s3_client.Object.return_value.load.side_effect = ex
        with pytest.raises(ClientError, match=r"An error occurred \(500\)"):
            mogile_file.exists_in_bucket(s3_client, 'my-bucket', 'my-key')

    def test_md5_fileobj(self, my_tempfile):
        assert (md5_fileobj_hex(my_tempfile) ==
                "5eb63bbbe01eeed093cb22bb8f5acdc3")
        assert md5_fileobj_b64(my_tempfile) == "XrY7u+Ae7tCTyyK7j1rNww=="

    def test_sha1_fileobj(self, my_tempfile):
        assert (sha1_fileobj_hex(my_tempfile) ==
                "2aae6c35c94fcfb415dbe95f408b9ce91ee846ed")
        assert (sha1_fileobj_b64(my_tempfile) ==
                "Kq5sNclPz7QV2+lfQIuc6R7oRu0=")

    def test_put(self, mogile_file: MogileFile,
                 mogile_client, s3_client,
                 requests_mock):
        requests_mock.get('http://example.org/1', content=b'hello world')
        requests_mock.get('http://example.org/2', content=b'hello world')
        mogile_file.put(mogile_client, s3_client, 'my-bucket')
        # Check that `put` was called with the correct MD5 sum.
        _, kwargs = s3_client.Object.return_value.put.call_args_list[0]
        assert kwargs['ContentMD5'] == 'XrY7u+Ae7tCTyyK7j1rNww=='

    def test_make_bucket_hash(self):
        assert make_bucket_hash("a:b,c:d") == {"a": "b", "c": "d"}
