import pytest

from botocore.exceptions import ClientError
from mock import Mock

from migrate import MogileFile, exists_in_bucket


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
    def row(self):
        return {
            'fid': 1,
            'dmid': 1,
            'dkey':
            'f6e6fa50746ea0d0bb698e1ba8506a3e2ea8a149-mogilefs-prod-repo',
            'length': 1593790,
            'classid': 0,
            'devcount': 2}

    @pytest.fixture
    def mogile_file(self):
        return MogileFile(
            sha1sum='f6e6fa50746ea0d0bb698e1ba8506a3e2ea8a149',
            fid=564879786,
            bucket='repo',
            length=1)

    def test_parse_row(self, row):
        file = MogileFile.parse_row(row)
        assert file.sha1sum == 'f6e6fa50746ea0d0bb698e1ba8506a3e2ea8a149'
        assert file.fid == 1
        assert file.length == 1593790

    def test_parse_bad_dmid(self, row):
        row['dmid'] = 2
        with pytest.raises(AssertionError, match='Bad domain'):
            MogileFile.parse_row(row)

    def test_parse_bad_class(self, row):
        row['classid'] = 1
        with pytest.raises(AssertionError, match='Bad class'):
            MogileFile.parse_row(row)

    def test_make_mogile_path(self, mogile_file):
        assert mogile_file.make_mogile_path() == '/0/564/879/0564879786.fid'

    def test_contentrepo_path(self, mogile_file):
        assert mogile_file.make_contentrepo_path() == \
            "/f6e6fa50746ea0d0bb698e1ba8506a3e2ea8a149"

    def test_exists_in_bucket(self, s3_client):
        assert exists_in_bucket(s3_client, 'my-bucket', 'my-path') is True

    def test_does_not_exist_in_bucket(self, s3_client):
        ex = ClientError({'Error': {'Code': '404'}}, 'Head')
        s3_client.Object.return_value.load.side_effect = ex
        assert exists_in_bucket(s3_client, 'my-bucket', 'my-file') is False

    def test_exists_in_bucket_raises_exception(self, s3_client):
        ex = ClientError({'Error': {'Code': '500'}}, 'Head')
        s3_client.Object.return_value.load.side_effect = ex
        with pytest.raises(ClientError, match=r"An error occurred \(500\)"):
            exists_in_bucket(s3_client, 'my-bucket', 'my-path')
