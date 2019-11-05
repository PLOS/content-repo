import pytest

from botocore.exceptions import ClientError
from mock import Mock

from migrate import MogileFile


class TestMigrate():
    @pytest.fixture
    def entry(self):
        return {'fid': 1,
                'dmid': 1,
                'dkey': 'f6e6fa50746ea0d0bb698e1ba8506a3e2ea8a149-mogilefs-prod-repo',
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

    def test_parse_entry(self, entry):
        file = MogileFile.parse_entry(entry)
        assert file.sha1sum == 'f6e6fa50746ea0d0bb698e1ba8506a3e2ea8a149'
        assert file.fid == 1
        assert file.length == 1593790

    def test_parse_bad_dmid(self, entry):
        entry['dmid'] = 2
        with pytest.raises(AssertionError, match='Bad domain'):
            MogileFile.parse_entry(entry)

    def test_parse_bad_class(self, entry):
        entry['classid'] = 1
        with pytest.raises(AssertionError, match='Bad class'):
            MogileFile.parse_entry(entry)

    def test_make_mogile_path(self, mogile_file):
        assert mogile_file.make_mogile_path() == '/0/564/879/0564879786.fid'

    def test_contentrepo_path(self, mogile_file):
        assert mogile_file.make_contentrepo_path() == \
            "/f6e6fa50746ea0d0bb698e1ba8506a3e2ea8a149"

    def test_exists_in_bucket(self, mogile_file, s3):
        assert mogile_file.exists_in_bucket(s3, 'my-bucket') == True

    def test_does_not_exist_in_bucket(self, mogile_file, s3):
        ex = ClientError({'Error': {'Code': '404'}}, 'Head')
        s3.Object.return_value.load.side_effect = ex
        assert mogile_file.exists_in_bucket(s3, 'my-bucket') == False

    def test_exists_in_bucket_raises_exception(self, mogile_file, s3):
        ex = ClientError({'Error': {'Code': '500'}}, 'Head')
        s3.Object.return_value.load.side_effect = ex
        with pytest.raises(ClientError, match=r"An error occurred \(500\)"):
            mogile_file.exists_in_bucket(s3, 'my-bucket')
