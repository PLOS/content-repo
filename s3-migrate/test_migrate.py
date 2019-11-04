import pytest
from migrate import MogileFile


class TestMigrate:
    def test_parse_entry(self):
        entry = {'fid': 1, 'dmid': 1, 'dkey':
                 'f6e6fa50746ea0d0bb698e1ba8506a3e2ea8a149-mogilefs-prod-repo',
                 'length': 1593790, 'classid': 0, 'devcount': 2}
        file = MogileFile.parse_entry(entry)
        assert file.sha1sum == 'f6e6fa50746ea0d0bb698e1ba8506a3e2ea8a149'
        assert file.fid == 1
        assert file.length == 1593790

    def test_parse_bad_dmid(self):
        entry = {'fid': 1, 'dmid': 2, 'dkey':
                 'f6e6fa50746ea0d0bb698e1ba8506a3e2ea8a149-mogilefs-prod-repo',
                 'length': 1593790, 'classid': 0, 'devcount': 2}
        with pytest.raises(AssertionError, match='Bad domain'):
            MogileFile.parse_entry(entry)

    def test_parse_bad_class(self):
        entry = {'fid': 1, 'dmid': 1, 'dkey':
                 'f6e6fa50746ea0d0bb698e1ba8506a3e2ea8a149-mogilefs-prod-repo',
                 'length': 1593790, 'classid': 1, 'devcount': 2}
        with pytest.raises(AssertionError, match='Bad class'):
            MogileFile.parse_entry(entry)
