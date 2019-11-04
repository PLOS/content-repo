import pytest
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
