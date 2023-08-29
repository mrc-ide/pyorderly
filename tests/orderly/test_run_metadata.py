import orderly
import pytest

from outpack.util import transient_working_directory


def test_resource_requires_that_files_exist_with_no_packet(tmp_path):
    path = tmp_path / "a"
    with transient_working_directory(tmp_path):
        with pytest.raises(Exception, match="File does not exist:"):
            orderly.resource(path)
    with open(path, "w"):
        pass
    with transient_working_directory(tmp_path):
        orderly.resource(path)
