import pygit2

from pyorderly.outpack.tools import GitInfo, git_info


def simple_git_example(path, remote=None):
    d = path / "subdir"
    d.mkdir()
    f = d / "file"
    f.write_text("hello")
    repo = pygit2.init_repository(path)
    repo.index.add_all()
    author = pygit2.Signature("Alice Author", "alice@example.com")
    message = "Initial commit"
    tree = repo.index.write_tree()
    result = repo.create_commit("HEAD", author, author, message, tree, [])
    if remote:
        for name, url in remote:
            repo.remotes.create(name, url)
    return str(result)


def test_git_report_no_info_without_git_repo(tmp_path):
    p = tmp_path / "sub"
    p.mkdir()
    assert git_info(p) is None


def test_git_report_git_info_if_possible(tmp_path):
    sha = simple_git_example(tmp_path)
    res = git_info(tmp_path)
    assert res == GitInfo(branch="master", sha=sha, url=[]) or res == GitInfo(
        branch="main", sha=sha, url=[]
    )


def test_git_report_single_url(tmp_path):
    simple_git_example(tmp_path, [("origin", "https://example.com/git")])
    res = git_info(tmp_path)
    assert res.url == ["https://example.com/git"]


def test_git_report_several_urls(tmp_path):
    simple_git_example(
        tmp_path,
        [
            ("origin", "https://example.com/git"),
            ("other", "https://example.com/git2"),
        ],
    )
    res = git_info(tmp_path)
    assert res.url == ["https://example.com/git", "https://example.com/git2"]


def test_git_report_from_subdir(tmp_path):
    simple_git_example(tmp_path)
    assert git_info(tmp_path) == git_info(tmp_path / "subdir")
