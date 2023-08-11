import pygit2


def git_info(path):
    repo = pygit2.discover_repository(path)
    if not repo:
        return None
    repo = pygit2.Repository(repo)
    sha = str(repo.head.target)
    branch = repo.head.shorthand
    url = [x.url for x in repo.remotes]
    return {"sha": sha, "branch": branch, "url": url}
