from dataclasses import dataclass

import pygit2
from dataclasses_json import dataclass_json


@dataclass_json
@dataclass
class GitInfo:
    sha: str
    branch: str
    url: list[str]


def git_info(path):
    repo = pygit2.discover_repository(path)
    if not repo:
        return None
    repo = pygit2.Repository(repo)
    sha = str(repo.head.target)
    branch = repo.head.shorthand
    url = [x.url for x in repo.remotes]
    return GitInfo(sha, branch, url)
