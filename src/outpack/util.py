from pathlib import Path


def find_file_descend(filename, path):
    path = Path(path)
    root = Path(path.root)

    while path != root:
        attempt = path / filename
        if attempt.exists():
            return str(attempt.parent)
        path = path.parent

    return None
