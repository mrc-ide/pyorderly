# pyorderly

[![PyPI - Version](https://img.shields.io/pypi/v/pyorderly.svg)](https://pypi.org/project/pyorderly)
[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/pyorderly.svg)](https://pypi.org/project/pyorderly)

-----

**Table of Contents**

- [Installation](#installation)
- [License](#license)

## Installation

```console
pip install pyorderly
```

## Development

Useful commands:

```
uv run task test  # Run tests
uv run task cov   # Run tests and print coverage report
uv run task check # Check format and types
uv run task fmt   # Re-format the code
```

### Modifying the parser

The outpack query parser is implemented in Rust as part of the
[outpack_server](https://github.com/mrc-ide/outpack_server) repository. When
developing new features in the query parser, they can be tested out in your
development environment of `pyorderly` by passing the `--with` flag to `uv`:

```
uv run --with /path/to/outpack_server ...
```

## Releasing

- Increment the version number using [the `uv version` command](https://docs.astral.sh/uv/guides/package/#updating-your-version).
- Commit the changes and create a PR.
- Get the PR approved and merged to main.
- Create a [GitHub release](https://github.com/mrc-ide/pyorderly/releases/new):
  - Set the tag name as `vX.Y.Z`, matching the version reported by uv.
  - Write some release notes (possibly using the `Generate release notes` button).
  - Publish the release!
- Sit back and relax while the release gets built and published.
- Check that the new version is available on [PyPI](https://pypi.org/project/pyorderly/#history).

## License

`pyorderly` is distributed under the terms of the [MIT](https://spdx.org/licenses/MIT.html) license.
