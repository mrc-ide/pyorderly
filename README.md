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

Useful hatch commands:

```
hatch shell
hatch run test
hatch run cov
hatch run lint:style
hatch run lint:fmt
```

### Modifying the parser

The outpack query parser is implemented in Rust as part of the
[outpack_server](https://github.com/mrc-ide/outpack_server) repository. When
developing new features in the query parser, they can be tested out in your
development environment of `pyorderly` by installing the parser from your local
checkout:

```
hatch run pip install /path/to/outpack_server
```

## Releasing

- Increment the version number using [the `hatch version` command](https://hatch.pypa.io/latest/version/#updating).
- Commit the changes and create a PR.
- Get the PR approved and merged to main.
- Create a [GitHub release](https://github.com/mrc-ide/pyorderly/releases/new):
  - Set the tag name as `vX.Y.Z`, matching the version reported by hatch.
  - Write some release notes (possibly using the `Generate release notes` button).
  - Publish the release!
- Sit back and relax while the release gets built and published.
- Check that the new version is available on [PyPI](https://pypi.org/project/pyorderly/#history).

## License

`pyorderly` is distributed under the terms of the [MIT](https://spdx.org/licenses/MIT.html) license.
