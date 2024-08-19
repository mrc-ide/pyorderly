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


## License

`pyorderly` is distributed under the terms of the [MIT](https://spdx.org/licenses/MIT.html) license.
