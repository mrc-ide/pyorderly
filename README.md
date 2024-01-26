# outpack

[![PyPI - Version](https://img.shields.io/pypi/v/outpack.svg)](https://pypi.org/project/outpack)
[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/outpack.svg)](https://pypi.org/project/outpack)

-----

**Table of Contents**

- [Installation](#installation)
- [License](#license)

## Installation

```console
pip install outpack
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
development environment of `outpack-py` by installing the parser from your local
checkout:

```
hatch run pip install /path/to/outpack_server
```


## License

`outpack` is distributed under the terms of the [MIT](https://spdx.org/licenses/MIT.html) license.
