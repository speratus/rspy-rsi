# README
This library is a python native module written in rust to solve a very
domain specific problem of running bulk inserts for Django many-to-many
relationships. As such, table values are hard-coded.

As a result, this library is not intended for general use, though it could
be adapted for more general use.

## Installation

1. Clone the repository and navigate into it.
2. Run `pipenv install` to install python dependencies.
3. Run `pipenv shell` to open a shell window with the correct `PATH`.
4. Run `maturin build` to Create a wheel file for your OS that can be
    installed into other virtual environments.