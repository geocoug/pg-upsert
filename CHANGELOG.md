# CHANGELOG

## [Unreleased]

### Added

- Added a `--generate-config` CLI option to generate a configuration file template, or to create a new configuration file with provided CLI arguments.

## [1.5.1] - 2025-01-15

- Converted CLI from `argparse` to `typer`.

## History

Changes prior to version 1.5.1 are not documented in this file. Below is a brief summary of the changes made prior to the creation of this file.

- 14874d7 update PgUpsert docstring
- c1a477e update screenshot url
- 9538a05 rm comment
- 31eabde + dev pytest config, revise db and ups fixture scope to function vs autouse, rm repetitve code in ups fixture
- 6d19cd4 + local pytest hook
- 40c81d6 fix badge
- 2031a13 set build and publish jobs to run only on tag push
- 3666389 update docs
- 7ddab0d update README
- 8ae2ee1 update documentation, rename PgUpsert._show() -> PgUpsert._tabulate_sql(), + PgUpsert.show_control() method
- 63817bc update docs & examples
- c317766 update docs & examples
- 010a69a cleanup
- 13041d5 apply custom debug log formatting
- 422dbe2 more tests
- 6b642b3 rename files, lint & format
- 96e9a49 switch to uv-based build
- a7d9b6d indentation issue
- 699ab94 autoupdate hooks
- b0adfb4 rm __del__ method
- 5162b1f refine dependency version requirements
- 5667bea + pyyaml requirement
- 871f818 rm unused imports
- e45a216 update docs
- e74a34c rm unused imports
- 02faca3 fix requirements.txt
- 6fcc422 Db conn params, conf file, update docs (#13)
- d8fbd29 reverse tag push conditional fix
- 7e2fdd4 fix tag push conditional
- 1ff21b4 update docs (#12)
- 0dc4bfa CLI reconfiguration, stream FK and PK errors to console and log file. (#11)
- 8287a94 reconfigure package logging
- a219eae rename _version.py --> __version__.py
- 9c58271 entrypoint cli --> main, rename _version.py to __version__.py
- a514faf + publisher table to test for null values in column with FK constraint
- c7ea4b7 switch from sphinx to mkdocs
- ddff26f switch from sphinx to mkdocs
- 1296727 update project links
- 1d92209 wrap cli entrypoint in try > except block
- f05ffa1 update README examples
- bb5e041 add docs status badge
- 966a477 fix invalid routine call
- 56887f7 more tests, update docs
- bb6fdac configure for readthedocs
- 8a90d5c update README
- 2027590 fix cli example
- 5b523e5 entrypoint
- 11379e9 fix docer build ci
- 6a8314b + nojekyll
- 9dda33d fix target version file
- ee52a52 cleanup
- b226416 set permission for docs build action
- 755bf81 update docs build
- aa33648 update docs build
- be35ef5 Dev (#3)
- 902ef2b Refactor (#2)
- c6567f8 remove unfinished test
- 610128e more tests
- 5343ce7 update requirements
- 46a4a11 begin pytest integration
- ef66675 refine python version requirements
- 6697e31 add command line entrypoint
- a5572c5 update README
- 08a177d upgrade pip version
- ed5ffe7 add check constraint qa checks in addition to not-null, pk, and fk checks
- 93ac086 update dependencies
- 97a1f3c autoupdate hooks
- 7a86126 module config
- b9b68d1 module config
- b8ebce0 update README
- 45cccc9 rm test script, fix screenshot link
- dcd5d88 upgrade tools, move configs to pyproject.toml, add `__version__` var, add README example
- 543ebd3 lint and format, move linter configs, update README
- da4e375 multi-arch build, update hooks
- 51be306 2024-02-02 13:30:19
- efc3a98 init
