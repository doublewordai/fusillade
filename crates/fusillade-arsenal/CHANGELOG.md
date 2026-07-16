# Changelog

## [1.2.0](https://github.com/doublewordai/fusillade/compare/fusillade-arsenal-v1.1.1...fusillade-arsenal-v1.2.0) (2026-07-16)


### Features

* add batch_requests_archive schema and routing columns (phase 3 … ([#349](https://github.com/doublewordai/fusillade/issues/349)) ([0e548d9](https://github.com/doublewordai/fusillade/commit/0e548d971fe0563e32b00dff71fc403dfc34739b))

## [1.1.1](https://github.com/doublewordai/fusillade/compare/fusillade-arsenal-v1.1.0...fusillade-arsenal-v1.1.1) (2026-07-16)


### Bug Fixes

* preserve independent workspace crate versions ([#345](https://github.com/doublewordai/fusillade/issues/345)) ([80824e2](https://github.com/doublewordai/fusillade/commit/80824e26109d20f3bf22c320641e8da11c12fa3b))


### Dependencies

* The following workspace dependencies were updated
  * dependencies
    * fusillade-core bumped from 1.1.0 to 1.1.1

## [1.1.0](https://github.com/doublewordai/fusillade/compare/fusillade-arsenal-v1.0.1...fusillade-arsenal-v1.1.0) (2026-07-13)


### Features

* freeze terminal batch counts on the batches row ([#329](https://github.com/doublewordai/fusillade/issues/329)) ([3e51c4c](https://github.com/doublewordai/fusillade/commit/3e51c4cf53588626fe29e8536b10a875ca484999))


### Dependencies

* The following workspace dependencies were updated
  * dependencies
    * fusillade-core bumped from 1.0.0 to 1.1.0

## [1.0.1](https://github.com/doublewordai/fusillade/compare/fusillade-arsenal-v1.0.0...fusillade-arsenal-v1.0.1) (2026-07-10)


### Bug Fixes

* package sqlx metadata with storage crate ([#332](https://github.com/doublewordai/fusillade/issues/332)) ([3123839](https://github.com/doublewordai/fusillade/commit/31238392c883c8007b78e89732a4797bd16d404b))

## [1.0.0](https://github.com/doublewordai/fusillade/compare/fusillade-arsenal-v0.1.0...fusillade-arsenal-v1.0.0) (2026-07-10)


### ⚠ BREAKING CHANGES

* split storage and daemon crates ([#323](https://github.com/doublewordai/fusillade/issues/323))
* mark streaming API as breaking. BREAKING CHANGE: removed stream field from RequestData, changed ReqwestHttpClient new signature

### feat\

* mark streaming API as breaking. BREAKING CHANGE: removed stream field from RequestData, changed ReqwestHttpClient new signature ([4220b4a](https://github.com/doublewordai/fusillade/commit/4220b4a892098564c734f23f245ac24007e7bb77))


### Features

* initial fusillade release ([4102ab7](https://github.com/doublewordai/fusillade/commit/4102ab771d991e43101e59adbd4525801924ca2b))
* split storage and daemon crates ([#323](https://github.com/doublewordai/fusillade/issues/323)) ([bd309b3](https://github.com/doublewordai/fusillade/commit/bd309b343843a5b05e1bfafa68ac091a0731a172))
* test release-please with manifest config ([081662d](https://github.com/doublewordai/fusillade/commit/081662d622397369f49f176a6a1f3c9d604d606d))


### Bug Fixes

* separate file stream aborts from fusillade errors ([b1cfb1e](https://github.com/doublewordai/fusillade/commit/b1cfb1e810bd0e8e85a5a537f59ae08db8f5e8ed))


### Dependencies

* The following workspace dependencies were updated
  * dependencies
    * fusillade-core bumped from 0.1.0 to 1.0.0
