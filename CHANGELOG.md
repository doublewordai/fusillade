# Changelog

## [2.4.0](https://github.com/doublewordai/fusillade/compare/fusillade-v2.3.1...fusillade-v2.4.0) (2026-01-27)


### Features

* add index for schedulable pending requests query ([#97](https://github.com/doublewordai/fusillade/issues/97)) ([48eb459](https://github.com/doublewordai/fusillade/commit/48eb4593bd766673dfa80920c7ee8299abb4216b))

## [2.3.1](https://github.com/doublewordai/fusillade/compare/fusillade-v2.3.0...fusillade-v2.3.1) (2026-01-27)


### Bug Fixes

* eliminate CPU spikes on batch cancellation ([#95](https://github.com/doublewordai/fusillade/issues/95)) ([6f50e91](https://github.com/doublewordai/fusillade/commit/6f50e916a2933e4d5d6fced3cb68e3953b2a4d74))

## [2.3.0](https://github.com/doublewordai/fusillade/compare/fusillade-v2.2.0...fusillade-v2.3.0) (2026-01-26)


### Features

* replace cascade deletes with SET NULL for performance ([#89](https://github.com/doublewordai/fusillade/issues/89)) ([aa47c5c](https://github.com/doublewordai/fusillade/commit/aa47c5caaaf4cff11b009a1116e4327353bf2f88))

## [2.2.0](https://github.com/doublewordai/fusillade/compare/fusillade-v2.1.2...fusillade-v2.2.0) (2026-01-23)


### Features

* race condition fix when creating file and fetching directly after in diff… ([#87](https://github.com/doublewordai/fusillade/issues/87)) ([1f3f690](https://github.com/doublewordai/fusillade/commit/1f3f690497da5883efd3bb4006b7c7eba8c88bc2))

## [2.1.2](https://github.com/doublewordai/fusillade/compare/fusillade-v2.1.1...fusillade-v2.1.2) (2026-01-23)


### Bug Fixes

* inserts config validation ([#84](https://github.com/doublewordai/fusillade/issues/84)) ([c5ad534](https://github.com/doublewordai/fusillade/commit/c5ad534913ad61a43d0a65574ab551761ccf3018))
* use primary pool to fetch batch after creation to avoid race conditions ([#86](https://github.com/doublewordai/fusillade/issues/86)) ([f53d197](https://github.com/doublewordai/fusillade/commit/f53d1974d42265b1174fe17829ad943bd7d62570))

## [2.1.1](https://github.com/doublewordai/fusillade/compare/fusillade-v2.1.0...fusillade-v2.1.1) (2026-01-23)


### Bug Fixes

* batch metadata parsing ([6fcca42](https://github.com/doublewordai/fusillade/commit/6fcca420cba3c2b9d84a696a7e9ad4b9deb92e56))
* simplify batch insert strategy - only batched ([#83](https://github.com/doublewordai/fusillade/issues/83)) ([ed77652](https://github.com/doublewordai/fusillade/commit/ed776528a992739f1e7a6d74033662e44018e53a))

## [2.1.0](https://github.com/doublewordai/fusillade/compare/fusillade-v2.0.1...fusillade-v2.1.0) (2026-01-23)


### Features

* templates batched writes ([#80](https://github.com/doublewordai/fusillade/issues/80)) ([94c82c8](https://github.com/doublewordai/fusillade/commit/94c82c8c710b524d70ce3661742cdd3537c149cc))


### Bug Fixes

* add sqlx pool provider ([#81](https://github.com/doublewordai/fusillade/issues/81)) ([dce6566](https://github.com/doublewordai/fusillade/commit/dce6566052a382ba5bafa3c39764f1e0618064f1))
* add test coverage for get batch lazy finalization path ([b1b4fa1](https://github.com/doublewordai/fusillade/commit/b1b4fa1782579daacd207e2ef2dffedca0d60db8))

## [2.0.1](https://github.com/doublewordai/fusillade/compare/fusillade-v2.0.0...fusillade-v2.0.1) (2026-01-22)


### Bug Fixes

* remove test files ([d4234c5](https://github.com/doublewordai/fusillade/commit/d4234c540d2ba01768b350a6b1ba0f46a108d84b))
* use write pool for UPDATE in get_batch() ([012796c](https://github.com/doublewordai/fusillade/commit/012796c8bf33561b33b7f2b6867581acf3c10279))

## [2.0.0](https://github.com/doublewordai/fusillade/compare/fusillade-v1.1.1...fusillade-v2.0.0) (2026-01-21)


### ⚠ BREAKING CHANGES

* separate out db connections into read and write ([#76](https://github.com/doublewordai/fusillade/issues/76))

### Features

* separate out db connections into read and write ([#76](https://github.com/doublewordai/fusillade/issues/76)) ([ce80c3f](https://github.com/doublewordai/fusillade/commit/ce80c3f1d0c63732b07df443e4098898fa899ad2))

## [1.1.1](https://github.com/doublewordai/fusillade/compare/fusillade-v1.1.0...fusillade-v1.1.1) (2026-01-19)


### Bug Fixes

* builder errors should be unretriable, they're a new class of error ([#72](https://github.com/doublewordai/fusillade/issues/72)) ([77995cc](https://github.com/doublewordai/fusillade/commit/77995cc6f65b02642a2d1756fde4e4a228c82179))
* remove queries joining request templates without need ([#74](https://github.com/doublewordai/fusillade/issues/74)) ([ca21c95](https://github.com/doublewordai/fusillade/commit/ca21c9564cccab48105ab76fec74e25e0efbcb79))

## [1.1.0](https://github.com/doublewordai/fusillade/compare/fusillade-v1.0.0...fusillade-v1.1.0) (2026-01-16)


### Features

* remove filename unique constraint ([#70](https://github.com/doublewordai/fusillade/issues/70)) ([3e68874](https://github.com/doublewordai/fusillade/commit/3e68874e34738d7d7f85b364cfa7f8183e5695c3))

## [1.0.0](https://github.com/doublewordai/fusillade/compare/fusillade-v0.16.0...fusillade-v1.0.0) (2026-01-13)


### ⚠ BREAKING CHANGES

* sla escalation through control layer ([#67](https://github.com/doublewordai/fusillade/issues/67))

### Bug Fixes

* sla escalation through control layer ([#67](https://github.com/doublewordai/fusillade/issues/67)) ([b0ada75](https://github.com/doublewordai/fusillade/commit/b0ada75f6ed51337ff121007fa9150f50d5c8210))

## [0.16.0](https://github.com/doublewordai/fusillade/compare/fusillade-v0.15.0...fusillade-v0.16.0) (2026-01-12)


### Features

* stream_batch_results in postgres manager ([#66](https://github.com/doublewordai/fusillade/issues/66)) ([05a96d5](https://github.com/doublewordai/fusillade/commit/05a96d59463dac3c12d7463e584d60b0f7088f14))

## [0.15.0](https://github.com/doublewordai/fusillade/compare/fusillade-v0.14.0...fusillade-v0.15.0) (2026-01-09)


### Features

* add enhanced retry and escalation observability metrics ([#64](https://github.com/doublewordai/fusillade/issues/64)) ([7786e97](https://github.com/doublewordai/fusillade/commit/7786e9712c251fe3d7739f69f3003da9499f9721))

## [0.14.0](https://github.com/doublewordai/fusillade/compare/fusillade-v0.13.0...fusillade-v0.14.0) (2026-01-08)


### Features

* Add metrics for (nearly) missed SLAs ([#61](https://github.com/doublewordai/fusillade/issues/61)) ([b1ab86e](https://github.com/doublewordai/fusillade/commit/b1ab86e725075670a1753c6dea93cc8b12e32db4))


### Bug Fixes

* supersede failed requests and do not create more than one escalated request ([#63](https://github.com/doublewordai/fusillade/issues/63)) ([f9be9e9](https://github.com/doublewordai/fusillade/commit/f9be9e9e189c22a98e79aedb00bac7148d63fe7c))

## [0.13.0](https://github.com/doublewordai/fusillade/compare/fusillade-v0.12.4...fusillade-v0.13.0) (2026-01-07)


### Features

* add prometheus metrics for daemon operations ([#58](https://github.com/doublewordai/fusillade/issues/58)) ([b86d526](https://github.com/doublewordai/fusillade/commit/b86d526c2c7f765b800d977d3bee018c4a252666))

## [0.12.4](https://github.com/doublewordai/fusillade/compare/fusillade-v0.12.3...fusillade-v0.12.4) (2026-01-07)


### Bug Fixes

* use ISO 8601 format for batch timestamp headers ([#56](https://github.com/doublewordai/fusillade/issues/56)) ([61306b8](https://github.com/doublewordai/fusillade/commit/61306b8eee506cd01ccb1da285d1e0b920787b4d))

## [0.12.3](https://github.com/doublewordai/fusillade/compare/fusillade-v0.12.2...fusillade-v0.12.3) (2026-01-07)


### Bug Fixes

* handle multiple escalated requests in supersede_racing_pair query ([#54](https://github.com/doublewordai/fusillade/issues/54)) ([0ac1068](https://github.com/doublewordai/fusillade/commit/0ac1068808b9eba52d90008a0bc80cec76b16b79))

## [0.12.2](https://github.com/doublewordai/fusillade/compare/fusillade-v0.12.1...fusillade-v0.12.2) (2026-01-06)


### Bug Fixes

* use targeted query for escalation lookup ([#52](https://github.com/doublewordai/fusillade/issues/52)) ([6e84c39](https://github.com/doublewordai/fusillade/commit/6e84c39e72d46f94e544674578bce6fac3564bfd))

## [0.12.1](https://github.com/doublewordai/fusillade/compare/fusillade-v0.12.0...fusillade-v0.12.1) (2025-12-24)


### Bug Fixes

* sla query to only focus on expiry time in order to pick all at risk requests ([#48](https://github.com/doublewordai/fusillade/issues/48)) ([e691424](https://github.com/doublewordai/fusillade/commit/e6914246aaeb8fe58084fba38ac805b28fa49986))

## [0.12.0](https://github.com/doublewordai/fusillade/compare/fusillade-v0.11.1...fusillade-v0.12.0) (2025-12-22)


### Features

* custom ID header ([#46](https://github.com/doublewordai/fusillade/issues/46)) ([1a6a5d2](https://github.com/doublewordai/fusillade/commit/1a6a5d25d048e5ffbfa68b641e6abb3166837640))

## [0.11.1](https://github.com/doublewordai/fusillade/compare/fusillade-v0.11.0...fusillade-v0.11.1) (2025-12-20)


### Bug Fixes

* single persistence to failed for retryable errrors ([#44](https://github.com/doublewordai/fusillade/issues/44)) ([cd1005a](https://github.com/doublewordai/fusillade/commit/cd1005a5ba705a5c161a8438c775ed062f422e52))
* use dashes not underscores for headers ([#42](https://github.com/doublewordai/fusillade/issues/42)) ([d941f22](https://github.com/doublewordai/fusillade/commit/d941f226e6479a29607dc07a5cbdc18bef04cb54))

## [0.11.0](https://github.com/doublewordai/fusillade/compare/fusillade-v0.10.0...fusillade-v0.11.0) (2025-12-19)


### Features

* pass in fields to forward as headers ([#38](https://github.com/doublewordai/fusillade/issues/38)) ([3d1e309](https://github.com/doublewordai/fusillade/commit/3d1e309a986b4c32245bb92f1fcaeefc3fa9311c))
* Sla escalation ([#34](https://github.com/doublewordai/fusillade/issues/34)) ([6c6b802](https://github.com/doublewordai/fusillade/commit/6c6b802285caa417cc2f169be3e464610be6fa77))

## [0.10.0](https://github.com/doublewordai/fusillade/compare/fusillade-v0.9.0...fusillade-v0.10.0) (2025-12-19)


### Features

* file and batch search ([#39](https://github.com/doublewordai/fusillade/issues/39)) ([8564fac](https://github.com/doublewordai/fusillade/commit/8564facaa483a64dfeac88f3d91c630fa970ca9f))

## [0.9.0](https://github.com/doublewordai/fusillade/compare/fusillade-v0.8.2...fusillade-v0.9.0) (2025-12-18)


### Features

* delete files batches requests ([#36](https://github.com/doublewordai/fusillade/issues/36)) ([764ce6e](https://github.com/doublewordai/fusillade/commit/764ce6e6da45d3a8f928967d5b303f3303b66778))

## [0.8.2](https://github.com/doublewordai/fusillade/compare/fusillade-v0.8.1...fusillade-v0.8.2) (2025-12-17)


### Bug Fixes

* remove test column from requests ([#33](https://github.com/doublewordai/fusillade/issues/33)) ([508e2a7](https://github.com/doublewordai/fusillade/commit/508e2a786b81a4fab9525d6f222030405261f4bf))

## [0.8.1](https://github.com/doublewordai/fusillade/compare/fusillade-v0.8.0...fusillade-v0.8.1) (2025-12-17)


### Bug Fixes

* dummy migration on requests table ([#31](https://github.com/doublewordai/fusillade/issues/31)) ([34d9dff](https://github.com/doublewordai/fusillade/commit/34d9dff140e87183c2b73075c6fcc2546e4c3990))

## [0.8.0](https://github.com/doublewordai/fusillade/compare/fusillade-v0.7.0...fusillade-v0.8.0) (2025-12-15)


### Features

* Retry failed batch requests up to SLA and provide manual retry afterwards ([#28](https://github.com/doublewordai/fusillade/issues/28)) ([df7a2b4](https://github.com/doublewordai/fusillade/commit/df7a2b421c7db5cc9818b05b8020651b6b3ee48c))


### Bug Fixes

* retry tests ([#30](https://github.com/doublewordai/fusillade/issues/30)) ([9fdb5de](https://github.com/doublewordai/fusillade/commit/9fdb5de171b6813d5dfbe162c5723b56ae3d8379))

## [0.7.0](https://github.com/doublewordai/fusillade/compare/fusillade-v0.6.0...fusillade-v0.7.0) (2025-12-10)


### Features

* get_file_template_stats ([#26](https://github.com/doublewordai/fusillade/issues/26)) ([cdd2487](https://github.com/doublewordai/fusillade/commit/cdd248727601b6ea3bcbf01e0cc432b31f214b44))

## [0.6.0](https://github.com/doublewordai/fusillade/compare/fusillade-v0.5.1...fusillade-v0.6.0) (2025-12-02)


### Features

* Output & Error File sizes ([#14](https://github.com/doublewordai/fusillade/issues/14))  ([a19b0ff](https://github.com/doublewordai/fusillade/commit/a19b0ff292ee60be0714ef83fbd659c13ac8709c))

## [0.5.1](https://github.com/doublewordai/fusillade/compare/fusillade-v0.5.0...fusillade-v0.5.1) (2025-12-02)


### Bug Fixes

* batch id header ([#22](https://github.com/doublewordai/fusillade/issues/22)) ([9fd20f3](https://github.com/doublewordai/fusillade/commit/9fd20f394490e5cab2c3aa30f918ab598f918f9e))

## [0.5.0](https://github.com/doublewordai/fusillade/compare/fusillade-v0.4.0...fusillade-v0.5.0) (2025-12-01)


### Features

* X-Fusillade-Request-Id ([#20](https://github.com/doublewordai/fusillade/issues/20)) ([3af81a1](https://github.com/doublewordai/fusillade/commit/3af81a1362c02b336775cbda5368d3034da8b23e))

## [0.4.0](https://github.com/doublewordai/fusillade/compare/fusillade-v0.3.0...fusillade-v0.4.0) (2025-11-28)


### Features

* initial fusillade release ([4102ab7](https://github.com/doublewordai/fusillade/commit/4102ab771d991e43101e59adbd4525801924ca2b))
* test release-please with manifest config ([081662d](https://github.com/doublewordai/fusillade/commit/081662d622397369f49f176a6a1f3c9d604d606d))


### Bug Fixes

* cancellations ([#17](https://github.com/doublewordai/fusillade/issues/17)) ([c9b9f25](https://github.com/doublewordai/fusillade/commit/c9b9f258a19dedb65e67842789e65ce52ef5bbea))
* coordinate multiple replicas ([#13](https://github.com/doublewordai/fusillade/issues/13)) ([9838f21](https://github.com/doublewordai/fusillade/commit/9838f2148da42e7b7eabd8e8233be5787c7ba27a))
* remove useless trigger ([#15](https://github.com/doublewordai/fusillade/issues/15)) ([828f30e](https://github.com/doublewordai/fusillade/commit/828f30edc039f35740ad7e8c78e2f84c9480110a))

## [0.3.0](https://github.com/doublewordai/control-layer/compare/fusillade-v0.2.0...fusillade-v0.3.0) (2025-11-24)


### Features

* add actual cancellation of in-progress batch requests ([#170](https://github.com/doublewordai/control-layer/issues/170)) ([2044218](https://github.com/doublewordai/control-layer/commit/2044218ecaffbb763b1cf8750e4d072469b4be62))

## [0.2.0](https://github.com/doublewordai/control-layer/compare/fusillade-v0.1.1...fusillade-v0.2.0) (2025-11-20)


### Features

* add capacity & batch capacity ([#106](https://github.com/doublewordai/control-layer/issues/106)) ([d7f3f6d](https://github.com/doublewordai/control-layer/commit/d7f3f6d5435717dd10e2fda304bf4022a3179dc8))
* batches endpoints ([#72](https://github.com/doublewordai/control-layer/issues/72)) ([f2143c6](https://github.com/doublewordai/control-layer/commit/f2143c6be2ed1cdc1cba60e630259feb1166ab7e))
* daemon status tracking ([#96](https://github.com/doublewordai/control-layer/issues/96)) ([9222649](https://github.com/doublewordai/control-layer/commit/9222649f6706756fc5166c4747893e356f196914))
* openAI compatible files endpoints ([#60](https://github.com/doublewordai/control-layer/issues/60)) ([5c2eccd](https://github.com/doublewordai/control-layer/commit/5c2eccd3aafc8b2fabe6baadad4d26552a80da41))
* track batch status via triggers, and query in bulk rather than doing N+1 queries ([#100](https://github.com/doublewordai/control-layer/issues/100)) ([68d005d](https://github.com/doublewordai/control-layer/commit/68d005dadb00c2a4afc066b8a62c2afb528d57ef))


### Bug Fixes

* add default 30-day expiry for files when none specified ([#131](https://github.com/doublewordai/control-layer/issues/131)) ([0cce7cd](https://github.com/doublewordai/control-layer/commit/0cce7cdc266b65ed014f2ddf96b255b492d412d2)), closes [#117](https://github.com/doublewordai/control-layer/issues/117)
* better claiming logic ([#99](https://github.com/doublewordai/control-layer/issues/99)) ([a5759ff](https://github.com/doublewordai/control-layer/commit/a5759ffa1088978a6a3f575672ae3167d684a8ee))
* claim on polling interval ([#105](https://github.com/doublewordai/control-layer/issues/105)) ([2103553](https://github.com/doublewordai/control-layer/commit/2103553fa8093491fbb6f28cefe909e147b31dae))
* fewer triggers ([#103](https://github.com/doublewordai/control-layer/issues/103)) ([07f5fdf](https://github.com/doublewordai/control-layer/commit/07f5fdf5e72bbb534d175e463dbd3c8fc7f35b35))
* migrate to race-safe compaction function ([1ae9ef2](https://github.com/doublewordai/control-layer/commit/1ae9ef2656b8ce25aa537d31b0129bda68db2164))
* retries ([#102](https://github.com/doublewordai/control-layer/issues/102)) ([031c09f](https://github.com/doublewordai/control-layer/commit/031c09ffa7d109aac0eea23fb1399fc9c164972a))
* revert to aggregating batch status on demand ([#112](https://github.com/doublewordai/control-layer/issues/112)) ([04e9498](https://github.com/doublewordai/control-layer/commit/04e9498fc92e2461482f8df016c6b0e4974f0a78))
* wal for status updates ([#104](https://github.com/doublewordai/control-layer/issues/104)) ([1061b78](https://github.com/doublewordai/control-layer/commit/1061b78a4fec3c6af3dab6efb6a3a1e4a4c2c16d))

## [0.1.1](https://github.com/doublewordai/control-layer/compare/fusillade-v0.1.0...fusillade-v0.1.1) (2025-11-06)


### Bug Fixes

* add just release target, setup idempotent publishing ([3084ce1](https://github.com/doublewordai/control-layer/commit/3084ce18c95ddabc23a9716e9918dcb244e51141))

## 0.1.0 (2025-11-06)


### Features

* add fusillade: a daemon implementation for sending batched requests ([#55](https://github.com/doublewordai/control-layer/issues/55)) ([af4a60e](https://github.com/doublewordai/control-layer/commit/af4a60ed91c7e7732e6fa16427522e013b86c50b))
* trigger release please ([95a195b](https://github.com/doublewordai/control-layer/commit/95a195bf677a6c09114a23a08e60a28143e112f6))
