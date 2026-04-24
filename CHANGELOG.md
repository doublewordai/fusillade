# Changelog

## [16.5.0](https://github.com/doublewordai/fusillade/compare/fusillade-v16.4.0...fusillade-v16.5.0) (2026-04-24)


### Features

* create single request batch ([#236](https://github.com/doublewordai/fusillade/issues/236)) ([02ca49d](https://github.com/doublewordai/fusillade/commit/02ca49d5b5212b7b97cb761be646a52f8310c389))

## [16.4.0](https://github.com/doublewordai/fusillade/compare/fusillade-v16.3.0...fusillade-v16.4.0) (2026-04-23)


### Features

* request methods for external daemons ([#233](https://github.com/doublewordai/fusillade/issues/233)) ([cffdd56](https://github.com/doublewordai/fusillade/commit/cffdd56608cb812e2b88cc99923f80ee81bd6ffa))

## [16.3.0](https://github.com/doublewordai/fusillade/compare/fusillade-v16.2.0...fusillade-v16.3.0) (2026-04-22)


### Features

* add service_tier column to requests table ([#230](https://github.com/doublewordai/fusillade/issues/230)) ([e54d518](https://github.com/doublewordai/fusillade/commit/e54d518cad5f2c11bbea18b7c68af767aa996a3d))

## [16.2.0](https://github.com/doublewordai/fusillade/compare/fusillade-v16.1.3...fusillade-v16.2.0) (2026-04-17)


### Features

* add cascade_batch_state_to_requests method ([#229](https://github.com/doublewordai/fusillade/issues/229)) ([dc91537](https://github.com/doublewordai/fusillade/commit/dc91537a46e2e2b1c4ff56bc266fd74c1bd2d9e6))


### Bug Fixes

* list_requests/get_request_detail project canonical state on cancel ([#226](https://github.com/doublewordai/fusillade/issues/226)) ([56e97d3](https://github.com/doublewordai/fusillade/commit/56e97d35334a82ed491681220198f46c125e132e))

## [16.1.3](https://github.com/doublewordai/fusillade/compare/fusillade-v16.1.2...fusillade-v16.1.3) (2026-04-16)


### Bug Fixes

* index list_requests active_first ordering ([#224](https://github.com/doublewordai/fusillade/issues/224)) ([3755f85](https://github.com/doublewordai/fusillade/commit/3755f85ca6aae29540f09937a42abe347cbd62e0))

## [16.1.2](https://github.com/doublewordai/fusillade/compare/fusillade-v16.1.1...fusillade-v16.1.2) (2026-04-16)


### Bug Fixes

* clippy rust 1.95 ([b954afd](https://github.com/doublewordai/fusillade/commit/b954afd5a9ade0473c6227feda6e25c69a11b104))

## [16.1.1](https://github.com/doublewordai/fusillade/compare/fusillade-v16.1.0...fusillade-v16.1.1) (2026-04-16)


### Bug Fixes

* fast total_count for list_requests via short-timeout exact + planner estimate ([#221](https://github.com/doublewordai/fusillade/issues/221)) ([868d579](https://github.com/doublewordai/fusillade/commit/868d57976ffd445c0e224b621cfd846854d55c2b))

## [16.1.0](https://github.com/doublewordai/fusillade/compare/fusillade-v16.0.0...fusillade-v16.1.0) (2026-04-15)


### Features

* add cross-batch request query methods to Storage trait ([#219](https://github.com/doublewordai/fusillade/issues/219)) ([b403e7d](https://github.com/doublewordai/fusillade/commit/b403e7d59ec8ed1cba4a7d1d50443787dbb45b04))

## [16.0.0](https://github.com/doublewordai/fusillade/compare/fusillade-v15.1.1...fusillade-v16.0.0) (2026-04-14)


### ⚠ BREAKING CHANGES

* generalise pending-request counts to start/end windows ([#217](https://github.com/doublewordai/fusillade/issues/217))

### Features

* generalise pending-request counts to start/end windows ([#217](https://github.com/doublewordai/fusillade/issues/217)) ([edd7d5e](https://github.com/doublewordai/fusillade/commit/edd7d5ebcdfaa7cacbc78fa3d1ed74f05f5dc6c1))

## [15.1.1](https://github.com/doublewordai/fusillade/compare/fusillade-v15.1.0...fusillade-v15.1.1) (2026-04-13)


### Bug Fixes

* inject priority under nvext.agent_hints with dynamo sign convention ([#215](https://github.com/doublewordai/fusillade/issues/215)) ([67882e1](https://github.com/doublewordai/fusillade/commit/67882e11fb2e0c4334a6081b8a5b90be03a36669))

## [15.1.0](https://github.com/doublewordai/fusillade/compare/fusillade-v15.0.0...fusillade-v15.1.0) (2026-04-13)


### Features

* inject deadline-derived priority into outbound request bodies ([#213](https://github.com/doublewordai/fusillade/issues/213)) ([463d08c](https://github.com/doublewordai/fusillade/commit/463d08c63d96e8053a3ae6e4c572259aa3ddcc2b))

## [15.0.0](https://github.com/doublewordai/fusillade/compare/fusillade-v14.2.2...fusillade-v15.0.0) (2026-04-08)


### ⚠ BREAKING CHANGES

* external connections ([#209](https://github.com/doublewordai/fusillade/issues/209))

### Features

* external connections ([#209](https://github.com/doublewordai/fusillade/issues/209)) ([472bac8](https://github.com/doublewordai/fusillade/commit/472bac88d66e765fba943be774de78a424452339))

## [14.2.2](https://github.com/doublewordai/fusillade/compare/fusillade-v14.2.1...fusillade-v14.2.2) (2026-04-07)


### Bug Fixes

* bump reassembler version to 0.3.0 ([#210](https://github.com/doublewordai/fusillade/issues/210)) ([6b59947](https://github.com/doublewordai/fusillade/commit/6b59947de6e7c7ce535676631d8b9d96fe347a34))

## [14.2.1](https://github.com/doublewordai/fusillade/compare/fusillade-v14.2.0...fusillade-v14.2.1) (2026-03-31)


### Bug Fixes

* pick up errors and replace the 200 status ([#196](https://github.com/doublewordai/fusillade/issues/196)) ([cbf68f5](https://github.com/doublewordai/fusillade/commit/cbf68f5a417e64bbbcbe77c4856ff0ee8e242793))

## [14.2.0](https://github.com/doublewordai/fusillade/compare/fusillade-v14.1.0...fusillade-v14.2.0) (2026-03-31)


### Features

* split user throughput metrics sla ([#202](https://github.com/doublewordai/fusillade/issues/202)) ([3472748](https://github.com/doublewordai/fusillade/commit/3472748c8039969dfcececc9123d6a55427f09f6))


### Bug Fixes

* add retries and error logging to critical state persisting ([#200](https://github.com/doublewordai/fusillade/issues/200)) ([953b2f3](https://github.com/doublewordai/fusillade/commit/953b2f3c290be36dfc50a25661d699203b796652))

## [14.1.0](https://github.com/doublewordai/fusillade/compare/fusillade-v14.0.1...fusillade-v14.1.0) (2026-03-30)


### Features

* prometheus metric user completed_requests ([#197](https://github.com/doublewordai/fusillade/issues/197)) ([9f0b5b0](https://github.com/doublewordai/fusillade/commit/9f0b5b02bfc4531d047000fa7745e2daa7fb4cf6))

## [14.0.1](https://github.com/doublewordai/fusillade/compare/fusillade-v14.0.0...fusillade-v14.0.1) (2026-03-27)


### Bug Fixes

* separate file stream aborts from fusillade errors ([b1cfb1e](https://github.com/doublewordai/fusillade/commit/b1cfb1e810bd0e8e85a5a537f59ae08db8f5e8ed))

## [14.0.0](https://github.com/doublewordai/fusillade/compare/fusillade-v13.1.1...fusillade-v14.0.0) (2026-03-27)


### ⚠ BREAKING CHANGES

* per-user fair usage scheduling ([#189](https://github.com/doublewordai/fusillade/issues/189))

### Features

* per-user fair usage scheduling ([#189](https://github.com/doublewordai/fusillade/issues/189)) ([440604f](https://github.com/doublewordai/fusillade/commit/440604f6bed08e71137d361a752f9a1c7dfaac9a))

## [13.1.1](https://github.com/doublewordai/fusillade/compare/fusillade-v13.1.0...fusillade-v13.1.1) (2026-03-26)


### Bug Fixes

* create output file on synchronous part of batch creation ([#190](https://github.com/doublewordai/fusillade/issues/190)) ([5ac0b8d](https://github.com/doublewordai/fusillade/commit/5ac0b8d5d30c63c1231be4483e672c06d0723cbe))

## [13.1.0](https://github.com/doublewordai/fusillade/compare/fusillade-v13.0.1...fusillade-v13.1.0) (2026-03-25)


### Features

* split create_batch in two to allow job dispatch ([#188](https://github.com/doublewordai/fusillade/issues/188)) ([3f1ca8b](https://github.com/doublewordai/fusillade/commit/3f1ca8b5cc32a7705f49b7b0a113006648a9760d))


### Bug Fixes

* add migration for pending, claimed and processing index ([#184](https://github.com/doublewordai/fusillade/issues/184)) ([fa9c9a1](https://github.com/doublewordai/fusillade/commit/fa9c9a1bbbdd984f5158cce63cf1aff924b2c0e5))

## [13.0.1](https://github.com/doublewordai/fusillade/compare/fusillade-v13.0.0...fusillade-v13.0.1) (2026-03-23)


### Bug Fixes

* adds support for streamed batch /v1/responses, bump reassembler ([#185](https://github.com/doublewordai/fusillade/issues/185)) ([9ab71a4](https://github.com/doublewordai/fusillade/commit/9ab71a4dec393d85bdcf8fab4a416cba312be918))

## [13.0.0](https://github.com/doublewordai/fusillade/compare/fusillade-v12.1.0...fusillade-v13.0.0) (2026-03-17)


### ⚠ BREAKING CHANGES

* Batch execution now uses the batch creator's API key, not the file uploader's ([#181](https://github.com/doublewordai/fusillade/issues/181))

### Features

* Batch execution now uses the batch creator's API key, not the file uploader's ([#181](https://github.com/doublewordai/fusillade/issues/181)) ([1d7749a](https://github.com/doublewordai/fusillade/commit/1d7749a7cfae2b74ffb4ed45927abe75cee52277))

## [12.1.0](https://github.com/doublewordai/fusillade/compare/fusillade-v12.0.0...fusillade-v12.1.0) (2026-03-16)


### Features

* server side, active batch first sorting on toggle ([#175](https://github.com/doublewordai/fusillade/issues/175)) ([d27db87](https://github.com/doublewordai/fusillade/commit/d27db873ff0c0ab41af35c8fc0d50a8b148d83af))

## [12.0.0](https://github.com/doublewordai/fusillade/compare/fusillade-v11.0.1...fusillade-v12.0.0) (2026-03-13)


### ⚠ BREAKING CHANGES

* mark streaming API as breaking. BREAKING CHANGE: removed stream field from RequestData, changed ReqwestHttpClient new signature

### feat\

* mark streaming API as breaking. BREAKING CHANGE: removed stream field from RequestData, changed ReqwestHttpClient new signature ([4220b4a](https://github.com/doublewordai/fusillade/commit/4220b4a892098564c734f23f245ac24007e7bb77))


### Features

* Stream requests for specified endpoints ([#178](https://github.com/doublewordai/fusillade/issues/178)) ([a25be4c](https://github.com/doublewordai/fusillade/commit/a25be4cc7fbfd753968237dc769d14d125ced8b4))

## [11.0.1](https://github.com/doublewordai/fusillade/compare/fusillade-v11.0.0...fusillade-v11.0.1) (2026-03-12)


### Bug Fixes

* FIFO claim ordering via batch-first LATERAL join ([#176](https://github.com/doublewordai/fusillade/issues/176)) ([0226c14](https://github.com/doublewordai/fusillade/commit/0226c1400314c92ac7608e2af89c67e15a84036f))

## [11.0.0](https://github.com/doublewordai/fusillade/compare/fusillade-v10.0.0...fusillade-v11.0.0) (2026-03-11)


### ⚠ BREAKING CHANGES

* allow filtering batches and files by multiple api key ids, so we can … ([#173](https://github.com/doublewordai/fusillade/issues/173))

### Features

* allow filtering batches and files by multiple api key ids, so we can … ([#173](https://github.com/doublewordai/fusillade/issues/173)) ([a8d2352](https://github.com/doublewordai/fusillade/commit/a8d2352f5e33af3e2974852e763b4cdc68655dea))

## [10.0.0](https://github.com/doublewordai/fusillade/compare/fusillade-v9.0.0...fusillade-v10.0.0) (2026-03-11)


### ⚠ BREAKING CHANGES

* Split timeouts ([#163](https://github.com/doublewordai/fusillade/issues/163))

### Features

* Split timeouts ([#163](https://github.com/doublewordai/fusillade/issues/163)) ([6087e9f](https://github.com/doublewordai/fusillade/commit/6087e9fdc94f27e5bc470983bac11fda781bae72))

## [9.0.0](https://github.com/doublewordai/fusillade/compare/fusillade-v8.2.0...fusillade-v9.0.0) (2026-03-11)


### ⚠ BREAKING CHANGES

* add api_key_id attribution to batches and files for org-scoped … ([#165](https://github.com/doublewordai/fusillade/issues/165))

### Features

* add api_key_id attribution to batches and files for org-scoped … ([#165](https://github.com/doublewordai/fusillade/issues/165)) ([0de6400](https://github.com/doublewordai/fusillade/commit/0de6400d8347084692316b7a24d456a04a9264c4))

## [8.2.0](https://github.com/doublewordai/fusillade/compare/fusillade-v8.1.1...fusillade-v8.2.0) (2026-03-11)


### Features

* state transition events ([#161](https://github.com/doublewordai/fusillade/issues/161)) ([165ea9f](https://github.com/doublewordai/fusillade/commit/165ea9f44e5c043c379f035e75bafcef381ac5fb))

## [8.1.1](https://github.com/doublewordai/fusillade/compare/fusillade-v8.1.0...fusillade-v8.1.1) (2026-03-11)


### Bug Fixes

* pre-filter active batches in claim query to avoid orphaned rows ([#168](https://github.com/doublewordai/fusillade/issues/168)) ([a66ab16](https://github.com/doublewordai/fusillade/commit/a66ab163bdc3149e8082c5e73c4138261e07796d))

## [8.1.0](https://github.com/doublewordai/fusillade/compare/fusillade-v8.0.0...fusillade-v8.1.0) (2026-03-11)


### Features

* single LATERAL query for all-model claim ([#166](https://github.com/doublewordai/fusillade/issues/166)) ([5f4974a](https://github.com/doublewordai/fusillade/commit/5f4974a05405d06b283fcbfc32b4b818f109169b))

## [8.0.0](https://github.com/doublewordai/fusillade/compare/fusillade-v7.2.0...fusillade-v8.0.0) (2026-03-11)


### ⚠ BREAKING CHANGES

* acquire semaphore permits before claiming requests ([#157](https://github.com/doublewordai/fusillade/issues/157))

### Features

* acquire semaphore permits before claiming requests ([#157](https://github.com/doublewordai/fusillade/issues/157)) ([a0c8d94](https://github.com/doublewordai/fusillade/commit/a0c8d94bfdaad52546197c7b1a022d45e9dd91b7))


### Bug Fixes

* move test-log to dev-dependencies ([#162](https://github.com/doublewordai/fusillade/issues/162)) ([e7322ca](https://github.com/doublewordai/fusillade/commit/e7322ca9e0ad4c67d3fad76429a43fdbf6710950))

## [7.2.0](https://github.com/doublewordai/fusillade/compare/fusillade-v7.1.0...fusillade-v7.2.0) (2026-03-09)


### Features

* add error code label to retriable HTTP status metric ([#159](https://github.com/doublewordai/fusillade/issues/159)) ([1efbd3a](https://github.com/doublewordai/fusillade/commit/1efbd3a7893d1000230b786003455395a1f58fcc))

## [7.1.0](https://github.com/doublewordai/fusillade/compare/fusillade-v7.0.2...fusillade-v7.1.0) (2026-03-03)


### Features

* bulk cancellation check query for daemon polling loop ([#149](https://github.com/doublewordai/fusillade/issues/149)) ([39da1fb](https://github.com/doublewordai/fusillade/commit/39da1fb104ea025b66ba209663a88827070425f6))

## [7.0.2](https://github.com/doublewordai/fusillade/compare/fusillade-v7.0.1...fusillade-v7.0.2) (2026-02-25)


### Bug Fixes

* when a batch is retried, clear the status from the batch as well ([#154](https://github.com/doublewordai/fusillade/issues/154)) ([647cd04](https://github.com/doublewordai/fusillade/commit/647cd04c56e17ce9b88d4d904b0a685be9d97d79))

## [7.0.1](https://github.com/doublewordai/fusillade/compare/fusillade-v7.0.0...fusillade-v7.0.1) (2026-02-24)


### Bug Fixes

* Tracing clean-up ([#152](https://github.com/doublewordai/fusillade/issues/152)) ([2d5855d](https://github.com/doublewordai/fusillade/commit/2d5855dbcb31765798d0b02198a14f7eb452f9df))

## [7.0.0](https://github.com/doublewordai/fusillade/compare/fusillade-v6.0.0...fusillade-v7.0.0) (2026-02-19)


### ⚠ BREAKING CHANGES

* parameterise get pending counts  ([#150](https://github.com/doublewordai/fusillade/issues/150))

### Features

* parameterise get pending counts  ([#150](https://github.com/doublewordai/fusillade/issues/150)) ([a2c6f28](https://github.com/doublewordai/fusillade/commit/a2c6f28610310084b6ac4ccbe7350b9dff44e7f5))

## [6.0.0](https://github.com/doublewordai/fusillade/compare/fusillade-v5.5.0...fusillade-v6.0.0) (2026-02-17)


### ⚠ BREAKING CHANGES

* remove hiding of retriable errors ([#130](https://github.com/doublewordai/fusillade/issues/130))

### Features

* remove hiding of retriable errors ([#130](https://github.com/doublewordai/fusillade/issues/130)) ([1717596](https://github.com/doublewordai/fusillade/commit/17175964f98d5e2dd24bb7cfe912b3246c74fc27))

## [5.5.0](https://github.com/doublewordai/fusillade/compare/fusillade-v5.4.4...fusillade-v5.5.0) (2026-02-17)


### Features

* add comprehensive daemon loop metrics ([#144](https://github.com/doublewordai/fusillade/issues/144)) ([efd0e65](https://github.com/doublewordai/fusillade/commit/efd0e65ba4470f6db1d322b0062f09d023111d37))
* add Timeout variant to FailureReason ([#146](https://github.com/doublewordai/fusillade/issues/146)) ([03e3f9c](https://github.com/doublewordai/fusillade/commit/03e3f9cb3e3d82a3bed72800b7890709c396983f))

## [5.4.4](https://github.com/doublewordai/fusillade/compare/fusillade-v5.4.3...fusillade-v5.4.4) (2026-02-16)


### Bug Fixes

* optimise deletion (again) ([#142](https://github.com/doublewordai/fusillade/issues/142)) ([35089c7](https://github.com/doublewordai/fusillade/commit/35089c7efed12d67ebac43c746e64342661712ed))

## [5.4.3](https://github.com/doublewordai/fusillade/compare/fusillade-v5.4.2...fusillade-v5.4.3) (2026-02-16)


### Bug Fixes

* consolidate batch request traces into single trace trees ([#140](https://github.com/doublewordai/fusillade/issues/140)) ([6d5a197](https://github.com/doublewordai/fusillade/commit/6d5a197a759d1cd5a326bbf15bdf8db485dca87c))

## [5.4.2](https://github.com/doublewordai/fusillade/compare/fusillade-v5.4.1...fusillade-v5.4.2) (2026-02-16)


### Bug Fixes

* avoid full scans and ensure safe concurrency in purge queries ([#138](https://github.com/doublewordai/fusillade/issues/138)) ([3c41a15](https://github.com/doublewordai/fusillade/commit/3c41a15bb7aa7d452e59b41eccc5a4439719bacd))

## [5.4.1](https://github.com/doublewordai/fusillade/compare/fusillade-v5.4.0...fusillade-v5.4.1) (2026-02-13)


### Bug Fixes

* reclaim requests from dead or stale-heartbeat daemons ([#136](https://github.com/doublewordai/fusillade/issues/136)) ([53c0391](https://github.com/doublewordai/fusillade/commit/53c0391591d74909f550b620003477e597c3099f))

## [5.4.0](https://github.com/doublewordai/fusillade/compare/fusillade-v5.3.1...fusillade-v5.4.0) (2026-02-13)


### Features

* make concurrency limit per-daemon instead of global ([#134](https://github.com/doublewordai/fusillade/issues/134)) ([2f4f66d](https://github.com/doublewordai/fusillade/commit/2f4f66dfd62e1eed1576002623d3233996ae9dbc))


### Bug Fixes

* **deps:** update rust crate rand to 0.10 ([#124](https://github.com/doublewordai/fusillade/issues/124)) ([4e877c0](https://github.com/doublewordai/fusillade/commit/4e877c024cad71035504ab5dcefb1423d826f4c6))

## [5.3.1](https://github.com/doublewordai/fusillade/compare/fusillade-v5.3.0...fusillade-v5.3.1) (2026-02-12)


### Bug Fixes

* Purge query optimisation ([#132](https://github.com/doublewordai/fusillade/issues/132)) ([76f256b](https://github.com/doublewordai/fusillade/commit/76f256b275bf5a3b142b752db2a2e2bce8450c07))

## [5.3.0](https://github.com/doublewordai/fusillade/compare/fusillade-v5.2.1...fusillade-v5.3.0) (2026-02-11)


### Features

* periodic purge of orphaned request_templates and requests ([#127](https://github.com/doublewordai/fusillade/issues/127)) ([4ecda5f](https://github.com/doublewordai/fusillade/commit/4ecda5f6b8bddd05a613d2b0d92898ffcfd37a0e))

## [5.2.1](https://github.com/doublewordai/fusillade/compare/fusillade-v5.2.0...fusillade-v5.2.1) (2026-02-10)


### Bug Fixes

* ensure we don't notify on existing batches ([#128](https://github.com/doublewordai/fusillade/issues/128)) ([33419a5](https://github.com/doublewordai/fusillade/commit/33419a558f0bef81bd269f7ebb562ff7fd93df10))

## [5.2.0](https://github.com/doublewordai/fusillade/compare/fusillade-v5.1.0...fusillade-v5.2.0) (2026-02-09)


### Features

* Track when batches have been actioned/accepted as completed by the upstream. ([#120](https://github.com/doublewordai/fusillade/issues/120)) ([1cc6a14](https://github.com/doublewordai/fusillade/commit/1cc6a146ac9d9c6575530903acddac2f0d225973))

## [5.1.0](https://github.com/doublewordai/fusillade/compare/fusillade-v5.0.0...fusillade-v5.1.0) (2026-02-09)


### Features

* add metric to track requests completing after SLA expiry ([#122](https://github.com/doublewordai/fusillade/issues/122)) ([b634064](https://github.com/doublewordai/fusillade/commit/b6340645925106feaac7f8c78d39d5c3e0782e8b))

## [5.0.0](https://github.com/doublewordai/fusillade/compare/fusillade-v4.1.0...fusillade-v5.0.0) (2026-02-02)


### ⚠ BREAKING CHANGES

* add method to get pending request counts by model and completion window ([#111](https://github.com/doublewordai/fusillade/issues/111))

### Features

* add method to get pending request counts by model and completion window ([#111](https://github.com/doublewordai/fusillade/issues/111)) ([a05018c](https://github.com/doublewordai/fusillade/commit/a05018c8441249bbe3321171563bea11d2ca5ab9))

## [4.1.0](https://github.com/doublewordai/fusillade/compare/fusillade-v4.0.0...fusillade-v4.1.0) (2026-02-02)


### Features

* standardised tracing names ([a5334d0](https://github.com/doublewordai/fusillade/commit/a5334d0e97776897e48c0e7fff65c9be2700ec45))

## [4.0.0](https://github.com/doublewordai/fusillade/compare/fusillade-v3.0.1...fusillade-v4.0.0) (2026-01-29)


### ⚠ BREAKING CHANGES

* Boolean parameter (`hide_retriable_before_sla`) across all Storage trait methods to ignore retryable errors before sla completion.

### Features

* add error filtering into fusillade batch methods ([#102](https://github.com/doublewordai/fusillade/issues/102)) ([ec454ba](https://github.com/doublewordai/fusillade/commit/ec454baaf41433e08689d125dc60dbee4b10b2c0))

## [3.0.1](https://github.com/doublewordai/fusillade/compare/fusillade-v3.0.0...fusillade-v3.0.1) (2026-01-28)


### Bug Fixes

* add limit to stale request unclaim query ([#105](https://github.com/doublewordai/fusillade/issues/105)) ([b03c0f4](https://github.com/doublewordai/fusillade/commit/b03c0f4bed5169ed79110c5d523f6a282cbfb43a))
* update model field in request body when escalating ([#108](https://github.com/doublewordai/fusillade/issues/108)) ([7527537](https://github.com/doublewordai/fusillade/commit/75275373815892177ec615d1a4838a69cc91fd9a))

## [3.0.0](https://github.com/doublewordai/fusillade/compare/fusillade-v2.5.0...fusillade-v3.0.0) (2026-01-28)


### ⚠ BREAKING CHANGES

* Removes the following from the public API:
    - SlaThreshold struct and related types
    - Superseded request state
    - Escalation-related fields on RequestData (is_escalated,
      escalated_from_request_id, superseded_at, superseded_by_request_id)
    - Manager trait methods: find_pending_escalation, get_at_risk_batches,
      get_missed_sla_batches, create_escalated_requests
    - DaemonConfig fields: sla_check_interval_seconds, sla_thresholds

### Features

* replace escalation racing with route-at-claim-time ([#98](https://github.com/doublewordai/fusillade/issues/98)) ([8fa21ea](https://github.com/doublewordai/fusillade/commit/8fa21ea313ccfd50a43b592678265966b06279bf))

## [2.5.0](https://github.com/doublewordai/fusillade/compare/fusillade-v2.4.0...fusillade-v2.5.0) (2026-01-28)


### Features

* implement soft deletes for files and batches ([#101](https://github.com/doublewordai/fusillade/issues/101)) ([37f4726](https://github.com/doublewordai/fusillade/commit/37f47265cc7a17aac8d092c913217d64a6044f2b))

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
