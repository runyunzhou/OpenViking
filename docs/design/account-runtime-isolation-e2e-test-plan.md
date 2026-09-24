# Account Runtime Isolation End-to-End Test Plan

## 1. 测试目标

验证当前分支 feat/account-runtime-isolation 的真实端到端行为，重点覆盖请求身份、Embedding/VLM 隔离、VectorDB 隔离、运行时配置语义、Token 统计和 worktree 运行隔离。

测试原则：先用确定性本地代理验证 OpenViking 自身逻辑，再切换真实模型服务验证 Provider 兼容性；所有运行数据放在仓库外，不使用当前 checkout 的 ./mydata，不修改仓库根目录 ov.conf。

## 2. 测试拓扑

```text
测试客户端
    |
    | API Key
    v
OpenViking 当前分支实例 :1934
    |                         \
    | Embedding / VLM           \ VectorDB
    v                           v
Model Proxy :1940            VectorDB Mock :1941
    |
    | forwarding 模式
    v
真实模型服务
```

建议运行目录：/private/tmp/ov-e2e/{config,workspace,proxy,vectordb,logs}。

### 2.1 可执行测试工具

仓库内提供以下配套工具：

- `tests/e2e/account_runtime_isolation/model_proxy.py`：确定性 OpenAI-compatible
  Embedding/VLM 代理，支持请求 ledger、Account/model 校验和故障注入。
- `tests/e2e/account_runtime_isolation/vectordb_mock.py`：使用真实 HTTP VectorDB
  协议和本地存储实现的 Mock，支持请求 ledger 和故障注入。
- `tests/e2e/account_runtime_isolation/generate_config.py`：生成独立 `ov.conf` 及
  `account_a`、`account_b` 的创建期 settings。
- `tests/e2e/account_runtime_isolation/run_e2e.py`：生成配置、启动三个服务、运行
  smoke 并回收进程的一键入口。

一键运行确定性 smoke：

```bash
.venv/bin/python tests/e2e/account_runtime_isolation/run_e2e.py
```

执行结果写入 `/private/tmp/ov-e2e/report/results.json`，其中包含逐 Case
描述、预期、状态和调用证据。

每个 Case 执行前会清空 Model Proxy 和 VectorDB ledger，并用唯一
`E2E_TRACE_*` marker 关联业务请求和模型调用；模型 ledger 逐条校验
`account_id -> operation -> model`，VectorDB ledger 逐条校验
`collection -> project`。Proxy 自身的 Account/model 校验由独立 pytest 覆盖。

需要分别调试各服务时，可手动运行：

```bash
.venv/bin/python tests/e2e/account_runtime_isolation/generate_config.py
.venv/bin/python tests/e2e/account_runtime_isolation/model_proxy.py \
  --port 1940 --mode deterministic --dimension 8
.venv/bin/python tests/e2e/account_runtime_isolation/vectordb_mock.py \
  --port 1941 --persist-path /private/tmp/ov-e2e/vectordb
.venv/bin/openviking-server \
  --config /private/tmp/ov-e2e/config/ov.conf --port 1934 --workers 1
.venv/bin/python tests/e2e/account_runtime_isolation/smoke.py
```

`smoke.py` 创建 `account_a/account_b`，执行身份伪造、单租户、交错、并发、
文本/图片/视频资源导入、异步任务、Session commit、reindex、OVPack recompute、
Embedding/VLM 失败重试、usage、配置读取和 create-only PATCH
场景，并保存每个 Case 的预期、实际证据和结果。每次执行前应使用全新的
`/private/tmp/ov-e2e` 目录。

代理控制接口：

```text
GET  /health
GET  /proxy/requests | /mock/requests
GET  /proxy/usage    | /mock/state
POST /proxy/reset    | /mock/reset
POST /proxy/faults   | /mock/faults
```

### 2.2 白盒入口覆盖矩阵

| 外部入口 | 关键内部链路 | 下游断言 | E2E Case |
| --- | --- | --- | --- |
| `POST /search/search` | context assembler -> query embedding / rewrite | VLM + Embedding + VectorDB search | `MODEL-001..004` |
| `POST /resources/temp_upload` + `/resources` | `AddResourceMsg` -> semantic worker -> embedding worker | 文本 VLM + Embedding + VectorDB upsert | `INGEST-TEXT-001/002` |
| 图片资源导入 | image parser -> account-bound vision completion -> semantic/vector queues | image modality VLM + Embedding + VectorDB | `INGEST-IMAGE-001/002` |
| 视频资源导入 | media capability check -> semantic/vector queues | 无不支持的 video payload；伴随文本 VLM + Embedding + VectorDB | `INGEST-VIDEO-001/002` |
| 异步资源导入 + `/tasks/{id}` | 持久化 `AddResourceMsg.account_id` -> 后台 worker context | A/B 并发任务全链路隔离 | `INGEST-ASYNC-001/002` |
| `POST /sessions/{id}/commit` | `SessionCommitMsg` -> Phase-2 extractor -> memory search/write | VLM + Embedding + VectorDB | `SESSION-001/002` |
| `POST /content/reindex` | owner ctx -> account VLM resolver / vector config resolver | VLM + Embedding + VectorDB | `REINDEX-001/002` |
| `POST /pack/export` + `/pack/import` | vector snapshot config -> recompute queue | Account A Embedding + VectorDB only | `PACK-001` |
| `PATCH /admin/accounts/{id}/configuration` | runtime config manager -> provider retirement/publication | 新旧 credential、并发 PATCH、create-only | `CFG-*` |

Session training components（rollout、gradient、policy optimizer、trajectory analyzer）没有稳定的
公开在线 API 入口，本轮由实现层测试覆盖 resolver 传播，不将其伪装成 HTTP E2E。

## 3. 测试数据

Account：account_a、account_b。

用户：account_a/alice、account_a/bob、account_b/carol、account_b/dave。

```text
A_ONLY_MARKER: tenant-a-secret-001
B_ONLY_MARKER: tenant-b-secret-001
SHARED_MARKER: same-content-for-both-tenants
```

A 只能检索 A_ONLY_MARKER，B 只能检索 B_ONLY_MARKER；SHARED_MARKER 在两个 Account 中都写入，用于检查相同内容是否覆盖。

## 4. 本地模型代理

### Deterministic 模式

提供 POST /v1/embeddings、POST /v1/chat/completions、GET /proxy/requests、GET /proxy/usage。代理记录 request_id、operation、Account header、model、prompt_tokens、completion_tokens、total_tokens、retry_attempt 和结果。

代理必须校验：

- Account header 存在且有效；
- header Account 与预期一致；
- model 属于该 Account；
- A 的 model 不得携带 B 的 header，反之亦然；
- 不得出现缺少 Account header 的模型请求。

Embedding 返回固定维度向量；VLM 返回固定内容和 usage，例如 prompt=20、completion=8、total=28。

### Forwarding 模式

Deterministic 测试全部通过后，代理读取测试配置中的真实 VLM/Embedding provider 并转发请求，同时保留 Account header 和 ledger。真实模式每个 Account 只执行一次 Embedding、一次 VLM、一次检索。

## 5. 执行阶段

### Phase 0：环境确认

记录：

```text
git branch --show-current
git rev-parse HEAD
.venv/bin/python --version
.venv/bin/python -c 'import openviking; print(openviking.__file__)'
.venv/bin/python -c 'import openviking_cli; print(openviking_cli.__file__)'
```

确认 Python >= 3.10；import 路径属于当前 worktree；使用单 worker；workspace、日志、Usage/Audit 数据库在仓库外；1934/1940/1941 未占用。

### Phase 1：启动真实 OV

1. 生成独立绝对路径的测试 ov.conf。
2. 配置独立 workspace 和 Usage/Audit SQLite。
3. 使用当前分支的 .venv，以 workers=1、端口 1934 启动。
4. 检查 /health、/api/v1/system/status、启动日志和数据库。
5. 停止并重启，再次执行 health 和基础查询。

通过标准：healthy=true；auth mode 正确；无 SQLite/vector lock 异常；无遗留进程；数据没有落到仓库 mydata。

### Phase 2：认证和 Account 身份

用 ROOT key 创建两个 Account 和用户 API key。API key 只在内存变量或受控测试环境中使用。

| ID | 测试 | 预期 |
| --- | --- | --- |
| AUTH-001 | alice key 访问 FS | 身份为 account_a |
| AUTH-002 | carol key 访问 FS | 身份为 account_b |
| AUTH-003 | alice key 携带 X-OpenViking-Account: account_b | 仍为 account_a |
| AUTH-004 | carol key 携带 X-OpenViking-Account: account_a | 仍为 account_b |
| AUTH-005 | ROOT key 查询 Account 管理接口 | 成功 |
| AUTH-006 | account_a 用户访问 account_b 管理接口 | 401/403 |
| AUTH-007 | 非法或缺失 API key | 认证失败 |

### Phase 3：模型请求 Account 隔离

为两个 Account 配置不同的 model 和 extra_headers：

```text
account_a: test-embedding-a / test-vlm-a / X-OV-Test-Account=account_a
account_b: test-embedding-b / test-vlm-b / X-OV-Test-Account=account_b
```

两个 Account 的 api_base 指向 Model Proxy。

| ID | 测试 | 预期 |
| --- | --- | --- |
| MODEL-001 | A 写入内容 | 代理看到 A model/header |
| MODEL-002 | B 写入内容 | 代理看到 B model/header |
| MODEL-003 | A/B 顺序交替写入 | 无 header 串租户 |
| MODEL-004 | A/B 并发写入 | 无异步上下文串租户 |
| MODEL-005 | A/B 查询 | query embedding 使用各自配置 |
| MODEL-006 | A/B 使用相同文本 | 请求仍保持不同 Account 信息 |

### Phase 4：Embedding 隔离

| ID | 测试 | 预期 |
| --- | --- | --- |
| EMB-001 | A/B 首次调用 | 创建独立 resource 和 tracker |
| EMB-002 | A 连续调用 | 可缓存 client，但不复用 B |
| EMB-003 | 修改 A credential/header | A 旧 resource retire，B 不变 |
| EMB-004 | 代理返回错误维度 | 不写入 VectorDB |
| EMB-005 | 首次 5xx、随后成功 | 重试和 Token 统计符合定义 |
| EMB-006 | A 超时或失败 | B 不受影响 |
| EMB-007 | A/B 并发查询 | tracker、cache 不串租户 |

### Phase 5：VLM 隔离

| ID | 测试 | 预期 |
| --- | --- | --- |
| VLM-001 | A/B 分别调用 VLM | model/header 各自正确 |
| VLM-002 | A/B 并发 VLM | 无 wrapper 或上下文串租户 |
| VLM-003 | 修改 A credential | A 资源失效，B 不变 |
| VLM-004 | A primary 失败切 backup | 只在 A 配置内切换 |
| VLM-005 | A backup 失败 | 不切换到 B credential |
| VLM-006 | 查询 A/B usage | 各自只包含对应 Account |
| VLM-007 | malformed response 或缺失 usage | 错误和估算语义符合实现 |

### Phase 6：VectorDB 隔离

#### 6.1 本地共享 backend

| ID | 测试 | 预期 |
| --- | --- | --- |
| VEC-001 | A 写入 A_ONLY_MARKER | A 可以检索 |
| VEC-002 | B 写入 B_ONLY_MARKER | B 可以检索 |
| VEC-003 | A 查询 B marker | 空结果或 ACL 拒绝 |
| VEC-004 | B 查询 A marker | 空结果或 ACL 拒绝 |
| VEC-005 | A/B 写入相同文本 | 两条 Account 记录独立存在 |
| VEC-006 | 删除 A 数据 | B 数据不受影响 |
| VEC-007 | A/B 并发写入 | 不覆盖、不串结果 |
| VEC-008 | 重启后查询 | Account 维度仍保留 |
| VEC-009 | 删除 A Account | A 资源释放，B 不受影响 |

#### 6.2 Account-owned remote backend

使用两个 VectorDB Mock：account_a -> collection_a，account_b -> collection_b。Mock 记录 collection、index、upsert、search、delete。

| ID | 测试 | 预期 |
| --- | --- | --- |
| VEC-D-001 | A/B 初始化 backend | 只连接各自 collection |
| VEC-D-002 | A/B 写入 | 只出现在各自 collection |
| VEC-D-003 | A 查询 | 不访问 B collection |
| VEC-D-004 | PATCH 修改 A VectorDB 配置 | create-only 校验拒绝，A/B backend 均不变 |
| VEC-D-005 | 创建 Account 时 dimension 不匹配 | 创建失败，不创建 Account 或 backend |
| VEC-D-006 | 删除 A Account | 不删除 B collection |

Deterministic Mock 通过后，再将同一用例替换为真实 VikingDB 测试。

### Phase 7：运行时配置读取和变更

接口：GET/PATCH /api/v1/admin/accounts/{account_id}/configuration。

PATCH body：

```json
{"settings": {"embedding": {}, "vectordb": {}, "vlm": {}}}
```

验证继承顺序：Account Override > Cluster Runtime Override > ov.conf > Program Defaults。

| ID | 测试 | 预期 |
| --- | --- | --- |
| CFG-001 | 获取未配置 Account | 只返回该 Account 的显式配置 |
| CFG-002 | A 配置 Embedding | A 能读到，B 读不到 |
| CFG-003 | A 配置 VectorDB | B 不继承 A 连接信息 |
| CFG-004 | 无 override 的 B 修改 Cluster 后读取 | B 使用新 Cluster 配置 |
| CFG-005 | A 有 override 后修改 Cluster | A 保持自己的 override |
| CFG-006 | PATCH 未出现字段 | 字段保持不变 |
| CFG-007 | PATCH null | 按三态 PATCH 清除 override |
| CFG-008 | 空 PATCH | 不误清空全部配置 |
| CFG-009 | dimension 不一致 | 整体失败，不产生半更新 |
| CFG-010 | 修改 create-only 字段 | 被拒绝 |
| CFG-011 | A PATCH 期间 B 请求 | B 不读到 A 临时配置 |
| CFG-012 | 配置变更后首次请求 | 使用新 resource/header/backend |
| CFG-013 | 重启后读取 | 配置持久化且正确加载 |
| CFG-014 | 对动态字段 PATCH null 清除 override | A cache/resource 释放；create-only VectorDB 不可清除 |

#### 7.1 同一 Account 的并发配置修改

该专项必须单独执行，不能只用 A/B 两个 Account 并发请求代替。由于 embedding、vectordb、vlm 属于 ROOT-only 配置，所有请求针对同一个 Account（例如 account_a），由多个并发 ROOT 客户端同时 PATCH；不要使用 alice/bob 等普通 Account 用户直接执行这些 PATCH。

| ID | 测试 | 预期 |
| --- | --- | --- |
| CFG-CON-001 | 两个 ROOT 客户端同时修改 account_a 的同一动态字段 | 按实现定义串行化；最终值必须是某个完整 PATCH 的结果，不能出现字段撕裂或丢失更新 |
| CFG-CON-002 | 两个请求同时修改 account_a 的不同字段 | 两个合法修改都保留；不应发生后写覆盖先写导致的无关字段丢失 |
| CFG-CON-003 | 多个 ROOT 客户端同时替换 account_a 的 embedding credential header、api_key 和 provider binding | 每次候选配置都完整校验；credential 数组作为完整单元发布，不得发布半套 credential |
| CFG-CON-004 | 一个请求修改 account_a 的 embedding，另一个请求同时读取 configuration | GET 只能看到旧配置或完整新配置，不能看到中间状态 |
| CFG-CON-005 | account_a 正在 PATCH 时发起 Embedding/VLM 请求 | 模型请求只能使用旧 resource 或完整新 resource，不得使用混合配置 |
| CFG-CON-006 | account_a 正在 PATCH 时发起 VectorDB 请求 | backend 只能绑定旧配置或完整新配置，不能使用旧连接加新 collection |
| CFG-CON-007 | 同一字段并发 PATCH，其中一个请求校验失败 | 失败请求不得影响成功请求，也不得留下部分持久化数据 |
| CFG-CON-008 | 同一字段并发 PATCH，其中一个客户端超时后重试 | 检查幂等语义、最终配置和资源失效次数，不得产生无法解释的重复发布 |
| CFG-CON-009 | 并发 PATCH 与 account_a 配置 refresh 同时发生 | refresh 不得回退已经成功发布的配置 |
| CFG-CON-010 | 并发 PATCH account_a 与 account_b | account_a 的修改不进入 account_b，account_b 的修改不进入 account_a |

每个并发用例至少保存：并发请求开始顺序、服务端接收顺序、请求 ID、PATCH body、HTTP 响应、最终 GET 配置、配置 revision（如果接口可见）、模型代理请求记录和资源失效记录。

重点断言：

1. 同一 Account 的 read/merge/write 必须按照 scope 串行化。
2. 最终持久化文档必须是合法完整文档，不得出现两个 PATCH 深层字段互相覆盖造成的半配置。
3. 配置发布、consumer 通知和资源 retire 的顺序符合当前实现，不得让新请求拿到已经失效的旧 resource。
4. 失败 PATCH 不得改变成功 PATCH 的结果，也不得污染另一个 Account。
5. 普通 Account 用户尝试修改这些 ROOT-only 配置时必须被拒绝；授权失败不能改变并发 PATCH 的最终结果。

### Phase 8：Token 统计对账

每次模型调用记录 account_id、user_id、operation、provider、model、prompt_tokens、completion_tokens、total_tokens、request_id、retry_attempt、success/failure。

对账关系：Model Proxy ledger = Account provider tracker = Usage/Audit SQLite 或 console 查询结果。

| ID | 测试 | 预期 |
| --- | --- | --- |
| TOK-001 | A/B 单次 Embedding | 只增加各自 input token |
| TOK-002 | A/B 单次 VLM | 只增加各自 input/output |
| TOK-003 | A/B 交错和并发调用 | 汇总不串 Account/user |
| TOK-004 | 固定 Embedding/VLM usage | OV 等于代理 usage |
| TOK-005 | 模型重试 | 统计符合当前重试定义 |
| TOK-006 | 模型失败或 usage 缺失 | 统计符合失败/估算语义 |
| TOK-007 | A 修改模型后继续调用 | 新旧 model usage 分开 |
| TOK-008 | ROOT 查询 console | 可查看 Account 汇总 |
| TOK-009 | 普通用户查询 console | 不能查看其他 Account |

查询：GET /api/v1/console/tokens、/dashboard/summary、/audit。

### Phase 9：并发和异常

执行：A/B 同时首次初始化资源；同一 Account 的并发配置修改；配置 PATCH 与模型调用并发；代理延迟、500、超时；A 取消而 B 继续；大量交错写入；重启时存在未完成 queue；Account 删除与后台任务同时发生；配置 refresh 与请求并发。

同一 Account 并发配置修改至少覆盖 CFG-CON-001 到 CFG-CON-010，尤其要验证同一 scope 的 read/merge/write 是否串行化，以及动态配置发布期间请求是否只能看到完整的旧配置或完整的新配置。

通过标准：无 header、model、VectorDB、Token tracker、错误状态和 circuit breaker 的跨 Account 污染。

### Phase 10：真实模型转发

在 Phase 3-8 全部通过后：切换 forwarding 模式；A/B 各执行一次 Embedding、VLM、检索；对比代理 ledger、真实 response usage、OV tracker 和 Usage/Audit。通过标准是请求成功、Account header 保留、usage 可提取、A/B 统计不串、无意外重复计费。

## 6. Worktree 对本地测试进程的影响

这一节是运行原理说明，不是线上部署方案，也不是必须单独执行的业务测试。

本地 OV 进程启动后，已经导入的 Python 模块代码位于该进程内存中。此时在另一个 worktree 修改 Python 源文件，通常不会改变已经运行的测试进程；测试进程也不会因为另一个 worktree 的文件变化而自动重载代码，当前启动方式没有开启 hot reload。

真正可能产生影响的只有以下几类本地操作：

1. 重启测试进程。重启时如果使用了另一个 worktree 的 Python 环境或 editable install，加载的可能就不是原来测试的代码。
2. 运行时重新读取外部文件。如果代码路径、模板、配置或其他资源是在每次请求时从文件读取，修改共享文件可能影响后续请求；Python 模块本身不属于这种情况。
3. 共享运行资源。如果开发中的另一个 OV 实例和测试实例共用 workspace、端口、配置文件或本地数据库，两个实例会互相影响。这是多实例资源冲突，不是 worktree 修改自动影响已运行进程。

因此本次本地测试只需要记录测试进程的 branch、commit、Python executable、import 路径、配置路径和 workspace 路径。测试进程保持运行期间，用户可以在另一个 worktree 开发代码；如果要重启测试进程，再重新确认这些路径即可。

## 7. 最终验收标准

- 身份：API key 解析出的 account_id 是最终身份，伪造 X-OpenViking-Account 无法切换租户。
- Embedding/VLM：A/B 只使用各自 model、credential、extra_headers、resource、cache、tracker。
- VectorDB：A/B 不能检索对方专属内容，相同内容不覆盖，backend 变更不影响另一方。
- 配置：GET 只返回对应 Account 显式配置；PATCH 遵守三态 merge；create-only 不可修改；失败不半更新。
- Token：代理 ledger、Account tracker、Usage/Audit 结果可对账，Account/user/model 维度不串。
- Worktree：运行中服务不受其他 worktree 影响，独立 venv/config/workspace/port 时互不影响。

## 8. 推荐执行顺序

```text
1. Phase 0：branch、commit、Python、import 路径
2. Phase 1：独立 OV health/status
3. Phase 2：Account/user/API key 身份测试
4. Phase 3：deterministic Model Proxy
5. Phase 4：Embedding 隔离
6. Phase 5：VLM 隔离
7. Phase 6：本地和 remote VectorDB 隔离
8. Phase 7：配置读取和 PATCH
9. Phase 8：Token 三方对账
10. Phase 9：并发、失败、超时和删除
11. Phase 10：真实模型 forwarding
12. 记录并确认本地测试进程运行路径
```

建议第一轮执行到 Phase 8；确定性代理、配置变更和 Token 对账通过后再进入真实模型和真实 VectorDB。

## 9. 测试结果记录

```text
case_id:
status: PASS | FAIL | BLOCKED
branch:
commit:
environment:
account_id:
user_id:
request_id:
proxy_request_ids:
expected:
actual:
evidence:
failure_category: code | provider | vectordb | environment | test-data
```

失败时保留 request_id、代理 ledger、OV 日志和 workspace 内 Usage/Audit 数据库路径，用于区分代码、Provider、VectorDB、环境和测试数据问题。

## 10. 2026-09-24 执行结果

执行环境：`feat/account-runtime-isolation`，HEAD `9101e64e`，Python 3.13.2，单 worker，
独立 workspace `/private/tmp/ov-e2e`。

| 测试层 | 结果 | 说明 |
| --- | --- | --- |
| 确定性三进程 E2E | 35 passed, 0 failed | OpenViking + Model Proxy + VectorDB Mock |
| 功能相关实现层门禁 | 411 passed, 0 failed | 13 条无关测试 deselected |
| Mock 协议自测 | 8 passed, 0 failed | OpenAI 兼容协议、modality、memory extraction、fault、ledger、VectorDB aggregate/坐标 |
| 扩展回归 | 424 passed, 0 failed | 已修正 6 条过期的模板渲染断言及 1 条主线遗留的 account status 断言 |
| 分支变更文件全量回归 | 1633 passed, 19 failed | 1652 条；19 条单独重跑仍失败 |
| 真实外部服务 | BLOCKED | VLM 受 429 配额限制；VikingDB 缺少凭证和测试 collection |

已执行的 E2E 覆盖身份与权限、双向伪造 Account header、A/B 模型和 collection 路由、
同步及异步文本资源导入、图片 vision payload、视频 provider capability 分支、
Session Phase-2 memory extraction、semantic/vector reindex、OVPack vector recompute、
Embedding/VLM credential 热切换、重试、错误维度、malformed 降级、配置三态 PATCH、
失败原子性、同 Account 并发 PATCH、跨 Account 并发 PATCH 和 create-only 约束。

视频用例使用 OpenAI-compatible 测试 provider。该 provider 不声明原生 audio/video
能力，因此断言不发送 video media payload；资源导入所触发的目录摘要、reason memory
linking 等文本 VLM，以及后续 Embedding/VectorDB 调用仍必须逐条满足 Account 隔离。
原生视频理解需在 Phase 10 使用支持该能力的真实 provider 继续验证。

实现层门禁补充覆盖资源创建去重、cache key、in-flight retire、取消释放、provider close、
VectorDB backend refresh、配置文件原子写和损坏恢复、refresh 顺序、删除重试以及
Usage/Audit SQLite 和 console 权限。

扩大到当前分支涉及的全部 Python 测试文件后，稳定失败集中在 snapshot ACL（1）、
server health/lifespan（3）、可选 tau2 rollout 集成（10）、semantic stall（3）和
vector transfer（2）。这些失败不改变 35 条黑盒隔离 E2E 的结果，但在完成归因或修复前
仍属于交付阻塞项。

交付结论：**HOLD**。隔离特性确定性门禁和扩展回归均通过，但在完成真实
provider/VikingDB 验证、真实 provider 到 console 的 usage 三方对账及 in-flight
进程重启 E2E 前，不应标记为客户可交付。

逐 Case 结果保存在 `/private/tmp/ov-e2e/report/results.json`。
