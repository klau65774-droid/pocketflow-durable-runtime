# PocketFlow Durable Runtime 独立审计与测试报告

审计日期 **2026-11-26**。最终裁定 **PASS WITH WARNINGS**。七项仓库验收门禁与外部回放的五项检查均通过。保留两项适用范围记录，即 Windows 默认时钟下的原样上游测试差异，以及模拟故障和真实进程强杀之间的恢复边界。

## 审计范围与方法

审查覆盖 `backend/pocketflow_runtime/`、`backend/scripts/`、`frontend/src/`、`tests/backend/`、`benchmarks/`、两个 README 和 `docs/` 的接口、使用与评测资料。检查了节点准备、执行和回调的控制流，SQLite 写入及读取事务，审批与副作用生命周期，HTTP 输入与错误边界，OTLP/SSE 输出，React 状态和迭代键，以及外部评测的计数与判定。

验证采用源码追踪、单元回归、进程内 ASGI 调用、两连接 SQLite 竞争测试、事务失败注入、旧记录摘要与新记录完整性检查、原样上游测试和本地外部数据回放。故障用例使用临时数据库和合成输入。付费 LLM API 调用数为 **0**。

上游基线固定为 `f74d023f93607b8c3268133339a5e532a949898c`。14 个上游源文件、测试、构建元数据及许可证文件的原始字节分别匹配固定提交的 Git blob。加上本地 `UPSTREAM_META.json` 的快照共 15 个，审计前后 SHA-256 一致。GitHub 写入次数和远端配置写入次数均为 **0**。 [固定上游提交](https://github.com/The-Pocket/PocketFlow/tree/f74d023f93607b8c3268133339a5e532a949898c)

## 严重度与处理记录

位置为修复后入口或关键语句。Critical 确认项为 **0**。

| 严重度 | 位置 | 根因与实际影响 | 状态与处理 |
| --- | --- | --- | --- |
| High | `backend/pocketflow_runtime/engine.py:139`、`:276`、`:335`、`:986` | 用户补丁能够覆盖审批及执行状态。分叉预设批准，比较流程自动决策。 | **FIXED**。保护运行时字段，先校验补丁，拒绝后禁止恢复。退款前的审批分叉和比较暂停均保留显式审批。 |
| High | `backend/pocketflow_runtime/engine.py:302`、`:574`、`backend/pocketflow_runtime/storage.py:171`、`:281` | 决策先消费待审记录，再独立保存运行状态。暂停摘要、检查点和待审记录也可能部分写入。多连接可以从同一恢复状态起步。 | **FIXED**。在 SQLite 写事务中一起提交。使用状态与版本时间认领运行，失败时回滚并记录可恢复中断。 |
| High | `backend/pocketflow_runtime/storage.py:399`、`:421`、`backend/pocketflow_runtime/engine.py:676` | 幂等查找、生成结果及插入分离，竞争者可能都被报告为新执行。父子运行使用彼此独立的键空间。 | **FIXED**。写事务加唯一键给出唯一赢家。以运行谱系找回历史键和新指纹键，用账本重建模拟退款状态。 |
| High | `backend/pocketflow_runtime/storage.py:296`、`:376`、`:542`、`backend/pocketflow_runtime/engine.py:621` | 已存摘要从未验证，重复序号会覆盖历史，按检查点数量推断游标会重跑已提交步骤。步骤 2 的故障在承诺的写入之前触发。 | **FIXED**。新摘要覆盖状态、节点、动作、状态码、差异及运行标识。支持验证旧摘要。核验新写入的运行状态，从已校验的节点继续，保留真实的故障注入窗口。 |
| High | `backend/pocketflow_runtime/compat.py:58`、`:397`、`:488`、`backend/pocketflow_runtime/resilience.py:84` | 重试上下文被不同嵌套节点共用。批任务可以在调用方失败返回后继续工作。半开熔断器放行多个探测，旧结果可以覆盖新状态。 | **FIXED**。重试按节点绑定并在异常及超时后恢复，退出前取消并收齐兄弟任务。熔断器锁定准入并核对世代，每次尝试单独检查。 |
| High | `backend/pocketflow_runtime/api.py:48`、`:145`、`:236`、`backend/pocketflow_runtime/redaction.py:110`、`:169` | 任意大小的请求可在校验前被解析，默认校验可回显输入。映射键、结构化凭据、审批文字和部分长凭据能够绕过旧值扫描。 | **FIXED**。限制正文、深度、容器入口和字段类型，统一错误响应。扩展键、值与元数据脱敏，PEM 单次扫描匹配边界，保留经过校验的业务标识。 |
| High | `benchmarks/run_sealed_benchmarks.py:177`、`:416`、`:775`、`backend/pocketflow_runtime/evaluator.py:72` | τ-bench 的恢复依赖进程内变量，BFCL 可把空结果判成通过，重复试验聚合了不同题目。部分层完全失败时总体门禁仍可能通过。 | **FIXED**。回读 SQLite，逐题完成三轮，按实际输出和逐项精确轨迹评分，五项均纳入门禁及 CLI 退出码。 |
| Medium | `backend/pocketflow_runtime/compat.py:77`、`:98`、`:540`、`:634` | 舱壁限制工作中的回调却提前创建全部任务。线程卸载后，同一共享状态可由兄弟线程竞争修改。取消等待不等于线程完成。 | **FIXED**。有并发限制时使用固定数量的工作任务。含线程卸载的共享分支顺序执行。取消等待线程退出，并兑现分支参数隔离开关。 |
| Medium | `backend/pocketflow_runtime/guards.py:26`、`:99`、`:110`、`backend/pocketflow_runtime/engine.py:447` | 快照先完全展开再检查字节。失败状态可能过大，未提交步骤会被计数。震荡检测忽略小窗口配置并截断哈希。 | **FIXED**。克隆时累计 UTF-8 字节，限制深度与非有限值，只计已提交步，保存最后有效状态。使用完整摘要和精确窗口，按执行初始化跟踪器。 |
| Medium | `backend/pocketflow_runtime/config.py:13`、`:42`、`backend/pocketflow_runtime/storage.py:68`、`:542` | 目录本身可作为数据库路径。配置的默认步数未用于创建接口。数据库初始化未确认 WAL，聚合读取可能跨多个并发提交。 | **FIXED**。校验文件路径和配置范围，使用显式 CORS 来源，初始化核验 WAL 并启用 `synchronous=FULL`，详情共享一个读取事务。 |
| Medium | `backend/pocketflow_runtime/api.py:266`、`:400`、`backend/pocketflow_runtime/api_models.py:121`、`frontend/src/api.ts:16`、`docs/API_CONTRACT.md:7` | 导出只有宽泛对象，状态码、响应字段、SSE 媒体类型和前端端点清单不一致。单向路径测试无法识别多余接口。 | **FIXED**。声明 14 个操作的请求、响应与错误，导出 OpenAPI，验证运行时内容与导出相等并核对文档和前端操作。 |
| Medium | `backend/pocketflow_runtime/tracer.py:87`、`backend/pocketflow_runtime/api.py:383` | 多段运行的 OTLP 导出把恢复段的 trace ID 覆盖为第一段 ID，并把枚举编码为字符串。 | **FIXED**。保留每个 span 的 trace ID，使用 OTLP JSON 的数值枚举。 [OTLP JSON 映射](https://opentelemetry.io/docs/specs/otlp/#json-protobuf-encoding) |
| Medium | `backend/pocketflow_runtime/engine.py:986`、`frontend/src/api.ts:82`、`frontend/src/benchmarks.ts:7` | 各模式使用的故障不一致。界面可以把空成功输出标绿、把实际受保护状态标成失败，或用写死的数字遮住评测失败。 | **FIXED**。按所选故障运行并验证结果，保留 `PROTECTED` 与 `INTERRUPTED`，按指标和保护检查绘制结果，编辑输入时清除旧扫描输出。 |
| Medium | `tests/backend/test_upstream_compat.py:46` | 原包装器在基线上执行 52 个方法，在兼容层执行 54 个。未完整运行的两个排序敏感用例从通过摘要中消失。 | **FIXED**。每边强制 10 套、56 个方法、零跳过。使用真实高精度时钟的配置与原生时钟的实测结果均见下表。 |
| Low | `.gitignore`、`backend/pocketflow_runtime/api.py:193` | 本地 SQLite 文件与 WAL 边车可能进入发布文件。导入应用时启动存储也会触及本地运行库。 | **FIXED**。忽略默认数据目录中的 SQLite 文件，按应用生命周期打开和关闭存储。 |
| Low | `frontend/src/App.tsx:324`、`frontend/src/components/WorkflowGraph.tsx:59` | 复核 React 迭代产生的相邻元素键。 | **VERIFIED_OK**。9 个相关迭代均使用前缀、索引和业务标识的组合键。 |
| Low | `backend/upstream_baseline/UPSTREAM_META.json`、`benchmarks/run_sealed_benchmarks.py:817` | 核对固定上游与外部数据隔离。 | **VERIFIED_OK**。15 个基线文件快照一致，运行时的内置演示样例和脱敏样例为本地合成输入。外部数据只由基准脚本消费。 |
| Low | `backend/upstream_baseline/tests/test_async_parallel_batch_flow.py:34`、`tests/backend/test_upstream_compat.py:46` | 原样测试聚合器依赖等时任务的顺序。在 Windows Python 3.12 默认时钟配置下，两个用例两边均出现 `KeyError`。 | **INFO**。这是测试调度依赖。正常项目门禁两边共同使用真实高精度单调时钟。Windows Python 3.14 原生时钟也独立通过。 |

## 回归覆盖

| 范围 | 新增或补强的回归 |
| --- | --- |
| 执行与并发 | 嵌套重试及超时后上下文恢复，错误与取消后的兄弟任务收尾，固定任务池与输出顺序，半开竞争及过时结果，线程取消与嵌套分支共享更新，执行守卫重置。 |
| SQLite 与审批 | 四个故障点分别重新打开数据库，检查点原序号保持稳定，旧摘要读取与新动作或状态损坏阻断，同一键的双连接竞争，暂停和拒绝两处 SQL 回滚，审批中断禁止旁路，相同退款的分叉重用账本。 |
| 快照与隐私 | 按真实 UTF-8 存储字节检查上限，重复别名和非有限数字，完整摘要与精确逐出，短结构化凭据、长凭据、对象键冲突和 PEM，良性长文、订单编号、UUID、哈希和 Bearer 普通叙述。 |
| 接口与控制台 | 实时 OpenAPI 完整相等，所有方法与状态双向比对，错误包络和正文上限，CORS 预检，路径限制，多段 OTLP 和有限 SSE，空输出和故障指标界面状态，编辑后清除上次扫描结果。 |
| 基准与文档 | 轨迹近似但缺步的失败，BFCL 空输出、重复工具和缺参数情形，回放与资源门禁失败，README 表格与 JSON 的延时、计数和通过率一致。 |

## 验收门禁

| 门禁命令 | 退出码与观测结果 |
| --- | --- |
| `.venv\Scripts\ruff.exe check --config backend\pyproject.toml backend tests benchmarks` | 0，`All checks passed!` |
| `.venv\Scripts\mypy.exe --config-file backend\pyproject.toml --strict backend\pocketflow_runtime tests\backend benchmarks` | 0，`Success: no issues found in 19 source files` |
| `.venv\Scripts\pytest.exe tests\backend -v` | 0，**46 passed**，其中含两组完整上游包装用例。 |
| `npm --prefix frontend run test -- --run` | 0，**1 test file, 6 tests passed**。 |
| `.\frontend\node_modules\.bin\tsc.cmd -p frontend\tsconfig.json --noEmit` | 0，零诊断。 |
| `npm --prefix frontend run build` | 0，Vite 转换 **32** 个模块并生成生产构建。 |
| `powershell -ExecutionPolicy Bypass -File $sanitizer -ProjectDir (Get-Location).Path -DryRun` | 0，`Zero privacy, path, gitlink, or forbidden-artifact findings detected.` |

运行脚本时，`$sanitizer` 指向用户工具目录中的 `.gemini\config\skills\oss-portfolio-workbench\scripts\sanitize_and_publish.ps1`。Git 的安全目录例外仅注入到执行进程。Python 验证使用 `PYTHONDONTWRITEBYTECODE=1`。Windows 沙箱默认网络隔离会阻塞 asyncio 的本机自唤醒套接字，后端测试和基准使用已获准的本机回环环境完成。OpenAPI 通过 `.venv\Scripts\python.exe backend\scripts\export_openapi.py` 导出，并与运行时 JSON 逐字典比较。

### 上游原样方法

| 时钟与解释器 | 原版基线 | 兼容层 | 说明 |
| --- | --- | --- | --- |
| Windows Python 3.12.15，项目测试配置 | 56/56 | 56/56 | 两边采用实际高精度单调时钟，十套用例完整执行。 |
| Windows Python 3.14.3，原生时钟 | 56/56 | 56/56 | 独立运行，两边各十套、56 个方法。 |
| Windows Python 3.12.15，审计初次原生时钟运行 | 54/56 | 54/56 | 两边的 `test_multiple_batch_sizes` 与 `test_parallel_batch_flow` 均失败，记录为失败。 |

可将 `POCKETFLOW_NATIVE_TEST_CLOCK` 设置为 `1` 检查默认事件循环时钟。相同定时器截止时刻下，聚合器对顺序的假设仍由上游原样用例体现。

## 外部基准与样本范围

运行 `.venv\Scripts\python.exe benchmarks\run_sealed_benchmarks.py --dataset-dir $env:SEALED_BENCHMARK_DIR --output docs\test_metrics.json`，退出码 **0**，总耗时 **12.23 s**。五项检查均为 true。回放结果同步到两个 README 和基准接口。 [原始汇总指标](test_metrics.json)

| 样本与方法 | 基线 | 持久化运行时 |
| --- | --- | --- |
| τ-bench 的 156 个独立任务，468 次试验 | **12.82%**，Wilson 95% CI `[0.0845, 0.1897]`，`pass^3=0.1282`，重复变更 **408**，P95 **0.272 ms**，P99 **0.388 ms** | **100.0%**，Wilson 95% CI `[0.976, 1.0]`，`pass^3=1.0`，重复变更 **0**，P95 **40.306 ms**，P99 **44.472 ms** |
| BFCL v4 的 5 个文件和 824 个配对回放 | **44.66%**，Wilson 95% CI `[0.413, 0.4807]`，P95 **0.045 ms**，P99 **0.105 ms** | **100.0%**，Wilson 95% CI `[0.9954, 1.0]`，P95 **0.087 ms**，P99 **0.169 ms** |
| AgentDojo 四套任务索引驱动的 86 次混沌检查 | **0.0%**，Wilson 95% CI `[0.0, 0.0428]` | **100.0%**，Wilson 95% CI `[0.9572, 1.0]`，震荡拦截中位数 **0.05 ms** |
| 合成脱敏和资源保护 | — | **15/15** 召回，**0/18** 良性误报，1,000 项扇出峰值 **16**，循环引用和 **262,144** 字节上限检查通过 |

τ-bench 的 156 个任务含 113 个零售任务和 43 个航空任务。原始数据可提取 165 个任务，盘点的数据库记录为 3,000 条。通过率按任务首轮计算，`pass^3` 要求同一任务三轮都通过，重复变更覆盖全部三轮。持久化侧延时包含 SQLite 同步提交和重开连接的成本。延时是本次主机实测。

BFCL 的分数来自受控工具回放与路由检查。AgentDojo 的任务索引用于调度三种合成故障。报告衡量运行时组件行为，Tier 3 的召回和误报率以明示样本为分母。

## 最终裁定

**PASS WITH WARNINGS**。本轮门禁验证了模拟中断、已提交 SQLite 事务、重开数据库、审批和分叉，以及本地模拟退款的谱系内幂等。真实进程强制终止以后仍标为 `RUNNING` 的记录，当前缺少跨进程租约来区分存活执行和失效所有者。外部工具副作用与物理掉电也超出本轮实测。Windows Python 3.12 的原生时钟差异保留为兼容性警告。
