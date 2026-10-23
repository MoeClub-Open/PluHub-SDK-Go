# PluHub Go SDK

Go 1.27，JSON-RPC 2.0 标准输入/输出协议 v1。逐 Chunk 数据不经过控制协议。

~~~sh
go test ./...
go build -o examples/echo/dist/echo ./examples/echo
~~~

将 examples/echo 作为插件目录安装，其 Manifest 声明 Linux 平台。SDK 支持初始化、能力调用、Host 反向调用、取消、健康检查、排空和停止。Handler 必须响应 context.Context 取消。

Handler 通过 `RequestConfig(ctx)` 获取当前调用的配置快照，返回值为独立的 `json.RawMessage` 副本。配置热切换后，在途调用仍使用原快照。

调用 `Host.Call(ctx, ...)` 或 `Host.Secret(ctx, ...)` 时，应传入 Handler 收到的 `ctx` 或从它派生的 Context。SDK 会携带调用的 `contextId`，宿主据此恢复请求的配置与能力绑定作用域；不要用 `context.Background()` 替换原请求上下文。调用结束后，该作用域失效。

共享样例来自同级 PluHub-Spec/fixtures。Linux CI 执行 go vet 和竞态检测。

许可证：Apache-2.0。

宿主 `0.1.0-alpha.15` 起，`Host.Providers(ctx, capability)` 返回请求快照内可用提供商的 ID、名称及版本；`Host.CallProvider(ctx, capability, providerID, input, result)` 显式选择实现。两者仍要求 Manifest 声明能力依赖，并保留请求上下文和取消信号。
