# Fleet Monitor

四台主机的只读监控服务，Go 后端配合 Python 采集器和 React 页面；包含主机选择、趋势、事件及独立数据盘读数。

**此公开版本经过脱敏，不可直接用于现有生产环境。** `internal/fleet/types.go` 的地址、用户和主机角色为示例，`internal/fleet/seed.json` 是虚构占位清单，`collector.py` 的服务名单是示例；部署文件中的账户及密钥路径也是占位值。使用前必须按目标环境审查并配置，并以隔离环境完成验证。不要提交真实密钥、资产清单或生产运行记录。

## 构建与检查

需要 Go、Node.js 和 npm：

```sh
cd web
npm ci
npm run build
cd ..
go test ./...
go vet ./...
go build .
```

前端构建结果通过 Go embed 编入二进制。
