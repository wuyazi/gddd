
## 执行顺序

```
|              Command              |
|       Repository      | Aggregate |
|            | EventBus |
|       EventStore      |
```

## Command（命令）

命令

## Aggregate（聚合）

用于描述业务，并产生事件
定义实体，和事件，所有实体更改只能通过事件

## Repository（仓库）

用于存储数据的接口

## EventBus（事件总线）

用于传输事件

## EventStore（事件存储）

用于存储事件，并可回溯（EventSourcing）

## TODO

- [ ] 事件消费
- [ ] 事件归档：将每日的事件归档
- [ ] 历史事件溯源
- [ ] go-zero 适配
- [ ] 唯一索引：创建用户时，邮箱唯一
- [ ] 文档
