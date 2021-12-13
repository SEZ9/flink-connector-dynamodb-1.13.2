# flink-connector-dynamodb-1.13.2
Flink SQL connector with AWS Dynamodb v2 SDK

基于Flink 1.13.2 Table Connector ，实现了 AWS Dynamodb 的简单SQL Connector。

#### 当前版本支持：
1. 支持定义primary key ，Table Name ，代码hard code 到 AP_SOUTHEAST_1 区域
2. 支持Source/ Sink ，source 暂时只实现了 scan 模式，效率较低，全表扫描。 Sink 端支持了实时流式append写入，支持kafka、S3等Stream/Batch 数据源写入
3. 当前仅支持 定义String 类型
#### 使用方法
```
CREATE TABLE IF NOT EXISTS test_database.test_dynamodb_table(
    field1 STRING,
    field2 STRING,
    field3 STRING
)
WITH (
 'connector' = 'dynamodb-v2',
 'table_name' = 'tableName',
 'primary_key' = 'primaryKey'
)

-- 查询
select * from test_database.test_dynamodb_table;

-- 写入
insert into 
    test_database.test_dynamodb_table 
select * from 
    test_database_source.test_source_table
```

#### todo
1. 集成lookup 查询，异步缓存
2. 支持Dynamodb 原生支持的多种字段type
3. 支持设置参数定义aws region、读写并发、 DDL 建表等功能

####
欢迎有需求的同学一起完善，交流！