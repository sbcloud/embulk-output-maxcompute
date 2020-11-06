[English](./README.md) | 简体中文 | [日本語](./README_ja-JP.md)

# 适用于 Embulk 的 MaxCompute 写入插件

此插件用于向 MaxCompute 中写入数据

## 概览

* **Plugin type**: output
* **Load all or nothing**: no
* **Resume supported**: no
* **Cleanup supported**: yes

## 配置项

- **accessKeyId**: 阿里云账号的 Access Key ID (string, 必填项)
- **accessKeySecret**: 阿里云账号的 Access Key Secret (string, 必填项)
- **odpsUrl**: 配置 ODPS endpoint, 具体取值参考 https://www.alibabacloud.com/help/doc-detail/34951.htm ，基于不同的地区和连接方式进行选择 (string, 默认值: `"http://service.ap-northeast-1.maxcompute.aliyun.com/api"`)
- **tunnelUrl**: 配置 ODPS tunnel endpoint, 具体取值参考 https://www.alibabacloud.com/help/doc-detail/34951.htm ，基于不同的地区和连接方式进行选择 (string, 默认值: `"http://dt.ap-northeast-1.maxcompute.aliyun.com"`)
- **projectName**: ODPS 目标项目名 (string, 必填项)
- **tableName**: ODPS 目标表名，请务必在运行之前创建成功 (string, 必填项)
- **partition**: 分区信息，格式类似 'pt=20201026'，仅对分区表生效。非分区表可忽略此配置项，目标表为非分区表时，设置此配置项会导致错误。 (string, 默认值: `null`)
- **overwrite**: 设置为 true 则会在更新数据之前清除已存在的数据。 对于非分区表，将会执行 `truncate table` 来清除数据；对于分区表，则会删除 `partition` 指定的对应分区 (boolean, 默认值: `false`)
- **mappings**: 定义字段对应关系，如果 MaxCompute 表的列名和数据读取插件中的名称一一对应，可以不用额外配置 (Map of string, 默认值: `{}`)

## 数据类型
请确保 MaxCompute 表结构和数据读取插件中获取的数据结构能一一对应，不然会出现错误 `No such Columns` 。
你可以使用 `mappings` 配置项来定义对应关系，也可以通过名称直接对应。
以下是 MaxCompute 相关的数据转换

| Embulk 数据类型    |ODPS 数据类型   |
| --------         | ----- |
| Long             |bigint |
| Double           |double |
| String           |string |
| Timestamp        |datetime |
| Boolean          |bool |

## 配置文件样例

下面的配置文件将会上传数据到日本地区的分区表中。
由于默认地区为日本，因此没有必要通过 odpsUrl 和 tunnelUrl 两个配置项来定义项目地域。

```yaml
out:
  type: maxcompute
  accessKeyId: XXXXXXXXXXXXXXXXX
  accessKeySecret: XXXXXXXXXXXXXXXXX
  projectName: demo_dev
  tableName: embulk_test_partition
  partition: update_date=20201026
  overwrite: true
  mappings: {id: id, account: account, time: time, purchase: purchase, comments: comments}
```

下面的配置项将会上传数据到新加坡地区的非分区表，使用 odpsUrl 和 tunnelUrl 两个配置项来指定项目地域。
不同地区和连接方式可依据 https://www.alibabacloud.com/help/doc-detail/34951.htm 定义的 Endpoint 设定 odpsUrl 和 tunnelUrl 两个配置项。
对于非分区表来说，`partition` 这个配置项没有必要设置。
配置项中 `overwrite` 设为 false，因此不会再上传之前清空已存在的数据。
没有配置 `mappings` ，因此将默认使用名称来匹配 MaxCompute 表结构和数据读取插件传入的表结构。

```yaml
out:
  type: maxcompute
  accessKeyId: XXXXXXXXXXXXXXXXX
  accessKeySecret: XXXXXXXXXXXXXXXXX
  odpsUrl: http://service.ap-southeast-1.maxcompute.aliyun.com/api
  tunnelUrl: http://dt.ap-southeast-1.maxcompute.aliyun.com
  projectName: demo_dev
  tableName: embulk_non_partition
  overwrite: false
```


## 构建

```
$ ./gradlew gem  # -t to watch change of files and rebuild continuously
```
