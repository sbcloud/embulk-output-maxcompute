English | [简体中文](./README_zh-CN.md) | [日本語](./README_ja-JP.md)

# Maxcompute output plugin for Embulk

This output plugin is used to write record to Aliyun Maxcompute

## Overview

* **Plugin type**: output
* **Load all or nothing**: no
* **Resume supported**: no
* **Cleanup supported**: yes

## Configuration

- **accessKeyId**: Aliyun account access key id (string, required)
- **accessKeySecret**: Aliyun account access key secret (string, required)
- **odpsUrl**: ODPS endpoint, refer to https://www.alibabacloud.com/help/doc-detail/34951.htm , based on different regions and connection mode (string, default: `"http://service.ap-northeast-1.maxcompute.aliyun.com/api"`)
- **tunnelUrl**: ODPS tunnel endpoint, refer to https://www.alibabacloud.com/help/doc-detail/34951.htm , based on different regions and connection mode (string, default: `"http://dt.ap-northeast-1.maxcompute.aliyun.com"`)
- **projectName**: Target ODPS project name (string, required)
- **tableName**: Target ODPS table name, need to be created before running the job (string, required)
- **partition**: Partition spec like 'pt=20201026', only used for partition tables. no need to set for non-partition table, will pop up errors if set values with non-partition tables (string, default: `null`)
- **overwrite**: Clear existing data at the beginning if the value is true. For non-partition table, clear data with `truncate table`; For partition table, drop partition defined in `partition` parameter (boolean, default: `false`)
- **mappings**: Defined mapping relationships for columns, make sure your maxcompute table columns could map input schema by names if you do not set related values (Map of string, default: `{}`)

## Data Format
Make sure your maxcompute table columns could map input schema, otherwise, you will get `No such Columns` error.
You could define mapping relationships in the configuration file or mapped by names as default.

| Embulk Data Type | ODPS Data Type    |
| --------         | ----- |
| Long             |bigint |
| Double           |double |
| String           |string |
| Timestamp        |datetime |
| Boolean          |bool |

## Example

Upload data to partition table in JP region.
No need to set odpsUrl and tunnelUrl as default region is JP.

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

Upload data to non-partition table in SG region, use odpsUrl and tunnelUrl to change the target region.
For Different regions and connection mode,Can set two configuration items odpsurl and tunnelurl by the endpoint defined on the site https://www.alibabacloud.com/help/doc-detail/34951.htm.
For non-partition tables, remove partition from configuration file.
Will not clean up existing data as overwirite is false.
Remove mappings from configuration file as input schema could match maxcompute table structure by names.
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

## Build

```
$ ./gradlew gem  # -t to watch change of files and rebuild continuously
```
