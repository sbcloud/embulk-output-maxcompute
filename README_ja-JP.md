[English](./README.md) | [简体中文](./README_zh-CN.md) | 日本語

# Embulk用MaxComputeアウトプットプラグイン

このプラグインはAliyun Maxcomputeにデータを書き込むためのものです

## 概要

* **Plugin type**: output
* **Load all or nothing**: no
* **Resume supported**: no
* **Cleanup supported**: yes

## 設定項目

- **accessKeyId**: アリババクラウドアカウントのaccess key id (string, 必須項目)
- **accessKeySecret**: アリババクラウドアカウントのaccess key secret (string, 必須項目)
- **odpsUrl**:ODPS endpointの設定はこのURLを参照ください（https://www.alibabacloud.com/help/doc-detail/34951.htm ),異なる地域と接続モードに基づいて選択する (string, デフォルト: `"http://service.ap-northeast-1.maxcompute.aliyun.com/api"`)
- **tunnelUrl**: ODPS tunnel endpointの設定はこのURLを参照ください（https://www.alibabacloud.com/help/doc-detail/34951.htm ）, 異なる地域と接続モードに基づいて選択する (string, デフォルト: `"http://dt.ap-northeast-1.maxcompute.aliyun.com"`)
- **projectName**: ODPSターゲットプロジェクト名（string, 必須項目）
- **tableName**:  ODPSターゲットテーブル名, 実行前に作成してください（string, 必須項目）
- **partition**: パーティション、フォーマットは'pt=20201026', パーティションテーブルのみが有効である. 非パーティションテーブルは無視してください, ターゲットテーブルは非パーティションテーブルの場合、パーティションを設定するとエラーが発生する (string, default: `null`)
- **overwrite**: trueを設定する場合、データを更新する前に既存のデータをクリアする。非パーティションテーブルの場合、`truncate table`でデータをクリアする。パーティションテーブルの場合、`partition` 対応のパーティションを削除する (boolean, デフォルト: `false`)
- **mappings**: フィールドの対応関係を定義する、Maxcomputeテーブルの列名とデータ読み込みプラグインにある名前と一致している場合、この設定がしなくてもいい (Map of string, デフォルト: `{}`)

## データフォーマット
Maxcomputeのテーブル構成とデータインプットプラグインで取得したテーブル構成が一致でないと、`No such Columns`エラーが発生する。
`mappings`設定項目を使って対応関係を定義するか、名前で直接対応してもいいです。
MaxCompute関連のデータ転換は下記の通り：

| Embulk データタイプ | ODPS データタイプ    |
| --------         | ----- |
| Long             |bigint |
| Double           |double |
| String           |string |
| Timestamp        |datetime |
| Boolean          |bool |

## 例

下記の設定ファイルは日本リージョンのパーティションテーブルにデータをアップローする。
デフォルトは日本リージョンなので、odpsUrlとtunnelUrl二つの設定項目を通してプロジェクトのリージョンを設定する必要はありません。

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

下記の設定ファイルはシンガポールリージョンの非パーティションテーブルにデータをアップローする。
odpsUrlとtunnelUrl二つの設定項目を指定することによってプロジェクトのリージョンを設定する。
異なるリージョンと接続モードはhttps://www.alibabacloud.com/help/doc-detail/34951.htm
サイトで定義のEndpointによりodpsUrlとtunnelUrlを設定する。
非パーティションテーブルでは、`partition`の項目を設定する必要がありません。
設定項目に`overwrite`はfalseの場合、データをアップロード前に既存のデータをクリアしません。
`mappings`が設定していないため、デフォルトで名前を使ってMaxComputeテーブル構成とデータインプットプラグインで導入したテーブル構成をマッピングする。

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


## ビルド

```
$ ./gradlew gem  # -t to watch change of files and rebuild continuously
```
