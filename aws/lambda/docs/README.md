# Credit scores API

## 概要

MLOps基盤にデプロイされているvader/anakin/obi-wanの3つのモデルのAPI仕様にについてまとめます。

[API Gateway](https://ap-northeast-1.console.aws.amazon.com/apigateway/main/apis/frp9v64ky0/resources?api=frp9v64ky0&region=ap-northeast-1)

## API仕様

### GET /vader_scores/{office_id}

vaderスコアを返します

#### パラメータ

| パラメータ | タイプ | 型     | 必須 | 説明                                           |
| ---------- | ------ | ------ | ---- | ---------------------------------------------- |
| office_id  | path   | Number | 必須 | MFCにおける事業所ID(officesテーブルのIDカラム) |

#### レスポンス

Content-Type: application/json

200: 成功

| 項目名          | 型                                  | 説明               |
| --------------- | ----------------------------------- | ------------------ |
| vader_score     | Number                              | vaderスコア        |
| last_month_year | { "year": Number, "month": Number } | 残高データの最終月 |
| created_at      | Date                                | スコアの算出日     |

### GET /anakin_scores/{office_id}

anakinスコアを返します

#### パラメータ

| パラメータ     | タイプ | 型     | 必須 | 説明                                           |
| -------------- | ------ | ------ | ---- | ---------------------------------------------- |
| office_id      | path   | Number | 必須 | MFCにおける事業所ID(officesテーブルのIDカラム) |
| desired_amount | query  | Number | 必須 | 希望調達額                                     |

#### レスポンス

Content-Type: application/json

200: 成功

| 項目名 | 型     | 説明         |
| ------ | ------ | ------------ |
| score  | Number | anakinスコア |


### GET /obi_wan_scores/{office_id}

obi-wanスコアを返します

#### パラメータ

| パラメータ     | タイプ | 型     | 必須 | 説明                                                                          |
| -------------- | ------ | ------ | ---- | ----------------------------------------------------------------------------- |
| office_id      | path   | Number | 必須 | MFCにおける事業所ID(officesテーブルのIDカラム)                                |
| desired_amount | query  | Number | 必須 | 希望調達額                                                                    |
| date           | query  | String | 任意 | 日次残高指定日（指定しない場合、最新の日次残高を使用）   |
| feature14      | query  | Number | 必須 | 前前々期年商（千円）                                                          |
| feature15      | query  | Number | 必須 | 前期年商（千円）                                                              |
| feature17      | query  | Number | 必須 | 売掛金回転率（前期売掛債権/(前期年商/12)）                                    |
| feature26      | query  | Number | 必須 | 年商/調達希望金額（調達希望金額はLukeと同じように定量化、売上高は前年度売上） |
| feature31      | query  | Number | 必須 | 銀行等からの借入金額（前期決算ベース）（千円）                                |
| feature35      | query  | Number | 必須 | 営業利益率（前期決算ベース）                                                  |
| feature45      | query  | Number | 必須 | 代表の役員報酬金額（前期決算ベース）                                          |
| feature46      | query  | Number | 必須 | 代表の役員報酬金額の成長率（前々期・前期比）                                  |
| feature47      | query  | Number | 必須 | 「貸付金」「現金」「仮払金」「前渡金（事業性のもの以外）」合計 / 総資産       |


#### レスポンス

Content-Type: application/json

200: 成功

| 項目名 | 型     | 説明          |
| ------ | ------ | ------------- |
| score  | Number | obi-wanスコア |



### GET /luke_scores/{office_id}

lukeスコアを返します。日次残高の中央値と希望調達額の比率を計算します。

#### パラメータ

| パラメータ     | タイプ | 型     | 必須 | 説明                                                                          |
| -------------- | ------ | ------ | ---- | ----------------------------------------------------------------------------- |
| office_id      | path   | Number | 必須 | MFCにおける事業所ID(officesテーブルのIDカラム)                                |
| desired_amount | query  | Number | 必須 | 希望調達額                                                                    |
| date           | query  | String | 任意 | 日次残高指定日（指定しない場合、最新の日次残高を使用）                        |

#### レスポンス

Content-Type: application/json

200: 成功

| 項目名      | 型     | 説明                    |
| ----------- | ------ | ----------------------- |
| score       | Number | lukeスコア              |
| med_balance | Number | 日次残高の中央値（円）  |



### (共通)エラーの場合

400: パラメータが不正

| 項目名 | 型     | 説明             |
| ------ | ------ | ---------------- |
| error  | string | エラーメッセージ |

404: データが存在しない

| 項目名 | 型     | 説明             |
| ------ | ------ | ---------------- |
| error  | string | エラーメッセージ |

500: サーバー内部エラー

| 項目名 | 型     | 説明             |
| ------ | ------ | ---------------- |
| error  | string | エラーメッセージ |



