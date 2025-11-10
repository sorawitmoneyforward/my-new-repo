## 目的

このガイドは、エンジニアが Databricks プロジェクトのフォルダ構造や、ローカルと本番環境での役割、実行方法を理解して、開発をスムーズに進めるための手引きです。

## ディレクトリ構成

```
databricks/
│
├── src/                 # 共通ロジックや重要ロジックが配置されている
│   ├── fetch_daily_balances.py
│   ├── ...
│   └── utils
│       └── s3_client.py
│
├── scripts/             # src のモジュールを python script として実行することができる
│   ├── script_fetch_daily_balances.py
│   └── ...
│
├── notebooks/           # src のモジュールを databricks job として実行するための notebook file が格納されている
│   ├── notebook_fetch_daily_balances.ipynb
│   └── ...
│
├── workflow/            # notebook job を実行する定義ファイル
│   ├── vader.json
│   └── ...
│
├── tests/               # src に格納されている共通ロジック・重要ロジックを pytest でテストを実行する
│   ├── test_fetch_daily_balances.py
│   ├── input/
│   │   ├── test_data_daily_balances_case_1.json
│   │   ├── test_data_daily_balances_case_2.json
│   │   └── ...
│   └── expected_output/
│       ├── test_data_daily_balances_case_1.pkl
│       ├── test_data_daily_balances_case_2.pkl
│       └── ...
│
├── Dockerfile           # ローカル環境の Docker Container を定義した Dockerfile
└── .env                 # 環境設定ファイル
```

## 各ディレクトリの役割

- **src**: プロジェクトの共通ロジックや重要なロジックを格納する。このモジュールは、scripts や notebooks で利用される。ローカル環境は script 前提での検証となるため、 src 配下のモジュールをテスト対象とし、 scripts や notebooks 配下の資源には、ロジックの実装は極力行わないこととする。
- **scripts**: `src` で定義したモジュールを Python スクリプトとして実行するためのスクリプトが格納される。これを使ってローカルで簡単にテストやデバッグを行うことができる。
- **notebooks**: Databricks における Job として実行するための Notebook file が格納される。
- **workflow**: Job を実行するための設定ファイルが含まれている。これを用いて Databricks 環境へのデプロイが行える。
- **tests**: `src` に格納された共通ロジックや重要なロジックを、pytest によるテストを実行するためのコードが含まれている。入力データや期待される出力データもここに保存される。

## 環境構成

### ローカル環境

- root ディレクトリより、ローカル環境の Docker Container を立ち上げる。
   ```bash
   docker-compose up -d
   docker exec -it databricks-workflow-test bash
   ```
- ローカル環境では、`scripts` を利用して機能のテストやデバッグを行う。以下のように、モジュールモードにて実行する。
   ```bash
   python -m scripts.script_fetch_daily_balances
   ```
- `tests` ディレクトリを使用して、ユニットテストを実行する。
   ```bash
   python -m pytest tests/
   ```

### 本番環境（Databricks） ※ 編集予定

- 本番環境では、`notebooks` 内のノートブックファイルを Databricks 上で実行します。
- `workflow` 設定をもとに、デプロイしたいノートブックジョブを管理します。

## 命名規則: ファイル名

- **src 配下**: ${module-name}.py
- **scripts 配下**: script_${module-name}.py
- **notebooks 配下**: notebook_${module-name}.jpynb
- **tests 配下**: test_${module-name}.py
  - **data/input or expected_output 配下**: test_data_${module-name}.py

## テストコードの配置

- 全てのテストコードは `tests` ディレクトリに配置する。各テストファイルは、対応する `src` モジュールに関連付けをする。
- 入力データは `tests/input/`、期待される出力データは `tests/expected_output/` に格納する。

## S3 Path

```
xxx-bucket/
│
└── data/
    ├── vader.daily_balances/
    │   ├── ${yyyy-mm-dd}/
    │   │   ├── ${office_id}.pkl
    │   │   └── ...
    │   ├── ...
    │   └── latest/
    │       ├── ${office_id}.pkl
    │       └── ...
    ├── vader.daily_balance_info/
    │   ├── ${yyyy-mm-dd}/
    │   │   ├── ${office_id}.pkl
    │   │   └── ...
    │   ├── ...
    │   └── latest/
    │       ├── ${office_id}.pkl
    │       └── ...
    ├── vader.stats/
    │   ├── ${yyyy-mm-dd}/
    │   │   ├── ${office_id}.pkl
    │   │   └── ...
    │   ├── ...
    │   └── latest/
    │       ├── ${office_id}.pkl
    │       └── ...
    ├── vader.scores/
    │   ├── ${yyyy-mm-dd}/
    │   │   ├── ${office_id}.pkl
    │   │   └── ...
    │   ├── ...
    │   └── latest/
    │       ├── ${office_id}.pkl
    │       └── ...
    └── vader.errors/
        ├── ${yyyy-mm-dd}/
        │   ├── ${office_id}.pkl
        │   └── ...
        ├── ...
        └── latest/
            ├── ${office_id}.pkl
            └── ...
```

