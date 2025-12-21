# Amazon Bedrock Agent によるガバメントクラウド運用自動化プロジェクト

本リポジトリは、Amazon Bedrock Agent を「司令塔」とし、ガバメントクラウドにおける Transit Gateway (TGW) 設定の自動化と、Mermaid による構成図の自動レンダリングを実現する自律型運用モデルのソースコードを公開しています。

## 📝 関連解説記事

本プロジェクトの設計思想、アーキテクチャの詳細は以下の記事をご参照ください。

* **前編**: [Amazon Bedrock Agentによる自律型ワークフローの構築と視覚的バリデーション](https://www.google.com/search?q=https://example.com/article-part1)
* **後編**: [Step Functions を用いた Human-in-the-loop な自動デプロイの実装](https://www.google.com/search?q=https://example.com/article-part2)

---

## 🚀 プロジェクト概要

ガバメントクラウド運用における「設定ミスのリスク」と「リードタイムの長期化」を解決するため、以下の2段階のアプローチを実装しました。

1. **Phase 1: 構造化データへの正規化**
* Excel（非構造データ）から正確な設定値を抽出し、YAML形式へ構造化。


2. **Phase 2: 視覚的バリデーション (Visual Validation)**
* 生成された IaC (YAML) から構成図 (Mermaid) を自動生成し、人間が「絵」で見て直感的に検品できる仕組みを構築。



---

## 📂 ディレクトリ構成

```text
.
├── lambda/                 # 各フェーズで実行される AWS Lambda 関数
│   ├── br*.py / .js        # Bedrock Agent 連携用 (Excel抽出、Mermaid描画)
│   ├── sf*.py / .js        # Step Functions ワークフロー制御用
│   ├── br*.py / .js        # Bedrock Agent 連携用 (Excel抽出、Mermaid描画)
│   ├── sf*.py / .js        # Step Functions ワークフロー制御用
│   ├── sf*.py / .js        # Step Functions ワークフロー制御用
│   ├── br*.py / .js        # Bedrock Agent 連携用 (Excel抽出、Mermaid描画)
│   ├── sf*.py / .js        # Step Functions ワークフロー制御用
│   └── tg*.py              # TGWリソース走査・マッピング生成用
├── APIschema/              # Bedrock Agent アクショングループ定義
│   ├── br1_apischema.yml   # TGW設定抽出 (Excel to JSONL)
│   ├── br4_apischema.yml   # Mermaidレンダリング (YAML to PNG)
│   ├── br1_apischema.yml   # TGW設定抽出 (Excel to JSONL)
│   └── br4_apischema.yml   # Mermaidレンダリング (YAML to PNG)
└── README.md

```

## 🛠 主要コンポーネント詳細

### 1. TGW設定抽出 (Lambda: `br1`, `tg1`)

Excel設計書から特定のパラメータを抽出し、一意のルートテーブル名を採番して構造化データを生成します。

* **Key Logic**: AssumeRole によるクロスアカウントでのVPCオーナー特定。

### 2. 視覚的バリデーション (Lambda: `br4`)

生成されたYAMLの論理構造を解析し、Mermaid.js を用いてネットワーク構成図を PNG レンダリングします。

* **Tech Stack**: Node.js, Puppeteer-core, @sparticuz/chromium, Mermaid.js.

### 3. 自律型デプロイ・フロー (Step Functions連携)

Task Token を用いた「承認待ち」ステータスの管理、DynamoDB による短縮URL発行、CloudFront + OAC によるセキュアなプレビュー配信を統合しています。

---

## 📈 導入効果

本システムの適用により、2つのVPC接続におけるリードタイムを **約35分から4分（89%削減）** へ短縮し、手作業の回数を **90%以上削減** することに成功しました。

| 評価指標 | 従来法（手動） | 本システム |
| --- | --- | --- |
| 合計リードタイム | 35分 | **4分** |
| 手作業回数 | 40回以上 | **4回** |

---

## ⚠️ セットアップの注意点

* **Lambda Layers**: `br4_lambda_function.js` を動かすには、Chromium と Mermaid.js を含むレイヤーが必要です。
* **Memory**: レンダリング用 Lambda は 2048MB 以上の設定を推奨します。
* **IAM Role**: TGW情報の取得やS3へのアクセス、AssumeRole に関する適切なポリシー設定が必要です。

## 👤 著者

**野村 稜武** (NTT西日本 サービス開発担当)
クラウドインフラの自動化と生成AIの実用化に取り組んでいます。

---

### 免責事項

本リポジトリのコードは、学習およびデモンストレーションを目的としています。実環境への適用に際しては、各組織のセキュリティポリシーに従い、十分な検証を行ってください。
