import json
import os
import boto3
from botocore.exceptions import ClientError # <-- ここを追加
from typing import Dict, Any

# DynamoDB クライアントの初期化
# Regionは環境変数から取得することを推奨
dynamodb_client = boto3.client('dynamodb', region_name=os.environ.get('AWS_REGION', 'ap-northeast-1'))

# 環境変数からテーブル名を取得 (前のLambdaと一致させる)
DYNAMODB_TABLE_NAME = os.environ.get('DYNAMODB_TABLE_NAME', 'ShortenedUrlStore')

# S3 URL タイプと DynamoDB 属性名のマッピング
LINK_TYPE_MAPPING = {
    'full': 'FullDiagramUrl',
    'diff': 'DiffDiagramUrl',
    'yaml': 'YamlDiffUrl'
}

def short_url_resolver_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    API Gatewayからのリクエストを受け取り、ShortIdに基づいてDynamoDBからS3 URLを取得し、
    HTTP 302リダイレクトを返す。
    """
    
    print(f"Received event: {json.dumps(event)}")
    
    # 1. クエリパラメータの抽出
    # Lambdaプロキシ統合ではクエリパラメータは 'queryStringParameters' に含まれる
    query_params = event.get('queryStringParameters', {})
    short_id = query_params.get('id')
    link_type = query_params.get('type') # 'full', 'diff', 'yaml'
    
    # 必須パラメータのチェック
    if not short_id or not link_type:
        print("Missing 'id' or 'type' query parameter.")
        # エラーメッセージを返す
        return {
            'statusCode': 400,
            'body': 'Bad Request: Missing ID or link type.'
        }
    
    if link_type not in LINK_TYPE_MAPPING:
        print(f"Invalid link type received: {link_type}")
        return {
            'statusCode': 400,
            'body': 'Bad Request: Invalid link type.'
        }
        
    # DynamoDBで検索する属性名を取得
    long_url_attribute = LINK_TYPE_MAPPING[link_type]
    
    try:
        # 2. DynamoDBからアイテムを取得
        response = dynamodb_client.get_item(
            TableName=DYNAMODB_TABLE_NAME,
            Key={'ShortId': {'S': short_id}},
            ProjectionExpression=long_url_attribute # 必要な属性のみを取得
        )
        
        item = response.get('Item')
        
        # 3. アイテムの存在チェック (TTLで削除されている可能性)
        if not item:
            print(f"ShortId {short_id} not found or expired.")
            # リンクが期限切れ、または承認/却下によりクリーンアップされた後の場合は404
            return {
                'statusCode': 404, 
                'headers': { "Content-Type": "text/html" },
                'body': '<html><head><title>リンク切れ</title></head><body><h1>404 Not Found</h1><p>このリンクは期限切れか、既に処理が完了し無効化されました。</p></body></html>'
            }
        
        # 4. 長い S3 署名付き URL の抽出
        long_url = item.get(long_url_attribute, {}).get('S')

        if not long_url:
            # ShortIdは存在するが、特定のリンクタイプ(例: diff)のデータが保存されていない場合
            print(f"ShortId {short_id} found, but {long_url_attribute} is missing.")
            return {
                'statusCode': 404, 
                'headers': { "Content-Type": "text/html" },
                'body': f'<html><head><title>リンクタイプなし</title></head><body><h1>404 Not Found</h1><p>このリンクに必要なデータタイプ ({link_type}) は見つかりませんでした。</p></body></html>'
            }


        # 5. HTTP 302 リダイレクトを返す
        print(f"Redirecting {short_id} ({link_type}) to S3 URL.")
        return {
            'statusCode': 302,
            'headers': {
                'Location': long_url, 
                'Cache-Control': 'no-cache, no-store, must-revalidate' 
            },
            'body': ''
        }
    
    except ClientError as e:
        # Boto3のClientErrorをキャッチ
        print(f"DynamoDB Client Error resolving link for {short_id}: {e}")
        return {'statusCode': 500, 'body': 'Internal server error (Database access failed).'}
    except Exception as e:
        # その他の予期せぬエラーをキャッチ
        print(f"Unhandled error resolving link for {short_id}: {e}")
        return {'statusCode': 500, 'body': 'Internal server error.'}