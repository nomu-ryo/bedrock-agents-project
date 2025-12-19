# -*- coding: utf-8 -*- 
import os
import json
import boto3
from urllib.parse import quote
from typing import Dict, Any, Optional
import time
import secrets
import string
from botocore.exceptions import ClientError

# --------------------------------------------------------------------------
# 環境変数とクライアントの初期化
# --------------------------------------------------------------------------

# AWS クライアントの初期化
# Regionは環境変数から取得することを推奨
REGION_NAME = os.environ.get('AWS_REGION', 'ap-northeast-1')
sns_client = boto3.client('sns', region_name=REGION_NAME)
dynamodb_client = boto3.client('dynamodb', region_name=REGION_NAME)

# DynamoDBのテーブル名 (環境変数からの取得を推奨)
DYNAMODB_TABLE_NAME = os.environ.get('DYNAMODB_TABLE_NAME', 'ShortenedUrlStore')
# API GatewayのベースURL (短縮リンクのリゾルバー用)
API_GATEWAY_BASE_URL = os.environ.get('API_GATEWAY_BASE_URL', 'https://<YOUR_API_ID>.execute-api.<REGION>.amazonaws.com/prod')
# 短縮リンクの有効期限 (秒)。DynamoDBのTTLとして使用されます。
URL_EXPIRATION_SECONDS = 3600 # 1時間

# --------------------------------------------------------------------------
# DynamoDBへの保存ロジック (短縮キーの生成)
# --------------------------------------------------------------------------

def generate_short_id(length=8):
    """ランダムな英数字のShortIdを生成する"""
    characters = string.ascii_letters + string.digits
    return ''.join(secrets.choice(characters) for i in range(length))

def store_urls_in_dynamodb(full_url: str, diff_url: Optional[str], yaml_url: Optional[str]) -> Optional[str]:
    """
    3つのS3署名付きURLをDynamoDBに保存し、生成されたShortIdを返す。
    保存失敗時はNoneを返す。
    """
    short_id = generate_short_id()
    
    # TTL (Time To Live) の計算: 現在時刻 + 有効期限
    ttl = int(time.time()) + URL_EXPIRATION_SECONDS
    
    item = {
        # パーティションキー
        'ShortId': {'S': short_id},
        # TTL属性 (数値: Epoch Time)
        'TTL': {'N': str(ttl)},
        # データ属性
        'FullDiagramUrl': {'S': full_url}
    }
    
    if diff_url:
        item['DiffDiagramUrl'] = {'S': diff_url}
    if yaml_url:
        item['YamlDiffUrl'] = {'S': yaml_url}

    try:
        dynamodb_client.put_item(
            TableName=DYNAMODB_TABLE_NAME,
            Item=item
        )
        print(f"URLs stored in DynamoDB with ShortId: {short_id}. TTL set to: {ttl}")
        return short_id
    except ClientError as e:
        print(f"DynamoDB put_item failed for {short_id}: {e}")
        return None

# --------------------------------------------------------------------------
# SNSメッセージ本文生成 (短縮リンクを使用)
# --------------------------------------------------------------------------

def build_sns_message(
    bucket_name: str, 
    object_key: str, 
    short_id: Optional[str],
    api_base_url: str,
    approval_link: str, 
    rejection_link: str
) -> str:
    """SNSでEメールサブスクリプション向けに送信するメッセージ本文を生成するヘルパー関数"""

    # 短縮リンクの有効期限メッセージ
    expiration_message = f"（約{URL_EXPIRATION_SECONDS // 60}分間有効）" 
    
    diagram_links_section = ""
    if short_id:
        # 短縮リンクの構造: [API GWベースURL]/view?id=[ShortId]&type=[LinkType]
        full_diagram_short_link = f"{api_base_url}/view?id={short_id}&type=full"
        diff_diagram_short_link = f"{api_base_url}/view?id={short_id}&type=diff"
        yaml_diff_short_link = f"{api_base_url}/view?id={short_id}&type=yaml"

        diagram_links_section = (
            f"--- ダイアグラムリンク {expiration_message} ---\n"
            f"フルダイアグラム: <{full_diagram_short_link}>\n"
            f"差分ダイアグラム: <{diff_diagram_short_link}>\n"
            f"CFn YAML差分: <{yaml_diff_short_link}>\n\n"
        )
    else:
        # ShortIdの生成に失敗した場合は警告
        diagram_links_section = "警告: リンクの短縮に失敗しました。リンクはメールに含まれません。詳細についてはログを確認してください。\n\n"


    plain_text = (
        f"【S3オブジェクト承認リクエスト】\n\n"
        f"Transit Gatewayのルーティング図が更新されました。オブジェクトの承認または却下を行ってください。\n\n"
        f"--- オブジェクト詳細 ---\n"
        f"バケット名: {bucket_name}\n"
        f"メインオブジェクトキー: {object_key}\n\n"
        
        f"{diagram_links_section}" # 短縮リンクセクションの挿入
        
        f"--- アクションを選択してください ---\n"
        f"✅ 承認する: <{approval_link}>\n"
        f"❌ 却下する: <{rejection_link}>\n\n"
        f"（このメッセージはAWS Step Functionsワークフローから送信されました。）"
    )

    return plain_text

# --------------------------------------------------------------------------
# Lambda エントリポイント
# --------------------------------------------------------------------------

def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, str]:
    """Step Functionsの入力ペイロードを受け取り、SNSにメッセージを公開する"""
    
    try:
        # Step Functionsから渡される入力ペイロードを抽出
        task_token = event.get('TaskToken')
        presigned_url = event.get('S3PresignedUrl')
        diff_presigned_url = event.get('DiffS3PresignedUrl')
        yaml_diff_presigned_url = event.get('YamlDiffPreSignedUrl') 
        
        bucket_name = event.get('bucketName')
        object_key = event.get('objectKey')
        sns_topic_arn = event.get('SNSTopicArn') 

        # 必須パラメータのバリデーション
        if not all([task_token, bucket_name, object_key, sns_topic_arn]):
            raise ValueError("Required parameters (TaskToken, bucketName, objectKey, SNSTopicArn) are missing.")
        
        if not presigned_url and not diff_presigned_url and not yaml_diff_presigned_url:
             raise ValueError("No S3 PreSigned URLs were provided in the input.")


        # 1. S3の長いURLをDynamoDBに保存し、ShortIdを取得
        short_id = store_urls_in_dynamodb(
            presigned_url, 
            diff_presigned_url, 
            yaml_diff_presigned_url
        )
        
        if not short_id:
             print("FATAL: Failed to generate and store ShortId. Sending email without short links.")


        # 2. 承認/否認リンクの構築
        encoded_token = quote(task_token)
        
        # DynamoDBレコード削除のため、ShortIdを承認/却下リンクに含める
        short_id_param = f"&shortid={short_id}" if short_id else ""

        # API Gateway URLは、承認/却下処理用のLambdaにルーティングされる想定
        base_url_approval = API_GATEWAY_BASE_URL 
        approval_link = f"{base_url_approval}/approve?token={encoded_token}{short_id_param}"
        rejection_link = f"{base_url_approval}/reject?token={encoded_token}{short_id_param}"
        
        # 3. SNSメッセージ本文の生成
        email_subject = f"【重要】TGW-RTB通信状況図 承認リクエスト: {bucket_name}/{object_key.split('/')[-1]}"
        plain_message = build_sns_message(
            bucket_name, 
            object_key, 
            short_id,        
            API_GATEWAY_BASE_URL, 
            approval_link, 
            rejection_link
        )

        # 4. SNSによるメッセージ公開
        response = sns_client.publish(
            TopicArn=sns_topic_arn,
            Subject=email_subject,
            Message=plain_message,
            MessageStructure='string'
        )
        
        print(f"Message published successfully to SNS Topic {sns_topic_arn}. Message ID: {response['MessageId']}")

        return {
            "status": "NotificationSentViaSNS",
            "messageId": response['MessageId'],
            "ShortId": short_id # Step Functionsの次のタスクのためにShortIdを返しておく
        }

    except Exception as e:
        print(f"Error during execution: {str(e)}")
        # Step Functionsにエラーを伝播させる
        raise Exception(f"SNSPublishingFailed: {str(e)}")