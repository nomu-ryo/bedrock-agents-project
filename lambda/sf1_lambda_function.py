import json
import boto3
from botocore.exceptions import ClientError
from typing import Dict, Any

# S3クライアントを初期化
s3_client = boto3.client('s3')

# 署名付きURLの有効期限（秒）
PRESIGNED_URL_EXPIRATION_SECONDS = 120 # 2分間

def generate_s3_presigned_url(bucket_name: str, object_key: str) -> str:
    """
    指定されたS3オブジェクトキーに対する署名付きURLを生成するヘルパー関数
    """
    if not object_key:
        return None

    try:
        presigned_url = s3_client.generate_presigned_url(
            'get_object', 
            Params={'Bucket': bucket_name, 'Key': object_key},
            ExpiresIn=PRESIGNED_URL_EXPIRATION_SECONDS
        )
        print(f"Successfully generated PreSignedUrl for s3://{bucket_name}/{object_key}")
        return presigned_url
    except ClientError as e:
        error_message = f"S3 Client Error for {object_key}: {e.response['Error']['Message']}"
        print(error_message)
        # URL生成失敗時はNoneを返す
        return None

def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Step Functionsからの入力（bucketName, objectKey, diffObjectKey, yamlDiffObjectKey）を受け取り、
    3つのS3オブジェクトの署名付きURLを生成して返します。
    """
    
    payload = event
    
    try:
        # Step Functionsの入力からS3パラメータを取得
        bucket_name = payload.get('bucketName')
        object_key = payload.get('objectKey')             # 1. フルダイアグラム: .../tgw_routing_diagram.png
        diff_object_key = payload.get('diffObjectKey')    # 2. 差分ダイアグラム: .../tgw_routing_diagram_diff.png
        # ★★★ 追加 ★★★
        yaml_diff_object_key = payload.get('yamlDiffObjectKey') # 3. YAML差分ファイル: .../tgw_routing_cfn.yaml.diff 
        # ★★★ 追加終わり ★★★
        
        # パラメータのバリデーション (必須のS3バケットとフルダイアグラムのキー)
        if not bucket_name or not object_key:
            print(f"Error: Missing required S3 parameters. bucketName: {bucket_name}, objectKey: {object_key}")
            return {
                "error": "InputValidationError",
                "message": "Required parameters 'bucketName' or 'objectKey' are missing for S3 access.",
                "originalInput": payload 
            }

        # 1. フルダイアグラムの署名付きURLを生成
        presigned_url = generate_s3_presigned_url(bucket_name, object_key)
        
        # 2. 差分ダイアグラムの署名付きURLを生成 (diffObjectKeyが存在する場合のみ)
        diff_presigned_url = None
        if diff_object_key:
            diff_presigned_url = generate_s3_presigned_url(bucket_name, diff_object_key)
        else:
            print("Info: diffObjectKey is missing in payload. Skipping Diff PreSignedUrl generation.")
            
        # ★★★ 3. YAML差分ファイルの署名付きURLを生成 (yamlDiffObjectKeyが存在する場合のみ) ★★★
        yaml_diff_presigned_url = None
        if yaml_diff_object_key:
            yaml_diff_presigned_url = generate_s3_presigned_url(bucket_name, yaml_diff_object_key)
        else:
            print("Info: yamlDiffObjectKey is missing in payload. Skipping YAML Diff PreSignedUrl generation.")
        # ★★★ 追加終わり ★★★
        
        
        # 4. ペイロードを更新
        
        # フルダイアグラムのURL
        payload['PreSignedUrl'] = presigned_url if presigned_url else None
            
        # 差分ダイアグラムのURL
        payload['DiffPreSignedUrl'] = diff_presigned_url if diff_presigned_url or diff_object_key else None
        
        # 💡 YAML差分ファイルのURLを追加
        payload['YamlDiffPreSignedUrl'] = yaml_diff_presigned_url if yaml_diff_presigned_url or yaml_diff_object_key else None
        
        payload['presignedUrlExpirationSeconds'] = PRESIGNED_URL_EXPIRATION_SECONDS
        
        # このオブジェクトが次のタスクの入力として渡されます。
        return payload
        
    except Exception as e:
        error_message = f"Internal Error: {str(e)}"
        print(error_message)
        # Step Functionsにエラーを伝播させる
        raise Exception(f"InternalError: {error_message}")