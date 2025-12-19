import json
import os
import boto3
import datetime
import time
from botocore.config import Config
from botocore.exceptions import ClientError, WaiterError
from typing import Dict, Any, Union, List
from urllib.parse import unquote_plus # URLデコードのために追加

# =========================================================================
# Configuration (Environment Variables and Constants)
# =========================================================================
# S3バケット名は環境変数から取得することを想定
YAML_BUCKET = os.environ.get('YAML_OUTPUT_BUCKET', 'transitgateway-automation-rag')
CFN_YAML_FILENAME = "tgw_routing_cfn.yaml"
CFN_STACK_SUFFIX_BASE = "-TKY-TGW"

# ★★★ 修正: CFN_ROLE_NAMEをサフィックスに置き換え ★★★
# dynamic_prefixに続く部分 (例: -prd-gcopm-bedrock-agent-role)
CFN_ROLE_SUFFIX = "-your-deploy-role-suffix"
REGION = 'ap-northeast-1'

# ターゲット情報ファイル名
TGW_ID_CONFIG_FILENAME = "tgw_id_config.jsonl"
CFN_IMPORT_MAPPING_FILENAME = "cfn_import_mapping.json" 

# =========================================================================
# Helper Functions
# =========================================================================

def assume_role_and_get_client(account_id: str, role_name: str, service: str, region: str):
    """ターゲットアカウントにAssumeRoleし、指定されたサービス用のクライアントを返す。"""
    try:
        # 1. AssumeRole実行
        sts_client = boto3.client('sts', region_name=region)
        session_name = f"{service.replace(':', '')}DeploymentSession"
        
        print(f"[INFO] Attempting to AssumeRole: arn:aws:iam::{account_id}:role/{role_name}")
        assumed_role_object = sts_client.assume_role(
            RoleArn=f"arn:aws:iam::{account_id}:role/{role_name}",
            RoleSessionName=session_name
        )
        credentials = assumed_role_object['Credentials']
        
        print(f"[INFO] Successfully assumed role in account {account_id} for {service} deployment.")
        
        # 2. 認証情報を使用してクライアントを生成
        return boto3.client(
            service,
            aws_access_key_id=credentials['AccessKeyId'],
            aws_secret_access_key=credentials['SecretAccessKey'],
            aws_session_token=credentials['SessionToken'],
            region_name=region,
            config=Config(retries={'max_attempts': 3})
        )
    except ClientError as e:
        print(f"[ERROR] Failed to assume role in account {account_id}: {e}")
        raise 

def fetch_s3_json_data(bucket: str, key: str) -> Union[Dict[str, Any], List[Any], None]:
    """S3からJSONファイルを読み込む"""
    s3 = boto3.client('s3')
    try:
        print(f"[INFO] Attempting to fetch S3 data: s3://{bucket}/{key}")
        response = s3.get_object(Bucket=bucket, Key=key)
        # JSONをパースして返す
        content = response['Body'].read().decode('utf-8').strip()
        if not content:
             return None
             
        # JSONLまたは単一JSONオブジェクトに対応するため、try-exceptでパースを試みる
        try:
            return json.loads(content)
        except json.JSONDecodeError:
            # JSONL形式で、複数行ある場合は最初の行のみパースを試みる
            first_line = content.split('\n')[0].strip()
            if first_line:
                return json.loads(first_line)
            return None
            
    except ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchKey':
            return None
        raise 

def get_tgw_and_account_id(bucket: str, prefix: str) -> Dict[str, str]:
    """tgw_id_config.jsonlからTGW IDとAccount IDを読み込む"""
    key = f"{prefix}/extractsheet/{TGW_ID_CONFIG_FILENAME}"
    data = fetch_s3_json_data(bucket, key)
    
    if data is None:
        raise Exception(f"Required file not found: s3://{bucket}/{key}")
        
    target_data = {}
    if isinstance(data, list) and data:
        target_data = data[0]
    elif isinstance(data, dict):
        target_data = data
    else:
        raise Exception(f"Failed to parse TGW ID and Account ID from {key}. Invalid data format.")

    # ユーザー指定のキー名で値を取得
    tgw_id = target_data.get('tgw_id')
    account_id = target_data.get('account id') # ユーザー指定のキー名

    # 取得結果の検証
    if not tgw_id:
        raise Exception(f"TGW ID ('tgw_id') is missing in {key}.")
        
    if not account_id:
        raise Exception(f"Account ID ('account id') is missing in {key}.")
        
    return {
        'tgw_id': str(tgw_id),
        'target_account_id': str(account_id)
    }

def wait_for_change_set(cfn_client, stack_name: str, change_set_id: str, request_id: str):
    """変更セットの作成完了を待機し、失敗した場合は詳細をログに出力する。"""
    
    waiter = cfn_client.get_waiter('change_set_create_complete')
    
    try:
        print(f"[INFO] {request_id} Waiting for Change Set {change_set_id} to complete...")
        waiter.wait(
            ChangeSetName=change_set_id,
            StackName=stack_name,
            WaiterConfig={
                'Delay': 5,
                'MaxAttempts': 30
            }
        )
        print(f"[INFO] {request_id} Change Set {change_set_id} created successfully.")
    except WaiterError as e:
        try:
            response = cfn_client.describe_change_set(
                ChangeSetName=change_set_id,
                StackName=stack_name
            )
            status = response.get('Status')
            status_reason = response.get('StatusReason', 'No reason provided.')
            error_msg = f"Change Set creation FAILED: {status_reason} Status: {status}"
        except ClientError as describe_e:
            error_msg = f"Change Set creation FAILED. Could not get details: {describe_e}"
            
        print(f"[ERROR] {request_id} {error_msg}")
        raise Exception(error_msg) from e


def wait_for_stack_operation(cfn_client, stack_name: str, operation_type: str, request_id: str):
    """スタック操作（IMPORT/UPDATE）の完了を待機する"""
    
    waiter_name = 'stack_import_complete' if operation_type == 'IMPORT' else 'stack_update_complete'
    waiter = cfn_client.get_waiter(waiter_name)
    
    try:
        print(f"[INFO] {request_id} Waiting for stack {stack_name} to complete {operation_type}...")
        waiter.wait(
            StackName=stack_name,
            WaiterConfig={
                'Delay': 10,
                'MaxAttempts': 60 # 10分
            }
        )
        print(f"[INFO] {request_id} Stack {stack_name} completed {operation_type}_COMPLETE successfully.")
    except WaiterError as e:
        try:
            response = cfn_client.describe_stacks(StackName=stack_name)['Stacks'][0]
            stack_status = response.get('StackStatus')
            stack_reason = response.get('StackStatusReason', 'No reason provided.')
            error_msg = f"Stack {operation_type} FAILED: Final Status: {stack_status}, Reason: {stack_reason}"
        except ClientError:
            error_msg = f"Stack {operation_type} FAILED. Could not retrieve final stack status."
            
        print(f"[ERROR] {request_id} {error_msg}")
        raise Exception(error_msg) from e


# =========================================================================
# Lambda Handler (cfn_import_mapping.json トリガー対応版)
# =========================================================================

def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    request_id = context.aws_request_id
    
    # 失敗時のメッセージで確認できるよう、ロール名を初期化
    cfn_assume_role_name = ""
    
    try:
        # 1. S3イベントから必要な情報を抽出
        if 'Records' not in event or not event['Records']:
            raise Exception(f"{request_id} Invalid event structure. Not an S3 event notification.")

        s3_record = event['Records'][0]['s3']
        bucket_name = s3_record['bucket']['name']
        # S3キーをURLデコードして取得
        object_key = unquote_plus(s3_record['object']['key'])
        
        print(f"[INFO] {request_id} S3 Event received: s3://{bucket_name}/{object_key}")
        
        # 2. トリガーファイルの検証と dynamic_prefix の抽出
        expected_suffix = f"/extractsheet/{CFN_IMPORT_MAPPING_FILENAME}"

        if not object_key.endswith(expected_suffix):
             print(f"[WARNING] {request_id} Event was not triggered by the expected file: {CFN_IMPORT_MAPPING_FILENAME} in /extractsheet/. Exiting.")
             return {'statusCode': 200, 'body': json.dumps("Not the target import mapping file. Ignored.")}

        # 例: hub_dev8-01/extractsheet/cfn_import_mapping.json -> hub_dev8-01
        key_parts = object_key.split('/')
        if len(key_parts) < 3:
             raise Exception(f"{request_id} S3 key structure is too shallow to extract prefix: {object_key}")
             
        dynamic_prefix = key_parts[0]
        
        print(f"[INFO] {request_id} Extracted dynamic_prefix: {dynamic_prefix}")
        
        # ★★★ 修正: dynamic_prefix を使用して CFN_ROLE_NAME を動的に構築 ★★★
        # 例: dynamic_prefix (hub_dev8-01) + CFN_ROLE_SUFFIX (-prd-gcopm-bedrock-agent-role)
        cfn_assume_role_name = dynamic_prefix + CFN_ROLE_SUFFIX
        print(f"[INFO] {request_id} Dynamically constructed CFN Assume Role Name: {cfn_assume_role_name}")
        # ★★★ 修正終わり ★★★
        
        # 3. CFnインポート用リソースリストの取得
        import_mapping_key = object_key
        
        import_mapping_data = fetch_s3_json_data(bucket_name, import_mapping_key)

        if import_mapping_data is None:
            raise Exception(f"{request_id} Triggered S3 mapping file is empty or missing: s3://{bucket_name}/{import_mapping_key}")
            
        if not isinstance(import_mapping_data, list):
            raise Exception(f"{request_id} {CFN_IMPORT_MAPPING_FILENAME} is not a JSON list as required for CFn Import.")
            
        resources_to_import: List[Dict[str, Any]] = import_mapping_data
        
        # 4. ターゲット情報とTGW IDを、tgw_id_config.jsonlからまとめて取得する
        tgw_info = get_tgw_and_account_id(bucket_name, dynamic_prefix)
        target_account_id = tgw_info['target_account_id']
        tgw_id = tgw_info['tgw_id']

        print(f"[INFO] {request_id} Target Account ID fetched from S3: {target_account_id}")
        print(f"[INFO] {request_id} TGW ID fetched from S3: {tgw_id}")
        
        # 5. CFnクライアントとAssumeRoleの実行
        # ★★★ 修正: 動的に構築したロール名を使用 ★★★
        cfn_client = assume_role_and_get_client(
            account_id=target_account_id,
            role_name=cfn_assume_role_name, 
            service='cloudformation',
            region=REGION
        )
        # ★★★ 修正終わり ★★★
        
        # 6. スタック名とテンプレートURLの決定
        sanitized_prefix = dynamic_prefix.replace('_', '-')
        today_date_str = datetime.datetime.now().strftime('%Y%m%d')
        dynamic_suffix = f"{CFN_STACK_SUFFIX_BASE}-{today_date_str}"
        stack_name = f"{sanitized_prefix}{dynamic_suffix}"
        
        # テンプレートは {dynamic_prefix}/cfn/ の下に置かれていると仮定
        template_key = f"{dynamic_prefix}/cfn/{CFN_YAML_FILENAME}"
        template_url = f"https://{bucket_name}.s3.amazonaws.com/{template_key}"
        
        print(f"[INFO] {request_id} Constructed CFn Stack Name: {stack_name}")
        print(f"[INFO] {request_id} Constructed CFn Template URL: {template_url}")
        
        # 7. ChangeSetタイプを決定 (IMPORTまたはUPDATE)
        change_set_type = 'IMPORT'
        try:
            cfn_client.describe_stacks(StackName=stack_name)
            change_set_type = 'UPDATE'
            print(f"[INFO] {request_id} Stack {stack_name} exists. Creating UPDATE Change Set.")
        except ClientError as e:
            if 'does not exist' in str(e):
                print(f"[INFO] {request_id} Stack {stack_name} does not exist. Creating IMPORT Change Set.")
            else:
                raise e
                
        # 8. IMPORTだがリソースがない場合は実行しない
        if change_set_type == 'IMPORT' and not resources_to_import:
             print(f"[INFO] {request_id} No TGW resources found to import. Exiting.")
             return {'statusCode': 200, 'body': json.dumps("No TGW resources found to import.")}


        # 9. Change Setの作成
        change_set_name = f"{change_set_type}Resources-{int(time.time())}"
        
        create_params = {
            'StackName': stack_name,
            'TemplateURL': template_url,
            # TGW IDをCFnのParametersとして渡す
            'Parameters': [{'ParameterKey': 'TransitGatewayId', 'ParameterValue': tgw_id}],
            'Capabilities': ['CAPABILITY_NAMED_IAM'],
            'ChangeSetName': change_set_name,
            'ChangeSetType': change_set_type,
            'Tags': [{'Key': 'AutoGenerated', 'Value': 'TGW-Router-CFn'}]
        }
        
        if change_set_type == 'IMPORT':
            create_params['ResourcesToImport'] = resources_to_import
            # IMPORT操作ではTagsは許可されないため、Tagsを削除する
            if 'Tags' in create_params:
                print(f"[INFO] {request_id} Removing 'Tags' from CreateChangeSet parameters for IMPORT operation.")
                del create_params['Tags']

        response = cfn_client.create_change_set(**create_params)
        change_set_id = response['Id']
        
        # 10. Change Setの完了を待機、実行、スタック完了の待機
        wait_for_change_set(cfn_client, stack_name, change_set_id, request_id)
        
        print(f"[INFO] {request_id} Executing Change Set {change_set_id} for stack {stack_name}...")
        cfn_client.execute_change_set(ChangeSetName=change_set_id, StackName=stack_name)
        
        wait_for_stack_operation(cfn_client, stack_name, change_set_type, request_id)
        
        return {
            'statusCode': 200,
            'body': json.dumps(f"CFN Import/Deploy successful for stack {stack_name}."),
            'stackName': stack_name 
        }

    except Exception as e:
        error_msg = f"CFn Import/Deploy failed: {e}. Check AssumeRole permissions for {cfn_assume_role_name} in account {target_account_id if 'target_account_id' in locals() else 'N/A'}."
        print(f"[ERROR] {request_id} {error_msg}")
        raise e