import json
import os
import boto3
import datetime
import time
from botocore.config import Config
from botocore.exceptions import ClientError, WaiterError
from typing import Dict, Any, Union, List

# =========================================================================
# Configuration (Step Functionsの入力から取得できない定数を定義)
# =========================================================================
# 環境変数または定数としてS3バケット名とファイル名を定義
YAML_BUCKET = os.environ.get('YAML_BUCKET_NAME', 'transitgateway-automation-rag') 
CFN_YAML_FILENAME = "tgw_routing_cfn.yaml"
CFN_STACK_SUFFIX_BASE = "-TKY-TGW"

# ★★★ 修正: CFN_ROLE_NAMEをサフィックスに置き換え ★★★
# dynamic_prefixに続く部分を定義 (例: -prd-gcopm-bedrock-agent-role)
CFN_ROLE_SUFFIX = "-your-service-deploy-role"
REGION = 'ap-northeast-1'

# ターゲット情報ファイル名 (S3内でのパス)
TGW_ID_CONFIG_FILENAME = "tgw_id_config.jsonl"
ACCOUNT_ID_CONFIG_FILENAME = "target_account.jsonl" 

# =========================================================================
# Helper Functions
# =========================================================================

def assume_role_and_get_client(account_id: str, role_name: str, service: str, region: str):
    """ターゲットアカウントにAssumeRoleし、指定されたサービス用のクライアントを返す。"""
    try:
        sts_client = boto3.client('sts', region_name=region)
        session_name = f"{service.replace(':', '')}DeploymentSession"
        
        print(f"[INFO] Attempting to AssumeRole: arn:aws:iam::{account_id}:role/{role_name}")
        assumed_role_object = sts_client.assume_role(
            RoleArn=f"arn:aws:iam::{account_id}:role/{role_name}",
            RoleSessionName=session_name
        )
        credentials = assumed_role_object['Credentials']
        
        print(f"[INFO] Successfully assumed role in account {account_id} for {service} deployment.")
        
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
        
        content = response['Body'].read().decode('utf-8').strip()
        if not content:
            return None
        
        # JSONLを考慮し、最初の行のみパースを試みる
        try:
            return json.loads(content)
        except json.JSONDecodeError:
            first_line = content.split('\n')[0].strip()
            if first_line:
                return json.loads(first_line)
            return None

    except ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchKey':
            return None
        raise 

def get_target_account_id(bucket: str, prefix: str) -> str:
    """tgw_id_config.jsonlからターゲットアカウントIDを読み込む"""
    key = f"{prefix}/extractsheet/{TGW_ID_CONFIG_FILENAME}"
    data = fetch_s3_json_data(bucket, key)
    
    if data is None:
        raise Exception(f"Required file not found: s3://{bucket}/{key}")
        
    if isinstance(data, list) and data:
        account_id = data[0].get('account id')
    elif isinstance(data, dict):
        account_id = data.get('account id')
    else:
        raise Exception(f"Failed to parse target account ID from {key}. Invalid data format.")

    if not account_id:
        raise Exception(f"Target account ID ('account id' key) is missing in {key}.")
        
    return str(account_id)

def get_tgw_id(bucket: str, prefix: str) -> str:
    """tgw_id_config.jsonlからTGW IDを読み込む"""
    key = f"{prefix}/extractsheet/{TGW_ID_CONFIG_FILENAME}"
    data = fetch_s3_json_data(bucket, key)
    
    if data is None:
        raise Exception(f"Required file not found: s3://{bucket}/{key}")
        
    if isinstance(data, list) and data:
        tgw_id = data[0].get('tgw_id')
    elif isinstance(data, dict):
        tgw_id = data.get('tgw_id')
    else:
        raise Exception(f"Failed to parse TGW ID from {key}. Invalid data format.")
    
    if not tgw_id:
        raise Exception(f"TGW ID is missing in {key}.")
        
    return str(tgw_id)


def wait_for_change_set(cfn_client, stack_name: str, change_set_id: str, request_id: str):
    """変更セットの作成完了を待機し、失敗した場合は詳細をログに出力する。"""
    waiter = cfn_client.get_waiter('change_set_create_complete')
    
    try:
        print(f"[INFO] {request_id} Waiting for Change Set {change_set_id} to complete...")
        waiter.wait(
            ChangeSetName=change_set_id,
            StackName=stack_name,
            WaiterConfig={'Delay': 5, 'MaxAttempts': 30}
        )
        print(f"[INFO] {request_id} Change Set {change_set_id} created successfully.")
    except WaiterError as e:
        try:
            response = cfn_client.describe_change_set(ChangeSetName=change_set_id, StackName=stack_name)
            status_reason = response.get('StatusReason', 'No reason provided.')
            error_msg = f"Change Set creation FAILED: {status_reason} Status: {response.get('Status')}"
        except ClientError as describe_e:
            error_msg = f"Change Set creation FAILED. Could not get details: {describe_e}"
            
        print(f"[ERROR] {request_id} {error_msg}")
        raise Exception(error_msg) from e


def wait_for_stack_operation(cfn_client, stack_name: str, operation_type: str, request_id: str):
    """スタック操作（UPDATE/IMPORT）の完了を待機する"""
    
    waiter_name = 'stack_create_complete' if operation_type == 'CREATE' else 'stack_update_complete' 
    waiter = cfn_client.get_waiter(waiter_name)
    
    try:
        print(f"[INFO] {request_id} Waiting for stack {stack_name} to complete {operation_type}...")
        waiter.wait(
            StackName=stack_name,
            WaiterConfig={'Delay': 10, 'MaxAttempts': 60}
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
# Lambda Handler (Step Functionsの出力を処理)
# =========================================================================

def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Step Functionsの出力を受け取り、ターゲットアカウントにCFnをデプロイする。
    """
    request_id = context.aws_request_id
    
    # ロール名を初期化（失敗時のエラーメッセージに含めるため）
    cfn_assume_role_name = ""
    target_account_id = ""

    try:
        # Step FunctionsのvalidatedPayloadからDynamicPrefixを取得する
        validated_payload = event.get('validatedPayload', {})
        dynamic_prefix = validated_payload.get('DynamicPrefix')
        
        if not dynamic_prefix:
            # 必要な情報がない場合は処理を続行できない
            raise Exception(f"{request_id} DynamicPrefix is missing in the Step Functions event (expected at validatedPayload.DynamicPrefix). Cannot proceed with deployment.")

        # ★★★ 修正: dynamic_prefix を使用して CFN_ROLE_NAME を動的に構築 ★★★
        # 例: dynamic_prefix (hub_dev8-01) + CFN_ROLE_SUFFIX (-prd-gcopm-bedrock-agent-role)
        cfn_assume_role_name = dynamic_prefix + CFN_ROLE_SUFFIX
        print(f"[INFO] {request_id} Dynamically constructed CFN Assume Role Name: {cfn_assume_role_name}")
        # ★★★ 修正終わり ★★★

        # ターゲットアカウントIDとTGW IDを取得 (両方 tgw_id_config.jsonl を参照)
        target_account_id = get_target_account_id(YAML_BUCKET, dynamic_prefix)
        tgw_id = get_tgw_id(YAML_BUCKET, dynamic_prefix)

        print(f"[INFO] {request_id} Target Account ID fetched: {target_account_id}")
        print(f"[INFO] {request_id} TGW ID fetched: {tgw_id}")
        
        # ターゲットアカウントにAssumeRoleし、CFnクライアントを生成
        # ★★★ 修正: 動的に構築したロール名を使用 ★★★
        cfn_client = assume_role_and_get_client(
            account_id=target_account_id,
            role_name=cfn_assume_role_name,
            service='cloudformation',
            region=REGION
        )
        # ★★★ 修正終わり ★★★
        
        # スタック名とテンプレートURLの決定
        sanitized_prefix = dynamic_prefix.replace('_', '-')
        
        today_date_str = datetime.datetime.now().strftime('%Y%m%d')
        # スタック名に日付サフィックスを含めるロジックを保持
        stack_name = f"{sanitized_prefix}{CFN_STACK_SUFFIX_BASE}-{today_date_str}" 
        
        # S3のテンプレートURL
        template_url = f"https://{YAML_BUCKET}.s3.amazonaws.com/{dynamic_prefix}/cfn/{CFN_YAML_FILENAME}"
        
        print(f"[INFO] {request_id} Constructed CFn Stack Name: {stack_name}")
        
        # ChangeSetタイプを決定 (CREATE/UPDATE)
        change_set_type = 'CREATE' 
        
        # ★★★ 既存スタックのステータスチェックと待機ロジック ★★★
        while True:
            try:
                # スタックが存在するか確認
                response = cfn_client.describe_stacks(StackName=stack_name)
                current_stack_status = response['Stacks'][0]['StackStatus']
                
                if current_stack_status == 'UPDATE_ROLLBACK_COMPLETE_CLEANUP_IN_PROGRESS':
                    print(f"[WARNING] {request_id} Stack is in {current_stack_status}. Waiting 10 seconds before retry.")
                    time.sleep(10)
                elif current_stack_status not in ['DELETE_COMPLETE']:
                    change_set_type = 'UPDATE'
                    print(f"[INFO] {request_id} Stack {stack_name} exists. Creating UPDATE Change Set.")
                    break # 有効な状態であればループを抜ける
                else:
                    # スタックがDELETE_COMPLETEの場合はCREATE
                    break # ループを抜ける

            except ClientError as e:
                if 'does not exist' in str(e):
                    change_set_type = 'CREATE'
                    print(f"[INFO] {request_id} Stack {stack_name} does not exist. Creating CREATE Change Set.")
                    break # ループを抜ける
                else:
                    raise e
        # ★★★ 待機ロジック終了 ★★★
        
        # 6. Change Setの作成
        change_set_name = f"{change_set_type}CFN-{int(time.time())}"
        
        create_params = {
            'StackName': stack_name,
            'TemplateURL': template_url,
            'Parameters': [{'ParameterKey': 'TransitGatewayId', 'ParameterValue': tgw_id}],
            'Capabilities': ['CAPABILITY_NAMED_IAM'],
            'ChangeSetName': change_set_name,
            'ChangeSetType': change_set_type,
            # 'Tags': [{'Key': 'AutoGenerated', 'Value': 'TGW-Router-CFn'}] <--- 既存コードを尊重し、タグは含めない
        }
        
        response = cfn_client.create_change_set(**create_params)
        change_set_id = response['Id']
        
        # 7. Change Setの完了を待機
        wait_for_change_set(cfn_client, stack_name, change_set_id, request_id)

        # 8. Change Setの実行
        print(f"[INFO] {request_id} Executing Change Set {change_set_id} for stack {stack_name}...")
        cfn_client.execute_change_set(
            ChangeSetName=change_set_id,
            StackName=stack_name
        )
        print(f"[INFO] {request_id} Successfully initiated CFn Execute Change Set operation.")

        # 9. スタック実行の完了を待機
        wait_for_stack_operation(cfn_client, stack_name, change_set_type, request_id)
        
        return {
            'statusCode': 200,
            'body': json.dumps(f"CFN Deployment successful for stack {stack_name}.")
        }

    except Exception as e:
        error_msg = (
            f"CFn Deployment failed: {e}. "
            f"Check AssumeRole permissions for {cfn_assume_role_name} "
            f"in account {target_account_id if target_account_id else 'N/A'}."
        )
        print(f"[ERROR] {request_id} {error_msg}")
        # Step Functionsに失敗を伝播させるために例外を再送出
        raise e