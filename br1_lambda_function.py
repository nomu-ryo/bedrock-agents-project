import pandas as pd
import boto3
import io
import re
import json
import traceback
import logging
from collections import defaultdict
import os
from botocore.exceptions import ClientError

# ロガー設定
logger = logging.getLogger()
logger.setLevel(logging.DEBUG) 

# Boto3クライアントはグローバルに定義
s3 = boto3.client('s3')

# TGWルーティング関連のパターンと定数
TGW_ATTACH_PATTERN = re.compile(r'^tgw-attach-', re.IGNORECASE)
ONPRE_ATTACH_ID = None 

# --- 設定定数 ---
# 実行環境設定
FALLBACK_DYNAMIC_PREFIX = 'experiment' 
FALLBACK_SOURCE_BUCKET = 'transitgateway-automation-rag' 
FALLBACK_SOURCE_KEY = 'settingsheets/AI-Setting sheet-20250930(vpc-routing).xlsx' 
FALLBACK_TARGET_KEY_SUFFIX = 'extractsheet/tgw_config.jsonl' 
FALLBACK_MAPPING_KEY_SUFFIX = 'extractsheet/tgw_mapping_table.jsonl' 
# TGW IDとAccount IDのマッピングキーを再利用
FALLBACK_TGW_ID_CONFIG_KEY_SUFFIX = 'extractsheet/tgw_id_config.jsonl' 
FALLBACK_SOURCE_SHEET_NAME = 'X-HUBマネージド(東京リージョン)'
FALLBACK_MAPPING_BUCKET = 'transitgateway-automation-rag' 

# Assume Role のサフィックス
TGW_ASSUME_ROLE_SUFFIX = "-prd-gcopm-bedrock-agent-role" 

# TGW 操作に必要なリージョン (固定)
TGW_REGION = 'ap-northeast-1' 

# onpre RTBの固定サフィックスを定義 (dynamic_prefixに続く部分)
ONPRE_RTB_SUFFIX = "-prd-tokyo-gcopm-onpre-rtb"

def get_dynamic_rtb_patterns(dynamic_prefix):
    """実行時のdynamic_prefixに基づき、動的なRTB関連のパターンと名前を生成する"""
    prefix_escaped = re.escape(dynamic_prefix)
    rtb_name_pattern_dynamic = re.compile(rf'{prefix_escaped}-prd-tokyo-asp(\d{{2}})-(\d{{2}})-tgw-rtb', re.IGNORECASE)
    
    rtb_onpre_dynamic = f"{dynamic_prefix}{ONPRE_RTB_SUFFIX}"
    
    rtb_onpre_propagate_dynamic = rtb_onpre_dynamic 
    return rtb_name_pattern_dynamic, rtb_onpre_dynamic, rtb_onpre_propagate_dynamic

def get_unique_key(record):
    """TGW設定レコードから一意キー (重複排除用) を生成する"""
    return (
        record.get("task_id"), 
        record.get("rtb_name"), 
        record.get("target_attachment_id")
    )

def extract_agent_params(event):
    """Bedrock Agentのペイロードからパラメータを安全に抽出する"""
    extracted_params = {}
    properties_list = event.get('requestBody', {}).get('content', {}).get('application/json', {}).get('properties', [])
    if properties_list:
        extracted_params = {
            prop['name']: prop.get('value', '') 
            for prop in properties_list 
            if 'name' in prop 
        }
    elif not event.get('requestBody'):
        extracted_params = event

    params = {
        'dynamic_prefix': extracted_params.get('dynamic_prefix', FALLBACK_DYNAMIC_PREFIX),
        'source_bucket': extracted_params.get('source_bucket', FALLBACK_SOURCE_BUCKET),
        'source_key': extracted_params.get('source_key', FALLBACK_SOURCE_KEY),
        'source_sheet_name': extracted_params.get('source_sheet_name', FALLBACK_SOURCE_SHEET_NAME),
        'mapping_bucket': extracted_params.get('mapping_bucket', FALLBACK_MAPPING_BUCKET),
    }

    if params['dynamic_prefix'] == FALLBACK_DYNAMIC_PREFIX:
        target_key_val = extracted_params.get('target_key')
        if target_key_val and '/' in target_key_val:
            extracted_prefix = target_key_val.split('/')[0]
            if extracted_prefix:
                params['dynamic_prefix'] = extracted_prefix
                logger.info(f"🔑 Dynamic Prefix Overridden from target_key: {params['dynamic_prefix']}")
    
    return params

def resolve_s3_keys(params):
    """パラメータから最終的なS3キーパスを決定する"""
    dynamic_prefix = params['dynamic_prefix']
    source_bucket = params['source_bucket']
    source_key = params['source_key']
    
    params['target_key'] = f"{dynamic_prefix}/{FALLBACK_TARGET_KEY_SUFFIX}" 
    params['mapping_key'] = f"{dynamic_prefix}/{FALLBACK_MAPPING_KEY_SUFFIX}" 
    # 💡 TGW ID config の S3 キーを再利用
    params['tgw_id_config_key'] = f"{dynamic_prefix}/{FALLBACK_TGW_ID_CONFIG_KEY_SUFFIX}"
    
    params['target_bucket'] = source_bucket
    params['s3_config_key'] = f"s3://{source_bucket}/{params['target_key']}"
    
    full_source_key = source_key
    
    if 'settingsheets/' not in full_source_key and not full_source_key.startswith(dynamic_prefix):
        full_source_key = f"settingsheets/{full_source_key}"
        logger.info(f"🔑 Source Key resolved (Prepended missing 'settingsheets/' folder): {full_source_key}")

    if dynamic_prefix and not full_source_key.startswith(dynamic_prefix):
        full_source_key = f"{dynamic_prefix}/{full_source_key}"
        full_source_key = full_source_key.replace('//', '/')
        logger.info(f"🔑 Source Key resolved (Prepended Prefix): {full_source_key}")
    
    params['full_source_key'] = full_source_key
    
    return params

# -------------------------------------------------------------
# TGWオーナーアカウントでAssume Roleし、VPCアタッチメントのオーナーアカウントIDを取得する関数 (変更なし)
# -------------------------------------------------------------
def resolve_tgw_attachment_owner_account(tgw_owner_account_id, tgw_attach_id, dynamic_prefix):
    """
    TGWオーナーアカウントでAssume Roleし、指定されたTGWアタッチメントの
    ResourceOwnerId (VPCオーナーアカウントID) を取得する。
    """
    
    target_role_name = dynamic_prefix + TGW_ASSUME_ROLE_SUFFIX
    role_arn = f"arn:aws:iam::{tgw_owner_account_id}:role/{target_role_name}"
    
    logger.info(f"Attempting to assume TGW Owner Role to resolve VPC Owner ID. TGW Owner Account: {tgw_owner_account_id}, Role: {role_arn}")
    
    sts = boto3.client('sts', region_name=TGW_REGION) 
    try:
        assumed_role = sts.assume_role(
            RoleArn=role_arn,
            RoleSessionName="TGWOwnerResolutionSession"
        )
        credentials = assumed_role['Credentials']
        logger.info(f"✅ Assume TGW Owner Role successful for VPC Owner ID resolution.")
    except ClientError as e:
        error_trace = traceback.format_exc()
        logger.error(f"❌ Assume TGW Owner Role failed for account {tgw_owner_account_id}: {e}")
        logger.error(f"❌ Full Traceback of AssumeRole failure:\n{error_trace}")
        return None, f"Assume Role failed (TGW Owner): {e}"
    except Exception as e:
        logger.error(f"❌ Unexpected error during Assume TGW Owner Role: {e}")
        return None, f"Unexpected error during Assume Role: {e}"
        
    # 2. VPC Owner IDを取得
    ec2 = boto3.client(
        'ec2',
        region_name=TGW_REGION, 
        aws_access_key_id=credentials['AccessKeyId'],
        aws_secret_access_key=credentials['SecretAccessKey'],
        aws_session_token=credentials['SessionToken']
    )
    
    try:
        response = ec2.describe_transit_gateway_attachments(
            TransitGatewayAttachmentIds=[tgw_attach_id]
        )
        attachments = response.get('TransitGatewayAttachments')
        
        if attachments:
            vpc_owner_id = attachments[0].get('ResourceOwnerId')
            if vpc_owner_id:
                logger.info(f"✅ Resolved VPC Owner ID for {tgw_attach_id}: {vpc_owner_id}")
                return vpc_owner_id, None
            else:
                return None, f"ResourceOwnerId not found for attachment {tgw_attach_id}."
        else:
            return None, f"TGW Attachment {tgw_attach_id} not found."

    except ClientError as e:
        error_trace = traceback.format_exc()
        logger.error(f"❌ DescribeTGWAttachments failed for {tgw_attach_id}: {e}")
        logger.error(f"❌ Full Traceback:\n{error_trace}")
        return None, f"DescribeTGWAttachments failed: {e}"
    except Exception as e:
        logger.error(f"❌ Unexpected error during TGW Attachment description: {e}")
        return None, f"Unexpected error during description: {e}"

# -------------------------------------------------------------
# TGWオーナーアカウントIDでAssume Roleし、タグ付けを実行する関数 (変更なし)
# -------------------------------------------------------------
def tag_tgw_attachment_via_tgw_owner_account(tgw_owner_account_id, tgw_attach_id, rtb_name, dynamic_prefix):
    """
    TGWオーナーアカウントの権限でAssume Roleし、TGWアタッチメントにタグ付けを行う。
    """
    
    target_role_name = dynamic_prefix + TGW_ASSUME_ROLE_SUFFIX
    # 💡 TGWオーナーアカウントIDを使用してロールARNを構築
    role_arn = f"arn:aws:iam::{tgw_owner_account_id}:role/{target_role_name}"
    
    logger.info(f"Attempting to assume TGW Owner Role for tagging. TGW Owner Account: {tgw_owner_account_id}, Role: {role_arn}")
    
    # 1. Assume Role を実行し、一時認証情報を取得
    sts = boto3.client('sts', region_name=TGW_REGION) 
    try:
        assumed_role = sts.assume_role(
            RoleArn=role_arn,
            RoleSessionName="TGWAttachmentTaggingSession"
        )
        credentials = assumed_role['Credentials']
        # 💡 TGWオーナーアカウントでAssume Roleしたことをログに記録
        logger.info(f"✅ Assume TGW Owner Role successful for account: {tgw_owner_account_id}.")
    except ClientError as e:
        error_trace = traceback.format_exc()
        logger.error(f"❌ AssumeRole failed for TGW Owner account {tgw_owner_account_id} (Role: {role_arn}): {e}")
        logger.error(f"❌ Full Traceback of AssumeRole failure:\n{error_trace}")
        return False
    except Exception as e:
        error_trace = traceback.format_exc()
        logger.error(f"❌ Unexpected error during Assume TGW Owner Role: {e}")
        logger.error(f"❌ Full Traceback of unexpected AssumeRole error:\n{error_trace}")
        return False
        
    # 2. TGW Attachment ID から新しい名前を決定 (rtb -> attach)
    new_attach_name = re.sub(r'-rtb$', '-attach', rtb_name)
    if new_attach_name == rtb_name:
         logger.warning(f"⚠️ RTB name '{rtb_name}' did not match '-rtb$' suffix. Using original name for Name tag.")
         new_attach_name = rtb_name 

    # 3. EC2クライアントを初期化し、タグ付けを実行
    ec2 = boto3.client(
        'ec2',
        region_name=TGW_REGION, 
        aws_access_key_id=credentials['AccessKeyId'],
        aws_secret_access_key=credentials['SecretAccessKey'],
        aws_session_token=credentials['SessionToken']
    )
    
    tags = [
        {'Key': 'Env', 'Value': 'prd'},
        {'Key': 'System', 'Value': 'gcopm'},
        {'Key': 'Name', 'Value': new_attach_name},
    ]
    
    logger.info(f"Starting ec2:CreateTags for TGW Attachment: {tgw_attach_id} using TGW Owner Account credentials.")

    try:
        ec2.create_tags(Resources=[tgw_attach_id], Tags=tags)
        logger.info(f"✅ Tagging complete for {tgw_attach_id} via TGW Owner account {tgw_owner_account_id}. New Name: {new_attach_name}")
        return True
    except ClientError as e:
        error_trace = traceback.format_exc()
        logger.error(f"❌ Tagging failed for {tgw_attach_id} via TGW Owner account {tgw_owner_account_id}: {e}")
        logger.error(f"❌ Full Traceback of Tagging failure:\n{error_trace}")
        return False
    except Exception as e:
        error_trace = traceback.format_exc()
        logger.error(f"❌ Unexpected error during Tagging for {tgw_attach_id}: {e}")
        logger.error(f"❌ Full Traceback of unexpected Tagging error:\n{error_trace}")
        return False

# -------------------------------------------------------------
# load_target_account_id (TGWオーナーアカウントIDを取得) は変更なし
# -------------------------------------------------------------
def load_target_account_id(bucket, key):
    """S3からTGWオーナーアカウントIDをロードする"""
    account_id = None
    logger.info(f"Starting to load TGW Owner Account ID from s3://{bucket}/{key}")
    
    try:
        s3_object = s3.get_object(Bucket=bucket, Key=key)
        jsonl_content = s3_object['Body'].read().decode('utf-8')
        
        # JSONLの最初の有効な行からアカウントIDを抽出
        first_line = jsonl_content.strip().split('\n')[0]
        if not first_line:
            logger.error(f"❌ TGW ID config file s3://{bucket}/{key} is empty.")
            return None
        
        record = json.loads(first_line)
        
        # 💡 "account_id" または "account id" キーの値を抽出
        account_key = next((k for k in record if 'account' in k.lower()), None)
        
        if account_key:
            account_id = str(record[account_key]).strip()
            logger.info(f"✅ Loaded TGW Owner Account ID from JSONL: {account_id}")
        else:
            logger.error(f"❌ TGW Owner Account ID key (e.g., 'account_id') is missing in the first record of {key}. Content: {record}")
            
    except s3.exceptions.NoSuchKey:
        logger.error(f"FATAL: TGW Owner Account ID config file s3://{bucket}/{key} NOT found. Tagging will be skipped.")
    except ClientError as e:
        error_trace = traceback.format_exc()
        logger.error(f"ERROR: TGW Owner Account ID config S3 ClientErrorが発生しました: {e}")
        logger.error(f"❌ Full Traceback of Account ID Config Load failure:\n{error_trace}")
    except Exception as e:
        error_trace = traceback.format_exc()
        logger.error(f"ERROR: TGW Owner Account ID configファイルの読み込み中に予期せぬエラーが発生しました: {e}")
        logger.error(f"❌ Full Traceback of Account ID Config Load failure:\n{error_trace}")
        
    return account_id

# (後続のユーティリティ関数は変更なし)

def load_mapping_table(bucket, key, rtb_name_pattern_dynamic, rtb_onpre_dynamic):
    """S3からマッピングテーブルをロードし、RTB採番ステータスとオンプレミスIDを更新する (既存関数)"""
    global ONPRE_ATTACH_ID 
    
    mapping = {}
    rtb_naming_status = defaultdict(lambda: defaultdict(int))
    jsonl_content = ""
    
    logger.info(f"Starting to load mapping file from s3://{bucket}/{key}")
    
    try:
        s3_object = s3.get_object(Bucket=bucket, Key=key)
        jsonl_content = s3_object['Body'].read().decode('utf-8')
        
        for line in jsonl_content.strip().split('\n'):
            if not line: continue
            record = json.loads(line)
            
            if 'tgw-attach-id' in record:
                mapping[record['tgw-attach-id']] = record
            
            if record.get('rtb-name') == rtb_onpre_dynamic and 'tgw-attach-id' in record:
                ONPRE_ATTACH_ID = record['tgw-attach-id']
            
            match = rtb_name_pattern_dynamic.search(record.get('rtb-name', ''))
            if match and 'account-id' in record:
                asp_xx = int(match.group(1))
                asp_yy = int(match.group(2))
                acc_id = record['account-id']
                # ロジックの維持
                if asp_yy > rtb_naming_status[acc_id][asp_xx]:
                    rtb_naming_status[acc_id][asp_xx] = asp_yy
                elif asp_yy == rtb_naming_status[acc_id][asp_xx] and asp_xx > list(rtb_naming_status[acc_id].keys())[-1]:
                    rtb_naming_status[acc_id][asp_xx] = asp_yy
                            
    except s3.exceptions.NoSuchKey:
        logger.info(f"INFO: マッピングファイル s3://{bucket}/{key} が見つかりません。新規作成します。")
    except ClientError as e:
        error_trace = traceback.format_exc()
        logger.error(f"ERROR: マッピングファイルのS3 ClientErrorが発生しました: {e}")
        logger.error(f"❌ Full Traceback of Mapping Load failure:\n{error_trace}")
    except Exception as e:
        error_trace = traceback.format_exc()
        logger.error(f"ERROR: マッピングファイルの読み込み中に予期せぬエラーが発生しました: {e}")
        logger.error(f"❌ Full Traceback of Mapping Load failure:\n{error_trace}")
        
    return mapping, rtb_naming_status, jsonl_content.strip()

def generate_new_rtb_name(dynamic_prefix, account_id, rtb_naming_status):
    """新しい RTB 名 (aspXX-YY) を生成する (既存関数)"""
    all_xx = set()
    for xx_groups in rtb_naming_status.values():
        all_xx.update(xx_groups.keys())
    
    is_new_system = not bool(all_xx)
    
    if is_new_system:
        new_asp_xx = 1
        new_asp_yy = 1
    elif account_id in rtb_naming_status:
        max_yy_in_account = 0
        target_asp_xx = 0 
        for asp_xx, max_yy_for_xx in rtb_naming_status[account_id].items():
            if max_yy_in_account < max_yy_for_xx:
                max_yy_in_account = max_yy_for_xx
                target_asp_xx = asp_xx
            elif max_yy_for_xx == max_yy_in_account and asp_xx > target_asp_xx:
                target_asp_xx = asp_xx
        
        new_asp_xx = target_asp_xx if target_asp_xx > 0 else 1 
        new_asp_yy = max_yy_in_account + 1 
    else:
        max_xx_overall = max(all_xx) if all_xx else 0
        new_asp_xx = max_xx_overall + 1
        new_asp_yy = 1
    
    return f"{dynamic_prefix}-prd-tokyo-asp{new_asp_xx:02d}-{new_asp_yy:02d}-tgw-rtb"

def extract_prefix_from_rtb(rtb_name, rtb_name_pattern_dynamic, rtb_onpre_dynamic):
    """RTB名からTGWタスクIDに使う 'ASPXX-YY' または 'ONPRE' のプレフィックスを抽出する"""
    match = rtb_name_pattern_dynamic.search(rtb_name)
    if match:
        asp_xx = match.group(1)
        asp_yy = match.group(2)
        return f"ASP{asp_xx}-{asp_yy}"
    
    if rtb_name == rtb_onpre_dynamic:
        return "ONPRE"
        
    return "UNKNOWN" 

def get_prefix_from_attachment_id(attachment_id, final_mapping, actual_onpre_attach_id, rtb_name_pattern_dynamic, rtb_onpre_dynamic):
    """Attachment ID に対応する RTB プレフィックス ('ASPXX-YY' or 'ONPRE') を取得する"""
    if attachment_id == actual_onpre_attach_id:
        return "ONPRE"
    
    rtb_name = final_mapping.get(attachment_id, {}).get('rtb-name')
    if rtb_name:
        return extract_prefix_from_rtb(rtb_name, rtb_name_pattern_dynamic, rtb_onpre_dynamic)
        
    return "UNKNOWN"

def build_agent_response(agent_info, body_message, response_state, additional_params=None):
    """Bedrock Agentが期待する厳密なJSON応答構造を生成します。"""
    if additional_params is None: additional_params = {}
        
    response_body_content = {
        'body': body_message 
    }
    if 's3_config_key' in additional_params:
        response_body_content['s3_config_key'] = additional_params['s3_config_key']

    return {
        'messageVersion': '1.0',
        'response': {
            'actionGroup': agent_info['actionGroup'],
            'apiPath': agent_info['apiPath'],
            'httpMethod': agent_info['httpMethod'],
            'functionResponse': {
                'responseState': response_state,
                'responseBody': {
                    'application/json': response_body_content
                }
            }
        }
    }


# --- Lambda ハンドラ ---

def extractTGWConfig(event, context):
    global ONPRE_ATTACH_ID
    
    agent_info = {
        'apiPath': event.get('apiPath', '/extractTGWConfig'), 
        'httpMethod': event.get('httpMethod', 'POST'),
        'actionGroup': event.get('actionGroup', 'data-extraction') 
    }
    
    logger.info("--- Starting extractTGWConfig (TGW Owner Tagging Logic) ---")
    
    try:
        # --- 1. パラメータの抽出とS3キーの解決 ---
        logger.info("STEP 1: Parameter extraction and S3 key resolution.")
        params = extract_agent_params(event)
        params = resolve_s3_keys(params)
        
        dynamic_prefix = params['dynamic_prefix']
        source_bucket = params['source_bucket']
        full_source_key = params['full_source_key']
        source_sheet_name = params['source_sheet_name']
        mapping_bucket = params['mapping_bucket']
        mapping_key = params['mapping_key']
        target_bucket = params['target_bucket']
        target_key = params['target_key']
        s3_config_key = params['s3_config_key']
        tgw_id_config_key = params['tgw_id_config_key'] 
        
        logger.info(f"Context: Prefix={dynamic_prefix}, Source Bucket/Key={source_bucket}/{full_source_key}, Target Config Key={s3_config_key}")
        
        rtb_name_pattern_dynamic, rtb_onpre_dynamic, rtb_onpre_propagate_dynamic = get_dynamic_rtb_patterns(dynamic_prefix)

        # --- 2. S3設定ファイルのロード ---
        logger.info("STEP 2.1: Loading TGW Attachment/RTB mapping table.")
        current_mapping, rtb_naming_status, existing_jsonl_content = load_mapping_table(
            mapping_bucket, mapping_key, rtb_name_pattern_dynamic, rtb_onpre_dynamic
        )
        actual_onpre_attach_id = ONPRE_ATTACH_ID if ONPRE_ATTACH_ID else "tgw-attach-XX_NOT_FOUND" 
        logger.info(f"Loaded {len(current_mapping)} existing mappings. ONPRE_ATTACH_ID: {actual_onpre_attach_id}")
        
        # 💡 STEP 2.2: TGW オーナーアカウントIDのロード (Assume Roleのターゲットアカウント)
        logger.info("STEP 2.2: Loading TGW Owner Account ID from JSONL config.")
        tgw_owner_account_id = load_target_account_id(mapping_bucket, tgw_id_config_key)
        
        if not tgw_owner_account_id:
             error_msg = f"❌ FATAL: Could not load the required TGW Owner Account ID from s3://{mapping_bucket}/{tgw_id_config_key}. Processing halted."
             logger.error(error_msg)
             return build_agent_response(agent_info, error_msg, 'FAILURE')

        # --- 3. Excelファイルの読み込みとデータ抽出 ---
        logger.info(f"STEP 3: Reading Excel file s3://{source_bucket}/{full_source_key}")
        try:
            s3_object = s3.get_object(Bucket=source_bucket, Key=full_source_key) 
        except s3.exceptions.NoSuchKey:
            error_msg = f"❌ INPUT FILE NOT FOUND: S3 key s3://{source_bucket}/{full_source_key} does not exist."
            logger.error(error_msg)
            return build_agent_response(agent_info, error_msg, 'FAILURE')
        
        excel_data = io.BytesIO(s3_object['Body'].read())
        import warnings
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            df = pd.read_excel(excel_data, sheet_name=source_sheet_name, header=0) 
        
        logger.info(f"Successfully read Excel sheet: {source_sheet_name}. DataFrame shape: {df.shape}")
        
        e_column_name = df.columns[4]
        c_column_name = df.columns[2] 

        matching_rows = df[
            df[e_column_name].astype(str).str.match(TGW_ATTACH_PATTERN, na=False)
        ]
        
        if matching_rows.empty:
            warning_msg = "TGW Attachment ID行がありません。処理をスキップします。"
            logger.warning(warning_msg)
            placeholder_key = f"{target_bucket}/empty/tgw_config_placeholder.jsonl"
            return build_agent_response(agent_info, warning_msg, 'SUCCESS', {'s3_config_key': placeholder_key})
        
        # --- 4. マッピングテーブルの採番/更新処理 & VPCオーナーアカウントの特定 ---
        logger.info(f"STEP 4: Processing {len(matching_rows)} matching rows for mapping/tagging.")
        new_attachments_to_map = {} 
        new_jsonl_lines = []
        is_onprem_example_skipped = False
        is_vpc_example_skipped = False
        
        # 💡 新規マッピングとタグ付けが必要なアタッチメントを一時的に保持
        attachments_to_tag = {} 
        
        for index, row in matching_rows.iterrows():
            target_tgw_id_e = str(row[e_column_name]).strip()
            target_tgw_id_c = str(row[c_column_name]).strip()
            is_vpc_routing = TGW_ATTACH_PATTERN.match(target_tgw_id_c) 
            
            # 例外行のスキップ (ロジック維持)
            if (is_vpc_routing and not is_vpc_example_skipped):
                is_vpc_example_skipped = True
                continue
            if (not is_vpc_routing and not is_onprem_example_skipped):
                is_onprem_example_skipped = True
                continue
            
            # 💡 新規TGW Attachment IDの検出とRTB名生成 (C列の値が空でないこともトリガー条件に含まれる)
            if (
                not is_vpc_routing 
                and not pd.isna(row[c_column_name]) 
                and target_tgw_id_e not in current_mapping 
                and target_tgw_id_e not in new_attachments_to_map
            ):
                # 💡 TGWオーナーアカウントIDを使って、VPCアタッチメントのオーナーアカウントIDを特定する
                vpc_owner_account_id, error = resolve_tgw_attachment_owner_account(tgw_owner_account_id, target_tgw_id_e, dynamic_prefix)

                if not vpc_owner_account_id:
                    logger.error(f"❌ Skipping mapping for {target_tgw_id_e} due to failure in resolving VPC Owner ID: {error}")
                    continue
                    
                # RTB名の生成は、特定されたVPCオーナーアカウントIDを使用する
                new_rtb_name = generate_new_rtb_name(dynamic_prefix, vpc_owner_account_id, rtb_naming_status)
                
                # rtb_naming_statusを更新 (採番後の新しいステータスを反映)
                match = rtb_name_pattern_dynamic.search(new_rtb_name)
                if match:
                    asp_xx = int(match.group(1))
                    asp_yy = int(match.group(2))
                    # 💡 採番ステータスは、VPCオーナーアカウントIDで管理される
                    rtb_naming_status[vpc_owner_account_id][asp_xx] = asp_yy
                         
                new_record = {
                    # 💡 マッピングテーブルの 'account-id' に特定されたVPCオーナーアカウントIDを設定 (変更なし)
                    'account-id': vpc_owner_account_id,
                    'tgw-attach-id': target_tgw_id_e,
                    'rtb-name': new_rtb_name
                }
                new_attachments_to_map[target_tgw_id_e] = new_record
                
                # 💡 JSONLのフォーマットを既存の行に合わせるため、スペースを削除してコンパクトに出力
                new_jsonl_lines.append(json.dumps(new_record, separators=(',', ':')))
                
                # タグ付け候補に追加 
                attachments_to_tag[target_tgw_id_e] = new_record
                
        # マッピングテーブルのS3更新
        if new_jsonl_lines:
            logger.info(f"Found {len(new_attachments_to_map)} new attachments. Updating mapping table.")
            final_jsonl_content = existing_jsonl_content + '\n' + '\n'.join(new_jsonl_lines) + '\n' if existing_jsonl_content else '\n'.join(new_jsonl_lines) + '\n'
            
            # マッピングテーブルS3 PUT
            s3.put_object(
                Bucket=mapping_bucket, 
                Key=mapping_key, 
                Body=final_jsonl_content.encode('utf-8'), 
                ContentType='application/jsonl'
            )
            logger.info(f"✅ Mapping table updated successfully. S3 Key: s3://{mapping_bucket}/{mapping_key}")
            
            final_mapping = {**current_mapping, **new_attachments_to_map}
            
            # 💡 TGW タグ付けの実行 (TGWオーナーアカウント経由)
            logger.info(f"✨ Starting cross-account tagging for {len(attachments_to_tag)} new attachments using TGW Owner Account.")
            for attach_id, record in attachments_to_tag.items():
                rtb_name = record['rtb-name']
                
                # 💡 TGWオーナーアカウントIDを渡してAssume Roleとタグ付けを実行
                tag_tgw_attachment_via_tgw_owner_account(tgw_owner_account_id, attach_id, rtb_name, dynamic_prefix)
        else:
            final_mapping = current_mapping 
            logger.info("No new TGW Attachments found for mapping update and tagging.")

        # --- 5. TGW Config JSONLレコードの生成と重複排除 (ロジック変更なし) ---
        logger.info("STEP 5: Generating TGW Config Records.")
        all_jsonl_records = []
        is_onprem_example_skipped = False 
        is_vpc_example_skipped = False
        onpre_associate_added = False 
        
        for index, row in matching_rows.iterrows():
            target_tgw_id_e = str(row[e_column_name]).strip()
            target_tgw_id_c = str(row[c_column_name]).strip()
            is_vpc_routing = TGW_ATTACH_PATTERN.match(target_tgw_id_c)
            
            # 例外行のスキップ (ロジック維持)
            if (is_vpc_routing and not is_vpc_example_skipped):
                is_vpc_example_skipped = True
                continue
            if (not is_vpc_routing and not is_onprem_example_skipped):
                is_onprem_example_skipped = True
                continue

            if not is_vpc_routing:
                # --- VPC <-> On-Prem のルーティング設定 ---
                
                asp_rtb_name = final_mapping.get(target_tgw_id_e, {}).get('rtb-name', f'{dynamic_prefix}-prd-tokyo-aspXX-YY-tgw-rtb-FALLBACK')
                asp_prefix = extract_prefix_from_rtb(asp_rtb_name, rtb_name_pattern_dynamic, rtb_onpre_dynamic)
                
                onpre_rtb_name = rtb_onpre_propagate_dynamic 
                onpre_prefix = extract_prefix_from_rtb(onpre_rtb_name, rtb_name_pattern_dynamic, rtb_onpre_dynamic)

                if not onpre_associate_added:
                    all_jsonl_records.append({"task_id": "TGW_ONPRE_ASSOCIATE", "rtb_name": onpre_rtb_name, "attachment_id": actual_onpre_attach_id, "target_attachment_id": None, "action": "associate"})
                    onpre_associate_added = True
                
                all_jsonl_records.extend([
                    {"task_id": f"TGW_{asp_prefix}_ASSOCIATE", "rtb_name": asp_rtb_name, "attachment_id": target_tgw_id_e, "target_attachment_id": None, "action": "associate"},
                    {"task_id": f"TGW_{onpre_prefix}_PROPAGATE", "rtb_name": asp_rtb_name, "attachment_id": None, "target_attachment_id": actual_onpre_attach_id, "action": "propagate"},
                    {"task_id": f"TGW_{asp_prefix}_PROPAGATE", "rtb_name": onpre_rtb_name, "attachment_id": None, "target_attachment_id": target_tgw_id_e, "action": "propagate"}
                ])

            else:
                # --- VPC-VPC 相互伝播の設定 ---
                
                prefix_c = get_prefix_from_attachment_id(target_tgw_id_c, final_mapping, actual_onpre_attach_id, rtb_name_pattern_dynamic, rtb_onpre_dynamic)
                prefix_e = get_prefix_from_attachment_id(target_tgw_id_e, final_mapping, actual_onpre_attach_id, rtb_name_pattern_dynamic, rtb_onpre_dynamic)

                rtb_name_c = final_mapping.get(target_tgw_id_c, {}).get('rtb-name', f'{dynamic_prefix}-prd-tokyo-aspXX-YY-tgw-rtb-FALLBACK-C')
                rtb_name_e = final_mapping.get(target_tgw_id_e, {}).get('rtb-name', f'{dynamic_prefix}-prd-tokyo-aspXX-YY-tgw-rtb-FALLBACK-E')

                all_jsonl_records.extend([
                    {"task_id": f"TGW_{prefix_e}_PROPAGATE", "rtb_name": rtb_name_c, "attachment_id": target_tgw_id_c, "target_attachment_id": target_tgw_id_e, "action": "propagate"},
                    {"task_id": f"TGW_{prefix_c}_PROPAGATE", "rtb_name": rtb_name_e, "attachment_id": target_tgw_id_e, "target_attachment_id": target_tgw_id_c, "action": "propagate"}
                ])
                
        # 既存ファイルのロードと新規レコードの統合 (重複排除)
        logger.info(f"Total generated records before deduplication: {len(all_jsonl_records)}")
        unique_records = {} 
        try:
            s3_object = s3.get_object(Bucket=target_bucket, Key=target_key)
            existing_config_content = s3_object['Body'].read().decode('utf-8').strip()
            for line in existing_config_content.split('\n'):
                if line.strip():
                    record = json.loads(line)
                    unique_records[get_unique_key(record)] = record
            logger.info(f"Loaded {len(unique_records)} unique records from existing config.")
        except s3.exceptions.NoSuchKey:
            logger.info("No existing TGW config found. Creating new file.")
        except ClientError as e:
            logger.warning(f"Warning: Could not read existing TGW config file (ClientError). Error: {e}")
        except Exception as e:
            logger.warning(f"Warning: Could not read existing TGW config file (Unexpected Error). Error: {e}")

        for new_record in all_jsonl_records:
            unique_records[get_unique_key(new_record)] = new_record
        
        # S3への最終書き込み
        logger.info("STEP 6: Writing Final TGW Config to S3.")
        final_records_list = list(unique_records.values())
        # 💡 TGW configのJSONLもコンパクトな形式で出力
        final_jsonl_content = '\n'.join([json.dumps(rec, separators=(',', ':')) for rec in final_records_list]) + '\n'
        total_records = len(final_records_list)
        
        s3.put_object(
            Bucket=target_bucket,
            Key=target_key,
            Body=final_jsonl_content.encode('utf-8'), 
            ContentType='application/jsonl'
        )
        
        logger.info(f"✅ S3 write complete. Key: {target_key}. Total unique records: {total_records}.")
        
        # --- 最終成功応答 ---
        success_message = f"TGW routing config successfully extracted, new attachments resolved and tagged (if any), and saved to S3. Key: {s3_config_key}. Total unique records: {total_records}."
        logger.info("--- Finished extractTGWConfig successfully ---")
        
        return build_agent_response(
            agent_info,
            success_message,
            'SUCCESS', 
            {'s3_config_key': s3_config_key}
        )

    # -----------------------------------------------------------------
    # 🚨 ULTIMATE CATCH BLOCK
    # -----------------------------------------------------------------
    except Exception as e:
        error_trace = traceback.format_exc()
        error_message = f"❌ FATAL UNHANDLED ERROR in Action 1: {e}. Check CloudWatch for full stack trace."
        logger.error(error_message)
        logger.error(f"❌ FULL TRACEBACK:\n{error_trace}") 
        
        logger.info("--- Finished extractTGWConfig with fatal error ---")
        
        return build_agent_response(
            agent_info,
            error_message,
            'FAILURE'
        )