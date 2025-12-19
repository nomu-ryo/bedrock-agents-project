import json
import logging
import os
import traceback
import re
from io import StringIO
from typing import Dict, List, Any, Set, Tuple, Union

import boto3
from botocore.exceptions import ClientError
import yaml

# --- 1. 定数の定義と初期設定 ---
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# S3バケット名は環境変数から取得
YAML_BUCKET = os.environ.get("YAML_BUCKET", "transitgateway-automation-rag")

# TGWオーナーアカウントでAssumeRoleするロール名のサフィックス部分を定義 (固定部分)
# dynamic_prefix + TGW_ASSUME_ROLE_SUFFIX で完全なロール名が構築される
TGW_ASSUME_ROLE_SUFFIX = "-your-org-tgw-admin-role"

# TGW設定ファイルのS3パスを環境変数から取得 (動的に構築するため、この環境変数はもはや必須ではありません)
TGW_CONFIG_S3_PATH = os.environ.get("TGW_CONFIG_S3_PATH")

# ファイル名定数
MAPPING_TABLE_FILENAME = "tgw_mapping_table.jsonl"
CFN_YAML_FILENAME = "tgw_routing_cfn.yaml"
TASK_JSONL_FILENAME = "tgw_config.jsonl"
# CFnリソースインポート用のマッピングJSONファイル名
IMPORT_MAPPING_FILENAME = "cfn_import_mapping.json"
# TGW ID設定ファイルの入力ファイル名 (パスを動的に構築するために使用)
TGW_CONFIG_INPUT_FILENAME = "tgw_id_config.jsonl"

# TGW伝播がアクティブな状態を示すAWS APIステータス
ACTIVE_PROPAGATION_STATES = ['enabled', 'propagated']

# TGW設定ファイルから取得されるキー名
TGW_ID_KEY = 'tgw_id'
ACCOUNT_ID_KEY = 'account id' 

# Boto3クライアント（グローバルに定義）
s3 = boto3.client('s3')
sts = boto3.client('sts') 

# YAMLセクション区切りコメント
RTB_SEP = "\n# =========================================================================\n# --- TransitGatewayRouteTable Resources ---\n# =========================================================================\n"
ASSOC_SEP = "\n# =========================================================================\n# --- TransitGatewayRouteTableAssociation Resources ---\n# =========================================================================\n"
PROP_SEP = "\n# =========================================================================\n# --- TransitGatewayRouteTablePropagation Resources ---\n# =========================================================================\n"


# --- 2. 補助関数 ---
def upload_to_s3(bucket: str, key: str, data: str, content_type: str = 'application/json') -> None:
    """S3にデータをアップロードする共通関数"""
    try:
        s3.put_object(
            Bucket=bucket,
            Key=key,
            Body=data.encode('utf-8'),
            ContentType=content_type
        )
        logger.info(f"Successfully uploaded data to s3://{bucket}/{key}")
    except Exception as e:
        logger.error(f"Error uploading to S3: {e}")
        raise

def get_tgw_config_from_s3(s3_path: str) -> Dict[str, str]:
    """S3パスからTGW IDとアカウントIDを含むJSONLファイルを読み込み、最初のレコードを取得する"""
    try:
        if not s3_path.startswith("s3://"):
            raise ValueError("S3 path must start with s3://")
        
        # s3://bucket/key の形式からバケット名とキーを抽出
        s3_parts = s3_path[5:].split('/', 1)
        bucket = s3_parts[0]
        key = s3_parts[1]

        response = s3.get_object(Bucket=bucket, Key=key)
        # BodyはJSONLなので、一行目を読み込む
        content = response['Body'].read().decode('utf-8').splitlines()[0]
        
        # JSONLをパース
        config_data = json.loads(content)
        
        tgw_id = config_data.get(TGW_ID_KEY)
        account_id = config_data.get(ACCOUNT_ID_KEY) 

        if not tgw_id or not account_id:
            raise ValueError(f"Required keys ('{TGW_ID_KEY}' and '{ACCOUNT_ID_KEY}') not found in JSONL content: {content}")
        
        if not re.match(r'^tgw-[0-9a-f]{17}$', tgw_id):
            logger.warning(f"Extracted TGW ID '{tgw_id}' does not look like a valid TGW ID. Proceeding anyway.")
            
        logger.info(f"Successfully retrieved TGW ID: {tgw_id} and TGW Owner Account ID: {account_id} from {s3_path}")
        
        return {
            TGW_ID_KEY: tgw_id,
            ACCOUNT_ID_KEY: account_id
        }

    except ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchKey':
            raise FileNotFoundError(f"S3 file not found: {s3_path}") from e
        raise
    except Exception as e:
        logger.error(f"Error reading TGW config from S3 {s3_path}: {e}")
        raise

def assume_cross_account_role(account_id: str, role_name: str, session_name: str = "TGWExtractorSession") -> boto3.client:
    """別アカウントのIAMロールを引き受ける (AssumeRole)"""
    role_arn = f'arn:aws:iam::{account_id}:role/{role_name}'
    logger.info(f"Attempting to assume role: {role_arn} in account {account_id}")
    
    try:
        response = sts.assume_role(
            RoleArn=role_arn,
            RoleSessionName=session_name
        )
        credentials = response['Credentials']
        
        return boto3.client(
            'ec2',
            aws_access_key_id=credentials['AccessKeyId'],
            aws_secret_access_key=credentials['SecretAccessKey'],
            aws_session_token=credentials['SessionToken'],
            region_name='ap-northeast-1' 
        )
        
    except ClientError as e:
        error_code = e.response.get('Error', {}).get('Code')
        if error_code == 'AccessDenied':
            logger.error(f"Access Denied when trying to assume role {role_arn}. Check Lambda Execution Role permissions for sts:AssumeRole.")
        else:
            logger.error(f"Error assuming role {role_arn}: {e}")
        raise
    except Exception as e:
        logger.error(f"UNEXPECTED ERROR during AssumeRole: {traceback.format_exc()}")
        raise
# -----------------------------------

# --- 3. 命名規則ヘルパー関数 ---
def to_pascal_case(name: str) -> str:
    """ハイフン、ドット、スペース、アンダースコアを削除し、PascalCaseに変換する"""
    words = re.split(r'[^a-zA-Z0-9]+', name)
    return "".join(word.capitalize() for word in words if word)

def get_rtb_cfn_name(rtb_name: str) -> str:
    """RTBのCFn論理IDを生成する (例: Hubdev801PrdTokyoAsp0101RTB)"""
    # 1. PascalCaseに変換
    pascal_name = to_pascal_case(rtb_name)
    # 2. 'rtb' の部分は常に大文字の 'RTB' にする (Rtb -> RTB)
    final_name = pascal_name.replace('Rtb', 'RTB')
    
    # 🚨 修正ロジックの追加: HubDev8... (Dが大文字) に強制的に変換
    if final_name.startswith('Hubdev'):
        final_name = final_name.replace('Hubdev', 'HubDev')
    
    return final_name

def get_attach_cfn_prefix(attach_name: str) -> str:
    """
    Association/PropagationリソースID用のシンプルなAttachment名プレフィックスを生成する (例: ONPRE, SHARED, ASP0101)。
    Attachment名に含まれる冗長なTGW/ATTACHメント/GCOPM関連の文字列を削除するよう、ロジックを強化。
    """
    # 1. すべて大文字に変換し、非英数字をアンダースコアに置換
    upper_name = re.sub(r'[^a-zA-Z0-9]+', '_', attach_name.upper()) 
    
    # 2. 環境固有の冗長なプレフィックスを削除
    cleaned_name = re.sub(r'^YOUR_PROJECT_PREFIX_?', '', upper_name)
    
    # 3. Attachment関連の冗長な部分を削除
    # TGW/ATTACH/MENTなどの文字列を削除
    cleaned_name = re.sub(r'(TGW)?_?ATTACH(MENT)?', '', cleaned_name)
    
    # 4. 'VPC' を削除
    cleaned_name = cleaned_name.replace('_VPC', '')
    
    # 💡 5. 【ご要望に基づく最終修正】Attachment名に含まれる冗長な 'GCOPM' を削除
    # GCOPM_ONPRE -> ONPRE および GCOPM_SHARED -> SHARED にするために、
    # 'GCOPM' + アンダースコア/空文字 が行頭にあれば削除します。
    # このロジックを適用することで、ONPRE/SHAREDの両方でGCOPMが削除されます。
    cleaned_name = re.sub(r'^GCOPM_?', '', cleaned_name)

    # 6. 連続するアンダースコアを一つにまとめ、両端のアンダースコアを削除
    cleaned_name = re.sub(r'_+', '_', cleaned_name).strip('_')
    
    # 7. 最後に残ったアンダースコアを削除し、全て英数字にする (cfn_resource_nameに合うよう結合)
    return re.sub(r'[^A-Z0-9]+', '', cleaned_name)
    
# --- YAMLダンプ結果のインデント処理ヘルパー関数 ---
def indent_yaml_dump(dump_string: str, spaces: int = 2) -> str:
    """
    yaml.dumpで出力された文字列全体に指定されたスペース数でインデントを適用する。
    """
    indent = " " * spaces
    indented_lines = [indent + line for line in dump_string.splitlines()]
    return "\n".join(indented_lines) + "\n"
# ------------------------------

# =================================================================
# --- 4. コアデータ抽出ロジック ---
# =================================================================

def get_tgw_configuration(tgw_id: str, ec2_client: boto3.client) -> Dict[str, Any]:
    """
    TGWのルートテーブル、アタッチメント、アソシエーション、伝播の全情報を取得する。
    ec2_clientはAssumeRoleによって生成されたクライアントを受け取る。
    """
    # ec2_client は既にAssumeRoleによりTGWアカウントの認証情報を持つ
    ec2 = ec2_client 
    config = {
        'rtbs': {},
        'attachments': {},
        'associations': {},  # {AttachmentId: RtbId}
        'propagations': {}  # {RTB_ID: {AttachmentId, ...}}
    }
    
    # --- 1. ルートテーブルの取得 ---
    rtb_response = ec2.describe_transit_gateway_route_tables(
        Filters=[
            {'Name': 'transit-gateway-id', 'Values': [tgw_id]},
            {'Name': 'state', 'Values': ['available']}
        ]
    )
    for rtb in rtb_response.get('TransitGatewayRouteTables', []):
        rtb_id = rtb['TransitGatewayRouteTableId']
        rtb_name = next((tag['Value'] for tag in rtb.get('Tags', []) if tag['Key'] == 'Name'), None)
        
        if not rtb_name:
            logger.warning(f"Skipping RTB {rtb_id} because it lacks a Name tag.")
            continue
            
        config['rtbs'][rtb_id] = {
            'RtbName': rtb_name,
            'Tags': rtb.get('Tags', []) # 抽出した既存のタグをそのまま保持
        }
        config['propagations'][rtb_id] = set() 

    # --- 2. アタッチメントとアソシエーションの取得 ---
    all_attachments_response = ec2.describe_transit_gateway_attachments(
        Filters=[
            {'Name': 'transit-gateway-id', 'Values': [tgw_id]},
            {'Name': 'state', 'Values': ['available']}
        ]
    )
    for attachment in all_attachments_response.get('TransitGatewayAttachments', []):
        attach_id = attachment.get('TransitGatewayAttachmentId')
        attach_name_tag = next((tag['Value'] for tag in attachment.get('Tags', []) if tag['Key'] == 'Name'), attach_id)
        
        config['attachments'][attach_id] = {
            'ResourceOwnerId': attachment.get('ResourceOwnerId'), 
            'AttachmentName': attach_name_tag,
            'ResourceId': attachment.get('ResourceId'), 
            'Tags': attachment.get('Tags', [])
        }
        
        # アソシエーション情報の収集
        assoc = attachment.get('Association', {})
        assoc_rtb_id = assoc.get('TransitGatewayRouteTableId')
        if (assoc_rtb_id and 
            assoc.get('State') == 'associated' and 
            assoc_rtb_id in config['rtbs']):
            config['associations'][attach_id] = assoc_rtb_id

    # --- 3. プロパゲーション情報の取得 ---
    API_METHOD_NAME = 'get_transit_gateway_route_table_propagations' 
    if not hasattr(ec2, API_METHOD_NAME):
        logger.warning(
            f"Skipping Propagation data (API client lacks method): EC2 client lacks '{API_METHOD_NAME}'. Check Boto3 version/Lambda Runtime."
        )
    else:
        for rtb_id in config['rtbs'].keys():
            try:
                prop_response = ec2.get_transit_gateway_route_table_propagations(
                    TransitGatewayRouteTableId=rtb_id
                )
                
                for prop in prop_response.get('TransitGatewayRouteTablePropagations', []):
                    prop_attach_id = prop['TransitGatewayAttachmentId']
                    
                    if prop.get('State') in ACTIVE_PROPAGATION_STATES and prop_attach_id in config['attachments']:
                        config['propagations'][rtb_id].add(prop_attach_id)
                
                logger.info(f"Propagation data successfully extracted for RTB {rtb_id}.")
                
            except ClientError as e:
                error_code = e.response.get('Error', {}).get('Code')
                logger.error(f"Propagation API call failed. Error Code: {error_code}. Check if assumed role has 'ec2:GetTransitGatewayRouteTablePropagations'.")
            except Exception as e:
                logger.error(f"UNEXPECTED ERROR during propagation fetching for RTB {rtb_id}: {traceback.format_exc()}")
                
    return config

# =================================================================
# --- 5. CloudFormation YAMLコード生成ロジック ---
# =================================================================

def generate_cfn_yaml(config: Dict[str, Any], tgw_id: str, dynamic_prefix: str) -> str:
    """
    取得したTGW設定から、CloudFormationのYAMLテンプレートを生成する。
    """
    rtb_ref_map: Dict[str, str] = {}
    rtb_resources = {}
    assoc_resources = {}
    prop_resources = {}
    
    # --- YAML Dump カスタム設定 (Ref/GetAttのショートハンド化) ---
    def represent_cfn_tag(dumper: yaml.Dumper, data: Union[Dict, List]) -> yaml.ScalarNode:
        """CFnの組み込み関数 (Fn::) や Ref をショートハンドタグ形式で出力する"""
        if isinstance(data, dict) and len(data) == 1 and 'Ref' in data:
            return dumper.represent_scalar('!Ref', data['Ref'])
        if isinstance(data, dict) and len(data) == 1 and 'Fn::GetAtt' in data:
            return dumper.represent_scalar('!GetAtt', f"{data['Fn::GetAtt'][0]}.{data['Fn::GetAtt'][1]}")
        return dumper.represent_dict(data)

    class CustomDumper(yaml.Dumper):
        pass

    CustomDumper.add_representer(dict, represent_cfn_tag)
    
    # --- 5a. TransitGatewayRouteTable リソースの準備 ---
    for rtb_id, rtb_detail in config['rtbs'].items():
        # RTB名からCFnリソース名を生成 
        cfn_resource_name = get_rtb_cfn_name(rtb_detail['RtbName'])
        rtb_ref_map[rtb_id] = cfn_resource_name
        
        # 🚨 修正点: 自動生成タグを削除し、CFnメタデータを削除
        # AutoGeneratedを削除
        filtered_tags = [
            tag for tag in rtb_detail['Tags'] 
            if not tag['Key'].startswith('aws:cloudformation:') and 
                tag['Key'] != 'AutoGenerated'
        ]
        
        rtb_resources[cfn_resource_name] = {
            'Type': 'AWS::EC2::TransitGatewayRouteTable',
            'Properties': {
                'TransitGatewayId': {'Ref': 'TransitGatewayId'},
                # 抽出した既存のタグのみを使用
                'Tags': filtered_tags 
            },
            'DeletionPolicy': 'Retain' 
        }
    
    # --- 5b. TransitGatewayRouteTableAssociation リソースの準備 ---
    for attach_id, assoc_rtb_id in config['associations'].items():
        rtb_cfn_name = rtb_ref_map.get(assoc_rtb_id)
        if not rtb_cfn_name: continue
        
        attach_detail = config['attachments'][attach_id]
        
        # 💡 修正されたget_attach_cfn_prefixを使用
        attach_prefix = get_attach_cfn_prefix(attach_detail['AttachmentName'])
        
        # 命名規則: TGW + プレフィックス + ASSOCIATETo + RTB名 
        cfn_resource_name = f'TGW{attach_prefix}ASSOCIATETo{rtb_cfn_name}'
        
        assoc_resources[cfn_resource_name] = {
            'Type': 'AWS::EC2::TransitGatewayRouteTableAssociation',
            'Properties': {
                'TransitGatewayAttachmentId': attach_id,
                'TransitGatewayRouteTableId': {'Ref': rtb_cfn_name}
            },
            'DependsOn': rtb_cfn_name,
            'DeletionPolicy': 'Retain' 
        }

    # --- 5c. TransitGatewayRouteTablePropagation リソースの準備 ---
    for rtb_id, prop_attach_ids in config['propagations'].items():
        rtb_cfn_name = rtb_ref_map.get(rtb_id)
        if not rtb_cfn_name: continue
        
        for attach_id in prop_attach_ids:
            attach_detail = config['attachments'][attach_id]
            
            # 💡 修正されたget_attach_cfn_prefixを使用
            attach_prefix = get_attach_cfn_prefix(attach_detail['AttachmentName'])
            
            # 命名規則: TGW + プレフィックス + PROPAGATETo + RTB名 
            cfn_resource_name = f'TGW{attach_prefix}PROPAGATETo{rtb_cfn_name}'
            
            prop_resources[cfn_resource_name] = {
                'Type': 'AWS::EC2::TransitGatewayRouteTablePropagation',
                'Properties': {
                    'TransitGatewayAttachmentId': attach_id,
                    'TransitGatewayRouteTableId': {'Ref': rtb_cfn_name}
                },
                'DependsOn': rtb_cfn_name,
                'DeletionPolicy': 'Retain' 
            }
            
    # --- 5d. YAMLを手動で組み立て、区切りコメントを挿入 ---
    
    cfn_header = {
        'AWSTemplateFormatVersion': '2010-09-09',
        # Descriptionは動的なまま維持
        'Description': f'Generated TGW Routing Configuration for TGW {tgw_id} in {dynamic_prefix}. (No Outputs/Exports)',
        'Parameters': {
            'TransitGatewayId': {
                'Type': 'String',
                'Description': 'TGW ID to apply routing changes',
                'Default': tgw_id
            }
        }
    }

    output_buffer = StringIO()
    
    # 1. ヘッダーとパラメータ
    output_buffer.write(yaml.dump(cfn_header, sort_keys=False, default_flow_style=False))

    # Resources: キーを出力
    output_buffer.write("Resources:\n")

    # 2. RTBリソースと区切りコメント
    output_buffer.write(RTB_SEP)
    rtb_dump = yaml.dump(rtb_resources, Dumper=CustomDumper, sort_keys=False, default_flow_style=False, indent=2)
    output_buffer.write(indent_yaml_dump(rtb_dump, 2))

    # 3. Associationリソースと区切りコメント
    output_buffer.write(ASSOC_SEP)
    assoc_dump = yaml.dump(assoc_resources, Dumper=CustomDumper, sort_keys=False, default_flow_style=False, indent=2)
    output_buffer.write(indent_yaml_dump(assoc_dump, 2))

    # 4. Propagationリソースと区切りコメント
    output_buffer.write(PROP_SEP)
    prop_dump = yaml.dump(prop_resources, Dumper=CustomDumper, sort_keys=False, default_flow_style=False, indent=2)
    output_buffer.write(indent_yaml_dump(prop_dump, 2))
    
    yaml_string = output_buffer.getvalue()
    
    # S3へのアップロード
    cfn_s3_key = f"{dynamic_prefix}/cfn/{CFN_YAML_FILENAME}"
    upload_to_s3(YAML_BUCKET, cfn_s3_key, yaml_string, 'text/yaml')
    
    return f"s3://{YAML_BUCKET}/{cfn_s3_key}"

# =================================================================
# --- 6. マッピングテーブル JSONL 生成ロジック ---
def generate_mapping_table(config: Dict[str, Any], dynamic_prefix: str) -> str:
    """
    TGWの設定情報から、AttachmentとAssociationに基づいたマッピングテーブル（JSONL形式）を生成する。
    """
    jsonl_output = StringIO()
    rtb_name_map = {rtb_id: detail['RtbName'] for rtb_id, detail in config['rtbs'].items()}
    
    for attach_id, assoc_rtb_id in config['associations'].items():
        attach_detail = config['attachments'][attach_id]
        
        output_data = {
            "account-id": attach_detail.get('ResourceOwnerId'),
            "tgw-attach-id": attach_id,
            "rtb-name": rtb_name_map.get(assoc_rtb_id)
        }
        
        if not output_data.get("rtb-name"):
            logger.warning(f"Skipping mapping for {attach_id}: Associated RTB {assoc_rtb_id} name not found.")
            continue
            
        output_data["rtb-name"] = output_data["rtb-name"].strip()
        json_output = json.dumps(output_data, ensure_ascii=False, separators=(',', ':'))
        jsonl_output.write(json_output + '\n')
            
    jsonl_data = jsonl_output.getvalue()
    
    # S3へのアップロード
    mapping_s3_key = f"{dynamic_prefix}/extractsheet/{MAPPING_TABLE_FILENAME}"
    upload_to_s3(YAML_BUCKET, mapping_s3_key, jsonl_data, 'application/jsonl')
    
    return f"s3://{YAML_BUCKET}/{mapping_s3_key}"

# =================================================================
# --- 7. タスクJSONL生成ロジック ---
def extract_rtb_suffix(rtb_name: str) -> str:
    """
    RTB名からタスクIDに使用するサフィックス部分（例: 'asp03-01' や 'onpre'）を抽出する。
    """
    # 1. '-tokyo-' と '-rtb$' の間の部分を抽出
    match = re.search(r'-tokyo-([a-zA-Z0-9_-]+)-rtb$', rtb_name.strip())
    
    if match:
        suffix = match.group(1).upper()
    else:
        # Fallback: 全体を大文字化
        suffix = rtb_name.upper()
        
    # ハイフンをアンダースコアに変換
    cleaned_suffix = suffix.replace('-', '_')
    
    # --- ユーザーの要望に基づくクリーニング ---
    
    # 1. 冗長な接尾辞 _TGW を削除 (例: ASP01_01_TGW -> ASP01_01)
    cleaned_suffix = re.sub(r'_TGW$', '', cleaned_suffix)
    
    # 2. 冗長な接頭辞 GCOPM_ を削除 (例: GCOPM_ONPRE -> ONPRE)
    cleaned_suffix = re.sub(r'^GCOPM_', '', cleaned_suffix)
    
    # 最後に残った非英数字を削除 & 連続するアンダースコアを一つにまとめる & 両端のアンダースコアを削除
    cleaned_suffix = re.sub(r'[^A-Z0-9_]+', '', cleaned_suffix)
    return cleaned_suffix.replace('__', '_').strip('_')


def generate_task_jsonl(config: Dict[str, Any], dynamic_prefix: str) -> str:
    """
    TGWの設定情報から、Association/Propagationタスクリスト（JSONL形式）を生成する。
    """
    task_list = []
    
    rtb_name_map = {rtb_id: detail['RtbName'] for rtb_id, detail in config['rtbs'].items()}
    attach_assoc_rtb_map = config['associations']
    
    # Association タスクの生成
    for attach_id, assoc_rtb_id in config['associations'].items():
        rtb_name = rtb_name_map.get(assoc_rtb_id)
        if not rtb_name: continue
            
        rtb_suffix = extract_rtb_suffix(rtb_name) 
        task_id = f"TGW_{rtb_suffix}_ASSOCIATE"
        
        task_list.append({
            "task_id": task_id,
            "rtb_name": rtb_name,
            "attachment_id": attach_id,
            "target_attachment_id": None,
            "action": "associate"
        })

    # Propagation タスクの生成
    for rtb_id, prop_attach_ids in config['propagations'].items():
        for target_attach_id in prop_attach_ids:
            # Propagate先のAttachmentが、Associationを持っているか確認
            assoc_rtb_id = attach_assoc_rtb_map.get(target_attach_id)
            if not assoc_rtb_id: continue 
            
            assoc_rtb_name = rtb_name_map.get(assoc_rtb_id)
            if not assoc_rtb_name: continue

            rtb_suffix = extract_rtb_suffix(assoc_rtb_name) 
            task_id = f"TGW_{rtb_suffix}_PROPAGATE"
            
            task_list.append({
                "task_id": task_id,
                "rtb_name": rtb_name_map.get(rtb_id), 
                "attachment_id": None,
                "target_attachment_id": target_attach_id, 
                "action": "propagate"
            })
            
    jsonl_output = StringIO()
    for task in task_list:
        jsonl_output.write(json.dumps(task, ensure_ascii=False, separators=(',', ':')) + '\n')
        
    jsonl_data = jsonl_output.getvalue()
    
    # S3へのアップロード
    task_s3_key = f"{dynamic_prefix}/extractsheet/{TASK_JSONL_FILENAME}"
    upload_to_s3(YAML_BUCKET, task_s3_key, jsonl_data, 'application/jsonl')
    
    return f"s3://{YAML_BUCKET}/{task_s3_key}"
# -----------------------------------

# =================================================================
# --- 8. CFnインポートマッピング JSON生成ロジック ---
def generate_import_mapping_json(config: Dict[str, Any], dynamic_prefix: str) -> str:
    """
    CloudFormationリソースインポートに必要な物理IDと論理IDのマッピングJSONを生成する。
    """
    resources_to_import: List[Dict[str, Any]] = []
    
    # RTB IDとCFn論理IDのマッピングを生成
    rtb_ref_map: Dict[str, str] = {}
    for rtb_id, rtb_detail in config['rtbs'].items():
        rtb_cfn_name = get_rtb_cfn_name(rtb_detail['RtbName'])
        rtb_ref_map[rtb_id] = rtb_cfn_name
        
        # 1. TransitGatewayRouteTable のインポートマッピングを追加
        resources_to_import.append({
            'ResourceType': 'AWS::EC2::TransitGatewayRouteTable',
            'LogicalResourceId': rtb_cfn_name,
            'ResourceIdentifier': {'TransitGatewayRouteTableId': rtb_id}
        })

    # TransitGatewayRouteTableAssociation のインポートマッピングを追加
    for attach_id, assoc_rtb_id in config['associations'].items():
        rtb_cfn_name = rtb_ref_map.get(assoc_rtb_id)
        if not rtb_cfn_name: continue
        
        attach_detail = config['attachments'][attach_id]
        # 💡 修正されたget_attach_cfn_prefixを使用
        attach_prefix = get_attach_cfn_prefix(attach_detail['AttachmentName'])
        
        # 論理IDの生成: TGW + プレフィックス + ASSOCIATETo + RTB名
        cfn_resource_name = f'TGW{attach_prefix}ASSOCIATETo{rtb_cfn_name}'
        
        resources_to_import.append({
            'ResourceType': 'AWS::EC2::TransitGatewayRouteTableAssociation',
            'LogicalResourceId': cfn_resource_name,
            'ResourceIdentifier': {
                'TransitGatewayAttachmentId': attach_id,
                'TransitGatewayRouteTableId': assoc_rtb_id
            }
        })

    # TransitGatewayRouteTablePropagation のインポートマッピングを追加
    for rtb_id, prop_attach_ids in config['propagations'].items():
        rtb_cfn_name = rtb_ref_map.get(rtb_id)
        if not rtb_cfn_name: continue
        
        for attach_id in prop_attach_ids:
            attach_detail = config['attachments'][attach_id]
            # 💡 修正されたget_attach_cfn_prefixを使用
            attach_prefix = get_attach_cfn_prefix(attach_detail['AttachmentName'])
            
            # 論理IDの生成: TGW + プレフィックス + PROPAGATETo + RTB名
            cfn_resource_name = f'TGW{attach_prefix}PROPAGATETo{rtb_cfn_name}'
            
            resources_to_import.append({
                'ResourceType': 'AWS::EC2::TransitGatewayRouteTablePropagation',
                'LogicalResourceId': cfn_resource_name,
                'ResourceIdentifier': {
                    'TransitGatewayAttachmentId': attach_id,
                    'TransitGatewayRouteTableId': rtb_id
                }
            })
            
    # JSONとして出力
    json_data = json.dumps(resources_to_import, indent=2, ensure_ascii=False)
    
    # S3へのアップロード
    import_s3_key = f"{dynamic_prefix}/extractsheet/{IMPORT_MAPPING_FILENAME}"
    upload_to_s3(YAML_BUCKET, import_s3_key, json_data, 'application/json')
    
    return f"s3://{YAML_BUCKET}/{import_s3_key}"

# =================================================================
# --- 9. メインディスパッチャ (Lambda専用) ---
def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """AWS Lambdaエントリーポイント (直接呼び出しを想定)"""
    
    logger.info(f"Received event: {json.dumps(event)}")
    
    # dynamic_prefix の動的参照を取得
    dynamic_prefix = event.get('dynamic_prefix')
    
    if not dynamic_prefix:
        logger.error("Required value 'dynamic_prefix' is missing in event.")
        return {'status': 'FAILURE', 'message': "Missing required value 'dynamic_prefix' in Lambda event (This value is used to determine the input config path)."}

    # エラーハンドリングのために、ロール名を try ブロックの外で初期化
    tgw_assume_role_name = ""

    try:
        # TGW設定ファイルの完全なS3入力パスを、dynamic_prefix を使って動的に構築する
        tgw_config_s3_path = f"s3://{YAML_BUCKET}/{dynamic_prefix}/extractsheet/{TGW_CONFIG_INPUT_FILENAME}"
        logger.info(f"Using dynamic TGW config S3 input path: {tgw_config_s3_path}")

        # TGW IDとAccount IDをS3から取得 (JSONL形式)
        config_from_s3 = get_tgw_config_from_s3(tgw_config_s3_path)
        tgw_id = config_from_s3[TGW_ID_KEY]
        owner_account_id = config_from_s3[ACCOUNT_ID_KEY] 

        # ★★★ 修正箇所: dynamic_prefix を使用してTGW_ASSUME_ROLE_NAMEを動的に構築 ★★★
        tgw_assume_role_name = dynamic_prefix + TGW_ASSUME_ROLE_SUFFIX
        logger.info(f"Dynamically constructed TGW Assume Role Name: {tgw_assume_role_name}")

        # --- クロスアカウント対応の追加ロジック ---
        # 1. TGWオーナーアカウントへのロールスイッチ
        cross_account_ec2 = assume_cross_account_role(owner_account_id, tgw_assume_role_name)
        logger.info(f"Successfully assumed role into TGW Owner Account {owner_account_id} using role {tgw_assume_role_name}.")
        
        # 2. TGW設定の全情報を取得 (AssumeRoleクライアントを使用)
        config = get_tgw_configuration(tgw_id, cross_account_ec2)
        # ----------------------------------------
        
        if not config['rtbs']:
            return {'status': 'FAILURE', 'message': f"No available TGW Route Tables found for TGW ID: {tgw_id} in account {owner_account_id}"}
            
        # 3. CloudFormation YAMLの生成とアップロード
        cfn_s3_path = generate_cfn_yaml(config, tgw_id, dynamic_prefix)
        
        # 4. マッピングテーブル JSONLの生成とアップロード
        mapping_table_s3_path = generate_mapping_table(config, dynamic_prefix)
        
        # 5. Task JSONLの生成とアップロード
        task_jsonl_s3_path = generate_task_jsonl(config, dynamic_prefix)
        
        # 6. CFnインポートマッピング JSONの生成とアップロード
        import_mapping_s3_path = generate_import_mapping_json(config, dynamic_prefix)

        success_message = (
            f"TGW configuration successfully exported from account {owner_account_id}. "
            f"CFn YAML uploaded to: {cfn_s3_path}. "
            f"CFn Import Mapping uploaded to: {import_mapping_s3_path}. "
            f"Task JSONL uploaded to: {task_jsonl_s3_path}. "
        )
        
        return {
            'status': 'SUCCESS', 
            'message': success_message,
            'cfn_file': cfn_s3_path,
            'import_mapping_file': import_mapping_s3_path,
            'mapping_file': mapping_table_s3_path,
            'task_list_file': task_jsonl_s3_path
        }

    except Exception as e:
        logger.error(f"Extraction execution failed: {traceback.format_exc()}")
        role_to_check = tgw_assume_role_name if tgw_assume_role_name else "TGW_ASSUME_ROLE"
        return {
            'status': 'FAILURE', 
            'message': f"Lambda execution failed: {str(e)}. Check AssumeRole permissions for {role_to_check}."
        }
# -----------------------------------