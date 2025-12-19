import json
import logging
import traceback
import boto3
import yaml 
import os
from typing import Dict, Any, List

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Boto3クライアント
s3 = boto3.client('s3')

# 定数
# YAMLの出力先バケット名（Agentパラメータで上書き可能）
YAML_BUCKET = 'transitgateway-automation-rag'

# --- YAML Custom Classes and Representers ---

class RefTag(str):
    """CloudFormationの!Refタグを表現するためのカスタム文字列型"""
    pass

def ref_representer(dumper, data):
    """YAMLの!Refタグを表現するカスタムリプレゼンター"""
    return dumper.represent_scalar('!Ref', data)

class CustomDumper(yaml.Dumper):
    """
    YAMLの整形を調整し、特定のセクション（Resources内のリソース定義後）の前に改行を追加して見やすくするカスタムダンパー。
    """
    def write_line_break(self, data=None):
        super().write_line_break(data)
        # 1つ目のネスト（インデントが2スペース）の後、空行を追加
        if self.indents == 2 and self.event_data is not None:
            # 特殊な区切り文字の置換で整形を完了させるため、ここではデフォルトの動作を継承
            pass 

CustomDumper.add_representer(RefTag, ref_representer)

# --- Utility Functions ---

def split_s3_path(s3_path: str) -> tuple[str, str]:
    """S3パス文字列をバケット名とキーに分割する"""
    if not s3_path:
        return '', ''
    
    # 's3://' プレフィックスを削除
    if s3_path.startswith('s3://'):
        s3_path = s3_path[5:] 
            
    parts = s3_path.split('/', 1)
    
    bucket = parts[0]
    key = parts[1] if len(parts) == 2 else ''
        
    return bucket, key

def build_agent_response(agent_info: Dict[str, Any], body_message: str, response_state: str) -> Dict[str, Any]:
    """
    Bedrock Agentが期待する厳密なJSON応答構造を生成します。
    """
    return {
        'messageVersion': '1.0',
        'response': {
            'actionGroup': agent_info['actionGroup'],
            'apiPath': agent_info['apiPath'],
            'httpMethod': agent_info['httpMethod'],
            'functionResponse': {
                'responseState': response_state,
                'responseBody': {
                    'application/json': { 
                        'body': body_message 
                    }
                }
            }
        }
    }

# --- Lambda Handler Core Logic ---

def making_yamlfile(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    
    # -----------------------------------------------------------------
    # 🚨 動的な Bedrock Agent 応答のための情報抽出 🚨
    # -----------------------------------------------------------------
    agent_info = {
        'apiPath': event.get('apiPath', '/makingYamlFile'),
        'httpMethod': event.get('httpMethod', 'POST'),
        'actionGroup': event.get('actionGroup', 'making-yaml')
    }
    
    # デフォルト値を設定
    s3_config_key = None
    yaml_bucket = YAML_BUCKET
    current_key_name = '' # エラーログ用
    dynamic_prefix = 'experiment' # デフォルトまたはフォールバック
    yaml_file_name = 'tgw_routing_cfn.yaml' # デフォルト
    
    try:
        logger.info(f"Action 2 (making_yamlfile) started.")
        logger.info(f"Received event: {json.dumps(event)}")
        
        # -----------------------------------------------------------------
        # 1. パラメータ抽出ロジック（Agentペイロードを含む）
        # -----------------------------------------------------------------
        
        # Bedrock Agent の requestBody からパラメータを抽出
        params = {}
        if 'requestBody' in event:
            try:
                props = event['requestBody']['content']['application/json']['properties']
                params = {prop['name']: prop['value'] for prop in props}
                
            except (KeyError, TypeError):
                logger.warning("Agent payload parsing failed or structure is flat. Trying direct parameter extraction.")
                # 古いAgent形式やLambdaテストイベントからの直接抽出を試みる
                params = {
                    k: v for k, v in event.items() 
                    if k in ['s3_config_key', 'yaml_bucket', 'dynamic_prefix', 'yaml_file_name']
                }

        # 抽出されたパラメータの取得とデフォルト値の適用
        s3_config_key = params.get('s3_config_key', s3_config_key)
        # yaml_bucketは定数YAML_BUCKETの値をデフォルトとする
        yaml_bucket = params.get('yaml_bucket', yaml_bucket) 
        
        # dynamic_prefix と yaml_file_name を取得
        dynamic_prefix = params.get('dynamic_prefix', dynamic_prefix)
        yaml_file_name = params.get('yaml_file_name', yaml_file_name) 
        
        # yaml_key を dynamic_prefix を使って動的に構築
        # 構造: {dynamic_prefix}/cfn/{yaml_file_name}
        yaml_key = f"{dynamic_prefix}/cfn/{yaml_file_name}"

        if not s3_config_key:
            error_msg = "Error: Missing s3_config_key parameter. Cannot proceed."
            logger.error(error_msg)
            return build_agent_response(agent_info, error_msg, 'FAILURE')

        logger.info(f"Parameters extracted: s3_config_key={s3_config_key}, yaml_bucket={yaml_bucket}, yaml_key={yaml_key}, dynamic_prefix={dynamic_prefix}")
        
        # -----------------------------------------------------------------
        # 2. S3からの設定データ読み込み (Route Table Config - JSONL)
        # -----------------------------------------------------------------
        current_key_name = 'Route Table Config' 

        # 🚨 FIX: s3_config_keyを強制的にクリーンアップしてキーのみを取得 🚨
        # Agentからs3://bucket/key形式で渡された場合に対応するため
        _temp_bucket, config_key = split_s3_path(s3_config_key) 
        
        # バケット名は、Agentパラメータまたは定数から取得したものを使用
        config_bucket = yaml_bucket
        
        s3_access_path = f"s3://{config_bucket}/{config_key}"
        
        try:
            logger.info(f"Attempting to load {current_key_name} from S3. Path: {s3_access_path}")
            
            # Keyにはs3://プレフィックスを含まない、純粋なキーが渡される
            s3_object = s3.get_object(Bucket=config_bucket, Key=config_key)
            jsonl_content = s3_object['Body'].read().decode('utf-8')
            
            # JSONLをパースしてレコードのリストにする
            rtb_config: List[Dict[str, Any]] = [
                json.loads(line) 
                for line in jsonl_content.strip().split('\n') 
                if line.strip() # 空行を除外
            ]
            logger.info(f"Successfully loaded {len(rtb_config)} records from S3 key: {s3_access_path}")
            
        except s3.exceptions.NoSuchKey:
            error_msg = f"Error: {current_key_name} file not found. Key: {s3_access_path}. Cannot proceed."
            logger.error(error_msg)
            return build_agent_response(agent_info, error_msg, 'FAILURE')
            
        except s3.exceptions.ClientError as e:
            logger.error(f"❌ FATAL S3 ERROR ({current_key_name}): S3アクセスエラーが発生しました。\n- 対象: {s3_access_path}\n- 詳細: {type(e).__name__}: {str(e)}")
            error_message = f"An S3 access error occurred while reading {config_key}: {e}"
            return build_agent_response(agent_info, error_message, 'FAILURE')
        except Exception as e:
            logger.error(f"❌ FATAL S3 ERROR ({current_key_name}): S3アクセスエラーが発生しました。\n- 対象: {s3_access_path}\n- 詳細: {type(e).__name__}: {str(e)}")
            error_message = f"An unexpected error occurred while reading {config_key}: {e}"
            return build_agent_response(agent_info, error_message, 'FAILURE')


        # -----------------------------------------------------------------
        # 3. S3からの TGW ID 読み込み (TGW ID Config - JSONL)
        # -----------------------------------------------------------------
        tgw_id = '' 
        current_key_name = 'TGW ID Config' 
        
        # TGW ID設定キーを dynamic_prefix を使って動的に構築
        # 構造: s3://{yaml_bucket}/{dynamic_prefix}/extractsheet/tgw_id_config.jsonl
        tgw_id_config_key_dynamic = f"{yaml_bucket}/{dynamic_prefix}/extractsheet/tgw_id_config.jsonl"
        
        # split_s3_pathを使ってバケットとキーを分離（この場合はキー名のみが返る）
        tgw_config_bucket, tgw_config_key = split_s3_path(tgw_id_config_key_dynamic)
        # tgw_config_bucketが空になるため、yaml_bucketを使用
        tgw_config_bucket = yaml_bucket 
        
        s3_access_path_tgw = f"s3://{tgw_config_bucket}/{tgw_config_key}"
        
        try:
            logger.info(f"Attempting to load {current_key_name} from S3. Path: {s3_access_path_tgw}")
            
            if tgw_config_key:
                tgw_object = s3.get_object(Bucket=tgw_config_bucket, Key=tgw_config_key)
                tgw_jsonl = tgw_object['Body'].read().decode('utf-8').strip().split('\n')
                
                if tgw_jsonl and tgw_jsonl[0]:
                    tgw_data = json.loads(tgw_jsonl[0])
                    # tgw_idは最初のレコードの 'tgw_id' フィールドから抽出されることを想定
                    tgw_id = tgw_data.get('tgw_id', '') 
                    logger.info(f"Extracted TGW ID: {tgw_id}")
            
        except s3.exceptions.NoSuchKey:
            logger.warning(f"Warning: {current_key_name} file not found. Path: {s3_access_path_tgw}. Proceeding without TGW ID.")
            pass # TGW IDがない場合は後のチェックで失敗させるため、ここでは処理を中断しない
        except Exception as e:
            logger.error(f"Error reading TGW config file ({s3_access_path_tgw}): {e}")
            pass # 同上
            
        if not tgw_id:
            error_msg = f"Error: TGW ID could not be extracted from S3 key: {s3_access_path_tgw}. Cannot proceed with YAML creation."
            logger.error(error_msg)
            return build_agent_response(agent_info, error_msg, 'FAILURE')

        # -----------------------------------------------------------------
        # 4. CFn YAML生成ロジック
        # -----------------------------------------------------------------
        
        cfn_resources = {}
        defined_rtb_logical_ids = set()
        rtb_definitions = []
        association_definitions = [] 
        propagation_definitions = [] 
        
        # テンプレートの基本構造
        yaml_config = {
            'AWSTemplateFormatVersion': '2010-09-09',
            'Description': 'Generated TGW Routing Configuration by Bedrock Agent.',
            'Parameters': {
                'TransitGatewayId': {
                    'Type': 'String',
                    'Description': 'TGW ID to apply routing changes',
                    # 抽出したTGW IDをデフォルト値として設定
                    'Default': tgw_id 
                }
            },
            'Resources': cfn_resources
        }
        
        # 🚨 RTBリソースの論理IDリストを保持
        rtb_logical_ids_map = {} 
        
        for record in rtb_config:
            action = record['action']
            rtb_name = record['rtb_name']
            
            # RTB論理IDを生成 (例: "prd-shared-rtb" -> "PrdSharedRTB")
            rtb_logical_id = "".join([s.capitalize() for s in rtb_name.split('-')]).replace('_', '').replace('Rtb', 'RTB')
            
            # 🚨 修正ロジックの追加: 論理IDを HubDev... (Dが大文字) に強制的に変換
            # 既存スタックとの互換性を確保するため
            if rtb_logical_id.startswith('Hubdev'):
                 rtb_logical_id = rtb_logical_id.replace('Hubdev', 'HubDev')
            
            # 論理IDの対応マップを更新
            rtb_logical_ids_map[rtb_name] = rtb_logical_id
            
            
            # RTBリソースがまだ定義されていない場合にのみ定義を追加
            if rtb_logical_id not in defined_rtb_logical_ids:
                base_tags = [
                    {'Key': 'Name', 'Value': rtb_name},
                    {'Key': 'Env', 'Value': 'prd'},
                    {'Key': 'System', 'Value': 'gcopm'}
                ]
                
                rtb_definitions.append({
                    'logical_id': rtb_logical_id,
                    'resource': {
                        'Type': 'AWS::EC2::TransitGatewayRouteTable',
                        'Properties': {
                            'TransitGatewayId': RefTag('TransitGatewayId'), 
                            'Tags': base_tags 
                        },
                        'DeletionPolicy': 'Retain' # デプロイテンプレートに合わせて追加
                    }
                })
                defined_rtb_logical_ids.add(rtb_logical_id)
            
            # Association/Propagationタスクの論理IDを生成
            task_id_base = record['task_id'].replace('-', '').replace('_', '')
            rtb_id_suffix = rtb_logical_id 
            
            # Association/Propagationのリソース名も、RTB論理IDに合わせて修正されたものを使用
            task_logical_id = f"{task_id_base}To{rtb_id_suffix}"

            rtb_ref = RefTag(rtb_logical_id)
            
            if action == 'associate':
                association_definitions.append({ 
                    'logical_id': task_logical_id,
                    'resource': {
                        'Type': 'AWS::EC2::TransitGatewayRouteTableAssociation',
                        'Properties': {
                            'TransitGatewayAttachmentId': record['attachment_id'],
                            'TransitGatewayRouteTableId': rtb_ref
                        },
                        'DependsOn': rtb_logical_id,
                        'DeletionPolicy': 'Retain' # デプロイテンプレートに合わせて追加
                    }
                })
            elif action == 'propagate':
                propagation_definitions.append({ 
                    'logical_id': task_logical_id,
                    'resource': {
                        'Type': 'AWS::EC2::TransitGatewayRouteTablePropagation',
                        'Properties': {
                            # propagateのターゲットは 'target_attachment_id' を使用
                            'TransitGatewayAttachmentId': record['target_attachment_id'], 
                            'TransitGatewayRouteTableId': rtb_ref
                        },
                        'DependsOn': rtb_logical_id,
                        'DeletionPolicy': 'Retain' # デプロイテンプレートに合わせて追加
                    }
                })

        # Resourcesセクションに定義を順番に追加
        
        # RTBセクションの区切りコメントを追加
        # NOTE: YAMLダンプ後にこのコメントを整形するため、ダミーキーを使用しない
        
        # ----------------------------------------------------------------------
        # RTB Resources を追加
        # ----------------------------------------------------------------------
        cfn_resources['___GROUP_SEPARATOR_RTB___'] = '' 
        
        for item in rtb_definitions:
            cfn_resources[item['logical_id']] = item['resource']
            
        # ----------------------------------------------------------------------
        # Association Resources を追加
        # ----------------------------------------------------------------------
        # 🚨 修正箇所 1: Associationセクションとの区切り文字を挿入
        cfn_resources['___GROUP_SEPARATOR_ASSOCIATION___'] = '' 

        for item in association_definitions:
            if item['logical_id'] not in cfn_resources:
                cfn_resources[item['logical_id']] = item['resource']

        # ----------------------------------------------------------------------
        # Propagation Resources を追加
        # ----------------------------------------------------------------------
        # 🚨 修正箇所 2: Propagationセクションとの区切り文字を挿入
        cfn_resources['___GROUP_SEPARATOR_PROPAGATION___'] = ''

        for item in propagation_definitions:
            if item['logical_id'] not in cfn_resources:
                cfn_resources[item['logical_id']] = item['resource']

        # YAMLダンプ
        yaml_output = yaml.dump(yaml_config, Dumper=CustomDumper, default_flow_style=False, sort_keys=False, allow_unicode=True)
        
        # 🚨 修正箇所 3: ダミーリソースをコメント行に置換
        # '  ___GROUP_SEPARATOR_...: ''\n' のパターンで置換を行う
        
        # RTBセクションの区切り文字をコメントに置換
        yaml_output = yaml_output.replace(
            '  ___GROUP_SEPARATOR_RTB___: \'\'\n',
            '\n\n# =========================================================================\n# --- TransitGatewayRouteTable Resources ---\n# =========================================================================\n' # RTBの前は改行を少なめに
        )
        
        # Associationの区切り文字をコメントと改行に置換し、整形を完了させる
        yaml_output = yaml_output.replace(
            '  ___GROUP_SEPARATOR_ASSOCIATION___: \'\'\n', 
            '\n\n# =========================================================================\n# --- TransitGatewayRouteTableAssociation Resources ---\n# =========================================================================\n\n'
        )

        # Propagationの区切り文字をコメントと改行に置換し、整形を完了させる
        yaml_output = yaml_output.replace(
            '  ___GROUP_SEPARATOR_PROPAGATION___: \'\'\n', 
            '\n\n# =========================================================================\n# --- TransitGatewayRouteTablePropagation Resources ---\n# =========================================================================\n\n'
        )
        
        # -----------------------------------------------------------------
        # 5. S3への保存
        # -----------------------------------------------------------------
        s3_access_path = f"s3://{yaml_bucket}/{yaml_key}" 
        logger.info(f"Attempting to upload YAML to S3. Path: {s3_access_path}")
        
        s3.put_object(
            Bucket=yaml_bucket,
            Key=yaml_key,
            Body=yaml_output.encode('utf-8'),
            ContentType='text/yaml'
        )
        
        # --- 最終成功応答 ---
        success_message = (
            f"TGW routing CFn YAML file generated successfully. "
            f"The CloudFormation file is available at S3 path: {s3_access_path}"
        )
        
        return build_agent_response(agent_info, success_message, 'SUCCESS')

    except Exception as e:
        # 最後の catch-all: 致命的なエラーや予期せぬエラーを捕捉
        logger.error(f"❌ FATAL ERROR in Action 2: {traceback.format_exc()}")
            
        error_message = f"An error occurred during CFn YAML file creation: {e}"
        
        # エラー時の Bedrock Agent 互換の応答を返す
        return build_agent_response(agent_info, error_message, 'FAILURE')

# --- Lambda Entry Point ---
def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """AWS Lambdaエントリーポイント"""
    return making_yamlfile(event, context)