#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import logging
import traceback
import boto3
import yaml 
import os
import warnings
from typing import Dict, Any, List, Optional, Tuple, Set

# ---------------------------
# Logger Configuration
# ---------------------------
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Boto3クライアント
s3 = boto3.client('s3')

# ---------------------------
# Constants & Environment Variables
# ---------------------------
# 元の 'transitgateway-automation-rag' を環境変数化。未設定時はプレースホルダを使用。
YAML_BUCKET = os.environ.get('YAML_BUCKET', 'your-org-automation-bucket')
DEFAULT_SYSTEM_NAME = os.environ.get('SYSTEM_NAME', 'your-system-name')
DEFAULT_ENV_TAG = os.environ.get('ENV_TAG', 'prd')

# --- YAML Custom Classes and Representers/Constructors ---

class RefTag(str):
    """CloudFormationの!Refタグを表現するためのカスタム文字列型"""
    pass

def ref_representer(dumper, data):
    """YAMLの!Refタグを表現するカスタムリプレゼンター (出力用)"""
    return dumper.represent_scalar('!Ref', data)

def ref_constructor(loader, node):
    """YAMLの!Refタグを処理するカスタムコンストラクタ (入力用)"""
    # 文字列として値を読み込む
    return RefTag(loader.construct_scalar(node))

class CustomDumper(yaml.Dumper):
    """
    YAMLの整形を調整し、特定のセクションの前に改行を追加して見やすくするカスタムダンパー。
    """
    def write_line_break(self, data=None):
        super().write_line_break(data)
        if self.indents == 2 and self.event_data is not None:
            pass 

CustomDumper.add_representer(RefTag, ref_representer)

# 💡 YAMLパーサーに入力用のコンストラクタを登録
class CustomLoader(yaml.SafeLoader):
    """YAMLの!Refタグを読み込み時に適切に処理するカスタムローダー"""
    pass
CustomLoader.add_constructor('!Ref', ref_constructor)


# --- Utility Functions ---

def split_s3_path(s3_path: str) -> Tuple[str, str]:
    """S3パス文字列をバケット名とキーに分割する"""
    if not s3_path:
        return '', ''
    
    if s3_path.startswith('s3://'):
        s3_path = s3_path[5:] 
            
    parts = s3_path.split('/', 1)
    
    bucket = parts[0]
    key = parts[1] if len(parts) == 2 else ''
        
    return bucket, key
    
def build_agent_response(agent_info: Dict[str, Any], body_message: str, response_state: str, http_method: str) -> Dict[str, Any]:
    """
    Bedrock Agentが期待する厳密なJSON応答構造を生成します。
    """
    return {
        'messageVersion': '1.0',
        'response': {
            'actionGroup': agent_info['actionGroup'],
            'apiPath': agent_info['apiPath'],
            'httpMethod': http_method, 
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

def get_logical_ids_from_yaml(yaml_content: str) -> Set[str]:
    """YAMLコンテンツからすべてのCFnリソースの論理ID（キー）を抽出する"""
    try:
        # カスタムローダー (CustomLoader) を使用して !Ref タグを処理する
        data = yaml.load(yaml_content, Loader=CustomLoader)
        if not data or 'Resources' not in data:
            return set()
            
        # Resourcesセクションから論理IDを抽出。セパレータキーは除外。
        resource_ids = {
            k for k in data['Resources'].keys() 
            if not k.startswith('___GROUP_SEPARATOR_') and k not in ['___GROUP_SEPARATOR_RTB___', '___GROUP_SEPARATOR_ASSOCIATION___', '___GROUP_SEPARATOR_PROPAGATION___']
        }
        return resource_ids
    except Exception as e:
        # ログにエラーを出力するが、処理は続行
        logger.error(f"Failed to parse YAML content for logical IDs: {e}") 
        return set()

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
    
    http_method = agent_info['httpMethod'] 
    
    s3_config_key = None
    yaml_bucket = YAML_BUCKET
    current_key_name = '' 
    dynamic_prefix = os.environ.get('DEFAULT_PREFIX', 'experiment')
    yaml_file_name = 'tgw_routing_cfn.yaml' 
    
    try:
        logger.info(f"Action (making_yamlfile) started.")
        
        # -----------------------------------------------------------------
        # 1. パラメータ抽出ロジック（Agentペイロードを含む）
        # -----------------------------------------------------------------
        params = {}
        if 'requestBody' in event:
            try:
                # Bedrock Agent 経由の入力を解析
                props = event['requestBody']['content']['application/json']['properties']
                params = {prop['name']: prop['value'] for prop in props}
            except (KeyError, TypeError):
                # 直接的なペイロードの場合
                params = {
                    k: v for k, v in event.items() 
                    if k in ['s3_config_key', 'yaml_bucket', 'dynamic_prefix', 'yaml_file_name']
                }
        else:
            # Lambda直接実行などのフォールバック
            params = event

        s3_config_key = params.get('s3_config_key', s3_config_key)
        yaml_bucket = params.get('yaml_bucket', yaml_bucket) 
        dynamic_prefix = params.get('dynamic_prefix', dynamic_prefix)
        yaml_file_name = params.get('yaml_file_name', yaml_file_name) 
        yaml_key = f"{dynamic_prefix}/cfn/{yaml_file_name}"

        if not s3_config_key:
            error_msg = "Error: Missing s3_config_key parameter. Cannot proceed."
            logger.error(error_msg)
            return build_agent_response(agent_info, error_msg, 'FAILURE', http_method)
        
        # -----------------------------------------------------------------
        # 2. S3からの設定データ読み込み (Route Table Config - JSONL)
        # -----------------------------------------------------------------
        current_key_name = 'Route Table Config' 
        _temp_bucket, config_key = split_s3_path(s3_config_key) 
        config_bucket = yaml_bucket
        
        try:
            s3_object = s3.get_object(Bucket=config_bucket, Key=config_key)
            jsonl_content = s3_object['Body'].read().decode('utf-8')
            rtb_config: List[Dict[str, Any]] = [
                json.loads(line) 
                for line in jsonl_content.strip().split('\n') 
                if line.strip()
            ]
        except s3.exceptions.NoSuchKey:
            error_msg = f"Error: {current_key_name} file not found. Key: {config_bucket}/{config_key}. Cannot proceed."
            logger.error(error_msg)
            return build_agent_response(agent_info, error_msg, 'FAILURE', http_method)
        except Exception as e:
            error_message = f"An unexpected error occurred while reading {config_key}: {e}"
            logger.error(error_message)
            return build_agent_response(agent_info, error_message, 'FAILURE', http_method)

        # -----------------------------------------------------------------
        # 3. S3からの TGW ID 読み込み (TGW ID Config - JSONL)
        # -----------------------------------------------------------------
        tgw_id = '' 
        # パス構築のロジックを正確に維持
        tgw_config_key_dynamic = f"{dynamic_prefix}/extractsheet/tgw_id_config.jsonl"
        tgw_config_bucket = yaml_bucket 
        
        try:
            tgw_object = s3.get_object(Bucket=tgw_config_bucket, Key=tgw_config_key_dynamic)
            tgw_jsonl = tgw_object['Body'].read().decode('utf-8').strip().split('\n')
            if tgw_jsonl and tgw_jsonl[0]:
                tgw_data = json.loads(tgw_jsonl[0])
                tgw_id = tgw_data.get('tgw_id', '') 
        except Exception as e:
            logger.warning(f"Could not read TGW ID config: {e}")
            
        if not tgw_id:
            error_msg = f"Error: TGW ID could not be extracted. Cannot proceed with YAML creation."
            logger.error(error_msg)
            return build_agent_response(agent_info, error_msg, 'FAILURE', http_method)

        # -----------------------------------------------------------------
        # 4. CFn YAML生成ロジックと安定化 (新しいYAMLの生成)
        # -----------------------------------------------------------------
        cfn_resources = {}
        defined_rtb_logical_ids = set()
        rtb_definitions = []
        association_definitions = [] 
        propagation_definitions = [] 
        
        yaml_config = {
            'AWSTemplateFormatVersion': '2010-09-09',
            'Description': 'Generated TGW Routing Configuration by Bedrock Agent.',
            'Parameters': {
                'TransitGatewayId': {
                    'Type': 'String',
                    'Description': 'TGW ID to apply routing changes',
                    'Default': tgw_id 
                }
            },
            'Resources': cfn_resources
        }
        
        new_resource_logical_ids: Set[str] = set()
        
        for record in rtb_config:
            action = record['action']
            rtb_name = record['rtb_name']
            
            # 論理ID生成ルール（正確に復元）
            rtb_logical_id = "".join([s.capitalize() for s in rtb_name.split('-')]).replace('_', '').replace('Rtb', 'RTB')
            if rtb_logical_id.startswith('Hubdev'):
                rtb_logical_id = rtb_logical_id.replace('Hubdev', 'HubDev')
            
            if rtb_logical_id not in defined_rtb_logical_ids:
                base_tags = [
                    {'Key': 'Env', 'Value': DEFAULT_ENV_TAG},
                    {'Key': 'Name', 'Value': rtb_name},
                    {'Key': 'System', 'Value': DEFAULT_SYSTEM_NAME}
                ]
                base_tags.sort(key=lambda x: x['Key'])
                
                rtb_definitions.append({
                    'logical_id': rtb_logical_id,
                    'resource': {
                        'Type': 'AWS::EC2::TransitGatewayRouteTable',
                        'Properties': {
                            'TransitGatewayId': RefTag('TransitGatewayId'), 
                            'Tags': base_tags 
                        },
                        'DeletionPolicy': 'Retain'
                    }
                })
                defined_rtb_logical_ids.add(rtb_logical_id)
                new_resource_logical_ids.add(rtb_logical_id)
            
            task_id_base = record['task_id'].replace('-', '').replace('_', '')
            rtb_id_suffix = rtb_logical_id 
            task_logical_id = f"{task_id_base}To{rtb_id_suffix}"
            new_resource_logical_ids.add(task_logical_id)

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
                        'DeletionPolicy': 'Retain'
                    }
                })
            elif action == 'propagate':
                propagation_definitions.append({ 
                    'logical_id': task_logical_id,
                    'resource': {
                        'Type': 'AWS::EC2::TransitGatewayRouteTablePropagation',
                        'Properties': {
                            'TransitGatewayAttachmentId': record['target_attachment_id'], 
                            'TransitGatewayRouteTableId': rtb_ref
                        },
                        'DependsOn': rtb_logical_id,
                        'DeletionPolicy': 'Retain'
                    }
                })

        # 順序の安定化
        rtb_definitions.sort(key=lambda x: x['logical_id'])
        cfn_resources['___GROUP_SEPARATOR_RTB___'] = '' 
        for item in rtb_definitions:
            cfn_resources[item['logical_id']] = item['resource']
            
        association_definitions.sort(key=lambda x: x['logical_id'])
        cfn_resources['___GROUP_SEPARATOR_ASSOCIATION___'] = '' 
        for item in association_definitions:
            if item['logical_id'] not in cfn_resources:
                cfn_resources[item['logical_id']] = item['resource']

        propagation_definitions.sort(key=lambda x: x['logical_id'])
        cfn_resources['___GROUP_SEPARATOR_PROPAGATION___'] = ''
        for item in propagation_definitions:
            if item['logical_id'] not in cfn_resources:
                cfn_resources[item['logical_id']] = item['resource']

        # YAMLダンプ
        yaml_output = yaml.dump(yaml_config, Dumper=CustomDumper, default_flow_style=False, sort_keys=False, allow_unicode=True)
        
        # リソースグループのセパレータ置換
        yaml_output = yaml_output.replace(
            '___GROUP_SEPARATOR_RTB___: \'\'\n',
            '# =========================================================================\n# --- TransitGatewayRouteTable Resources ---\n# =========================================================================\n'
        )
        yaml_output = yaml_output.replace(
            '___GROUP_SEPARATOR_ASSOCIATION___: \'\'\n', 
            '\n\n# =========================================================================\n# --- TransitGatewayRouteTableAssociation Resources ---\n# =========================================================================\n\n'
        )
        yaml_output = yaml_output.replace(
            '___GROUP_SEPARATOR_PROPAGATION___: \'\'\n', 
            '\n\n# =========================================================================\n# --- TransitGatewayRouteTablePropagation Resources ---\n# =========================================================================\n\n'
        )
        
        # -----------------------------------------------------------------
        # 5. S3からの既存YAMLロードと純粋な差分生成 (論理IDベース)
        # -----------------------------------------------------------------
        old_yaml_content = None
        yaml_s3_path = f"s3://{yaml_bucket}/{yaml_key}"
        diff_key = f"{yaml_key}.diff"
        diff_s3_path = f"s3://{yaml_bucket}/{diff_key}"
        
        try:
            s3_object = s3.get_object(Bucket=yaml_bucket, Key=yaml_key)
            old_yaml_content = s3_object['Body'].read().decode('utf-8')
        except s3.exceptions.NoSuchKey:
            logger.info("Existing YAML not found, skipping diff.")
        except Exception as e:
            logger.warning(f"Warning: Failed to load existing YAML file {yaml_s3_path} for diff: {e}")
        
        diff_output = ""
        if old_yaml_content is not None:
            old_resource_logical_ids: Set[str] = get_logical_ids_from_yaml(old_yaml_content)
            added_ids = new_resource_logical_ids - old_resource_logical_ids
            removed_ids = old_resource_logical_ids - new_resource_logical_ids
            
            diff_lines = ["# --- Pure Logical Difference (Resource Addition/Removal) ---\n"]
            if added_ids:
                diff_lines.append("\n## 🆕 Added Resources (New CFn Resources to be created):\n")
                for logical_id in sorted(list(added_ids)):
                    diff_lines.append(f"+ {logical_id}\n")
            if removed_ids:
                diff_lines.append("\n## 🗑️ Removed Resources (Existing CFn Resources to be deleted):\n")
                for logical_id in sorted(list(removed_ids)):
                    diff_lines.append(f"- {logical_id}\n")
            
            if len(diff_lines) > 1:
                diff_output = "".join(diff_lines)
                s3.put_object(
                    Bucket=yaml_bucket, Key=diff_key, 
                    Body=diff_output.encode('utf-8'), 
                    ContentType='text/plain' 
                )
                logger.info(f"✅ Pure Diff file uploaded successfully to {diff_s3_path}.")
        
        # -----------------------------------------------------------------
        # 6. 新しいYAMLのS3への保存
        # -----------------------------------------------------------------
        s3.put_object(
            Bucket=yaml_bucket, Key=yaml_key,
            Body=yaml_output.encode('utf-8'),
            ContentType='text/yaml'
        )
        
        success_message = f"TGW routing CFn YAML file generated successfully at S3 path: {yaml_s3_path}. "
        if diff_output:
             success_message += f"A **pure diff** file showing resource additions/removals has also been uploaded to {diff_s3_path}."
        else:
             success_message += "No significant resource additions or removals were detected."

        return build_agent_response(agent_info, success_message, 'SUCCESS', http_method)

    except Exception as e:
        logger.error(f"❌ FATAL ERROR in Action: {traceback.format_exc()}")
        error_message = f"An error occurred during CFn YAML file creation: {e}"
        return build_agent_response(agent_info, error_message, 'FAILURE', http_method)

# --- Lambda Entry Point ---
def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    return making_yamlfile(event, context)