import json
import yaml
import re
import boto3
from collections import defaultdict
from io import StringIO
import os 
from typing import Dict, Any, Optional, Union, List

# S3クライアントを初期化
s3 = boto3.client('s3')

# 💡 Agent対応修正: ハードコード定数の設定
OUTPUT_BUCKET = "transitgateway-automation-rag"
OUTPUT_KEY_SUFFIX = "/mermaid/tgw_routing_diagram.md"
# 💡 新規追加: 差分Mermaidのファイル名サフィックス
DIFF_OUTPUT_KEY_SUFFIX = "/mermaid/tgw_routing_diagram_diff.md" 
MAPPING_KEY_SUFFIX = "/extractsheet/tgw_mapping_table.jsonl" 
# CFN YAMLファイル名の固定
CFN_YAML_FILE_NAME = "tgw_routing_cfn.yaml"


# =========================================================================
# YAML Tag Handling
# =========================================================================
class CfnSafeLoader(yaml.SafeLoader):
    pass

# PyYAMLのadd_multi_constructorは3引数関数を期待する
def construct_cfn_tag(loader, tag_suffix, node):
    """'!'で始まるカスタムタグ（!Ref, !Subなど）を捕捉し、単一の文字列として返す。"""
    tag_name = tag_suffix
    
    if isinstance(node, yaml.ScalarNode):
        value = loader.construct_scalar(node)
    elif isinstance(node, yaml.SequenceNode):
        value = loader.construct_sequence(node)
    elif isinstance(node, yaml.MappingNode):
        value = loader.construct_mapping(node)
    else:
        value = loader.construct_object(node, deep=True)

    if isinstance(value, str):
        return f"!{tag_name} {value}"
    else:
        return f"!{tag_name} [Complex Value]"

CfnSafeLoader.add_multi_constructor('!', construct_cfn_tag)


# =========================================================================
# S3操作ヘルパー関数
# =========================================================================

def read_s3_content(bucket_name: str, key: str, version_id: Optional[str] = None) -> Optional[str]:
    """S3バケットからファイルを読み込み、文字列として返す (バージョンIDに対応)"""
    print(f"Attempting to read s3://{bucket_name}/{key} (Version: {version_id or 'Latest'})")
    try:
        params = {'Bucket': bucket_name, 'Key': key}
        if version_id:
            params['VersionId'] = version_id
        response = s3.get_object(**params)
        content = response['Body'].read().decode('utf-8')
        return content
    except s3.ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchKey':
            print(f"❌ ERROR: S3 key s3://{bucket_name}/{key} not found.")
        else:
            print(f"❌ ERROR reading S3 object: {e}")
        return None

def read_yaml_from_s3(bucket_name, key):
    """S3バケットからYAMLファイルを読み込み、文字列として返す (互換性維持)"""
    return read_s3_content(bucket_name, key)

def write_mermaid_to_s3(mermaid_code, output_bucket, output_key):
    """生成されたMermaidコードをS3バケットにファイルとして書き込む"""
    try:
        s3.put_object(
            Bucket=output_bucket,
            Key=output_key,
            Body=mermaid_code.encode('utf-8'),
            ContentType='text/markdown'
        )
        print(f"✅ SUCCESS: Mermaid code saved to s3://{output_bucket}/{output_key}")
        return True
    except s3.ClientError as e:
        print(f"❌ ERROR writing to S3. Check destination bucket and IAM permissions: {e}")
        return False

def load_asp_mapping(bucket_name: str, key: str) -> Dict[str, str]:
    """tgw_mapping_table.jsonl をS3から読み込み、tgw-attach-id -> asp-name の辞書を作成する"""
    mapping_content = read_s3_content(bucket_name, key)
    if not mapping_content:
        return {}

    asp_mapping = {}
    for line in mapping_content.strip().split('\n'):
        if not line:
            continue
        try:
            data = json.loads(line)
            # tgw-attach-id と asp-name の存在を確認
            if 'tgw-attach-id' in data and 'asp-name' in data:
                # 辞書キーは小文字（tgw-attach-xxx）
                asp_mapping[data['tgw-attach-id'].lower()] = data['asp-name']
        except json.JSONDecodeError as e:
            print(f"⚠️ Warning: Skipping malformed JSON line in mapping file: {line}. Error: {e}")
    
    print(f"Loaded {len(asp_mapping)} ASP mappings.")
    return asp_mapping

def get_s3_file_versions(bucket_name: str, key: str) -> List[str]:
    """指定されたS3キーのバージョンIDを降順でリストとして取得する"""
    print(f"Listing versions for key: s3://{bucket_name}/{key}")
    try:
        response = s3.list_object_versions(Bucket=bucket_name, Prefix=key)
        
        versions = response.get('Versions', [])
        if not versions:
            print("No versions found (versioning might be disabled or file does not exist).")
            return []
        
        # 最終更新日時でソート（新しいものが先頭）
        sorted_versions = sorted(
            versions, 
            key=lambda x: x.get('LastModified'), 
            reverse=True
        )
        
        # IDが "null" でない有効なバージョンIDのみを抽出
        version_ids = [v['VersionId'] for v in sorted_versions if v.get('VersionId') and v.get('VersionId') != 'null']
        return version_ids
    
    except Exception as e:
        print(f"❌ ERROR listing S3 versions: {e}")
        return []

def get_previous_version_content(bucket_name: str, key: str) -> Optional[str]:
    """S3キーの1つ前のバージョンのファイル内容を取得する"""
    version_ids = get_s3_file_versions(bucket_name, key)
    
    # version_ids[0] は今アップロードしたばかりの最新バージョン
    # version_ids[1] がその前のバージョン
    if len(version_ids) < 2:
        print("⚠️ Warning: Only one or zero file versions found. Cannot determine previous version for diff.")
        return None
    
    previous_version_id = version_ids[1] # 2番目の要素が1つ前のバージョン
    print(f"Previous Version ID found: {previous_version_id}")
    
    # バージョンIDを指定してコンテンツを取得
    content = read_s3_content(bucket_name, key, version_id=previous_version_id)
    return content

# =========================================================================
# 差分生成ロジック (最終修正版: ノード定義の包含ロジックを修正)
# =========================================================================

def extract_mermaid_elements(mermaid_code: str) -> Dict[str, Union[List[str], List[str]]]:
    """
    Mermaidコードから、ノード定義行と接続定義行を分離して抽出する。
    """
    lines = {
        'nodes': [],
        'connections': [],
        'tgw_id': 'tgw-084ee5f3ada7fea1c' # デフォルト値を設定
    }
    in_code_block = False
    
    for line in mermaid_code.split('\n'):
        # タブ文字(\t)と全角スペース(\xa0)を排除してスペースに統一
        line = line.strip().replace('\xa0', ' ').replace('\t', ' ')
        
        if line.startswith("```mermaid"):
            in_code_block = True
            continue
        elif line.startswith("```"):
            in_code_block = False
            continue
        
        if in_code_block:
            # TGW IDの抽出
            tgw_match = re.search(r'subgraph Transit Gateway (tgw-[0-9a-f]{17})', line)
            if tgw_match:
                lines['tgw_id'] = tgw_match.group(1)

            # 除外する行
            if (line.startswith(('flowchart', 'graph', 'subgraph', 'end', '%%', '注', 'direction', 'classDef', 'linkStyle')) or 
                not line):
                continue
                
            # ノード定義行の検出 (例: NODEID(Label) または NODEID[Label])
            is_node_definition = re.match(r'^[A-Z0-9_-]+[\(\{].*[\)\}]$', line) and ('-->' not in line)
            
            if is_node_definition:
                # 定義行をそのまま保存
                lines['nodes'].append(line)
            # 接続定義行の検出 (例: A <-- B, B <--> C)
            elif '-->' in line or '<--' in line:
                # 接続ラベルを含めてオリジナルを保存
                lines['connections'].append(line)
                
    # 重複を排除して返す
    lines['nodes'] = sorted(list(set(lines['nodes'])))
    lines['connections'] = sorted(list(set(lines['connections'])))
    
    return lines


def generate_diff_mermaid(current_code: str, previous_code: Optional[str]) -> Optional[str]:
    """
    新旧のMermaidコードを比較し、差分（追加されたノードと接続）のみを抽出し、
    レンダリング可能なMermaidコードとしてラップして返す。
    """
    if not previous_code:
        print("Diff skipped: Previous version content is missing.")
        return None

    # 1. 有効なMermaid要素を抽出
    current_elements = extract_mermaid_elements(current_code)
    previous_elements = extract_mermaid_elements(previous_code)

    tgw_id = current_elements.get('tgw_id', 'tgw-084ee5f3ada7fea1c')

    # 2. 差分を計算
    added_nodes_defs = set(current_elements['nodes']) - set(previous_elements['nodes'])
    added_connections = set(current_elements['connections']) - set(previous_elements['connections'])
    
    total_changes = len(added_nodes_defs) + len(added_connections)
    
    if total_changes == 0:
        print("Diff skipped: No significant structural changes found between versions (nodes or connections).")
        return None

    print(f"Found {len(added_nodes_defs)} new nodes and {len(added_connections)} new connections for diff rendering.")

    # 3. 必要なノードIDを収集し、定義を取得
    nodes_to_include_defs = {}
    required_node_ids = set() 
    all_current_node_ids = set() # 現在のフル図にある全てのノードID

    for node_def in current_elements['nodes']:
        # ノードIDを抽出 (例: ASP0201(ASP0201...) から ASP0201 を抽出)
        node_id_match = re.match(r'^([A-Z0-9_-]+)[\(\{].*[\)\}]$', node_def)
        if node_id_match:
            node_id = node_id_match.group(1)
            all_current_node_ids.add(node_id)
            nodes_to_include_defs[node_id] = node_def # 全ノードの定義を一旦保持

    # (i) 新規に追加されたノードの定義を収集
    for node_def in added_nodes_defs:
        node_id_match = re.match(r'^([A-Z0-9_-]+)[\(\{].*[\)\}]$', node_def)
        if node_id_match:
            required_node_ids.add(node_id_match.group(1))

    # (ii) 💡 修正: 新規接続で使用されている全てのノードIDを収集
    for connection_line in added_connections:
        conn_match = re.match(r'^\s*([A-Z0-9_-]+)\s*[\-<]+.*[\->]+\s*([A-Z0-9_-]+)\s*$', connection_line.strip())
        if conn_match:
            node_a = conn_match.group(1)
            node_b = conn_match.group(2)
            
            # フル図に存在するノードのみを対象とする
            if node_a in all_current_node_ids:
                required_node_ids.add(node_a)
            if node_b in all_current_node_ids:
                required_node_ids.add(node_b)

    # 4. 差分 Mermaid 構文の生成
    diff_mermaid_lines = []
    
    diff_mermaid_lines.append("```mermaid")
    diff_mermaid_lines.append("flowchart TB") 
    diff_mermaid_lines.append(f"    subgraph Transit Gateway {tgw_id}")
    diff_mermaid_lines.append("\n        %% Attachment ノードの定義 (新規ノードと新規接続で使用される既存ノード)")

    # ノード定義
    for node_id in sorted(required_node_ids):
        def_line = nodes_to_include_defs.get(node_id)
        if def_line:
            diff_mermaid_lines.append(f"        {def_line}")
    
    diff_mermaid_lines.append("\n        %% 疎通成立 (新規接続のみ)")
    
    # 差分接続の配置
    for line in added_connections:
        modified_line = re.sub(r'疎通成立\s*\(Reachability\)', 
                               r'疎通成立 (New Reachability)', line)
        diff_mermaid_lines.append(f"        {modified_line}")
            
    diff_mermaid_lines.append("\n      %% 注: この図は最新バージョンに追加されたノードと接続のみを表します。")
    diff_mermaid_lines.append("    end")
    diff_mermaid_lines.append("```")
    
    final_mermaid = "\n".join(diff_mermaid_lines)
    return final_mermaid.replace('\xa0', ' ').replace('\t', ' ')

# =========================================================================
# 解析ロジックヘルパー関数 (変更なし)
# =========================================================================

def get_attachment_info(logical_id):
    """CFnの論理IDからMermaidで安全なノードIDと表示名を生成する"""
    prefix_parts = logical_id.split('TGW')
    name_base = logical_id
    if len(prefix_parts) > 1:
        name_segment = prefix_parts[1]
        if 'ASSOCIATETo' in name_segment:
            name_base = name_segment.split('ASSOCIATETo')[0]
        elif 'PROPAGATETo' in name_segment:
            name_base = name_segment.split('PROPAGATETo')[0]
        else:
            name_base = name_segment
    
    display_name = name_base.upper() if name_base else logical_id[:10].upper()
    
    if display_name.startswith('GCOPM'):
        display_name = display_name[len('GCOPM'):]
    
    node_id = f"{display_name}".replace('-', '').replace('_', '').replace(' ', '')
    attach_ref = f"!Ref {logical_id}" 
    
    return {
        'node_id': node_id,
        'display_name': display_name, 
        'attach_ref': attach_ref 
    }


# =========================================================================
# コア解析ロジック関数
# =========================================================================

def parse_cfn_and_generate_mermaid(yaml_data: str, asp_mapping: Dict[str, str]) -> str:
    """CFn YAMLデータを解析し、Mermaid記法（疎通成立）を生成する"""
    if not yaml_data:
        return ""

    try:
        data = yaml.load(StringIO(yaml_data), Loader=CfnSafeLoader)
    except yaml.YAMLError as e:
        error_detail = str(e).split('\n')[0]
        print(f"❌ ERROR parsing YAML: {error_detail}")
        return f"Error parsing YAML: {error_detail}"

    resources = data.get('Resources', {})
    rtb_map = {}
    att_map = {}
    att_display_info = {}
    associations = {}
    propagations = defaultdict(set)

    for logical_id, props in resources.items():
        if props is None or logical_id.startswith('___GROUP_SEPARATOR_'):
            print(f"Skipping separator or null resource: {logical_id}")
            continue

        resource_type = props.get('Type')
        properties = props.get('Properties', {})

        if resource_type == 'AWS::EC2::TransitGatewayRouteTable':
            suffix = logical_id.replace('TgwRTB', '')
            suffix = suffix.replace('Hubdev801PrdTokyoGcopm', '').replace('Hubdev801PrdTokyo', '')
            rtb_node_id = f"RTB{suffix.upper()}".replace('_', '')
            rtb_map[logical_id] = rtb_node_id

        if resource_type in ['AWS::EC2::TransitGatewayRouteTableAssociation', 'AWS::EC2::TransitGatewayRouteTablePropagation']:
            att_id_ref = properties.get('TransitGatewayAttachmentId')
            rtb_ref = properties.get('TransitGatewayRouteTableId')
            
            att_node_id = None
            att_logical_id_base = None
            att_ref_for_display = None
            
            if isinstance(att_id_ref, str):
                if att_id_ref.startswith('!Ref '):
                    att_logical_id_base = att_id_ref.split(' ')[-1].strip("'\"")
                    att_ref_for_display = att_id_ref
                elif att_id_ref.startswith('tgw-attach-'):
                    att_logical_id_base = logical_id
                    att_ref_for_display = att_id_ref
                else:
                    continue
            else:
                continue

            if att_logical_id_base:
                att_info = get_attachment_info(att_logical_id_base)
                att_node_id = att_info['node_id']
                
                display_value = att_ref_for_display
                if display_value.lower().startswith('tgw-attach-'):
                    asp_name = asp_mapping.get(display_value.lower())
                    if asp_name:
                        display_value = asp_name
                
                att_info['attach_ref'] = display_value
                att_map[att_logical_id_base] = att_node_id
                att_display_info[att_node_id] = att_info

            if isinstance(rtb_ref, str) and rtb_ref.startswith('!Ref '):
                rtb_logical_id = rtb_ref.split(' ')[-1].strip("'\"")
                if att_node_id and rtb_logical_id in rtb_map:
                    rtb_node = rtb_map[rtb_logical_id]
                    if resource_type == 'AWS::EC2::TransitGatewayRouteTableAssociation':
                        associations[att_node_id] = rtb_node
                    elif resource_type == 'AWS::EC2::TransitGatewayRouteTablePropagation':
                        propagations[rtb_node].add(att_node_id)


    tgw_param = data.get('Parameters', {}).get('TransitGatewayId', {})
    tgw_id = str(tgw_param.get('Default', 'tgw-084ee5f3ada7fea1c')) 

    mermaid_lines = []
    mermaid_lines.append("```mermaid")
    mermaid_lines.append("flowchart TB")
    mermaid_lines.append(f"    subgraph Transit Gateway {tgw_id}")
    mermaid_lines.append("\n        %% Attachment ノードの定義")
    
    sorted_att_nodes = sorted(att_display_info.keys())
    for node_id in sorted_att_nodes:
        info = att_display_info[node_id]
        mermaid_lines.append(f"        {node_id}({info['display_name']} <br> {info['attach_ref']})")

    if 'ONPRE' not in att_display_info.keys():
        mermaid_lines.append(f"        ONPRE(ONPRE)")

    mermaid_lines.append("\n        %% 疎通成立 (Reachability) - AssociationとPropagationの双方向チェック")
    all_nodes_for_check = sorted_att_nodes + ['ONPRE'] if 'ONPRE' not in att_display_info.keys() else sorted_att_nodes

    for i, node_a in enumerate(all_nodes_for_check):
        for node_b in all_nodes_for_check[i+1:]:
            rtb_a_assoc = associations.get(node_a)
            rtb_b_assoc = associations.get(node_b)

            if node_a in sorted_att_nodes and node_b in sorted_att_nodes:
                if not rtb_a_assoc or not rtb_b_assoc:
                    continue
                a_to_b = node_b in propagations.get(rtb_a_assoc, set())
                b_to_a = node_a in propagations.get(rtb_b_assoc, set())
                if a_to_b and b_to_a:
                    mermaid_lines.append(f"        {node_a} <-- 疎通成立 (Reachability) --> {node_b}")

    mermaid_lines.append("\n      %% 注: 疎通成立はAssociationとPropagationの双方向の組み合わせに基づきます。")
    mermaid_lines.append("    end")
    mermaid_lines.append("```")
    
    return "\n".join(mermaid_lines).replace('\xa0', ' ').replace('\t', ' ')

# =========================================================================
# AWS Lambda ハンドラ用ヘルパー
# =========================================================================

def extract_agent_parameters(event: Dict[str, Any]) -> Dict[str, str]:
    """Agentのネストされたペイロードから 'bucket' と 'dynamic_prefix' を抽出する"""
    params = {}
    if 'bucket' in event:
        params['bucket'] = event['bucket']
    if 'dynamic_prefix' in event:
        params['dynamic_prefix'] = event['dynamic_prefix']
    try:
        properties = event['requestBody']['content']['application/json']['properties']
        for prop in properties:
            if prop['name'] == 'bucket':
                params['bucket'] = prop['value']
            elif prop['name'] == 'dynamic_prefix':
                params['dynamic_prefix'] = prop['value']
    except (KeyError, TypeError):
        pass
    return params

def extract_agent_metadata(event: Dict[str, Any]) -> Dict[str, str]:
    return {
        'actionGroup': event.get('actionGroup', 'making-mermaid'),
        'apiPath': event.get('apiPath', '/generateMermaidDiagram'),
        'httpMethod': event.get('httpMethod', 'POST')
    }

def build_agent_response(agent_info, body_message, response_state, full_output_key=None, diff_output_key=None):
    body_payload = {"status_message": body_message}
    if full_output_key:
        body_payload['inputKey'] = full_output_key 
        body_payload['s3_output_uri'] = full_output_key 
    if diff_output_key:
        body_payload['diffInputKey'] = diff_output_key
    
    return {
        'messageVersion': '1.0',
        'response': {
            'actionGroup': agent_info['actionGroup'],
            'apiPath': agent_info['apiPath'],
            'httpMethod': agent_info['httpMethod'],
            'functionResponse': {
                'responseState': response_state,
                'responseBody': {'application/json': {'body': json.dumps(body_payload)}}
            }
        }
    }

# =========================================================================
# Lambda Handler
# =========================================================================

def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    print("--- Lambda Handler Started (Agent Mode) ---")
    agent_info = extract_agent_metadata(event)
    extracted_params = extract_agent_parameters(event)
    input_bucket = extracted_params.get('bucket')
    dynamic_prefix = extracted_params.get('dynamic_prefix')
    
    if not input_bucket or not dynamic_prefix:
        return build_agent_response(agent_info, "Missing bucket or prefix", 'FAILURE')

    input_key_cfn = f"{dynamic_prefix}/cfn/{CFN_YAML_FILE_NAME}"
    mapping_key = f"{dynamic_prefix}{MAPPING_KEY_SUFFIX}"
    output_key_mermaid = f"{dynamic_prefix}{OUTPUT_KEY_SUFFIX}"
    diff_output_key_mermaid = f"{dynamic_prefix}{DIFF_OUTPUT_KEY_SUFFIX}"

    asp_mapping = load_asp_mapping(input_bucket, mapping_key)
    yaml_content = read_yaml_from_s3(input_bucket, input_key_cfn) 
    
    if not yaml_content:
        return build_agent_response(agent_info, "YAML not found", 'FAILURE')
    
    current_mermaid_code = parse_cfn_and_generate_mermaid(yaml_content, asp_mapping)
    if not write_mermaid_to_s3(current_mermaid_code, input_bucket, output_key_mermaid):
        return build_agent_response(agent_info, "S3 write failed", 'FAILURE')

    previous_mermaid_code = get_previous_version_content(input_bucket, output_key_mermaid)
    diff_mermaid_code = generate_diff_mermaid(current_mermaid_code, previous_mermaid_code)
    
    diff_ret = None
    if diff_mermaid_code:
        if write_mermaid_to_s3(diff_mermaid_code, input_bucket, diff_output_key_mermaid):
            diff_ret = diff_output_key_mermaid

    return build_agent_response(agent_info, "Success", 'SUCCESS', output_key_mermaid, diff_ret)