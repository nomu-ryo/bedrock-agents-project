#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import io
import re
import json
import logging
import traceback
from collections import defaultdict
from typing import Any, Dict, List, Optional, Tuple

import boto3
import pandas as pd
from botocore.exceptions import ClientError

# ---------------------------
# Logger
# ---------------------------
logger = logging.getLogger("extract_tgw_config_v2")
logger.setLevel(logging.INFO)
if not logger.handlers:
    ch = logging.StreamHandler()
    ch.setFormatter(logging.Formatter('%(asctime)s %(levelname)s %(name)s - %(message)s'))
    logger.addHandler(ch)

# ---------------------------
# Defaults / Constants (機密情報は環境変数から取得するように変更)
# ---------------------------
# GitHub公開時は、ここにある具体的なバケット名やプレフィックスを環境変数経由にする
FALLBACK_DYNAMIC_PREFIX = os.environ.get('FALLBACK_DYNAMIC_PREFIX', 'default-project')
FALLBACK_SOURCE_BUCKET = os.environ.get('FALLBACK_SOURCE_BUCKET', 'your-org-automation-bucket')
FALLBACK_SOURCE_KEY = os.environ.get('FALLBACK_SOURCE_KEY', 'settings/tgw_routing_sheet.xlsx')
FALLBACK_SOURCE_SHEET_NAME = os.environ.get('FALLBACK_SOURCE_SHEET_NAME', 'Sheet1')

# 固定パス・サフィックス（命名規則）
FALLBACK_TARGET_KEY_SUFFIX = 'extractsheet/tgw_config.jsonl'
FALLBACK_MAPPING_KEY_SUFFIX = 'extractsheet/tgw_mapping_table.jsonl'
FALLBACK_TGW_ID_CONFIG_KEY_SUFFIX = 'extractsheet/tgw_id_config.jsonl'

TGW_ASSUME_ROLE_SUFFIX = os.environ.get('TGW_ASSUME_ROLE_SUFFIX', "-tgw-automation-role")
TGW_REGION = os.environ.get('TGW_REGION', 'ap-northeast-1')
ONPRE_RTB_SUFFIX = os.environ.get('ONPRE_RTB_SUFFIX', "-onpre-rtb")

# Strict regex: fullmatch for tgw-attach-... (hex/alphanumeric)
TGW_ATTACH_FULL_PATTERN = re.compile(r'^tgw-attach-[0-9a-z]+$', re.IGNORECASE)

# boto3 clients
s3 = boto3.client('s3')

# ---------------------------
# Helpers: S3 and JSONL
# ---------------------------

def s3_read_text(bucket: str, key: str) -> Optional[str]:
    try:
        obj = s3.get_object(Bucket=bucket, Key=key)
        return obj['Body'].read().decode('utf-8')
    except ClientError as e:
        code = e.response.get('Error', {}).get('Code')
        if code in ('NoSuchKey', '404'):
            logger.info(f"s3_read_text: s3://{bucket}/{key} not found")
            return None
        logger.error(f"s3_read_text: error reading s3://{bucket}/{key}: {e}")
        raise

def s3_write_text(bucket: str, key: str, content: str, content_type: str = 'application/jsonl'):
    try:
        s3.put_object(Bucket=bucket, Key=key, Body=content.encode('utf-8'), ContentType=content_type)
        logger.info(f"s3_write_text: wrote s3://{bucket}/{key} ({len(content)} bytes)")
    except ClientError as e:
        logger.error(f"s3_write_text: failed to write s3://{bucket}/{key}: {e}")
        raise

def safe_json_loads(line: str) -> Optional[Dict[str, Any]]:
    try:
        return json.loads(line)
    except Exception:
        return None

def parse_jsonl(content: Optional[str]) -> List[Dict[str, Any]]:
    if not content:
        return []
    lines = [ln for ln in content.splitlines() if ln.strip()]
    recs = []
    for ln in lines:
        obj = safe_json_loads(ln)
        if obj is not None:
            recs.append(obj)
    return recs

# ---------------------------
# RTB name helpers
# ---------------------------

def get_dynamic_rtb_patterns(dynamic_prefix: str) -> Tuple[re.Pattern, str, str]:
    prefix_escaped = re.escape(dynamic_prefix)
    rtb_pattern = re.compile(rf'{prefix_escaped}-.*-asp(\d{{2}})-(\d{{2}})-tgw-rtb', re.IGNORECASE)
    onpre = f"{dynamic_prefix}{ONPRE_RTB_SUFFIX}"
    return rtb_pattern, onpre, onpre

def generate_new_rtb_name(dynamic_prefix: str, account_id: str, rtb_naming_status: Dict[str, Dict[int, int]]) -> str:
    all_xx = set()
    for g in rtb_naming_status.values():
        all_xx.update(g.keys())
    is_new_system = not bool(all_xx)
    if is_new_system:
        new_asp_xx = 1; new_asp_yy = 1
    elif account_id in rtb_naming_status:
        max_yy = 0; target_xx = 0
        for xx, yy in rtb_naming_status[account_id].items():
            if yy > max_yy:
                max_yy = yy; target_xx = xx
            elif yy == max_yy and xx > target_xx:
                target_xx = xx
        new_asp_xx = target_xx if target_xx > 0 else 1
        new_asp_yy = max_yy + 1
    else:
        max_xx = max(all_xx) if all_xx else 0
        new_asp_xx = max_xx + 1
        new_asp_yy = 1
    return f"{dynamic_prefix}-prd-tokyo-asp{new_asp_xx:02d}-{new_asp_yy:02d}-tgw-rtb"

# ---------------------------
# AssumeRole and EC2 helpers
# ---------------------------

def assume_role_get_creds(account_id: str, role_name: str) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    sts = boto3.client('sts', region_name=TGW_REGION)
    role_arn = f"arn:aws:iam::{account_id}:role/{role_name}"
    try:
        resp = sts.assume_role(RoleArn=role_arn, RoleSessionName="tgw-agent-session")
        creds = resp['Credentials']
        return creds, None
    except ClientError as e:
        logger.error(f"assume_role_get_creds ClientError: {e}")
        return None, str(e)
    except Exception as e:
        logger.error(f"assume_role_get_creds unexpected: {e}")
        return None, str(e)

def ec2_client_with_creds(creds: Dict[str, Any]):
    return boto3.client('ec2', region_name=TGW_REGION,
                        aws_access_key_id=creds['AccessKeyId'],
                        aws_secret_access_key=creds['SecretAccessKey'],
                        aws_session_token=creds['SessionToken'])

def describe_attachment_with_owner(tgw_owner_account_id: str, attachment_id: str, dynamic_prefix: str) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    role_name = dynamic_prefix + TGW_ASSUME_ROLE_SUFFIX
    creds, err = assume_role_get_creds(tgw_owner_account_id, role_name)
    if not creds:
        return None, f"AssumeRole failed: {err}"
    ec2 = ec2_client_with_creds(creds)
    try:
        resp = ec2.describe_transit_gateway_attachments(TransitGatewayAttachmentIds=[attachment_id])
        atts = resp.get('TransitGatewayAttachments', [])
        if not atts:
            return None, f"Attachment {attachment_id} not found"
        return atts[0], None
    except ClientError as e:
        logger.error(f"describe_attachment_with_owner ClientError: {e}")
        return None, str(e)
    except Exception as e:
        logger.error(f"describe_attachment_with_owner unexpected: {e}")
        return None, str(e)

def accept_attachment_via_owner(tgw_owner_account_id: str, attachment_id: str, dynamic_prefix: str) -> Tuple[bool, Optional[str]]:
    role_name = dynamic_prefix + TGW_ASSUME_ROLE_SUFFIX
    creds, err = assume_role_get_creds(tgw_owner_account_id, role_name)
    if not creds:
        return False, f"AssumeRole failed: {err}"
    ec2 = ec2_client_with_creds(creds)
    try:
        resp = ec2.accept_transit_gateway_vpc_attachment(TransitGatewayAttachmentId=attachment_id)
        state = resp.get('TransitGatewayVpcAttachment', {}).get('State')
        logger.info(f"accept_attachment_via_owner: {attachment_id} -> {state}")
        if state in ('available', 'modifying'):
            return True, None
        return False, f"Unexpected state after accept: {state}"
    except ClientError as e:
        logger.error(f"accept_client_error: {e}")
        return False, str(e)
    except Exception as e:
        logger.error(f"accept unexpected: {e}")
        return False, str(e)

def tag_attachment_via_owner_if_no_name(tgw_owner_account_id: str, attachment_id: str, name_value: str, dynamic_prefix: str) -> Tuple[bool, Optional[str]]:
    role_name = dynamic_prefix + TGW_ASSUME_ROLE_SUFFIX
    creds, err = assume_role_get_creds(tgw_owner_account_id, role_name)
    if not creds:
        return False, f"AssumeRole failed: {err}"
    ec2 = ec2_client_with_creds(creds)
    try:
        resp = ec2.describe_transit_gateway_attachments(TransitGatewayAttachmentIds=[attachment_id])
        atts = resp.get('TransitGatewayAttachments', [])
        if not atts:
            return False, f"Attachment not found for tagging: {attachment_id}"
        tags = atts[0].get('Tags', [])
        if any(t.get('Key') == 'Name' for t in tags):
            logger.info(f"tagging skipped: Name already exists for {attachment_id}")
            return True, None
        ec2.create_tags(Resources=[attachment_id], Tags=[{'Key': 'Name', 'Value': name_value}])
        logger.info(f"Created Name tag '{name_value}' for {attachment_id}")
        return True, None
    except ClientError as e:
        logger.error(f"tagging ClientError for {attachment_id}: {e}")
        return False, str(e)
    except Exception as e:
        logger.error(f"tagging unexpected for {attachment_id}: {e}")
        return False, str(e)

# ---------------------------
# Mapping table load/update
# ---------------------------

def load_mapping_table(bucket: str, key: str, rtb_name_pattern_dynamic: re.Pattern, rtb_onpre_dynamic: str) -> Tuple[Dict[str, dict], Dict[str, Dict[int,int]], str]:
    mapping: Dict[str, dict] = {}
    rtb_naming_status: Dict[str, Dict[int,int]] = defaultdict(lambda: defaultdict(int))
    raw = s3_read_text(bucket, key)
    if not raw:
        logger.info(f"No existing mapping at s3://{bucket}/{key}")
        return mapping, rtb_naming_status, ""
    for line in raw.splitlines():
        if not line.strip():
            continue
        rec = safe_json_loads(line)
        if not rec:
            continue
        attach_id = rec.get('tgw-attach-id')
        if attach_id:
            mapping[str(attach_id)] = rec
        rtb_name = rec.get('rtb-name') or ''
        if rtb_name:
            m = rtb_name_pattern_dynamic.search(rtb_name)
            if m and 'account-id' in rec:
                try:
                    asp_xx = int(m.group(1)); asp_yy = int(m.group(2))
                    acc = rec['account-id']
                    prev = rtb_naming_status[acc].get(asp_xx, 0)
                    if asp_yy > prev:
                        rtb_naming_status[acc][asp_xx] = asp_yy
                except Exception:
                    continue
    return mapping, rtb_naming_status, raw or ""

def load_target_account_id(bucket: str, key: str) -> Optional[str]:
    content = s3_read_text(bucket, key)
    if not content:
        logger.error(f"TGW owner account config not found s3://{bucket}/{key}")
        return None
    for ln in content.splitlines():
        if not ln.strip():
            continue
        rec = safe_json_loads(ln)
        if not rec:
            continue
        for k, v in rec.items():
            if 'account' in k.lower():
                return str(v).strip()
    return None

# ---------------------------
# Build tgw_config from dataframe
# ---------------------------

def extract_prefix_from_rtb(rtb_name: str, rtb_name_pattern_dynamic: re.Pattern, rtb_onpre_dynamic: str) -> str:
    m = rtb_name_pattern_dynamic.search(rtb_name)
    if m:
        return f"ASP{m.group(1)}_{m.group(2)}"
    if rtb_name == rtb_onpre_dynamic:
        return "ONPRE"
    return None

def get_prefix_from_attachment_id(attachment_id: str, final_mapping: Dict[str, dict], actual_onpre_attach_id: Optional[str], rtb_name_pattern_dynamic: re.Pattern, rtb_onpre_dynamic: str) -> Optional[str]:
    if not attachment_id:
        return None
    if actual_onpre_attach_id and attachment_id == actual_onpre_attach_id:
        return "ONPRE"
    rtb_name = final_mapping.get(attachment_id, {}).get('rtb-name')
    if rtb_name:
        return extract_prefix_from_rtb(rtb_name, rtb_name_pattern_dynamic, rtb_onpre_dynamic)
    return None

def build_tgw_config_from_df(df: pd.DataFrame, final_mapping: Dict[str, dict], dynamic_prefix: str, rtb_name_pattern_dynamic: re.Pattern, rtb_onpre_dynamic: str, actual_onpre_attach_id: Optional[str]) -> List[dict]:
    records = []
    # カラムインデックスによる動的指定
    e_col = df.columns[4]
    c_col = df.columns[2]
    
    is_onprem_example_skipped = False
    is_vpc_example_skipped = False
    onpre_associate_added = False

    for idx, row in df.iterrows():
        raw_e = row.get(e_col)
        raw_c = row.get(c_col)
        e_val = str(raw_e).strip() if (not pd.isna(raw_e) and raw_e is not None) else ""
        c_val = str(raw_c).strip() if (not pd.isna(raw_c) and raw_c is not None) else ""

        is_vpc_routing = bool(TGW_ATTACH_FULL_PATTERN.fullmatch(c_val))
        
        # サンプル行のスキップロジックの再現
        if is_vpc_routing and not is_vpc_example_skipped:
            is_vpc_example_skipped = True
            continue
        if (not is_vpc_routing) and not is_onprem_example_skipped:
            is_onprem_example_skipped = True
            continue

        if not e_val or not TGW_ATTACH_FULL_PATTERN.fullmatch(e_val):
            continue

        if not is_vpc_routing:
            # Case: VPC <-> On-Prem
            asp_rtb_name = final_mapping.get(e_val, {}).get('rtb-name')
            if not asp_rtb_name:
                continue
            asp_prefix = extract_prefix_from_rtb(asp_rtb_name, rtb_name_pattern_dynamic, rtb_onpre_dynamic)
            if not asp_prefix:
                continue
            onpre_rtb_name = rtb_onpre_dynamic
            onpre_prefix = "ONPRE"
            
            if actual_onpre_attach_id and not onpre_associate_added:
                records.append({"task_id":"TGW_ONPRE_ASSOCIATE","rtb_name":onpre_rtb_name,"attachment_id":actual_onpre_attach_id,"target_attachment_id":None,"action":"associate"})
                onpre_associate_added = True
            
            records.extend([
                {"task_id":f"TGW_{asp_prefix}_ASSOCIATE","rtb_name":asp_rtb_name,"attachment_id":e_val,"target_attachment_id":None,"action":"associate"},
                {"task_id":f"TGW_ONPRE_PROPAGATE","rtb_name":asp_rtb_name,"attachment_id":None,"target_attachment_id":actual_onpre_attach_id,"action":"propagate"},
                {"task_id":f"TGW_{asp_prefix}_PROPAGATE","rtb_name":onpre_rtb_name,"attachment_id":None,"target_attachment_id":e_val,"action":"propagate"}
            ])
        else:
            # Case: VPC <-> VPC
            if not TGW_ATTACH_FULL_PATTERN.fullmatch(c_val):
                continue
            if e_val not in final_mapping or c_val not in final_mapping:
                continue
            
            prefix_c = get_prefix_from_attachment_id(c_val, final_mapping, actual_onpre_attach_id, rtb_name_pattern_dynamic, rtb_onpre_dynamic)
            prefix_e = get_prefix_from_attachment_id(e_val, final_mapping, actual_onpre_attach_id, rtb_name_pattern_dynamic, rtb_onpre_dynamic)
            
            if not prefix_c or not prefix_e:
                continue
            
            rtb_name_c = final_mapping[c_val]['rtb-name']
            rtb_name_e = final_mapping[e_val]['rtb-name']
            
            records.extend([
                {"task_id":f"TGW_{prefix_e}_PROPAGATE","rtb_name":rtb_name_c,"attachment_id":c_val,"target_attachment_id":e_val,"action":"propagate"},
                {"task_id":f"TGW_{prefix_c}_PROPAGATE","rtb_name":rtb_name_e,"attachment_id":e_val,"target_attachment_id":c_val,"action":"propagate"}
            ])
    return records

# ---------------------------
# Core processing
# ---------------------------

def process_excel_and_sync(event_params: Dict[str, Any]) -> Dict[str, Any]:
    dynamic_prefix = event_params['dynamic_prefix']
    source_bucket = event_params['source_bucket']
    full_source_key = event_params['full_source_key']
    source_sheet_name = event_params['source_sheet_name']
    mapping_bucket = event_params['mapping_bucket']
    mapping_key = event_params['mapping_key']
    target_bucket = event_params['target_bucket']
    target_key = event_params['target_key']
    tgw_id_config_key = event_params['tgw_id_config_key']

    rtb_name_pattern_dynamic, rtb_onpre_dynamic, _ = get_dynamic_rtb_patterns(dynamic_prefix)

    tgw_owner_account_id = load_target_account_id(mapping_bucket, tgw_id_config_key)
    if not tgw_owner_account_id:
        raise RuntimeError(f"TGW owner account id config not found at s3://{mapping_bucket}/{tgw_id_config_key}")

    mapping_dict, rtb_naming_status, existing_mapping_raw = load_mapping_table(mapping_bucket, mapping_key, rtb_name_pattern_dynamic, rtb_onpre_dynamic)

    # read excel
    s3obj = s3.get_object(Bucket=source_bucket, Key=full_source_key)
    bytes_io = io.BytesIO(s3obj['Body'].read())
    df = pd.read_excel(bytes_io, sheet_name=source_sheet_name, header=0)

    if len(df.columns) <= 4:
        raise RuntimeError("Excel sheet has insufficient columns.")

    e_col = df.columns[4]
    c_col = df.columns[2]

    new_mapping_entries = []
    tagging_success = []
    tagging_failures = []
    skipped_first_attach = False

    for idx, row in df.iterrows():
        raw_e = row.get(e_col)
        raw_c = row.get(c_col)
        e_val = str(raw_e).strip() if (not pd.isna(raw_e) and raw_e is not None) else ""
        c_val = str(raw_c).strip() if (not pd.isna(raw_c) and raw_c is not None) else ""

        if e_val and TGW_ATTACH_FULL_PATTERN.fullmatch(e_val) and not skipped_first_attach:
            skipped_first_attach = True
            continue
        if c_val and TGW_ATTACH_FULL_PATTERN.fullmatch(c_val):
            continue
        if not e_val or not TGW_ATTACH_FULL_PATTERN.fullmatch(e_val):
            continue

        attach_id = e_val
        att, err = describe_attachment_with_owner(tgw_owner_account_id, attach_id, dynamic_prefix)
        if err: continue

        state = att.get('State')
        resource_owner = att.get('ResourceOwnerId')

        if state == 'pendingAcceptance':
            ok, _ = accept_attachment_via_owner(tgw_owner_account_id, attach_id, dynamic_prefix)
            if not ok: continue
            att, _ = describe_attachment_with_owner(tgw_owner_account_id, attach_id, dynamic_prefix)
            state = att.get('State')

        if state in ('available', 'modifying'):
            tags = att.get('Tags', []) or []
            if any(t.get('Key') == 'Name' for t in tags):
                continue
            
            mapping_has = attach_id in mapping_dict
            rtb_name = mapping_dict[attach_id].get('rtb-name') if mapping_has else generate_new_rtb_name(dynamic_prefix, str(resource_owner), rtb_naming_status)
            
            if not mapping_has:
                m = rtb_name_pattern_dynamic.search(rtb_name)
                if m: rtb_naming_status[str(resource_owner)][int(m.group(1))] = int(m.group(2))

            name_tag_value = re.sub(r'-rtb$', '-attach', rtb_name)
            success, tag_err = tag_attachment_via_owner_if_no_name(tgw_owner_account_id, attach_id, name_tag_value, dynamic_prefix)
            if not success:
                tagging_failures.append({"tgw-attach-id": attach_id, "error": tag_err})
                continue
            
            tagging_success.append({"tgw-attach-id": attach_id, "rtb-name": rtb_name})
            if not mapping_has:
                new_rec = {"account-id": str(resource_owner), "tgw-attach-id": attach_id, "rtb-name": rtb_name}
                new_mapping_entries.append(new_rec)
                mapping_dict[attach_id] = new_rec

    if new_mapping_entries:
        existing_lines = [ln for ln in existing_mapping_raw.splitlines() if ln.strip()]
        new_lines = [json.dumps(rec, separators=(',', ':')) for rec in new_mapping_entries]
        s3_write_text(mapping_bucket, mapping_key, '\n'.join(existing_lines + new_lines) + '\n')

    # Re-load final mapping and build config
    mapping_after = parse_jsonl(s3_read_text(mapping_bucket, mapping_key))
    final_mapping = {rec.get('tgw-attach-id'): rec for rec in mapping_after if rec.get('tgw-attach-id')}
    actual_onpre_id = next((r.get('tgw-attach-id') for r in mapping_after if r.get('rtb-name') == rtb_onpre_dynamic), None)

    tgw_config_records = build_tgw_config_from_df(df, final_mapping, dynamic_prefix, rtb_name_pattern_dynamic, rtb_onpre_dynamic, actual_onpre_id)

    # Merge with existing config
    existing_config_raw = s3_read_text(target_bucket, target_key)
    unique_records = {}
    if existing_config_raw:
        for ln in existing_config_raw.splitlines():
            r = safe_json_loads(ln)
            if r: unique_records[(r.get('task_id'), r.get('rtb_name'), r.get('target_attachment_id'))] = r
    for r in tgw_config_records:
        unique_records[(r.get('task_id'), r.get('rtb_name'), r.get('target_attachment_id'))] = r

    final_list = list(unique_records.values())
    s3_write_text(target_bucket, target_key, '\n'.join([json.dumps(r, separators=(',', ':')) for r in final_list]) + '\n')

    return {"new_mapping_count": len(new_mapping_entries), "tagging_success_count": len(tagging_success), "tagging_failures_count": len(tagging_failures)}

# ---------------------------
# Bedrock Agent Handlers
# ---------------------------

def extract_agent_params(event: Dict[str, Any]) -> Dict[str, Any]:
    # Bedrock Agentからの入力をパース
    extracted = {}
    try:
        properties = event.get('requestBody', {}).get('content', {}).get('application/json', {}).get('properties', [])
        if properties:
            extracted = {p['name']: p.get('value', '') for p in properties if 'name' in p}
        elif not event.get('requestBody'):
            extracted = dict(event)
    except Exception:
        pass
    
    return {
        'dynamic_prefix': extracted.get('dynamic_prefix', FALLBACK_DYNAMIC_PREFIX),
        'source_bucket': extracted.get('source_bucket', FALLBACK_SOURCE_BUCKET),
        'source_key': extracted.get('source_key', FALLBACK_SOURCE_KEY),
        'source_sheet_name': extracted.get('source_sheet_name', FALLBACK_SOURCE_SHEET_NAME),
        'mapping_bucket': extracted.get('mapping_bucket', FALLBACK_SOURCE_BUCKET)
    }

def resolve_s3_keys(params: Dict[str, Any]) -> Dict[str, Any]:
    prefix = params['dynamic_prefix']
    params['target_key'] = f"{prefix}/{FALLBACK_TARGET_KEY_SUFFIX}"
    params['mapping_key'] = f"{prefix}/{FALLBACK_MAPPING_KEY_SUFFIX}"
    params['tgw_id_config_key'] = f"{prefix}/{FALLBACK_TGW_ID_CONFIG_KEY_SUFFIX}"
    params['target_bucket'] = params['source_bucket']
    
    full_key = params['source_key']
    if 'settingsheets/' not in full_key and not full_key.startswith(prefix):
        full_key = f"settingsheets/{full_key}"
    if prefix and not full_key.startswith(prefix):
        full_key = f"{prefix}/{full_key}".replace('//', '/')
    params['full_source_key'] = full_key
    return params

def extractTGWConfig(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    # Main Entry Point for Lambda
    agent_info = {
        'apiPath': event.get('apiPath', '/extractTGWConfig'),
        'httpMethod': event.get('httpMethod', 'POST'),
        'actionGroup': event.get('actionGroup', 'data-extraction')
    }
    try:
        params = resolve_s3_keys(extract_agent_params(event))
        result = process_excel_and_sync(params)
        body = f"Success: {result['new_mapping_count']} new mapping(s), tagging_success={result['tagging_success_count']}. Config: {params['target_key']}"
        
        return {
            'messageVersion': '1.0',
            'response': {
                'actionGroup': agent_info['actionGroup'],
                'apiPath': agent_info['apiPath'],
                'httpMethod': agent_info['httpMethod'],
                'functionResponse': {
                    'responseState': 'SUCCESS',
                    'responseBody': {'application/json': {'body': body, 's3_config_key': f"s3://{params['target_bucket']}/{params['target_key']}"}}
                }
            }
        }
    except Exception as e:
        logger.error(traceback.format_exc())
        return {
            'messageVersion': '1.0',
            'response': {
                'actionGroup': agent_info['actionGroup'],
                'apiPath': agent_info['apiPath'],
                'httpMethod': agent_info['httpMethod'],
                'functionResponse': {'responseState': 'FAILURE', 'responseBody': {'application/json': {'body': str(e)}}}
            }
        }

if __name__ == "__main__":
    # Local Test
    print(json.dumps(extractTGWConfig({}, None), indent=2))