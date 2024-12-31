from collections import defaultdict
import requests
import json
from typing import List, Dict, Optional
import datetime
import time
from loguru import logger
import sys
from pprint import pprint
import pandas as pd
import os
from ar_onchain_util import ArweaveClient, ArweaveConfig

def setup_logger(level="INFO", log_size="100MB"):
    """
    Set up the logger configuration.

    Parameters:
    level (str): Log level, default is "INFO"
    log_size (str): Maximum size of the log file, default is "100MB"

    Function:
    - Remove existing log handlers
    - Add standard error output handler
    - Add file handler with size rotation
    - Log initialization information
    """
    logger.remove()
    logger.add(sys.stderr, level=level)
    logger.add(f"./logs/{level.lower()}.log", rotation=log_size, level=level)
    logger.info(f"Logging level set to: {level} Size: {log_size}")

# 1.1 Get entity_create_data
def entity_create_data(client: ArweaveClient, entity_id: str, event_start_utc: datetime.datetime, event_end_utc: datetime.datetime, save_to: str):
    setup_logger(level="INFO", log_size="100MB")
    
    # 转换为北京时间并记录
    beijing_tz = datetime.timezone(datetime.timedelta(hours=8))
    event_start_beijing = event_start_utc.astimezone(beijing_tz)
    event_end_beijing = event_end_utc.astimezone(beijing_tz)
    
    # 记录时间信息
    logger.info(f"Event start (UTC): {event_start_utc}")
    logger.info(f"Event end (UTC): {event_end_utc}")
    logger.info(f"Event start (Beijing): {event_start_beijing}")
    logger.info(f"Event end (Beijing): {event_end_beijing}")
    logger.info(
        f"Event start: {client.time_to_timestamp(event_start_utc)}, \
        event end: {client.time_to_timestamp(event_end_utc)} in timestamp"
    )

    # 获取交易摘要
    entity_create_res = client.get_all_transaction_summaries(
        entity_id=entity_id,
        query_type="received_action_entityCreate",
        min_ingested_at=client.time_to_timestamp(event_start_utc),
        max_ingested_at=client.time_to_timestamp(event_end_utc),
        check_point_path=save_to
    )
    entity_create_total = len(entity_create_res)
    print(f"entity_create统计： {entity_create_total}")
    return entity_create_res

# 1.1 Get entity_update_position_data
def entity_update_position_data(client: ArweaveClient, entity_id: str, event_start_utc: datetime.datetime, event_end_utc: datetime.datetime):
    entity_update_position_res = client.get_all_transaction_summaries(
        entity_id=entity_id,
        query_type="received_action_entityUpdatePosition",
        min_ingested_at=client.time_to_timestamp(event_start_utc),
        max_ingested_at=client.time_to_timestamp(event_end_utc),
    )

    entity_update_position_total = len(entity_update_position_res)
    print(f"entity_update_position统计： {entity_update_position_total}")
    return entity_update_position_res

# 1.2 Get received_data
def received(client: ArweaveClient, entity_id: str, event_start_utc: datetime.datetime, event_end_utc: datetime.datetime):
    received_res = client.get_all_transaction_summaries(
        entity_id=entity_id,
        query_type="received",
        min_ingested_at=client.time_to_timestamp(event_start_utc),
        max_ingested_at=client.time_to_timestamp(event_end_utc),
    )
    return received_res

# 1.2 Get received_user_data
def received_user_data(received_res):
    # Add filtering logic here
    user_received_res = [
        tx for tx in received_res 
        if not any(tag['name'] == 'From-Process' for tag in tx['node']['tags'])
    ]
    # print(f"user_received_res: {len(user_received_res)}")
    unique_user_received_addresses = list(set([tx['node']['owner']['address'] for tx in user_received_res]))
    print(f"唯一用户地址统计: {len(unique_user_received_addresses)}")
    return user_received_res

# 1.3 Get chatMessage_data
def chat_message_data(client: ArweaveClient, entity_id: str, event_start_utc: datetime.datetime, event_end_utc: datetime.datetime):
    chat_message_res = client.get_all_transaction_summaries(
        entity_id=entity_id,
        query_type="received_action_chatMessage",
        min_ingested_at=client.time_to_timestamp(event_start_utc),
        max_ingested_at=client.time_to_timestamp(event_end_utc),
    )
    return chat_message_res

# 1.3 Get chatMessage_data from 1.2
def chat_message_data_from_received(received_res):
    chat_message_data_res = [
        tx for tx in received_res 
        if any(tag['name'] == 'Action' and tag['value'] == 'ChatMessage' for tag in tx['node']['tags'])
    ]
    # Count of chatMessage for all players
    print(f"玩家chat_message数量: {len(chat_message_data_res)}")
    return chat_message_data_res

if __name__ == "__main__":
    # Initialize ArweaveClient
    config = ArweaveConfig(
        max_retries=3,
        batch_sleep_time=1.0
    )
    client = ArweaveClient(config)
    
    # 时间配置
    event_start_str_utc = "2024-07-26 0:06:50"
    event_end_str_utc = "2025-12-20 8:06:55"
    entity_id = "lA4WPP5v9iUowzLJtCjZsSH_m6WV2FUbGlPSlG7KbnM"

    utc_tz = datetime.timezone.utc
    event_start_utc = datetime.datetime.strptime(event_start_str_utc, "%Y-%m-%d %H:%M:%S").replace(tzinfo=utc_tz)
    event_end_utc = datetime.datetime.strptime(event_end_str_utc, "%Y-%m-%d %H:%M:%S").replace(tzinfo=utc_tz)

    # 1.1
    entity_create_res = entity_create_data(client, entity_id, event_start_utc, event_end_utc, save_to="./data/entity_create_res.json")
    entity_update_position_res = entity_update_position_data(client, entity_id, event_start_utc, event_end_utc)
    # save list of dict to json
    with open(f"./data/entity_create_res.json", "w") as f:
        json.dump(entity_create_res, f)
    with open(f"./data/entity_update_position_res.json", "w") as f:
        json.dump(entity_update_position_res, f)

    # 1.2
    received_res = received(client, entity_id, event_start_utc, event_end_utc)
    received_user_data(received_res)

    # save list of dict to json
    with open(f"./data/received_res.json", "w") as f:
        json.dump(received_res, f)


    # 1.3
    chat_message_data_res = chat_message_data_from_received(received_res)

