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
def entity_create_data(client: ArweaveClient, entity_id: str, event_start_utc: datetime.datetime, event_end_utc: datetime.datetime):
    setup_logger(level="INFO", log_size="100MB")
    
    # Convert to Beijing time and log
    beijing_tz = datetime.timezone(datetime.timedelta(hours=8))
    event_start_beijing = event_start_utc.astimezone(beijing_tz)
    event_end_beijing = event_end_utc.astimezone(beijing_tz)
    
    # Log time information
    logger.info(f"Event start (UTC): {event_start_utc}")
    logger.info(f"Event end (UTC): {event_end_utc}")
    logger.info(f"Event start (Beijing): {event_start_beijing}")
    logger.info(f"Event end (Beijing): {event_end_beijing}")
    logger.info(
        f"Event start: {client.time_to_timestamp(event_start_utc)}, \
        event end: {client.time_to_timestamp(event_end_utc)} in timestamp"
    )

    # Get transaction summary
    entity_create_res = client.get_all_transaction_summaries(
        entity_id=entity_id,
        query_type="received_action_entityCreate",
        min_ingested_at=client.time_to_timestamp(event_start_utc),
        max_ingested_at=client.time_to_timestamp(event_end_utc),
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
    print(f"entity_update_position： {entity_update_position_total}")
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

# 1.2 Get received_unique_user_data
def received_user_data(received_res):
    # Add filtering logic here
    user_received_res = [
        tx for tx in received_res 
        if not any(tag['name'] == 'From-Process' for tag in tx['node']['tags'])
    ]
    unique_user_received_addresses = list(set([tx['node']['owner']['address'] for tx in user_received_res]))
    print(f"Unique user address statistics: {len(unique_user_received_addresses)}")
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
    # All players' chat_message count
    print(f"Player chat_message count: {len(chat_message_data_res)}")
    return chat_message_data_res

def create_statistics_plot(
    entity_create_total: int,
    entity_update_position_total: int,
    unique_users_count: int,
    chat_messages_count: int
):
    """
    Create a subplot displaying four statistics
    """
    import plotly.subplots as sp
    import plotly.graph_objects as go

    # 创建2x2的子图布局
    fig = sp.make_subplots(
        rows=2, cols=2,
        subplot_titles=(
            'Entity Create Count',
            'Entity Update Position Count',
            'Unique Users Count',
            'Chat Message Count'
        )
    )

    # 添加四个柱状图
    fig.add_trace(
        go.Bar(x=['Entity Create'], y=[entity_create_total], marker_color='rgb(55, 83, 109)'),
        row=1, col=1
    )
    fig.add_trace(
        go.Bar(x=['Entity Update Position'], y=[entity_update_position_total], marker_color='rgb(26, 118, 255)'),
        row=1, col=2
    )
    fig.add_trace(
        go.Bar(x=['Unique Users'], y=[unique_users_count], marker_color='rgb(158, 202, 225)'),
        row=2, col=1
    )
    fig.add_trace(
        go.Bar(x=['Chat Messages'], y=[chat_messages_count], marker_color='rgb(98, 182, 149)'),
        row=2, col=2
    )

    # 更新布局
    fig.update_layout(
        title_text="Game Data Statistics",
        showlegend=False,
        height=800,
        width=1000,
    )

    # Show the chart
    fig.show()

    # Save as HTML file
    fig.write_html("game_statistics.html")
    logger.info("Statistics saved as game_statistics.html")

if __name__ == "__main__":
    # Initialize ArweaveClient
    config = ArweaveConfig(
        max_retries=3,
        batch_sleep_time=1.0
    )
    client = ArweaveClient(config)
    
    entity_id = "lA4WPP5v9iUowzLJtCjZsSH_m6WV2FUbGlPSlG7KbnM"


    # 1.1
    event_start_str_utc = "2024-12-22 8:06:50"
    event_end_str_utc = "2024-12-22 8:10:55"
    event_start_utc = client.str_to_datetime(event_start_str_utc)
    event_end_utc = client.str_to_datetime(event_end_str_utc)
    entity_create_res = entity_create_data(client, entity_id, event_start_utc, event_end_utc)
    entity_update_position_res = entity_update_position_data(client, entity_id, event_start_utc, event_end_utc)

    # 1.2
    event_start_str_utc = "2024-12-14 7:06:50"
    event_end_str_utc = "2024-12-14 7:15:55"
    event_start_utc = client.str_to_datetime(event_start_str_utc)
    event_end_utc = client.str_to_datetime(event_end_str_utc)
    # Temporarily use data fetched from the chain again; if based on 1.1, directly change received_res to the two summaries from 1.1
    received_res = received(client, entity_id, event_start_utc, event_end_utc) 
    received_user_data_res = received_user_data(received_res)
    unique_user_received_addresses = list(set([tx['node']['owner']['address'] for tx in received_user_data_res]))

    # # 1.3
    chat_message_data_res = chat_message_data_from_received(received_res)
    
    # # Get statistical data
    entity_create_total = len(entity_create_res)
    entity_update_position_total = len(entity_update_position_res)
    unique_users_count = len(unique_user_received_addresses)
    chat_messages_count = len(chat_message_data_res)

    # Create statistics plot
    create_statistics_plot(
        entity_create_total,
        entity_update_position_total,
        unique_users_count,
        chat_messages_count
    )
