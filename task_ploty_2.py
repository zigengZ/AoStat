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

# 2.1 Get token_transfers
def token_transfers_data(client: ArweaveClient, entity_id: str, event_start_utc: datetime.datetime, event_end_utc: datetime.datetime):
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

    # Get transaction summaries
    token_transfers_res = client.get_all_transaction_summaries(
        entity_id=entity_id,
        query_type="token_transfers_from_process",
        search_tags_from_process="pazXumQI-HPH7iFGfTC-4_7biSnqz_U67oFAGry5zUY",
        min_ingested_at=client.time_to_timestamp(event_start_utc),
        max_ingested_at=client.time_to_timestamp(event_end_utc),
    )

    # Calculate all tickets
    all_ticket_num = 0
    for item in token_transfers_res:
        """ item is:
            {'cursor': 'eyJzZWFyY2hfYWZ0ZXIiOlsxNzM0NTcyMzk2LCIyZG1FMm9CSXRuNXNsMW9SbXgzSUhQOUFsdWhvOE1uTVZET3VuUnRtZE1FIl0sImluZGV4IjoxNTh9', 'node': {'id': '2dmE2oBItn5sl1oRmx3IHP9Aluho8MnMVDOunRtmdME', 'recipient': 'sZe_mf4uJs1khzh0QZmNnaxdoXtBa51LRh2uhnDyk3Y', 'ingested_at': 1734572396, 'block': {'timestamp': 1734570863, 'height': 1570955}, 'tags': [{'name': 'Ref_', 'value': '785338'}, {'name': 'X-Origin', 'value': ''}, {'name': 'Action', 'value': 'Debit-Notice'}, {'name': 'X-Reference', 'value': ''}, {'name': 'Recipient', 'value': '9YcwODxcwVKMLM2_PtrMuTMOACbv3iq0vU4-z0Na9rg'}, {'name': 'Quantity', 'value': '200000000000000'}, {'name': 'Data-Protocol', 'value': 'ao'}, {'name': 'Type', 'value': 'Message'}, {'name': 'Variant', 'value': 'ao.TN.1'}, {'name': 'From-Process', 'value': 'pazXumQI-HPH7iFGfTC-4_7biSnqz_U67oFAGry5zUY'}, {'name': 'From-Module', 'value': 'Pq2Zftrqut0hdisH_MC2pDOT6S4eQFoxGsFUzR6r350'}, {'name': 'Pushed-For', 'value': 'RpRAkxjLBLWWTdvG9SNr_gljtrjOk8VlqIqBB8ofNBI'}], 'data': {'size': '102'}, 'owner': {'address': 'fcoN_xJeisVsPXA-trzVAuIiqO3ydLQxM-L4XbrQKzY'}}}
        """
        # Extract the value of the tag with name 'Quantity' from item['node']['tags'] and divide by 5000000000000 to calculate the number of tickets
        quantity = next(tag['value'] for tag in item['node']['tags'] if tag['name'] == 'Quantity')
        ticket_num = int(quantity) / 5000000000000
        all_ticket_num += ticket_num

    print(f"Sold: {all_ticket_num} tickets")
    token_transfers_total = len(token_transfers_res)

    # Print the first and last
    # print(f"token_transfers_res[0]: {token_transfers_res[0]}")
    # print(f"token_transfers_res[-1]: {token_transfers_res[-1]}")
    return token_transfers_res

def get_catch_data(client: ArweaveClient, entity_id: str, event_start_utc: datetime.datetime, event_end_utc: datetime.datetime):
    setup_logger(level="INFO", log_size="100MB")

    catch_res = client.get_all_transaction_summaries(
        entity_id=entity_id,
        query_type="sent_action_catch",
        min_ingested_at=client.time_to_timestamp(event_start_utc),
        max_ingested_at=client.time_to_timestamp(event_end_utc),
    )
    # Print the first and last
    print(f"catch_res[0]: {catch_res[0]}")
    print(f"catch_res[-1]: {catch_res[-1]}")

    # Define valuescatch_types
    valuescatch_types = {2: "Common",3: "Rare",4: "Legendary",5: "Crown",6: "MessageBottle",7: "Boot",8: "Chips",9: "Hat",10: "Trash",}
    
    # Catch statistics
    catch_count = {"Common": 0, "Rare": 0, "Legendary": 0, "Crown": 0, "MessageBottle": 0, "Boot": 0, "Chips": 0, "Hat": 0, "Trash": 0}
    for item in catch_res:
        """
        item is:
            {'cursor': 'eyJzZWFyY2hfYWZ0ZXIiOlsxNzM0NjM4MjEzLCJ4SE94bnBOVFNya0tKb3lQVHhvNU9fWTJlNlplUHJnQmE2YWtCT1RoNy0wIl0sImluZGV4Ijo5fQ==', 'node': {'id': 'xHOxnpNTSrkKJoyPTxo5O_Y2e6ZePrgBa6akBOTh7-0', 'recipient': 'qm42I2DnuXg62dfhJ4nmRDCCLQttCGMcMdlTazqH748', 'ingested_at': 1734638213, 'block': {'timestamp': 1734637068, 'height': 1571476}, 'tags': [{'name': 'Ref_', 'value': '153805'}, {'name': 'Action', 'value': 'Catch'}, {'name': 'Sender', 'value': '-fhAVRzhpIsEymbDKgjGZvUuHQpzwuN-n2t2h0z0b9I'}, {'name': 'Casts', 'value': '7'}, {'name': 'Name', 'value': 'bagusdwi'}, {'name': 'Catch', 'value': '7'}, {'name': 'Data-Protocol', 'value': 'ao'}, {'name': 'Type', 'value': 'Message'}, {'name': 'Variant', 'value': 'ao.TN.1'}, {'name': 'From-Process', 'value': 'sZe_mf4uJs1khzh0QZmNnaxdoXtBa51LRh2uhnDyk3Y'}, {'name': 'From-Module', 'value': 'cNlipBptaF9JeFAf4wUmpi43EojNanIBos3EfNrEOWo'}, {'name': 'Pushed-For', 'value': 'D9XH-MN4Yllsti81Fy4O886ajUYRI-xYBShs0OSbMws'}], 'data': {'size': '1'}, 'owner': {'address': 'fcoN_xJeisVsPXA-trzVAuIiqO3ydLQxM-L4XbrQKzY'}}}
        """
        # Extract all values of the tag with name 'Catch' from item['node']['tags']
        catch_values = [tag['value'] for tag in item['node']['tags'] if tag['name'] == 'Catch']
        # Count different types of values
        for value in catch_values:
            catch_count[valuescatch_types[int(value)]] += 1
    
    print(f"Fish type statistics: {catch_count}")
    return catch_res

def visualize_data(token_transfers_res, catch_res):
    """
    Visualize data statistics using Plotly
    """
    import plotly.graph_objects as go
    from plotly.subplots import make_subplots
    
    # Create subplot layout
    fig = make_subplots(
        rows=2, cols=1,
        subplot_titles=('Ticket Sales Time Distribution', 'Catch Item Statistics')
    )
    
    # 1. Ticket sales time series plot
    timestamps = []
    ticket_counts = []
    cumulative_tickets = 0
    
    for item in token_transfers_res:
        timestamp = datetime.datetime.fromtimestamp(item['node']['ingested_at'])
        quantity = int(next(tag['value'] for tag in item['node']['tags'] if tag['name'] == 'Quantity'))
        tickets = quantity / 5000000000000
        cumulative_tickets += tickets
        
        timestamps.append(timestamp)
        ticket_counts.append(cumulative_tickets)
    
    fig.add_trace(
        go.Scatter(
            x=timestamps,
            y=ticket_counts,
            mode='lines+markers',
            name='Cumulative Ticket Sales'
        ),
        row=1, col=1
    )
    
    # 2. Catch item statistics bar chart
    catch_types = {2: "Common", 3: "Rare", 4: "Legendary", 5: "Crown", 
                  6: "MessageBottle", 7: "Boot", 8: "Chips", 9: "Hat", 10: "Trash"}
    
    catch_count = defaultdict(int)
    for item in catch_res:
        catch_value = next(tag['value'] for tag in item['node']['tags'] if tag['name'] == 'Catch')
        catch_type = catch_types[int(catch_value)]
        catch_count[catch_type] += 1
    
    fig.add_trace(
        go.Bar(
            x=list(catch_count.keys()),
            y=list(catch_count.values()),
            name='Catch Statistics'
        ),
        row=2, col=1
    )
    
    # Update layout
    fig.update_layout(
        height=800,
        title_text="Game Data Statistics",
        showlegend=True
    )
    
    # Update x-axis and y-axis labels
    fig.update_xaxes(title_text="Time", row=1, col=1)
    fig.update_xaxes(title_text="Catch Item Type", row=2, col=1)
    # 更新x轴和y轴标签
    fig.update_xaxes(title_text="时间", row=1, col=1)
    fig.update_xaxes(title_text="捕获物品类型", row=2, col=1)
    fig.update_yaxes(title_text="累计票数", row=1, col=1)
    fig.update_yaxes(title_text="数量", row=2, col=1)
    
    # 显示图表
    fig.show()
    
    # 保存为静态HTML文件
    output_file = "fishing_stats.html"
    fig.write_html(output_file)
    logger.info(f"图表已保存至: {output_file}")
    
    # # 可选：同时保存为图片
    # fig.write_image("fishing_stats.png")

if __name__ == "__main__":
    # 初始化 ArweaveClient
    config = ArweaveConfig(
        max_retries=3,
        batch_sleep_time=1.0
    )
    client = ArweaveClient(config)
    
    # 时间配置
    entity_id = "sZe_mf4uJs1khzh0QZmNnaxdoXtBa51LRh2uhnDyk3Y"



    # 2.1 获取token_transfers 的From-Process = pazXumQI-HPH7iFGfTC-4_7biSnqz_U67oFAGry5zUY 的票数
    event_start_str_utc = "2024-12-19 1:06:50"
    event_end_str_utc = "2024-12-20 23:06:55"
    utc_tz = datetime.timezone.utc
    event_start_utc = datetime.datetime.strptime(event_start_str_utc, "%Y-%m-%d %H:%M:%S").replace(tzinfo=utc_tz)
    event_end_utc = datetime.datetime.strptime(event_end_str_utc, "%Y-%m-%d %H:%M:%S").replace(tzinfo=utc_tz)
    token_transfers_res = token_transfers_data(client, entity_id, event_start_utc, event_end_utc)

    # 2.2 获取catch数据
    
    event_start_str_utc = "2024-12-19 19:56:50"
    event_end_str_utc = "2024-12-19 19:56:55"
    utc_tz = datetime.timezone.utc
    event_start_utc = datetime.datetime.strptime(event_start_str_utc, "%Y-%m-%d %H:%M:%S").replace(tzinfo=utc_tz)
    event_end_utc = datetime.datetime.strptime(event_end_str_utc, "%Y-%m-%d %H:%M:%S").replace(tzinfo=utc_tz)
    catch_res = get_catch_data(client, entity_id, event_start_utc, event_end_utc)
    
    # 2.3 数据可视化
    visualize_data(token_transfers_res, catch_res)
