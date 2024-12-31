from dataclasses import dataclass
import json
from typing import List, Dict, Optional, Union, Tuple
import datetime
import requests
import pandas as pd
from loguru import logger
from collections import defaultdict
import time
from tqdm import tqdm
import os

@dataclass
class ArweaveConfig:
    """Arweave query configuration class"""
    graphql_endpoint: str = "https://arweave-search.goldsky.com/graphql"
    max_retries: int = 3
    retry_delay: float = 1.0
    batch_sleep_time: float = 1.0
    max_batch_size: int = 500
    initial_batch_size: int = 100

class ArweaveQueryError(Exception):
    """Base class for Arweave query exceptions"""
    pass

class ArweaveClient:
    """Arweave on-chain data query client"""
    
    def __init__(self, config: Optional[ArweaveConfig] = None):
        """
        Initialize Arweave client

        Args:
            config: Query configuration, if None uses default configuration
        """
        self.config = config or ArweaveConfig()
        self._session = requests.Session()
        self._setup_session()

    def _setup_session(self) -> None:
        """Set up default headers for HTTP session"""
        self._session.headers.update({
            "Content-Type": "application/json",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Origin": "https://www.ao.link",
            "Referer": "https://www.ao.link/"
        })

    @staticmethod
    def timestamp_to_time(timestamp: int, tz_shift: int = 0) -> datetime.datetime:
        """Convert timestamp to datetime object"""
        return datetime.datetime.fromtimestamp(
            timestamp, 
            tz=datetime.timezone(datetime.timedelta(hours=tz_shift))
        )

    @staticmethod
    def time_to_timestamp(time: datetime.datetime, tz_shift: int = 0) -> int:
        """Convert datetime object to timestamp"""
        time = time.astimezone(datetime.timezone(datetime.timedelta(hours=tz_shift)))
        return int(time.timestamp())

    def get_transaction_details(
        self,
        message_id: str,
        from_process_id: str,
        limit: int = 100,
        cursor: str = "",
        query_type: str = "resulting",
        is_initial_request: bool = True,
    ) -> Optional[Dict]:
        """
        Get transaction details

        Args:
            message_id: Message ID
            from_process_id: Source process ID
            limit: Limit per page, default 100
            cursor: Pagination cursor, default empty string
            query_type: Query type ("resulting"/"linked"), default "resulting"
            is_initial_request: Whether it is the initial request, default True. If True, returns total count

        Returns:
            Dictionary containing transaction list and count
            {
                "list_tx": List[Dict],  # Transaction list
                "total_tx_count": int   # Only returned when is_initial_request=True
            }

        Raises:
            ValueError: When parameter validation fails
            ArweaveQueryError: When query fails
        """
        # Validate query type
        conditions = {
            "resulting": f'tags: [{{name: "Pushed-For", values: [$messageId]}}, {{name: "From-Process", values: [$fromProcessId]}}], ingested_at: {{min: 1696107600}}',
            "linked": f'tags: [{{name: "Pushed-For", values: [$messageId]}}, {{name: "From-Process", values: [$fromProcessId]}}], ingested_at: {{min: 1696107600}}',
        }

        if query_type not in conditions:
            raise ValueError(f"Unsupported query_type: {query_type}")
        
        condition = conditions[query_type]
        count_field = "count" if is_initial_request else ""

        # Build GraphQL query
        query_template = """
        query ($fromProcessId: String!, $messageId: String!, $limit: Int!, $sortOrder: SortOrder!, $cursor: String) {{
            transactions(
                sort: $sortOrder
                first: $limit
                after: $cursor
                {condition}
            ) {{
                {count_field}
                edges {{
                    cursor
                    node {{
                        id
                        ingested_at
                        recipient
                        block {{
                            timestamp
                            height
                            __typename
                        }}
                        tags {{
                            name
                            value
                            __typename
                        }}
                        data {{
                            size
                            __typename
                        }}
                        owner {{
                            address
                            __typename
                        }}
                        __typename
                    }}
                    __typename
                }}
                __typename
            }}
        }}
        """

        # Build query and variables
        query = query_template.format(condition=condition, count_field=count_field)
        variables = {
            "fromProcessId": from_process_id,
            "messageId": message_id,
            "limit": limit,
            "sortOrder": "INGESTED_AT_DESC",
            "cursor": cursor,
        }

        payload = {"query": query, "variables": variables}
        logger.debug(f"Constructed query: {query}")
        logger.debug(f"Variables: {variables}")

        try:
            # Send request
            response = self._session.post(self.config.graphql_endpoint, json=payload)
            response.raise_for_status()

            # Parse response
            data = response.json()
            logger.debug(f"Received response: {data}")
            
            if "errors" in data:
                raise ArweaveQueryError(f"GraphQL errors: {data['errors']}")
            
            transactions = data["data"]["transactions"]
            result = {"list_tx": transactions["edges"]}
            
            if is_initial_request:
                result["total_tx_count"] = transactions["count"]

            return result

        except requests.RequestException as e:
            logger.error(f"Request failed: {e}")
            raise ArweaveQueryError(f"Request failed: {str(e)}")
        except (KeyError, json.JSONDecodeError) as e:
            logger.error(f"Failed to parse response: {e}")
            raise ArweaveQueryError(f"Failed to parse response: {str(e)}")

    def get_transaction_summary_stats(
        self,
        entity_id: str,
        query_type: str = "sent",
        cursor: str = "",
        limit: int = 100,
        is_initial_request: bool = True,
        min_ingested_at: Optional[int] = None,
        max_ingested_at: Optional[int] = None,
        min_block: Optional[int] = None,
        max_block: Optional[int] = None,
        search_tags_from_process: Optional[str] = None,
        max_retries: int = 3,
        error_log_path: str = "error_cursors.log"
    ) -> Optional[Dict]:
        """
        Retrieve summary statistics of transactions related to the given entity ID.

        Args:
            entity_id: Entity ID for retrieving transaction statistics
            query_type: Query type ("sent"/"received"/"credit"/"debit"/"transfer")
            cursor: Pagination cursor
            limit: Limit per page
            is_initial_request: Whether it is the initial request
            min_ingested_at: Minimum ingested_at timestamp
            max_ingested_at: Maximum ingested_at timestamp
            min_block: Minimum block height
            max_block: Maximum block height

        Returns:
            Dictionary containing transaction list and count
        """
        # Parameter validation
        if min_ingested_at is not None and max_ingested_at is not None:
            if min_ingested_at >= max_ingested_at:
                raise ValueError("min_ingested_at must be less than max_ingested_at")

        if min_block is not None and max_block is not None:
            if min_block >= max_block:
                raise ValueError("min_block must be less than max_block")

        # Define query conditions
        # sent is user's outgoing
        # sent_process is the argame's outgoing
        # received is incoming
        logger.debug(f"search_tags_from_process: {search_tags_from_process}")
        conditions = {
            "sent": f'tags: [{{name: "Data-Protocol", values: ["ao"]}}], owners: [$entityId]',
            "sent_process": f'tags: [{{name: "From-Process", values: [$entityId]}}]',
            "sent_action_catch": f'tags: [{{name: "From-Process", values: [$entityId]}}, {{name: "Action", values: ["Catch"]}}]',
            "received": f"recipients: [$entityId]",
            "received_action_entityCreate": f'tags: [{{name: "Action", values: ["Reality.EntityCreate"]}}], recipients:[$entityId]',
            "received_action_entityUpdatePosition": f'tags: [{{name: "Action", values: ["Reality.EntityUpdatePosition"]}}], recipients:[$entityId]',
            "received_action_chatMessage": f'tags: [{{name: "Action", values: ["ChatMessage"]}}], recipients:[$entityId]',
            "debit": f'tags: [{{name: "Action", values: ["Debit-Notice"]}}], recipients:[$entityId]',
            "credit": f'tags: [{{name: "Action", values: ["Credit-Notice"]}}], recipients:[$entityId]',
            "transfer": f'tags: [{{name: "Action", values: ["Credit-Notice", "Debit-Notice"]}}], recipients:[$entityId]',
            "token_transfers": f'tags: [{{name: "Action", values: ["Credit-Notice", "Debit-Notice"]}}], recipients:[$entityId], ingested_at: {{min: 1696107600}}',
            "token_transfers_from_process": f'tags: [{{name: "Action", values: ["Credit-Notice", "Debit-Notice"]}}, {{name: "From-Process", values: ["{search_tags_from_process}"]}}], recipients:[$entityId]',

        }

        if query_type not in conditions:
            raise ValueError(f"Unsupported query_type: {query_type}")

        # Build filter conditions
        filters = []
        if min_ingested_at is not None or max_ingested_at is not None:
            ingested_filter = "ingested_at: {"
            if min_ingested_at is not None:
                ingested_filter += f"min: {min_ingested_at}"
            if max_ingested_at is not None:
                ingested_filter += f"{',' if min_ingested_at is not None else ''}max: {max_ingested_at}"
            ingested_filter += "}"
            filters.append(ingested_filter)

        if min_block is not None or max_block is not None:
            block_filter = "block: {"
            if min_block is not None:
                block_filter += f"min: {min_block}"
            if max_block is not None:
                block_filter += f"{',' if min_block is not None else ''}max: {max_block}"
            block_filter += "}"
            filters.append(block_filter)

        # Build query
        condition = conditions[query_type]
        logger.debug(f"condition: {condition}")
        filters_str = ", ".join([condition] + filters)
        count_field = "count" if is_initial_request else ""

        query = f"""
        query ($entityId: String!, $limit: Int!, $sortOrder: SortOrder!, $cursor: String) {{
          transactions(
            sort: $sortOrder
            first: $limit
            after: $cursor
            {filters_str}
          ) {{
            {count_field}
            edges {{
              cursor
              node {{
                id
                recipient
                ingested_at
                block {{
                  timestamp
                  height
                }}
                tags {{
                  name
                  value
                }}
                data {{
                  size
                }}
                owner {{
                  address
                }}
              }}
            }}
          }}
        }}
        """

        variables = {
            "entityId": entity_id,
            "limit": limit,
            "sortOrder": "INGESTED_AT_DESC",
            "cursor": cursor,
        }

        payload = {"query": query, "variables": variables}
        
        for attempt in range(max_retries):
            try:
                response = self._session.post(self.config.graphql_endpoint, json=payload)
                response.raise_for_status()

                # Parse response
                data = response.json()
                if "errors" in data:
                    raise ArweaveQueryError(f"GraphQL errors: {data['errors']}")
                
                transactions = data["data"]["transactions"]
                result = {"list_tx": transactions["edges"]}
                
                if is_initial_request:
                    result["total_tx_count"] = transactions["count"]
                    
                return result

            except requests.RequestException as e:
                logger.error(f"Request failed on attempt {attempt + 1}: {e}")
                if attempt < max_retries - 1:
                    time.sleep(1)  # Wait for a while before retrying
                else:
                    logger.error(f"Max retries reached for cursor: {cursor}. Saving to error log.")
                    self._log_error_cursor(cursor, error_log_path)  # Save error cursor
                    return None  # Return None instead of raising exception
            except (KeyError, json.JSONDecodeError) as e:
                logger.error(f"Failed to parse response on attempt {attempt + 1}: {e}")
                if attempt < max_retries - 1:
                    time.sleep(1)  # Wait for a while before retrying
                else:
                    logger.error(f"Max retries reached for cursor: {cursor}. Saving to error log.")
                    self._log_error_cursor(cursor, error_log_path)  # Save error cursor
                    return None  # Return None instead of raising exception

    def _log_error_cursor(self, cursor: str, error_log_path: str) -> None:
        """Save the error cursor to the log file"""
        with open(error_log_path, 'a') as f:
            f.write(f"{cursor}\n")
        logger.info(f"Cursor {cursor} saved to error log.")

    def get_all_transaction_summaries(
        self,
        entity_id: str,
        query_type: str = "sent",
        initial_batch_size: Optional[int] = None,
        max_batch_size: Optional[int] = None,
        max_retries: Optional[int] = None,
        retry_delay: Optional[float] = None,
        batch_sleep_time: Optional[float] = None,
        max_total_transactions: Optional[int] = None,
        min_ingested_at: Optional[int] = None,
        max_ingested_at: Optional[int] = None,
        min_block: Optional[int] = None,
        max_block: Optional[int] = None,
        include_non_final: bool = True,
        search_tags_from_process: Optional[str] = None,
        check_point_path: Optional[str] = None,
        check_point_step: int = 100,
    ) -> List[Dict]:
        """
        Get all transaction summaries for the entity.

        Args:
            entity_id: Entity ID
            query_type: Query type
            initial_batch_size: Initial batch size
            max_batch_size: Maximum batch size
            max_retries: Maximum number of retries
            retry_delay: Retry delay time
            batch_sleep_time: Interval time between batches
            max_total_transactions: Maximum number of transactions to retrieve
            min_ingested_at: Minimum ingested_at timestamp
            max_ingested_at: Maximum ingested_at timestamp
            min_block: Minimum block height
            max_block: Maximum block height
            include_non_final: Whether to include non-final transactions
            check_point_path: Checkpoint file path
            check_point_step: Checkpoint step size
        Returns:
            List of transactions
        """
        # Use configuration values or defaults
        initial_batch_size = initial_batch_size or self.config.initial_batch_size
        max_batch_size = max_batch_size or self.config.max_batch_size
        max_retries = max_retries or self.config.max_retries
        retry_delay = retry_delay or self.config.retry_delay
        batch_sleep_time = batch_sleep_time or self.config.batch_sleep_time

        all_transactions = []
        cursor = ""
        batch_size = initial_batch_size
        actual_max_batch_size = max_batch_size
        seen_ids = defaultdict(int)

        # Get initial response to get total count
        initial_response = self.get_transaction_summary_stats(
            entity_id,
            query_type,
            min_ingested_at=min_ingested_at,
            max_ingested_at=max_ingested_at,
            min_block=min_block,
            max_block=max_block,
            search_tags_from_process=search_tags_from_process,
        )

        if initial_response is None:
            logger.error("Failed to get initial response")
            return []

        total_count = initial_response.get("total_tx_count", 0)
        logger.info(f"Total transactions to fetch: {total_count}")

        # Read checkpoint file to get last cursor
        if check_point_path and os.path.exists(check_point_path):
            with open(check_point_path, 'r') as f:
                all_transactions = json.load(f)
                if all_transactions:
                    cursor = all_transactions[-1]["cursor"]  # Get last cursor
                    logger.info(f"Resuming from cursor: {cursor}")

        with tqdm(
            total=min(total_count, max_total_transactions) if max_total_transactions else total_count,
            desc="Fetching transactions",
        ) as pbar:
            while True:
                if max_total_transactions and len(all_transactions) >= max_total_transactions:
                    break

                # Retry logic
                for attempt in range(max_retries):
                    response = self.get_transaction_summary_stats(
                        entity_id,
                        query_type,
                        cursor=cursor,
                        limit=batch_size,
                        is_initial_request=False,
                        min_ingested_at=min_ingested_at,
                        max_ingested_at=max_ingested_at,
                        min_block=min_block,
                        max_block=max_block,
                        search_tags_from_process=search_tags_from_process,
                    )

                    if response is not None:
                        break

                    if attempt < max_retries - 1:
                        time.sleep(retry_delay)
                else:
                    logger.error(f"Failed to fetch transactions after {max_retries} attempts")
                    break

                new_transactions = response.get("list_tx", [])
                if not new_transactions:
                    break

                # Process transactions
                filtered_count = 0
                duplicate_count = 0
                non_final_count = 0

                unique_new_transactions = []
                for tx in new_transactions:
                    tx_id = tx["node"]["id"]
                    seen_ids[tx_id] += 1

                    if seen_ids[tx_id] > 1:
                        duplicate_count += 1
                        modified_tx = dict(tx)
                        modified_tx["node"] = dict(tx["node"])
                        modified_tx["node"]["original_id"] = tx["node"]["id"]
                        modified_tx["node"]["id"] = f"{seen_ids[tx_id]-1}-{tx_id}"
                        tx = modified_tx

                    if not include_non_final and not self._is_final_tx(tx):
                        non_final_count += 1
                        continue

                    filtered_count += 1
                    unique_new_transactions.append(tx)

                  
                logger.debug(
                    f"Batch stats - Total: {len(new_transactions)}, "
                    f"Filtered: {filtered_count}, "
                    f"Duplicates: {duplicate_count}, "
                    f"Non-final: {non_final_count}"
                )

                all_transactions.extend(unique_new_transactions)
                pbar.update(len(unique_new_transactions))


                # Every check_point_step transactions, save to check_point_path
                if check_point_path and check_point_step > 0 and len(all_transactions) % check_point_step == 0:
                    try:
                        self._save_checkpoint(all_transactions, check_point_path)
                    except Exception as e:
                        logger.error(f"Failed to save checkpoint: {e}")

                if len(unique_new_transactions) == 0:
                    break

                cursor = unique_new_transactions[-1]["cursor"] if unique_new_transactions else None

                # Update batch size
                self._update_batch_size(len(new_transactions), batch_size, actual_max_batch_size)

                time.sleep(batch_sleep_time)

        if max_total_transactions and len(all_transactions) > max_total_transactions:
            all_transactions = all_transactions[:max_total_transactions]

        logger.info(f"Retrieved {len(all_transactions)} transactions in total")
        return all_transactions

    def _save_checkpoint(self, transactions: List[Dict], check_point_path: str) -> None:
        """Save current transactions to checkpoint file"""
        try:
            with open(check_point_path, 'w') as f:
                json.dump(transactions, f)
            logger.debug(f"Checkpoint saved to {check_point_path}")
        except (IOError, OSError) as e:
            logger.error(f"Failed to write to checkpoint file {check_point_path}: {e}")
        except TypeError as e:
            logger.error(f"Failed to serialize transactions to JSON: {e}")
        except Exception as e:
            logger.error(f"Unexpected error while saving checkpoint: {e}")

    @staticmethod
    def _is_final_tx(tx: Dict) -> bool:
        """Check if the transaction is final"""
        if tx["node"]["block"] is not None and isinstance(tx["node"]["block"]["height"], int):
            return True
        return False

    def _update_batch_size(self, new_count: int, current_size: int, max_size: int) -> Tuple[int, int]:
        """Update batch size"""
        if new_count == current_size:
            previous_size = current_size
            current_size = min(current_size * 2, max_size)
            
            if current_size > max_size:
                current_size = previous_size
                max_size = current_size
                logger.debug(f"Actual maximum batch size discovered: {max_size}")
        elif new_count < current_size:
            max_size = new_count
            current_size = max_size
            logger.debug(f"Actual maximum batch size discovered: {max_size}")
            
        return current_size, max_size

    @staticmethod
    def str_to_datetime(time_str: str, tz: datetime.timezone = datetime.timezone.utc) -> datetime.datetime:
        """
        Convert time string to datetime object

        Args:
            time_str: Time string in the format "YYYY-MM-DD HH:MM:SS"
            tz: Timezone, default is UTC

        Returns:
            datetime.datetime: Datetime object with timezone

        Example:
            >>> client = ArweaveClient()
            >>> dt = client.str_to_datetime("2024-12-19 01:06:50")
            >>> print(dt)
            2024-12-19 01:06:50+00:00
        """
        return datetime.datetime.strptime(time_str, "%Y-%m-%d %H:%M:%S").replace(tzinfo=tz)

class DataProcessor:
    """Data processing utility class"""

    @staticmethod   
    def transactions_to_dataframe(
        transactions: List[Dict],
        process_type: str = "rod"
    ) -> pd.DataFrame:
        """
        Convert transaction data to DataFrame

        Args:
            transactions: List of transaction data
            process_type: Processing type ("rod"/"game"/"token")

        Returns:
            Processed DataFrame
        """
        # ... implementation code ...

class DataCollector:
    """Data collection utility class"""

    def __init__(
        self,
        arweave_client: ArweaveClient,
        data_processor: DataProcessor,
        output_folder: Optional[str] = "./data"
    ):
        self.client = arweave_client
        self.processor = data_processor
        self.output_folder = output_folder

    def collect_rod_data(
        self,
        time_range: Tuple[datetime.datetime, datetime.datetime],
        rod_ids: List[str]
    ) -> None:
        """Collect fishing rod data"""
        # ... implementation code ...

    def collect_game_process_data(
        self,
        time_range: Optional[Tuple[datetime.datetime, datetime.datetime]] = None,
        query_type: str = "sent"
    ) -> None:
        """Collect game process data"""
        # ... implementation code ...

# # Usage example:
# def main():
#     # Create configuration
#     config = ArweaveConfig(
#         max_retries=5,
#         batch_sleep_time=2.0
#     )
    
#     # Initialize client
#     client = ArweaveClient(config)
#     processor = DataProcessor()
    
#     # Create data collector
#     collector = DataCollector(
#         arweave_client=client,
#         data_processor=processor,
#         output_folder="./output"
#     )
    
#     # Set time range
#     start_time = datetime.datetime(2024, 9, 2, 12, 0, tzinfo=datetime.timezone.utc)
#     end_time = datetime.datetime(2024, 9, 3, 12, 0, tzinfo=datetime.timezone.utc)
    
#     # Collect data
#     try:
#         collector.collect_rod_data(
#             time_range=(start_time, end_time),
#             rod_ids=[
#                 "Idp5zhRPmtc5YIdsk5gYqz3HXfjQiTG_ftl5OoBs3pY",
#                 # ... other rod_ids
#             ]
#         )
#     except ArweaveQueryError as e:
#         logger.error(f"Data collection failed: {e}")

# if __name__ == "__main__":
#     main()

