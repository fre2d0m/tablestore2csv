"""Core TableStore exporter with multi-threading support."""

import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, Any, List, Optional, Tuple
from dataclasses import dataclass

from tablestore import OTSClient, Direction, OTSClientError, OTSServiceError
from tqdm import tqdm

from config.manager import ConfigManager
from tasks.loader import TaskLoader, TaskDefinition
from filters.engine import FilterEngine
from filters.time_chunker import TimeChunk
from progress.manager import ProgressManager, Progress
from exporter.query_builder import QueryBuilder
from exporter.writer import CSVWriter
from utils.logger import get_logger

logger = get_logger(__name__)


@dataclass
class TaskResult:
    """Result of a single task export."""
    task_id: str
    success: bool
    rows_exported: int
    read_cu: int = 0  # Total read capacity units consumed
    error_message: Optional[str] = None
    duration: float = 0.0


class TableStoreExporter:
    """Main exporter for TableStore data."""
    
    def __init__(
        self,
        config_manager: ConfigManager,
        max_workers: int = 4,
        batch_size: int = 5000
    ):
        """
        Initialize exporter.
        
        Args:
            config_manager: Configuration manager
            max_workers: Number of concurrent worker threads
            batch_size: Number of rows per batch write
        """
        self.config = config_manager
        self.max_workers = max_workers
        self.batch_size = batch_size
        
        # Create main client
        self.client = self._create_client()
        
        # Initialize components
        self.filter_engine = FilterEngine(self.config.get_schema())
        self.query_builder = QueryBuilder(
            self.config.get_schema(),
            self.config.get_append_columns()
        )
        self.writer = CSVWriter(self.config.get_output_directory())
        self.progress_manager = ProgressManager()
        
        # Load tasks
        task_loader = TaskLoader()
        self.tasks = task_loader.load(self.config.get_tasks_config())
        
        logger.info(f"Loaded {len(self.tasks)} tasks")
        logger.info(f"Max workers: {max_workers}")
        logger.info(f"Batch size: {batch_size}")
    
    def _create_client(self) -> OTSClient:
        """Create TableStore client."""
        conn = self.config.connection_config
        return OTSClient(
            conn.endpoint,
            conn.access_key_id,
            conn.access_key_secret,
            conn.instance_name
        )
    
    def export_all_tasks(self, resume: bool = False) -> Dict[str, Any]:
        """
        Export all tasks with multi-threading.
        
        Args:
            resume: Resume from checkpoint
            
        Returns:
            Export summary
        """
        start_time = time.time()
        
        # Load or create progress
        progress = None
        if resume:
            progress = self.progress_manager.load_progress()
            if progress:
                # Validate config hash
                if not self.progress_manager.validate_config_hash(
                    progress,
                    self.config.export_config.to_dict()
                ):
                    logger.warning("Configuration changed, ignoring previous progress")
                    progress = None
        
        if not progress:
            progress = self.progress_manager.create_new_progress(
                self.config.export_config.to_dict(),
                len(self.tasks)
            )
        
        # Get pending tasks
        all_task_ids = list(self.tasks.keys())
        pending_task_ids = self.progress_manager.get_pending_tasks(all_task_ids, progress)
        
        logger.info(f"Total tasks: {len(self.tasks)}")
        logger.info(f"Completed: {len(progress.completed_tasks)}")
        logger.info(f"Pending: {len(pending_task_ids)}")
        logger.info(f"Workers: {self.max_workers}")
        
        if not pending_task_ids:
            logger.info("All tasks already completed!")
            return self.progress_manager.get_progress_summary(progress)
        
        logger.info("Starting export with progress tracking...")
        logger.info(f"Note: You will see '→ Starting task: <id>' messages as tasks begin")
        logger.info("=" * 80)
        
        # Export with thread pool and progress bar
        results = []
        failed_count = 0
        total_rows_exported = 0
        total_read_cu = 0
        start_time_for_cu = time.time()
        
        # Create progress bar with more visible updates
        progress_bar = tqdm(
            total=len(self.tasks),
            initial=len(progress.completed_tasks),
            desc="Exporting",
            unit=" tasks",
            bar_format='{desc}: {percentage:3.0f}%|{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}]',
            ncols=130,
            smoothing=0,
            mininterval=0.05
        )
        
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # Submit all pending tasks
            future_to_task_id = {}
            
            tqdm.write(f"Submitting {len(pending_task_ids)} tasks to {self.max_workers} workers...")
            
            for idx, task_id in enumerate(pending_task_ids):
                task_def = self.tasks[task_id]
                future = executor.submit(self._export_single_task, task_id, task_def)
                future_to_task_id[future] = task_id
                
                # Show submission progress for first few tasks
                if idx < 5:
                    tqdm.write(f"  Submitted: {task_id}")
            
            if len(pending_task_ids) > 5:
                tqdm.write(f"  ... and {len(pending_task_ids) - 5} more tasks")
            
            tqdm.write("")  # Blank line
            tqdm.write("Tasks are now running in parallel. Watch for '→ Starting task' messages below:")
            tqdm.write("")
            
            # Process completed tasks
            completed_count = len(progress.completed_tasks)
            for future in as_completed(future_to_task_id):
                task_id = future_to_task_id[future]
                
                try:
                    result = future.result()
                    results.append(result)
                    
                    completed_count += 1
                    
                    if result.success:
                        total_rows_exported += result.rows_exported
                        total_read_cu += result.read_cu
                        
                        # Calculate CU/s rate
                        elapsed = time.time() - start_time_for_cu
                        cu_per_sec = total_read_cu / elapsed if elapsed > 0 else 0
                        
                        # Update progress bar - do update before set_postfix
                        progress_bar.update(1)
                        
                        # Update progress bar postfix with detailed stats
                        progress_bar.set_postfix({
                            'current': task_id[:10],
                            'rows': f'{total_rows_exported:,}',
                            'CU': f'{total_read_cu:,}',
                            'CU/s': f'{cu_per_sec:.1f}'
                        })
                        progress_bar.refresh()
                        
                        self.progress_manager.mark_task_completed(
                            progress,
                            result.task_id,
                            result.rows_exported
                        )
                    else:
                        # Update progress bar - do update before set_postfix
                        progress_bar.update(1)
                        
                        progress_bar.set_postfix({
                            'current': task_id[:10],
                            'status': '✗ FAILED'
                        })
                        progress_bar.refresh()
                        
                        self.progress_manager.mark_task_failed(
                            progress,
                            result.task_id,
                            result.error_message or "Unknown error"
                        )
                        failed_count += 1
                        
                        # Log error details
                        tqdm.write(f"✗ Task {task_id} failed: {result.error_message}")
                    
                    # Save progress periodically
                    if completed_count % 10 == 0:
                        self.progress_manager.save_progress(progress)
                
                except Exception as e:
                    # Update progress bar first
                    progress_bar.update(1)
                    progress_bar.refresh()
                    tqdm.write(f"✗ Unexpected error for task {task_id}: {e}")
                    failed_count += 1
        
        # Close progress bar
        progress_bar.close()
        
        # Final progress save
        self.progress_manager.save_progress(progress)
        
        # Summary
        duration = time.time() - start_time
        summary = self.progress_manager.get_progress_summary(progress)
        summary['duration'] = duration
        summary['failed_count'] = failed_count
        summary['total_read_cu'] = total_read_cu
        summary['avg_cu_per_sec'] = total_read_cu / duration if duration > 0 else 0
        
        logger.info("=" * 80)
        logger.info("Export Summary")
        logger.info("=" * 80)
        logger.info(f"Total tasks: {summary['total_tasks']}")
        logger.info(f"Completed: {summary['completed']}")
        logger.info(f"Failed: {summary['failed']}")
        logger.info(f"Total rows: {summary['total_rows_exported']:,}")
        logger.info(f"Total Read CU: {total_read_cu:,}")
        logger.info(f"Average CU/s: {summary['avg_cu_per_sec']:.2f}")
        logger.info(f"Duration: {duration:.2f}s")
        logger.info("=" * 80)
        
        return summary
    
    def _export_single_task(self, task_id: str, task_def: TaskDefinition) -> TaskResult:
        """
        Export a single task (called by thread pool).
        
        Args:
            task_id: Task ID (partition key value)
            task_def: Task definition
            
        Returns:
            TaskResult
        """
        start_time = time.time()
        
        # Log task start immediately
        tqdm.write(f"→ Starting task: {task_id}")
        
        try:
            # Create dedicated client for this thread
            client = self._create_client()
            
            # Parse filters
            filters_config = {**self.config.get_filters(), **task_def.filters}
            regular_filters, time_range_filter = self.filter_engine.parse_filters(filters_config)
            
            if not time_range_filter:
                return TaskResult(
                    task_id=task_id,
                    success=False,
                    rows_exported=0,
                    read_cu=0,
                    error_message="No time range filter defined"
                )
            
            # Get time chunks
            time_chunks = self.filter_engine.get_time_chunks(time_range_filter)
            
            # Separate PK and attribute filters
            pk_filters, attr_filters = self.filter_engine.separate_pk_and_attr_filters(regular_filters)
            
            # Export each time chunk
            total_rows = 0
            total_read_cu = 0
            
            tqdm.write(f"  {task_id}: Processing {len(time_chunks)} year(s)...")
            
            for chunk in time_chunks:
                tqdm.write(f"  {task_id}: Querying year {chunk.year}...")
                
                rows_in_chunk, cu_in_chunk = self._export_time_chunk(
                    client,
                    task_id,
                    task_def,
                    pk_filters,
                    attr_filters,
                    chunk
                )
                total_rows += rows_in_chunk
                total_read_cu += cu_in_chunk
                
                if rows_in_chunk > 0:
                    tqdm.write(f"  {task_id}: Year {chunk.year} - {rows_in_chunk:,} rows, {cu_in_chunk} CU")
            
            duration = time.time() - start_time
            
            # Log completion
            tqdm.write(f"✓ Completed task: {task_id} - {total_rows:,} rows, {total_read_cu} CU, {duration:.1f}s")
            
            return TaskResult(
                task_id=task_id,
                success=True,
                rows_exported=total_rows,
                read_cu=total_read_cu,
                duration=duration
            )
        
        except Exception as e:
            logger.error(f"Error exporting task {task_id}: {e}")
            import traceback
            traceback.print_exc()
            
            return TaskResult(
                task_id=task_id,
                success=False,
                rows_exported=0,
                read_cu=0,
                error_message=str(e),
                duration=time.time() - start_time
            )
    
    def _export_time_chunk(
        self,
        client: OTSClient,
        task_id: str,
        task_def: TaskDefinition,
        pk_filters: List,
        attr_filters: List,
        time_chunk: TimeChunk
    ) -> Tuple[int, int]:
        """
        Export data for a single time chunk.
        
        Args:
            client: OTS client
            task_id: Task ID
            task_def: Task definition
            pk_filters: Primary key filters
            attr_filters: Attribute filters
            time_chunk: Time chunk to export
            
        Returns:
            Number of rows exported
        """
        # Build primary key range
        inclusive_start, exclusive_end = self.query_builder.build_primary_key_range(
            task_id,
            pk_filters,
            time_chunk
        )
        
        # Generate output filename
        filename = self.config.get_output_filename(task_id, time_chunk.year)
        
        # Build headers
        headers = self.query_builder.build_csv_headers(task_def.columns)
        
        # Query and write data
        total_rows = 0
        total_read_cu = 0
        batch_rows = []
        
        # Determine write mode (append if file exists)
        write_mode = 'a' if self.writer.file_exists(filename) else 'w'
        
        try:
            while True:
                # Query TableStore
                consumed, next_start_primary_key, row_list, next_token = client.get_range(
                    self.config.get_table_name(),
                    Direction.FORWARD,
                    inclusive_start,
                    exclusive_end,
                    columns_to_get=[],
                    limit=5000,
                    max_version=1
                )
                
                # Accumulate consumed read CU
                if consumed and hasattr(consumed, 'read'):
                    total_read_cu += consumed.read
                elif consumed and hasattr(consumed, 'capacity_unit'):
                    total_read_cu += consumed.capacity_unit.read
                
                # Process rows
                for row in row_list:
                    # Extract row data
                    row_data = self.query_builder.extract_row_data(row, task_def.columns)
                    
                    # Apply attribute filters
                    if attr_filters:
                        # Build dict for filtering
                        row_dict = dict(zip(headers, row_data))
                        if not self.filter_engine.apply_attribute_filters(row_dict, attr_filters):
                            continue
                    
                    batch_rows.append(row_data)
                    total_rows += 1
                    
                    # Write batch if reached threshold
                    if len(batch_rows) >= self.batch_size:
                        self.writer.write_batch(filename, headers, batch_rows, mode=write_mode)
                        batch_rows = []
                        write_mode = 'a'  # Switch to append mode after first write
                
                # Check if done
                if next_start_primary_key is None:
                    break
                
                # Continue with next page
                inclusive_start = next_start_primary_key
        
        except (OTSClientError, OTSServiceError) as e:
            logger.error(f"TableStore error for {task_id} chunk {time_chunk.year}: {e}")
            raise
        
        # Write remaining rows
        if batch_rows:
            self.writer.write_batch(filename, headers, batch_rows, mode=write_mode)
        
        # Only log if we exported data
        if total_rows > 0:
            logger.debug(f"Exported {total_rows} rows for {task_id} year {time_chunk.year} (Read CU: {total_read_cu})")
        else:
            logger.debug(f"No data found for {task_id} year {time_chunk.year}, skipping file creation")
        
        return total_rows, total_read_cu

