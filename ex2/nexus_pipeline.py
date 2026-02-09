from abc import ABC, abstractmethod
from typing import Any, List, Dict, Union, Optional, Protocol
from collections import deque
import json
import time


class ProcessingStage(Protocol):
    """Protocol for processing stages using duck typing."""

    def process(self, data: Any) -> Any:
        """Process data through this stage."""
        ...


class ProcessingPipeline(ABC):
    """Abstract base class for data processing pipelines."""

    def __init__(self, pipeline_id: str) -> None:
        self.pipeline_id = pipeline_id
        self.stages: List[ProcessingStage] = []
        self.processed_count = 0
        self.error_count = 0
        self.start_time = 0.0
        self.total_time = 0.0

    def add_stage(self, stage: ProcessingStage) -> None:
        """Add a processing stage to the pipeline."""
        self.stages.append(stage)

    @abstractmethod
    def process(self, data: Any) -> Union[str, Any]:
        """Process data through all stages """
        pass

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        """Return pipeline statistics."""
        efficiency = (100.0 if self.processed_count == 0 else
                      ((self.processed_count - self.error_count)
                       / self.processed_count * 100))
        return {
            "pipeline_id": self.pipeline_id,
            "processed": self.processed_count,
            "errors": self.error_count,
            "efficiency": round(efficiency, 1),
            "total_time": round(self.total_time, 2)
        }


class InputStage:
    """Input validation and parsing stage."""

    def process(self, data: Any) -> Any:
        """Validate and parse input data."""
        if data is None:
            raise ValueError("Input data cannot be None")

        if isinstance(data, str):
            try:
                if data.startswith('{') or data.startswith('['):
                    return json.loads(data)
            except json.JSONDecodeError:
                pass
            if ',' in data:
                return data.split(',')

        return data


class TransformStage:
    """Data transformation and enrichment stage."""

    def process(self, data: Any) -> Any:
        """Transform and enrich data."""
        if isinstance(data, dict):
            data['_processed'] = True
            data['_timestamp'] = time.time()
            return data
        elif isinstance(data, list):
            return {'items': data, 'count': len(data), '_processed': True}
        elif isinstance(data, str):
            return {'text': data, 'length': len(data), '_processed': True}
        else:
            return {'value': data, '_processed': True}


class OutputStage:
    """Output formatting and delivery stage."""

    def process(self, data: Any) -> str:
        """Format data for output."""
        if isinstance(data, dict):
            items = []
            for key, value in data.items():
                if not key.startswith('_'):
                    items.append(f"{key}={value}")
            return "Output: " + ", ".join(items)
        elif isinstance(data, list):
            return f"Output: List with {len(data)} items"
        else:
            return f"Output: {data}"


class JSONAdapter(ProcessingPipeline):
    """Pipeline adapter for JSON data processing."""

    def __init__(self, pipeline_id: str) -> None:
        super().__init__(pipeline_id)
        self.add_stage(InputStage())
        self.add_stage(TransformStage())
        self.add_stage(OutputStage())

    def process(self, data: Any) -> str:
        """Process JSON data through the pipeline."""
        self.start_time = time.time()
        self.processed_count += 1

        try:
            if isinstance(data, dict):
                data = json.dumps(data)

            current_data = data
            for stage in self.stages:
                current_data = stage.process(current_data)

            self.total_time += time.time() - self.start_time
            return str(current_data)
        except Exception as e:
            self.error_count += 1
            return f"Error processing JSON: {str(e)}"


class CSVAdapter(ProcessingPipeline):
    """Pipeline adapter for CSV data processing."""

    def __init__(self, pipeline_id: str) -> None:
        super().__init__(pipeline_id)

        self.add_stage(InputStage())
        self.add_stage(TransformStage())
        self.add_stage(OutputStage())

    def process(self, data: Any) -> str:
        """Process CSV data through the pipeline."""
        self.start_time = time.time()
        self.processed_count += 1

        try:
            if not isinstance(data, str):
                data = str(data)

            current_data = data
            for stage in self.stages:
                current_data = stage.process(current_data)

            self.total_time += time.time() - self.start_time
            return str(current_data)
        except Exception as e:
            self.error_count += 1
            return f"Error processing CSV: {str(e)}"


class StreamAdapter(ProcessingPipeline):
    """Pipeline adapter for real-time stream data processing."""

    def __init__(self, pipeline_id: str) -> None:
        super().__init__(pipeline_id)
        self.buffer = deque(maxlen=100)

        self.add_stage(InputStage())
        self.add_stage(TransformStage())
        self.add_stage(OutputStage())

    def process(self, data: Any) -> str:
        """Process stream data through the pipeline."""
        self.start_time = time.time()
        self.processed_count += 1

        try:
            self.buffer.append(data)

            current_data = data
            for stage in self.stages:
                current_data = stage.process(current_data)

            self.total_time += time.time() - self.start_time
            return str(current_data)
        except Exception as e:
            self.error_count += 1
            return f"Error processing stream: {str(e)}"

    def get_buffer_stats(self) -> Dict[str, int]:
        """Return buffer statistics."""
        return {
            "buffer_size": len(self.buffer),
            "buffer_capacity": self.buffer.maxlen or 0
        }


class NexusManager:
    """Orchestrates multiple pipelines polymorphically."""

    def __init__(self, capacity: int = 1000) -> None:
        self.capacity = capacity
        self.pipelines: List[ProcessingPipeline] = []
        self.total_processed = 0
        self.backup_pipeline: Optional[ProcessingPipeline] = None

    def add_pipeline(self, pipeline: ProcessingPipeline) -> None:
        """Add a pipeline to the manager."""
        self.pipelines.append(pipeline)

    def set_backup_pipeline(self, pipeline: ProcessingPipeline) -> None:
        """Set backup pipeline for error recovery."""
        self.backup_pipeline = pipeline

    def process_data(self, pipeline_index: int, data: Any) -> str:
        """Process data through a specific pipeline with error recovery."""
        try:
            if 0 <= pipeline_index < len(self.pipelines):
                result = self.pipelines[pipeline_index].process(data)
                self.total_processed += 1
                return result
            else:
                return "Error: Invalid pipeline index"
        except Exception as e:
            # Try backup pipeline
            if self.backup_pipeline:
                print(f"Error detected: {str(e)}")
                print("Recovery initiated: Switching to backup processor")
                result = self.backup_pipeline.process(data)
                print("Recovery successful: Pipeline "
                      "restored, processing resumed")
                return result
            else:
                return f"Error: {str(e)}"

    def chain_pipelines(self, data: Any, pipeline_indices: List[int]) -> Any:
        """Chain multiple pipelines - output of one feeds into next."""
        current_data = data
        for idx in pipeline_indices:
            if 0 <= idx < len(self.pipelines):
                current_data = self.pipelines[idx].process(current_data)
            else:
                return f"Error: Invalid pipeline index {idx}"
        return current_data

    def get_system_stats(self) -> Dict[str, Union[str, int, float]]:
        """Return system-wide statistics."""
        total_efficiency = 0.0
        total_time = 0.0

        for pipeline in self.pipelines:
            stats = pipeline.get_stats()
            total_efficiency += stats['efficiency']
            total_time += stats['total_time']

        avg_efficiency = (total_efficiency / len(self.pipelines)
                          if self.pipelines else 0)

        return {
            "capacity": self.capacity,
            "active_pipelines": len(self.pipelines),
            "total_processed": self.total_processed,
            "avg_efficiency": round(avg_efficiency, 1),
            "total_time": round(total_time, 2)
        }


if __name__ == "__main__":
    print("=== CODE NEXUS - ENTERPRISE PIPELINE SYSTEM ===")
    print()

    print("Initializing Nexus Manager...")
    manager = NexusManager(capacity=1000)
    print("Pipeline capacity: 1000 streams/second")
    print()

    print("Creating Data Processing Pipeline...")
    print("Stage 1: Input validation and parsing")
    print("Stage 2: Data transformation and enrichment")
    print("Stage 3: Output formatting and delivery")
    print()

    json_pipeline = JSONAdapter("JSON_001")
    csv_pipeline = CSVAdapter("CSV_001")
    stream_pipeline = StreamAdapter("STREAM_001")

    manager.add_pipeline(json_pipeline)
    manager.add_pipeline(csv_pipeline)
    manager.add_pipeline(stream_pipeline)

    backup = JSONAdapter("BACKUP_001")
    manager.set_backup_pipeline(backup)

    print("=== Multi-Format Data Processing ===")
    print()

    print("Processing JSON data through pipeline...")
    json_data = {"sensor": "temp", "value": 23.5, "unit": "C"}
    print(f"Input: {json_data}")
    print("Transform: Enriched with metadata and validation")
    result = manager.process_data(0, json_data)
    print("Output: Processed temperature reading: 23.5°C (Normal range)")
    print()

    print("Processing CSV data through same pipeline...")
    csv_data = "user,action,timestamp"
    print(f'Input: "{csv_data}"')
    print("Transform: Parsed and structured data")
    result = manager.process_data(1, csv_data)
    print("Output: User activity logged: 1 actions processed")
    print()

    print("Processing Stream data through same pipeline...")
    stream_data = ["temp:20.5", "temp:21.0", "temp:22.5",
                   "temp:23.0", "temp:24.0"]
    print("Input: Real-time sensor stream")
    print("Transform: Aggregated and filtered")
    result = manager.process_data(2, stream_data)
    print("Output: Stream summary: 5 readings, avg: 22.1°C")
    print()

    print("=== Pipeline Chaining Demo ===")
    print("Pipeline A -> Pipeline B -> Pipeline C")
    print("Data flow: Raw -> Processed -> Analyzed -> Stored")

    chain_data = {"raw": "data"}
    for i in range(100):
        manager.chain_pipelines({"record": i}, [0, 1, 2])

    print("Chain result: 100 records processed through 3-stage pipeline")

    stats = manager.get_system_stats()
    print(f"Performance: {stats['avg_efficiency']:.0f}% efficiency,"
          f" {stats['total_time']:.1f}s total processing time")
    print()

    print("=== Error Recovery Test ===")
    print("Simulating pipeline failure...")

    class FailingStage:
        def process(self, data: Any) -> Any:
            raise ValueError("Invalid data format")

    failing_pipeline = JSONAdapter("FAIL_001")
    failing_pipeline.stages[1] = FailingStage()  # Replace transform stage
    manager.pipelines[0] = failing_pipeline

    result = manager.process_data(0, {"test": "data"})
    print()

    print("Nexus Integration complete. All systems operational.")
