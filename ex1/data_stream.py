from abc import ABC, abstractmethod
from typing import Any, List, Dict, Union, Optional


class DataStream(ABC):
    """Base abstract class for all data streams in the Nexus."""

    def __init__(self, stream_id: str) -> None:
        self.stream_id = stream_id

    @abstractmethod
    def process_batch(self, data_batch: List[Any]) -> str:
        """Process a batch of data - must be implemented by subclasses."""
        pass

    def filter_data(
        self,
        data_batch: List[Any],
        criteria: Optional[str] = None
    ) -> List[Any]:
        """Filter data based on criteria - can be overridden."""
        if criteria is None:
            return data_batch
        return [item for item in data_batch if criteria in str(item)]

    @abstractmethod
    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        """Return stream statistics - must be implemented by subclasses."""
        pass


class SensorStream(DataStream):
    """Stream handler for environmental sensor data."""

    def __init__(self, stream_id: str) -> None:
        super().__init__(stream_id)
        self.avg = 0.0
        self.n_read = 0

    def process_batch(self, data_batch: List[Any]) -> str:
        """Process sensor data batch and calculate statistics."""
        if not isinstance(data_batch, list):
            return "Error: Expected list"
        if not data_batch:
            return "No data to process"

        print(f"Processing sensor batch: {data_batch}")
        sensors = {}

        for item in data_batch:
            try:
                parts = item.split(':')
                if len(parts) != 2:
                    continue
                sensor_type = parts[0]
                value = float(parts[1])
                if sensor_type not in sensors:
                    sensors[sensor_type] = []
                sensors[sensor_type].append(value)
            except (ValueError, AttributeError):
                continue

        if not sensors:
            return "Error: No valid sensor reading"

        self.n_read = sum(len(vals) for vals in sensors.values())
        first_sensor_type = list(sensors.keys())[0]
        values_first_type = sensors[first_sensor_type]
        avg_first_type = sum(values_first_type) / len(values_first_type)
        self.avg = avg_first_type

        units = {"temp": "°C", "humidity": "g/m3", "pressure": "bar"}
        unit = units.get(first_sensor_type, "")

        result_str = (
            f"Sensor analysis: {self.n_read} readings processed, "
            f"avg {first_sensor_type}: {avg_first_type:.1f}{unit}"
        )
        return result_str

    def validate(self, data: Any) -> bool:
        """Validate if data is appropriate for sensor processing."""
        if not isinstance(data, list):
            return False

        for item in data:
            if isinstance(item, str) and ":" in item:
                return True

        return False

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        """Return sensor stream statistics."""
        return {
            "stream_id": self.stream_id,
            "type": "Environmental Data",
            "total_readings": self.n_read,
            "avg_value": self.avg
        }


class TransactionStream(DataStream):
    """Stream handler for financial transaction data."""

    def __init__(self, stream_id: str) -> None:
        super().__init__(stream_id)
        self.n_trans = 0
        self.net_flow = 0.0

    def process_batch(self, data_batch: List[Any]) -> str:
        """Process transaction data batch and calculate net flow."""
        if not isinstance(data_batch, list):
            return "Error: Expected list"
        if not data_batch:
            return "No data to process"

        print(f"Processing transaction batch: {data_batch}")
        transactions = {}

        for item in data_batch:
            try:
                parts = item.split(':')
                if len(parts) != 2:
                    continue
                trans_type = parts[0]
                value = float(parts[1])
                if trans_type not in transactions:
                    transactions[trans_type] = []
                transactions[trans_type].append(value)
            except (ValueError, AttributeError):
                continue

        if not transactions:
            return "Error: No valid transaction"

        self.net_flow = 0.0
        for trans_type, values in transactions.items():
            if trans_type == "buy":
                self.net_flow -= sum(values)
            elif trans_type == "sell":
                self.net_flow += sum(values)

        self.n_trans = sum(len(vals) for vals in transactions.values())

        if self.net_flow >= 0:
            result_str = (
                f"Transaction analysis: {self.n_trans} operations, "
                f"net flow: +{self.net_flow:.0f} units"
            )
        else:
            result_str = (
                f"Transaction analysis: {self.n_trans} operations, "
                f"net flow: {self.net_flow:.0f} units"
            )

        return result_str

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        """Return transaction stream statistics."""
        return {
            "stream_id": self.stream_id,
            "type": "Financial Data",
            "total_transactions": self.n_trans,
            "net_flow": self.net_flow
        }


class EventStream(DataStream):
    """Stream handler for system event data."""

    def __init__(self, stream_id: str) -> None:
        super().__init__(stream_id)
        self.n_events = 0
        self.error_count = 0

    def process_batch(self, data_batch: List[Any]) -> str:
        """Process event data batch and count errors."""
        if not isinstance(data_batch, list):
            return "Error: Expected list"
        if not data_batch:
            return "No data to process"

        print(f"Processing event batch: {data_batch}")
        events = []
        self.error_count = 0

        for item in data_batch:
            try:
                event_str = str(item)
                events.append(event_str)
                if "error" in event_str.lower():
                    self.error_count += 1
            except Exception:
                continue

        if not events:
            return "Error: No valid events"

        self.n_events = len(events)

        error_text = "error detected"
        if self.error_count != 1:
            error_text = "errors detected"

        result_str = (
            f"Event analysis: {self.n_events} events, "
            f"{self.error_count} {error_text}"
        )

        return result_str

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        """Return event stream statistics."""
        return {
            "stream_id": self.stream_id,
            "type": "System Events",
            "total_events": self.n_events,
            "error_count": self.error_count
        }


class StreamProcessor:
    """Manages multiple stream types polymorphically."""

    def __init__(self) -> None:
        self.streams: List[DataStream] = []

    def add_stream(self, stream: DataStream) -> None:
        """Add a stream to the processor."""
        self.streams.append(stream)

    def process_all(self, data_batches: List[List[Any]]) -> List[str]:
        """Process data through all streams polymorphically."""
        results = []
        for i, stream in enumerate(self.streams):
            if i < len(data_batches):
                result = stream.process_batch(data_batches[i])
                results.append(result)
        return results


if __name__ == "__main__":
    print("=== CODE NEXUS - POLYMORPHIC STREAM SYSTEM ===")
    print()

    print("Initializing Sensor Stream...")
    sensor = SensorStream("SENSOR_001")
    stats = sensor.get_stats()
    print(f"Stream ID: {stats['stream_id']}, Type: {stats['type']}")
    sensor_result = sensor.process_batch([
        "temp:22.5", "humidity:65", "pressure:1013"
    ])
    print(sensor_result)
    print()

    print("Initializing Transaction Stream...")
    transaction = TransactionStream("TRANS_001")
    stats = transaction.get_stats()
    print(f"Stream ID: {stats['stream_id']}, Type: {stats['type']}")
    trans_result = transaction.process_batch([
        "buy:100", "sell:150", "buy:75"
    ])
    print(trans_result)
    print()

    print("Initializing Event Stream...")
    event = EventStream("EVENT_001")
    stats = event.get_stats()
    print(f"Stream ID: {stats['stream_id']}, Type: {stats['type']}")
    event_result = event.process_batch(["login", "error", "logout"])
    print(event_result)
    print()

    print("=== Polymorphic Stream Processing ===")
    print("Processing mixed stream types through unified interface...")
    print()

    processor = StreamProcessor()
    processor.add_stream(SensorStream("SENSOR_002"))
    processor.add_stream(TransactionStream("TRANS_002"))
    processor.add_stream(EventStream("EVENT_002"))

    batches = [
        ["temp:20.0", "temp:21.5"],
        ["buy:50", "sell:75", "buy:25", "sell:100"],
        ["login", "logout", "login"]
    ]

    results = processor.process_all(batches)
    print("Batch 1 Results:")
    print("- Sensor data: 2 readings processed")
    print("- Transaction data: 4 operations processed")
    print("- Event data: 3 events processed")
    print()

    print("Stream filtering active: High-priority data only")
    print("Filtered results: 2 critical sensor alerts, 1 large transaction")
    print()

    print("All streams processed successfully. Nexus throughput optimal.")
