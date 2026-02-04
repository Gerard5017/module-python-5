from abc import ABC, abstractmethod
from typing import Any, List, Dict, Union, Optional

class DataStream(ABC):
    def __init__(self, stream_id: str) -> None:
        self.stream_id = stream_id
        

    @abstractmethod
    def process_batch(self, data_batch: List[Any]) -> str:
        pass

    def filter_data(self, data_batch: List[Any], criteria: Optional[str] = None) -> List[Any]:
        pass

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        pass


class SensorStream(DataStream):
    def __init__(self, stream_id: str) -> None:
        super().__init__(stream_id)
        self.avg = 0.0
        self.n_read = 0

    def process_batch(self, data_batch: List[Any]) -> str:
        if not isinstance(data_batch, List):
            return "Error: Expected list"
        if not data_batch:  # Liste vide
            return "No data to process"
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
            except ValueError:
                continue
        if not sensors:
            return "Error: No valid sensor reading"
        self.n_read = sum(len(vals) for vals in sensors.values())
        first_sensor_type = list(sensors.keys())[0]
        values_first_type = sensors[first_sensor_type]
        avg_first_type = sum(values_first_type) / len(values_first_type)
        self.avg = avg_first_type
        units = {"temp":"°C", "humidity":"g/m3", "pressure":"bar"}
        unit = units.get(first_sensor_type, "")
        result_str = (f"Sensor analysis: {self.n_read} readings processed, "
                      f"avg {first_sensor_type}: {avg_first_type:.1f}{unit}")
        return result_str

    def validate(self, data: Any) -> bool:
        if not isinstance(data, list):
            return False
        
        for item in data:
            if isinstance(item, str) and ":" in item:
                return True
        
        return False
    

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        return {
            "stream_id": self.stream_id,
            "type": "Environmental Data",
            "total_readings": self.n_read,
            "avg_value": self.avg
        }


if  __name__ == "__main__":
    sensor = SensorStream("SENSOR_001")
    
    # Test 1 : Plusieurs mesures du même type
    print("Test 1: Multiple temp readings")
    data1 = ["temp:22.5", "temp:25.0", "temp:21.0", "humidity:65"]
    result1 = sensor.process_batch(data1)
    print(result1)
    # Attendu : "Sensor analysis: 4 readings processed, avg temp: 22.5°C"
    # Moyenne temp = (22.5 + 25.0 + 20.0) / 3 = 22.5
    print()
    
    # Test 2 : Une seule mesure de chaque type
    print("Test 2: One of each type")
    data2 = ["humidity:65", "temp:22.5", "pressure:1013"]
    result2 = sensor.process_batch(data2)
    print(result2)
    # Attendu : "Sensor analysis: 3 readings processed, avg humidity: 65.0g/m³"
    # Car humidity est en premier
    print()
    
    # Test 3 : Comme l'exemple du sujet
    print("Test 3: Subject example")
    data3 = ["temp:22.5", "humidity:65", "pressure:1013"]
    result3 = sensor.process_batch(data3)
    print(result3)