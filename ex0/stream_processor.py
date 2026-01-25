from abc import ABC, abstractmethod
from typing import Any, List, Dict, Union, Optional

class DataProcessor(ABC):
    @abstractmethod
    def  process(self, data: Any) -> str:
        pass

    @abstractmethod
    def validate(self, data: Any) -> bool:
        pass

    def format_output(self, result: str) -> str:
        return(f"Output: {result}")


class NumericProcessor(DataProcessor):
    def  process(self, data: List[int]) -> str:
        print(f"Processing data: {data}")
        if not self.validate(data):
            result_str = "Error was found"
            return self.format_output(result_str)
        print("Validation: Numeric data verified")
        sum_data = sum(data)
        len_data = len(data)
        average = sum_data / len_data
        result_str = f"Processed {len_data} numeric values, sum={sum_data}, avg={average}"
        return self.format_output(result_str)


    def validate(self, data: List[int]) -> bool:
        try:
            if not isinstance(data, list):
                raise ValueError
        except ValueError:
            print("Validation: Data is not a List")
            return False
        
        if len(data) == 0:
            print("Validation: Data is empty")
            return False
        
        for i in data:
            try:
                int(i)
            except (ValueError,  TypeError):
                print("Validation: Data is not numeric")
                return False
        return True
    

class TextProcessor(DataProcessor):
    def  process(self, data: str) -> str:
        print(f"Processing data: {data}")
        if not self.validate(data):
            result_str = "Error was found"
            return self.format_output(result_str)
        print("Validation: Text data verified")
        word_data = len(data.split())
        char_data = len(data)
        result_str = f"Processed text: {char_data} characters, {word_data} words"
        return self.format_output(result_str)


    def validate(self, data: str) -> bool:
        try:
            if not isinstance(data, str):
                raise ValueError
        except ValueError:
            print("Validation: Data is not a string")
            return False
        
        return True
    

class LogProcessor(DataProcessor):
    def  process(self, data: str) -> str:
        print(f"Processing data: {data}")
        if not self.validate(data):
            result_str = "Error was found"
            return self.format_output(result_str)
        print("Validation: Log entry verified")
        alert = data.split(":")
        logtype_alert = ["ERROR"]
        if alert[0] in logtype_alert:
            alert_type = "[ALERT]"
        else:
            alert_type = "[INFO]"
        result_str = f"{alert_type} {alert[0]} level detected:{alert[1]}"
        return self.format_output(result_str)


    def validate(self, data: str) -> bool:
        try:
            if not isinstance(data, str):
                raise ValueError
        except ValueError:
            print("Validation: Data is not a string")
            return False
        
        valid_logtype = ["ERROR", "INFO", "WARN", "DEBUG", "LOG"]
        alert = data.split(":")
        log_type = alert[0].strip()
        if log_type not in valid_logtype:
            print("ERROR: Data is not log type")
            return False
        
        return True
    

if __name__ == "__main__":
    print("=== CODE NEXUS - DATA PROCESSOR FOUNDATION ===")
    print("\nInitializing Numeric Processor...")
    number = NumericProcessor()
    print(number.process([1, 2, 3, 4, 5, 'a']))

    print("\nInitializing Text Processor...")
    text = TextProcessor()
    print(text.process("Hello Nexus World"))

    print("\nInitializing Log Processor...")
    Log = LogProcessor()
    print(Log.process("LOG: Hello Nexus World"))

    print("\n=== Polymorphic Processing Demo ===")
    print("Processing multiple data types through same interface...\n")
    data = [(NumericProcessor(), [3, 2, 1]),
         (TextProcessor(), "Hey Mouse!!!"),
         (LogProcessor(), "INFO: System ready")]
    i = 1
    for processor, data in data:
        result = processor.process(data)
        print(f"Result {i}: {result}\n")
        i += 1
    print("\nFoundation systems online. Nexus ready for advanced streams.")
    