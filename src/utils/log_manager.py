from typing import Dict
from .logger import Logger

class LogManager:
    _instances: Dict[str, Logger] = {}
    
    @classmethod
    def get_logger(cls, name: str) -> Logger:
        if name not in cls._instances:
            cls._instances[name] = Logger(name)
        return cls._instances[name]
