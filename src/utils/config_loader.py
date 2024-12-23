import yaml
from pathlib import Path
from typing import Dict, Any

class ConfigLoader:
    @staticmethod
    def load_config(config_path: str = "config/config.yaml") -> Dict[str, Any]:
        try:
            with open(config_path, 'r', encoding='utf-8') as file:
                config = yaml.safe_load(file)
            return config
        except FileNotFoundError:
            raise Exception(f"配置文件未找到: {config_path}")
        except yaml.YAMLError as e:
            raise Exception(f"配置文件格式错误: {str(e)}")
