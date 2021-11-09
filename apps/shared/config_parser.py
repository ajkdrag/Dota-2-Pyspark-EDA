import json
from pathlib import Path


class ConfigParser:
    def __init__(self, config_file: str):
        base_config_file = "config.json"
        self.config_path = Path(config_file)
        self.base_dir = self.config_path.parent
        self.base_config_path = self.base_dir / base_config_file

    @staticmethod
    def read_json(config_path: Path):
        with config_path.open() as stream:
            return json.load(stream)

    def parse_config(self):
        base_config = ConfigParser.read_json(self.base_config_path)
        config = ConfigParser.read_json(self.config_path)
        base_config.update(config)
        return base_config
        
