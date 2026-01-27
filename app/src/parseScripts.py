import yaml
from typing import Dict, List, Union
from datetime import datetime
import os

SCRIPTS_FILE_PATH = os.getenv('SCRIPT_FILE_PATH','./config/scripts.yml')

class ParseScripts:
    scripts :dict
    file_path :str = SCRIPTS_FILE_PATH

    def __init__(self, file_path :str = SCRIPTS_FILE_PATH):

        self.scripts = {}
        self.file_path = file_path
        self.loadFromYml()

    def loadFromYml(self) -> Dict[str, Dict[str, str]]:
        with open(self.file_path, 'r', encoding='utf-8') as f:
            data = yaml.safe_load(f)
        
        self.scripts = {}
        
        if 'scripts' not in data:
            raise ValueError("YAML файл должен содержать ключ 'scripts'")
        
        self.scripts = data.get('scripts', {})
        
        return self.scripts
    
    
scripts = ParseScripts()