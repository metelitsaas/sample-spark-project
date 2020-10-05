import yaml
import os

CONFIG_PATH = 'config/environment-config.yml'


# Environment of application config class
class EnvironmentConfig:
    # Get environment config by type from file
    # @param type: type of environment (prod, dev, etc)
    @staticmethod
    def get(env_type):

        # Generate file absolute path
        file_path = os.path.dirname(os.path.abspath(__file__))
        project_path = os.path.abspath(os.path.join(file_path, os.path.pardir))
        data_path = os.path.join(project_path, CONFIG_PATH)

        # Load yaml
        with open(data_path, 'r') as stream:
            return yaml.safe_load(stream)[env_type]
