from abc import ABCMeta, abstractmethod
from utils.spark_session_config import SparkSessionConfig
from utils.parameters import Parameters
from utils.environment_config import EnvironmentConfig
from functions.base_transforms import BaseTransforms


# Abstract class of ETL job
class MainJob(metaclass=ABCMeta):
    # @param args: arguments of application
    def __init__(self, args):
        # Init jobs parameters
        self.params = Parameters.get(args)

        # Set environment configuration
        self.env = EnvironmentConfig.get(self.params.get('env'))

        # Set SparkSession configuration
        self.app_name = self.__class__.__name__  # Name of the class
        self.master = self.env['master']  # Type of master from environment config
        self.spark_session_config = SparkSessionConfig(self.app_name, self.master)

        self.spark = self.spark_session_config.spark
        self.logger = self.spark_session_config.logger

        # Init transformations
        self.base_transforms = BaseTransforms(self.spark)

        # Run job
        self.logger.info('Running job')
        self.run()
        self.logger.info('Job finished')

    # Run job abstract function
    @abstractmethod
    def run(self):
        pass

    # Chain transformation support
    # @param f: name of transformation function
    # @return : transformation function
    def transform(self, f):
        return f(self)
