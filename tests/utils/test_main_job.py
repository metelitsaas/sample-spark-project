import unittest
from abc import ABC
from utils.main_job import MainJob
from utils.parameters import Parameters
from utils.environment_config import EnvironmentConfig


class TestMainJob(unittest.TestCase):

    class ObjectMainJob(MainJob, ABC):
        def run(self):
            pass

    def setUp(self):
        self.args = ["--set", "env=dev"]
        self.ObjectMainJob(self.args).run()
        self.params = self.ObjectMainJob(self.args).params
        self.env = self.ObjectMainJob(self.args).env
        self.app_name = self.ObjectMainJob(self.args).app_name
        self.master = self.ObjectMainJob(self.args).master
        self.spark_session_config = self.ObjectMainJob(self.args).spark_session_config
        self.spark = self.ObjectMainJob(self.args).spark
        self.logger = self.ObjectMainJob(self.args).logger
        self.base_transforms = self.ObjectMainJob(self.args).base_transforms

    def tearDown(self):
        self.spark.stop()

    def test_initialize_params(self):
        params = Parameters.get(self.args)

        self.assertEqual(params, self.params)

    def test_initialize_env(self):
        env = EnvironmentConfig.get('dev')

        self.assertEqual(env, self.env)

    def test_initialize_app_name(self):
        app_name = 'ObjectMainJob'

        self.assertEqual(app_name, self.app_name)

    def test_initialize_master(self):
        master = 'local'

        self.assertEqual(master, self.master)

    def test_initialize_spark(self):
        tmp_value = self.spark \
            .sql('select count(*)') \
            .head()[0]

        self.assertEqual(1, tmp_value)

    def test_initialize_logger(self):
        self.logger.info('Info message')
        self.logger.warn('Warning message')

    @unittest.skip
    def test_base_transforms(self):
        bt = self.base_transforms
        pass
