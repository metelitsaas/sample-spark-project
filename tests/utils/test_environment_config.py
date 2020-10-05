import unittest
from utils.environment_config import EnvironmentConfig


class TestEnvironmentConfig(unittest.TestCase):

    def test_get(self):
        prod_env = EnvironmentConfig.get('prod')
        dev_env = EnvironmentConfig.get('dev')

        self.assertEqual('yarn', prod_env['master'])
        self.assertEqual('prod_spark', prod_env['source_schema'])
        self.assertEqual('prod_spark', prod_env['target_schema'])
        self.assertEqual('local', dev_env['master'])
        self.assertEqual('test_spark', dev_env['source_schema'])
        self.assertEqual('test_spark', dev_env['target_schema'])
