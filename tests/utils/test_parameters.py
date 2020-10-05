import unittest
from utils.parameters import Parameters


class TestParameters(unittest.TestCase):

    def test_get(self):
        args = [
            "--set",
            "JOB_NM=test1",
            "TEST2 =test2",
            "TEST3=test3"
        ]
        params = Parameters.get(args)

        self.assertEqual('test1', params.get('JOB_NM'))
        self.assertEqual('test2', params.get('TEST2'))
        self.assertEqual('test3', params.get('TEST3'))
