import unittest
from datetime import datetime
from functions.date_functions import DateFunctions


class TestDateFunctions(unittest.TestCase):

    def test_current_timestamp(self):
        pass

    def test_string_to_date(self):
        self.assertEqual(datetime(2020, 12, 1), DateFunctions.string_to_date('2020-12-01'))
        self.assertEqual(datetime(2020, 1, 1), DateFunctions.string_to_date('2020-01-01'))
        self.assertEqual(datetime(2020, 12, 12), DateFunctions.string_to_date('2020-12-12'))
        self.assertEqual(datetime(2020, 6, 1), DateFunctions.string_to_date('2020-06-01'))
        self.assertEqual(datetime(2020, 2, 25), DateFunctions.string_to_date('2020-02-25'))

    def test_date_to_string(self):
        self.assertEqual('2020-12-01', DateFunctions.date_to_string(datetime(2020, 12, 1)))
        self.assertEqual('2020-01-01', DateFunctions.date_to_string(datetime(2020, 1, 1)))
        self.assertEqual('2020-12-12', DateFunctions.date_to_string(datetime(2020, 12, 12)))
        self.assertEqual('2020-06-01', DateFunctions.date_to_string(datetime(2020, 6, 1)))
        self.assertEqual('2020-02-25', DateFunctions.date_to_string(datetime(2020, 2, 25)))

    def test_month_begin(self):
        self.assertEqual(datetime(2020, 12, 1),
                         DateFunctions.month_begin(datetime(2020, 12, 1)))
        self.assertEqual(datetime(2020, 1, 1),
                         DateFunctions.month_begin(datetime(2020, 1, 1)))
        self.assertEqual(datetime(2020, 12, 1),
                         DateFunctions.month_begin(datetime(2020, 12, 12)))
        self.assertEqual(datetime(2020, 6, 1),
                         DateFunctions.month_begin(datetime(2020, 6, 1)))
        self.assertEqual(datetime(2020, 2, 1),
                         DateFunctions.month_begin(datetime(2020, 2, 25)))

    def test_month_end(self):
        self.assertEqual(datetime(2020, 12, 31),
                         DateFunctions.month_end(datetime(2020, 12, 1)))
        self.assertEqual(datetime(2020, 1, 31),
                         DateFunctions.month_end(datetime(2020, 1, 1)))
        self.assertEqual(datetime(2020, 12, 31),
                         DateFunctions.month_end(datetime(2020, 12, 12)))
        self.assertEqual(datetime(2020, 6, 30),
                         DateFunctions.month_end(datetime(2020, 6, 1)))
        self.assertEqual(datetime(2020, 2, 29),
                         DateFunctions.month_end(datetime(2020, 2, 25)))
