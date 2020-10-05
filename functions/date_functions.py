from datetime import datetime
from calendar import monthrange

DATE_FORMAT = '%Y-%m-%d'
DATETIME_FORMAT = '%Y-%m-%d %H:%M:%S'


# Data functions class
class DateFunctions:
    # Current date and time
    # @return : current timestamp
    @staticmethod
    def current_timestamp(): return datetime.now().strftime(DATETIME_FORMAT)

    # Parse string to date
    # @param str: string of date
    # @return : date in datetime of date
    @staticmethod
    def string_to_date(string): return datetime.strptime(string, DATE_FORMAT)

    @staticmethod
    # Parse date to string
    # @param date: date in datetime of date
    # @return : string of date
    def date_to_string(date): return date.strftime(DATE_FORMAT)

    @staticmethod
    # Get first of month from date
    # @param date: date in date
    # @return : first day of month in date
    def month_begin(date): return datetime(date.year, date.month, 1)

    @staticmethod
    # Get last day of month from date
    # @param date: date in date
    # @return : last day of month in date
    def month_end(date): return date.replace(day=monthrange(date.year, date.month)[1])
