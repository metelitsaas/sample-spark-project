import argparse


# Application parameters
class Parameters:
    # Return parameters of application from arguments sting
    # @param args: string of main function arguments
    # @return params: dict of main function arguments
    @classmethod
    def get(cls, args):
        parser = argparse.ArgumentParser()
        parser.add_argument('--set', nargs='*')
        return cls.__parse_vars(parser.parse_args(args).set)

    # Parse list of the variables
    # @params items: list of arguments
    # @return d: dict of arguments
    @classmethod
    def __parse_vars(cls, items):
        d = {}
        if items:
            for item in items:
                key, value = cls.__parse_var(item)
                d[key] = value
        return d

    # Parse variable
    # @param string: key=value in string format
    # @return key: key of string
    # @return value: value of string
    @staticmethod
    def __parse_var(string):
        items = string.split('=')
        key = items[0].strip()
        if len(items) > 1:
            value = '='.join(items[1:])
            return key, value
