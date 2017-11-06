import abc


class Parser(object):
    """Abstract class for data parsing to exact attributes (fields)"""
    __metaclass__ = abc.ABCMeta

    def __init__(self, fields):
        self.fields = fields
        self.fields_set = set(fields) if fields else set()

    @abc.abstractmethod
    def parse(self, data, **kwargs):
        """
        Override this method for fields extraction from data
        :param data: data can be in any appropriate format
        (text, json or other)
        :return: list of dictionaries where key is
        one of defined fields and value is this field's value
        """
        if len(self.fields) == 0:
            return None
        return [{f: None for f in self.fields}]
