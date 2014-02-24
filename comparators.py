import re

class ComparisonError(Exception):
    pass

##### Private methods #####
def _make_comparator(name, method):
    class comparator:
        def __init__(self, operand):
            self._operand = operand

        def __repr__(self):
            return "<cmp {}({})>".format(self.__class__.__name__, self._operand)

        def __call__(self, value):
            try:
                return method(value, self._operand)
            except Exception as err:
                raise ComparsionError("Invalid operands for {}: {} vs. {}".format(self, value, self._operand)) from err
    comparator.__name__ = name + "_comparator"
    return comparator


##### Public constants #####
COMPARATORS_MAP = {
    name: _make_comparator(name, method) for (name, method) in (
        ("ne",          lambda value, operand: value != operand),
        ("ge",          lambda value, operand: value >= operand),
        ("gt",          lambda value, operand: value > operand),
        ("le",          lambda value, operand: value <= operand),
        ("lt",          lambda value, operand: value < operand),
        ("in_list",     lambda value, operand: value in operand),
        ("not_in_list", lambda value, operand: value not in operand),
        ("regexp",      lambda value, operand: re.match(operand, value) is not None),
        ("eq",          lambda value, operand: value == operand),
    )
}

