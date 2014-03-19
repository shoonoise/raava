import re


##### Exceptions #####
class ComparsionError(Exception):
    pass


##### Private methods #####
def _make_comparator(name, method):
    class comparator: # pylint: disable=C0103
        def __init__(self, operand):
            self._operand = operand

        def __repr__(self):
            return "<cmp {}({})>".format(self.__class__.__name__, self._operand)

        def __call__(self, value):
            try:
                return method(value, self._operand)
            except Exception as err:
                raise ComparsionError("Invalid operands for {}: {} vs. {}".format(self, value, self._operand)) from err

    # XXX: Hack for pickling error:
    #   pickle.PicklingError: Can't pickle <class 'raava.comparators.eq_comparator'>: it's not found as raava.comparators.eq_comparator
    # TODO: Change this one to types.new_class() (included in the library "types", starting with Python 3.3)
    globals()[comparator.__name__] = comparator

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

EQ = COMPARATORS_MAP["eq"]

