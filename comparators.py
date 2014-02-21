import re

from . import rules


##### Private methods #####
def _make_comparator(name, method):
    class comparator(rules.AbstractComparator): # pylint: disable=C0103,W0223
        compare = lambda self, value: method(value, self.get_operand())
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
        # XXX: "eq" == rules.EqComparator, Use key=value directly instead of this
    )
}

