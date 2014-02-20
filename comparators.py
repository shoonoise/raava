import re

from . import rules


##### Public classes #####
class InListComparator(rules.AbstractComparator):
    def __init__(self, *variants):
        rules.AbstractComparator.__init__(self, variants)

    def compare(self, value):
        return ( value in self._operand )

class NotInListComparator(rules.AbstractComparator):
    def __init__(self, *variants):
        rules.AbstractComparator.__init__(self, variants)

    def compare(self, value):
        return ( value not in self._operand )

class RegexpComparator(rules.AbstractComparator):
    def __init__(self, regexp):
        rules.AbstractComparator.__init__(self, re.compile(regexp))

    def compare(self, value):
        return ( self._operand.match(value) is not None )

EqComparator = rules.EqComparator

class NeComparator(rules.AbstractComparator):
    def compare(self, value):
        return ( value != self._operand )

class GeComparator(rules.AbstractComparator):
    def compare(self, value):
        return ( value >= self._operand )

class GtComparator(rules.AbstractComparator):
    def compare(self, value):
        return ( value > self._operand )

class LeComparator(rules.AbstractComparator):
    def compare(self, value):
        return ( value <= self._operand )

class LtComparator(rules.AbstractComparator):
    def compare(self, value):
        return ( value < self._operand )

