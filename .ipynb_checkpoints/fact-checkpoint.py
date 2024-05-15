from typing import *


class Fact:
    def __init__(
        self, 
        name: str, 
        ref_cols: Optional[List[str]] = [],
        description: str = ""
    ):
        self.name = name
        self.ref_cols = ref_cols
        self.is_valid()
        self.description = description

    def __eq__(self, other):
        if not isinstance(other, Fact):
            raise TypeError("Comparisons should only involve Fact objects.")
        if (
            self.ref_cols != other.ref_cols
            or self.name != other.name
        ):
            return False

        return True

    def is_valid(self):
        if '*' in self.ref_cols and len(self.ref_cols) > 1:
            raise ValueError("Asterisk * in SQL meaning all columns, can not aggregate on more columns.")
        return True

    def __hash__(self) -> int:
        return hash(self.name)
