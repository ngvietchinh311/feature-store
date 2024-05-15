from typing import *


class Entity:
    def __init__(
        self,
        name: str,
        keys: List[str],
        description: str = ""
    ):
        self.name = name
        self.keys = keys
        self.description = description

    def __eq__(self, other):
        if not isinstance(other, Entity):
            raise TypeError("Comparisons should only involve Entity objects.")
        if (
            self.name != other.name
            or self.keys != other.keys
        ):
            return False

        return True

    def __str__(self):
        return "Entity"


    def __hash__(self) -> int:
        return hash(self.name)
        

