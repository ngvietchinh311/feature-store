from typing import Union
from datetime import date


DimValueType = Union[
    list[list[str, str]],
    list[tuple[str, str]],
    tuple[tuple[str, str]],
    dict[str, str],
]


class Feature:
    def __init__(
        self,
        name: str,
        source: str,
        target_entity: Union[str, list[str], tuple[str]],
        dim_value: DimValueType,
        description: str,
        observe_date: date,
    ) -> None:
        self.name = name
        self.source = source
        self.target_entity = target_entity

        # Cast the mapping `dim` to `dict` type.
        if isinstance(dim_value, dict):
            self.dim_value = dim_value
        elif isinstance(dim_value, (list, tuple)):
            try:
                self.dim_value = dict(dim_value)
            except ValueError:
                raise ValueError("Dim value should be a list/tuple of key-value pairs.")
        else:
            raise TypeError("Not suitable type.")

        self.description = description
        self.observe_date = observe_date

    def __repr__(self) -> str:
        return f"Feature: {self.name} from source {self.source}."

