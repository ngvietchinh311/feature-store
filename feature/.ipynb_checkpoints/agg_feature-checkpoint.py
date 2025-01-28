from typing import Union
from datetime import date

from .feature_base import Feature
from ..timely import Timely
from ..aggregation_unit import AggregationUnit

DimValueType = Union[
    list[list[str, str]],
    list[tuple[str, str]],
    tuple[tuple[str, str]],
    dict[str, str],
]

class AggFeature(Feature):
    def __init__(
        self,
        name: str,
        source: str,
        target_entity: Union[str, list[str], tuple[str]],
        dim_value: DimValueType,
        description: str,
        observe_date: date,
        window_agg: AggregationUnit,
        window_slide: Timely,
    ) -> None:
        super().__init__(name, source, target_entity, dim_value, description, observe_date)

        # Asign timely
        self.window_agg = window_agg
        self.window_slide = window_slide