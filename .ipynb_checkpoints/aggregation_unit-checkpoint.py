import pyspark
import pyspark.sql.functions as F
from typing import *

"""
Fixed pyspark functions provided
"""
MAPPING_AGGREGATION = {
    "lit": F.lit,
    "avg": F.avg,
    "min": F.min,
    "max": F.max,
    "sum": F.sum,
    "count": F.count,
    "countdistinct": F.countDistinct,
    "count_distinct": F.countDistinct
}


class AggregationUnit:
    """
    Represent a full-components-aggregation 
    """
    def __init__(
        self,
        # entity: Entity,
        # dim_combination: List[Dim],
        cols: List[str],
        function: str,
        alias: str
    ):
        # self.entity = entity
        # self.dim_combination = dim_combination
        self.cols = cols
        self.function = MAPPING_AGGREGATION[function]
        self.alias = alias

    def __eq__(self, other):
        if not isinstance(other, Fact):
            raise TypeError("Comparisons should only involve AggregationUnit objects.")
        if (
            self.cols != other.cols
            or self.function != other.function
        ):
            return False

        return True

    def grouped_col(self) -> pyspark.sql.column.Column:
        return self.function(*(self.cols)).alias(self.alias)
        