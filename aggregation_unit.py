import pyspark.sql.functions as F
from typing import Union

MAPPING_AGG_DESC = {
    "count": "đếm",
    "countDistinct": "đếm riêng biệt",
    "sum": "tổng",
    "min": "giá trị nhỏ nhất",
    "max": "giá trị lớn nhất",
}

class AggregationUnit(object):
    def __init__(
        self,
        agg_function: str,
        target_column: Union[str, list[str]],
        alias: str = None,
        desc: str = "",
    ) -> None:
        """
        Initialize an aggregation unit.

        :param agg_function: The aggregation function name (e.g., 'count', 'sum', etc.).
        :param target_column: The target column for the aggregation.
        :param alias: Alias for the result of the aggregation.
        :param desc: The script describes the actual meaning of the target columns
        """
        self.agg_function = agg_function
        self.target_column = target_column
        # If alias left `None`, assign a default value to it
        if alias is None:
            if isinstance(target_column, list):
                columns_str = "_".join(target_column)
            else:
                columns_str = target_column
            self.alias = f"{agg_function}_{columns_str}"
        else:
            self.alias = alias
        self.desc = desc

    def build(self):
        """
        Build the PySpark aggregation operation.

        :return: A PySpark Column representing the aggregation operation.
        """

        # Map of aggregation functions
        AGG_FUNCTIONS = {
            "avg": F.avg,
            "count": F.count,
            "sum": F.sum,
            "max": F.max,
            "min": F.min,
            "count_distinct": F.count_distinct,
            "countdistinct": F.count_distinct,
        }

        # Get the PySpark function dynamically based on agg_function
        agg_func = AGG_FUNCTIONS.get(self.agg_function.lower(), None)

        if agg_func is None:
            raise ValueError(
                f"Aggregation function '{self.agg_function}' is not supported."
            )

        # Return the aggregation operation with an alias
        return agg_func(self.target_column).alias(self.alias)

    def __repr__(self) -> str:
        """
        Provide a string representation of the aggregation unit.
        """
        return f"AggregationUnit(agg_function='{self.agg_function}', target_column='{self.target_column}', alias='{self.alias}', desc='{self.desc}')"
