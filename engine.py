import re
import itertools
import pyspark.sql.functions as F
import copy
from functools import reduce
from typing import Union
from datetime import date
from pyspark.sql.dataframe import DataFrame

from .feature import Feature, AggFeature
from .timely import Daily, Monthly
from .aggregation_unit import MAPPING_AGG_DESC

class FSKitSession:
    def __init__(
        self,
        df: DataFrame,
        source: str,
        target_entity: Union[str, list[str], tuple[str]],
        dims: list,
        aggs: list,
        time_index: str = None,
    ) -> None:
        """
        Initialize a FSKitSession Object.

        :param df: The api acts as Pyspark DataFrame
        :param target_column: The target column for the aggregation.
        :param alias: Alias for the result of the aggregation.
        :param desc: The script describes the actual meaning of the target columns
        """
        self.df = df
        self.source = source
        self.target_entity: list[str] = list(target_entity)
        self.dims = dims
        self.aggs = aggs
        self.time_index = time_index

        # Internal param
        self._window_slide = None

    def __getattr__(self, name: str) -> None:
        # List the name of attributes follow the window slides
        att_day_list = [f"l{x}d" for x in range(1, 36501)]
        att_month_list = [f"l{x}m" for x in range(1, 121)]

        att_list = att_day_list + att_month_list
        if name in att_list:
            window_slide_num = int(name[1:-1])
            temp_obj = copy.copy(self)
            if name in att_day_list:
                temp_obj._window_slide = Daily(window_slide_num)
            elif name in att_month_list:
                temp_obj._window_slide = Monthly(window_slide_num)
            return temp_obj
        else:
            raise AttributeError(
                f"'{type(self).__name__}' object has no attribute '{name}'"
            )

    def _normalize_naming(self, input_string):
        """
        Normalize a string to camelCase.

        Params
        ------
        input_string: str
            Input String
        Return
        ------
        :str
            Normalized camelCase String
        """
        # Condition to be normalized
        if input_string == "" or input_string is None:
            return input_string

        # Split by any non-aphanumeric character
        words = re.split(r"[^a-zA-Z0-9]", input_string)
        words = [w.lower() for w in words]

        # Convert to camelCase
        normalized_output = reduce(
            lambda accumulated_arg, cur_arg: accumulated_arg
            + cur_arg[0].upper()
            + cur_arg[1:],
            words[1:],
            words[0],
        )

        return normalized_output

    def _normalized_dataframe(self, df) -> DataFrame:
        """
        Normalize Dataframe

        Params
        ------
        df: DataFrame
            Input Dataframe

        Return
        ------
        DataFrame
            Normalized the dimensions of current dataframe
        """
        rollup_df = df
        normalized_dim_cond = [
            reduce(
                lambda accumulated_arg, cur_arg: accumulated_arg.when(
                    F.col(dim.dim_name) == F.lit(cur_arg),
                    F.lit(self._normalize_naming(cur_arg)),
                ),
                dim.dim_values,
                F.when(F.lit(False), F.lit(None)),
            ).otherwise(F.col(dim.dim_name))
            for dim in self.dims
        ]

        dim_cols = [d.dim_name for d in self.dims]
        for dim_col, nor_cond in zip(dim_cols, normalized_dim_cond):
            rollup_df = rollup_df.withColumn(dim_col, nor_cond)

        return rollup_df

    def roll_up_dataframe(self, observe_date: date = None) -> DataFrame:
        """
        Summary to the `dimension` level

        Params
        ------
        observe_date: date
            The flag date that we look back.

        Return
        ------
        DataFrame
            Pyspark Dataframe contain
        """
        # Filtering the input dataframe
        if self.time_index is None:
            input_df = self.df
        else:
            try:
                _filter_qry = (
                    F.col(self.time_index)
                    >= (observe_date - self._window_slide.get_timedelta())
                ) & (F.col(self.time_index) < observe_date)
                input_df = self.df.filter(_filter_qry)
            except Exception:
                raise Exception(
                    "Missing param `observe_date` for the function or `time_index` for the object `FSKitSession`!"
                )

        # Filtering all the Dimension we need
        for d in self.dims:
            filter_query = F.col(d.dim_name).isin(d.dim_values)
            input_df = input_df.filter(filter_query)

        # Extract the grouped columns
        target_entity = self.target_entity
        dimensions = [d.dim_name for d in self.dims]

        # Grouped Columns & Aggregations
        grouped_cols = list(target_entity.__add__(dimensions))
        grouped_aggregations = [agg.build() for agg in self.aggs]

        # Dim Fact Roll Ups
        return input_df.groupBy(grouped_cols).agg(*grouped_aggregations)

    def extract_feature(self, observe_date: date = None) -> DataFrame:
        """
        Extract all the features in a FS session

        Params
        ------
        observe_date: date
            The flag date that we look back.

        Return
        ------
        DataFrame
            Pyspark Dataframe contain all the features that extracted from the FSSession/
        """
        # Get all the aggregations, metric columns or the alias we named in the AggregationUnit object
        grouped_aggregations = self.aggs
        agg_funcs = [
            self._normalize_naming(agg.agg_function) for agg in grouped_aggregations
        ]
        metric_columns = [
            self._normalize_naming(agg.alias) for agg in grouped_aggregations
        ]
        dim_cols = [dim.dim_name for dim in self.dims]

        # Get roll_up dataframe, normalized the roll_up dataframe
        rollup_df = self.roll_up_dataframe(observe_date=observe_date)
        rollup_df = self._normalized_dataframe(rollup_df)

        # Extract window slides features
        output_df = (
            rollup_df.withColumn(
                "_pivot_col",
                F.concat_ws(
                    "_",
                    F.lit(self._normalize_naming(self.source)),
                    *[F.col(d) for d in dim_cols],
                ),
            )
            .groupBy(self.target_entity)
            .pivot("_pivot_col")
            .agg(
                *[
                    F.first(F.col(_col)).alias(
                        "_".join(
                            [
                                self._normalize_naming(_col),
                                _agg,
                                str(self._window_slide),
                            ]
                            if self._window_slide is not None
                            else [self._normalize_naming(_col), _agg]
                        )
                    )
                    for _agg, _col in zip(agg_funcs, metric_columns)
                ]
            )
        )

        return output_df

    def get_dim_info(self):
        """
        Generate dimension combinations and their metadata.

        Returns
        -------
        list[list[str]]
            return a list of (dim_value, dc_name, dc_desc). In the return list of
            dim_value contains of `dim_name: dim_value`; dc_name: combination of
            dim value connected by `_`; dc_desc: dims combination description in VNese.
        """
        # Assign list of values of all the dimensions to `dim_combs` variable
        dim_combs = list(
            itertools.product(
                *[
                    [(d, self._normalize_naming(n_d)) for n_d in d.dim_values]
                    for d in self.dims
                ]
            )
        )
        # Assign the description for dimensions combination
        dim_meanings = list(
            itertools.product(
                *[[d.dim_description[n_d] for n_d in d.dim_values] for d in self.dims]
            )
        )
        # Extract all the dimensions information into a list[tuple]
        dim_info = []
        for dc, dm in zip(dim_combs, dim_meanings):
            dim_value = {_d[0]: _d[1] for _d in dc}
            dc_name = "_".join([_d[1] for _d in dc])
            dc_desc = " ".join(dm)
            dim_info.append([dim_value, dc_name, dc_desc])

        return dim_info

    def get_agg_info(self):
        """
        Generate aggregation unit descriptions.

        Returns
        -------
        list[dict]
            Return aggregation information which is a list of multiple dicts
            each contains the name of aggregation and the description of the
            aggregation itself
        """
        agg_info = []
        for agg in self.aggs:
            agg_desc = " ".join(
                [
                    MAPPING_AGG_DESC[self._normalize_naming(agg.agg_function)],
                    agg.desc,
                ]
            )
            agg_info.append([agg, agg_desc])
        return agg_info

    def get_agg_features(self, observe_date: date) -> list[Feature]:
        """
        Store the window features extracted in the session to the list
        """
        # Get all the information needed
        source = self.source
        target_entity = self.target_entity

        # Get all the dimension info
        dim_info = self.get_dim_info()

        # Get all the aggregation info
        agg_info = self.get_agg_info()

        full_info = list(itertools.product(dim_info, agg_info))
        feature_objs = []
        for info in full_info:
            # Gather all the window feature information
            dim_info = info[0]
            agg_info = info[1]
            ft_name = "_".join(
                [
                    source,
                    dim_info[1],
                    "_".join(
                        [
                            self._normalize_naming(agg_info[0].alias),
                            self._normalize_naming(agg_info[0].agg_function),
                        ]
                    ),
                ]
            ) + ("" if self._window_slide is None else "_" + str(self._window_slide))

            dim_value = dim_info[0]
            ft_description = " ".join(
                [
                    agg_info[-1],
                    dim_info[-1],
                    "" if self._window_slide is None else self._window_slide.__repr__(),
                ]
            )
            window_agg = agg_info[0]

            # create object and send it to the return list
            ft = AggFeature(
                name=ft_name,
                source=source,
                target_entity=target_entity,
                dim_value=dim_value,
                description=ft_description,
                observe_date=observe_date,
                window_agg=window_agg,
                window_slide=self._window_slide,
            )
            feature_objs.append(ft)

        return feature_objs
