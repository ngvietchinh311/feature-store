"""
SummaryEngine --> Engine
ExtractEngine --> Engine
Engine is capable of tuning the design of extracting feature. This samplize how to extract the feature 
follow the rule
"""
from timely import *
from dimension import *
from entity import *
from fact import *
from aggregation_unit import *


class Engine:
    def __init__(
        self,
        spark,
        base_path: str,
        output_path: str,
        partition_col: str,
        entity: Entity,
        mapping_dim: Dict[str, Dim],
        mapping_fact: Dict[str, Fact],
        base_agg: List[AggregationUnit],
        summary_agg: List[AggregationUnit]
    ) -> None:
        self.spark = spark
        self.base_path = base_path
        self.output_path = output_path
        self.partition_col = partition_col
        self.mapping_entity = entity
        self.mapping_dim = mapping_dim
        self.mapping_fact = mapping_fact
        self.base_agg = base_agg
        self.summary_agg = summary_agg
        
        self._data_date: Union[datetime, datetime.date]
        self._timely_source: Timely
        self._timely_target: Timely
        self._dim_combs: List[List[str]] = []


    
    def get_input_df(self) -> Dict[List[str], "DataFrame"]:
        try:
            self._timely_source
            self._data_date
            self._dim_combs
        except:
            raise ValueError("Missing Inputs, Denpendency timely is not declared.")

        dims_combs_dict = {}
        
        if self._timely_source == self._timely_target:
            input_df = self.spark.read.option("partitionCol", self.partition_col).parquet(self.base_path)\
            .filter(F.col(f"{self.partition_col}").between(self._data_date, self._data_date))

            for dim_comb in self._dim_combs:
                dims_combs_dict[".".join(dim_comb)] = input_df
        else:
            input_path = (self.output_path if self.output_path.endswith("/") else self.output_path + "/")\
            + "summary_layer"\
            + f"last_{self._timely_source.time_range}_{self._timely_source.time_type}"

            for dim_comb in self._dim_combs:
                if self._timely_source.time_type == self._timely_target.time_type:
                    input_df = self.spark.read.option("partitionCol", self.partition_col).parquet(input_path + ".".join(dim_comb))\
                    .filter(F.col(f"{self.partition_col}").between(self._data_date - eval(f"relativedelta({self._timely_target.time_type}s={self._timely_target.time_range})")\
                                                              + eval(f"relativedelta({self._timely_source.time_type}s={self._timely_source.time_range})"), 
                                                              self._data_date))
                else:
                    input_df = self.spark.read.option("partitionCol", self.partition_col).parquet(input_path + ".".join(dim_comb))\
                    .filter(F.col(f"{self.partition_col}").between(self._data_date - eval(f"relativedelta({self._timely_target.time_type}s={self._timely_target.time_range})"), 
                                                              self._data_date - eval(f"relativedelta({self._timely_source.time_type}s={self._timely_source.time_range})")))
                
                dims_combs_dict[".".join(dim_comb)] = input_df
            
        
        return dims_combs_dict
        
        
    def data_date(self, data_date) -> "Engine":
        """
        Set the data_date which is the time when extract the Feature Store
        """
        self._data_date = data_date
        return self


    def combine_dimensions(self, *args):
        """
        Get the dims' combinations to extract the neccessary features.
        """
        dims_comb = []
        for i in range(len(args)):
            dims_comb.append(args[i])

        self._dim_combs.append(dims_comb)

        return self
    

    def dependency(self, timely_source: Timely, timely_target: Timely) -> "Engine":
        """
        Set timely dependency to the source of Data
        """
        self._timely_source = timely_source
        self._timely_target = timely_target
        self._validate_startup_dependency()

        return self


    def _validate_startup_dependency(self) -> None:
        ...
    

    def summary_layer(self):
        
        input_df_dict = self.get_input_df()

        res = []
        
        if self._timely_source == self._timely_target:
            list_agg = [i.grouped_col() for i in self.base_agg]
        else:
            list_agg = self.summary_agg

        entity_cols = self.mapping_entity.keys
        for inputs in input_df_dict.items():
            dim_comb = inputs[0]
            df = inputs[1]
            if dim_comb == "":
                dim_comb = []
            else:
                dim_comb = inputs[0].split(".")    # List[str]
            grouped_cols = [*entity_cols, *dim_comb]
            
            ret_df = df.groupBy(grouped_cols).agg(*list_agg)
                
            ret_path = (self.output_path if self.output_path.endswith("/") else self.output_path + "/")\
            + "/".join(["summary_layer", f"last_{self._timely_target.time_range}_{self._timely_target.time_type}"])

            res.append((ret_df, ret_path, self._timely_source, self._timely_target
                        , self._data_date.strftime(self._timely_target.format)))
        
        return res
        


    def feature_layer(self):
        ...




class FS:
    
    def __init__(
        self,
        spark: SparkSession,
        input_path: str,
        output_path: str,
        partition_col: str
    ) -> None:
        self.spark = spark
        self.input_path = input_path
        self.output_path = output_path
        self.partition_col = partition_col

        self._timely: Timely = None
        self._entity: Entity = None
        self._dimension: List[Dim] = []
        self._fact: List[Fact] = []
        self._base_agg: List[AggregationUnit] = []
        self._summary_agg: List[AggregationUnit] = []

        self.engine: Engine = None
        


    def set_engine(self):
        ret_engine=  Engine(self.spark,
                            self.input_path,
                            self.output_path,
                            self.partition_col,
                            self._entity,
                            self.get_dimension_mapping(),
                            self.get_fact_mapping(),
                            self._base_agg,
                            self._summary_agg)

        self.engine = ret_engine

        return self


    def define_timely(self, timely:Timely):
        self._timely = timely

        return self
        

    def entity(self, 
               name: str, 
               ref_cols: List[str], 
               desc: str=""):
        self._entity = Entity(name=name, keys=ref_cols, description=desc)

        return self


    def dimension(self, 
                  col_name: str, 
                  accepted_values: List[str], 
                  desc=None) -> "FS":
        if desc is None:
            self._dimension.append(Dim(col_name=col_name, accepted_values=accepted_values))
        else:
            self._dimension.append(Dim(col_name=col_name, accepted_values=accepted_values, descriptions=desc))

        return self


    def fact(self, 
             name: str, 
             ref_cols: Optional[List[str]], 
             desc: str=""):
        self._fact.append(Fact(name=name, ref_cols=ref_cols, description=desc))

        return self


    def get_entity(self):
        """
        Return the mapping of entity's name to its corresponding object.
        
        """
        return self._entity


    def get_dimension_mapping(self):
        """
        Return the mapping of dimension's column-name to its corresponding object.
        
        """
        mapping_dimension = dict()
        for dim_obj in self._dimension:
            mapping_dimension[dim_obj.col_name] = dim_obj
        return mapping_dimension


    def get_fact_mapping(self):
        """
        Return the mapping of fact's name to its corresponding object.
        
        """
        mapping_fact = dict()
        for fact_obj in self._fact:
            mapping_fact[fact_obj.name] = fact_obj
        return mapping_fact


    def aggregate(self,
                  func_name: str, 
                  fact_name: str,
                  main_function:bool=False):
        """
        Aggregate function to load all the configs to the AggregationUnit Class, 
        Notes: fact_name ~ fact's name (only 1 string)
        """
        mapping_fact = self.get_fact_mapping()
        fact_obj = mapping_fact[fact_name]

        self._base_agg.append(AggregationUnit(cols=fact_obj.ref_cols,
                                              function=func_name,
                                              alias="_".join([func_name, fact_name])))
        self._summary_agg.append(AggregationUnit(cols=fact_obj.ref_cols,
                                                 function=func_name,
                                                 alias="_".join([func_name, fact_name])))
        
        if main_function:
            ref_funcs = ["sum", "avg", "min", "max"]
            ref_funcs.remove(func_name)
            for tmp_func in ref_funcs:
                self._summary_agg.append(AggregationUnit(cols=fact_obj.ref_cols,
                                                      function=tmp_func,
                                                      alias="_".join([tmp_func, func_name, fact_name])))


        return self

    


    """
    It can be read(), write(), ...
    """
    ...

