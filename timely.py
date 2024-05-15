from dateutil.relativedelta import relativedelta
from typing import *
from datetime import datetime, date


class Timely:
    """
    An object that defines the time range of calculation

    Attributes:
        time_range: The range of time-series data that covers all the need of time in the data.
        time_type: Type of time we are focusing on.
        deps_timely: Dependencies Timely() objects.
    """
    
    def __init__(self):
        self.time_type = None
        self.time_range = None
        self.format = None
        

    def __eq__(self, other):
        if not isinstance(other, Timely):
            raise TypeError("Comparisons should only involve Timelys.")
        if (
            self.time_type != other.time_type
            or self.time_range != other.time_range
        ):
            return False

        return True

    def __copy__(self):
        timely_obj = Timely()
        timely_obj.time_type = self.time_type
        timely_obj.time_range = self.time_range

        return timely_obj


    def __lt__(self, other):
        if not isinstance(other, type(self)):
            raise TypeError("Comparisons should be comparable.")
        return self.time_range < other.time_range


    def __hash__(self) -> int:
        return hash((self.time_type, self.time_range))
    

class Monthly(Timely):
    """
    The child object of Timely object that represents the focused time type is `month`.

    Attributes:
        time_range: The range of time-series data that covers all the need of time in the data.
        time_type: `month`.
    """
    
    def __init__(
        self, 
        time_range: int
    ):
        
        self.time_range = time_range
        self.time_type = "month"
        self.format = "%Y-%m"


    def get_timedelta(self):
        return relativedelta(months=self.time_range)


    


class Daily(Timely):
    """
    The child object of Timely object that represents the focused time type is `day`.

    Attributes:
        time_range: The range of time-series data that covers all the need of time in the data.
        time_type: `day`.
    """    
    def __init__(
        self, 
        time_range: int):
        
        self.time_range = time_range
        self.time_type = "day"
        self.format = "%Y-%m-%d"

    def get_timedelta(self):
        return relativedelta(days=self.time_range)

    def __str__(self):
        return "daily"


