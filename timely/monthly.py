from dateutil.relativedelta import relativedelta
from .timely_base import Timely


class Monthly(Timely):
    """
    The child object of Timely object that represents the focused time
    type is `month`.

    Params
    ------
    time_range: int
        The range of time-series data that covers all the
        need of time in the data.
    time_type: str
        `month`.
    """

    def __init__(self, time_range: int):
        self.time_range = time_range
        self.time_type = "month"
        self.format = "%Y-%m"

    def get_timedelta(self):
        return relativedelta(months=self.time_range)

    def __str__(self) -> str:
        return f"l{self.time_range}m"

    def __repr__(self) -> str:
        return f"trong vòng {self.time_range} tháng gần đây"