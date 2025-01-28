
class Timely:
    """
    An object that defines the time range of calculation

    Params
    ------
    time_range: int
        The range of time-series data that covers all the need of tim
        in the data.
    time_type: str
        Type of time we are focusing on.
    format: str
        Format that follows the rules of `datetime` lib in Python
        %Y: Year with century (e.g., 2024)
        %y: Year without century (e.g., 24)
        %m: Month as a zero-padded decimal number (01 to 12)
        %d: Day of the month as a zero-padded decimal number (01 to 31)
        %H: Hour (00 to 23)
        %M: Minute (00 to 59)
        %S: Second (00 to 59)
        %A: Full weekday name (e.g., Monday)
        %a: Abbreviated weekday name (e.g., Mon)
        %B: Full month name (e.g., January)
        %b: Abbreviated month name (e.g., Jan)
        >>> Timely().format = '%Y-%m-%d'
    """

    def __init__(self):
        self.time_type = None
        self.time_range = None
        self.format = None

    def __eq__(self, other):
        if not isinstance(other, Timely):
            raise TypeError("Comparisons should only involve Timelys.")
        if self.time_type != other.time_type or self.time_range != other.time_range:
            return False

        return True

    def __copy__(self) -> "Timely":
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

    def __repr__(self) -> str:
        return f"{self.time_range} {self.time_type}"