
class Dim:
    """
    Dimensions that provides explicitly the features of the entity
    
    """
    def __init__(
        self,
        col_name: str,
        accepted_values: List[str],
        descriptions: Dict[str, str] = dict(),
    ):
        self.col_name = col_name
        self.accepted_values = accepted_values
        self.descriptions = descriptions

    def get_desc(self, value):
        return self.descriptions[value]

    def __eq__(self, other):
        if not isinstance(other, Dim):
            raise TypeError("Comparisons should only involve Dim objects.")
        if (
            self.col_name != other.col_name
        ):
            return False

        return True

    def __hash__(self) -> int:
        return hash(self.col_name)




