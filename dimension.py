import warnings
from typing import Callable, Union, Dict


class Dimension:
    def __init__(
        self,
        dim_name: str,
        dim_values: Union[list[str], tuple[str]],
        dim_description: Union[list[list[str]], tuple[tuple[str]], dict] = None,
    ) -> None:
        """
        Initialize a dimension object.

        :param dim_name: Dimension's name
        :param dim_values: All values of the dimension
        :param dim_description: The dictionary script describes the actual meaning of the all dim's values
        """
        self.dim_name = dim_name
        self.dim_values = dim_values

        # Dimension description types handle
        if dim_description is None:
            warnings.warn(
                f"If the dimension {dim_name} left empty, there will be no metadata about features."
            )
        else:
            if isinstance(dim_description, dict):
                self.dim_description = dim_description
            elif isinstance(dim_description, (list, tuple)):
                try:
                    self.dim_description = dict(dim_description)
                except ValueError:
                    raise ValueError(
                        f"Invalid format: List or Tuple of the dimension {dim_name} description must contain key-value pairs!"
                    )
            else:
                raise TypeError(
                    "Error: dim_description must be a dictionary, a list/tuple of key-value pairs, or an object with attributes."
                )

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Dimension):
            raise TypeError("The comparision object must be `Dimension`.")
        return (self.dim_name, self.dim_values) == (other.dim_name, other.dim_values)

    def __hash__(self):
        return hash((self.dim_name, tuple(self.dim_values)))

    def __repr__(self):
        return f"Dimension({repr(self.dim_name)}, {repr(self.dim_values)}, {repr(self.dim_description)})"

    def validate_value(self, dim_value: str) -> bool:
        """
        Verify the dim value, make sure the value is within the allowed dimension values.

        Params
        ------
        dim_value: str
            The value needs to be evaluated

        Returns
        -------
        bool
            True if the value is allowed, False otherwise
        """
        return dim_value in self.dim_values


class DimensionFilterEngine:
    def __init__(self, mapping_dim: Dict[Dimension, str]):
        self.mapping_dim = mapping_dim

    def validate(self):
        """
        Validate all the mapping dimension to ensure that each reference value
        is in the corresponding Dimension.

        Returns
        -------
        bool
            True if all the values in the corresponding Dimension, False otherwise.
        """
        for dim, value in self.mapping_dim:
            if not isinstance(dim, Dimension):
                raise TypeError(
                    "The condition is not refer to the Dimension!!! Not `Dimension` object."
                )
            if not dim.validate_value(value):
                return False
        # After pass all the False cases
        return True

    def filter(
        self, *conditions: Callable[[Dimension, str], bool]
    ) -> Dict[Dimension, str]: ...
