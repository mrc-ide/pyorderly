from dataclasses import dataclass
from typing import Optional


@dataclass
class SearchOptions:
    """
    Control over how outpack will search for packets.

    Attributes
    ----------
    location : array of strings, optional
        Locations that will be included for search. If `None`, then all
        known locations will be included
    allow_remote : bool
        Indicates if we will consider packets that are only available
        remotely to be found.
    pull_metadata : bool
        Indicates if we will pull metadata from the locations before
        searching
    """

    location: Optional[list[str]] = None
    allow_remote: bool = False
    pull_metadata: bool = False

    @staticmethod
    def create(obj):
        """
        Construct a `SearchOptions` object from some object.

        Parameters
        ----------
        obj : any
            Typically this will be `None` (default construct the
            `SearchOptions` object), a `SearchOptions` object or a `dict`
            with some of the fields present in `SearchOptions`. An
            `TypeError` is thrown if any other type is passed.

        Returns
        -------
        A new `SearchOptions` object.
        """
        if obj is None:
            return SearchOptions()
        elif isinstance(obj, SearchOptions):
            return obj
        elif isinstance(obj, dict):
            return SearchOptions(**obj)
        else:
            msg = "Invalid object type for creating a SearchOptions"
            raise TypeError(msg)
