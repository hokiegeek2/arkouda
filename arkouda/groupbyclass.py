from __future__ import annotations
import enum
from typing import cast, List, Sequence, Tuple, Union, TYPE_CHECKING, Any
if TYPE_CHECKING:
    from arkouda.categorical import Categorical
import numpy as np # type: ignore
from typeguard import typechecked, check_type
from arkouda.client import generic_msg
from arkouda.pdarrayclass import pdarray, create_pdarray
from arkouda.sorting import argsort, coargsort
from arkouda.strings import Strings
from arkouda.pdarraycreation import array, zeros, arange
from arkouda.logger import getArkoudaLogger
from arkouda.dtypes import int64, uint64

__all__ = ["GroupBy", "broadcast", "GROUPBY_REDUCTION_TYPES"]

class GroupByReductionType(enum.Enum):
    SUM = 'sum'
    PROD = 'prod' 
    MEAN = 'mean'
    MIN = 'min'
    MAX = 'max'
    ARGMIN = 'argmin'
    ARGMAX = 'argmax'
    NUNUNIQUE = 'nunique'
    ANY = 'any'
    ALL = 'all'
    OR = 'or'
    AND = 'and'
    XOR = 'xor'
    
    def __str__(self) -> str:
        """
        Overridden method returns value, which is useful in outputting
        a GroupByReductionType as a request parameter
        """
        return self.value
    
    def __repr__(self) -> str:
        """
        Overridden method returns value, which is useful in outputting
        a GroupByReductionType as a request parameter
        """
        return self.value
    
GROUPBY_REDUCTION_TYPES = frozenset([member.value for _, member 
                                  in GroupByReductionType.__members__.items()])

groupable_element_type = Union[pdarray, Strings, 'Categorical']
groupable = Union[groupable_element_type, Sequence[groupable_element_type]]

class GroupBy:
    """
    Group an array or list of arrays by value, usually in preparation 
    for aggregating the within-group values of another array.

    Parameters
    ----------
    keys : (list of) pdarray, Strings, or Categorical
        The array to group by value, or if list, the column arrays to group by row
    assume_sorted : bool
        If True, assume keys is already sorted (Default: False)

    Attributes
    ----------
    nkeys : int
        The number of key arrays (columns)
    size : int
        The length of the input array(s), i.e. number of rows
    permutation : pdarray
        The permutation that sorts the keys array(s) by value (row)
    unique_keys : (list of) pdarray, Strings, or Categorical
        The unique values of the keys array(s), in grouped order
    ngroups : int
        The length of the unique_keys array(s), i.e. number of groups
    segments : pdarray
        The start index of each group in the grouped array(s)
    logger : ArkoudaLogger
        Used for all logging operations

    Raises
    ------
    TypeError
        Raised if keys is a pdarray with a dtype other than int64

    Notes
    -----
    Integral pdarrays, Strings, and Categoricals are natively supported, but
    float64 and bool arrays are not. 

    For a user-defined class to be groupable, it must inherit from pdarray
    and define or overload the grouping API:
      1) a ._get_grouping_keys() method that returns a list of pdarrays
         that can be (co)argsorted.
      2) (Optional) a .group() method that returns the permutation that 
         groups the array
    If the input is a single array with a .group() method defined, method 2
    will be used; otherwise, method 1 will be used.

    """
    Reductions = GROUPBY_REDUCTION_TYPES

    def __init__(self, keys: groupable,
                 assume_sorted: bool = False, hash_strings: bool = True) -> None:
        # Type Checks required because @typechecked was removed for causing other issues
        # This prevents non-bool values that can be evaluated to true (ie non-empty arrays)
        # from causing unexpected results. Experienced when forgetting to wrap multiple key arrays in [].
        # See Issue #1267
        if not isinstance(assume_sorted, bool):
            raise TypeError("assume_sorted must be of type bool.")
        if not isinstance(hash_strings, bool):
            raise TypeError("hash_strings must be of type bool.")
        from arkouda.categorical import Categorical
        self.logger = getArkoudaLogger(name=self.__class__.__name__)
        self.assume_sorted = assume_sorted
        self.hash_strings = hash_strings
        self.keys : groupable
        self.permutation : pdarray

        # Get all grouping keys, even if not required for finding permutation
        # They will be required later for finding segment boundaries
        if hasattr(keys, "_get_grouping_keys"):
            # Single groupable array
            self.nkeys = 1
            self.keys = cast(groupable_element_type, keys)
            self.size = cast(int, self.keys.size)
            self._grouping_keys = self.keys._get_grouping_keys()
        else:
            # Sequence of groupable arrays
            # Because of type checking, this is the only other possibility
            self.keys = cast(Sequence[groupable_element_type], keys)
            self.nkeys = len(self.keys)
            self.size = cast(int, self.keys[0].size)
            self._grouping_keys = []
            for k in self.keys:
                if k.size != self.size:
                    raise ValueError("Key arrays must all be same size")
                if not hasattr(k, "_get_grouping_keys"):
                    # Type checks should ensure we never get here
                    raise TypeError("{} does not support grouping".format(type(k)))
                self._grouping_keys.extend(cast(list, k._get_grouping_keys()))
        # Get permutation
        if assume_sorted:
            # Permutation is identity
            self.permutation = cast(pdarray, arange(self.size))
        elif hasattr(self.keys, "group"):
            # If an object wants to group itself (e.g. Categoricals),
            # let it set the permutation
            perm = self.keys.group() # type: ignore
            self.permutation = cast(pdarray, perm)
        elif len(self._grouping_keys) == 1:
            self.permutation = cast(pdarray, argsort(self._grouping_keys[0]))
        else:
            self.permutation = cast(pdarray, coargsort(self._grouping_keys))
                
        # Finally, get segment offsets and unique keys 
        self.find_segments()       
            
    def find_segments(self) -> None:
        from arkouda.categorical import Categorical
        cmd = "findSegments"

        if self.nkeys == 1:
            # for Categorical
            # Most categoricals already store segments and unique keys
            if hasattr(self.keys, 'segments') and cast(Categorical, 
                                                       self.keys).segments is not None:
                self.unique_keys: Any = cast(Categorical, self.keys)._categories_used
                self.segments = cast(pdarray, cast(Categorical, self.keys).segments)
                self.ngroups = self.unique_keys.size
                return

        keynames = [k.name for k in self._grouping_keys]
        keytypes = [k.objtype for k in self._grouping_keys]
        effectiveKeys = len(self._grouping_keys)
        args = "{} {:n} {} {}".format(self.permutation.name,
                                           effectiveKeys,
                                           ' '.join(keynames),
                                           ' '.join(keytypes))
        repMsg = generic_msg(cmd=cmd,args=args)
        segAttr, uniqAttr = cast(str,repMsg).split("+")
        self.logger.debug('{},{}'.format(segAttr, uniqAttr))
        self.segments = cast(pdarray, create_pdarray(repMsg=cast(str,segAttr)))
        unique_key_indices = create_pdarray(repMsg=cast(str,uniqAttr))
        if self.nkeys == 1:
            self.unique_keys = cast(groupable, 
                                    self.keys[unique_key_indices])
            self.ngroups = cast(groupable_element_type, self.unique_keys).size
        else:
            self.unique_keys = cast(groupable, 
                                    [k[unique_key_indices] for k in self.keys])
            self.ngroups = self.unique_keys[0].size
        # Free up memory, because _grouping_keys are not user-facing and no longer needed
        del self._grouping_keys


    def count(self) -> Tuple[groupable,pdarray]:
        '''
        Count the number of elements in each group, i.e. the number of times
        each key appears.

        Parameters
        ----------
        none

        Returns
        -------
        unique_keys : (list of) pdarray or Strings
            The unique keys, in grouped order
        counts : pdarray, int64
            The number of times each unique key appears
        
        Examples
        --------
        >>> a = ak.randint(1,5,10)
        >>> a
        array([3, 2, 3, 1, 2, 4, 3, 4, 3, 4])
        >>> g = ak.GroupBy(a)
        >>> keys,counts = g.count()
        >>> keys
        array([1, 2, 3, 4])
        >>> counts
        array([1, 2, 4, 3])        
        '''
        cmd = "countReduction"
        args = "{} {}".format(cast(pdarray, self.segments).name, self.size)
        repMsg = generic_msg(cmd=cmd, args=args)
        self.logger.debug(repMsg)
        return self.unique_keys, create_pdarray(repMsg)
    
    def aggregate(self, values: groupable, operator: str, skipna: bool=True) \
                    -> Tuple[groupable, pdarray]:
        '''
        Using the permutation stored in the GroupBy instance, group another 
        array of values and apply a reduction to each group's values. 

        Parameters
        ----------
        values : pdarray
            The values to group and reduce
        operator: str
            The name of the reduction operator to use

        Returns
        -------
        unique_keys : groupable
            The unique keys, in grouped order
        aggregates : groupable
            One aggregate value per unique key in the GroupBy instance
            
        Raises
        ------
        TypeError
            Raised if the values array is not a pdarray
        ValueError
            Raised if the key array size does not match the values size or
            if the operator is not in the GroupBy.Reductions array
        RuntimeError
            Raised if the requested operator is not supported for the
            values dtype
 
        Examples
        --------
        >>> keys = ak.arange(0, 10)
        >>> vals = ak.linspace(-1, 1, 10)
        >>> g = ak.GroupBy(keys)
        >>> g.aggregate(vals, 'sum')
        (array([0, 1, 2, 3, 4, 5, 6, 7, 8, 9]), array([-1, -0.77777777777777768, 
        -0.55555555555555536, -0.33333333333333348, -0.11111111111111116, 
        0.11111111111111116, 0.33333333333333348, 0.55555555555555536, 0.77777777777777768, 
        1]))
        >>> g.aggregate(vals, 'min')
        (array([0, 1, 2, 3, 4, 5, 6, 7, 8, 9]), array([-1, -0.77777777777777779, 
        -0.55555555555555558, -0.33333333333333337, -0.11111111111111116, 0.11111111111111116, 
        0.33333333333333326, 0.55555555555555536, 0.77777777777777768, 1]))
        '''
        
        operator = operator.lower()
        if operator not in self.Reductions:
            raise ValueError(("Unsupported reduction: {}\nMust be one of {}")\
                                  .format(operator, self.Reductions))
        
        # TO DO: remove once logic is ported over to Chapel
        if operator == 'nunique':
            return self.nunique(values)

        # All other aggregations operate on pdarray
        if cast(pdarray, values).size != self.size:
            raise ValueError(("Attempt to group array using key array of " +
                             "different length"))
        
        if self.assume_sorted:
            permuted_values = cast(pdarray, values)
        else:
            permuted_values = cast(pdarray, values)[cast(pdarray, self.permutation)]

        cmd = "segmentedReduction"
        args = "{} {} {} {}".format(permuted_values.name,
                                    self.segments.name,
                                    operator,
                                    skipna)
        repMsg = generic_msg(cmd=cmd,args=args)
        self.logger.debug(repMsg)
        if operator.startswith('arg'):
            return (self.unique_keys, 
                              cast(pdarray, self.permutation[create_pdarray(repMsg)]))
        else:
            return self.unique_keys, create_pdarray(repMsg)

    def sum(self, values : pdarray, skipna : bool=True) \
                         -> Tuple[groupable, pdarray]:
        """
        Using the permutation stored in the GroupBy instance, group 
        another array of values and sum each group's values. 

        Parameters
        ----------
        values : pdarray
            The values to group and sum

        Returns
        -------
        unique_keys : (list of) pdarray or Strings
            The unique keys, in grouped order
        group_sums : pdarray
            One sum per unique key in the GroupBy instance

        Raises
        ------
        TypeError
            Raised if the values array is not a pdarray object
        ValueError
            Raised if the key array size does not match the values size or
            if the operator is not in the GroupBy.Reductions array

        Notes
        -----
        The grouped sum of a boolean ``pdarray`` returns integers.
        
        Examples
        --------
        >>> a = ak.randint(1,5,10)
        >>> a
        array([3, 3, 4, 3, 3, 2, 3, 2, 4, 2])
        >>> g = ak.GroupBy(a)
        >>> g.keys
        array([3, 3, 4, 3, 3, 2, 3, 2, 4, 2])
        >>> b = ak.randint(1,5,10)
        >>> b
        array([3, 3, 3, 4, 1, 1, 3, 3, 3, 4])
        >>> g.sum(b)
        (array([2, 3, 4]), array([8, 14, 6]))
        """
        return self.aggregate(values, "sum", skipna)
    
    def prod(self, values : pdarray, skipna : bool=True) \
                    -> Tuple[groupable, pdarray]:
        """
        Using the permutation stored in the GroupBy instance, group
        another array of values and compute the product of each group's 
        values. 

        Parameters
        ----------
        values : pdarray
            The values to group and multiply

        Returns
        -------
        unique_keys : (list of) pdarray or Strings
            The unique keys, in grouped order
        group_products : pdarray, float64
            One product per unique key in the GroupBy instance

        Raises
        ------
        TypeError
            Raised if the values array is not a pdarray object
        ValueError
            Raised if the key array size does not match the values size
            or if the operator is not in the GroupBy.Reductions array
        RuntimeError
            Raised if prod is not supported for the values dtype

        Notes
        -----
        The return dtype is always float64.

        Examples
        --------
        >>> a = ak.randint(1,5,10)
        >>> a
        array([3, 3, 4, 3, 3, 2, 3, 2, 4, 2])
        >>> g = ak.GroupBy(a)
        >>> g.keys
        array([3, 3, 4, 3, 3, 2, 3, 2, 4, 2])
        >>> b = ak.randint(1,5,10)
        >>> b
        array([3, 3, 3, 4, 1, 1, 3, 3, 3, 4])
        >>> g.prod(b)
        (array([2, 3, 4]), array([12, 108.00000000000003, 8.9999999999999982]))
        """
        return self.aggregate(values, "prod", skipna)
    
    def mean(self, values : pdarray, skipna : bool=True) \
                    -> Tuple[groupable, pdarray]:
        """
        Using the permutation stored in the GroupBy instance, group 
        another array of values and compute the mean of each group's 
        values. 

        Parameters
        ----------
        values : pdarray
            The values to group and average

        Returns
        -------
        unique_keys : (list of) pdarray or Strings
            The unique keys, in grouped order
        group_means : pdarray, float64
            One mean value per unique key in the GroupBy instance

        Raises
        ------
        TypeError
            Raised if the values array is not a pdarray object
        ValueError
            Raised if the key array size does not match the values size
            or if the operator is not in the GroupBy.Reductions array

        Notes
        -----
        The return dtype is always float64.
        
        Examples
        --------
        >>> a = ak.randint(1,5,10)
        >>> a
        array([3, 3, 4, 3, 3, 2, 3, 2, 4, 2])
        >>> g = ak.GroupBy(a)
        >>> g.keys
        array([3, 3, 4, 3, 3, 2, 3, 2, 4, 2])
        >>> b = ak.randint(1,5,10)
        >>> b
        array([3, 3, 3, 4, 1, 1, 3, 3, 3, 4])
        >>> g.mean(b)
        (array([2, 3, 4]), array([2.6666666666666665, 2.7999999999999998, 3]))
        """
        return self.aggregate(values, "mean", skipna)
    
    def min(self, values : pdarray, skipna : bool=True) \
                    -> Tuple[groupable, pdarray]:
        """
        Using the permutation stored in the GroupBy instance, group 
        another array of values and return the minimum of each group's 
        values. 

        Parameters
        ----------
        values : pdarray
            The values to group and find minima

        Returns
        -------
        unique_keys : (list of) pdarray or Strings
            The unique keys, in grouped order
        group_minima : pdarray
            One minimum per unique key in the GroupBy instance

        Raises
        ------
        TypeError
            Raised if the values array is not a pdarray object or if min is
            not supported for the values dtype
        ValueError
            Raised if the key array size does not match the values size
            or if the operator is not in the GroupBy.Reductions array
        RuntimeError
            Raised if min is not supported for the values dtype

        Examples
        --------
        >>> a = ak.randint(1,5,10)
        >>> a
        array([3, 3, 4, 3, 3, 2, 3, 2, 4, 2])
        >>> g = ak.GroupBy(a)
        >>> g.keys
        array([3, 3, 4, 3, 3, 2, 3, 2, 4, 2])
        >>> b = ak.randint(1,5,10)
        >>> b
        array([3, 3, 3, 4, 1, 1, 3, 3, 3, 4])
        >>> g.min(b)
        (array([2, 3, 4]), array([1, 1, 3]))
        """
        if values.dtype == bool:
            raise TypeError('min is only supported for pdarrays of dtype float64, uint64, and int64')
        return self.aggregate(values, "min", skipna)
    
    def max(self, values : pdarray, skipna : bool=True) \
                    -> Tuple[groupable, pdarray]:
        """
        Using the permutation stored in the GroupBy instance, group
        another array of values and return the maximum of each 
        group's values. 

        Parameters
        ----------
        values : pdarray
            The values to group and find maxima

        Returns
        -------
        unique_keys : (list of) pdarray or Strings
            The unique keys, in grouped order
        group_maxima : pdarray
            One maximum per unique key in the GroupBy instance

        Raises
        ------
        TypeError
            Raised if the values array is not a pdarray object or if max is 
            not supported for the values dtype
        ValueError
            Raised if the key array size does not match the values size or
            if the operator is not in the GroupBy.Reductions array
        RuntimeError
            Raised if max is not supported for the values dtype
            
        Examples
        --------
        >>> a = ak.randint(1,5,10)
        >>> a
        array([3, 3, 4, 3, 3, 2, 3, 2, 4, 2])
        >>> g = ak.GroupBy(a)
        >>> g.keys
        array([3, 3, 4, 3, 3, 2, 3, 2, 4, 2])
        >>> b = ak.randint(1,5,10)
        >>> b
        array([3, 3, 3, 4, 1, 1, 3, 3, 3, 4])
        >>> g.max(b)
        (array([2, 3, 4]), array([4, 4, 3]))
        """
        if values.dtype == bool:
            raise TypeError('max is only supported for pdarrays of dtype float64, uint64, and int64')
        return self.aggregate(values, "max", skipna)
    
    def argmin(self, values : pdarray) \
                    -> Tuple[groupable, pdarray]:
        """
        Using the permutation stored in the GroupBy instance, group   
        another array of values and return the location of the first 
        minimum of each group's values. 

        Parameters
        ----------
        values : pdarray
            The values to group and find argmin

        Returns
        -------
        unique_keys : (list of) pdarray or Strings
            The unique keys, in grouped order
        group_argminima : pdarray, int64
            One index per unique key in the GroupBy instance

        Raises
        ------
        TypeError
            Raised if the values array is not a pdarray object or if argmax
            is not supported for the values dtype
        ValueError
            Raised if the key array size does not match the values
            size or if the operator is not in the GroupBy.Reductions array
        RuntimeError
            Raised if argmin is not supported for the values dtype

        Notes
        -----
        The returned indices refer to the original values array as
        passed in, not the permutation applied by the GroupBy instance.

        Examples
        --------
        >>> a = ak.randint(1,5,10)
        >>> a
        array([3, 3, 4, 3, 3, 2, 3, 2, 4, 2])
        >>> g = ak.GroupBy(a)
        >>> g.keys
        array([3, 3, 4, 3, 3, 2, 3, 2, 4, 2])
        >>> b = ak.randint(1,5,10)
        >>> b
        array([3, 3, 3, 4, 1, 1, 3, 3, 3, 4])
        >>> g.argmin(b)
        (array([2, 3, 4]), array([5, 4, 2]))       
        """
        if values.dtype == bool:
            raise TypeError('argmin is only supported for pdarrays of dtype float64, uint64, and int64')
        return self.aggregate(values, "argmin")
    
    def argmax(self, values : pdarray)\
                    -> Tuple[groupable, pdarray]:
        """
        Using the permutation stored in the GroupBy instance, group   
        another array of values and return the location of the first 
        maximum of each group's values. 

        Parameters
        ----------
        values : pdarray
            The values to group and find argmax

        Returns
        -------
        unique_keys : (list of) pdarray or Strings
            The unique keys, in grouped order
        group_argmaxima : pdarray, int64
            One index per unique key in the GroupBy instance

        Raises
        ------
        TypeError
            Raised if the values array is not a pdarray object or if argmax
            is not supported for the values dtype
        ValueError
            Raised if the key array size does not match the values size or
            if the operator is not in the GroupBy.Reductions array

        Notes
        -----
        The returned indices refer to the original values array as passed in,
        not the permutation applied by the GroupBy instance.

        Examples
        --------
        >>> a = ak.randint(1,5,10)
        >>> a
        array([3, 3, 4, 3, 3, 2, 3, 2, 4, 2])
        >>> g = ak.GroupBy(a)
        >>> g.keys
        array([3, 3, 4, 3, 3, 2, 3, 2, 4, 2])
        >>> b = ak.randint(1,5,10)
        >>> b
        array([3, 3, 3, 4, 1, 1, 3, 3, 3, 4])
        >>> g.argmax(b)
        (array([2, 3, 4]), array([9, 3, 2]))
        """
        if values.dtype == bool:
            raise TypeError('argmax is only supported for pdarrays of dtype float64, uint64, and int64')
        return self.aggregate(values, "argmax")
    
    def nunique(self, values : groupable) -> Tuple[groupable, pdarray]:
        """
        Using the permutation stored in the GroupBy instance, group another
        array of values and return the number of unique values in each group. 

        Parameters
        ----------
        values : pdarray, int64
            The values to group and find unique values

        Returns
        -------
        unique_keys : groupable
            The unique keys, in grouped order
        group_nunique : groupable
            Number of unique values per unique key in the GroupBy instance
            
        Raises
        ------
        TypeError
            Raised if the dtype(s) of values array(s) does/do not support 
            the nunique method
        ValueError
            Raised if the key array size does not match the values size or
            if the operator is not in the GroupBy.Reductions array
        RuntimeError
            Raised if nunique is not supported for the values dtype
            
        Examples
        --------
        >>> data = ak.array([3, 4, 3, 1, 1, 4, 3, 4, 1, 4])
        >>> data
        array([3, 4, 3, 1, 1, 4, 3, 4, 1, 4])
        >>> labels = ak.array([1, 1, 1, 2, 2, 2, 3, 3, 3, 4])
        >>> labels
        ak.array([1, 1, 1, 2, 2, 2, 3, 3, 3, 4])
        >>> g = ak.GroupBy(labels)
        >>> g.keys
        ak.array([1, 1, 1, 2, 2, 2, 3, 3, 3, 4])
        >>> g.nunique(data)
        array([1,2,3,4]), array([2, 2, 3, 1])
        #    Group (1,1,1) has values [3,4,3] -> there are 2 unique values 3&4
        #    Group (2,2,2) has values [1,1,4] -> 2 unique values 1&4
        #    Group (3,3,3) has values [3,4,1] -> 3 unique values
        #    Group (4) has values [4] -> 1 unique value
        """
        # TO DO: defer to self.aggregate once logic is ported over to Chapel
        # return self.aggregate(values, "nunique")
        
        ukidx = self.broadcast(arange(self.ngroups), permute=True)
        # Test if values is single array, i.e. either pdarray, Strings,
        # or Categorical (the last two have a .group() method).
        # Can't directly test Categorical due to circular import.
        if isinstance(values, pdarray):
            if cast(pdarray, values).dtype != int64 and cast(pdarray, values).dtype != uint64:
                raise TypeError("nunique unsupported for this dtype")
            togroup = [ukidx, values]
        elif hasattr(values, "group"):
            togroup = [ukidx, values]
        else:
            for v in values:
                if isinstance(values, pdarray) and cast(pdarray, values).dtype != int64 and cast(pdarray, values).dtype != uint64:
                    raise TypeError("nunique unsupported for this dtype")
            togroup = [ukidx] + list(values)
        # Find unique pairs of (key, val)
        g = GroupBy(togroup)
        # Group unique pairs again by original key
        g2 = GroupBy(g.unique_keys[0], assume_sorted=True)
        # Count number of unique values per key
        _, nuniq = g2.count()
        # Re-join unique counts with original keys (sorting guarantees same order)
        return self.unique_keys, nuniq
    
    def any(self, values : pdarray) \
                    -> Tuple[Union[pdarray,List[Union[pdarray,Strings]]],pdarray]:
        """
        Using the permutation stored in the GroupBy instance, group another 
        array of values and perform an "or" reduction on each group. 

        Parameters
        ----------
        values : pdarray, bool
            The values to group and reduce with "or"

        Returns
        -------
        unique_keys : (list of) pdarray or Strings
            The unique keys, in grouped order
        group_any : pdarray, bool
            One bool per unique key in the GroupBy instance
            
        Raises
        ------
        TypeError
            Raised if the values array is not a pdarray or if the pdarray
            dtype is not bool
        ValueError
            Raised if the key array size does not match the values size or
            if the operator is not in the GroupBy.Reductions array
        """
        if values.dtype != bool:
            raise TypeError('any is only supported for pdarrays of dtype bool')
        return self.aggregate(values, "any")  # type: ignore

    def all(self, values : pdarray) \
                    -> Tuple[Union[pdarray,List[Union[pdarray,Strings]]],pdarray]:
        """
        Using the permutation stored in the GroupBy instance, group  
        another array of values and perform an "and" reduction on 
        each group. 

        Parameters
        ----------
        values : pdarray, bool
            The values to group and reduce with "and"

        Returns
        -------
        unique_keys : (list of) pdarray or Strings
            The unique keys, in grouped order
        group_any : pdarray, bool
            One bool per unique key in the GroupBy instance
            
        Raises
        ------
        TypeError
            Raised if the values array is not a pdarray or if the pdarray
            dtype is not bool
        ValueError
            Raised if the key array size does not match the values size or
            if the operator is not in the GroupBy.Reductions array
        RuntimeError
            Raised if all is not supported for the values dtype
        """
        if values.dtype != bool:
            raise TypeError('all is only supported for pdarrays of dtype bool')

        return self.aggregate(values, "all")  # type: ignore

    def OR(self, values : pdarray) \
                    -> Tuple[Union[pdarray,List[Union[pdarray,Strings]]],pdarray]:
        """
        Bitwise OR of values in each segment.
        
        Using the permutation stored in the GroupBy instance, group  
        another array of values and perform a bitwise OR reduction on 
        each group. 

        Parameters
        ----------
        values : pdarray, int64
            The values to group and reduce with OR

        Returns
        -------
        unique_keys : (list of) pdarray or Strings
            The unique keys, in grouped order
        result : pdarray, int64
            Bitwise OR of values in segments corresponding to keys
            
        Raises
        ------
        TypeError
            Raised if the values array is not a pdarray or if the pdarray
            dtype is not int64
        ValueError
            Raised if the key array size does not match the values size or
            if the operator is not in the GroupBy.Reductions array
        RuntimeError
            Raised if all is not supported for the values dtype
        """
        if values.dtype != int64 and values.dtype != uint64:
            raise TypeError('OR is only supported for pdarrays of dtype int64 or uint64')

        return self.aggregate(values, "or")  # type: ignore

    def AND(self, values : pdarray) \
                    -> Tuple[Union[pdarray,List[Union[pdarray,Strings]]],pdarray]:
        """
        Bitwise AND of values in each segment.
        
        Using the permutation stored in the GroupBy instance, group  
        another array of values and perform a bitwise AND reduction on 
        each group. 

        Parameters
        ----------
        values : pdarray, int64
            The values to group and reduce with AND

        Returns
        -------
        unique_keys : (list of) pdarray or Strings
            The unique keys, in grouped order
        result : pdarray, int64
            Bitwise AND of values in segments corresponding to keys
            
        Raises
        ------
        TypeError
            Raised if the values array is not a pdarray or if the pdarray
            dtype is not int64
        ValueError
            Raised if the key array size does not match the values size or
            if the operator is not in the GroupBy.Reductions array
        RuntimeError
            Raised if all is not supported for the values dtype
        """
        if values.dtype != int64 and values.dtype != uint64:
            raise TypeError('AND is only supported for pdarrays of dtype int64 or uint64')

        return self.aggregate(values, "and")  # type: ignore

    def XOR(self, values : pdarray) \
                    -> Tuple[Union[pdarray,List[Union[pdarray,Strings]]],pdarray]:
        """
        Bitwise XOR of values in each segment.
        
        Using the permutation stored in the GroupBy instance, group  
        another array of values and perform a bitwise XOR reduction on 
        each group. 

        Parameters
        ----------
        values : pdarray, int64
            The values to group and reduce with XOR

        Returns
        -------
        unique_keys : (list of) pdarray or Strings
            The unique keys, in grouped order
        result : pdarray, int64
            Bitwise XOR of values in segments corresponding to keys
            
        Raises
        ------
        TypeError
            Raised if the values array is not a pdarray or if the pdarray
            dtype is not int64
        ValueError
            Raised if the key array size does not match the values size or
            if the operator is not in the GroupBy.Reductions array
        RuntimeError
            Raised if all is not supported for the values dtype
        """
        if values.dtype != int64 and values.dtype != uint64:
            raise TypeError('XOR is only supported for pdarrays of dtype int64 or uint64')

        return self.aggregate(values, "xor")  # type: ignore

    @typechecked
    def broadcast(self, values : pdarray, permute : bool=True) -> pdarray:
        """
        Fill each group's segment with a constant value.

        Parameters
        ----------
        values : pdarray
            The values to put in each group's segment
        permute : bool
            If True (default), permute broadcast values back to the ordering
            of the original array on which GroupBy was called. If False, the
            broadcast values are grouped by value.

        Returns
        -------
        pdarray
            The broadcast values
            
        Raises
        ------
        TypeError
            Raised if value is not a pdarray object
        ValueError
            Raised if the values array does not have one 
            value per segment

        Notes
        -----
        This function is a sparse analog of ``np.broadcast``. If a
        GroupBy object represents a sparse matrix (tensor), then
        this function takes a (dense) column vector and replicates
        each value to the non-zero elements in the corresponding row.

        Examples
        --------
        >>> a = ak.array([0, 1, 0, 1, 0])
        >>> values = ak.array([3, 5])
        >>> g = ak.GroupBy(a)
        # By default, result is in original order
        >>> g.broadcast(values)
        array([3, 5, 3, 5, 3])
        
        # With permute=False, result is in grouped order
        >>> g.broadcast(values, permute=False)
        array([3, 3, 3, 5, 5]
        
        >>> a = ak.randint(1,5,10)
        >>> a
        array([3, 1, 4, 4, 4, 1, 3, 3, 2, 2])
        >>> g = ak.GroupBy(a)
        >>> keys,counts = g.count()
        >>> g.broadcast(counts > 2)
        array([True False True True True False True True False False])
        >>> g.broadcast(counts == 3)
        array([True False True True True False True True False False])
        >>> g.broadcast(counts < 4)
        array([True True True True True True True True True True])
        """
        if values.size != self.segments.size:
            raise ValueError("Must have one value per segment")
        cmd = "broadcast"
        args = "{} {} {} {} {}".format(self.permutation.name,
                                                self.segments.name,
                                                values.name,
                                                permute,
                                                self.size)
        repMsg = generic_msg(cmd=cmd,args=args)
        return create_pdarray(repMsg)

def broadcast(segments : pdarray, values : pdarray, size : Union[int,np.int64,np.uint64]=-1,
              permutation : Union[pdarray, None]=None):
    '''
    Broadcast a dense column vector to the rows of a sparse matrix or grouped array.
    
    Parameters
    ----------
    segments : pdarray, int64
        Offsets of the start of each row in the sparse matrix or grouped array.
        Must be sorted in ascending order.
    values : pdarray
        The values to broadcast, one per row (or group)
    size : int
        The total number of nonzeros in the matrix. If permutation is given, this
        argument is ignored and the size is inferred from the permutation array.
    permutation : pdarray, int64
        The permutation to go from the original ordering of nonzeros to the ordering
        grouped by row. To broadcast values back to the original ordering, this
        permutation will be inverted. If no permutation is supplied, it is assumed
        that the original nonzeros were already grouped by row. In this case, the
        size argument must be given.
        
    Returns
    -------
    pdarray
        The broadcast values, one per nonzero
        
    Raises
    ------
    ValueError
        - If segments and values are different sizes
        - If segments are empty
        - If number of nonzeros (either user-specified or inferred from permutation)
          is less than one
        
    Examples
    --------
    # Define a sparse matrix with 3 rows and 7 nonzeros
    >>> row_starts = ak.array([0, 2, 5])
    >>> nnz = 7
    # Broadcast the row number to each nonzero element
    >>> row_number = ak.arange(3)
    >>> ak.broadcast(row_starts, row_number, nnz)
    array([0 0 1 1 1 2 2])
    
    # If the original nonzeros were in reverse order...
    >>> permutation = ak.arange(6, -1, -1)
    >>> ak.broadcast(row_starts, row_number, permutation=permutation)
    array([2 2 1 1 1 0 0])
    '''
    if segments.size != values.size:
        raise ValueError("segments and values arrays must be same size")
    if segments.size == 0:
        raise ValueError("cannot broadcast empty array")
    if permutation is None:
        if size == -1:
            raise ValueError("must either supply permutation or size")
        pname = "none"
        permute = False
    else:
        pname = permutation.name
        permute = True
        size = permutation.size
    if size < 1:
        raise ValueError("result size must be greater than zero")
    cmd = "broadcast"
    args = "{} {} {} {} {}".format(pname,
                                            segments.name,
                                            values.name,
                                            permute,
                                            size)
    repMsg = generic_msg(cmd=cmd,args=args)
    return create_pdarray(repMsg)
