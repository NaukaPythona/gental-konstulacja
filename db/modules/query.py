from typing import List, Any


class Query:    
    def __init__(self, column_name: str) -> None:
        self.column_name = column_name
        self.query_tree: List[List[Query]] = [[self]]
        # AND adds Queries to same inner list
        # OR  creates another list
        #
        # a AND b or C and D
        # tree = [[a, b], [c, d]]
        # (any inner list must be True to succeed.)
        self.repr = ""
        
    def __repr__(self) -> str:
        return self.repr
        
    def resolve(self, dictmodel: dict) -> bool:
        """
        Check all queries in query tree. Does given res_dict apply to query?
        """
        
        bin_result = [all([s._check(dictmodel) for s in statements]) for statements in self.query_tree]
        result = any(bin_result)
        self.__bool__ = lambda: result
        return result
       
    def _check(self, res_dict: dict) -> bool:
        raise NotImplementedError
    
    def __bool__(self):
        if not self.repr:
            raise NotImplementedError
        
    def __eq__(self, value: Any):
        self._check = lambda d: d.get(self.column_name) == value
        self.repr = f"({self.column_name} == {value})"
        return self
    
    def __ne__(self, value: Any):
        self._check = lambda d: d.get(self.column_name) != value
        self.repr = f"({self.column_name} != {value})"
        return self

    def __lt__(self, value: Any):
        self._check = lambda d: d.get(self.column_name) < value
        self.repr = f"({self.column_name} < {value})"
        return self
    
    def __gt__(self, value: Any):
        print("gt")
        self._check = lambda d: d.get(self.column_name) > value
        self.repr = f"({self.column_name} > {value})"
        return self
    
    def __le__(self, value: Any):
        self._check = lambda d: d.get(self.column_name) <= value
        self.repr = f"({self.column_name} <= {value})"
        return self
    
    def __ge__(self, value: Any):
        self._check = lambda d: d.get(self.column_name) >= value
        self.repr = f"({self.column_name} >= {value})"
        return self
    
    def __contains__(self, value: Any):
        self._check = lambda d: value in d.get(self.column_name)
        self.repr = f"({value} IN {self.column_name})"
        return self

