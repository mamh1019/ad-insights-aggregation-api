#!/usr/bin/env python
from typing import Any, Iterable, Sequence, List


def array_split(arr: Sequence[Any], chunk_size: int) -> List[List[Any]]:
    """
    Split arr into chunks of chunk_size (last chunk may be smaller).
    Example: len=250, chunk_size=100 -> [100,100,50]
        len=5,   chunk_size=3000 -> [5]
    """
    if chunk_size <= 0:
        raise ValueError("chunk_size must be > 0")
    return [list(arr[i : i + chunk_size]) for i in range(0, len(arr), chunk_size)]


def list_intersect(a: Iterable[Any], b: Iterable[Any]) -> list:
    return list(set(a).intersection(b))


def list_complement(a: Iterable[Any], b: Iterable[Any]) -> list:
    return list(set(a) - set(b))


def apply_dict_tree(origin_dict: dict, tree_nodes: list, node_value: any) -> None:
    """set dict key by list order
    dict = {}
    list = ['A', 'B', 'C']
    apply_dict_tree(dict, list, 1000)
    print(dict)
    {'A': {'B': {'C': 1000}}}
    """
    tmp = None
    max_index = len(tree_nodes) - 1
    for index, node in enumerate(tree_nodes):
        if tmp is None:
            next_val = {}
            if index == max_index:
                next_val = node_value
            tmp = origin_dict.setdefault(node, next_val)
        else:
            val = node_value if index == max_index else {}
            tmp = tmp.setdefault(node, val)
