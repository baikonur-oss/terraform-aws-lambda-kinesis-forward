from typing import List


def split_list(lst: list, size: int) -> List[list]:
    for i in range(0, len(lst), size):
        yield lst[i:i + size]
