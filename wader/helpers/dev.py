from typing import Type


def get_fqcn(t: Type) -> str:
    return f'{t.__module__}.{t.__qualname__}'


def get_fqcn_of(o: object) -> str:
    return get_fqcn(type(o))
