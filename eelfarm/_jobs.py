from functools import partial

import numpy as np
from eelbrain import NDVar, boosting
from eelbrain._data_obj import dataobj_repr


class JobWithID:
    _next_id = 0

    def __init__(self):
        self.id = JobWithID._next_id
        JobWithID._next_id += 1


def ndvar_description(ndvar):
    if isinstance(ndvar, NDVar):
        return dataobj_repr(ndvar)
    else:
        return ' + '.join(map(dataobj_repr, ndvar))


def job_description(func, args, kwargs):
    if isinstance(func, partial):
        args = (*func.args, *args)
        kwargs = {**func.keywords, **kwargs}
        func = func.func

    if func is boosting:
        out = 'boosting('
        if args:
            out += ndvar_description(args[0])
            args = args[1:]
            if args:
                out += ' ~ ' + ndvar_description(args[0])
                args = args[1:]
        if args or kwargs:
            out += ', '
    else:
        out = f'{func.__name__}('

    if args:
        out += ', '.join(map(arg_repr, args))
        if kwargs:
            out += ', '

    if kwargs:
        out += ', '.join(f'{key}={arg_repr(arg)}' for key, arg in kwargs.items())

    return out + ')'


def arg_repr(arg):
    if isinstance(arg, NDVar):
        return arg.name if arg.name else dataobj_repr(arg)
    elif isinstance(arg, np.ndarray):
        return f'<array {arg.shape}>'
    else:
        try:
            return repr(arg)
        except Exception:
            return f"<{arg.__class__.__name__}>"
