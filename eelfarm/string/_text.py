"""Text generation helpers"""
from collections.abc import Iterable


def enumeration(items: Iterable[object], link: str = 'and'):
    "['a', 'b', 'c'] -> 'a, b and c'"
    items = list(map(str, items))
    if len(items) >= 2:
        return f"{', '.join(items[:-1])} {link} {items[-1]}"
    elif len(items) == 1:
        return items[0]
    else:
        raise ValueError(f"{items=}")


def ms(t_s):
    "Convert time in seconds to rounded milliseconds"
    return int(round(t_s * 1000))


def ms_window(t0, t1):
    return f"{ms(t0)} - {ms(t1)} ms"


def named_list(items, name='item'):
    "named_list([1, 2, 3], 'number') -> 'numbers (1, 2, 3)"
    if len(items) == 1:
        return f"{name} ({items[0]!r})"
    else:
        if name.endswith('y'):
            name = name[:-1] + 'ie'
        return f"{name}s ({', '.join(map(repr, items))})"


def n_of(
        n: int,
        of: str,
        plural_for_0: bool = False,
):
    "n_of(3, 'epoch') -> '3 epochs'"
    if n == 0:
        return f"no {plural(of, not plural_for_0)}"
    return f"{n} {plural(of, n)}"


PLURALS = {
    'is': 'are',
}


def plural(singular, n):
    "plural('house', 2) -> 'houses'"
    if n == 1:
        return singular
    elif singular in PLURALS:
        return PLURALS[singular]
    elif singular[-1] == 'y' and singular[-2] != 'e':
        return singular[:-1] + 'ies'
    else:
        return singular + 's'

def ask(
        message: str,
        options: dict[str, str],
        allow_empty: bool = False,
        help: str = None,
        default: str = '',
) -> str:
    """Ask user for input

    Parameters
    ----------
    message
        Message.
    options
        ``{command: description}`` mapping.
    allow_empty
        Allow empty string as command.
    help
        If provided, add a "help" option that prints ``help``.
    default
        Default answer; implies ``allow_empty``, but will substitute ``default``
        when user input is the empty string.

    Returns
    -------
    command : str
        The user command.
    """
    options = dict(options)
    if help is not None:
        assert 'help' not in options
        options['help'] = 'display help'
    if default:
        options[default] += ' (default)'
        allow_empty = True
    print(message)
    print('---')
    print('\n'.join(f'{k}:  {v}' for k, v in options.items()))
    while True:
        command = input(" > ") or default
        if command in options or (allow_empty and not command):
            if help is not None and command == 'help':
                print(help)
            else:
                return command
        else:
            print(f"Invalid entry - type one of ({', '.join(options)})")