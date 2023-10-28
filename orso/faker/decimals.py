import decimal
import random


def generate_random_decimal(precision: int, scale: int) -> decimal.Decimal:
    """
    Generates a random decimal number based on the given precision and scale.

    Parameters:
        precision: int
            The total number of digits in the decimal number.

        scale: int
            The number of digits to the right of the decimal point.

    Returns:
        Decimal: Randomly generated decimal number.
    """
    if scale > precision:
        raise ValueError("Scale can't be greater than precision")

    int_part_len = precision - scale

    int_part = "".join(str(random.randint(0, 9)) for _ in range(int_part_len))
    frac_part = "".join(str(random.randint(0, 9)) for _ in range(scale))

    # Ensure the integer part is not all zeros.
    if all(d == "0" for d in int_part):  # pragma: no cover
        int_part = str(random.randint(1, 9)) + int_part[1:]

    return decimal.Decimal(f"{int_part}.{frac_part}")
