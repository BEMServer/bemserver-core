"""Heating / Cooling Degree Days computations"""
from bemserver_core.time_utils import PANDAS_PERIOD_ALIASES


def compute_hdd(air_temp, period="year", base=18):
    """Compute heating degree days

    :param Series air_temp: Outside air temperature
    :param string period: One of "day", "month", "year"
    :param int|float base: Base temperature

    :returns Series: Heating degree days

    Note: base unit must match air_temp unit
    """
    min_s = air_temp.resample("D").min()
    max_s = air_temp.resample("D").max()
    avg_s = (min_s + max_s) / 2
    hdd = (base - avg_s).clip(0).rename("hdd")
    return hdd.resample(PANDAS_PERIOD_ALIASES[period]).sum()


def compute_cdd(air_temp, period="year", base=18):
    """Compute cooling degree days

    :param Series air_temp: Outside air temperature
    :param string period: One of "day", "month", "year"
    :param int|float base: Base temperature

    :returns Series: Cooling degree days

    Note: base unit must match air_temp unit
    """
    min_s = air_temp.resample("D").min()
    max_s = air_temp.resample("D").max()
    avg_s = (min_s + max_s) / 2
    hdd = (avg_s - base).clip(0).rename("cdd")
    return hdd.resample(PANDAS_PERIOD_ALIASES[period]).sum()
