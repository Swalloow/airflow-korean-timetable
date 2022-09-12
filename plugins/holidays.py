from dataclasses import dataclass

from pendulum import Date


@dataclass(frozen=True)
class Holidays:
    date: Date
    days: int = 1


holidays = [
    Holidays(date=Date(2022, 8, 15)),
    Holidays(date=Date(2022, 9, 9), days=4),
    Holidays(date=Date(2022, 10, 3)),
    Holidays(date=Date(2022, 10, 9)),
    Holidays(date=Date(2022, 12, 25)),
    Holidays(date=Date(2023, 1, 21), days=4),
    Holidays(date=Date(2023, 3, 1)),
    Holidays(date=Date(2023, 5, 5)),
]
