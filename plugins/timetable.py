from typing import Optional

from airflow.plugins_manager import AirflowPlugin
from airflow.timetables.base import DagRunInfo, DataInterval, TimeRestriction, Timetable
from airflow.utils.log.logging_mixin import LoggingMixin
from holidays import holidays
from pendulum import DateTime, Time, constants


class BeforeWorkdayTimetable(Timetable, LoggingMixin):
    @property
    def summary(self) -> str:
        return "@beforework"

    def init_time(self, start: DateTime) -> DateTime:
        """Update to 00:00:00 UTC"""
        return start.set(hour=0, minute=0, second=0, tz="UTC")

    def skip_holidays(self, last_start: DateTime) -> DateTime:
        next_end = last_start.add(days=2).date()
        skip_days = 0

        for holiday in holidays:
            if next_end == holiday.date:
                skip_days = holiday.days
        return last_start.add(days=skip_days)

    def infer_manual_data_interval(self, run_after: DateTime) -> DataInterval:
        end = self.init_time(run_after)
        return DataInterval(start=end.subtract(days=1), end=end)

    def next_dagrun_info(
        self,
        *,
        last_automated_data_interval: Optional[DataInterval],
        restriction: TimeRestriction,
    ) -> Optional[DagRunInfo]:
        if last_automated_data_interval is not None:
            # There was a previous run on the regular schedule.
            last_start = last_automated_data_interval.start
            last_start = self.init_time(last_start)
            last_start = self.skip_holidays(last_start)
            last_start_weekday = last_start.day_of_week
            if constants.SUNDAY <= last_start_weekday < constants.THURSDAY:
                # Last run on Sunday through Thursday -- next is tomorrow.
                days = 1
            else:
                # Last run on Friday -- skip to next Monday.
                days = 7 - last_start_weekday
            next_start = last_start.add(days=days)
        else:
            # This is the first ever run on the regular schedule.
            next_start = restriction.earliest
            if next_start is None:
                # No start_date. Don't schedule.
                return None
            if not restriction.catchup:
                # If the DAG has catchup=False, today is the earliest to consider.
                next_start = max(next_start, self.init_time(DateTime.today()))
            elif next_start.time() != Time.min:
                # If earliest does not fall on midnight, skip to the next day.
                next_day = next_start.add(days=1)
                next_start = self.init_time(next_day)

            next_start = self.skip_holidays(next_start)
            next_start_weekday = next_start.day_of_week
            if next_start_weekday in (constants.FRIDAY, constants.SATURDAY):
                # If next end is in the weekend, go to next week.
                days = 7 - next_start_weekday
                next_start = next_start.add(days=days)
        if restriction.latest is not None and next_start > restriction.latest:
            return None  # Over the DAG's scheduled end; don't schedule.
        return DagRunInfo.interval(start=next_start, end=next_start.add(days=1))


class WorkdayTimetablePlugin(AirflowPlugin):
    name = "workday_timetable_plugin"
    timetables = [BeforeWorkdayTimetable]
