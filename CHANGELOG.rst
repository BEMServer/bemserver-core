Changelog
---------

0.10.0 (2023-02-28)
+++++++++++++++++++

Features:

- Add unit conversions, convert on-the-fly when getting timeseries data
- Remove wh_conversion_factor from EnergyConsumptionTimeseriesBySite/Building

Bug fixes:

- Fix migrations/env.py for SQLAlchemy 2.0

0.9.1 (2023-02-08)
++++++++++++++++++

Other changes:

- Reintroduce SQLAlchemy 1.4 support

0.9.0 (2023-02-07)
++++++++++++++++++

Features:

- BEMServerCoreCelery: get DB URL from config file rather than env var

Bug fixes:

- TimeseriesDataIO.get_*: fix columns order in returned dataframe, which fixes
  an issue with the completeness computation process

Other changes:

- Require SQLAlchemy 2.x

0.8.1 (2023-02-01)
++++++++++++++++++

Features:

- Add Notification.mark_all_as_read and Notification.get_count_by_campaign
- Add Notification campaign_id filter

Bug fixes:

- Make TimeseriesDataIODatetimeError child of TimeseriesDataIOError

0.8.0 (2023-01-17)
++++++++++++++++++

Features:

- Check outliers data scheduled task

0.7.0 (2023-01-11)
++++++++++++++++++

Features:

- Rework Timeseries event filter
- Rework Timeseries site,... filters
- Rework Event site,... filters

0.6.0(2023-01-06)
++++++++++++++++++

Features:

- Add Notification
- Add EventCategoryByUser
- Create notifications on event creation, asynchronously (Celery)

0.5.0 (2022-12-22)
++++++++++++++++++

Features:

- Split Timeseries site_id/... and event_id filters into separate functions
- Add Event campaign_id, user_id, timeseries_id and site_id/... filters

0.4.0 (2022-12-15)
++++++++++++++++++

Features:

- Replace EventLevel table with EventLevelEnum
- Add Timeseries.get event_id filter

0.3.0 (2022-12-09)
++++++++++++++++++

Features:

- Add EventBySite, EventByBuilding,...
- Fix tables relation and backref names for consistency
- Enable and fix SQLAlchemy 2.0 compatibilty warnings

Other changes:

- Fix CI to test Python 3.11


0.2.1 (2022-12-06)
++++++++++++++++++

Features:

- Event model
- Check missing data scheduled task

Other changes:

- Support Python 3.11

The migration revision for this release was named 0.3 by mistake.

0.2.0 (2022-11-30)
++++++++++++++++++

Features:

- Timeseries data IO: JSON I/O
- Timeseries data IO: improve error handling

0.1.0 (2022-11-18)
++++++++++++++++++

Features:

- User management
- Authorization layer (Oso)
- Timeseries data storage
- Site, building,... data model
- Completeness, cleanup and energy consumption processes
- Cleanup scheduled task (Celery)
