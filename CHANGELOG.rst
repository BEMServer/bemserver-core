Changelog
---------

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
