Changelog
---------

0.18.4 (2024-01-18)
+++++++++++++++++++

Bug fixes:

- Fix energyindex2energy: divide by interval

0.18.3 (2024-09-09)
+++++++++++++++++++

Bug fixes:

- Fix filtering timeseries by multiple properties (again)

0.18.2 (2024-09-06)
+++++++++++++++++++

Bug fixes:

- Fix filtering timeseries by multiple properties

0.18.1 (2024-07-10)
+++++++++++++++++++

Features:

- Allow filtering by property data when querying timeseries

0.18.0 (2024-06-05)
+++++++++++++++++++

Features:

- Remove bemserver_core.__version__

Other changes:

- Require pint>=0.23
- Change license to MIT

0.17.1 (2024-02-12)
+++++++++++++++++++

Other changes:

- Support Python 3.12

0.17.0 (2024-02-12)
+++++++++++++++++++

Features:

- Rehash password on login if needed

Other changes:

- Remove passlib dependency
- Require pandas >= 2.2.0

0.16.7 (2023-09-25)
+++++++++++++++++++

Bug fixes:

- Don't forward fill interpolate after last value in indexenergy2power/energy

Other changes:

- Support Python 3.11

0.16.6 (2023-09-15)
+++++++++++++++++++

Features:

- Completeness: include full buckets on edges

0.16.5 (2023-09-08)
+++++++++++++++++++

Features:

- Improve missing/outlier data event description

0.16.4 (2023-09-06)
+++++++++++++++++++

Features:

- Improve notification email subject

0.16.3 (2023-09-06)
+++++++++++++++++++

Bug fixes:

- CSV import: catch too many columns error
- Bulk insert: don't use fixed VALUES clause
- Email: use send_message, not sendmail

0.16.2 (2023-07-25)
+++++++++++++++++++

Bug fixes:

- Enforce model (reanalysis vs. forecast) when getting weather data

0.16.1 (2023-06-20)
+++++++++++++++++++

Bug fixes:

- Validate unit on Timeseries flush

0.16.0 (2023-06-09)
+++++++++++++++++++

Features:

- Add energy <=> power conversion processes
- Add send email feature
- Send email on notification

Other changes:

- Require psycopg 3.x

0.15.4 (2023-05-26)
+++++++++++++++++++

Bug fixes:

- Forward fill process: ceil start_dt to respect bucket width parameters

0.15.3 (2023-05-26)
+++++++++++++++++++

Bug fixes:

- Fix DownloadWeatherData and DownloadWeatherForecastData tasks names

0.15.2 (2023-05-23)
+++++++++++++++++++

Bug fixes:

- Rollback session on end of task

Other changes:

- Remove official Python 3.11 support

0.15.1 (2023-05-22)
+++++++++++++++++++

Features:

- Add forward fill process
- Add TimeseriesDataIO.get_last
- Add unit and ratio arguments to energy consumption breakdown process
- Add get_property_value method to Site/Building/...

Bug fixes:

- Fix energy consumption breakdown computation crash on timeseries duplicate

0.15.0 (2023-05-05)
+++++++++++++++++++

Features:

- Download forecast weather feature

0.14.0 (2023-05-05)
+++++++++++++++++++

Features:

- Weather: differentiate forecast data

0.13.5 (2023-05-02)
+++++++++++++++++++

Features:

- Add "ratio" unit

Bug fixes:

- TimeseriesDataJSONIO: catch wrong value type error
- Fix conversion to "%"

Other changes:

- Require Pint 0.21

0.13.4 (2023-04-21)
+++++++++++++++++++

Features:

- TimeseriesDataIO TS stats: add count

0.13.3 (2023-04-21)
+++++++++++++++++++

Features:

- TimeseriesDataIO: TS stats

0.13.2 (2023-04-18)
+++++++++++++++++++

Features:

- Add Heating/Cooling Degree Days computation process
- Add BEMServerCoreUnitError base exception for unit errors

Bug fixes:

- CSV IO: don't crash on empty file

0.13.1 (2023-04-12)
+++++++++++++++++++

Features:

- Weather data download: catch API key error

0.13.0 (2023-04-11)
+++++++++++++++++++

Features:

- Rework session management: only commit in tasks and commands
- Add latitude and longitude to Site
- Add convert_from argument to TimeseriesDataIO and TimeseriesDataCSV/JSONIO
- Add weather data download feature: Oikolab client, model, process, task
- Ignore underscore variables in config files

Bug fixes:

- Catch DefinitionSyntaxError in BEMServerUnitRegistry.validate_unit
- Scheduled task: skip if is_enabled is False

Other changes:

- Require Pandas 2.x
- Require SQLAlchemy 2.x
- Set upper bound to requirements versions in setup.py

0.12.0 (2023-03-14)
+++++++++++++++++++

Features:

- Load configuration from Python file provided by BEMSERVER_CORE_SETTINGS_FILE
  environment variable
- Init authentication at BEMServerCore init
- Add direct/diffuse solar radiation to WeatherParameterEnum

0.11.1 (2023-03-03)
+++++++++++++++++++

Bug fixes:

- TimeseriesDataCSV/JSONIO: catch OutOfBoundsDatetime when loading data

0.11.0 (2023-03-01)
+++++++++++++++++++

Features:

- Rename EnergySource -> Energy
- Add EnergyProductionTechnology
- Add EnergyProductionTimeseriesBySite/Building
- Add WeatherParameterEnum and WeatherTimeseriesBySite

0.10.1 (2023-02-28)
+++++++++++++++++++

Bug fixes:

- Add bemserver_core/common/units.txt to MANIFEST.in

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
