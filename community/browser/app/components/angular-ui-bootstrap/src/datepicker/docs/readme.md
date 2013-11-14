A clean, flexible, and fully customizable date picker.

User can navigate through months and years.
The datepicker shows dates that come from other than the main month being displayed. These other dates are also selectable.

Everything is formatted using the [date filter](http://docs.angularjs.org/api/ng.filter:date) and thus is also localized.

### Settings ###

All settings can be provided as attributes in the `<datepicker>` or globally configured through the `datepickerConfig`.

 * `ng-model` <i class="icon-eye-open"></i>
 	:
 	The date object.

 * `show-weeks` <i class="icon-eye-open"></i>
 	_(Defaults: true)_ :
 	Whether to display week numbers.

 * `starting-day`
 	_(Defaults: 0)_ :
 	Starting day of the week from 0-6 (0=Sunday, ..., 6=Saturday).

 * `min` <i class="icon-eye-open"></i>
 	_(Default: null)_ :
 	Defines the minimum available date.

 * `max` <i class="icon-eye-open"></i>
 	_(Default: null)_ :
 	Defines the maximum available date.

 * `date-disabled (date, mode)`
 	_(Default: null)_ :
 	An optional expression to disable visible options based on passing date and current mode _(day|month|year)_.

 * `day-format`
 	_(Default: 'dd')_ :
 	Format of day in month.

 * `month-format`
 	_(Default: 'MMMM')_ :
 	Format of month in year.

 * `year-format`
 	_(Default: 'yyyy')_ :
 	Format of year in year range.

 * `year-range`
 	_(Default: 20)_ :
 	Number of years displayed in year selection.

 * `day-header-format`
 	_(Default: 'EEE')_ :
 	Format of day in week header.

 * `day-title-format`
 	_(Default: 'MMMM yyyy')_ :
 	Format of title when selecting day.

 * `month-title-format`
 	_(Default: 'yyyy')_ :
 	Format of title when selecting month.
