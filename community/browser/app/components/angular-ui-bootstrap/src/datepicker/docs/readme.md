A clean, flexible, and fully customizable date picker.

User can navigate through months and years.
The datepicker shows dates that come from other than the main month being displayed. These other dates are also selectable.

Everything is formatted using the [date filter](http://docs.angularjs.org/api/ng.filter:date) and thus is also localized.

### Datepicker Settings ###

All settings can be provided as attributes in the `datepicker` or globally configured through the `datepickerConfig`.

 * `ng-model` <i class="glyphicon glyphicon-eye-open"></i>
 	:
 	The date object.

 * `datepicker-mode` <i class="glyphicon glyphicon-eye-open"></i>
   _(Defaults: 'day')_ :
   Current mode of the datepicker _(day|month|year)_. Can be used to initialize datepicker to specific mode.

 * `min-date` <i class="glyphicon glyphicon-eye-open"></i>
 	_(Default: null)_ :
 	Defines the minimum available date.

 * `max-date` <i class="glyphicon glyphicon-eye-open"></i>
 	_(Default: null)_ :
 	Defines the maximum available date.

 * `date-disabled (date, mode)`
 	_(Default: null)_ :
 	An optional expression to disable visible options based on passing date and current mode _(day|month|year)_.

 * `show-weeks`
 	_(Defaults: true)_ :
 	Whether to display week numbers.

 * `starting-day`
 	_(Defaults: 0)_ :
 	Starting day of the week from 0-6 (0=Sunday, ..., 6=Saturday).

 * `init-date`
 	:
 	The initial date view when no model value is not specified.

 * `min-mode`
   _(Defaults: 'day')_ :
   Set a lower limit for mode.

 * `max-mode`
   _(Defaults: 'year')_ :
   Set an upper limit for mode.

 * `format-day`
 	_(Default: 'dd')_ :
 	Format of day in month.

 * `format-month`
 	_(Default: 'MMMM')_ :
 	Format of month in year.

 * `format-year`
 	_(Default: 'yyyy')_ :
 	Format of year in year range.

 * `format-day-header`
 	_(Default: 'EEE')_ :
 	Format of day in week header.

 * `format-day-title`
 	_(Default: 'MMMM yyyy')_ :
 	Format of title when selecting day.

 * `format-month-title`
 	_(Default: 'yyyy')_ :
 	Format of title when selecting month.

 * `year-range`
 	_(Default: 20)_ :
 	Number of years displayed in year selection.


### Popup Settings ###

Options for datepicker can be passed as JSON using the `datepicker-options` attribute.
Specific settings for the `datepicker-popup`, that can globally configured through the `datepickerPopupConfig`, are:

 * `datepicker-popup`
 	_(Default: 'yyyy-MM-dd')_ :
 	The format for displayed dates.

 * `show-button-bar`
 	_(Default: true)_ :
 	Whether to display a button bar underneath the datepicker.

 * `current-text`
 	_(Default: 'Today')_ :
 	The text to display for the current day button.

 * `clear-text`
 	_(Default: 'Clear')_ :
 	The text to display for the clear button.

 * `close-text`
 	_(Default: 'Done')_ :
 	The text to display for the close button.

 * `close-on-date-selection`
 	_(Default: true)_ :
 	Whether to close calendar when a date is chosen.

 * `datepicker-append-to-body`
  _(Default: false)_:
  Append the datepicker popup element to `body`, rather than inserting after `datepicker-popup`. For global configuration, use `datepickerPopupConfig.appendToBody`.

### Keyboard Support ###

Depending on datepicker's current mode, the date may reffer either to day, month or year. Accordingly, the term view reffers either to a month, year or year range.

 * `Left`: Move focus to the previous date. Will move to the last date of the previous view, if the current date is the first date of a view.
 * `Right`: Move focus to the next date. Will move to the first date of the following view, if the current date is the last date of a view.
 * `Up`: Move focus to the same column of the previous row. Will wrap to the appropriate row in the previous view.
 * `Down`: Move focus to the same column of the following row. Will wrap to the appropriate row in the following view.
 * `PgUp`: Move focus to the same date of the previous view. If that date does not exist, focus is placed on the last date of the month.
 * `PgDn`: Move focus to the same date of the following view. If that date does not exist, focus is placed on the last date of the month.
 * `Home`: Move to the first date of the view.
 * `End`: Move to the last date of the view.
 * `Enter`/`Space`: Select date.
 * `Ctrl`+`Up`: Move to an upper mode.
 * `Ctrl`+`Down`: Move to a lower mode.
 * `Esc`: Will close popup, and move focus to the input.