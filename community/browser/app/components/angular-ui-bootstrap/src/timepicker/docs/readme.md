A lightweight & configurable timepicker directive.

### Settings ###

All settings can be provided as attributes in the `<timepicker>` or globally configured through the `timepickerConfig`.

 * `ng-model` <i class="glyphicon glyphicon-eye-open"></i>
 	:
 	The Date object that provides the time state.

 * `hour-step` <i class="glyphicon glyphicon-eye-open"></i>
 	_(Defaults: 1)_ :
 	 Number of hours to increase or decrease when using a button.

 * `minute-step` <i class="glyphicon glyphicon-eye-open"></i>
 	_(Defaults: 1)_ :
 	 Number of minutes to increase or decrease when using a button.

 * `show-meridian` <i class="glyphicon glyphicon-eye-open"></i>
 	_(Defaults: true)_ :
 	Whether to display 12H or 24H mode.

 * `meridians`
 	_(Defaults: null)_ :
 	 Meridian labels based on locale. To override you must supply an array like ['AM', 'PM'].

 * `readonly-input`
 	_(Defaults: false)_ :
 	 Whether user can type inside the hours & minutes input.

 * `mousewheel`
 	_(Defaults: true)_ :
 	 Whether user can scroll inside the hours & minutes input to increase or decrease it's values.
