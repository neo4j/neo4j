Rating directive that will take care of visualising a star rating bar.

### Settings ###

#### `<rating>` ####

 * `ng-model` <i class="glyphicon glyphicon-eye-open"></i>
 	:
 	The current rate.

 * `max`
 	_(Defaults: 5)_ :
 	Changes the number of icons.

 * `readonly` <i class="icon-eye-open"></i>
 	_(Defaults: false)_ :
 	Prevent user's interaction.

 * `on-hover(value)`
 	:
 	An optional expression called when user's mouse is over a particular icon.

 * `on-leave()`
 	:
 	An optional expression called when user's mouse leaves the control altogether.

 * `state-on`
 	_(Defaults: null)_ :
 	A variable used in template to specify the state (class, src, etc) for selected icons.

 * `state-off`
 	_(Defaults: null)_ :
 	A variable used in template to specify the state for unselected icons.

 * `rating-states`
 	_(Defaults: null)_ :
 	An array of objects defining properties for all icons. In default template, `stateOn` & `stateOff` property is used to specify the icon's class.
