AngularJS version of the tabs directive.

### Settings ###

#### `<tabset>` ####

 * `vertical`
 	_(Defaults: false)_ :
 	Whether tabs appear vertically stacked.

 * `justified`
 	_(Defaults: false)_ :
 	Whether tabs fill the container and have a consistent width.

 * `type`
 	_(Defaults: 'tabs')_ :
 	Navigation type. Possible values are 'tabs' and 'pills'.

#### `<tab>` ####

 * `heading` or `<tab-heading>`
 	:
 	Heading text or HTML markup.

 * `active` <i class="glyphicon glyphicon-eye-open"></i>
 	_(Defaults: false)_ :
 	Whether tab is currently selected.

 * `disabled` <i class="glyphicon glyphicon-eye-open"></i>
 	_(Defaults: false)_ :
 	Whether tab is clickable and can be activated.

 * `select()`
 	_(Defaults: null)_ :
 	An optional expression called when tab is activated.
    
 * `deselect()`
 	_(Defaults: null)_ :
 	An optional expression called when tab is deactivated.