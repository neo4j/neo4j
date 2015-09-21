bootstrap-slider [![Build Status](https://travis-ci.org/seiyria/bootstrap-slider.png?branch=master)](https://travis-ci.org/seiyria/bootstrap-slider)
================
Originally began as a loose "fork" of bootstrap-slider found on http://www.eyecon.ro/ by Stefan Petre.

Over time, this project has diverged sigfinicantly from Stefan Petre's version and is now almost completly different.

__Please ensure that you are using this library instead of the Petre version before creating issues in the repository Issue tracker!!__

Installation
============
Clone the repository, then run `npm install`

Want to use bower? `bower install seiyria-bootstrap-slider`

Want to use npm? `npm install bootstrap-slider`

__NOTE for NPM users__: In order to keep the version numbers in our dist/ file consistent with our Github tags, we do a patch version bump, generate a new dist, and create a commit/tag on postpublish.

This does mean the Github repo will always be one patch commit off of what is published to NPM. Note that this will not affect functionality, and is only used to keep package management system files and the dist file version numbers in sync.

Basic Setup
============
Load the plugin CSS and JavaScript into your web page, and everything should work!

Remember to load the plugin code after loading the Bootstrap CSS and JQuery.

__JQuery is optional and the plugin can operate with or without it.__

Look below to see an example of how to interact with the non-JQuery interface.

Supported Browsers
========
__We only support modern browsers!!! Basically, anything below IE9 is not compatible with this plugin!__

Examples
========
You can see all of our API examples [here](http://seiyria.github.io/bootstrap-slider/).

Using bootstrap-slider (with JQuery)
======================

Create an input element and call .slider() on it:

```js
// Instantiate a slider
var mySlider = $("input.slider").slider();

// Call a method on the slider
var value = mySlider.slider('getValue');

// For non-getter methods, you can chain together commands
	mySlider
		.slider('setValue', 5)
		.slider('setValue', 7);
```

What if there is already a _slider_ plugin bound to the JQuery namespace?
======================

If there is already a JQuery plugin named _slider_ bound to the JQuery namespace, then this plugin will take on the alternate namespace _bootstrapSlider_.

```
// Instantiate a slider
var mySlider = $("input.slider").bootstrapSlider();

// Call a method on the slider
var value = mySlider.bootstrapSlider('getValue');

// For non-getter methods, you can chain together commands
	mySlider
		.bootstrapSlider('setValue', 5)
		.bootstrapSlider('setValue', 7);
```

Using bootstrap-slider (without JQuery)
======================

Create an input element in the DOM, and then create an instance of Slider, passing it a selector string referencing the input element.

```js
// Instantiate a slider
var mySlider = new Slider("input.slider", {
	// initial options object
});

// Call a method on the slider
var value = mySlider.getValue();

// For non-getter methods, you can chain together commands
mySlider
	.setValue(5)
	.setValue(7);
```

Using as CommonJS module
=======
bootstrap-slider can be loaded as a CommonJS module via [Browserify](https://github.com/substack/node-browserify), [Webpack](https://github.com/webpack/webpack), or some other build tool.

```
var Slider = require("bootstrap-slider");

var mySlider = new Slider();
```

Note that the JQuery dependency is considered to be optional. For example, to exclude JQuery from being part of your Browserify build, you would call something like the following (assuming main.js is requiring bootstrap-slider as a dependency):

```
browserify -u jquery main.js > bundle.js
```

Please see the documentation for the specific CommonJS loader you are using to find out how to exclude dependencies.

Options
=======
Options can be passed either as a data (data-slider-foo) attribute, or as part of an object in the slider call. The only exception here is the formatter argument - that can not be passed as a data- attribute.


| Name | Type |	Default |	Description |
| ---- |:----:|:-------:|:----------- |
| id | string | '' | set the id of the slider element when it's created |
| min |	float	| 0 |	minimum possible value |
| max |	float |	10 |	maximum possible value |
| step | float |	1 |	increment step |
| precision | float |	number of digits after the decimal of _step_ value |	The number of digits shown after the decimal. Defaults to the number of digits after the decimal of _step_ value. |
| orientation |	string | 'horizontal' |	set the orientation. Accepts 'vertical' or 'horizontal' |
| value |	float,array |	5	| initial value. Use array to have a range slider. |
| range |	bool |	false	| make range slider. Optional if initial value is an array. If initial value is scalar, max will be used for second value. |
| selection |	string |	'before' |	selection placement. Accepts: 'before', 'after' or 'none'. In case of a range slider, the selection will be placed between the handles |
| tooltip |	string |	'show' |	whether to show the tooltip on drag, hide the tooltip, or always show the tooltip. Accepts: 'show', 'hide', or 'always' |
| tooltip_split |	bool |	false |	if false show one tootip if true show two tooltips one for each handler |
| tooltip_position |	string |	null |	Position of tooltip, relative to slider. Accepts 'top'/'bottom' for horizontal sliders and 'left'/'right' for vertically orientated sliders. Default positions are 'top' for horizontal and 'right' for vertical slider. |
| handle |	string |	'round' |	handle shape. Accepts: 'round', 'square', 'triangle' or 'custom' |
| reversed | bool | false | whether or not the slider should be reversed |
| enabled | bool | true | whether or not the slider is initially enabled |
| formatter |	function |	returns the plain value |	formatter callback. Return the value wanted to be displayed in the tooltip |
| natural_arrow_keys | bool | false | The natural order is used for the arrow keys. Arrow up select the upper slider value for vertical sliders, arrow right the righter slider value for a horizontal slider - no matter if the slider was reversed or not. By default the arrow keys are oriented by arrow up/right to the higher slider value, arrow down/left to the lower slider value. |
| ticks | array | [ ] | Used to define the values of ticks. Tick marks are indicators to denote special values in the range. This option overwrites min and max options. |
| ticks_positions | array | [ ] | Defines the positions of the tick values in percentages. The first value should always be 0, the last value should always be 100 percent. |
| ticks_labels | array | [ ] | Defines the labels below the tick marks. Accepts HTML input. |
| ticks_snap_bounds | float | 0 | Used to define the snap bounds of a tick. Snaps to the tick if value is within these bounds. |
| scale | string | 'linear' | Set to 'logarithmic' to use a logarithmic scale. |
| focus | bool | false | Focus the appropriate slider handle after a value change. |

Functions
=========
__NOTE:__ Optional parameters are italicized.

| Function | Parameters | Description |
| -------- | ----------- | ----------- |
| getValue | --- | Get the current value from the slider |
| setValue | newValue, _triggerSlideEvent_, _triggerChangeEvent_ | Set a new value for the slider. If optional triggerSlideEvent parameter is _true_, 'slide' events will be triggered. If optional triggerChangeEvent parameter is _true_, 'change' events will be triggered. This function takes `newValue` as either a `Number` or `Array`.|
| getElement | --- | Get the div slider element |
| destroy | --- | Properly clean up and remove the slider instance |
| disable | ---| Disables the slider and prevents the user from changing the value |
| enable | --- | Enables the slider |
| toggle | --- | Toggles the slider between enabled and disabled |
| isEnabled | --- |Returns true if enabled, false if disabled |
| setAttribute | attribute, value | Updates the slider's [attributes](#options) |
| getAttribute | attribute | Get the slider's [attributes](#options) |
| refresh | --- | Refreshes the current slider |
| on | eventType, callback | When the slider event _eventType_ is triggered, the callback function will be invoked |
| off | eventType, callback | Removes the callback function from the slider event _eventType_ |
| relayout | --- | Renders the tooltip again, after initialization. Useful in situations when the slider and tooltip are initially hidden. |

Events
======
| Event | Description | Value |
| ----- | ----------- | ----- |
| slide | This event fires when the slider is dragged | The new slider value |
| slideStart | This event fires when dragging starts | The new slider value |
| slideStop | This event fires when the dragging stops or has been clicked on | The new slider value |
| change | This event fires when the slider value has changed | An object with 2 properties: "oldValue" and "newValue" |
| slideEnabled | This event fires when the slider is enabled | N/A |
| slideDisabled | This event fires when the slider is disabled | N/A |

Version Bumping and Publishing (Maintainers Only)
=======
To bump the version number across all the various packagement systems the plugin is registered with, please use the [grunt bump](https://github.com/vojtajina/grunt-bump) plugin.

* _grunt bump:patch_ - patch version bump, __0.0.0 -> 0.0.1__
* _grunt bump:minor_ - minor version bump, __0.0.0 -> 0.1.0__
* _grunt bump:major_ - major version bump, __0.0.0 -> 1.0.0__

After bumping, type `npm publish` to update on NPM.


Other Platforms & Libraries
===========================
- [Ruby on Rails](https://github.com/YourCursus/bootstrap-slider-rails)
- [knockout.js](https://github.com/cosminstefanxp/bootstrap-slider-knockout-binding) ([@cosminstefanxp](https://github.com/cosminstefanxp), [#81](https://github.com/seiyria/bootstrap-slider/issues/81))
- [AngularJS](https://github.com/seiyria/angular-bootstrap-slider)
- [EmberJS](https://github.com/lifegadget/ui-slider) ([@ksnyde](https://github.com/ksnyde))
- [NuGet](https://www.nuget.org/packages/bootstrap-slider/) ([@ChrisMissal](https://github.com/ChrisMissal))
- [MeteorJS](https://github.com/kidovate/meteor-bootstrap-slider)
- [Maven](http://mvnrepository.com/artifact/org.webjars.bower/seiyria-bootstrap-slider)

Maintainers
============
- Kyle Kemp
	* Twitter: [@seiyria](https://twitter.com/seiyria)
	* Github: [seiyria](https://github.com/seiyria)
- Rohit Kalkur
	* Twitter: [@Rovolutionary](https://twitter.com/Rovolutionary)
	* Github: [rovolution](https://github.com/rovolution)
