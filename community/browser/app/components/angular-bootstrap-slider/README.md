angular-bootstrap-slider
========================

This plugin was mostly put together quickly with the intent of using something that worked. It has zero test coverage. It is, however, registered on bower as `angular-bootstrap-slider`. Just include `slider.js` and use the package `ui.bootstrap-slider`. You will also need to include bootstrap-sliders CSS and JS.

Available Options
=================
See [bootstrap-slider](https://github.com/seiyria/bootstrap-slider) for examples and options.

Sample Usage
============
```html
<!-- it can be used as an element -->
<slider ng-model="sliders.sliderValue" min="testOptions.min" step="testOptions.step" max="testOptions.max" value="testOptions.value"></slider>

<!-- ..or an attribute -->
<span slider ng-model="sliders.secondSliderValue" min="minTest"></span>
```

Troubleshooting
============
#### Tooltips
If you Want to hide the tooltip on your slider (or define a value for the bootstrap-slider `data-slider-tooltip` options, such as "show", "hide" or "always"), you should use the `tooltip` attribute, like this :
```html
<!-- it can be used as an element -->
<slider ng-model="sliders.sliderValue" min="testOptions.min" step="testOptions.step" max="testOptions.max" value="testOptions.value" tooltip="hide"></slider>
```
But, if the `tooltip` attribute is in conflict with another angular directive, you can use the alternative `slider-tooltip` attribute :
```html
<!-- it can be used as an element -->
<slider ng-model="sliders.sliderValue" min="testOptions.min" step="testOptions.step" max="testOptions.max" value="testOptions.value" slider-tooltip="hide"></slider>
```
