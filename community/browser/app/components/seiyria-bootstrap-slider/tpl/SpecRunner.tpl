<!doctype html>
<html>
<head>
  <meta charset="utf-8">
  <title>Jasmine Spec Runner</title>
<% css.forEach(function(style){ %>
  <link rel="stylesheet" type="text/css" href="<%= style %>">
<% }) %>
	<style type="text/css">
		#low-high-slider-styled .slider-track-low
		{
			background: rgb(0, 255, 0);
		}

		#low-high-slider-styled .slider-track-high
		{
			background: rgb(255, 0, 0);
		}
	</style>
</head>
<body>
	<input id="testSliderGeneric" type="text"/>

	<!-- Slider used for PublicMethodsSpec and EventsSpec -->
	<input id="testSlider1" type="text"/>

	<input id="testSlider2" type="text"/>

	<!-- Note: Two input elements with class 'makeSlider' are required for tests to run properly -->
  <input class="makeSlider" type="text"/>
	<input class="makeSlider" type="text"/>

	<!-- Sliders used for ElementDataSttributesSpec -->
	<input id="minSlider" type="text" data-slider-min="5"/>

	<input id="maxSlider" type="text" data-slider-max="5"/>

	<input id="orientationSlider" type="text" data-slider-orientation="vertical"/>

	<input id="stepSlider" type="text" data-slider-step="5"/>

	<input id="precisionSlider" type="text" data-slider-precision="2"/>

	<input id="valueSlider" type="text" data-slider-value="5"/>

	<input id="sliderWithTickMarksAndLabels" type="text" data-slider-ticks="[0, 100, 200, 300, 400]" data-slider-ticks-labels='["$0", "$100", "$200", "$300", "$400"]'/>

	<input id="selectionSlider" type="text" data-slider-selection="after"/>

	<input id="tooltipSlider" type="text" data-slider-tooltip="hide"/>

	<input id="handleSlider" type="text" data-slider-handle="triangle"/>

  <input id="customHandleSlider" type="text" data-slider-handle="custom"/>

	<input id="reversedSlider" type="text" data-slider-reversed="true"/>

	<input id="disabledSlider" type="text" data-slider-enabled="false"/>

	<input id="changeOrientationSlider" type="text"/>

	<input id="makeRangeSlider" type="text"/>

	<div id="relayoutSliderContainer" style="display: none">
		<input id="relayoutSliderInput" type="text"/>
	</div>

	<% with (scripts) { %>
	  <% [].concat(jasmine, vendor, src, specs, reporters, start).forEach(function(script){ %>
	  <script src="<%= script %>"></script>
	  <% }) %>
	<% }; %>
</body>
</html>
