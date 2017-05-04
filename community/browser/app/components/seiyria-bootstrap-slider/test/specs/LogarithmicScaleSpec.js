
/*
	*************************

	Logarithmic Scale Tests

	*************************

*/
describe("Slider with logarithmic scale tests", function() {

	var testSlider;

	it("Should have the number of tick marks you specify", function() {
		testSlider = $("#testSlider1").slider({
			min: 0,
			max: 10000,
			scale: 'logarithmic',
			value: 100 // This should be at 50%
		});

		var expectedPostition = 210 / 2 + 'px';

		var handle = $("#testSlider1").siblings('div.slider').find('.min-slider-handle');
		expect(handle.css('left')).toBe(expectedPostition);
	});

	it("Should use step size when navigating the keyboard", function() {
		testSlider = $("#testSlider1").slider({
			min: 0,
			max: 10000,
			scale: 'logarithmic',
			value: 100,
			step: 5
		});

		// Focus on handle1
		var handle1 = $("#testSlider1").siblings('div.slider:first').find('.slider-handle');
		handle1.focus();

		// Create keyboard event
		var keyboardEvent = document.createEvent("Events");
		keyboardEvent.initEvent("keydown", true, true);

		var keyPresses = 0;
		handle1.on("keydown", function() {
			keyPresses++;
			var value = $("#testSlider1").slider('getValue');
			expect(value).toBe(100 + keyPresses*5);
		});

		keyboardEvent.keyCode = keyboardEvent.which = 39; // RIGHT
		for (var i = 0; i < 5; i++) {
			handle1[0].dispatchEvent(keyboardEvent);
		}
	});

	it("Step size should be honored with mouse movements", function() {
		testSlider = $("#testSlider1").slider({
			min: 50,
			max: 10000,
			scale: 'logarithmic',
			value: 100,
			step: 100
		});
		var mouse = document.createEvent('MouseEvents');
		var dataSlider = testSlider.data('slider');
		var pos = (dataSlider.sliderElem[dataSlider.sizePos] / 2 +
				   dataSlider._state.offset[dataSlider.stylePos]);
		mouse.initMouseEvent(
			'mousedown', true, true, window, 1, pos,
			dataSlider._state.offset['top'], pos, dataSlider._state.offset['top'],
			false, false, false, false, 0, null);
		dataSlider._mousedown(mouse);
		/* Precise center value would have be 707.  It should be rounded to
		 * 750. */
		expect(testSlider.slider('getValue')).toBe(750);
	});

	afterEach(function() {
	    if(testSlider) {
	      testSlider.slider('destroy');
	      testSlider = null;
	    }
  	});
});
