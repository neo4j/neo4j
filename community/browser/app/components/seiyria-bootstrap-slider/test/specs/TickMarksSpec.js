/*
	*************************

	Tick Marks Tests

	*************************

    Verify that the number of tick marks matches what you set
    Verify the tick marks are at the correct intervals


*/
describe("Slider with ticks tests", function() {

	var testSlider;

	it("Should have the number of tick marks you specify", function() {
		testSlider = $("#testSlider1").slider({
			ticks: [100, 200, 300, 400, 500]
		});

		var numTicks = $("#testSlider1").siblings('div.slider').find('.slider-tick').length;
		expect(numTicks).toBe(5);
	});

	it("Should be at the default positions", function() {
		testSlider = $("#testSlider1").slider({
			ticks: [100, 200, 300, 400, 500]
		});

		$("#testSlider1").siblings('div.slider').find('.slider-tick').each(function(i) {
			expect(this.style.left).toBe(100 * i / 4.0 + '%');
		});
	});

	it("Should be at the positions you specify", function() {
		var tickPositions = [0, 10, 20, 30, 100];
		testSlider = $("#testSlider1").slider({
			ticks: [100, 200, 300, 400, 500],
			ticks_positions: tickPositions
		});

		$("#testSlider1").siblings('div.slider').find('.slider-tick').each(function(i) {
			expect(this.style.left).toBe(tickPositions[i] + '%');
		});
	});

	it("Should have the tick labels you specify", function() {
		var tickLabels = ['$0', '$100', '$200', '$300', '$400'];
		testSlider = $("#testSlider1").slider({
			ticks: [100, 200, 300, 400, 500],
		    ticks_labels: tickLabels
		});

		var tickLabelElements = $("#testSlider1").siblings('div.slider').find('.slider-tick-label');
		expect(tickLabelElements.length).toBe(tickLabels.length);
		tickLabelElements.each(function(i) {
			expect(this.innerHTML).toBe(tickLabels[i]);
		});
	});

	it("Should overwrite the min/max values", function() {
		testSlider = $("#testSlider1").slider({
			ticks: [100, 200, 300, 400, 500],
			min: 15000,
			max: 25000
		});

		expect(testSlider.slider('getAttribute','min')).toBe(100);
		expect(testSlider.slider('getAttribute','max')).toBe(500);
	});

	it("Should not snap to a tick within tick bounds when using the keyboard navigation", function() {
		testSlider = $("#testSlider1").slider({
			ticks: [100, 200, 300, 400, 500],
			ticks_snap_bounds: 30
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
			expect(value).toBe(100 + keyPresses);
		});

		keyboardEvent.keyCode = keyboardEvent.which = 39; // RIGHT
		for (var i = 0; i < 5; i++) {
			handle1[0].dispatchEvent(keyboardEvent);
		}
	});

	it("Should show the correct tick marks as 'in-selection', according to the `selection` property", function() {
		var options = {
			ticks: [100, 200, 300, 400, 500],
			value: 250,
			selection: 'after'
		},
		$el = $("#testSlider1");

		testSlider = $el.slider(options);
		expect($el.siblings('div.slider').find('.in-selection').length).toBe(3);

		testSlider.slider('destroy');

		options.selection = 'before';
		testSlider = $el.slider(options);
		expect($el.siblings('div.slider').find('.in-selection').length).toBe(2);
});

	afterEach(function() {
	    if(testSlider) {
	      testSlider.slider('destroy');
	      testSlider = null;
	    }
  	});
});
