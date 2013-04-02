/*!
Deck JS - deck.scale
Copyright (c) 2011-2012 Caleb Troughton
Dual licensed under the MIT license and GPL license.
https://github.com/imakewebthings/deck.js/blob/master/MIT-license.txt
https://github.com/imakewebthings/deck.js/blob/master/GPL-license.txt
*/

/*
This module adds automatic scaling to the deck.  Slides are scaled down
using CSS transforms to fit within the deck container. If the container is
big enough to hold the slides without scaling, no scaling occurs. The user
can disable and enable scaling with a keyboard shortcut.

Note: CSS transforms may make Flash videos render incorrectly.  Presenters
that need to use video may want to disable scaling to play them.  HTML5 video
works fine.
*/
(function($, deck, window, undefined) {
	var $d = $(document),
	$w = $(window),
	baseHeight, // Value to scale against
	timer, // Timeout id for debouncing
	rootSlides,

	/*
	Internal function to do all the dirty work of scaling the slides.
	*/
	scaleDeck = function() {
		var opts = $[deck]('getOptions'),
		obh = opts.baseHeight,
		$container = $[deck]('getContainer'),
		baseHeight = obh ? obh : $container.height();

		// Scale each slide down if necessary (but don't scale up)
		$.each(rootSlides, function(i, $slide) {
			var slideHeight = $slide.innerHeight(),
			$scaler = $slide.find('.' + opts.classes.scaleSlideWrapper),
			scale = $container.hasClass(opts.classes.scale) ?
				baseHeight / slideHeight :
				1;
			
			$.each('Webkit Moz O ms Khtml'.split(' '), function(i, prefix) {
				if (scale === 1) {
					$scaler.css(prefix + 'Transform', '');
				}
				else {
					$scaler.css(prefix + 'Transform', 'scale(' + scale + ')');
				}
			});
		});
	}

	/*
	Extends defaults/options.

	options.classes.scale
		This class is added to the deck container when scaling is enabled.
		It is enabled by default when the module is included.
	
	options.classes.scaleSlideWrapper
		Scaling is done using a wrapper around the contents of each slide. This
		class is applied to that wrapper.

	options.keys.scale
		The numeric keycode used to toggle enabling and disabling scaling.

	options.baseHeight
		When baseHeight is falsy, as it is by default, the deck is scaled in
		proportion to the height of the deck container. You may instead specify
		a height as a number of px, and slides will be scaled against this
		height regardless of the container size.

	options.scaleDebounce
		Scaling on the browser resize event is debounced. This number is the
		threshold in milliseconds. You can learn more about debouncing here:
		http://unscriptable.com/index.php/2009/03/20/debouncing-javascript-methods/

	*/
	$.extend(true, $[deck].defaults, {
		classes: {
			scale: 'deck-scale',
			scaleSlideWrapper: 'deck-slide-scaler'
		},

		keys: {
			scale: 83 // s
		},

		baseHeight: null,
		scaleDebounce: 200
	});

	/*
	jQuery.deck('disableScale')

	Disables scaling and removes the scale class from the deck container.
	*/
	$[deck]('extend', 'disableScale', function() {
		$[deck]('getContainer').removeClass($[deck]('getOptions').classes.scale);
		scaleDeck();
	});

	/*
	jQuery.deck('enableScale')

	Enables scaling and adds the scale class to the deck container.
	*/
	$[deck]('extend', 'enableScale', function() {
		$[deck]('getContainer').addClass($[deck]('getOptions').classes.scale);
		scaleDeck();
	});

	/*
	jQuery.deck('toggleScale')

	Toggles between enabling and disabling scaling.
	*/
	$[deck]('extend', 'toggleScale', function() {
		var $c = $[deck]('getContainer');
		$[deck]($c.hasClass($[deck]('getOptions').classes.scale) ?
			'disableScale' : 'enableScale');
	});

	$d.bind('deck.init', function() {
		var opts = $[deck]('getOptions'),
		slideTest = $.map([
			opts.classes.before,
			opts.classes.previous,
			opts.classes.current,
			opts.classes.next,
			opts.classes.after
		], function(el, i) {
			return '.' + el;
		}).join(', ');
		
		// Build top level slides array
		rootSlides = [];
		$.each($[deck]('getSlides'), function(i, $el) {
			if (!$el.parentsUntil(opts.selectors.container, slideTest).length) {
				rootSlides.push($el);
			}
		});
		
		// Use a wrapper on each slide to handle content scaling
		$.each(rootSlides, function(i, $slide) {
			$slide.children().wrapAll('<div class="' + opts.classes.scaleSlideWrapper + '"/>');
		});

		// Debounce the resize scaling
		$w.unbind('resize.deckscale').bind('resize.deckscale', function() {
			window.clearTimeout(timer);
			timer = window.setTimeout(scaleDeck, opts.scaleDebounce);
		})
		// Scale once on load, in case images or something change layout
		.unbind('load.deckscale').bind('load.deckscale', scaleDeck);

		// Bind key events
		$d.unbind('keydown.deckscale').bind('keydown.deckscale', function(e) {
			if (e.which === opts.keys.scale || $.inArray(e.which, opts.keys.scale) > -1) {
				$[deck]('toggleScale');
				e.preventDefault();
			}
		});

		// Enable scale on init
		$[deck]('enableScale');
	});
})(jQuery, 'deck', this);

