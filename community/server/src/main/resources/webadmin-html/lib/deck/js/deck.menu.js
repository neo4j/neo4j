/*!
Deck JS - deck.menu
Copyright (c) 2011 Caleb Troughton
Dual licensed under the MIT license and GPL license.
https://github.com/imakewebthings/deck.js/blob/master/MIT-license.txt
https://github.com/imakewebthings/deck.js/blob/master/GPL-license.txt
*/

/*
This module adds the methods and key binding to show and hide a menu of all
slides in the deck. The deck menu state is indicated by the presence of a class
on the deck container.
*/
(function($, deck, undefined) {
	var $d = $(document),
	rootSlides; // Array of top level slides
	
	/*
	Extends defaults/options.
	
	options.classes.menu
		This class is added to the deck container when showing the slide menu.
	
	options.keys.menu
		The numeric keycode used to toggle between showing and hiding the slide
		menu.
		
	options.touch.doubletapWindow
		Two consecutive touch events within this number of milliseconds will
		be considered a double tap, and will toggle the menu on touch devices.
	*/
	$.extend(true, $[deck].defaults, {
		classes: {
			menu: 'deck-menu'
		},
		
		keys: {
			menu: 77 // m
		},
		
		touch: {
			doubletapWindow: 400
		}
	});

	/*
	jQuery.deck('showMenu')
	
	Shows the slide menu by adding the class specified by the menu class option
	to the deck container.
	*/
	$[deck]('extend', 'showMenu', function() {
		var $c = $[deck]('getContainer'),
		opts = $[deck]('getOptions');
		
		if ($c.hasClass(opts.classes.menu)) return;
		
		// Hide through loading class to short-circuit transitions (perf)
		$c.addClass([opts.classes.loading, opts.classes.menu].join(' '));
		
		/* Forced to do this in JS until CSS learns second-grade math. Save old
		style value for restoration when menu is hidden. */
		if (Modernizr.csstransforms) {
			$.each(rootSlides, function(i, $slide) {
				$slide.data('oldStyle', $slide.attr('style'));
				$slide.css({
					'position': 'absolute',
					'left': ((i % 4) * 25) + '%',
					'top': (Math.floor(i / 4) * 25) + '%'
				});
			});
		}
		
		// Need to ensure the loading class renders first, then remove
		window.setTimeout(function() {
			$c.removeClass(opts.classes.loading)
				.scrollTop($[deck]('getSlide').offset().top);
		}, 0);
	});

	/*
	jQuery.deck('hideMenu')
	
	Hides the slide menu by removing the class specified by the menu class
	option from the deck container.
	*/
	$[deck]('extend', 'hideMenu', function() {
		var $c = $[deck]('getContainer'),
		opts = $[deck]('getOptions');
		
		if (!$c.hasClass(opts.classes.menu)) return;
		
		$c.removeClass(opts.classes.menu);
		$c.addClass(opts.classes.loading);
		
		/* Restore old style value */
		if (Modernizr.csstransforms) {
			$.each(rootSlides, function(i, $slide) {
				var oldStyle = $slide.data('oldStyle');

				$slide.attr('style', oldStyle ? oldStyle : '');
			});
		}
		
		window.setTimeout(function() {
			$c.removeClass(opts.classes.loading).scrollTop(0);
		}, 0);
	});

	/*
	jQuery.deck('toggleMenu')
	
	Toggles between showing and hiding the slide menu.
	*/
	$[deck]('extend', 'toggleMenu', function() {
		$[deck]('getContainer').hasClass($[deck]('getOptions').classes.menu) ?
		$[deck]('hideMenu') : $[deck]('showMenu');
	});

	$d.bind('deck.init', function() {
		var opts = $[deck]('getOptions'),
		touchEndTime = 0,
		currentSlide,
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
		
		// Bind key events
		$d.unbind('keydown.deckmenu').bind('keydown.deckmenu', function(e) {
			if (e.which === opts.keys.menu || $.inArray(e.which, opts.keys.menu) > -1) {
				$[deck]('toggleMenu');
				e.preventDefault();
			}
		});
		
		// Double tap to toggle slide menu for touch devices
		$[deck]('getContainer').unbind('touchstart.deckmenu').bind('touchstart.deckmenu', function(e) {
			currentSlide = $[deck]('getSlide');
		})
		.unbind('touchend.deckmenu').bind('touchend.deckmenu', function(e) {
			var now = Date.now();
			
			// Ignore this touch event if it caused a nav change (swipe)
			if (currentSlide !== $[deck]('getSlide')) return;
			
			if (now - touchEndTime < opts.touch.doubletapWindow) {
				$[deck]('toggleMenu');
				e.preventDefault();
			}
			touchEndTime = now;
		});
		
		// Selecting slides from the menu
		$.each($[deck]('getSlides'), function(i, $s) {
			$s.unbind('click.deckmenu').bind('click.deckmenu', function(e) {
				if (!$[deck]('getContainer').hasClass(opts.classes.menu)) return;

				$[deck]('go', i);
				$[deck]('hideMenu');
				e.stopPropagation();
				e.preventDefault();
			});
		});
	})
	.bind('deck.change', function(e, from, to) {
		var container = $[deck]('getContainer');
		
		if (container.hasClass($[deck]('getOptions').classes.menu)) {
			container.scrollTop($[deck]('getSlide', to).offset().top);
		}
	});
})(jQuery, 'deck');

