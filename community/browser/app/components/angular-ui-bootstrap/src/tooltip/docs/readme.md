A lightweight, extensible directive for fancy tooltip creation. The tooltip
directive supports multiple placements, optional transition animation, and more.

There are two versions of the tooltip: `tooltip` and `tooltip-html-unsafe`. The
former takes text only and will escape any HTML provided. The latter takes
whatever HTML is provided and displays it in a tooltip; it called "unsafe"
because the HTML is not sanitized. *The user is responsible for ensuring the
content is safe to put into the DOM!*

The tooltip directives provide several optional attributes to control how they
will display:

- `tooltip-placement`: Where to place it? Defaults to "top", but also accepts
  "bottom", "left", "right".
- `tooltip-animation`: Should it fade in and out? Defaults to "true".
- `tooltip-popup-delay`: For how long should the user have to have the mouse
  over the element before the tooltip shows (in milliseconds)? Defaults to 0.
- `tooltip-trigger`: What should trigger a show of the tooltip?
- `tooltip-append-to-body`: Should the tooltip be appended to `$body` instead of
  the parent element?

The tooltip directives require the `$position` service.

**Triggers**

The following show triggers are supported out of the box, along with their
provided hide triggers:

- `mouseenter`: `mouseleave`
- `click`: `click`
- `focus`: `blur`

For any non-supported value, the trigger will be used to both show and hide the
tooltip.

**$tooltipProvider**

Through the `$tooltipProvider`, you can change the way tooltips and popovers
behave by default; the attributes above always take precedence. The following
methods are available:

- `setTriggers( obj )`: Extends the default trigger mappings mentioned above
  with mappings of your own. E.g. `{ 'openTrigger': 'closeTrigger' }`.
- `options( obj )`: Provide a set of defaults for certain tooltip and popover
  attributes. Currently supports 'placement', 'animation', 'popupDelay', and
  `appendToBody`. Here are the defaults:

  <pre>
  placement: 'top',
  animation: true,
  popupDelay: 0,
  appendToBody: false
  </pre>

