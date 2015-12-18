A lightweight, extensible directive for fancy popover creation. The popover
directive supports multiple placements, optional transition animation, and more.

Like the Bootstrap jQuery plugin, the popover **requires** the tooltip
module.

The popover directives provides several optional attributes to control how it
will display:

- `popover-title`: A string to display as a fancy title.
- `popover-placement`: Where to place it? Defaults to "top", but also accepts
  "bottom", "left", "right".
- `popover-animation`: Should it fade in and out? Defaults to "true".
- `popover-popup-delay`: For how long should the user have to have the mouse
  over the element before the popover shows (in milliseconds)? Defaults to 0.
- `popover-trigger`: What should trigger the show of the popover? See the
  `tooltip` directive for supported values.
- `popover-append-to-body`: Should the tooltip be appended to `$body` instead of
  the parent element?

The popover directives require the `$position` service.

The popover directive also supports various default configurations through the
$tooltipProvider. See the [tooltip](#tooltip) section for more information.

