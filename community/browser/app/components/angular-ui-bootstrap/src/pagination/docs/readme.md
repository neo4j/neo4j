
A lightweight pagination directive that is focused on ... providing pagination & will take care of visualising a pagination bar and enable / disable buttons correctly!

### Pagination Settings ###

Settings can be provided as attributes in the `<pagination>` or globally configured through the `paginationConfig`.

 * `ng-model` <i class="glyphicon glyphicon-eye-open"></i>
 	:
 	Current page number. First page is 1.

 * `total-items` <i class="glyphicon glyphicon-eye-open"></i>
 	:
 	Total number of items in all pages.

 * `items-per-page` <i class="glyphicon glyphicon-eye-open"></i>
 	_(Defaults: 10)_ :
 	Maximum number of items per page. A value less than one indicates all items on one page.

 * `max-size` <i class="glyphicon glyphicon-eye-open"></i>
 	_(Defaults: null)_ :
 	Limit number for pagination size.

 * `num-pages` <small class="badge">readonly</small>
 	_(Defaults: angular.noop)_ :
 	An optional expression assigned the total number of pages to display.

 * `rotate`
 	_(Defaults: true)_ :
 	Whether to keep current page in the middle of the visible ones.

 * `direction-links`
 	_(Default: true)_ :
 	Whether to display Previous / Next buttons.

 * `previous-text`
 	_(Default: 'Previous')_ :
 	Text for Previous button.

 * `next-text`
 	_(Default: 'Next')_ :
 	Text for Next button.

 * `boundary-links`
 	_(Default: false)_ :
 	Whether to display First / Last buttons.

 * `first-text`
 	_(Default: 'First')_ :
 	Text for First button.

 * `last-text`
 	_(Default: 'Last')_ :
 	Text for Last button.

### Pager Settings ###

Settings can be provided as attributes in the `<pager>` or globally configured through the `pagerConfig`.  
For `ng-model`, `total-items`, `items-per-page` and `num-pages` see pagination settings. Other settings are:

 * `align`
 	_(Default: true)_ :
 	Whether to align each link to the sides.

 * `previous-text`
 	_(Default: '« Previous')_ :
 	Text for Previous button.

 * `next-text`
 	_(Default: 'Next »')_ :
 	Text for Next button.
