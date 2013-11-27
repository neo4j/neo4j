# 0.4.0 (2013-06-24)

## Features

- **buttons:** 
  - support dynamic values in btn-radio ([e8c5b548](http://github.com/angular-ui/bootstrap/commit/e8c5b548))  
- **carousel:** 
  - add option to prevent pause ([5f895c13](http://github.com/angular-ui/bootstrap/commit/5f895c13))  
- **datepicker:** 
  - add datepicker directive ([30a00a07](http://github.com/angular-ui/bootstrap/commit/30a00a07))  
- **pagination:** 
  - option for different mode when maxSize ([a023d082](http://github.com/angular-ui/bootstrap/commit/a023d082))  
  - add pager directive ([d9526475](http://github.com/angular-ui/bootstrap/commit/d9526475))  
- **tabs:** 
  - Change directive name, add features ([c5326595](http://github.com/angular-ui/bootstrap/commit/c5326595))  
  - support disabled state ([2b78dd16](http://github.com/angular-ui/bootstrap/commit/2b78dd16))  
  - add support for vertical option ([88d17a75](http://github.com/angular-ui/bootstrap/commit/88d17a75))  
  - add support for other navigation types, like 'pills' ([53e0a39f](http://github.com/angular-ui/bootstrap/commit/53e0a39f))  
- **timepicker:** 
  - add timepicker directive ([9bc5207b](http://github.com/angular-ui/bootstrap/commit/9bc5207b))  
- **tooltip:** 
  - add mouse placement option ([ace7bc60](http://github.com/angular-ui/bootstrap/commit/ace7bc60))
  - add *-append-to-body attribute ([d0896263](http://github.com/angular-ui/bootstrap/commit/d0896263))  
  - add custom trigger support ([dfa53155](http://github.com/angular-ui/bootstrap/commit/dfa53155))  
- **typeahead:** 
  - support typeahead-on-select callback ([91ac17c9](http://github.com/angular-ui/bootstrap/commit/91ac17c9))  
  - support wait-ms option ([7f35a3f2](http://github.com/angular-ui/bootstrap/commit/7f35a3f2))   

## Bug Fixes

- **accordion:** 
  - allow accordion heading directives as attributes. ([25f6e55c](http://github.com/angular-ui/bootstrap/commit/25f6e55c))
- **carousel:** 
  - do not allow user to change slide if transitioning ([1d19663f](http://github.com/angular-ui/bootstrap/commit/1d19663f))  
  - make slide 'active' binding optional ([17d6c3b5](http://github.com/angular-ui/bootstrap/commit/17d6c3b5))  
  - fix error with deleting multiple slides at once ([3fcb70f0](http://github.com/angular-ui/bootstrap/commit/3fcb70f0))  
- **dialog:** 
  - remove dialogOpenClass to get in line with v2.3 ([f009b23f](http://github.com/angular-ui/bootstrap/commit/f009b23f))  
- **pagination:** 
  - bind *-text attributes ([e1bff6b7](http://github.com/angular-ui/bootstrap/commit/e1bff6b7))  
- **progressbar:** 
  - user `percent` attribute instead of `value`. ([58efec80](http://github.com/angular-ui/bootstrap/commit/58efec80))  
- **tooltip:** 
  - fix positioning error when appendToBody is set to true ([76fee1f9](http://github.com/angular-ui/bootstrap/commit/76fee1f9))  
  - close tooltips appended to body on location change ([041261b5](http://github.com/angular-ui/bootstrap/commit/041261b5))  
  - tooltips will hide on scope.$destroy ([3e5a58e5](http://github.com/angular-ui/bootstrap/commit/3e5a58e5))  
  - support of custom $interpolate.startSymbol ([88c94ee6](http://github.com/angular-ui/bootstrap/commit/88c94ee6))  
  - make sure tooltip scope is evicted from cache ([9246905a](http://github.com/angular-ui/bootstrap/commit/9246905a))  
- **typeahead:** 
  - return focus to the input after selecting a suggestion ([04a21e33](http://github.com/angular-ui/bootstrap/commit/04a21e33))   

## Breaking Changes

- **pagination:** 
 The 'first-text', 'previous-text', 'next-text' and 'last-text'
  attributes are now binded to parent scope.

  To migrate your code, surround the text of these attributes with quotes.

  Before:

  <pagination first-text="<<" ...></pagination>

  After:

  <pagination first-text="'<<'" ...></pagination>

- **progressbar:** 
 The 'value' is replaced by 'percent'.

  Before:

  <progress value="..."></progress>

  After:

  <progress percent="..."></progress>

- **tabs:** 
 The 'tabs' directive has been renamed to 'tabset', and
 the 'pane' directive has been renamed to 'tab'.

    To migrate your code, follow the example below.

    Before:

    <tabs>
      <pane heading="one">
        First Content
      </pane>
      <pane ng-repeat="apple in basket" heading="{{apple.heading}}">
        {{apple.content}}
      </pane>
    </tabs>

    After:

    <tabset>
      <tab heading="one">
        First Content
      </tab>
      <tab ng-repeat="apple in basket" heading="{{apple.heading}}">
        {{apple.content}}
      </tab>
    </tabset>
 
# 0.3.0 (2013-04-30)

## Features

- **progressbar:**
  - add progressbar directive ([261f2072](https://github.com/angular-ui/bootstrap/commit/261f2072))
- **rating:**
  - add rating directive ([6b5e6369](https://github.com/angular-ui/bootstrap/commit/6b5e6369))
- **typeahead:**
  - support the editable property ([a40c3fbe](https://github.com/angular-ui/bootstrap/commit/a40c3fbe))
  - support typeahead-loading bindable expression ([b58c9c88](https://github.com/angular-ui/bootstrap/commit/b58c9c88))
- **tooltip:**
  - added popup-delay option ([a79a2ba8](https://github.com/angular-ui/bootstrap/commit/a79a2ba8))
  - added appendToBody to $tooltip ([1ee467f8](https://github.com/angular-ui/bootstrap/commit/1ee467f8))
  - added tooltip-html-unsafe directive ([45ed2805](https://github.com/angular-ui/bootstrap/commit/45ed2805))
  - support for custom triggers ([b1ba821b](https://github.com/angular-ui/bootstrap/commit/b1ba821b))

## Bug Fixes

- **alert:**
  - don't show close button if no close callback specified ([c2645f4a](https://github.com/angular-ui/bootstrap/commit/c2645f4a))
- **carousel:**
  - Hide navigation indicators if only one slide ([aedc0565](https://github.com/angular-ui/bootstrap/commit/aedc0565))
- **collapse:**
  - remove reference to msTransition for IE10 ([55437b16](https://github.com/angular-ui/bootstrap/commit/55437b16))
- **dialog:**
  - set _open to false on init ([dcc9ef31](https://github.com/angular-ui/bootstrap/commit/dcc9ef31))
  - close dialog on location change ([474ce52e](https://github.com/angular-ui/bootstrap/commit/474ce52e))
  - IE8 fix to not set data() against text nodes ([a6c540e5](https://github.com/angular-ui/bootstrap/commit/a6c540e5))
  - fix $apply in progres on $location change ([77e6acb9](https://github.com/angular-ui/bootstrap/commit/77e6acb9))
- **tabs:**
  - remove superfluous href from tabs template ([38c1badd](https://github.com/angular-ui/bootstrap/commit/38c1badd))
- **tooltip:**
  - fix positioning issues in tooltips and popovers ([6458f487](https://github.com/angular-ui/bootstrap/commit/6458f487))
- **typeahead:**
  - close matches popup on click outside typeahead ([acca7dcd](https://github.com/angular-ui/bootstrap/commit/acca7dcd))
  - stop keydown event propagation when ESC pressed to discard matches ([22a00cd0](https://github.com/angular-ui/bootstrap/commit/22a00cd0))
  - correctly render initial model value ([929a46fa](https://github.com/angular-ui/bootstrap/commit/929a46fa))
  - correctly higlight matches if query contains regexp-special chars ([467afcd6](https://github.com/angular-ui/bootstrap/commit/467afcd6))
  - fix matches pop-up positioning issues ([74beecdb](https://github.com/angular-ui/bootstrap/commit/74beecdb))

# 0.2.0 (2013-03-03)

## Features

- **dialog:**
  - Make $dialog 'resolve' property to work the same way of $routeProvider.when ([739f86f](https://github.com/angular-ui/bootstrap/commit/739f86f))
- **modal:**
  - allow global override of modal options ([acaf72b](https://github.com/angular-ui/bootstrap/commit/acaf72b))
- **buttons:**
  - add checkbox and radio buttons ([571ccf4](https://github.com/angular-ui/bootstrap/commit/571ccf4))
- **carousel:**
  - add slide indicators ([3b677ee](https://github.com/angular-ui/bootstrap/commit/3b677ee))
- **typeahead:**
  - add typeahead directive ([6a97da2](https://github.com/angular-ui/bootstrap/commit/6a97da2))
- **accordion:**
  - enable HTML in accordion headings ([3afcaa4](https://github.com/angular-ui/bootstrap/commit/3afcaa4))
- **pagination:**
  - add first/last link & constant congif options ([0ff0454](https://github.com/angular-ui/bootstrap/commit/0ff0454))

## Bug fixes

- **dialog:**
  - update resolve section to new syntax ([1f87486](https://github.com/angular-ui/bootstrap/commit/1f87486))
  - $compile entire modal ([7575b3c](https://github.com/angular-ui/bootstrap/commit/7575b3c))
- **tooltip:**
  - don't show tooltips if there is no content to show ([030901e](https://github.com/angular-ui/bootstrap/commit/030901e))
  - fix placement issues ([a2bbf4d](https://github.com/angular-ui/bootstrap/commit/a2bbf4d))
- **collapse:**
  - Avoids fixed height on collapse ([ff5d119](https://github.com/angular-ui/bootstrap/commit/ff5d119))
- **accordion:**
  - fix minification issues ([f4da4d6](https://github.com/angular-ui/bootstrap/commit/f4da4d6))
- **typeahead:**
  -  update inputs value on mapping where label is not derived from the model ([a5f64de](https://github.com/angular-ui/bootstrap/commit/a5f64de))

# 0.1.0 (2013-02-02)

_Very first, initial release_.

## Features

Version `0.1.0` was released with the following directives:

* accordion
* alert
* carousel
* dialog
* dropdownToggle
* modal
* pagination
* popover
* tabs
* tooltip
