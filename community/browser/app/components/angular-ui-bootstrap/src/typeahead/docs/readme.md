Typeahead is a AngularJS version of [Bootstrap v2's typeahead plugin](http://getbootstrap.com/2.3.2/javascript.html#typeahead).
This directive can be used to quickly create elegant typeaheads with any form text input.

It is very well integrated into AngularJS as it uses a subset of the
[select directive](http://docs.angularjs.org/api/ng.directive:select) syntax, which is very flexible. Supported expressions are:

* _label_ for _value_ in _sourceArray_
* _select_ as _label_ for _value_ in _sourceArray_

The `sourceArray` expression can use a special `$viewValue` variable that corresponds to the value entered inside the input.

This directive works with promises, meaning you can retrieve matches using the `$http` service with minimal effort.

The typeahead directives provide several attributes:

* `ng-model` <i class="glyphicon glyphicon-eye-open"></i>
   :
   Assignable angular expression to data-bind to

* `typeahead` <i class="glyphicon glyphicon-eye-open"></i>
   :
   Comprehension Angular expression (see [select directive](http://docs.angularjs.org/api/ng.directive:select))

* `typeahead-append-to-body` <i class="glyphicon glyphicon-eye-open"></i>
   _(Defaults: false)_ : Should the typeahead popup be appended to $body instead of the parent element?

* `typeahead-editable` <i class="glyphicon glyphicon-eye-open"></i>
   _(Defaults: true)_ :
   Should it restrict model values to the ones selected from the popup only ?

* `typeahead-input-formatter` <i class="glyphicon glyphicon-eye-open"></i>
   _(Defaults: undefined)_ :
   Format the ng-model result after selection

* `typeahead-loading` <i class="glyphicon glyphicon-eye-open"></i>
   _(Defaults: angular.noop)_ :
   Binding to a variable that indicates if matches are being retrieved asynchronously

* `typeahead-min-length` <i class="glyphicon glyphicon-eye-open"></i>
   _(Defaults: 1)_ :
   Minimal no of characters that needs to be entered before typeahead kicks-in

* `typeahead-on-select($item, $model, $label)`
   _(Defaults: null)_ :
   A callback executed when a match is selected

* `typeahead-template-url` <i class="glyphicon glyphicon-eye-open"></i>
   :
   Set custom item template

* `typeahead-wait-ms` <i class="glyphicon glyphicon-eye-open"></i>
   _(Defaults: 0)_ :
   Minimal wait time after last character typed before typeahead kicks-in

* `typeahead-focus-first`
   _(Defaults: true)_ :
   Should the first match automatically be focused as you type?
