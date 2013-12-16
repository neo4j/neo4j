Typeahead is a AngularJS version of [Twitter Bootstrap typeahead plugin](http://twitter.github.com/bootstrap/javascript.html#typeahead).
This directive can be used to quickly create elegant typeheads with any form text input.

It is very well integrated into the AngularJS as it uses subset of the
[select directive](http://docs.angularjs.org/api/ng.directive:select) syntax, which is very flexible. Supported expressions:

* _label_ for _value_ in _sourceArray_
* _select_ as _label_ for _value_ in _sourceArray_

The `sourceArray` expression can use a special `$viewValue` variable that corresponds to a value entered inside input by a user.

Also this directive works with promises and it means that you can retrieve matches using the `$http` service with minimal effort.