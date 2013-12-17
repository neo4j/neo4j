# UI.Codemirror directive [![Build Status](https://travis-ci.org/angular-ui/ui-codemirror.png)](https://travis-ci.org/angular-ui/ui-codemirror)

This directive allows you to add [CodeMirror](http://codemirror.net/) to your textarea elements.

## Requirements

- AngularJS
- [CodeMirror 3.19.x](https://github.com/marijnh/CodeMirror)


## Usage

You can get it from [Bower](http://bower.io/)

```sh
bower install angular-ui-codemirror
```

This will copy the UI.Codemirror files into a `bower_components` folder, along with its dependencies. Load the script files in your application:

```html
<link rel="stylesheet" type="text/css" href="bower_components/codemirror/lib/codemirror.css">
<script type="text/javascript" src="bower_components/codemirror/lib/codemirror.js"></script>
<script type="text/javascript" src="bower_components/angular/angular.js"></script>
<script type="text/javascript" src="bower_components/angular-ui-codemirror/ui-codemirror.js"></script>
```

Add the UI.Codemirror module as a dependency to your application module:

```javascript
var myAppModule = angular.module('MyApp', ['ui.codemirror']);
```

Finally, add the directive to your html,
as attribute :

```html
<textarea ui-codemirror></textarea>
// or
<div ui-codemirror></div>
```

as element :
```xml
<ui-codemirror></ui-codemirror>
```


## Options

All the [Codemirror configuration options](http://codemirror.net/doc/manual.html#config) can be passed through the directive.

```javascript
myAppModule.controller('MyController', [ '$scope', function($scope) {
	$scope.editorOptions = {
		lineWrapping : true,
		lineNumbers: true,
		readOnly: 'nocursor',
		mode: 'xml',
	};
}]);
```

If you update this variable with the new values, they will be merged and the ui will be updated.

```xml
<textarea ui-codemirror="editorOptions" ng-model="x"></textarea>
// or
<ui-codemirror ui-codemirror-opts="editorOptions"></ui-codemirror>
```

### Working with ng-model

The ui-codemirror directive plays nicely with ng-model.

The ng-model will be watched for to set the CodeMirror document value (by [setValue](http://codemirror.net/doc/manual.html#setValue)).

_The ui-codemirror directive stores and expects the model value to be a standard javascript String._

### ui-refresh directive

If you apply the refresh directive to element then any change to do this scope value will result to a [refresh of the CodeMirror instance](http://codemirror.net/doc/manual.html#refresh).

_The ui-refresh directive expects a scope variable that can be any thing...._

```html
<textarea ui-codemirror ng-model="x" ui-refresh='isSomething'></textarea>
```

Now you can set the _isSomething_ in the controller scope.

```javascript
$scope.isSomething = true;
```

Note: the comparison operator between the old and the new value is "!=="


### CodeMirror instance direct access

For more interaction with the CodeMirror instance in the directive, we provide a direct access to it.
Using

```html
<div ui-codemirror="{ onLoad : codemirrorLoaded }" ></div>
```

the `$scope.codemirrorLoaded` function will be called with the [CodeMirror editor instance](http://codemirror.net/doc/manual.html#CodeMirror) as first argument

```javascript
myAppModule.controller('MyController', [ '$scope', function($scope) {

  $scope.codemirrorLoaded = function(_editor){
    // Editor part
    var _doc = _editor.getDoc();
    _editor.focus();

    // Options
    _editor.setOption('firstLineNumber', 10);
    _doc.markClean()

    // Events
    _editor.on("beforeChange", function(){ ... });
    _editor.on("change", function(){ ... });
  };

}]);
```

## Testing

We use Karma and jshint to ensure the quality of the code.  The easiest way to run these checks is to use grunt:

```sh
npm install -g grunt-cli
npm install && bower install
grunt
```

The karma task will try to open Firefox and Chrome as browser in which to run the tests.  Make sure this is available or change the configuration in `test\karma.conf.js`


### Grunt Serve

We have one task to serve them all !

```sh
grunt serve
```

It's equal to run separately: 

* `grunt connect:server` : giving you a development server at [http://localhost:8000/](http://localhost:8000/).

* `grunt karma:server` : giving you a Karma server to run tests (at [http://localhost:9876/](http://localhost:9876/) by default). You can force a test on this server with `grunt karma:unit:run`.

* `grunt watch` : will automatically test your code and build your demo.  You can demo generation with `grunt build-doc`.
