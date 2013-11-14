# ui-codemirror directive [![Build Status](https://travis-ci.org/angular-ui/ui-codemirror.png)](https://travis-ci.org/angular-ui/ui-codemirror)

This directive allows you to add [CodeMirror](http://codemirror.net/) to your textarea elements.

# Requirements

- AngularJS
- [CodeMirror 3.x](https://github.com/marijnh/CodeMirror)

# Testing

We use karma (the new testacular) and jshint to ensure the quality of the code.  The easiest way to run these checks is to use grunt:

```sh
npm install -g grunt-cli
npm install
bower install
grunt
```

The karma task will try to open Firefox as a browser in which to run the tests.  Make sure this is available or change the configuration in `test\karma.conf.js`

# Usage

We use [bower](http://twitter.github.com/bower/) for dependency management.  Add

```json
dependencies: {
"angular-ui-codemirror": "latest"
}
```

To your `components.json` file. Then run

```sh
bower install
```

This will copy the ui-codemirror files into your `components` folder, along with its dependencies. Load the script files in your application:

```html
<script type="text/javascript" src="components/codemirror/lib/codemirror.js"></script>
<script type="text/javascript" src="components/angular/angular.js"></script>
<script type="text/javascript" src="components/angular-ui-codemirror/ui-codemirror.js"></script>
```

Add the CodeMirror module as a dependency to your application module:

```javascript
var myAppModule = angular.module('MyApp', ['ui.codemirror']);
```

Apply the directive to your form elements:

```html
<textarea ui-codemirror ng-model="x"></textarea>
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

```html
<textarea ui-codemirror="editorOptions" ng-model="x"></textarea>
```

## Working with ng-model

The ui-codemirror directive plays nicely with ng-model.

The ng-model will be watched for to set the CodeMirror document value (by [setValue](http://codemirror.net/doc/manual.html#setValue)).

_The ui-codemirror directive stores and expects the model value to be a standard javascript String._

## ui-codemirror events
The [CodeMirror events](http://codemirror.net/doc/manual.html#events) are supported has configuration options.
They keep the same name but are prefixed by _on_..
_This directive expects the events to be javascript Functions._
For example to handle changes of in the editor, we use _onChange_

```html
<textarea ui-codemirror="{
            lineWrapping : true,
            lineNumbers: true,
            mode: 'javascript',
            onChange: reParseInput
        }" ng-model="x"></textarea>
```

Now you can set the _reParseInput_ function in the controller.

```javascript
$scope.reParseInput = function(){
	$scope.errorMsg = "";
	$timeout.cancel(pending);
	pending = $timeout($scope.workHere, 500);
};
```

## ui-refresh directive

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
