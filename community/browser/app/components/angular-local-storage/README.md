angular-local-storage
=====================

An Angular module that gives you access to the browsers local storage, **v0.1.4**

[![Build Status](https://secure.travis-ci.org/grevory/angular-local-storage.png?branch=master)](https://travis-ci.org/grevory/) [![Coverage Status](https://img.shields.io/coveralls/grevory/angular-local-storage.svg)](https://coveralls.io/r/grevory/angular-local-storage?branch=master)

##Table of contents:
- [Get Started](#get-started)
- [Development](#development)
- [Configuration](#configuration)
 - [setPrefix](#setprefix)
 - [setStorageType](#setstoragetype)
 - [setStorageCookie](#setstoragecookie)
 - [setStorageCookieDomain](#setstoragecookiedomain)
 - [setNotify](#setnotify)
 - [Example](#configuration-example)
- [API Documentation](#api-documentation)
 - [isSupported](#issupported)
 - [getStorageType](#getstoragetype)
 - [set](#set)
 - [get](#get)
 - [keys](#keys)
 - [remove](#remove)
 - [clearAll](#clearall)
 - [bind](#bind)
 - [deriveKey](#derivekey)
 - [length](#length)
 - [cookie](#cookie)
    - [isSupported](#cookieissupported)
    - [set](#cookieset)
    - [get](#cookieget)
    - [remove](#cookieremove)
    - [clearAll](#cookieclearall)

##Get Started
**(1)** You can install angular-local-storage using 2 different ways:<br/>
**Git:**
clone & build [this](https://github.com/grevory/angular-local-storage.git) repository<br/>
**Bower:**
```bash
$ bower install angular-local-storage
```
**npm:**
```bash
$ npm install angular-local-storage
```
**(2)** Include `angular-local-storage.js` (or `angular-local-storage.min.js`) in your `index.html`, after including Angular itself.

**(3)** Add `'LocalStorageModule'` to your main module's list of dependencies.

When you're done, your setup should look similar to the following:

```html
<!doctype html>
<html ng-app="myApp">
<head>
   
</head>
<body>
    ...
    <script src="//ajax.googleapis.com/ajax/libs/angularjs/1.1.5/angular.min.js"></script>
    <script src="bower_components/js/angular-local-storage.min.js"></script>
    ...
    <script>
        var myApp = angular.module('myApp', ['LocalStorageModule']);

    </script>
    ...
</body>
</html>
```
##Configuration
###setPrefix
You could set a prefix to avoid overwriting any local storage variables from the rest of your app<br/>
**Default prefix:** `ls.<your-key>`
```js
myApp.config(function (localStorageServiceProvider) {
  localStorageServiceProvider
    .setPrefix('yourAppName');
});
```
###setStorageType
You could change web storage type to localStorage or sessionStorage<br/>
**Default storage:** `localStorage`
```js
myApp.config(function (localStorageServiceProvider) {
  localStorageServiceProvider
    .setStorageType('sessionStorage');
});
```
###setStorageCookie
Set cookie options (usually in case of fallback)<br/>
**expiry:** number of days before cookies expire (0 = does not expire). **default:** `30`<br/>
**path:** the web path the cookie represents. **default:** `'/'`
```js
myApp.config(function (localStorageServiceProvider) {
  localStorageServiceProvider
    .setStorageCookie(45, '<path>');
});
```
###setStorageCookieDomain
Set the cookie domain, since this runs inside a the `config()` block, only providers and constants can be injected.  As a result, `$location` service can't be used here, use a hardcoded string or `window.location`.<br/>
**No default value**
```js
myApp.config(function (localStorageServiceProvider) {
  localStorageServiceProvider
    .setStorageCookieDomain('<domain>');
});
```
###setNotify
Send signals for each of the following actions:<br/>
**setItem** , default: `true`<br/>
**removeItem** , default: `false`
```js
myApp.config(function (localStorageServiceProvider) {
  localStorageServiceProvider
    .setNotify(true, true);
});
```
###Configuration Example
Using all together
```js
myApp.config(function (localStorageServiceProvider) {
  localStorageServiceProvider
    .setPrefix('myApp')
    .setStorageType('sessionStorage')
    .setNotify(true, true)
});
```
##API Documentation
##isSupported
Checks if the browser support the current storage type(e.g: `localStorage`, `sessionStorage`).
**Returns:** `Boolean`
```js
myApp.controller('MainCtrl', function($scope, localStorageService) {
  //...
  if(localStorageService.isSupported) {
    //...
  }
  //...
});
```
###getStorageType
**Returns:** `String`
```js
myApp.controller('MainCtrl', function($scope, localStorageService) {
  //...
  var storageType = localStorageService.getStorageType(); //e.g localStorage
  //...
});
```
###set
Directly adds a value to local storage.<br/>
If local storage is not supported, use cookies instead.<br/>
**Returns:** `Boolean`
```js
myApp.controller('MainCtrl', function($scope, localStorageService) {
  //...
  function submit(key, val) {
   return localStorageService.set(key, value);
  }
  //...
});
```
###get
Directly get a value from local storage.<br/>
If local storage is not supported, use cookies instead.<br/>
**Returns:** `value from local storage`
```js
myApp.controller('MainCtrl', function($scope, localStorageService) {
  //...
  function getItem(key) {
   return localStorageService.get(key);
  }
  //...
});
```
###keys
Return array of keys for local storage, ignore keys that not owned.<br/>
**Returns:** `value from local storage`
```js
myApp.controller('MainCtrl', function($scope, localStorageService) {
  //...
  var lsKeys = localStorageService.keys();
  //...
});
```
###remove
Remove an item from local storage by key.<br/>
If local storage is not supported, use cookies instead.<br/>
**Returns:** `Boolean`
```js
myApp.controller('MainCtrl', function($scope, localStorageService) {
  //...
  function removeItem(key) {
   return localStorageService.remove(key);
  }
  //...
});
```
###clearAll
Remove all data for this app from local storage.<br/>
If local storage is not supported, use cookies instead.<br/>
**Note:** Optionally takes a regular expression string and removes matching.<br/>
**Returns:** `Boolean`
```js
myApp.controller('MainCtrl', function($scope, localStorageService) {
  //...
  function clearNumbers(key) {
   return localStorageService.clearAll(/^\d+$/);
  }
  //...
  function clearAll() {
   return localStorageService.clearAll();
  }
});
```
###bind
Bind $scope key to localStorageService.  
**Usage:** `localStorageService.bind(scope, property, value[optional], key[optional])`  
***key:*** The corresponding key used in local storage
**Returns:** deregistration function for this listener.
```js
myApp.controller('MainCtrl', function($scope, localStorageService) {
  //...
  localStorageService.set('property', 'oldValue');
  $scope.unbind = localStorageService.bind($scope, 'property');
  
  //Test Changes
  $scope.update = function(val) {
    $scope.property = val;
    $timeout(function() {
      alert("localStorage value: " + localStorageService.get('property'));
    });
  }
  //...
});
```
```html
<div ng-controller="MainCtrl">
  <p>{{property}}</p>
  <input type="text" ng-model="lsValue"/>
  <button ng-click="update(lsValue)">update</button>
  <button ng-click="unbind()">unbind</button>
</div>
```

###deriveKey
Return the derive key
**Returns** `String`
```js
myApp.controller('MainCtrl', function($scope, localStorageService) {
  //...
  localStorageService.set('property', 'oldValue');
  //Test Result
  console.log(localStorageService.deriveKey('property')); // ls.property
  //...
});
```
###length
Return localStorageService.length, ignore keys that not owned.  
**Returns** `Number`
```js
myApp.controller('MainCtrl', function($scope, localStorageService) {
  //...
  var lsLength = localStorageService.length(); // e.g: 7
  //...
});
```
##Cookie
Deal with browser's cookies directly.
##cookie.isSupported
Checks if cookies are enabled in the browser.
**Returns:** `Boolean`
```js
myApp.controller('MainCtrl', function($scope, localStorageService) {
  //...
  if(localStorageService.cookie.isSupported) {
    //...
  }
  //...
});
```
###cookie.set
Directly adds a value to cookies.<br/>
**Note:** Typically used as a fallback if local storage is not supported.<br/>
**Returns:** `Boolean`
```js
myApp.controller('MainCtrl', function($scope, localStorageService) {
  //...
  function submit(key, val) {
   return localStorageService.cookie.set(key, value);
  }
  //...
});
```
###cookie.get
Directly get a value from a cookie.<br/>
**Returns:** `value from local storage`
```js
myApp.controller('MainCtrl', function($scope, localStorageService) {
  //...
  function getItem(key) {
   return localStorageService.cookie.get(key);
  }
  //...
});
```
###cookie.remove
Remove directly value from a cookie.<br/>
**Returns:** `Boolean`
```js
myApp.controller('MainCtrl', function($scope, localStorageService) {
  //...
  function removeItem(key) {
   return localStorageService.cookie.remove(key);
  }
  //...
});
```
###clearAll
Remove all data for this app from cookie.<br/>
```js
myApp.controller('MainCtrl', function($scope, localStorageService) {
  //...
  function clearAll() {
   return localStorageService.cookie.clearAll();
  }
});
```

Check out the full demo at http://gregpike.net/demos/angular-local-storage/demo.html

##Development:
* Don't forget about tests.
* If you planning add some feature please create issue before.

Clone the project: 
```sh
$ git clone https://github.com/<your-repo>/angular-local-storage.git
$ npm install
$ bower install
```
Run the tests:
```sh
$ grunt test
```
**Deploy:**<br/>
Run the build task, update version before(bower,package)
```sh
$ grunt dist
$ git tag 0.*.*
$ git push origin master --tags
```


