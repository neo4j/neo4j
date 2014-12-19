# angular-base64

Encapsulation of Nick Galbreath's base64.js library for AngularJS

## Installation

### Bower

```
bower install angular-base64
```

**NB:** The `ngBase64` bower package is deprecated due to camel casing issues on case-sensitive file systems.

```html
<script src="bower_components/angular-base64/angular-base64.js"></script>
```

## Usage

```javascript
angular
    .module('myApp', ['base64'])
    .controller('myController', [
    
        '$base64', '$scope', 
        function($base64, $scope) {
        
            $scope.encoded = $base64.encode('a string');
            $scope.decoded = $base64.decode('YSBzdHJpbmc=');
    }]);
```
