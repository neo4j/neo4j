# $dialogProvider <small>(service in ui.bootstrap)</small>

## Description

Used for configuring global options for dialogs.

### Methods

#### `options(opts)`

Sets the default global options for your application. Options can be overridden when opening dialogs. Available options are:

*   `backdrop`: a boolean value indicating whether a backdrop should be used or not, defaults to true
*   `dialogClass`: the css class for the modal div, defaults to 'modal'
*   `backdropClass`: the css class for the backdrop, defaults to 'modal-backdrop'
*   `transitionClass`: the css class that applies transitions to the modal and backdrop, defaults to 'fade'
*   `triggerClass`: the css class that triggers the transitions, defaults to 'in'
*   `dialogOpenClass`: the css class that is added to body when dialog is opened, defaults to 'modal-open'
*   `resolve`: members that will be resolved and passed to the controller as locals
*   `controller`: the controller to associate with the included partial view
*   `backdropFade`: a boolean value indicating whether the backdrop should fade in and out using a CSS transition, defaults to false
*   `dialogFade`: a boolean value indicating whether the modal should fade in and out using a CSS transition, defaults to false
*   `keyboard`: indicates whether the dialog should be closable by hitting the ESC key, defaults to true
*   `backdropClick`: indicates whether the dialog should be closable by clicking the backdrop area, defaults to true

Example:

    var app = angular.module('App', ['ui.bootstrap.dialog'] , function($dialogProvider){
        $dialogProvider.options({backdropClick: false, dialogFade: true});
    });

# $dialog service

## Description

Allows you to open dialogs from within your controller.

### Methods

#### `dialog([templateUrl[, controller]])`

Creates a new dialog, optionally setting the `templateUrl`, and `controller` options.

Example:

    app.controller('MainCtrl', function($dialog, $scope) {
        $scope.openItemEditor = function(item){
            var d = $dialog.dialog({dialogFade: false, resolve: {item: function(){ return angular.copy(item); } }});
            d.open('dialogs/item-editor.html', 'EditItemController');
        };
    });

    // note that the resolved item as well as the dialog are injected in the dialog's controller
    app.controller('EditItemController', ['$scope', 'dialog', 'item', function($scope, dialog, item){
        $scope.item = item;
        $scope.submit = function(){
            dialog.close('ok');
        };
    }]);

#### `messageBox(title, message, buttons)`

Opens a message box with the specified `title`, `message` and a series of `buttons` can be provided, every button can specify:

*   `label`: the label of the button
*   `result`: the result used to invoke the close method of the dialog
*   `cssClass`: optional, the CSS class (e.g. btn-primary) to apply to the button

Example:

    app.controller('MainCtrl', function($dialog, $scope) {
        $scope.deleteItem = function(item){
            var msgbox = $dialog.messageBox('Delete Item', 'Are you sure?', [{label:'Yes, I\'m sure', result: 'yes'},{label:'Nope', result: 'no'}]);
            msgbox.open().then(function(result){
                if(result === 'yes') {deleteItem(item);}
            });
        };
    });

## Dialog class

The dialog object returned by the `$dialog` service methods `open` and `message`.

### Methods

#### `open`

(Re)Opens the dialog and returns a promise.

#### `close([result])`

Closes the dialog. Optionally a result can be specified. The result is used to resolve the promise returned by the `open` method.

#### `isOpen`

Returns true if the dialog is shown, else returns false.

