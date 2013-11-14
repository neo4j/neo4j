/*
 jQuery UI Sortable plugin wrapper

 @param [ui-sortable] {object} Options to pass to $.fn.sortable() merged onto ui.config
*/
angular.module('ui.sortable', []).value('uiSortableConfig',{}).directive('uiSortable', [
  'uiSortableConfig', function(uiSortableConfig) {
    return {
      require: '?ngModel',
      link: function(scope, element, attrs, ngModel) {
        var onReceive, onRemove, onStart, onUpdate, opts;

        opts = angular.extend({}, uiSortableConfig, scope.$eval(attrs.uiSortable));

        if (ngModel) {

          ngModel.$render = function() {
            element.sortable( "refresh" );
          };

          onStart = function(e, ui) {
            // Save position of dragged item
            ui.item.sortable = { index: ui.item.index() };
          };

          onUpdate = function(e, ui) {
            // For some reason the reference to ngModel in stop() is wrong
            ui.item.sortable.resort = ngModel;
          };

          onReceive = function(e, ui) {
            ui.item.sortable.relocate = true;
            // added item to array into correct position and set up flag
            ngModel.$modelValue.splice(ui.item.index(), 0, ui.item.sortable.moved);
          };

          onRemove = function(e, ui) {
            // copy data into item
            if (ngModel.$modelValue.length === 1) {
              ui.item.sortable.moved = ngModel.$modelValue.splice(0, 1)[0];
            } else {
              ui.item.sortable.moved =  ngModel.$modelValue.splice(ui.item.sortable.index, 1)[0];
            }
          };

          onStop = function(e, ui) {
            // digest all prepared changes
            if (ui.item.sortable.resort && !ui.item.sortable.relocate) {

              // Fetch saved and current position of dropped element
              var end, start;
              start = ui.item.sortable.index;
              end = ui.item.index();

              // Reorder array and apply change to scope
              ui.item.sortable.resort.$modelValue.splice(end, 0, ui.item.sortable.resort.$modelValue.splice(start, 1)[0]);

            }
            if (ui.item.sortable.resort || ui.item.sortable.relocate) {
              scope.$apply();
            }
          };

          // If user provided 'start' callback compose it with onStart function
          opts.start = (function(_start){
            return function(e, ui) {
              onStart(e, ui);
              if (typeof _start === "function")
                _start(e, ui);
            }
          })(opts.start);

          // If user provided 'start' callback compose it with onStart function
          opts.stop = (function(_stop){
            return function(e, ui) {
              onStop(e, ui);
              if (typeof _stop === "function")
                _stop(e, ui);
            }
          })(opts.stop);

          // If user provided 'update' callback compose it with onUpdate function
          opts.update = (function(_update){
            return function(e, ui) {
              onUpdate(e, ui);
              if (typeof _update === "function")
                _update(e, ui);
            }
          })(opts.update);

          // If user provided 'receive' callback compose it with onReceive function
          opts.receive = (function(_receive){
            return function(e, ui) {
              onReceive(e, ui);
              if (typeof _receive === "function")
                _receive(e, ui);
            }
          })(opts.receive);

          // If user provided 'remove' callback compose it with onRemove function
          opts.remove = (function(_remove){
            return function(e, ui) {
              onRemove(e, ui);
              if (typeof _remove === "function")
                _remove(e, ui);
            };
          })(opts.remove);
        }

        // Create sortable
        element.sortable(opts);
      }
    };
  }
]);
