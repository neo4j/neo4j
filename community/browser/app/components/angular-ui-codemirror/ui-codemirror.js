'use strict';
angular.module('ui.codemirror', []).constant('uiCodemirrorConfig', {}).directive('uiCodemirror', [
  'uiCodemirrorConfig',
  function (uiCodemirrorConfig) {
    return {
      restrict: 'EA',
      require: '?ngModel',
      priority: 1,
      compile: function compile(tElement) {
        if (angular.isUndefined(window.CodeMirror)) {
          throw new Error('ui-codemirror need CodeMirror to work... (o rly?)');
        }
        var value = tElement.text();
        var codeMirror = new window.CodeMirror(function (cm_el) {
            angular.forEach(tElement.prop('attributes'), function (a) {
              if (a.name === 'ui-codemirror') {
                cm_el.setAttribute('ui-codemirror-opts', a.textContent);
              } else {
                cm_el.setAttribute(a.name, a.textContent);
              }
            });
            if (tElement.parent().length <= 0) {
              tElement.wrap('<div>');
            }
            tElement.replaceWith(cm_el);
          }, { value: value });
        return function postLink(scope, iElement, iAttrs, ngModel) {
          var options, opts;
          options = uiCodemirrorConfig.codemirror || {};
          opts = angular.extend({}, options, scope.$eval(iAttrs.uiCodemirror), scope.$eval(iAttrs.uiCodemirrorOpts));
          function updateOptions(newValues) {
            for (var key in newValues) {
              if (newValues.hasOwnProperty(key)) {
                codeMirror.setOption(key, newValues[key]);
              }
            }
          }
          updateOptions(opts);
          if (angular.isDefined(scope.$eval(iAttrs.uiCodemirror))) {
            scope.$watch(iAttrs.uiCodemirror, updateOptions, true);
          }
          codeMirror.on('change', function (instance) {
            var newValue = instance.getValue();
            if (ngModel && newValue !== ngModel.$viewValue) {
              ngModel.$setViewValue(newValue);
            }
            if (!scope.$$phase) {
              scope.$apply();
            }
          });
          if (ngModel) {
            ngModel.$formatters.push(function (value) {
              if (angular.isUndefined(value) || value === null) {
                return '';
              } else if (angular.isObject(value) || angular.isArray(value)) {
                throw new Error('ui-codemirror cannot use an object or an array as a model');
              }
              return value;
            });
            ngModel.$render = function () {
              var safeViewValue = ngModel.$viewValue || '';
              codeMirror.setValue(safeViewValue);
            };
          }
          if (iAttrs.uiRefresh) {
            scope.$watch(iAttrs.uiRefresh, function (newVal, oldVal) {
              if (newVal !== oldVal) {
                codeMirror.refresh();
              }
            });
          }
          if (angular.isFunction(opts.onLoad)) {
            opts.onLoad(codeMirror);
          }
        };
      }
    };
  }
]);
