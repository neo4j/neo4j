angular.module('ui.bootstrap.timepicker', [])

.filter('pad', function() {
  return function(input) {
    if ( angular.isDefined(input) && input.toString().length < 2 ) {
      input = '0' + input;
    }
    return input;
  };
})

.constant('timepickerConfig', {
  hourStep: 1,
  minuteStep: 1,
  showMeridian: true,
  meridians: ['AM', 'PM'],
  readonlyInput: false,
  mousewheel: true
})

.directive('timepicker', ['padFilter', '$parse', 'timepickerConfig', function (padFilter, $parse, timepickerConfig) {
  return {
    restrict: 'EA',
    require:'ngModel',
    replace: true,
    templateUrl: 'template/timepicker/timepicker.html',
    scope: {
        model: '=ngModel'
    },
    link: function(scope, element, attrs, ngModelCtrl) {
      var selected = new Date(), meridians = timepickerConfig.meridians;

      var hourStep = timepickerConfig.hourStep;
      if (attrs.hourStep) {
        scope.$parent.$watch($parse(attrs.hourStep), function(value) {
          hourStep = parseInt(value, 10);
        });
      }

      var minuteStep = timepickerConfig.minuteStep;
      if (attrs.minuteStep) {
        scope.$parent.$watch($parse(attrs.minuteStep), function(value) {
          minuteStep = parseInt(value, 10);
        });
      }

      // 12H / 24H mode
      scope.showMeridian = timepickerConfig.showMeridian;
      if (attrs.showMeridian) {
        scope.$parent.$watch($parse(attrs.showMeridian), function(value) {
          scope.showMeridian = !! value;

          if ( ! scope.model ) {
            // Reset
            var dt = new Date( selected );
            var hours = getScopeHours();
            if (angular.isDefined( hours )) {
              dt.setHours( hours );
            }
            scope.model = new Date( dt );
          } else {
            refreshTemplate();
          }
        });
      }

      // Get scope.hours in 24H mode if valid
      function getScopeHours ( ) {
        var hours = parseInt( scope.hours, 10 );
        var valid = ( scope.showMeridian ) ? (hours > 0 && hours < 13) : (hours >= 0 && hours < 24);
        if ( !valid ) {
          return;
        }

        if ( scope.showMeridian ) {
          if ( hours === 12 ) {
            hours = 0;
          }
          if ( scope.meridian === meridians[1] ) {
            hours = hours + 12;
          }
        }
        return hours;
      }

      // Input elements
      var inputs = element.find('input');
      var hoursInputEl = inputs.eq(0), minutesInputEl = inputs.eq(1);

      // Respond on mousewheel spin
      var mousewheel = (angular.isDefined(attrs.mousewheel)) ? scope.$eval(attrs.mousewheel) : timepickerConfig.mousewheel;
      if ( mousewheel ) {

        var isScrollingUp = function(e) {
          if (e.originalEvent) {
            e = e.originalEvent;
          }
          return (e.detail || e.wheelDelta > 0);
        };

        hoursInputEl.bind('mousewheel', function(e) {
          scope.$apply( (isScrollingUp(e)) ? scope.incrementHours() : scope.decrementHours() );
          e.preventDefault();
        });

        minutesInputEl.bind('mousewheel', function(e) {
          scope.$apply( (isScrollingUp(e)) ? scope.incrementMinutes() : scope.decrementMinutes() );
          e.preventDefault();
        });
      }

      var keyboardChange = false;
      scope.readonlyInput = (angular.isDefined(attrs.readonlyInput)) ? scope.$eval(attrs.readonlyInput) : timepickerConfig.readonlyInput;
      if ( ! scope.readonlyInput ) {
        scope.updateHours = function() {
          var hours = getScopeHours();

          if ( angular.isDefined(hours) ) {
              keyboardChange = 'h';
              if ( scope.model === null ) {
                 scope.model = new Date( selected );
              }
              scope.model.setHours( hours );
          } else {
              scope.model = null;
              scope.validHours = false;
          }
        };

        hoursInputEl.bind('blur', function(e) {
          if ( scope.validHours && scope.hours < 10) {
            scope.$apply( function() {
              scope.hours = padFilter( scope.hours );
            });
          }
        });

        scope.updateMinutes = function() {
          var minutes = parseInt(scope.minutes, 10);
          if ( minutes >= 0 && minutes < 60 ) {
            keyboardChange = 'm';
            if ( scope.model === null ) {
              scope.model = new Date( selected );
            }
            scope.model.setMinutes( minutes );
          } else {
            scope.model = null;
            scope.validMinutes = false;
          }
        };

        minutesInputEl.bind('blur', function(e) {
          if ( scope.validMinutes && scope.minutes < 10 ) {
            scope.$apply( function() {
              scope.minutes = padFilter( scope.minutes );
            });
          }
        });
      } else {
        scope.updateHours = angular.noop;
        scope.updateMinutes = angular.noop;
      }

      scope.$watch( function getModelTimestamp() {
        return +scope.model;
      }, function( timestamp ) {
        if ( !isNaN( timestamp ) && timestamp > 0 ) {
          selected = new Date( timestamp );
          refreshTemplate();
        }
      });

      function refreshTemplate() {
        var hours = selected.getHours();
        if ( scope.showMeridian ) {
          // Convert 24 to 12 hour system
          hours = ( hours === 0 || hours === 12 ) ? 12 : hours % 12;
        }
        scope.hours =  ( keyboardChange === 'h' ) ? hours : padFilter(hours);
        scope.validHours = true;

        var minutes = selected.getMinutes();
        scope.minutes = ( keyboardChange === 'm' ) ? minutes : padFilter(minutes);
        scope.validMinutes = true;

        scope.meridian = ( scope.showMeridian ) ? (( selected.getHours() < 12 ) ? meridians[0] : meridians[1]) : '';

        keyboardChange = false;
      }

      function addMinutes( minutes ) {
        var dt = new Date( selected.getTime() + minutes * 60000 );
        if ( dt.getDate() !== selected.getDate()) {
          dt.setDate( dt.getDate() - 1 );
        }
        selected.setTime( dt.getTime() );
        scope.model = new Date( selected );
      }

      scope.incrementHours = function() {
        addMinutes( hourStep * 60 );
      };
      scope.decrementHours = function() {
        addMinutes( - hourStep * 60 );
      };
      scope.incrementMinutes = function() {
        addMinutes( minuteStep );
      };
      scope.decrementMinutes = function() {
        addMinutes( - minuteStep );
      };
      scope.toggleMeridian = function() {
        addMinutes( 12 * 60 * (( selected.getHours() < 12 ) ? 1 : -1) );
      };
    }
  };
}]);