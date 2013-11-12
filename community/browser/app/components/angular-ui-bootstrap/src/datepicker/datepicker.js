angular.module('ui.bootstrap.datepicker', [])

.constant('datepickerConfig', {
  dayFormat: 'dd',
  monthFormat: 'MMMM',
  yearFormat: 'yyyy',
  dayHeaderFormat: 'EEE',
  dayTitleFormat: 'MMMM yyyy',
  monthTitleFormat: 'yyyy',
  showWeeks: true,
  startingDay: 0,
  yearRange: 20
})

.directive( 'datepicker', ['dateFilter', '$parse', 'datepickerConfig', function (dateFilter, $parse, datepickerConfig) {
  return {
    restrict: 'EA',
    replace: true,
    scope: {
      model: '=ngModel',
      dateDisabled: '&'
    },
    templateUrl: 'template/datepicker/datepicker.html',
    link: function(scope, element, attrs) {
      scope.mode = 'day'; // Initial mode

      // Configuration parameters
      var selected = new Date(), showWeeks, minDate, maxDate, format = {};
      format.day   = angular.isDefined(attrs.dayFormat) ? scope.$eval(attrs.dayFormat) : datepickerConfig.dayFormat;
      format.month = angular.isDefined(attrs.monthFormat) ? scope.$eval(attrs.monthFormat) : datepickerConfig.monthFormat;
      format.year  = angular.isDefined(attrs.yearFormat) ? scope.$eval(attrs.yearFormat) : datepickerConfig.yearFormat;
      format.dayHeader  = angular.isDefined(attrs.dayHeaderFormat) ? scope.$eval(attrs.dayHeaderFormat) : datepickerConfig.dayHeaderFormat;
      format.dayTitle   = angular.isDefined(attrs.dayTitleFormat) ? scope.$eval(attrs.dayTitleFormat) : datepickerConfig.dayTitleFormat;
      format.monthTitle = angular.isDefined(attrs.monthTitleFormat) ? scope.$eval(attrs.monthTitleFormat) : datepickerConfig.monthTitleFormat;
      var startingDay   = angular.isDefined(attrs.startingDay) ? scope.$eval(attrs.startingDay) : datepickerConfig.startingDay;
      var yearRange = angular.isDefined(attrs.yearRange) ? scope.$eval(attrs.yearRange) : datepickerConfig.yearRange;

      if (attrs.showWeeks) {
        scope.$parent.$watch($parse(attrs.showWeeks), function(value) {
          showWeeks = !! value;
          updateShowWeekNumbers();
        });
      } else {
        showWeeks = datepickerConfig.showWeeks;
        updateShowWeekNumbers();
      }

      if (attrs.min) {
        scope.$parent.$watch($parse(attrs.min), function(value) {
          minDate = new Date(value);
          refill();
        });
      }
      if (attrs.max) {
        scope.$parent.$watch($parse(attrs.max), function(value) {
          maxDate = new Date(value);
          refill();
        });
      }

      function updateCalendar (rows, labels, title) {
        scope.rows = rows;
        scope.labels = labels;
        scope.title = title;
      }

      // Define whether the week number are visible
      function updateShowWeekNumbers() {
        scope.showWeekNumbers = ( scope.mode === 'day' && showWeeks );
      }

      function compare( date1, date2 ) {
        if ( scope.mode === 'year') {
          return date2.getFullYear() - date1.getFullYear();
        } else if ( scope.mode === 'month' ) {
          return new Date( date2.getFullYear(), date2.getMonth() ) - new Date( date1.getFullYear(), date1.getMonth() );
        } else if ( scope.mode === 'day' ) {
          return (new Date( date2.getFullYear(), date2.getMonth(), date2.getDate() ) - new Date( date1.getFullYear(), date1.getMonth(), date1.getDate() ) );
        }
      }

      function isDisabled(date) {
        return ((minDate && compare(date, minDate) > 0) || (maxDate && compare(date, maxDate) < 0) || (scope.dateDisabled && scope.dateDisabled({ date: date, mode: scope.mode })));
      }

      // Split array into smaller arrays
      var split = function(a, size) {
        var arrays = [];
        while (a.length > 0) {
          arrays.push(a.splice(0, size));
        }
        return arrays;
      };
      var getDaysInMonth = function( year, month ) {
        return new Date(year, month + 1, 0).getDate();
      };

      var fill = {
        day: function() {
          var days = [], labels = [], lastDate = null;

          function addDays( dt, n, isCurrentMonth ) {
            for (var i =0; i < n; i ++) {
              days.push( {date: new Date(dt), isCurrent: isCurrentMonth, isSelected: isSelected(dt), label: dateFilter(dt, format.day), disabled: isDisabled(dt) } );
              dt.setDate( dt.getDate() + 1 );
            }
            lastDate = dt;
          }

          var d = new Date(selected);
          d.setDate(1);

          var difference = startingDay - d.getDay();
          var numDisplayedFromPreviousMonth = (difference > 0) ? 7 - difference : - difference;

          if ( numDisplayedFromPreviousMonth > 0 ) {
            d.setDate( - numDisplayedFromPreviousMonth + 1 );
            addDays(d, numDisplayedFromPreviousMonth, false);
          }
          addDays(lastDate || d, getDaysInMonth(selected.getFullYear(), selected.getMonth()), true);
          addDays(lastDate, (7 - days.length % 7) % 7, false);

          // Day labels
          for (i = 0; i < 7; i++) {
            labels.push(  dateFilter(days[i].date, format.dayHeader) );
          }
          updateCalendar( split( days, 7 ), labels, dateFilter(selected, format.dayTitle) );
        },
        month: function() {
          var months = [], i = 0, year = selected.getFullYear();
          while ( i < 12 ) {
            var dt = new Date(year, i++, 1);
            months.push( {date: dt, isCurrent: true, isSelected: isSelected(dt), label: dateFilter(dt, format.month), disabled: isDisabled(dt)} );
          }
          updateCalendar( split( months, 3 ), [], dateFilter(selected, format.monthTitle) );
        },
        year: function() {
          var years = [], year = parseInt((selected.getFullYear() - 1) / yearRange, 10) * yearRange + 1;
          for ( var i = 0; i < yearRange; i++ ) {
            var dt = new Date(year + i, 0, 1);
            years.push( {date: dt, isCurrent: true, isSelected: isSelected(dt), label: dateFilter(dt, format.year), disabled: isDisabled(dt)} );
          }
          var title = years[0].label + ' - ' + years[years.length - 1].label;
          updateCalendar( split( years, 5 ), [], title );
        }
      };
      var refill = function() {
        fill[scope.mode]();
      };
      var isSelected = function( dt ) {
        if ( scope.model && scope.model.getFullYear() === dt.getFullYear() ) {
          if ( scope.mode === 'year' ) {
            return true;
          }
          if ( scope.model.getMonth() === dt.getMonth() ) {
            return ( scope.mode === 'month' || (scope.mode === 'day' && scope.model.getDate() === dt.getDate()) );
          }
        }
        return false;
      };

      scope.$watch('model', function ( dt, olddt ) {
        if ( angular.isDate(dt) ) {
          selected = angular.copy(dt);
        }

        if ( ! angular.equals(dt, olddt) ) {
          refill();
        }
      });
      scope.$watch('mode', function() {
        updateShowWeekNumbers();
        refill();
      });

      scope.select = function( dt ) {
        selected = new Date(dt);

        if ( scope.mode === 'year' ) {
          scope.mode = 'month';
          selected.setFullYear( dt.getFullYear() );
        } else if ( scope.mode === 'month' ) {
          scope.mode = 'day';
          selected.setMonth( dt.getMonth() );
        } else if ( scope.mode === 'day' ) {
          scope.model = new Date(selected);
        }
      };
      scope.move = function(step) {
        if (scope.mode === 'day') {
          selected.setMonth( selected.getMonth() + step );
        } else if (scope.mode === 'month') {
          selected.setFullYear( selected.getFullYear() + step );
        } else if (scope.mode === 'year') {
          selected.setFullYear( selected.getFullYear() + step * yearRange );
        }
        refill();
      };
      scope.toggleMode = function() {
        scope.mode = ( scope.mode === 'day' ) ? 'month' : ( scope.mode === 'month' ) ? 'year' : 'day';
      };
      scope.getWeekNumber = function(row) {
        if ( scope.mode !== 'day' || ! scope.showWeekNumbers || row.length !== 7 ) {
          return;
        }

        var index = ( startingDay > 4 ) ? 11 - startingDay : 4 - startingDay; // Thursday
        var d = new Date( row[ index ].date );
        d.setHours(0, 0, 0);
        return Math.ceil((((d - new Date(d.getFullYear(), 0, 1)) / 86400000) + 1) / 7); // 86400000 = 1000*60*60*24;
      };
    }
  };
}]);