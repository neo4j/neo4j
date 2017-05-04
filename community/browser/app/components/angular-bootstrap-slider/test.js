angular.module('angular-bootstrap-slider-test', ['ui.bootstrap-slider'])
    .controller('TestCtrl', ['$scope', '$log', function ($scope, $log) {

        $scope.testOptions = {
            min: 5,
            max: 100,
            step: 5,
            precision: 2,
            orientation: 'horizontal',  // vertical
            handle: 'round', //'square', 'triangle' or 'custom'
            tooltip: 'show', //'hide','always'
            tooltipseparator: ':',
            tooltipsplit: false,
            enabled: true,
            naturalarrowkeys: false,
            range: false,
            ngDisabled: false,
            reversed: false
        };

        $scope.range = true;

        $scope.model = {
            first: 0,
            second: [],
            third: 0,
            fourth: 0,
            fifth: 0,
            sixth: 0,
            seventh: 0,
            eighth: 0,
            ninth: 0,
            tenth: 0
        };

        $scope.value = {
            first: $scope.testOptions.min + $scope.testOptions.step,
            second: [$scope.testOptions.min + $scope.testOptions.step, $scope.testOptions.max - $scope.testOptions.step],
            third: 0,
            fourth: 0,
            fifth: 0,
            sixth: 0,
            seventh: 0,
            eighth: 0,
            ninth: 0,
            tenth: 0
        };

        $scope.prefix = 'Current value: ';
        $scope.suffix = '%';

        $scope.formatterFn = function (value) {
            return $scope.prefix + value + $scope.suffix;
        };

        $scope.delegateEvent = null;
        $scope.slideDelegate = function (value, event) {
            if (angular.isObject(event)) {
                $log.log('Slide delegate value on ' + event.type + ': ' + value);
            }
            else {
                $log.log('Slide delegate value: ' + event);
            }
            $scope.delegateEvent = event;
        };
    }]
);