angular.module('ui.bootstrap-slider', [])
    .directive('slider', ['$parse', '$timeout', '$rootScope', function ($parse, $timeout, $rootScope) {
        return {
            restrict: 'AE',
            replace: true,
            template: '<div><input class="slider-input" type="text" style="width:100%" /></div>',
            require: 'ngModel',
            scope: {
                max: "=",
                min: "=",
                step: "=",
                value: "=",
                ngModel: '=',
                ngDisabled: '=',
                range: '=',
                sliderid: '=',
                ticks: '=',
                ticksLabels: '=',
                ticksSnapBounds: '=',
                ticksPositions: '=',
                scale: '=',
                formatter: '&',
                onStartSlide: '&',
                onStopSlide: '&',
                onSlide: '&'
            },
            link: function ($scope, element, attrs, ngModelCtrl, $compile) {
                var ngModelDeregisterFn, ngDisabledDeregisterFn;

                initSlider();

                function initSlider() {
                    var options = {};

                    function setOption(key, value, defaultValue) {
                        options[key] = value || defaultValue;
                    }

                    function setFloatOption(key, value, defaultValue) {
                        options[key] = value ? parseFloat(value) : defaultValue;
                    }

                    function setBooleanOption(key, value, defaultValue) {
                        options[key] = value ? value + '' === 'true' : defaultValue;
                    }

                    function getArrayOrValue(value) {
                        return (angular.isString(value) && value.indexOf("[") === 0) ? angular.fromJson(value) : value;
                    }

                    setOption('id', $scope.sliderid);
                    setOption('orientation', attrs.orientation, 'horizontal');
                    setOption('selection', attrs.selection, 'before');
                    setOption('handle', attrs.handle, 'round');
                    setOption('tooltip', attrs.sliderTooltip || attrs.tooltip, 'show');
                    setOption('tooltip_position', attrs.sliderTooltipPosition, 'top');
                    setOption('tooltipseparator', attrs.tooltipseparator, ':');
                    setOption('ticks', $scope.ticks);
                    setOption('ticks_labels', $scope.ticksLabels);
                    setOption('ticks_snap_bounds', $scope.ticksSnapBounds);
                    setOption('ticks_positions', $scope.ticksPositions);
                    setOption('scale', $scope.scale, 'linear');

                    setFloatOption('min', $scope.min, 0);
                    setFloatOption('max', $scope.max, 10);
                    setFloatOption('step', $scope.step, 1);
                    var strNbr = options.step + '';
                    var decimals = strNbr.substring(strNbr.lastIndexOf('.') + 1);
                    setFloatOption('precision', attrs.precision, decimals);

                    setBooleanOption('tooltip_split', attrs.tooltipsplit, false);
                    setBooleanOption('enabled', attrs.enabled, true);
                    setBooleanOption('naturalarrowkeys', attrs.naturalarrowkeys, false);
                    setBooleanOption('reversed', attrs.reversed, false);

                    setBooleanOption('range', $scope.range, false);
                    if (options.range) {
                        if (angular.isArray($scope.value)) {
                            options.value = $scope.value;
                        }
                        else if (angular.isString($scope.value)) {
                            options.value = getArrayOrValue($scope.value);
                            if (!angular.isArray(options.value)) {
                                var value = parseFloat($scope.value);
                                if (isNaN(value)) value = 5;

                                if (value < $scope.min) {
                                    value = $scope.min;
                                    options.value = [value, options.max];
                                }
                                else if (value > $scope.max) {
                                    value = $scope.max;
                                    options.value = [options.min, value];
                                }
                                else {
                                    options.value = [options.min, options.max];
                                }
                            }
                        }
                        else {
                            options.value = [options.min, options.max]; // This is needed, because of value defined at $.fn.slider.defaults - default value 5 prevents creating range slider
                        }
                        $scope.ngModel = options.value; // needed, otherwise turns value into [null, ##]
                    }
                    else {
                        setFloatOption('value', $scope.value, 5);
                    }

                    if ($scope.formatter) options.formatter = $scope.$eval($scope.formatter);


                    // check if slider jQuery plugin exists
                    if ('$' in window && $.fn.slider) {
                        // adding methods to jQuery slider plugin prototype
                        $.fn.slider.constructor.prototype.disable = function () {
                            this.picker.off();
                        };
                        $.fn.slider.constructor.prototype.enable = function () {
                            this.picker.on();
                        };
                    }

                    // destroy previous slider to reset all options
                    if (element[0].__slider)
                        element[0].__slider.destroy();

                    var slider = new Slider(element[0].getElementsByClassName('slider-input')[0], options);
                    element[0].__slider = slider;

                    // everything that needs slider element
                    var updateEvent = getArrayOrValue(attrs.updateevent);
                    if (angular.isString(updateEvent)) {
                        // if only single event name in string
                        updateEvent = [updateEvent];
                    }
                    else {
                        // default to slide event
                        updateEvent = ['slide'];
                    }
                    angular.forEach(updateEvent, function (sliderEvent) {
                        slider.on(sliderEvent, function (ev) {
                            ngModelCtrl.$setViewValue(ev);
                            $timeout(function () {
                                $scope.$apply();
                            });
                        });
                    });
                    slider.on('change', function (ev) {
                        ngModelCtrl.$setViewValue(ev.newValue);
                        $timeout(function () {
                            $scope.$apply();
                        });
                    });

                    // Event listeners
                    var sliderEvents = {
                        slideStart: 'onStartSlide',
                        slide: 'onSlide',
                        slideStop: 'onStopSlide'
                    };
                    angular.forEach(sliderEvents, function (sliderEventAttr, sliderEvent) {
                        var fn = $parse(attrs[sliderEventAttr]);
                        slider.on(sliderEvent, function (ev) {
                            if ($scope[sliderEventAttr]) {
                                
                                var callback = function () {
                                    fn($scope.$parent, { $event: ev, value: ev });
                                }

                                if ($rootScope.$$phase) {
                                    $scope.$evalAsync(callback);
                                } else {
                                    $scope.$apply(callback);
                                }
                            }
                        });
                    });

                    // deregister ngDisabled watcher to prevent memory leaks
                    if (angular.isFunction(ngDisabledDeregisterFn)) {
                        ngDisabledDeregisterFn();
                        ngDisabledDeregisterFn = null;
                    }

                    ngDisabledDeregisterFn = $scope.$watch('ngDisabled', function (value) {
                        if (value) {
                            slider.disable();
                        }
                        else {
                            slider.enable();
                        }
                    });

                    // deregister ngModel watcher to prevent memory leaks
                    if (angular.isFunction(ngModelDeregisterFn)) ngModelDeregisterFn();
                    ngModelDeregisterFn = $scope.$watch('ngModel', function (value) {
                        if($scope.range){
                            slider.setValue(value);
                        }else{
                            slider.setValue(parseFloat(value));
                        }
                    }, true);
                }


                var watchers = ['min', 'max', 'step', 'range', 'scale'];
                angular.forEach(watchers, function (prop) {
                    $scope.$watch(prop, function () {
                        initSlider();
                    });
                });
            }
        };
    }])
;
