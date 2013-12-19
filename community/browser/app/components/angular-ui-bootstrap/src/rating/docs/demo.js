var RatingDemoCtrl = function ($scope) {
  $scope.rate = 7;
  $scope.max = 10;
  $scope.isReadonly = false;
  $scope.hoveringOver = function(value) {
    $scope.overStar = value;
  };
};
