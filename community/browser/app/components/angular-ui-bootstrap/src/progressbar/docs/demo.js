var ProgressDemoCtrl = function ($scope) {
   
  $scope.random = function() {
    var value = Math.floor((Math.random()*100)+1);
    var type;

    if (value < 25) {
      type = 'success';
    } else if (value < 50) {
      type = 'info';
    } else if (value < 75) {
      type = 'warning';
    } else {
      type = 'danger';
    }

    $scope.dynamic = value;
    $scope.dynamicObject = {
      value: value,
      type: type
    };
  };
  $scope.random();
  
  var types = ['success', 'info', 'warning', 'danger'];
  $scope.randomStacked = function() {
    $scope.stackedArray = [];
    $scope.stacked = [];
    
    var n = Math.floor((Math.random()*4)+1);

    for (var i=0; i < n; i++) {
        var value = Math.floor((Math.random()*30)+1);
        $scope.stackedArray.push(value);
        
        var index = Math.floor((Math.random()*4));
        $scope.stacked.push({
          value: value,
          type: types[index]
        });
    }
  };
  $scope.randomStacked();
};
