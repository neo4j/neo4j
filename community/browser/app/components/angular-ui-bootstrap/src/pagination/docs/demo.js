var PaginationDemoCtrl = function ($scope) {
  $scope.noOfPages = 7;
  $scope.currentPage = 4;
  $scope.maxSize = 5;
  
  $scope.setPage = function (pageNo) {
    $scope.currentPage = pageNo;
  };

  $scope.bigNoOfPages = 18;
  $scope.bigCurrentPage = 1;
};
