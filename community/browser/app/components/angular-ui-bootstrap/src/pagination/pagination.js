angular.module('ui.bootstrap.pagination', [])

.controller('PaginationController', ['$scope', '$interpolate', function ($scope, $interpolate) {

  this.currentPage = 1;

  this.noPrevious = function() {
    return this.currentPage === 1;
  };
  this.noNext = function() {
    return this.currentPage === $scope.numPages;
  };

  this.isActive = function(page) {
    return this.currentPage === page;
  };

  this.reset = function() {
    $scope.pages = [];
    this.currentPage = parseInt($scope.currentPage, 10);

    if ( this.currentPage > $scope.numPages ) {
      $scope.selectPage($scope.numPages);
    }
  };

  var self = this;
  $scope.selectPage = function(page) {
    if ( ! self.isActive(page) && page > 0 && page <= $scope.numPages) {
      $scope.currentPage = page;
      $scope.onSelectPage({ page: page });
    }
  };

  this.getAttributeValue = function(attribute, defaultValue, interpolate) {
    return angular.isDefined(attribute) ? (interpolate ? $interpolate(attribute)($scope.$parent) : $scope.$parent.$eval(attribute)) : defaultValue;
  };
}])

.constant('paginationConfig', {
  boundaryLinks: false,
  directionLinks: true,
  firstText: 'First',
  previousText: 'Previous',
  nextText: 'Next',
  lastText: 'Last',
  rotate: true
})

.directive('pagination', ['paginationConfig', function(config) {
  return {
    restrict: 'EA',
    scope: {
      numPages: '=',
      currentPage: '=',
      maxSize: '=',
      onSelectPage: '&'
    },
    controller: 'PaginationController',
    templateUrl: 'template/pagination/pagination.html',
    replace: true,
    link: function(scope, element, attrs, paginationCtrl) {

      // Setup configuration parameters
      var boundaryLinks = paginationCtrl.getAttributeValue(attrs.boundaryLinks,  config.boundaryLinks      ),
      directionLinks    = paginationCtrl.getAttributeValue(attrs.directionLinks, config.directionLinks     ),
      firstText         = paginationCtrl.getAttributeValue(attrs.firstText,      config.firstText,     true),
      previousText      = paginationCtrl.getAttributeValue(attrs.previousText,   config.previousText,  true),
      nextText          = paginationCtrl.getAttributeValue(attrs.nextText,       config.nextText,      true),
      lastText          = paginationCtrl.getAttributeValue(attrs.lastText,       config.lastText,      true),
      rotate            = paginationCtrl.getAttributeValue(attrs.rotate,         config.rotate);

      // Create page object used in template
      function makePage(number, text, isActive, isDisabled) {
        return {
          number: number,
          text: text,
          active: isActive,
          disabled: isDisabled
        };
      }

      scope.$watch('numPages + currentPage + maxSize', function() {
        paginationCtrl.reset();

        // Default page limits
        var startPage = 1, endPage = scope.numPages;
        var isMaxSized = ( angular.isDefined(scope.maxSize) && scope.maxSize < scope.numPages );

        // recompute if maxSize
        if ( isMaxSized ) {
          if ( rotate ) {
            // Current page is displayed in the middle of the visible ones
            startPage = Math.max(paginationCtrl.currentPage - Math.floor(scope.maxSize/2), 1);
            endPage   = startPage + scope.maxSize - 1;

            // Adjust if limit is exceeded
            if (endPage > scope.numPages) {
              endPage   = scope.numPages;
              startPage = endPage - scope.maxSize + 1;
            }
          } else {
            // Visible pages are paginated with maxSize
            startPage = ((Math.ceil(paginationCtrl.currentPage / scope.maxSize) - 1) * scope.maxSize) + 1;

            // Adjust last page if limit is exceeded
            endPage = Math.min(startPage + scope.maxSize - 1, scope.numPages);
          }
        }

        // Add page number links
        for (var number = startPage; number <= endPage; number++) {
          var page = makePage(number, number, paginationCtrl.isActive(number), false);
          scope.pages.push(page);
        }

        // Add links to move between page sets
        if ( isMaxSized && ! rotate ) {
          if ( startPage > 1 ) {
            var previousPageSet = makePage(startPage - 1, '...', false, false);
            scope.pages.unshift(previousPageSet);
          }

          if ( endPage < scope.numPages ) {
            var nextPageSet = makePage(endPage + 1, '...', false, false);
            scope.pages.push(nextPageSet);
          }
        }

        // Add previous & next links
        if (directionLinks) {
          var previousPage = makePage(paginationCtrl.currentPage - 1, previousText, false, paginationCtrl.noPrevious());
          scope.pages.unshift(previousPage);

          var nextPage = makePage(paginationCtrl.currentPage + 1, nextText, false, paginationCtrl.noNext());
          scope.pages.push(nextPage);
        }

        // Add first & last links
        if (boundaryLinks) {
          var firstPage = makePage(1, firstText, false, paginationCtrl.noPrevious());
          scope.pages.unshift(firstPage);

          var lastPage = makePage(scope.numPages, lastText, false, paginationCtrl.noNext());
          scope.pages.push(lastPage);
        }
      });
    }
  };
}])

.constant('pagerConfig', {
  previousText: '« Previous',
  nextText: 'Next »',
  align: true
})

.directive('pager', ['pagerConfig', function(config) {
  return {
    restrict: 'EA',
    scope: {
      numPages: '=',
      currentPage: '=',
      onSelectPage: '&'
    },
    controller: 'PaginationController',
    templateUrl: 'template/pagination/pager.html',
    replace: true,
    link: function(scope, element, attrs, paginationCtrl) {

      // Setup configuration parameters
      var previousText = paginationCtrl.getAttributeValue(attrs.previousText, config.previousText, true),
      nextText         = paginationCtrl.getAttributeValue(attrs.nextText,     config.nextText,     true),
      align            = paginationCtrl.getAttributeValue(attrs.align,        config.align);

      // Create page object used in template
      function makePage(number, text, isDisabled, isPrevious, isNext) {
        return {
          number: number,
          text: text,
          disabled: isDisabled,
          previous: ( align && isPrevious ),
          next: ( align && isNext )
        };
      }

      scope.$watch('numPages + currentPage', function() {
        paginationCtrl.reset();

        // Add previous & next links
        var previousPage = makePage(paginationCtrl.currentPage - 1, previousText, paginationCtrl.noPrevious(), true, false);
        scope.pages.unshift(previousPage);

        var nextPage = makePage(paginationCtrl.currentPage + 1, nextText, paginationCtrl.noNext(), false, true);
        scope.pages.push(nextPage);
      });
    }
  };
}]);
