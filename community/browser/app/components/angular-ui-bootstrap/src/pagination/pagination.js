angular.module('ui.bootstrap.pagination', [])

.controller('PaginationController', ['$scope', function (scope) {

  scope.noPrevious = function() {
    return scope.currentPage === 1;
  };
  scope.noNext = function() {
    return scope.currentPage === scope.numPages;
  };

  scope.isActive = function(page) {
    return scope.currentPage === page;
  };

  scope.selectPage = function(page) {
    if ( ! scope.isActive(page) && page > 0 && page <= scope.numPages) {
      scope.currentPage = page;
      scope.onSelectPage({ page: page });
    }
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

.directive('pagination', ['paginationConfig', function(paginationConfig) {
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
    link: function(scope, element, attrs) {

      // Setup configuration parameters
      var boundaryLinks = angular.isDefined(attrs.boundaryLinks) ? scope.$eval(attrs.boundaryLinks) : paginationConfig.boundaryLinks;
      var directionLinks = angular.isDefined(attrs.directionLinks) ? scope.$eval(attrs.directionLinks) : paginationConfig.directionLinks;
      var firstText = angular.isDefined(attrs.firstText) ? scope.$parent.$eval(attrs.firstText) : paginationConfig.firstText;
      var previousText = angular.isDefined(attrs.previousText) ? scope.$parent.$eval(attrs.previousText) : paginationConfig.previousText;
      var nextText = angular.isDefined(attrs.nextText) ? scope.$parent.$eval(attrs.nextText) : paginationConfig.nextText;
      var lastText = angular.isDefined(attrs.lastText) ? scope.$parent.$eval(attrs.lastText) : paginationConfig.lastText;
      var rotate = angular.isDefined(attrs.rotate) ? scope.$eval(attrs.rotate) : paginationConfig.rotate;

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
        scope.pages = [];
        
        // Default page limits
        var startPage = 1, endPage = scope.numPages;
        var isMaxSized = ( angular.isDefined(scope.maxSize) && scope.maxSize < scope.numPages );

        // recompute if maxSize
        if ( isMaxSized ) {
          if ( rotate ) {
            // Current page is displayed in the middle of the visible ones
            startPage = Math.max(scope.currentPage - Math.floor(scope.maxSize/2), 1);
            endPage   = startPage + scope.maxSize - 1;

            // Adjust if limit is exceeded
            if (endPage > scope.numPages) {
              endPage   = scope.numPages;
              startPage = endPage - scope.maxSize + 1;
            }
          } else {
            // Visible pages are paginated with maxSize
            startPage = ((Math.ceil(scope.currentPage / scope.maxSize) - 1) * scope.maxSize) + 1;

            // Adjust last page if limit is exceeded
            endPage = Math.min(startPage + scope.maxSize - 1, scope.numPages);
          }
        }

        // Add page number links
        for (var number = startPage; number <= endPage; number++) {
          var page = makePage(number, number, scope.isActive(number), false);
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
          var previousPage = makePage(scope.currentPage - 1, previousText, false, scope.noPrevious());
          scope.pages.unshift(previousPage);

          var nextPage = makePage(scope.currentPage + 1, nextText, false, scope.noNext());
          scope.pages.push(nextPage);
        }

        // Add first & last links
        if (boundaryLinks) {
          var firstPage = makePage(1, firstText, false, scope.noPrevious());
          scope.pages.unshift(firstPage);

          var lastPage = makePage(scope.numPages, lastText, false, scope.noNext());
          scope.pages.push(lastPage);
        }

        if ( scope.currentPage > scope.numPages ) {
          scope.selectPage(scope.numPages);
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
      var previousText = angular.isDefined(attrs.previousText) ? scope.$parent.$eval(attrs.previousText) : config.previousText;
      var nextText = angular.isDefined(attrs.nextText) ? scope.$parent.$eval(attrs.nextText) : config.nextText;
      var align = angular.isDefined(attrs.align) ? scope.$parent.$eval(attrs.align) : config.align;

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
        scope.pages = [];

        // Add previous & next links
        var previousPage = makePage(scope.currentPage - 1, previousText, scope.noPrevious(), true, false);
        scope.pages.unshift(previousPage);

        var nextPage = makePage(scope.currentPage + 1, nextText, scope.noNext(), false, true);
        scope.pages.push(nextPage);

        if ( scope.currentPage > scope.numPages ) {
          scope.selectPage(scope.numPages);
        }
      });
    }
  };
}]);
