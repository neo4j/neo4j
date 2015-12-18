describe('pager directive', function () {
  var $compile, $rootScope, element;
  beforeEach(module('ui.bootstrap.pagination'));
  beforeEach(module('template/pagination/pager.html'));
  beforeEach(inject(function(_$compile_, _$rootScope_) {
    $compile = _$compile_;
    $rootScope = _$rootScope_;
    $rootScope.total = 47; // 5 pages
    $rootScope.currentPage = 3;
    element = $compile('<pager total-items="total" ng-model="currentPage"></pager>')($rootScope);
    $rootScope.$digest();
  }));

  function getPaginationBarSize() {
    return element.find('li').length;
  }

  function getPaginationEl(index) {
    return element.find('li').eq(index);
  }

  function clickPaginationEl(index) {
    getPaginationEl(index).find('a').click();
  }

  function updateCurrentPage(value) {
    $rootScope.currentPage = value;
    $rootScope.$digest();
  }

  it('has a "pager" css class', function() {
    expect(element.hasClass('pager')).toBe(true);
  });

  it('contains 2 li elements', function() {
    expect(getPaginationBarSize()).toBe(2);
    expect(getPaginationEl(0).text()).toBe('« Previous');
    expect(getPaginationEl(-1).text()).toBe('Next »');
  });

  it('aligns previous & next page', function() {
    expect(getPaginationEl(0)).toHaveClass('previous');
    expect(getPaginationEl(0)).not.toHaveClass('next');

    expect(getPaginationEl(-1)).not.toHaveClass('previous');
    expect(getPaginationEl(-1)).toHaveClass('next');
  });

  it('disables the "previous" link if current page is 1', function() {
    updateCurrentPage(1);
    expect(getPaginationEl(0)).toHaveClass('disabled');
  });

  it('disables the "next" link if current page is num-pages', function() {
    updateCurrentPage(5);
    expect(getPaginationEl(-1)).toHaveClass('disabled');
  });

  it('changes currentPage if the "previous" link is clicked', function() {
    clickPaginationEl(0);
    expect($rootScope.currentPage).toBe(2);
  });

  it('changes currentPage if the "next" link is clicked', function() {
    clickPaginationEl(-1);
    expect($rootScope.currentPage).toBe(4);
  });

  it('does not change the current page on "previous" click if already at first page', function() {
    updateCurrentPage(1);
    clickPaginationEl(0);
    expect($rootScope.currentPage).toBe(1);
  });

  it('does not change the current page on "next" click if already at last page', function() {
    updateCurrentPage(5);
    clickPaginationEl(-1);
    expect($rootScope.currentPage).toBe(5);
  });

  it('executes the `ng-change` expression when an element is clicked', function() {
    $rootScope.selectPageHandler = jasmine.createSpy('selectPageHandler');
    element = $compile('<pager total-items="total" ng-model="currentPage" ng-change="selectPageHandler()"></pager>')($rootScope);
    $rootScope.$digest();

    clickPaginationEl(-1);
    expect($rootScope.selectPageHandler).toHaveBeenCalled();
  });

  it('does not changes the number of pages when `total-items` changes', function() {
    $rootScope.total = 73; // 8 pages
    $rootScope.$digest();

    expect(getPaginationBarSize()).toBe(2);
    expect(getPaginationEl(0).text()).toBe('« Previous');
    expect(getPaginationEl(-1).text()).toBe('Next »');
  });

  describe('`items-per-page`', function () {
    beforeEach(function() {
      $rootScope.perpage = 5;
      element = $compile('<pager total-items="total" items-per-page="perpage" ng-model="currentPage"></pager>')($rootScope);
      $rootScope.$digest();
    });

    it('does not change the number of pages', function() {
      expect(getPaginationBarSize()).toBe(2);
      expect(getPaginationEl(0).text()).toBe('« Previous');
      expect(getPaginationEl(-1).text()).toBe('Next »');
    });

    it('selects the last page when it is too big', function() {
      $rootScope.perpage = 30;
      $rootScope.$digest();

      expect($rootScope.currentPage).toBe(2);
      expect(getPaginationBarSize()).toBe(2);
      expect(getPaginationEl(0)).not.toHaveClass('disabled');
      expect(getPaginationEl(-1)).toHaveClass('disabled');
    });
  });

  describe('when `page` is not a number', function () {
    it('handles string', function() {
      updateCurrentPage('1');
      expect(getPaginationEl(0)).toHaveClass('disabled');

      updateCurrentPage('05');
      expect(getPaginationEl(-1)).toHaveClass('disabled');
    });
  });

  describe('`num-pages`', function () {
    beforeEach(function() {
      $rootScope.numpg = null;
      element = $compile('<pager total-items="total" ng-model="currentPage" num-pages="numpg"></pager>')($rootScope);
      $rootScope.$digest();
    });

    it('equals to total number of pages', function() {
      expect($rootScope.numpg).toBe(5);
    });
  });

  describe('setting `pagerConfig`', function() {
    var originalConfig = {};
    beforeEach(inject(function(pagerConfig) {
      angular.extend(originalConfig, pagerConfig);
      pagerConfig.previousText = 'PR';
      pagerConfig.nextText = 'NE';
      pagerConfig.align = false;
      element = $compile('<pager total-items="total" ng-model="currentPage"></pager>')($rootScope);
      $rootScope.$digest();
    }));
    afterEach(inject(function(pagerConfig) {
      // return it to the original state
      angular.extend(pagerConfig, originalConfig);
    }));

    it('should change paging text', function () {
      expect(getPaginationEl(0).text()).toBe('PR');
      expect(getPaginationEl(-1).text()).toBe('NE');
    });

    it('should not align previous & next page link', function () {
      expect(getPaginationEl(0)).not.toHaveClass('previous');
      expect(getPaginationEl(-1)).not.toHaveClass('next');
    });
  });

  describe('override configuration from attributes', function () {
    beforeEach(function() {
      element = $compile('<pager align="false" previous-text="<" next-text=">" total-items="total" ng-model="currentPage"></pager>')($rootScope);
      $rootScope.$digest();
    });

    it('contains 2 li elements', function() {
      expect(getPaginationBarSize()).toBe(2);
    });

    it('should change paging text from attributes', function () {
      expect(getPaginationEl(0).text()).toBe('<');
      expect(getPaginationEl(-1).text()).toBe('>');
    });

    it('should not align previous & next page link', function () {
      expect(getPaginationEl(0)).not.toHaveClass('previous');
      expect(getPaginationEl(-1)).not.toHaveClass('next');
    });

    it('changes "previous" & "next" text from interpolated attributes', function() {
      $rootScope.previousText = '<<';
      $rootScope.nextText = '>>';
      element = $compile('<pager align="false" previous-text="{{previousText}}" next-text="{{nextText}}" total-items="total" ng-model="currentPage"></pager>')($rootScope);
      $rootScope.$digest();

      expect(getPaginationEl(0).text()).toBe('<<');
      expect(getPaginationEl(-1).text()).toBe('>>');
    });
  });

});
