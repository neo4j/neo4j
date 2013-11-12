describe('pager directive with default configuration', function () {
  var $rootScope, element;
  beforeEach(module('ui.bootstrap.pagination'));
  beforeEach(module('template/pagination/pager.html'));
  beforeEach(inject(function(_$compile_, _$rootScope_) {
    $compile = _$compile_;
    $rootScope = _$rootScope_;
    $rootScope.numPages = 5;
    $rootScope.currentPage = 3;
    element = $compile('<pager num-pages="numPages" current-page="currentPage"></pager>')($rootScope);
    $rootScope.$digest();
  }));

  it('has a "pager" css class', function() {
    expect(element.hasClass('pager')).toBe(true);
  });

  it('contains 2 li elements', function() {
    expect(element.find('li').length).toBe(2);
    expect(element.find('li').eq(0).text()).toBe('« Previous');
    expect(element.find('li').eq(-1).text()).toBe('Next »');
  });

  it('aligns previous & next page', function() {
    expect(element.find('li').eq(0).hasClass('previous')).toBe(true);
    expect(element.find('li').eq(0).hasClass('next')).toBe(false);

    expect(element.find('li').eq(-1).hasClass('previous')).toBe(false);
    expect(element.find('li').eq(-1).hasClass('next')).toBe(true);
  });

  it('disables the "previous" link if current-page is 1', function() {
    $rootScope.currentPage = 1;
    $rootScope.$digest();
    expect(element.find('li').eq(0).hasClass('disabled')).toBe(true);
  });

  it('disables the "next" link if current-page is num-pages', function() {
    $rootScope.currentPage = 5;
    $rootScope.$digest();
    expect(element.find('li').eq(-1).hasClass('disabled')).toBe(true);
  });

  it('changes currentPage if the "previous" link is clicked', function() {
    var previous = element.find('li').eq(0).find('a').eq(0);
    previous.click();
    $rootScope.$digest();
    expect($rootScope.currentPage).toBe(2);
  });

  it('changes currentPage if the "next" link is clicked', function() {
    var next = element.find('li').eq(-1).find('a').eq(0);
    next.click();
    $rootScope.$digest();
    expect($rootScope.currentPage).toBe(4);
  });

  it('does not change the current page on "previous" click if already at first page', function() {
    var previous = element.find('li').eq(0).find('a').eq(0);
    $rootScope.currentPage = 1;
    $rootScope.$digest();
    previous.click();
    $rootScope.$digest();
    expect($rootScope.currentPage).toBe(1);
  });

  it('does not change the current page on "next" click if already at last page', function() {
    var next = element.find('li').eq(-1).find('a').eq(0);
    $rootScope.currentPage = 5;
    $rootScope.$digest();
    next.click();
    $rootScope.$digest();
    expect($rootScope.currentPage).toBe(5);
  });

  it('executes the onSelectPage expression when the current page changes', function() {
    $rootScope.selectPageHandler = jasmine.createSpy('selectPageHandler');
    element = $compile('<pager num-pages="numPages" current-page="currentPage" on-select-page="selectPageHandler(page)"></pager>')($rootScope);
    $rootScope.$digest();
    var next = element.find('li').eq(-1).find('a').eq(0);
    next.click();
    $rootScope.$digest();
    expect($rootScope.selectPageHandler).toHaveBeenCalledWith(4);
  });

  it('does not changes the number of items when numPages changes', function() {
    $rootScope.numPages = 8;
    $rootScope.$digest();
    expect(element.find('li').length).toBe(2);
    expect(element.find('li').eq(0).text()).toBe('« Previous');
    expect(element.find('li').eq(-1).text()).toBe('Next »');
  });

  it('sets the current page to the last page if the numPages is changed to less than the current page', function() {
    $rootScope.selectPageHandler = jasmine.createSpy('selectPageHandler');
    element = $compile('<pager num-pages="numPages" current-page="currentPage" on-select-page="selectPageHandler(page)"></pager>')($rootScope);
    $rootScope.$digest();
    $rootScope.numPages = 2;
    $rootScope.$digest();
    expect(element.find('li').length).toBe(2);
    expect($rootScope.currentPage).toBe(2);
    expect($rootScope.selectPageHandler).toHaveBeenCalledWith(2);
  });
});

describe('setting pagerConfig', function() {
  var $rootScope, element;
  var originalConfig = {};
  beforeEach(module('ui.bootstrap.pagination'));
  beforeEach(module('template/pagination/pager.html'));
  beforeEach(inject(function(_$compile_, _$rootScope_, pagerConfig) {
    $compile = _$compile_;
    $rootScope = _$rootScope_;
    $rootScope.numPages = 5;
    $rootScope.currentPage = 3;
    angular.extend(originalConfig, pagerConfig);
    pagerConfig.previousText = 'PR';
    pagerConfig.nextText = 'NE';
    pagerConfig.align = false;
    element = $compile('<pager num-pages="numPages" current-page="currentPage"></pager>')($rootScope);
    $rootScope.$digest();
  }));
  afterEach(inject(function(pagerConfig) {
    // return it to the original state
    angular.extend(pagerConfig, originalConfig);
  }));

  it('contains 2 li elements', function() {
    expect(element.find('li').length).toBe(2);
  });

  it('should change paging text', function () {
    expect(element.find('li').eq(0).text()).toBe('PR');
    expect(element.find('li').eq(-1).text()).toBe('NE');
  });

  it('should not align previous & next page link', function () {
    expect(element.find('li').eq(0).hasClass('previous')).toBe(false);
    expect(element.find('li').eq(-1).hasClass('next')).toBe(false);
  });

});

describe('pagination bypass configuration from attributes', function () {
  var $rootScope, element;
  beforeEach(module('ui.bootstrap.pagination'));
  beforeEach(module('template/pagination/pager.html'));
  beforeEach(inject(function(_$compile_, _$rootScope_) {
    $compile = _$compile_;
    $rootScope = _$rootScope_;
    $rootScope.numPages = 5;
    $rootScope.currentPage = 3;
    element = $compile('<pager align="false" previous-text="\'<\'" next-text="\'>\'" num-pages="numPages" current-page="currentPage"></pager>')($rootScope);
    $rootScope.$digest();
  }));

  it('contains 2 li elements', function() {
    expect(element.find('li').length).toBe(2);
  });

  it('should change paging text from attributes', function () {
    expect(element.find('li').eq(0).text()).toBe('<');
    expect(element.find('li').eq(-1).text()).toBe('>');
  });

  it('should not align previous & next page link', function () {
    expect(element.find('li').eq(0).hasClass('previous')).toBe(false);
    expect(element.find('li').eq(-1).hasClass('next')).toBe(false);
  });

});