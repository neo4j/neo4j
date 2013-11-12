describe('pagination directive with default configuration', function () {
  var $rootScope, element;
  beforeEach(module('ui.bootstrap.pagination'));
  beforeEach(module('template/pagination/pagination.html'));
  beforeEach(inject(function(_$compile_, _$rootScope_) {
    $compile = _$compile_;
    $rootScope = _$rootScope_;
    $rootScope.numPages = 5;
    $rootScope.currentPage = 3;
    element = $compile('<pagination num-pages="numPages" current-page="currentPage"></pagination>')($rootScope);
    $rootScope.$digest();
  }));

  it('has a "pagination" css class', function() {
    expect(element.hasClass('pagination')).toBe(true);
  });

  it('contains one ul and num-pages + 2 li elements', function() {
    expect(element.find('ul').length).toBe(1);
    expect(element.find('li').length).toBe(7);
    expect(element.find('li').eq(0).text()).toBe('Previous');
    expect(element.find('li').eq(-1).text()).toBe('Next');
  });

  it('has the number of the page as text in each page item', function() {
    var lis = element.find('li');
    for(var i=1; i<=$rootScope.numPages;i++) {
      expect(lis.eq(i).text()).toEqual(''+i);
    }
  });

  it('sets the current-page to be active', function() {
    var currentPageItem = element.find('li').eq($rootScope.currentPage);
    expect(currentPageItem.hasClass('active')).toBe(true);
  });

  it('disables the "previous" link if current-page is 1', function() {
    $rootScope.currentPage = 1;
    $rootScope.$digest();
    var previousPageItem = element.find('li').eq(0);
    expect(previousPageItem.hasClass('disabled')).toBe(true);
  });

  it('disables the "next" link if current-page is num-pages', function() {
    $rootScope.currentPage = 5;
    $rootScope.$digest();
    var nextPageItem = element.find('li').eq(-1);
    expect(nextPageItem.hasClass('disabled')).toBe(true);
  });

  it('changes currentPage if a page link is clicked', function() {
    var page2 = element.find('li').eq(2).find('a');
    page2.click();
    $rootScope.$digest();
    expect($rootScope.currentPage).toBe(2);
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
    element = $compile('<pagination num-pages="numPages" current-page="currentPage" on-select-page="selectPageHandler(page)"></pagination>')($rootScope);
    $rootScope.$digest();
    var page2 = element.find('li').eq(2).find('a').eq(0);
    page2.click();
    $rootScope.$digest();
    expect($rootScope.selectPageHandler).toHaveBeenCalledWith(2);
  });

  it('changes the number of items when numPages changes', function() {
    $rootScope.numPages = 8;
    $rootScope.$digest();
    expect(element.find('li').length).toBe(10);
    expect(element.find('li').eq(0).text()).toBe('Previous');
    expect(element.find('li').eq(-1).text()).toBe('Next');
  });

  it('sets the current page to the last page if the numPages is changed to less than the current page', function() {
    $rootScope.selectPageHandler = jasmine.createSpy('selectPageHandler');
    element = $compile('<pagination num-pages="numPages" current-page="currentPage" on-select-page="selectPageHandler(page)"></pagination>')($rootScope);
    $rootScope.$digest();
    $rootScope.numPages = 2;
    $rootScope.$digest();
    expect(element.find('li').length).toBe(4);
    expect($rootScope.currentPage).toBe(2);
    expect($rootScope.selectPageHandler).toHaveBeenCalledWith(2);
  });
});

describe('pagination directive with max size option', function () {
  var $rootScope, element;
  beforeEach(module('ui.bootstrap.pagination'));
  beforeEach(module('template/pagination/pagination.html'));
  beforeEach(inject(function(_$compile_, _$rootScope_) {
    $compile = _$compile_;
    $rootScope = _$rootScope_;
    $rootScope.numPages = 10;
    $rootScope.currentPage = 3;
    $rootScope.maxSize = 5;
    element = $compile('<pagination num-pages="numPages" current-page="currentPage" max-size="maxSize"></pagination>')($rootScope);
    $rootScope.$digest();
  }));
  
  it('contains one ul and maxsize + 2 li elements', function() {
    expect(element.find('ul').length).toBe(1);
    expect(element.find('li').length).toBe($rootScope.maxSize + 2);
    expect(element.find('li').eq(0).text()).toBe('Previous');
    expect(element.find('li').eq(-1).text()).toBe('Next');
  });

  it('shows the page number even if it can\'t be shown in the middle', function() {
    $rootScope.currentPage = 1;
    $rootScope.$digest();
    var currentPageItem = element.find('li').eq(1);
    expect(currentPageItem.hasClass('active')).toBe(true);
    
    $rootScope.currentPage = 10;
    $rootScope.$digest();
    currentPageItem = element.find('li').eq(-2);
    expect(currentPageItem.hasClass('active')).toBe(true);
  });
  
  it('shows the page number in middle after the next link is clicked', function() {
    $rootScope.currentPage = 6;
    $rootScope.$digest();
    var next = element.find('li').eq(-1).find('a').eq(0);
    next.click();
    expect($rootScope.currentPage).toBe(7);
    var currentPageItem = element.find('li').eq(3);
    expect(currentPageItem.hasClass('active')).toBe(true);
    expect(parseInt(currentPageItem.text(), 10)).toBe($rootScope.currentPage);
  });
  
  it('shows the page number in middle after the prev link is clicked', function() {
    $rootScope.currentPage = 7;
    $rootScope.$digest();
    var prev = element.find('li').eq(0).find('a').eq(0);
    prev.click();
    expect($rootScope.currentPage).toBe(6);
    var currentPageItem = element.find('li').eq(3);
    expect(currentPageItem.hasClass('active')).toBe(true);
    expect(parseInt(currentPageItem.text(), 10)).toBe($rootScope.currentPage);
  });
  
  it('changes pagination bar size when max-size value changed', function() {
    $rootScope.maxSize = 7;
    $rootScope.$digest();
    expect(element.find('li').length).toBe(9);
  });  

  it('sets the pagination bar size to num-pages, if max-size is greater than num-pages ', function() {
    $rootScope.maxSize = 15;
    $rootScope.$digest();
    expect(element.find('li').length).toBe(12);
  });

  it('should not change value of max-size expression, if max-size is greater than num-pages ', function() {
    $rootScope.maxSize = 15;
    $rootScope.$digest();
    expect($rootScope.maxSize).toBe(15);
  });

  it('should not display page numbers, if max-size is zero', function() {
    $rootScope.maxSize = 0;
    $rootScope.$digest();
    expect(element.find('li').length).toBe(2);
    expect(element.find('li').eq(0).text()).toBe('Previous');
    expect(element.find('li').eq(-1).text()).toBe('Next');
  });

});

describe('pagination directive with max size option & no rotate', function () {
  var $rootScope, element;
  beforeEach(module('ui.bootstrap.pagination'));
  beforeEach(module('template/pagination/pagination.html'));
  beforeEach(inject(function(_$compile_, _$rootScope_) {
    $compile = _$compile_;
    $rootScope = _$rootScope_;
    $rootScope.numPages = 12;
    $rootScope.currentPage = 7;
    $rootScope.maxSize = 5;
    element = $compile('<pagination num-pages="numPages" current-page="currentPage" max-size="maxSize" rotate="false"></pagination>')($rootScope);
    $rootScope.$digest();
  }));

  it('contains one ul and maxsize + 4 elements', function() {
    expect(element.find('ul').length).toBe(1);
    expect(element.find('li').length).toBe($rootScope.maxSize + 4);
    expect(element.find('li').eq(0).text()).toBe('Previous');
    expect(element.find('li').eq(1).text()).toBe('...');
    expect(element.find('li').eq(2).text()).toBe('6');
    expect(element.find('li').eq(-3).text()).toBe('10');
    expect(element.find('li').eq(-2).text()).toBe('...');
    expect(element.find('li').eq(-1).text()).toBe('Next');
  });

  it('shows only the next ellipsis element on first page set', function() {
    $rootScope.currentPage = 3;
    $rootScope.$digest();
    expect(element.find('li').eq(1).text()).toBe('1');
    expect(element.find('li').eq(-3).text()).toBe('5');
    expect(element.find('li').eq(-2).text()).toBe('...');
  });

  it('shows only the previous ellipsis element on last page set', function() {
    $rootScope.currentPage = 12;
    $rootScope.$digest();
    expect(element.find('li').length).toBe(5);
    expect(element.find('li').eq(1).text()).toBe('...');
    expect(element.find('li').eq(2).text()).toBe('11');
    expect(element.find('li').eq(-2).text()).toBe('12');
  });

  it('moves to the previous set when first ellipsis is clicked', function() {
    var prev = element.find('li').eq(1).find('a').eq(0);
    expect(prev.text()).toBe('...');

    prev.click();
    expect($rootScope.currentPage).toBe(5);
    var currentPageItem = element.find('li').eq(-3);
    expect(currentPageItem.hasClass('active')).toBe(true);
  });

  it('moves to the next set when last ellipsis is clicked', function() {
    var next = element.find('li').eq(-2).find('a').eq(0);
    expect(next.text()).toBe('...');

    next.click();
    expect($rootScope.currentPage).toBe(11);
    var currentPageItem = element.find('li').eq(2);
    expect(currentPageItem.hasClass('active')).toBe(true);
  });

  it('should not display page numbers, if max-size is zero', function() {
    $rootScope.maxSize = 0;
    $rootScope.$digest();
    expect(element.find('li').length).toBe(2);
    expect(element.find('li').eq(0).text()).toBe('Previous');
    expect(element.find('li').eq(-1).text()).toBe('Next');
  });
});

describe('pagination directive with added first & last links', function () {
  var $rootScope, element;
  beforeEach(module('ui.bootstrap.pagination'));
  beforeEach(module('template/pagination/pagination.html'));
  beforeEach(inject(function(_$compile_, _$rootScope_) {
    $compile = _$compile_;
    $rootScope = _$rootScope_;
    $rootScope.numPages = 5;
    $rootScope.currentPage = 3;
    element = $compile('<pagination boundary-links="true" num-pages="numPages" current-page="currentPage"></pagination>')($rootScope);
    $rootScope.$digest();
  }));


  it('contains one ul and num-pages + 4 li elements', function() {
    expect(element.find('ul').length).toBe(1);
    expect(element.find('li').length).toBe(9);
    expect(element.find('li').eq(0).text()).toBe('First');
    expect(element.find('li').eq(1).text()).toBe('Previous');
    expect(element.find('li').eq(-2).text()).toBe('Next');
    expect(element.find('li').eq(-1).text()).toBe('Last');
  });

  it('has first and last li visible & with borders', function() {
    var firstLiEl = element.find('li').eq(0);
    var lastLiEl = element.find('li').eq(-1);

    expect(firstLiEl.text()).toBe('First');
    expect(firstLiEl.css('display')).not.toBe('none');
    expect(lastLiEl.text()).toBe('Last');
    expect(lastLiEl.css('display')).not.toBe('none');
  });


  it('disables the "first" & "previous" link if current-page is 1', function() {
    $rootScope.currentPage = 1;
    $rootScope.$digest();
    expect(element.find('li').eq(0).hasClass('disabled')).toBe(true);
    expect(element.find('li').eq(1).hasClass('disabled')).toBe(true);
  });

  it('disables the "last" & "next" link if current-page is num-pages', function() {
    $rootScope.currentPage = 5;
    $rootScope.$digest();
    expect(element.find('li').eq(-2).hasClass('disabled')).toBe(true);
    expect(element.find('li').eq(-1).hasClass('disabled')).toBe(true);
  });


  it('changes currentPage if the "first" link is clicked', function() {
    var first = element.find('li').eq(0).find('a').eq(0);
    first.click();
    $rootScope.$digest();
    expect($rootScope.currentPage).toBe(1);
  });

  it('changes currentPage if the "last" link is clicked', function() {
    var last = element.find('li').eq(-1).find('a').eq(0);
    last.click();
    $rootScope.$digest();
    expect($rootScope.currentPage).toBe($rootScope.numPages);
  });

  it('does not change the current page on "first" click if already at first page', function() {
    var first = element.find('li').eq(0).find('a').eq(0);
    $rootScope.currentPage = 1;
    $rootScope.$digest();
    first.click();
    $rootScope.$digest();
    expect($rootScope.currentPage).toBe(1);
  });

  it('does not change the current page on "last" click if already at last page', function() {
    var last = element.find('li').eq(-1).find('a').eq(0);
    $rootScope.currentPage = $rootScope.numPages;
    $rootScope.$digest();
    last.click();
    $rootScope.$digest();
    expect($rootScope.currentPage).toBe($rootScope.numPages);
  });

  it('changes "first" & "last" text from attributes', function() {
    element = $compile('<pagination boundary-links="true" first-text="\'<<<\'" last-text="\'>>>\'" num-pages="numPages" current-page="currentPage"></pagination>')($rootScope);
    $rootScope.$digest();

    expect(element.find('li').eq(0).text()).toBe('<<<');
    expect(element.find('li').eq(-1).text()).toBe('>>>');
  });

  it('changes "previous" & "next" text from attributes', function() {
    element = $compile('<pagination boundary-links="true" previous-text="\'<<\'" next-text="\'>>\'" num-pages="numPages" current-page="currentPage"></pagination>')($rootScope);
    $rootScope.$digest();

    expect(element.find('li').eq(1).text()).toBe('<<');
    expect(element.find('li').eq(-2).text()).toBe('>>');
  });

  it('changes "first" & "last" text from attribute variables', function() {
    $rootScope.myfirstText = '<<<';
    $rootScope.mylastText = '>>>';
    element = $compile('<pagination boundary-links="true" first-text="myfirstText" last-text="mylastText" num-pages="numPages" current-page="currentPage"></pagination>')($rootScope);
    $rootScope.$digest();

    expect(element.find('li').eq(0).text()).toBe('<<<');
    expect(element.find('li').eq(-1).text()).toBe('>>>');
  });

  it('changes "previous" & "next" text from attribute variables', function() {
    $rootScope.previousText = '<<';
    $rootScope.nextText = '>>';
    element = $compile('<pagination boundary-links="true" previous-text="previousText" next-text="nextText" num-pages="numPages" current-page="currentPage"></pagination>')($rootScope);
    $rootScope.$digest();

    expect(element.find('li').eq(1).text()).toBe('<<');
    expect(element.find('li').eq(-2).text()).toBe('>>');
  });

});

describe('pagination directive with just number links', function () {
  var $rootScope, element;
  beforeEach(module('ui.bootstrap.pagination'));
  beforeEach(module('template/pagination/pagination.html'));
  beforeEach(inject(function(_$compile_, _$rootScope_) {
    $compile = _$compile_;
    $rootScope = _$rootScope_;
    $rootScope.numPages = 5;
    $rootScope.currentPage = 3;
    element = $compile('<pagination direction-links="false" num-pages="numPages" current-page="currentPage"></pagination>')($rootScope);
    $rootScope.$digest();
  }));

  it('has a "pagination" css class', function() {
    expect(element.hasClass('pagination')).toBe(true);
  });

  it('contains one ul and num-pages li elements', function() {
    expect(element.find('ul').length).toBe(1);
    expect(element.find('li').length).toBe(5);
    expect(element.find('li').eq(0).text()).toBe('1');
    expect(element.find('li').eq(-1).text()).toBe(''+$rootScope.numPages);
  });

  it('has the number of the page as text in each page item', function() {
    var lis = element.find('li');
    for(var i=0; i<$rootScope.numPages;i++) {
      expect(lis.eq(i).text()).toEqual(''+(i+1));
    }
  });

  it('sets the current-page to be active', function() {
    var currentPageItem = element.find('li').eq($rootScope.currentPage-1);
    expect(currentPageItem.hasClass('active')).toBe(true);
  });

  it('does not disable the "1" link if current-page is 1', function() {
    $rootScope.currentPage = 1;
    $rootScope.$digest();
    var onePageItem = element.find('li').eq(0);
    expect(onePageItem.hasClass('disabled')).toBe(false);
    expect(onePageItem.hasClass('active')).toBe(true);
  });

  it('does not disable the "numPages" link if current-page is num-pages', function() {
    $rootScope.currentPage = 5;
    $rootScope.$digest();
    var lastPageItem = element.find('li').eq(-1);
    expect(lastPageItem.hasClass('disabled')).toBe(false);
    expect(lastPageItem.hasClass('active')).toBe(true);
  });

  it('changes currentPage if a page link is clicked', function() {
    var page2 = element.find('li').eq(1).find('a');
    page2.click();
    $rootScope.$digest();
    expect($rootScope.currentPage).toBe(2);
  });


  it('executes the onSelectPage expression when the current page changes', function() {
    $rootScope.selectPageHandler = jasmine.createSpy('selectPageHandler');
    element = $compile('<pagination direction-links="false" num-pages="numPages" current-page="currentPage" on-select-page="selectPageHandler(page)"></pagination>')($rootScope);
    $rootScope.$digest();
    var page2 = element.find('li').eq(1).find('a').eq(0);
    page2.click();
    $rootScope.$digest();
    expect($rootScope.selectPageHandler).toHaveBeenCalledWith(2);
  });

  it('changes the number of items when numPages changes', function() {
    $rootScope.numPages = 8;
    $rootScope.$digest();
    expect(element.find('li').length).toBe(8);
    expect(element.find('li').eq(0).text()).toBe('1');
    expect(element.find('li').eq(-1).text()).toBe(''+$rootScope.numPages);
  });

  it('sets the current page to the last page if the numPages is changed to less than the current page', function() {
    $rootScope.selectPageHandler = jasmine.createSpy('selectPageHandler');
    element = $compile('<pagination direction-links="false" num-pages="numPages" current-page="currentPage" on-select-page="selectPageHandler(page)"></pagination>')($rootScope);
    $rootScope.$digest();
    $rootScope.numPages = 2;
    $rootScope.$digest();
    expect(element.find('li').length).toBe(2);
    expect($rootScope.currentPage).toBe(2);
    expect($rootScope.selectPageHandler).toHaveBeenCalledWith(2);
  });
});

describe('setting paginationConfig', function() {
  var $rootScope, element;
  var originalConfig = {};
  beforeEach(module('ui.bootstrap.pagination'));
  beforeEach(module('template/pagination/pagination.html'));
  beforeEach(inject(function(_$compile_, _$rootScope_, paginationConfig) {
    $compile = _$compile_;
    $rootScope = _$rootScope_;
    $rootScope.numPages = 5;
    $rootScope.currentPage = 3;
    angular.extend(originalConfig, paginationConfig);
    paginationConfig.boundaryLinks = true;
    paginationConfig.directionLinks = true;
    paginationConfig.firstText = 'FI';
    paginationConfig.previousText = 'PR';
    paginationConfig.nextText = 'NE';
    paginationConfig.lastText = 'LA';
    element = $compile('<pagination num-pages="numPages" current-page="currentPage"></pagination>')($rootScope);
    $rootScope.$digest();
  }));
  afterEach(inject(function(paginationConfig) {
    // return it to the original state
    angular.extend(paginationConfig, originalConfig);
  }));

  it('should change paging text', function () {
    expect(element.find('li').eq(0).text()).toBe('FI');
    expect(element.find('li').eq(1).text()).toBe('PR');
    expect(element.find('li').eq(-2).text()).toBe('NE');
    expect(element.find('li').eq(-1).text()).toBe('LA');
  });

  it('contains one ul and num-pages + 4 li elements', function() {
    expect(element.find('ul').length).toBe(1);
    expect(element.find('li').length).toBe(9);
  });

});


describe('pagination directive with first, last & number links', function () {
  var $rootScope, element;
  beforeEach(module('ui.bootstrap.pagination'));
  beforeEach(module('template/pagination/pagination.html'));
  beforeEach(inject(function(_$compile_, _$rootScope_) {
    $compile = _$compile_;
    $rootScope = _$rootScope_;
    $rootScope.numPages = 5;
    $rootScope.currentPage = 3;
    element = $compile('<pagination boundary-links="true" direction-links="false" num-pages="numPages" current-page="currentPage"></pagination>')($rootScope);
    $rootScope.$digest();
  }));


  it('contains one ul and num-pages + 2 li elements', function() {
    expect(element.find('ul').length).toBe(1);
    expect(element.find('li').length).toBe(7);
    expect(element.find('li').eq(0).text()).toBe('First');
    expect(element.find('li').eq(1).text()).toBe('1');
    expect(element.find('li').eq(-2).text()).toBe(''+$rootScope.numPages);
    expect(element.find('li').eq(-1).text()).toBe('Last');
  });


  it('disables the "first" & activates "1" link if current-page is 1', function() {
    $rootScope.currentPage = 1;
    $rootScope.$digest();
    expect(element.find('li').eq(0).hasClass('disabled')).toBe(true);
    expect(element.find('li').eq(1).hasClass('disabled')).toBe(false);
    expect(element.find('li').eq(1).hasClass('active')).toBe(true);
  });

  it('disables the "last" & "next" link if current-page is num-pages', function() {
    $rootScope.currentPage = 5;
    $rootScope.$digest();
    expect(element.find('li').eq(-2).hasClass('disabled')).toBe(false);
    expect(element.find('li').eq(-2).hasClass('active')).toBe(true);
    expect(element.find('li').eq(-1).hasClass('disabled')).toBe(true);
  });


  it('changes currentPage if the "first" link is clicked', function() {
    var first = element.find('li').eq(0).find('a').eq(0);
    first.click();
    $rootScope.$digest();
    expect($rootScope.currentPage).toBe(1);
  });

  it('changes currentPage if the "last" link is clicked', function() {
    var last = element.find('li').eq(-1).find('a').eq(0);
    last.click();
    $rootScope.$digest();
    expect($rootScope.currentPage).toBe($rootScope.numPages);
  });

});

describe('pagination bypass configuration from attributes', function () {
  var $rootScope, element;
  beforeEach(module('ui.bootstrap.pagination'));
  beforeEach(module('template/pagination/pagination.html'));
  beforeEach(inject(function(_$compile_, _$rootScope_) {
    $compile = _$compile_;
    $rootScope = _$rootScope_;
    $rootScope.numPages = 5;
    $rootScope.currentPage = 3;
    element = $compile('<pagination boundary-links="true" first-text="\'<<\'" previous-text="\'<\'" next-text="\'>\'" last-text="\'>>\'" num-pages="numPages" current-page="currentPage"></pagination>')($rootScope);
    $rootScope.$digest();
  }));

  it('contains one ul and num-pages + 4 li elements', function() {
    expect(element.find('ul').length).toBe(1);
    expect(element.find('li').length).toBe(9);
  });

  it('should change paging text from attribute', function () {
    expect(element.find('li').eq(0).text()).toBe('<<');
    expect(element.find('li').eq(1).text()).toBe('<');
    expect(element.find('li').eq(-2).text()).toBe('>');
    expect(element.find('li').eq(-1).text()).toBe('>>');
  });

});
