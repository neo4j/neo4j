describe('typeaheadPopup - result rendering', function () {

  var scope, $rootScope, $compile;

  beforeEach(module('ui.bootstrap.typeahead'));
  beforeEach(module('template/typeahead/typeahead-popup.html'));
  beforeEach(module('template/typeahead/typeahead-match.html'));
  beforeEach(inject(function (_$rootScope_, _$compile_) {
    $rootScope = _$rootScope_;
    scope = $rootScope.$new();
    $compile = _$compile_;
  }));

  it('should render initial results', function () {

    scope.matches = ['foo', 'bar', 'baz'];
    scope.active = 1;

    var el = $compile("<div><typeahead-popup matches='matches' active='active' select='select(activeIdx)'></typeahead-popup></div>")(scope);
    $rootScope.$digest();

    var liElems = el.find('li');
    expect(liElems.length).toEqual(3);
    expect(liElems.eq(0)).not.toHaveClass('active');
    expect(liElems.eq(1)).toHaveClass('active');
    expect(liElems.eq(2)).not.toHaveClass('active');
  });

  it('should change active item on mouseenter', function () {

    scope.matches = ['foo', 'bar', 'baz'];
    scope.active = 1;

    var el = $compile("<div><typeahead-popup matches='matches' active='active' select='select(activeIdx)'></typeahead-popup></div>")(scope);
    $rootScope.$digest();

    var liElems = el.find('li');
    expect(liElems.eq(1)).toHaveClass('active');
    expect(liElems.eq(2)).not.toHaveClass('active');

    liElems.eq(2).trigger('mouseenter');

    expect(liElems.eq(1)).not.toHaveClass('active');
    expect(liElems.eq(2)).toHaveClass('active');
  });

  it('should select an item on mouse click', function () {

    scope.matches = ['foo', 'bar', 'baz'];
    scope.active = 1;
    $rootScope.select = angular.noop;
    spyOn($rootScope, 'select');

    var el = $compile("<div><typeahead-popup matches='matches' active='active' select='select(activeIdx)'></typeahead-popup></div>")(scope);
    $rootScope.$digest();

    var liElems = el.find('li');
    liElems.eq(2).find('a').trigger('click');
    expect($rootScope.select).toHaveBeenCalledWith(2);
  });
});
