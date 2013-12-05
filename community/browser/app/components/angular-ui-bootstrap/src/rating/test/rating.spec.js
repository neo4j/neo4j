describe('rating directive', function () {
  var $rootScope, element;
  beforeEach(module('ui.bootstrap.rating'));
  beforeEach(module('template/rating/rating.html'));
  beforeEach(inject(function(_$compile_, _$rootScope_) {
    $compile = _$compile_;
    $rootScope = _$rootScope_;
    $rootScope.rate = 3;
    element = $compile('<rating value="rate"></rating>')($rootScope);
    $rootScope.$digest();
  }));

  function getStars() {
    return element.find('i');
  }

  function getStar(number) {
    return getStars().eq( number - 1 );
  }

  function getState() {
    var stars = getStars();
    var state = [];
    for (var i = 0, n = stars.length; i < n; i++) {
      state.push( (stars.eq(i).hasClass('icon-star') && ! stars.eq(i).hasClass('icon-star-empty')) );
    }
    return state;
  }

  it('contains the default number of icons', function() {
    expect(getStars().length).toBe(5);
  });

  it('initializes the default star icons as selected', function() {
    expect(getState()).toEqual([true, true, true, false, false]);
  });

  it('handles correctly the click event', function() {
    getStar(2).click();
    $rootScope.$digest();
    expect(getState()).toEqual([true, true, false, false, false]);
    expect($rootScope.rate).toBe(2);

    getStar(5).click();
    $rootScope.$digest();
    expect(getState()).toEqual([true, true, true, true, true]);
    expect($rootScope.rate).toBe(5);
  });

  it('handles correctly the hover event', function() {
    getStar(2).trigger('mouseover');
    $rootScope.$digest();
    expect(getState()).toEqual([true, true, false, false, false]);
    expect($rootScope.rate).toBe(3);

    getStar(5).trigger('mouseover');
    $rootScope.$digest();
    expect(getState()).toEqual([true, true, true, true, true]);
    expect($rootScope.rate).toBe(3);

    element.trigger('mouseout');
    expect(getState()).toEqual([true, true, true, false, false]);
    expect($rootScope.rate).toBe(3);
  });

  it('changes the number of selected icons when value changes', function() {
    $rootScope.rate = 2;
    $rootScope.$digest();

    expect(getState()).toEqual([true, true, false, false, false]);
  });

  it('shows different number of icons when `max` attribute is set', function() {
    element = $compile('<rating value="rate" max="7"></rating>')($rootScope);
    $rootScope.$digest();

    expect(getStars().length).toBe(7);
  });

  it('shows different number of icons when `max` attribute is from scope variable', function() {
    $rootScope.max = 15;
    element = $compile('<rating value="rate" max="max"></rating>')($rootScope);
    $rootScope.$digest();
    expect(getStars().length).toBe(15);
  });

  it('handles readonly attribute', function() {
    $rootScope.isReadonly = true;
    element = $compile('<rating value="rate" readonly="isReadonly"></rating>')($rootScope);
    $rootScope.$digest();

    expect(getState()).toEqual([true, true, true, false, false]);

    var star5 = getStar(5);
    star5.trigger('mouseover');
    $rootScope.$digest();
    expect(getState()).toEqual([true, true, true, false, false]);

    $rootScope.isReadonly = false;
    $rootScope.$digest();

    star5.trigger('mouseover');
    $rootScope.$digest();
    expect(getState()).toEqual([true, true, true, true, true]);
  });

  it('should fire onHover', function() {
    $rootScope.hoveringOver = jasmine.createSpy('hoveringOver');
    element = $compile('<rating value="rate" on-hover="hoveringOver(value)"></rating>')($rootScope);
    $rootScope.$digest();

    getStar(3).trigger('mouseover');
    $rootScope.$digest();
    expect($rootScope.hoveringOver).toHaveBeenCalledWith(3);
  });

  it('should fire onLeave', function() {
    $rootScope.leaving = jasmine.createSpy('leaving');
    element = $compile('<rating value="rate" on-leave="leaving()"></rating>')($rootScope);
    $rootScope.$digest();

    element.trigger('mouseleave');
    $rootScope.$digest();
    expect($rootScope.leaving).toHaveBeenCalled();
  });

  describe('setting ratingConfig', function() {
    var originalConfig = {};
    beforeEach(inject(function(ratingConfig) {
      $rootScope.rate = 5;
      angular.extend(originalConfig, ratingConfig);
      ratingConfig.max = 10;
      element = $compile('<rating value="rate"></rating>')($rootScope);
      $rootScope.$digest();
    }));
    afterEach(inject(function(ratingConfig) {
      // return it to the original state
      angular.extend(ratingConfig, originalConfig);
    }));

    it('should change number of icon elements', function () {
      expect(getStars().length).toBe(10);
    });
  });
});
