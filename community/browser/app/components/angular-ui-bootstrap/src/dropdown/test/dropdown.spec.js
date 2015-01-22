describe('dropdownToggle', function() {
  var $compile, $rootScope, $document, element;

  beforeEach(module('ui.bootstrap.dropdown'));

  beforeEach(inject(function(_$compile_, _$rootScope_, _$document_) {
    $compile = _$compile_;
    $rootScope = _$rootScope_;
    $document = _$document_;
  }));

  var clickDropdownToggle = function(elm) {
    elm = elm || element;
    elm.find('a[dropdown-toggle]').click();
  };

  var triggerKeyDown = function (element, keyCode) {
    var e = $.Event('keydown');
    e.which = keyCode;
    element.trigger(e);
  };

  var isFocused = function(elm) {
    return elm[0] === document.activeElement;
  };

  describe('basic', function() {
    function dropdown() {
      return $compile('<li dropdown><a href dropdown-toggle></a><ul><li><a href>Hello</a></li></ul></li>')($rootScope);
    }

    beforeEach(function() {
      element = dropdown();
    });

    it('should toggle on `a` click', function() {
      expect(element.hasClass('open')).toBe(false);
      clickDropdownToggle();
      expect(element.hasClass('open')).toBe(true);
      clickDropdownToggle();
      expect(element.hasClass('open')).toBe(false);
    });

    it('should toggle when an option is clicked', function() {
      $document.find('body').append(element);
      expect(element.hasClass('open')).toBe(false);
      clickDropdownToggle();
      expect(element.hasClass('open')).toBe(true);

      var optionEl = element.find('ul > li').eq(0).find('a').eq(0);
      optionEl.click();
      expect(element.hasClass('open')).toBe(false);
      element.remove();
    });

    it('should close on document click', function() {
      clickDropdownToggle();
      expect(element.hasClass('open')).toBe(true);
      $document.click();
      expect(element.hasClass('open')).toBe(false);
    });

    it('should close on escape key & focus toggle element', function() {
      $document.find('body').append(element);
      clickDropdownToggle();
      triggerKeyDown($document, 27);
      expect(element.hasClass('open')).toBe(false);
      expect(isFocused(element.find('a'))).toBe(true);
      element.remove();
    });

    it('should not close on backspace key', function() {
      clickDropdownToggle();
      triggerKeyDown($document, 8);
      expect(element.hasClass('open')).toBe(true);
    });

    it('should close on $location change', function() {
      clickDropdownToggle();
      expect(element.hasClass('open')).toBe(true);
      $rootScope.$broadcast('$locationChangeSuccess');
      $rootScope.$apply();
      expect(element.hasClass('open')).toBe(false);
    });

    it('should only allow one dropdown to be open at once', function() {
      var elm1 = dropdown();
      var elm2 = dropdown();
      expect(elm1.hasClass('open')).toBe(false);
      expect(elm2.hasClass('open')).toBe(false);

      clickDropdownToggle( elm1 );
      expect(elm1.hasClass('open')).toBe(true);
      expect(elm2.hasClass('open')).toBe(false);

      clickDropdownToggle( elm2 );
      expect(elm1.hasClass('open')).toBe(false);
      expect(elm2.hasClass('open')).toBe(true);
    });

    it('should not toggle if the element has `disabled` class', function() {
      var elm = $compile('<li dropdown><a class="disabled" dropdown-toggle></a><ul><li>Hello</li></ul></li>')($rootScope);
      clickDropdownToggle( elm );
      expect(elm.hasClass('open')).toBe(false);
    });

    it('should not toggle if the element is disabled', function() {
      var elm = $compile('<li dropdown><button disabled="disabled" dropdown-toggle></button><ul><li>Hello</li></ul></li>')($rootScope);
      elm.find('button').click();
      expect(elm.hasClass('open')).toBe(false);
    });

    it('should not toggle if the element has `ng-disabled` as true', function() {
      $rootScope.isdisabled = true;
      var elm = $compile('<li dropdown><div ng-disabled="isdisabled" dropdown-toggle></div><ul><li>Hello</li></ul></li>')($rootScope);
      $rootScope.$digest();
      elm.find('div').click();
      expect(elm.hasClass('open')).toBe(false);

      $rootScope.isdisabled = false;
      $rootScope.$digest();
      elm.find('div').click();
      expect(elm.hasClass('open')).toBe(true);
    });

    it('should unbind events on scope destroy', function() {
      var $scope = $rootScope.$new();
      var elm = $compile('<li dropdown><button ng-disabled="isdisabled" dropdown-toggle></button><ul><li>Hello</li></ul></li>')($scope);
      $scope.$digest();

      var buttonEl = elm.find('button');
      buttonEl.click();
      expect(elm.hasClass('open')).toBe(true);
      buttonEl.click();
      expect(elm.hasClass('open')).toBe(false);

      $scope.$destroy();
      buttonEl.click();
      expect(elm.hasClass('open')).toBe(false);
    });

    // issue 270
    it('executes other document click events normally', function() {
      var checkboxEl = $compile('<input type="checkbox" ng-click="clicked = true" />')($rootScope);
      $rootScope.$digest();

      expect(element.hasClass('open')).toBe(false);
      expect($rootScope.clicked).toBeFalsy();

      clickDropdownToggle();
      expect(element.hasClass('open')).toBe(true);
      expect($rootScope.clicked).toBeFalsy();

      checkboxEl.click();
      expect($rootScope.clicked).toBeTruthy();
    });

    // WAI-ARIA
    it('should aria markup to the `dropdown-toggle`', function() {
      var toggleEl = element.find('a');
      expect(toggleEl.attr('aria-haspopup')).toBe('true');
      expect(toggleEl.attr('aria-expanded')).toBe('false');

      clickDropdownToggle();
      expect(toggleEl.attr('aria-expanded')).toBe('true');
      clickDropdownToggle();
      expect(toggleEl.attr('aria-expanded')).toBe('false');
    });
  });

  describe('integration with $location URL rewriting', function() {
    function dropdown() {

      // Simulate URL rewriting behavior
      $document.on('click', 'a[href="#something"]', function () {
        $rootScope.$broadcast('$locationChangeSuccess');
        $rootScope.$apply();
      });

      return $compile('<li dropdown><a href dropdown-toggle></a>' +
        '<ul><li><a href="#something">Hello</a></li></ul></li>')($rootScope);
    }

    beforeEach(function() {
      element = dropdown();
    });

    it('should close without errors on $location change', function() {
      $document.find('body').append(element);
      clickDropdownToggle();
      expect(element.hasClass('open')).toBe(true);
      var optionEl = element.find('ul > li').eq(0).find('a').eq(0);
      optionEl.click();
      expect(element.hasClass('open')).toBe(false);
    });
  });

  describe('without trigger', function() {
    beforeEach(function() {
      $rootScope.isopen = true;
      element = $compile('<li dropdown is-open="isopen"><ul><li>Hello</li></ul></li>')($rootScope);
      $rootScope.$digest();
    });

    it('should be open initially', function() {
      expect(element.hasClass('open')).toBe(true);
    });

    it('should toggle when `is-open` changes', function() {
      $rootScope.isopen = false;
      $rootScope.$digest();
      expect(element.hasClass('open')).toBe(false);
    });
  });

  describe('`is-open`', function() {
    beforeEach(function() {
      $rootScope.isopen = true;
      element = $compile('<li dropdown is-open="isopen"><a href dropdown-toggle></a><ul><li>Hello</li></ul></li>')($rootScope);
      $rootScope.$digest();
    });

    it('should be open initially', function() {
      expect(element.hasClass('open')).toBe(true);
    });

    it('should change `is-open` binding when toggles', function() {
      clickDropdownToggle();
      expect($rootScope.isopen).toBe(false);
    });

    it('should toggle when `is-open` changes', function() {
      $rootScope.isopen = false;
      $rootScope.$digest();
      expect(element.hasClass('open')).toBe(false);
    });

    it('focus toggle element when opening', function() {
      $document.find('body').append(element);
      clickDropdownToggle();
      $rootScope.isopen = false;
      $rootScope.$digest();
      expect(isFocused(element.find('a'))).toBe(false);
      $rootScope.isopen = true;
      $rootScope.$digest();
      expect(isFocused(element.find('a'))).toBe(true);
      element.remove();
    });
  });

  describe('`on-toggle`', function() {
    beforeEach(function() {
      $rootScope.toggleHandler = jasmine.createSpy('toggleHandler');
      $rootScope.isopen = false;
      element = $compile('<li dropdown on-toggle="toggleHandler(open)"  is-open="isopen"><a dropdown-toggle></a><ul><li>Hello</li></ul></li>')($rootScope);
      $rootScope.$digest();
    });

    it('should not have been called initially', function() {
      expect($rootScope.toggleHandler).not.toHaveBeenCalled();
    });

    it('should call it correctly when toggles', function() {
      $rootScope.isopen = true;
      $rootScope.$digest();
      expect($rootScope.toggleHandler).toHaveBeenCalledWith(true);

      clickDropdownToggle();
      expect($rootScope.toggleHandler).toHaveBeenCalledWith(false);
    });
  });

  describe('`on-toggle` with initially open', function() {
    beforeEach(function() {
      $rootScope.toggleHandler = jasmine.createSpy('toggleHandler');
      $rootScope.isopen = true;
      element = $compile('<li dropdown on-toggle="toggleHandler(open)" is-open="isopen"><a dropdown-toggle></a><ul><li>Hello</li></ul></li>')($rootScope);
      $rootScope.$digest();
    });

    it('should not have been called initially', function() {
      expect($rootScope.toggleHandler).not.toHaveBeenCalled();
    });

    it('should call it correctly when toggles', function() {
      $rootScope.isopen = false;
      $rootScope.$digest();
      expect($rootScope.toggleHandler).toHaveBeenCalledWith(false);

      $rootScope.isopen = true;
      $rootScope.$digest();
      expect($rootScope.toggleHandler).toHaveBeenCalledWith(true);
    });
  });

  describe('`on-toggle` without is-open', function() {
    beforeEach(function() {
      $rootScope.toggleHandler = jasmine.createSpy('toggleHandler');
      element = $compile('<li dropdown on-toggle="toggleHandler(open)"><a dropdown-toggle></a><ul><li>Hello</li></ul></li>')($rootScope);
      $rootScope.$digest();
    });

    it('should not have been called initially', function() {
      expect($rootScope.toggleHandler).not.toHaveBeenCalled();
    });

    it('should call it when clicked', function() {
      clickDropdownToggle();
      expect($rootScope.toggleHandler).toHaveBeenCalledWith(true);

      clickDropdownToggle();
      expect($rootScope.toggleHandler).toHaveBeenCalledWith(false);
    });
  });
});
