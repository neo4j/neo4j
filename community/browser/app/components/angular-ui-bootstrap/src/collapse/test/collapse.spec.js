describe('collapse directive', function () {

  var scope, $compile, $timeout, $transition;
  var element;

  beforeEach(module('ui.bootstrap.collapse'));
  beforeEach(inject(function(_$rootScope_, _$compile_, _$timeout_, _$transition_) {
    scope = _$rootScope_;
    $compile = _$compile_;
    $timeout = _$timeout_;
    $transition = _$transition_;
  }));

  beforeEach(function() {
    element = $compile('<div collapse="isCollapsed">Some Content</div>')(scope);
    angular.element(document.body).append(element);
  });

  afterEach(function() {
    element.remove();
  });

  it('should be hidden on initialization if isCollapsed = true without transition', function() {
    scope.isCollapsed = true;
    scope.$digest();
    //No animation timeout here
    expect(element.height()).toBe(0);
  });

  it('should collapse if isCollapsed = true with animation on subsequent use', function() {
    scope.isCollapsed = false;
    scope.$digest();
    scope.isCollapsed = true;
    scope.$digest();
    $timeout.flush();
    expect(element.height()).toBe(0);
  });

  it('should be shown on initialization if isCollapsed = false without transition', function() {
    scope.isCollapsed = false;
    scope.$digest();
    //No animation timeout here
    expect(element.height()).not.toBe(0);
  });

  it('should expand if isCollapsed = false with animation on subsequent use', function() {
    scope.isCollapsed = false;
    scope.$digest();
    scope.isCollapsed = true;
    scope.$digest();
    scope.isCollapsed = false;
    scope.$digest();
    $timeout.flush();
    expect(element.height()).not.toBe(0);
  });

  it('should expand if isCollapsed = true with animation on subsequent uses', function() {
    scope.isCollapsed = false;
    scope.$digest();
    scope.isCollapsed = true;
    scope.$digest();
    scope.isCollapsed = false;
    scope.$digest();
    scope.isCollapsed = true;
    scope.$digest();
    $timeout.flush();
    expect(element.height()).toBe(0);
    if ($transition.transitionEndEventName) {
      element.triggerHandler($transition.transitionEndEventName);
      expect(element.height()).toBe(0);
    }
  });

  describe('dynamic content', function() {

    var element;

    beforeEach(function() {
      element = angular.element('<div collapse="isCollapsed"><p>Initial content</p><div ng-show="exp">Additional content</div></div>');
      $compile(element)(scope);
      angular.element(document.body).append(element);
    });

    afterEach(function() {
      element.remove();
    });

    it('should grow accordingly when content size inside collapse increases', function() {
      scope.exp = false;
      scope.isCollapsed = false;
      scope.$digest();
      var collapseHeight = element.height();
      scope.exp = true;
      scope.$digest();
      expect(element.height()).toBeGreaterThan(collapseHeight);
    });

    it('should shrink accordingly when content size inside collapse decreases', function() {
      scope.exp = true;
      scope.isCollapsed = false;
      scope.$digest();
      var collapseHeight = element.height();
      scope.exp = false;
      scope.$digest();
      expect(element.height()).toBeLessThan(collapseHeight);
    });

  });
});