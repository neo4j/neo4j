describe('popover', function() {
  var elm,
      elmBody,
      scope,
      elmScope;

  // load the popover code
  beforeEach(module('ui.bootstrap.popover'));

  // load the template
  beforeEach(module('template/popover/popover.html'));

  beforeEach(inject(function($rootScope, $compile) {
    elmBody = angular.element(
      '<div><span popover="popover text">Selector Text</span></div>'
    );

    scope = $rootScope;
    $compile(elmBody)(scope);
    scope.$digest();
    elm = elmBody.find('span');
    elmScope = elm.scope();
  }));

  it('should not be open initially', inject(function() {
    expect( elmScope.tt_isOpen ).toBe( false );

    // We can only test *that* the popover-popup element wasn't created as the
    // implementation is templated and replaced.
    expect( elmBody.children().length ).toBe( 1 );
  }));

  it('should open on click', inject(function() {
    elm.trigger( 'click' );
    expect( elmScope.tt_isOpen ).toBe( true );

    // We can only test *that* the popover-popup element was created as the
    // implementation is templated and replaced.
    expect( elmBody.children().length ).toBe( 2 );
  }));

  it('should close on second click', inject(function() {
    elm.trigger( 'click' );
    elm.trigger( 'click' );
    expect( elmScope.tt_isOpen ).toBe( false );
  }));

  it('should not unbind event handlers created by other directives - issue 456', inject( function( $compile ) {

    scope.click = function() {
      scope.clicked = !scope.clicked;
    };

    elmBody = angular.element(
      '<div><input popover="Hello!" ng-click="click()" popover-trigger="mouseenter"/></div>'
    );
    $compile(elmBody)(scope);
    scope.$digest();

    elm = elmBody.find('input');

    elm.trigger( 'mouseenter' );
    elm.trigger( 'mouseleave' );
    expect(scope.clicked).toBeFalsy();

    elm.click();
    expect(scope.clicked).toBeTruthy();
  }));
});


