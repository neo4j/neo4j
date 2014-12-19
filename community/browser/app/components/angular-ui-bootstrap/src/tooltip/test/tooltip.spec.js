describe('tooltip', function() {
  var elm,
      elmBody,
      scope,
      elmScope,
      tooltipScope;

  // load the tooltip code
  beforeEach(module('ui.bootstrap.tooltip'));

  // load the template
  beforeEach(module('template/tooltip/tooltip-popup.html'));

  beforeEach(inject(function($rootScope, $compile) {
    elmBody = angular.element(
      '<div><span tooltip="tooltip text" tooltip-animation="false">Selector Text</span></div>'
    );

    scope = $rootScope;
    $compile(elmBody)(scope);
    scope.$digest();
    elm = elmBody.find('span');
    elmScope = elm.scope();
    tooltipScope = elmScope.$$childTail;
  }));

  it('should not be open initially', inject(function() {
    expect( tooltipScope.isOpen ).toBe( false );

    // We can only test *that* the tooltip-popup element wasn't created as the
    // implementation is templated and replaced.
    expect( elmBody.children().length ).toBe( 1 );
  }));

  it('should open on mouseenter', inject(function() {
    elm.trigger( 'mouseenter' );
    expect( tooltipScope.isOpen ).toBe( true );

    // We can only test *that* the tooltip-popup element was created as the
    // implementation is templated and replaced.
    expect( elmBody.children().length ).toBe( 2 );
  }));

  it('should close on mouseleave', inject(function() {
    elm.trigger( 'mouseenter' );
    elm.trigger( 'mouseleave' );
    expect( tooltipScope.isOpen ).toBe( false );
  }));

  it('should not animate on animation set to false', inject(function() {
    expect( tooltipScope.animation ).toBe( false );
  }));

  it('should have default placement of "top"', inject(function() {
    elm.trigger( 'mouseenter' );
    expect( tooltipScope.placement ).toBe( 'top' );
  }));

  it('should allow specification of placement', inject( function( $compile ) {
    elm = $compile( angular.element(
      '<span tooltip="tooltip text" tooltip-placement="bottom">Selector Text</span>'
    ) )( scope );
    scope.$apply();
    elmScope = elm.scope();
    tooltipScope = elmScope.$$childTail;

    elm.trigger( 'mouseenter' );
    expect( tooltipScope.placement ).toBe( 'bottom' );
  }));

  it('should work inside an ngRepeat', inject( function( $compile ) {

    elm = $compile( angular.element(
      '<ul>'+
        '<li ng-repeat="item in items">'+
          '<span tooltip="{{item.tooltip}}">{{item.name}}</span>'+
        '</li>'+
      '</ul>'
    ) )( scope );

    scope.items = [
      { name: 'One', tooltip: 'First Tooltip' }
    ];

    scope.$digest();

    var tt = angular.element( elm.find('li > span')[0] );

    tt.trigger( 'mouseenter' );

    expect( tt.text() ).toBe( scope.items[0].name );

    tooltipScope = tt.scope().$$childTail;
    expect( tooltipScope.content ).toBe( scope.items[0].tooltip );

    tt.trigger( 'mouseleave' );
  }));

  it('should show correct text when in an ngRepeat', inject( function( $compile, $timeout ) {

    elm = $compile( angular.element(
      '<ul>'+
        '<li ng-repeat="item in items">'+
          '<span tooltip="{{item.tooltip}}">{{item.name}}</span>'+
        '</li>'+
      '</ul>'
    ) )( scope );

    scope.items = [
      { name: 'One', tooltip: 'First Tooltip' },
      { name: 'Second', tooltip: 'Second Tooltip' }
    ];

    scope.$digest();

    var tt_1 = angular.element( elm.find('li > span')[0] );
    var tt_2 = angular.element( elm.find('li > span')[1] );

    tt_1.trigger( 'mouseenter' );
    tt_1.trigger( 'mouseleave' );

    $timeout.flush();

    tt_2.trigger( 'mouseenter' );

    expect( tt_1.text() ).toBe( scope.items[0].name );
    expect( tt_2.text() ).toBe( scope.items[1].name );

    tooltipScope = tt_2.scope().$$childTail;
    expect( tooltipScope.content ).toBe( scope.items[1].tooltip );
    expect( elm.find( '.tooltip-inner' ).text() ).toBe( scope.items[1].tooltip );

    tt_2.trigger( 'mouseleave' );

  }));

  it('should only have an isolate scope on the popup', inject( function ( $compile ) {
    var ttScope;

    scope.tooltipMsg = 'Tooltip Text';
    scope.alt = 'Alt Message';

    elmBody = $compile( angular.element(
      '<div><span alt={{alt}} tooltip="{{tooltipMsg}}" tooltip-animation="false">Selector Text</span></div>'
    ) )( scope );

    $compile( elmBody )( scope );
    scope.$digest();
    elm = elmBody.find( 'span' );
    elmScope = elm.scope();

    elm.trigger( 'mouseenter' );
    expect( elm.attr( 'alt' ) ).toBe( scope.alt );

    ttScope = angular.element( elmBody.children()[1] ).isolateScope();
    expect( ttScope.placement ).toBe( 'top' );
    expect( ttScope.content ).toBe( scope.tooltipMsg );

    elm.trigger( 'mouseleave' );

    //Isolate scope contents should be the same after hiding and showing again (issue 1191)
    elm.trigger( 'mouseenter' );

    ttScope = angular.element( elmBody.children()[1] ).isolateScope();
    expect( ttScope.placement ).toBe( 'top' );
    expect( ttScope.content ).toBe( scope.tooltipMsg );
  }));

  it('should not show tooltips if there is nothing to show - issue #129', inject(function ($compile) {

    elmBody = $compile(angular.element(
      '<div><span tooltip="">Selector Text</span></div>'
    ))(scope);
    scope.$digest();
    elmBody.find('span').trigger('mouseenter');

    expect(elmBody.children().length).toBe(1);
  }));

  it( 'should close the tooltip when its trigger element is destroyed', inject( function() {
    elm.trigger( 'mouseenter' );
    expect( tooltipScope.isOpen ).toBe( true );

    elm.remove();
    elmScope.$destroy();
    expect( elmBody.children().length ).toBe( 0 );
  }));

  it('issue 1191 - scope on the popup should always be child of correct element scope', function () {
    var ttScope;
    elm.trigger( 'mouseenter' );

    ttScope = angular.element( elmBody.children()[1] ).scope();
    expect( ttScope.$parent ).toBe( tooltipScope );

    elm.trigger( 'mouseleave' );

    // After leaving and coming back, the scope's parent should be the same
    elm.trigger( 'mouseenter' );

    ttScope = angular.element( elmBody.children()[1] ).scope();
    expect( ttScope.$parent ).toBe( tooltipScope );

    elm.trigger( 'mouseleave' );
  });

  describe('with specified enable expression', function() {

    beforeEach(inject(function ($compile) {
      scope.enable = false;
      elmBody = $compile(angular.element(
        '<div><span tooltip="tooltip text" tooltip-enable="enable">Selector Text</span></div>'
      ))(scope);
      scope.$digest();
      elm = elmBody.find('span');
      elmScope = elm.scope();
      tooltipScope = elmScope.$$childTail;

    }));

    it('should not open ', inject(function () {

      elm.trigger('mouseenter');
      expect(tooltipScope.isOpen).toBeFalsy();
      expect(elmBody.children().length).toBe(1);

    }));

    it('should open', inject(function () {

      scope.enable = true;
      scope.$digest();
      elm.trigger('mouseenter');
      expect(tooltipScope.isOpen).toBeTruthy();
      expect(elmBody.children().length).toBe(2);

    }));
  });

  describe('with specified popup delay', function () {

    beforeEach(inject(function ($compile) {
      scope.delay='1000';
      elm = $compile(angular.element(
        '<span tooltip="tooltip text" tooltip-popup-delay="{{delay}}">Selector Text</span>'
      ))(scope);
      elmScope = elm.scope();
      tooltipScope = elmScope.$$childTail;
      scope.$digest();
    }));

    it('should open after timeout', inject(function ($timeout) {

      elm.trigger('mouseenter');
      expect(tooltipScope.isOpen).toBe(false);

      $timeout.flush();
      expect(tooltipScope.isOpen).toBe(true);

    }));

    it('should not open if mouseleave before timeout', inject(function ($timeout) {
      elm.trigger('mouseenter');
      expect(tooltipScope.isOpen).toBe(false);

      elm.trigger('mouseleave');
      $timeout.flush();
      expect(tooltipScope.isOpen).toBe(false);
    }));

    it('should use default popup delay if specified delay is not a number', function(){
      scope.delay='text1000';
      scope.$digest();
      elm.trigger('mouseenter');
      expect(tooltipScope.isOpen).toBe(true);
    });

  });

  describe( 'with a trigger attribute', function() {
    var scope, elmBody, elm, elmScope;

    beforeEach( inject( function( $rootScope ) {
      scope = $rootScope;
    }));

    it( 'should use it to show but set the hide trigger based on the map for mapped triggers', inject( function( $compile ) {
      elmBody = angular.element(
        '<div><input tooltip="Hello!" tooltip-trigger="focus" /></div>'
      );
      $compile(elmBody)(scope);
      scope.$apply();
      elm = elmBody.find('input');
      elmScope = elm.scope();
      tooltipScope = elmScope.$$childTail;

      expect( tooltipScope.isOpen ).toBeFalsy();
      elm.trigger('focus');
      expect( tooltipScope.isOpen ).toBeTruthy();
      elm.trigger('blur');
      expect( tooltipScope.isOpen ).toBeFalsy();
    }));

    it( 'should use it as both the show and hide triggers for unmapped triggers', inject( function( $compile ) {
      elmBody = angular.element(
        '<div><input tooltip="Hello!" tooltip-trigger="fakeTriggerAttr" /></div>'
      );
      $compile(elmBody)(scope);
      scope.$apply();
      elm = elmBody.find('input');
      elmScope = elm.scope();
      tooltipScope = elmScope.$$childTail;

      expect( tooltipScope.isOpen ).toBeFalsy();
      elm.trigger('fakeTriggerAttr');
      expect( tooltipScope.isOpen ).toBeTruthy();
      elm.trigger('fakeTriggerAttr');
      expect( tooltipScope.isOpen ).toBeFalsy();
    }));

    it('should only set up triggers once', inject( function ($compile) {

      scope.test = true;
      elmBody = angular.element(
        '<div>' +
          '<input tooltip="Hello!" tooltip-trigger="{{ (test && \'mouseenter\' || \'click\') }}" />' +
          '<input tooltip="Hello!" tooltip-trigger="{{ (test && \'mouseenter\' || \'click\') }}" />' +
        '</div>'
      );

      $compile(elmBody)(scope);
      scope.$apply();
      var elm1 = elmBody.find('input').eq(0);
      var elm2 = elmBody.find('input').eq(1);
      var elmScope1 = elm1.scope();
      var elmScope2 = elm2.scope();
      var tooltipScope2 = elmScope2.$$childTail;

      scope.$apply('test = false');

      // click trigger isn't set
      elm2.click();
      expect( tooltipScope2.isOpen ).toBeFalsy();

      // mouseenter trigger is still set
      elm2.trigger('mouseenter');
      expect( tooltipScope2.isOpen ).toBeTruthy();
    }));
  });

  describe( 'with an append-to-body attribute', function() {
    var scope, elmBody, elm, elmScope, $body;

    beforeEach( inject( function( $rootScope ) {
      scope = $rootScope;
    }));

    afterEach(function () {
      $body.find('.tooltip').remove();
    });

    it( 'should append to the body', inject( function( $compile, $document ) {
      $body = $document.find( 'body' );
      elmBody = angular.element(
        '<div><span tooltip="tooltip text" tooltip-append-to-body="true">Selector Text</span></div>'
      );

      $compile(elmBody)(scope);
      scope.$digest();
      elm = elmBody.find('span');
      elmScope = elm.scope();
      tooltipScope = elmScope.$$childTail;

      var bodyLength = $body.children().length;
      elm.trigger( 'mouseenter' );

      expect( tooltipScope.isOpen ).toBe( true );
      expect( elmBody.children().length ).toBe( 1 );
      expect( $body.children().length ).toEqual( bodyLength + 1 );
    }));
  });

  describe('cleanup', function () {
    var elmBody, elm, elmScope, tooltipScope;

    function inCache() {
      var match = false;

      angular.forEach(angular.element.cache, function (item) {
        if (item.data && item.data.$scope === tooltipScope) {
          match = true;
        }
      });

      return match;
    }

    beforeEach(inject(function ( $compile, $rootScope ) {
      elmBody = angular.element('<div><input tooltip="Hello!" tooltip-trigger="fooTrigger" /></div>');

      $compile(elmBody)($rootScope);
      $rootScope.$apply();

      elm = elmBody.find('input');
      elmScope = elm.scope();
      elm.trigger('fooTrigger');
      tooltipScope = elmScope.$$childTail.$$childTail;
    }));

    it( 'should not contain a cached reference when not visible', inject( function( $timeout ) {
      expect( inCache() ).toBeTruthy();
      elmScope.$destroy();
      expect( inCache() ).toBeFalsy();
    }));
  });
});

describe('tooltipWithDifferentSymbols', function() {
    var elmBody;

    // load the tooltip code
    beforeEach(module('ui.bootstrap.tooltip'));

    // load the template
    beforeEach(module('template/tooltip/tooltip-popup.html'));

    // configure interpolate provider to use [[ ]] instead of {{ }}
    beforeEach(module( function($interpolateProvider) {
        $interpolateProvider.startSymbol('[[');
        $interpolateProvider.startSymbol(']]');
      }));

    it( 'should show the correct tooltip text', inject( function ( $compile, $rootScope ) {

      elmBody = angular.element(
        '<div><input type="text" tooltip="My tooltip" tooltip-trigger="focus" tooltip-placement="right" /></div>'
      );
      $compile(elmBody)($rootScope);
      $rootScope.$apply();
      var elmInput = elmBody.find('input');
      elmInput.trigger('focus');

      expect( elmInput.next().find('div').next().html() ).toBe('My tooltip');

    }));

});

describe( 'tooltipHtmlUnsafe', function() {
  var elm, elmBody, elmScope, tooltipScope, scope;

  // load the tooltip code
  beforeEach(module('ui.bootstrap.tooltip', function ( $tooltipProvider ) {
    $tooltipProvider.options({ animation: false });
  }));

  // load the template
  beforeEach(module('template/tooltip/tooltip-html-unsafe-popup.html'));

  beforeEach(inject(function($rootScope, $compile) {
    scope = $rootScope;
    scope.html = 'I say: <strong class="hello">Hello!</strong>';

    elmBody = $compile( angular.element(
      '<div><span tooltip-html-unsafe="{{html}}">Selector Text</span></div>'
    ))( scope );
    scope.$digest();
    elm = elmBody.find('span');
    elmScope = elm.scope();
    tooltipScope = elmScope.$$childTail;
  }));

  it( 'should render html properly', inject( function () {
    elm.trigger( 'mouseenter' );
    expect( elmBody.find('.tooltip-inner').html() ).toBe( scope.html );
  }));

  it( 'should show on mouseenter and hide on mouseleave', inject( function () {
    expect( tooltipScope.isOpen ).toBe( false );

    elm.trigger( 'mouseenter' );
    expect( tooltipScope.isOpen ).toBe( true );
    expect( elmBody.children().length ).toBe( 2 );

    expect( tooltipScope.content ).toEqual( scope.html );

    elm.trigger( 'mouseleave' );
    expect( tooltipScope.isOpen ).toBe( false );
    expect( elmBody.children().length ).toBe( 1 );
  }));
});

describe( '$tooltipProvider', function() {
  var elm,
      elmBody,
      scope,
      elmScope,
      tooltipScope;

  describe( 'popupDelay', function() {
    beforeEach(module('ui.bootstrap.tooltip', function($tooltipProvider){
      $tooltipProvider.options({popupDelay: 1000});
    }));

    // load the template
    beforeEach(module('template/tooltip/tooltip-popup.html'));

    beforeEach(inject(function($rootScope, $compile) {
      elmBody = angular.element(
        '<div><span tooltip="tooltip text">Selector Text</span></div>'
      );

      scope = $rootScope;
      $compile(elmBody)(scope);
      scope.$digest();
      elm = elmBody.find('span');
      elmScope = elm.scope();
      tooltipScope = elmScope.$$childTail;
    }));

    it('should open after timeout', inject(function($timeout) {

      elm.trigger( 'mouseenter' );
      expect( tooltipScope.isOpen ).toBe( false );

      $timeout.flush();
      expect( tooltipScope.isOpen ).toBe( true );

    }));

  });

  describe('appendToBody', function() {

    var $body;

    beforeEach(module('template/tooltip/tooltip-popup.html'));
    beforeEach(module('ui.bootstrap.tooltip', function ( $tooltipProvider ) {
        $tooltipProvider.options({ appendToBody: true });
    }));

    afterEach(function () {
      $body.find('.tooltip').remove();
    });


    it( 'should append to the body', inject( function( $rootScope, $compile, $document ) {
      $body = $document.find( 'body' );
      elmBody = angular.element(
        '<div><span tooltip="tooltip text">Selector Text</span></div>'
      );

      scope = $rootScope;
      $compile(elmBody)(scope);
      scope.$digest();
      elm = elmBody.find('span');
      elmScope = elm.scope();
      tooltipScope = elmScope.$$childTail;

      var bodyLength = $body.children().length;
      elm.trigger( 'mouseenter' );

      expect( tooltipScope.isOpen ).toBe( true );
      expect( elmBody.children().length ).toBe( 1 );
      expect( $body.children().length ).toEqual( bodyLength + 1 );
    }));

    it('should close on location change', inject( function( $rootScope, $compile) {

      elmBody = angular.element(
        '<div><span tooltip="tooltip text">Selector Text</span></div>'
      );

      scope = $rootScope;
      $compile(elmBody)(scope);
      scope.$digest();
      elm = elmBody.find('span');
      elmScope = elm.scope();
      tooltipScope = elmScope.$$childTail;

      elm.trigger( 'mouseenter' );
      expect( tooltipScope.isOpen ).toBe( true );

      scope.$broadcast('$locationChangeSuccess');
      scope.$digest();
      expect( tooltipScope.isOpen ).toBe( false );
    }));
  });

  describe( 'triggers', function() {
    describe( 'triggers with a mapped value', function() {
      beforeEach(module('ui.bootstrap.tooltip', function($tooltipProvider){
        $tooltipProvider.options({trigger: 'focus'});
      }));

      // load the template
      beforeEach(module('template/tooltip/tooltip-popup.html'));

      it( 'should use the show trigger and the mapped value for the hide trigger', inject( function ( $rootScope, $compile ) {
        elmBody = angular.element(
          '<div><input tooltip="tooltip text" /></div>'
        );

        scope = $rootScope;
        $compile(elmBody)(scope);
        scope.$digest();
        elm = elmBody.find('input');
        elmScope = elm.scope();
        tooltipScope = elmScope.$$childTail;

        expect( tooltipScope.isOpen ).toBeFalsy();
        elm.trigger('focus');
        expect( tooltipScope.isOpen ).toBeTruthy();
        elm.trigger('blur');
        expect( tooltipScope.isOpen ).toBeFalsy();
      }));

      it( 'should override the show and hide triggers if there is an attribute', inject( function ( $rootScope, $compile ) {
        elmBody = angular.element(
          '<div><input tooltip="tooltip text" tooltip-trigger="mouseenter"/></div>'
        );

        scope = $rootScope;
        $compile(elmBody)(scope);
        scope.$digest();
        elm = elmBody.find('input');
        elmScope = elm.scope();
        tooltipScope = elmScope.$$childTail;

        expect( tooltipScope.isOpen ).toBeFalsy();
        elm.trigger('mouseenter');
        expect( tooltipScope.isOpen ).toBeTruthy();
        elm.trigger('mouseleave');
        expect( tooltipScope.isOpen ).toBeFalsy();
      }));
    });

    describe( 'triggers with a custom mapped value', function() {
      beforeEach(module('ui.bootstrap.tooltip', function($tooltipProvider){
        $tooltipProvider.setTriggers({ 'customOpenTrigger': 'customCloseTrigger' });
        $tooltipProvider.options({trigger: 'customOpenTrigger'});
      }));

      // load the template
      beforeEach(module('template/tooltip/tooltip-popup.html'));

      it( 'should use the show trigger and the mapped value for the hide trigger', inject( function ( $rootScope, $compile ) {
        elmBody = angular.element(
          '<div><input tooltip="tooltip text" /></div>'
        );

        scope = $rootScope;
        $compile(elmBody)(scope);
        scope.$digest();
        elm = elmBody.find('input');
        elmScope = elm.scope();
        tooltipScope = elmScope.$$childTail;

        expect( tooltipScope.isOpen ).toBeFalsy();
        elm.trigger('customOpenTrigger');
        expect( tooltipScope.isOpen ).toBeTruthy();
        elm.trigger('customCloseTrigger');
        expect( tooltipScope.isOpen ).toBeFalsy();
      }));
    });

    describe( 'triggers without a mapped value', function() {
      beforeEach(module('ui.bootstrap.tooltip', function($tooltipProvider){
        $tooltipProvider.options({trigger: 'fakeTrigger'});
      }));

      // load the template
      beforeEach(module('template/tooltip/tooltip-popup.html'));

      it( 'should use the show trigger to hide', inject( function ( $rootScope, $compile ) {
        elmBody = angular.element(
          '<div><span tooltip="tooltip text">Selector Text</span></div>'
        );

        scope = $rootScope;
        $compile(elmBody)(scope);
        scope.$digest();
        elm = elmBody.find('span');
        elmScope = elm.scope();
        tooltipScope = elmScope.$$childTail;

        expect( tooltipScope.isOpen ).toBeFalsy();
        elm.trigger('fakeTrigger');
        expect( tooltipScope.isOpen ).toBeTruthy();
        elm.trigger('fakeTrigger');
        expect( tooltipScope.isOpen ).toBeFalsy();
      }));
    });
  });
});

