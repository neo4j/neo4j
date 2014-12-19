describe('dismissOnTimeout', function () {

  var scope, $compile, $timeout;

  beforeEach(module('ui.bootstrap.alert'));
  beforeEach(module('template/alert/alert.html'));
  beforeEach(inject(function ($rootScope, _$compile_, _$timeout_) {
    scope = $rootScope;
    $compile = _$compile_;
    $timeout = _$timeout_;
  }));

  it('should close automatically if auto-dismiss is defined on the element', function () {
    scope.removeAlert = jasmine.createSpy();
    $compile('<alert close="removeAlert()" dismiss-on-timeout="500">Default alert!</alert>')(scope);
    scope.$digest();

    $timeout.flush();
    expect(scope.removeAlert).toHaveBeenCalled();
  });
});
