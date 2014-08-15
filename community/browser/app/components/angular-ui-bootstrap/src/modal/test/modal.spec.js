describe('$modal', function () {
  var $rootScope, $document, $compile, $templateCache, $timeout, $q;
  var $modal, $modalProvider;

  var triggerKeyDown = function (element, keyCode) {
    var e = $.Event('keydown');
    e.which = keyCode;
    element.trigger(e);
  };

  var waitForBackdropAnimation = function () {
    inject(function ($transition) {
      if ($transition.transitionEndEventName) {
        $timeout.flush();
      }
    });
  };

  beforeEach(module('ui.bootstrap.modal'));
  beforeEach(module('template/modal/backdrop.html'));
  beforeEach(module('template/modal/window.html'));
  beforeEach(module(function(_$controllerProvider_, _$modalProvider_){
    $controllerProvider = _$controllerProvider_;
    $modalProvider = _$modalProvider_;
  }));

  beforeEach(inject(function (_$rootScope_, _$document_, _$compile_, _$templateCache_, _$timeout_, _$q_, _$modal_) {
    $rootScope = _$rootScope_;
    $document = _$document_;
    $compile = _$compile_;
    $templateCache = _$templateCache_;
    $timeout = _$timeout_;
    $q = _$q_;
    $modal = _$modal_;
  }));

  beforeEach(function () {
    this.addMatchers({

      toBeResolvedWith: function(value) {
        var resolved;
        this.message = function() {
          return 'Expected "' + angular.mock.dump(this.actual) + '" to be resolved with "' + value + '".';
        };
        this.actual.then(function(result){
          resolved = result;
        });
        $rootScope.$digest();

        return resolved === value;
      },

      toBeRejectedWith: function(value) {
        var rejected;
        this.message = function() {
          return 'Expected "' + angular.mock.dump(this.actual) + '" to be rejected with "' + value + '".';
        };
        this.actual.then(angular.noop, function(reason){
          rejected = reason;
        });
        $rootScope.$digest();

        return rejected === value;
      },

      toHaveModalOpenWithContent: function(content, selector) {

        var contentToCompare, modalDomEls = this.actual.find('body > div.modal > div.modal-dialog > div.modal-content');

        this.message = function() {
          return '"Expected "' + angular.mock.dump(modalDomEls) + '" to be open with "' + content + '".';
        };

        contentToCompare = selector ? modalDomEls.find(selector) : modalDomEls;
        return modalDomEls.css('display') === 'block' &&  contentToCompare.html() == content;
      },

      toHaveModalsOpen: function(noOfModals) {

        var modalDomEls = this.actual.find('body > div.modal');
        return modalDomEls.length === noOfModals;
      },

      toHaveBackdrop: function() {

        var backdropDomEls = this.actual.find('body > div.modal-backdrop');
        this.message = function() {
          return 'Expected "' + angular.mock.dump(backdropDomEls) + '" to be a backdrop element".';
        };

        return backdropDomEls.length === 1;
      }
    });
  });

  afterEach(function () {
    var body = $document.find('body');
    body.find('div.modal').remove();
    body.find('div.modal-backdrop').remove();
    body.removeClass('modal-open');
  });

  function open(modalOptions) {
    var modal = $modal.open(modalOptions);
    $rootScope.$digest();
    return modal;
  }

  function close(modal, result) {
    modal.close(result);
    $rootScope.$digest();
  }

  function dismiss(modal, reason) {
    modal.dismiss(reason);
    $timeout.flush();
    $rootScope.$digest();
  }

  describe('basic scenarios with default options', function () {

    it('should open and dismiss a modal with a minimal set of options', function () {

      var modal = open({template: '<div>Content</div>'});

      expect($document).toHaveModalsOpen(1);
      expect($document).toHaveModalOpenWithContent('Content', 'div');
      expect($document).toHaveBackdrop();

      dismiss(modal, 'closing in test');

      expect($document).toHaveModalsOpen(0);

      waitForBackdropAnimation();
      expect($document).not.toHaveBackdrop();
    });

    it('should open a modal from templateUrl', function () {

      $templateCache.put('content.html', '<div>URL Content</div>');
      var modal = open({templateUrl: 'content.html'});

      expect($document).toHaveModalsOpen(1);
      expect($document).toHaveModalOpenWithContent('URL Content', 'div');
      expect($document).toHaveBackdrop();

      dismiss(modal, 'closing in test');

      expect($document).toHaveModalsOpen(0);

      waitForBackdropAnimation();
      expect($document).not.toHaveBackdrop();
    });

    it('should support closing on ESC', function () {

      var modal = open({template: '<div>Content</div>'});
      expect($document).toHaveModalsOpen(1);

      triggerKeyDown($document, 27);
      $timeout.flush();
      $rootScope.$digest();

      expect($document).toHaveModalsOpen(0);
    });

    it('should support closing on backdrop click', function () {

      var modal = open({template: '<div>Content</div>'});
      expect($document).toHaveModalsOpen(1);

      $document.find('body > div.modal').click();
      $timeout.flush();
      $rootScope.$digest();

      expect($document).toHaveModalsOpen(0);
    });

    it('should resolve returned promise on close', function () {
      var modal = open({template: '<div>Content</div>'});
      close(modal, 'closed ok');

      expect(modal.result).toBeResolvedWith('closed ok');
    });

    it('should reject returned promise on dismiss', function () {

      var modal = open({template: '<div>Content</div>'});
      dismiss(modal, 'esc');

      expect(modal.result).toBeRejectedWith('esc');
    });

    it('should expose a promise linked to the templateUrl / resolve promises', function () {
      var modal = open({template: '<div>Content</div>', resolve: {
          ok: function() {return $q.when('ok');}
        }}
      );
      expect(modal.opened).toBeResolvedWith(true);
    });

    it('should expose a promise linked to the templateUrl / resolve promises and reject it if needed', function () {
      var modal = open({template: '<div>Content</div>', resolve: {
          ok: function() {return $q.reject('ko');}
        }}
      );
      expect(modal.opened).toBeRejectedWith(false);
    });

  });

  describe('default options can be changed in a provider', function () {

    it('should allow overriding default options in a provider', function () {

      $modalProvider.options.backdrop = false;
      var modal = open({template: '<div>Content</div>'});

      expect($document).toHaveModalOpenWithContent('Content', 'div');
      expect($document).not.toHaveBackdrop();
    });

    it('should accept new objects with default options in a provider', function () {

      $modalProvider.options = {
        backdrop: false
      };
      var modal = open({template: '<div>Content</div>'});

      expect($document).toHaveModalOpenWithContent('Content', 'div');
      expect($document).not.toHaveBackdrop();
    });
  });

  describe('option by option', function () {

    describe('template and templateUrl', function () {

      it('should throw an error if none of template and templateUrl are provided', function () {
        expect(function(){
          var modal = open({});
        }).toThrow(new Error('One of template or templateUrl options is required.'));
      });

      it('should not fail if a templateUrl contains leading / trailing white spaces', function () {

        $templateCache.put('whitespace.html', '  <div>Whitespaces</div>  ');
        open({templateUrl: 'whitespace.html'});
        expect($document).toHaveModalOpenWithContent('Whitespaces', 'div');
      });

    });

    describe('controller', function () {

      it('should accept controllers and inject modal instances', function () {
        var TestCtrl = function($scope, $modalInstance) {
          $scope.fromCtrl = 'Content from ctrl';
          $scope.isModalInstance = angular.isObject($modalInstance) && angular.isFunction($modalInstance.close);
        };

        var modal = open({template: '<div>{{fromCtrl}} {{isModalInstance}}</div>', controller: TestCtrl});
        expect($document).toHaveModalOpenWithContent('Content from ctrl true', 'div');
      });

      it('should accept controllerAs alias', function () {
        $controllerProvider.register('TestCtrl', function($modalInstance) {
          this.fromCtrl = 'Content from ctrl';
          this.isModalInstance = angular.isObject($modalInstance) && angular.isFunction($modalInstance.close);
        });

        var modal = open({template: '<div>{{test.fromCtrl}} {{test.isModalInstance}}</div>', controller: 'TestCtrl as test'});
        expect($document).toHaveModalOpenWithContent('Content from ctrl true', 'div');
      });

    });

    describe('resolve', function () {

      var ExposeCtrl = function($scope, value) {
        $scope.value = value;
      };

      function modalDefinition(template, resolve) {
        return {
          template: template,
          controller: ExposeCtrl,
          resolve: resolve
        };
      }

      it('should resolve simple values', function () {
        open(modalDefinition('<div>{{value}}</div>', {
          value: function () {
            return 'Content from resolve';
          }
        }));

        expect($document).toHaveModalOpenWithContent('Content from resolve', 'div');
      });

      it('should delay showing modal if one of the resolves is a promise', function () {

        open(modalDefinition('<div>{{value}}</div>', {
          value: function () {
            return $timeout(function(){ return 'Promise'; }, 100);
          }
        }));
        expect($document).toHaveModalsOpen(0);

        $timeout.flush();
        expect($document).toHaveModalOpenWithContent('Promise', 'div');
      });

      it('should not open dialog (and reject returned promise) if one of resolve fails', function () {

        var deferred = $q.defer();

        var modal = open(modalDefinition('<div>{{value}}</div>', {
          value: function () {
            return deferred.promise;
          }
        }));
        expect($document).toHaveModalsOpen(0);

        deferred.reject('error in test');
        $rootScope.$digest();

        expect($document).toHaveModalsOpen(0);
        expect(modal.result).toBeRejectedWith('error in test');
      });

      it('should support injection with minification-safe syntax in resolve functions', function () {

        open(modalDefinition('<div>{{value.id}}</div>', {
          value: ['$locale', function (e) {
            return e;
          }]
        }));

        expect($document).toHaveModalOpenWithContent('en-us', 'div');
      });

      //TODO: resolves with dependency injection - do we want to support them?
    });

    describe('scope', function () {

      it('should use custom scope if provided', function () {
        var $scope = $rootScope.$new();
        $scope.fromScope = 'Content from custom scope';
        open({
          template: '<div>{{fromScope}}</div>',
          scope: $scope
        });
        expect($document).toHaveModalOpenWithContent('Content from custom scope', 'div');
      });

      it('should create and use child of $rootScope if custom scope not provided', function () {

        var scopeTailBefore = $rootScope.$$childTail;

        $rootScope.fromScope = 'Content from root scope';
        open({
          template: '<div>{{fromScope}}</div>'
        });
        expect($document).toHaveModalOpenWithContent('Content from root scope', 'div');
      });
    });

    describe('keyboard', function () {

      it('should not close modals if keyboard option is set to false', function () {
        open({
          template: '<div>No keyboard</div>',
          keyboard: false
        });

        expect($document).toHaveModalsOpen(1);

        triggerKeyDown($document, 27);
        $rootScope.$digest();

        expect($document).toHaveModalsOpen(1);
      });
    });

    describe('backdrop', function () {

      it('should not have any backdrop element if backdrop set to false', function () {
        var modal =open({
          template: '<div>No backdrop</div>',
          backdrop: false
        });
        expect($document).toHaveModalOpenWithContent('No backdrop', 'div');
        expect($document).not.toHaveBackdrop();

        dismiss(modal);
        expect($document).toHaveModalsOpen(0);
      });

      it('should not close modal on backdrop click if backdrop is specified as "static"', function () {
        open({
          template: '<div>Static backdrop</div>',
          backdrop: 'static'
        });

        $document.find('body > div.modal-backdrop').click();
        $rootScope.$digest();

        expect($document).toHaveModalOpenWithContent('Static backdrop', 'div');
        expect($document).toHaveBackdrop();
      });

      it('should animate backdrop on each modal opening', function () {

        var modal = open({ template: '<div>With backdrop</div>' });
        var backdropEl = $document.find('body > div.modal-backdrop');
        expect(backdropEl).not.toHaveClass('in');

        $timeout.flush();
        expect(backdropEl).toHaveClass('in');

        dismiss(modal);
        waitForBackdropAnimation();

        modal = open({ template: '<div>With backdrop</div>' });
        backdropEl = $document.find('body > div.modal-backdrop');
        expect(backdropEl).not.toHaveClass('in');

      });
    });

    describe('custom window classes', function () {

      it('should support additional window class as string', function () {
        open({
          template: '<div>With custom window class</div>',
          windowClass: 'additional'
        });

        expect($document.find('div.modal')).toHaveClass('additional');
      });
    });
    
    describe('size', function () {
      
      it('should support creating small modal dialogs', function () {
        open({
          template: '<div>Small modal dialog</div>',
          size: 'sm'
        });
        
        expect($document.find('div.modal-dialog')).toHaveClass('modal-sm');
      });
      
      it('should support creating large modal dialogs', function () {
        open({
          template: '<div>Large modal dialog</div>',
          size: 'lg'
        });
        
        expect($document.find('div.modal-dialog')).toHaveClass('modal-lg');
      });
    });
  });

  describe('multiple modals', function () {

    it('it should allow opening of multiple modals', function () {

      var modal1 = open({template: '<div>Modal1</div>'});
      var modal2 = open({template: '<div>Modal2</div>'});
      expect($document).toHaveModalsOpen(2);

      dismiss(modal2);
      expect($document).toHaveModalsOpen(1);
      expect($document).toHaveModalOpenWithContent('Modal1', 'div');

      dismiss(modal1);
      expect($document).toHaveModalsOpen(0);
    });

    it('should not close any modals on ESC if the topmost one does not allow it', function () {

      var modal1 = open({template: '<div>Modal1</div>'});
      var modal2 = open({template: '<div>Modal2</div>', keyboard: false});

      triggerKeyDown($document, 27);
      $rootScope.$digest();

      expect($document).toHaveModalsOpen(2);
    });

    it('should not close any modals on click if a topmost modal does not have backdrop', function () {

      var modal1 = open({template: '<div>Modal1</div>'});
      var modal2 = open({template: '<div>Modal2</div>', backdrop: false});

      $document.find('body > div.modal-backdrop').click();
      $rootScope.$digest();

      expect($document).toHaveModalsOpen(2);
    });

    it('multiple modals should not interfere with default options', function () {

      var modal1 = open({template: '<div>Modal1</div>', backdrop: false});
      var modal2 = open({template: '<div>Modal2</div>'});
      $rootScope.$digest();

      expect($document).toHaveBackdrop();
    });

    it('should add "modal-open" class when a modal gets opened', function () {

      var body = $document.find('body');
      expect(body).not.toHaveClass('modal-open');

      var modal1 = open({template: '<div>Content1</div>'});
      expect(body).toHaveClass('modal-open');

      var modal2 = open({template: '<div>Content1</div>'});
      expect(body).toHaveClass('modal-open');

      dismiss(modal1);
      expect(body).toHaveClass('modal-open');

      dismiss(modal2);
      expect(body).not.toHaveClass('modal-open');
    });
  });
});
