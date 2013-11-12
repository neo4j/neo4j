describe('Give ui.boostrap.modal', function() {

	var $document, $compile, $scope, $rootScope, provider;

	beforeEach(module('ui.bootstrap.modal'));

	beforeEach(function(){
		module(function($dialogProvider){
			provider = $dialogProvider;
		});
		inject(function(_$document_, _$compile_, _$rootScope_){
			$document = _$document_;
			$compile = _$compile_;
			$scope = _$rootScope_.$new();
			$rootScope = _$rootScope_;
		});
	});

	var elm;

	var templateGenerator = function(expr, scopeExpressionContent, closeExpr) {
		var additionalExpression = scopeExpressionContent ? scopeExpressionContent : '';
		var closingExpr = closeExpr ? ' close="' + closeExpr + '" ': '';
		return '<div modal="' + expr + '" options="modalOpts"' + closingExpr + '>' +
					additionalExpression + 'Hello!</div>';
	};

	it('should have just one backdrop', function() {
		var numberOfSimultaneousModals = 5;
		var elems = [];
		for (var i = 0; i< 5; i++) {
			elems[i] = $compile(templateGenerator('modalShown' + i))($scope);
			$scope.$apply('modalShown' + i + ' = true');
		}	
		expect($document.find('body > div.modal-backdrop').length).toBe(1);
		expect($document.find('body > div.modal').length).toBe(numberOfSimultaneousModals);

		for (i = 0; i< 5; i++) {
			$scope.$apply('modalShown' + i + ' = false');
		}	
	});

	it('should work with expression instead of a variable', function() {			
		$scope.foo = true;
		$scope.shown = function() { return $scope.foo; };
		elm = $compile(templateGenerator('shown()'))($scope);
		$scope.$apply();
		expect($document.find('body > div.modal').length).toBe(1);
		$scope.$apply('foo = false');
		expect($document.find('body > div.modal').length).toBe(0);
	});

	it('should work with a close expression and escape close', function() {
		$scope.bar = true;
		$scope.show = function() { return $scope.bar; };
		elm = $compile(templateGenerator('show()', ' ', 'bar=false'))($scope);
		$scope.$apply();
		expect($document.find('body > div.modal').length).toBe(1);
		var e = $.Event('keydown');
		e.which = 27;
		$document.find('body').trigger(e);
		expect($document.find('body > div.modal').length).toBe(0);
		expect($scope.bar).not.toBeTruthy();
	});

	it('should work with a close expression and backdrop close', function() {
		$scope.baz = 1;
		$scope.hello = function() { return $scope.baz===1; };
		elm = $compile(templateGenerator('hello()', ' ', 'baz=0'))($scope);
		$scope.$apply();
		expect($document.find('body > div.modal').length).toBe(1);
		$document.find('body > div.modal-backdrop').click();
		expect($document.find('body > div.modal').length).toBe(0);
		expect($scope.baz).toBe(0);
	});

	it('should not close on escape if option is false', function() {
		$scope.modalOpts = {keyboard:false};
		elm = $compile(templateGenerator('modalShown'))($scope);
		$scope.modalShown = true;
		$scope.$apply();
		var e = $.Event('keydown');
		e.which = 27;
		expect($document.find('body > div.modal').length).toBe(1);
		$document.find('body').trigger(e);
		expect($document.find('body > div.modal').length).toBe(1);
		$scope.$apply('modalShown = false');
	});

	it('should not close on backdrop click if option is false', function() {
		$scope.modalOpts = {backdropClick:false};
		elm = $compile(templateGenerator('modalShown'))($scope);
		$scope.modalShown = true;
		$scope.$apply();
		expect($document.find('body > div.modal').length).toBe(1);
		$document.find('body > div.modal-backdrop').click();
		expect($document.find('body > div.modal').length).toBe(1);
		$scope.$apply('modalShown = false');
	});

	it('should use global $dialog options', function() {
		elm = $compile(templateGenerator('modalShown'))($scope);
		expect($document.find('.test-open-modal').length).toBe(0);
		$scope.$apply('modalShown = true');
		expect($document.find('body > div.modal').length).toBe(1);
		$scope.$apply('modalShown = false');
	});

	describe('dialog generated should have directives scope', function() {

		afterEach(function() {
			$scope.$apply('modalShown = false');
		});

		it('should call scope methods', function() {
			var clickSpy = jasmine.createSpy('localScopeFunction');
			$scope.myFunc = clickSpy;
			elm = $compile(templateGenerator('modalShown', '<button ng-click="myFunc()">Click</button>'))($scope);
			$scope.$apply('modalShown = true');
			$document.find('body > div.modal button').click();
			expect(clickSpy).toHaveBeenCalled();
		});

		it('should resolve scope vars', function() {
			$scope.buttonName = 'my button';
			elm = $compile(templateGenerator('modalShown', '<button>{{buttonName}}</button>'))($scope);
			$scope.$apply('modalShown = true');
			expect($document.find('body > div.modal button').text()).toBe('my button');
		});

	});

	describe('toogle modal dialog on model change', function() {

		beforeEach(function(){
			elm = $compile(templateGenerator('modalShown'))($scope);
			$scope.$apply('modalShown = true');
		});

		afterEach(function() {
			$scope.$apply('modalShown = false');
		});

		it('the backdrop should be displayed if specified (true by default)', function(){
			expect($document.find('body > div.modal-backdrop').css('display')).toBe('block');
		});

		it('the modal should be displayed', function(){
			expect($document.find('body > div.modal').css('display')).toBe('block');
		});

		it('the modal should not be displayed', function(){
			$scope.$apply('modalShown = false');
			expect($document.find('body > div.modal').length).toBe(0);
		});

		it('should update the model if the backdrop is clicked', function() {
			$document.find('body > div.modal-backdrop').click();
			$scope.$digest();
			expect($scope.modalShown).not.toBeTruthy();
		});

		it('should update the model if the esc is pressed', function() {
			var e = $.Event('keydown');
			e.which = 27;
			$document.find('body').trigger(e);
			$scope.$digest();
			expect($scope.modalShown).not.toBeTruthy();
		});
	});
});