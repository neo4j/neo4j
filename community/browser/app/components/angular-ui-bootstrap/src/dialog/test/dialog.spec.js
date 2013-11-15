describe('Given ui.bootstrap.dialog', function(){

	var $document, $compile, $scope, $rootScope, $dialog, q, provider;
	var template = '<div>I\'m a template</div> ';

	beforeEach(module('ui.bootstrap.dialog'));
	beforeEach(module('template/dialog/message.html'));

	beforeEach(function(){
		module(function($dialogProvider){
			provider = $dialogProvider;
		});
		inject(function(_$document_, _$compile_, _$rootScope_, _$dialog_, _$q_){
			$document = _$document_;
			$compile = _$compile_;
			$scope = _$rootScope_.$new();
			$rootScope = _$rootScope_;
			$dialog = _$dialog_;
			q = _$q_;
		});
	});

	// clean-up after ourselves
	afterEach(function(){
		closeDialog();
		clearGlobalOptions();
	});

	it('provider service should be injected', function(){
		expect(provider).toBeDefined();
	});

	it('dialog service should be injected', function(){
		expect($dialog).toBeDefined();
	});

	var dialog;

	var createDialog = function(opts){
		dialog = $dialog.dialog(opts);
	};

	var openDialog = function(templateUrl, controller){
		dialog.open(templateUrl, controller);
		$scope.$apply();
	};

	var closeDialog = function(result){
		if(dialog){
			dialog.close(result);
			$rootScope.$apply();
		}
	};

	var setGlobalOptions = function(opts){
		provider.options(opts);
	};

	var clearGlobalOptions = function(){
		provider.options({});
	};
	

	var dialogShouldBeClosed = function(){
		it('should not include a backdrop in the DOM', function(){
			expect($document.find('body > div.modal-backdrop').length).toBe(0);
		});

		it('should not include the modal in the DOM', function(){
			expect($document.find('body > div.modal').length).toBe(0);
		});

		it('should return false for isOpen()', function(){
			expect(dialog.isOpen()).toBeFalsy();
		});
	};

	var dialogShouldBeOpen = function(){
		it('the dialog.isOpen() should be true', function(){
			expect(dialog.isOpen()).toBe(true);
		});

		it('the backdrop should be displayed', function(){
			expect($document.find('body > div.modal-backdrop').css('display')).toBe('block');
		});

		it('the modal should be displayed', function(){
			expect($document.find('body > div.modal').css('display')).toBe('block');
		});
	};

	describe('Given global option', function(){

		var useDialogWithGlobalOption = function(opts){
			beforeEach(function(){
				setGlobalOptions(opts);
				createDialog({template:template});
				openDialog();
			});
		};

		describe('backdrop:false', function(){
			useDialogWithGlobalOption({backdrop: false});

			it('should not include a backdrop in the DOM', function(){
				expect($document.find('body > div.modal-backdrop').length).toBe(0);
			});

			it('should include the modal in the DOM', function(){
				expect($document.find('body > div.modal').length).toBe(1);
			});
		});

		describe('dialogClass:foo, backdropClass:bar', function(){
			useDialogWithGlobalOption({dialogClass: 'foo', backdropClass: 'bar'});

			it('backdrop class should be changed', function(){
				expect($document.find('body > div.bar').length).toBe(1);
			});

			it('the modal should be change', function(){
				expect($document.find('body > div.foo').length).toBe(1);
			});
		});

		/*
		describe('dialogFade:true, backdropFade:true', function(){
			useDialogWithGlobalOption({dialogFade:true, backdropFade:true});

			it('backdrop class should be changed', function(){
				expect($document.find('body > div.modal.fade').length).toBe(1);
			});

			it('the modal should be change', function(){
				expect($document.find('body > div.modal-backdrop.fade').length).toBe(1);
			});
		});*/
	});

	describe('Opening a dialog', function(){

		beforeEach(function(){
			createDialog({template:template});
			openDialog();
		});

		dialogShouldBeOpen();
	});

	describe('When opening a dialog with a controller', function(){

		var resolvedDialog;
		function Ctrl(dialog){
			resolvedDialog = dialog;
		}

		beforeEach(function(){
			createDialog({template:template, controller: Ctrl});
			openDialog();
		});

		dialogShouldBeOpen();

		it('should inject the current dialog in the controller', function(){
			expect(resolvedDialog).toBe(dialog);
		});
	});

	describe('When opening a dialog with resolves', function(){

		var resolvedFoo, resolvedBar, deferred, resolveObj;
		function Ctrl(foo, bar){
			resolvedFoo = foo;
			resolvedBar = bar;
		}

		beforeEach(function(){
			deferred = q.defer();
			resolveObj = {
				foo: function(){return 'foo';},
				bar: function(){return deferred.promise;}
			};

			createDialog({template:template, resolve: resolveObj, controller: Ctrl});
			deferred.resolve('bar');
			openDialog();
		});

		dialogShouldBeOpen();

		it('should inject resolved promises in the controller', function(){
			expect(resolvedBar).toBe('bar');
		});

		it('should inject simple values in the controller', function(){
			expect(resolvedFoo).toBe('foo');
		});
	});

	describe('when closing a dialog', function(){

		beforeEach(function(){
			createDialog({template:template});
			openDialog();
			closeDialog();
		});

		dialogShouldBeClosed();

		describe('When opening it again', function(){
			beforeEach(function(){
				expect($document.find('body > div.modal-backdrop').length).toBe(0);
				openDialog();
			});

			dialogShouldBeOpen();
		});
	});

	describe('when closing a dialog with a result', function(){
		var res;
		beforeEach(function(){
			createDialog({template:template});
			dialog.open().then(function(result){ res = result; });
			$rootScope.$apply();

			closeDialog('the result');
		});

		dialogShouldBeClosed();

		it('should call the then method with the specified result', function(){
			expect(res).toBe('the result');
		});
	});

	describe('when closing a dialog with backdrop click', function(){
		beforeEach(function(){
			createDialog({template:'foo'});
			openDialog();
			$document.find('body > div.modal-backdrop').click();
		});

		dialogShouldBeClosed();
	});

	describe('when closing a dialog with escape key', function(){
		beforeEach(function(){
			createDialog({template:'foo'});
			openDialog();
			var e = $.Event('keydown');
			e.which = 27;
			$document.find('body').trigger(e);
		});

		dialogShouldBeClosed();
	});

	describe('When opening a dialog with a template url', function(){

		beforeEach(function(){
			createDialog({templateUrl:'template/dialog/message.html'});
			openDialog();
		});

		dialogShouldBeOpen();
	});

	describe('When opening a dialog by passing template and controller to open method', function(){

		var controllerIsCreated;
		function Controller($scope, dialog){
			controllerIsCreated = true;
		}

		beforeEach(function(){
			createDialog({templateUrl:'this/will/not/be/used.html', controller: 'foo'});
			openDialog('template/dialog/message.html', Controller);
		});

		dialogShouldBeOpen();

		it('should used the specified controller', function(){
			expect(controllerIsCreated).toBe(true);
		});

		it('should use the specified template', function(){
			expect($document.find('body > div.modal > div.modal-header').length).toBe(1);
		});
	});

	describe('when opening it with a template containing white-space', function(){

		var controllerIsCreated;
		function Controller($scope, dialog){
			controllerIsCreated = true;
		}

		beforeEach(function(){
			createDialog({
				template:' <div>Has whitespace that IE8 does not like assigning data() to</div> ',
				controller: Controller
			});
			openDialog();
		});

		dialogShouldBeOpen();
	});
});
